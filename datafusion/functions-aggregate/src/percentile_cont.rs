// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    ArrowNumericType, ArrowPrimitiveType, BooleanArray, ListArray, PrimitiveArray,
    PrimitiveBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{DataType, Field, FieldRef, Float16Type, Float32Type, Float64Type},
};

use num_traits::AsPrimitive;

use arrow::array::ArrowNativeTypeOp;
use arrow::compute::DecimalCast;
use arrow::datatypes::{
    ArrowNativeType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DecimalType,
};
use datafusion_common::hash_utils::RandomState;
use datafusion_common::internal_err;
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_functions_aggregate_common::noop_accumulator::NoopAccumulator;

use crate::min_max::{max_udaf, min_udaf};
use crate::utils::validate_percentile_expr;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, internal_datafusion_err,
    utils::{SingleRowListArrayBuilder, take_function_args},
};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Expr, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_expr::{
    expr::{AggregateFunction, Sort},
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
    simplify::SimplifyContext,
};
use datafusion_expr::blocked_helpers::BlockedVecBuilder;
use datafusion_expr::blocked_helpers::get_heap_allocated_size::CommonHeapAllocatorSize;
use datafusion_expr::groups_accumulator::{BlockedEmitTo, BlockedGroupsAccumulator, BlocksIndex};
use datafusion_functions_aggregate_common::accumulator::BlockedAccumulatorArgs;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filtered_null_mask;
use datafusion_functions_aggregate_common::utils::Hashable;
use datafusion_macros::user_doc;

/// Precision multiplier for linear interpolation calculations.
///
/// This value of 1,000,000 was chosen to balance precision with overflow safety:
/// - Provides 6 decimal places of precision for the fractional component
/// - Small enough to avoid overflow when multiplied with typical numeric values
/// - Sufficient precision for most statistical applications
///
/// The interpolation formula: `lower + (upper - lower) * fraction`
/// is computed as: `lower + ((upper - lower) * (fraction * PRECISION)) / PRECISION`
/// to avoid floating-point operations on integer types while maintaining precision.
///
/// The interpolation arithmetic for floats is performed in f64 and then cast back to the
/// native type to avoid overflowing Float16 intermediates.
const INTERPOLATION_PRECISION: usize = 1_000_000;

create_func!(PercentileCont, percentile_cont_udaf);

/// Computes the exact percentile continuous of a set of numbers
pub fn percentile_cont(order_by: Sort, percentile: Expr) -> Expr {
    let expr = order_by.expr.clone();
    let args = vec![expr, percentile];

    Expr::AggregateFunction(AggregateFunction::new_udf(
        percentile_cont_udaf(),
        args,
        false,
        None,
        vec![order_by],
        None,
    ))
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the exact percentile of input values, interpolating between values if needed.",
    syntax_example = "percentile_cont(percentile) WITHIN GROUP (ORDER BY expression)",
    sql_example = r#"```sql
> SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+-----------------------------------------------------------+
| percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) |
+-----------------------------------------------------------+
| 45.5                                                      |
+-----------------------------------------------------------+
```

An alternate syntax is also supported:
```sql
> SELECT percentile_cont(column_name, 0.75) FROM table_name;
+---------------------------------------+
| percentile_cont(column_name, 0.75)    |
+---------------------------------------+
| 45.5                                  |
+---------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "The"),
    argument(
        name = "percentile",
        description = "Percentile to compute. Must be a float value between 0 and 1 (inclusive)."
    )
)]
/// PERCENTILE_CONT aggregate expression. This uses an exact calculation and stores all values
/// in memory before computing the result. If an approximation is sufficient then
/// APPROX_PERCENTILE_CONT provides a much more efficient solution.
///
/// If using the distinct variation, the memory usage will be similarly high if the
/// cardinality is high as it stores all distinct values in memory before computing the
/// result, but if cardinality is low then memory usage will also be lower.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct PercentileCont {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileCont {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    // Decimal signature: decimals, percentile
                    TypeSignature::Coercible(vec![
                        // value
                        Coercion::new_exact(TypeSignatureClass::Decimal),
                        // percentile
                        Coercion::new_implicit_native(
                            logical_float64(),
                            vec![TypeSignatureClass::Numeric],
                        ),
                    ]),
                    // Float signature: float, percentile
                    TypeSignature::Coercible(vec![
                        Coercion::new_implicit(
                            TypeSignatureClass::Float,
                            vec![TypeSignatureClass::Numeric],
                            NativeType::Float64,
                        ),
                        Coercion::new_implicit_native(
                            logical_float64(),
                            vec![TypeSignatureClass::Numeric],
                        ),
                    ]),
                ],
                Volatility::Immutable,
            )
            .with_parameter_names(vec!["expr", "percentile"])
            .unwrap(),
            aliases: vec![String::from("quantile_cont")],
        }
    }
}

impl AggregateUDFImpl for PercentileCont {
    fn name(&self) -> &str {
        "percentile_cont"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = args.input_fields[0].data_type().clone();
        if input_type.is_null() {
            return Ok(vec![
                Field::new(
                    format_state_name(args.name, self.name()),
                    DataType::Null,
                    true,
                )
                .into(),
            ]);
        }

        let field = Field::new_list_field(input_type, true);
        let state_name = if args.is_distinct {
            "distinct_percentile_cont"
        } else {
            "percentile_cont"
        };

        Ok(vec![
            Field::new(
                format_state_name(args.name, state_name),
                DataType::List(Arc::new(field)),
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Always verify percentiles
        let percentile = get_percentile(&args)?;
        create_percentile_accumulator(
            self.name(),
            percentile,
            args.expr_fields[0].data_type(),
            args.is_distinct,
        )
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && !args.expr_fields[0].data_type().is_null()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // Always verify percentiles
        let percentile = get_percentile(&args)?;
        create_percentile_groups_accumulator(
            self.name(),
            percentile,
            args.expr_fields[0].data_type(),
        )
    }

    fn blocked_groups_accumulator_supported(&self, args: BlockedAccumulatorArgs) -> bool {
        !args.is_distinct && !args.expr_fields[0].data_type().is_null()
    }

    fn create_blocked_groups_accumulator(
        &self,
        args: BlockedAccumulatorArgs,
    ) -> Result<Box<dyn BlockedGroupsAccumulator>> {
        // Always verify percentiles
        let percentile = get_percentile(&args)?;
        create_percentile_blocked_groups_accumulator(
            self.name(),
            percentile,
            args.expr_fields[0].data_type(),
            args.batch_size,
        )
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        Some(Box::new(|aggregate_function, info| {
            simplify_percentile_cont_aggregate(aggregate_function, info)
        }))
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn get_percentile(args: &AccumulatorArgs) -> Result<f64> {
    let percentile = validate_percentile_expr(&args.exprs[1], "PERCENTILE_CONT")?;

    let is_descending = args
        .order_bys
        .first()
        .map(|sort_expr| sort_expr.options.descending)
        .unwrap_or(false);

    let percentile = if is_descending {
        1.0 - percentile
    } else {
        percentile
    };

    Ok(percentile)
}

pub fn create_percentile_accumulator(
    name: &str,
    percentile: f64,
    input_dt: &DataType,
    is_distinct: bool,
) -> Result<Box<dyn Accumulator>> {
    // Null input evaluates to null
    if input_dt.is_null() {
        return Ok(Box::new(NoopAccumulator::default()));
    }

    macro_rules! helper {
        ($t:ty, $i:ty, $dt:expr) => {
            if is_distinct {
                Ok(Box::new(DistinctPercentileContAccumulator::<$t, $i>::new(
                    percentile,
                    $dt.clone(),
                )))
            } else {
                Ok(Box::new(PercentileContAccumulator::<$t, $i>::new(
                    percentile,
                    $dt.clone(),
                )))
            }
        };
    }
    match input_dt {
        DataType::Float16 => helper!(Float16Type, FloatInterpolator, input_dt),
        DataType::Float32 => helper!(Float32Type, FloatInterpolator, input_dt),
        DataType::Float64 => helper!(Float64Type, FloatInterpolator, input_dt),
        DataType::Decimal32(_, _) => {
            helper!(Decimal32Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal64(_, _) => {
            helper!(Decimal64Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal128(_, _) => {
            helper!(Decimal128Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal256(_, _) => {
            helper!(Decimal256Type, DecimalInterpolator, input_dt)
        }
        dt => internal_err!("Unsupported datatype for {} with {}", name, dt),
    }
}

pub fn create_percentile_groups_accumulator(
    name: &str,
    percentile: f64,
    input_dt: &DataType,
) -> Result<Box<dyn GroupsAccumulator>> {
    macro_rules! helper {
        ($t:ty, $i:ty, $dt:expr) => {
            Ok(Box::new(PercentileContGroupsAccumulator::<$t, $i>::new(
                percentile,
                $dt.clone(),
            )))
        };
    }
    match input_dt {
        DataType::Float16 => helper!(Float16Type, FloatInterpolator, input_dt),
        DataType::Float32 => helper!(Float32Type, FloatInterpolator, input_dt),
        DataType::Float64 => helper!(Float64Type, FloatInterpolator, input_dt),
        DataType::Decimal32(_, _) => {
            helper!(Decimal32Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal64(_, _) => {
            helper!(Decimal64Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal128(_, _) => {
            helper!(Decimal128Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal256(_, _) => {
            helper!(Decimal256Type, DecimalInterpolator, input_dt)
        }
        dt => internal_err!("Unsupported datatype for {} with {}", name, dt),
    }
}
pub fn create_percentile_blocked_groups_accumulator(
    name: &str,
    percentile: f64,
    input_dt: &DataType,
    block_size: usize,
) -> Result<Box<dyn BlockedGroupsAccumulator>> {
    macro_rules! helper {
        ($t:ty, $i:ty, $dt:expr) => {
            Ok(Box::new(PercentileContBlockedGroupsAccumulator::<$t, $i>::new(
                percentile,
                $dt.clone(),
                block_size
            )))
        };
    }
    match input_dt {
        DataType::Float16 => helper!(Float16Type, FloatInterpolator, input_dt),
        DataType::Float32 => helper!(Float32Type, FloatInterpolator, input_dt),
        DataType::Float64 => helper!(Float64Type, FloatInterpolator, input_dt),
        DataType::Decimal32(_, _) => {
            helper!(Decimal32Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal64(_, _) => {
            helper!(Decimal64Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal128(_, _) => {
            helper!(Decimal128Type, DecimalInterpolator, input_dt)
        }
        DataType::Decimal256(_, _) => {
            helper!(Decimal256Type, DecimalInterpolator, input_dt)
        }
        dt => internal_err!("Unsupported datatype for {} with {}", name, dt),
    }
}

fn simplify_percentile_cont_aggregate(
    aggregate_function: AggregateFunction,
    info: &SimplifyContext,
) -> Result<Expr> {
    enum PercentileRewriteTarget {
        Min,
        Max,
    }

    let params = &aggregate_function.params;
    let [value, percentile] = take_function_args("percentile_cont", &params.args)?;
    //
    // For simplicity we don't bother with null types (otherwise we'd need to
    // cast the return type)
    let input_type = info.get_data_type(value)?;
    if input_type.is_null() {
        return Ok(Expr::AggregateFunction(aggregate_function));
    }

    let is_descending = params
        .order_by
        .first()
        .map(|sort| !sort.asc)
        .unwrap_or(false);

    let rewrite_target = match percentile {
        Expr::Literal(ScalarValue::Float64(Some(0.0)), _) => {
            if is_descending {
                PercentileRewriteTarget::Max
            } else {
                PercentileRewriteTarget::Min
            }
        }
        Expr::Literal(ScalarValue::Float64(Some(1.0)), _) => {
            if is_descending {
                PercentileRewriteTarget::Min
            } else {
                PercentileRewriteTarget::Max
            }
        }
        _ => return Ok(Expr::AggregateFunction(aggregate_function)),
    };

    let udaf = match rewrite_target {
        PercentileRewriteTarget::Min => min_udaf(),
        PercentileRewriteTarget::Max => max_udaf(),
    };

    let rewritten = Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        vec![value.clone()],
        params.distinct,
        params.filter.clone(),
        vec![],
        params.null_treatment,
    ));
    Ok(rewritten)
}

/// The percentile_cont accumulator accumulates the raw input values
/// as native types.
///
/// The intermediate state is represented as a List of scalar values updated by
/// `merge_batch` and a `Vec` of native values that are converted to scalar values
/// in the final evaluation step so that we avoid expensive conversions and
/// allocations during `update_batch`.
#[derive(Debug)]
struct PercentileContAccumulator<
    T: ArrowNumericType + Debug,
    I: PercentileInterpolator<T>,
> {
    all_values: Vec<T::Native>,
    percentile: f64,
    data_type: DataType,
    _interpolator: PhantomData<I>,
}

impl<T: ArrowNumericType + Debug, I: PercentileInterpolator<T>>
    PercentileContAccumulator<T, I>
{
    fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            all_values: vec![],
            percentile,
            data_type,
            _interpolator: PhantomData,
        }
    }
}

impl<T, I> Accumulator for PercentileContAccumulator<T, I>
where
    T: ArrowNumericType + Debug,
    I: PercentileInterpolator<T> + 'static,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Convert `all_values` to `ListArray` and return a single List ScalarValue

        // Build offsets
        let offsets =
            OffsetBuffer::new(ScalarBuffer::from(vec![0, self.all_values.len() as i32]));

        // Build inner array
        let values_array = PrimitiveArray::<T>::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        )
        .with_data_type(self.data_type.clone());

        // Build the result list array
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(values_array),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        let additional = values.len() - values.null_count();
        self.all_values.try_reserve(additional).map_err(|e| {
            exec_datafusion_err!(
                "failed to reserve {additional} values for percentile_cont accumulator: {e}"
            )
        })?;
        if values.null_count() > 0 {
            self.all_values.extend(values.iter().flatten());
        } else {
            // Fast path: no nulls, so the values buffer can be appended wholesale.
            self.all_values.extend_from_slice(values.values());
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        // Feed all list elements from a batch
        for values in array.iter().flatten() {
            self.update_batch(&[values])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = calculate_percentile::<T, I>(&mut self.all_values, self.percentile)?;
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let mut to_remove: HashMap<Hashable<T::Native>, usize, RandomState> =
            HashMap::default();

        let arr = values[0].as_primitive::<T>();
        if arr.null_count() > 0 {
            for value in arr.iter().flatten() {
                *to_remove.entry(Hashable(value)).or_default() += 1;
            }
        } else {
            // Fast path: no nulls, so skip the per-element validity check.
            for value in arr.values().iter() {
                *to_remove.entry(Hashable(*value)).or_default() += 1;
            }
        }

        let mut i = 0;
        while i < self.all_values.len() {
            let k = Hashable(self.all_values[i]);
            if let Some(count) = to_remove.get_mut(&k)
                && *count > 0
            {
                self.all_values.swap_remove(i);
                *count -= 1;
                if *count == 0 {
                    to_remove.remove(&k);
                    if to_remove.is_empty() {
                        break;
                    }
                }
            } else {
                i += 1;
            }
        }

        // Retracting values that are not tracked means the accumulator state
        // has diverged from the window frame; continuing would silently
        // produce wrong results, so surface it as an error.
        if !to_remove.is_empty() {
            return internal_err!(
                "percentile_cont retract_batch: retracted value(s) not present in the window"
            );
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// The percentile_cont groups accumulator accumulates the raw input values
///
/// For calculating the exact percentile of groups, we need to store all values
/// of groups before final evaluation.
/// So values in each group will be stored in a `Vec<T>`, and the total group values
/// will be actually organized as a `Vec<Vec<T>>`.
#[derive(Debug)]
struct PercentileContGroupsAccumulator<
    T: ArrowNumericType + Debug,
    I: PercentileInterpolator<T>,
> {
    group_values: Vec<Vec<T::Native>>,
    percentile: f64,
    data_type: DataType,
    _interpolator: PhantomData<I>,
}

impl<T: ArrowNumericType + Debug, I: PercentileInterpolator<T>>
PercentileContGroupsAccumulator<T, I>
{
    fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            group_values: vec![],
            percentile,
            data_type,
            _interpolator: PhantomData,
        }
    }
}

impl<T, I> GroupsAccumulator for PercentileContGroupsAccumulator<T, I>
where
  T: ArrowNumericType + Debug + Send,
  I: PercentileInterpolator<T> + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // For ordered-set aggregates, we only care about the ORDER BY column (first element)
        // The percentile parameter is already stored in self.percentile

        let values = values[0].as_primitive::<T>();

        // Push the `not nulls + not filtered` row into its group
        self.group_values.resize(total_num_groups, Vec::new());
        accumulate(
            group_indices,
            values,
            opt_filter,
            |group_index, new_value| {
                self.group_values[group_index].push(new_value);
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");

        let input_group_values = values[0].as_list::<i32>();

        // Ensure group values big enough
        self.group_values.resize(total_num_groups, Vec::new());

        // Extend values to related groups
        group_indices
          .iter()
          .zip(input_group_values.iter())
          .for_each(|(&group_index, values_opt)| {
              if let Some(values) = values_opt {
                  let values = values.as_primitive::<T>();
                  self.group_values[group_index].extend(values.values().iter());
              }
          });

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // Emit values
        let emit_group_values = emit_to.take_needed(&mut self.group_values);

        // Build offsets
        let mut offsets = Vec::with_capacity(self.group_values.len() + 1);
        offsets.push(0);
        let mut cur_len = 0_i32;
        for group_value in &emit_group_values {
            cur_len += group_value.len() as i32;
            offsets.push(cur_len);
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));

        // Build inner array
        let flatten_group_values =
          emit_group_values.into_iter().flatten().collect::<Vec<_>>();
        let group_values_array =
          PrimitiveArray::<T>::new(ScalarBuffer::from(flatten_group_values), None)
            .with_data_type(self.data_type.clone());

        // Build the result list array
        let result_list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(group_values_array),
            None,
        );

        Ok(vec![Arc::new(result_list_array)])
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        // Emit values
        let mut emit_group_values = emit_to.take_needed(&mut self.group_values);

        // Calculate percentile for each group
        let mut evaluate_result_builder =
          PrimitiveBuilder::<T>::with_capacity(emit_group_values.len())
            .with_data_type(self.data_type.clone());
        for values in &mut emit_group_values {
            let value =
              calculate_percentile::<T, I>(values.as_mut_slice(), self.percentile)?;
            evaluate_result_builder.append_option(value);
        }

        Ok(Arc::new(evaluate_result_builder.finish()))
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        convert_percentile_cont_to_state::<T>(values, opt_filter, self.data_type.clone())
    }
    fn size(&self) -> usize {
        self.group_values
          .iter()
          .map(|values| values.capacity() * size_of::<T::Native>())
          .sum::<usize>()
          // account for size of self.group_values too
          + self.group_values.capacity() * size_of::<Vec<T::Native>>()
    }
}

/// The percentile_cont groups accumulator accumulates the raw input values
///
/// For calculating the exact percentile of groups, we need to store all values
/// of groups before final evaluation.
/// So values in each group will be stored in a `Vec<T>`, and the total group values
/// will be actually organized as a `Vec<Vec<T>>`.
#[derive(Debug)]
struct PercentileContBlockedGroupsAccumulator<
    T: ArrowNumericType + Debug,
    I: PercentileInterpolator<T>,
> {
    group_values: BlockedVecBuilder<true, Vec<T::Native>, CommonHeapAllocatorSize>,
    percentile: f64,
    data_type: DataType,
    _interpolator: PhantomData<I>,
}

impl<T: ArrowNumericType + Debug, I: PercentileInterpolator<T>>
PercentileContBlockedGroupsAccumulator<T, I>
{
    fn new(percentile: f64, data_type: DataType, batch_size: usize) -> Self {
        Self {
            group_values: BlockedVecBuilder::new(batch_size),
            percentile,
            data_type,
            _interpolator: PhantomData,
        }
    }

    fn build_state(&mut self, emit_group_values: Vec<Vec<<T as ArrowPrimitiveType>::Native>>) -> Vec<ArrayRef> {
        // Build offsets
        let mut offsets = Vec::with_capacity(emit_group_values.len() + 1);
        offsets.push(0);
        let mut cur_len = 0_i32;
        for group_value in &emit_group_values {
            cur_len += group_value.len() as i32;
            offsets.push(cur_len);
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));

        // Build inner array
        let flatten_group_values =
          emit_group_values.into_iter().flatten().collect::<Vec<_>>();
        let group_values_array =
          PrimitiveArray::<T>::new(ScalarBuffer::from(flatten_group_values), None)
            .with_data_type(self.data_type.clone());

        // Build the result list array
        let result_list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offsets,
            Arc::new(group_values_array),
            None,
        );

        vec![Arc::new(result_list_array)]
    }

    fn build_evaluate(&mut self, mut emit_group_values: Vec<Vec<<T as ArrowPrimitiveType>::Native>>) -> Result<ArrayRef> {
        // Calculate percentile for each group
        let mut evaluate_result_builder =
          PrimitiveBuilder::<T>::with_capacity(emit_group_values.len())
            .with_data_type(self.data_type.clone());
        for values in &mut emit_group_values {
            let value =
              calculate_percentile::<T, I>(values.as_mut_slice(), self.percentile)?;
            evaluate_result_builder.append_option(value);
        }

        Ok(Arc::new(evaluate_result_builder.finish()))
    }
}

impl<T, I> BlockedGroupsAccumulator for PercentileContBlockedGroupsAccumulator<T, I>
where
  T: ArrowNumericType + Debug + Send,
  I: PercentileInterpolator<T> + 'static,
{
    fn batch_size(&self) -> usize {
        self.group_values.block_size()
    }

    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // For ordered-set aggregates, we only care about the ORDER BY column (first element)
        // The percentile parameter is already stored in self.percentile

        let values = values[0].as_primitive::<T>();

        // Push the `not nulls + not filtered` row into its group
        {
            let prev_len = self.group_values.len();
            assert!(total_num_groups >= prev_len);
            self.group_values.push_value_n(Vec::new(), total_num_groups - prev_len);
        }
        accumulate(
            group_indices,
            values,
            opt_filter,
            |group_index, new_value| {
                self.group_values.index_mut_with_size(group_index, |item| {
                    item.push(new_value);
                });
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");

        let input_group_values = values[0].as_list::<i32>();

        // Ensure group values big enough
        {
            let prev_len = self.group_values.len();
            assert!(total_num_groups >= prev_len);
            self.group_values.push_value_n(Vec::new(), total_num_groups - prev_len);
        }

        // Extend values to related groups
        group_indices
          .iter()
          .zip(input_group_values.iter())
          .for_each(|(&group_index, values_opt)| {
              if let Some(values) = values_opt {
                  let values = values.as_primitive::<T>();
                  self.group_values.index_mut_with_size(group_index, |item| {
                      item.extend(values.values().iter());
                  });
              }
          });

        Ok(())
    }

    fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        match emit_to {
            BlockedEmitTo::All => {
                let mut blocks = Vec::with_capacity(self.group_values.num_blocks());

                for block in self.group_values.take_all() {
                    blocks.push(self.build_state(block));
                }

                Ok(blocks)
            }
            BlockedEmitTo::NextBlock => {
                // Emit values
                let Some(emit_group_values) = self.group_values.take_block() else {
                    return Ok(vec![]);
                };

                Ok(vec![self.build_state(emit_group_values)])
            }
            BlockedEmitTo::First(n) => {
                let values = self.group_values.take_n_fixed(n);
                Ok(vec![self.build_state(values)])
            }
        }
    }

    fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        match emit_to {
            BlockedEmitTo::All => {
                let mut blocks = Vec::with_capacity(self.group_values.num_blocks());

                for block in self.group_values.take_all() {
                    blocks.push(self.build_evaluate(block)?);
                }

                Ok(blocks)
            }
            BlockedEmitTo::NextBlock => {
                // Emit values
                let Some(emit_group_values) = self.group_values.take_block() else {
                    return Ok(vec![]);
                };

                Ok(vec![self.build_evaluate(emit_group_values)?])
            }
            BlockedEmitTo::First(n) => {
                let values = self.group_values.take_n_fixed(n);
                Ok(vec![self.build_evaluate(values)?])
            }
        }
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        convert_percentile_cont_to_state::<T>(values, opt_filter, self.data_type.clone())
    }

    fn size(&self) -> usize {
        self.group_values.allocated_size()
    }
}

fn convert_percentile_cont_to_state<T: ArrowNumericType>(
    values: &[ArrayRef],
    opt_filter: Option<&BooleanArray>,
    data_type: DataType,
) -> Result<Vec<ArrayRef>> {
    assert_eq!(values.len(), 1, "one argument to merge_batch");

    let input_array = values[0].as_primitive::<T>();

    // Directly convert the input array to states, each row will be
    // seen as a respective group.
    // For detail, the `input_array` will be converted to a `ListArray`.
    // And if row is `not null + not filtered`, it will be converted to a list
    // with only one element; otherwise, this row in `ListArray` will be set
    // to null.

    // Reuse values buffer in `input_array` to build `values` in `ListArray`
    let values = PrimitiveArray::<T>::new(input_array.values().clone(), None)
      .with_data_type(data_type.clone());

    // `offsets` in `ListArray`, each row as a list element
    let offset_end = i32::try_from(input_array.len()).map_err(|e| {
        internal_datafusion_err!(
                "cast array_len to i32 failed in convert_to_state of group percentile_cont, err:{e:?}"
            )
    })?;
    let offsets = (0..=offset_end).collect::<Vec<_>>();
    // Safety: The offsets vector is constructed as a sequential range from 0 to input_array.len(),
    // which guarantees all OffsetBuffer invariants:
    // 1. Offsets are monotonically increasing (each element is prev + 1)
    // 2. No offset exceeds the values array length (max offset = input_array.len())
    // 3. First offset is 0 and last offset equals the total length
    // Therefore new_unchecked is safe to use here.
    let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };

    // `nulls` for converted `ListArray`
    let nulls = filtered_null_mask(opt_filter, input_array);

    let converted_list_array = ListArray::new(
        Arc::new(Field::new_list_field(data_type.clone(), true)),
        offsets,
        Arc::new(values),
        nulls,
    );

    Ok(vec![Arc::new(converted_list_array)])
}

/// Sliding-window–capable accumulator for `percentile_cont(DISTINCT ...)`.
///
/// Distinct values are tracked with a per-value multiplicity count (how many
/// rows currently in the window carry that value) rather than a plain set, so
/// that `retract_batch` only drops a value once *all* of its occurrences have
/// left the window frame. The percentile is then computed over the set of keys
/// with a positive count.
#[derive(Debug)]
struct DistinctPercentileContAccumulator<
    T: ArrowNumericType,
    I: PercentileInterpolator<T>,
> {
    /// Distinct value -> number of in-window rows carrying it.
    ///
    /// Uses the same fast (foldhash) `RandomState` as the shared
    /// `GenericDistinctBuffer` rather than the standard library's default
    /// SipHash, which is considerably slower for this hot path.
    counts: HashMap<Hashable<T::Native>, usize, RandomState>,
    percentile: f64,
    data_type: DataType,
    _interpolator: PhantomData<I>,
}

impl<T: ArrowNumericType, I: PercentileInterpolator<T>>
    DistinctPercentileContAccumulator<T, I>
{
    fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            counts: HashMap::default(),
            percentile,
            data_type,
            _interpolator: PhantomData,
        }
    }
}

impl<T, I> Accumulator for DistinctPercentileContAccumulator<T, I>
where
    T: ArrowNumericType + Debug,
    I: PercentileInterpolator<T> + 'static,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Emit the distinct keys as a single List scalar, matching the state
        // shape declared in `state_fields` (a List of the input type). Counts
        // are window-local bookkeeping and are intentionally not serialized:
        // cross-partition merges only need the distinct key set.
        let arr = Arc::new(
            PrimitiveArray::<T>::from_iter_values(self.counts.keys().map(|v| v.0))
                .with_data_type(self.data_type.clone()),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // `values` may carry extra argument columns (e.g. the percentile
        // literal); only the first column holds the aggregated values.
        let arr = values[0].as_primitive::<T>();
        if arr.null_count() > 0 {
            for value in arr.iter().flatten() {
                *self.counts.entry(Hashable(value)).or_default() += 1;
            }
        } else {
            // Fast path: no nulls, so skip the per-element validity check.
            for value in arr.values().iter() {
                *self.counts.entry(Hashable(*value)).or_default() += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list = states[0].as_list::<i32>();
        for values in list.iter().flatten() {
            let arr = values.as_primitive::<T>();
            for value in arr.iter().flatten() {
                *self.counts.entry(Hashable(value)).or_default() += 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut values: Vec<T::Native> = self.counts.keys().map(|v| v.0).collect();
        let value = calculate_percentile::<T, I>(&mut values, self.percentile)?;
        ScalarValue::new_primitive::<T>(value, &self.data_type)
    }

    fn size(&self) -> usize {
        estimate_memory_size::<(Hashable<T::Native>, usize)>(
            self.counts.capacity(),
            size_of_val(self),
        )
        .unwrap()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = values[0].as_primitive::<T>();
        let mut decrement = |value: T::Native| {
            match self.counts.get_mut(&Hashable(value)) {
                Some(count) => {
                    *count -= 1;
                    if *count == 0 {
                        self.counts.remove(&Hashable(value));
                    }
                    Ok(())
                }
                // Retracting a value that isn't tracked means the accumulator
                // state has diverged from the window frame; continuing would
                // silently produce wrong results, so surface it as an error.
                None => internal_err!(
                    "percentile_cont(DISTINCT) retract_batch: retracted a value not present in the window"
                ),
            }
        };
        if arr.null_count() > 0 {
            for value in arr.iter().flatten() {
                decrement(value)?;
            }
        } else {
            // Fast path: no nulls, so skip the per-element validity check.
            for value in arr.values().iter() {
                decrement(*value)?;
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// A trait to abstract interpolation logic for percentile calculation
/// for floats and decimals.
trait PercentileInterpolator<T: ArrowNumericType>: Debug + Sync + Send {
    fn interpolate(
        lower: T::Native,
        upper: T::Native,
        fraction: f64,
    ) -> Result<T::Native>;
}

#[derive(Debug)]
struct FloatInterpolator;

/// The interpolation arithmetic for floats is performed in f64 and then cast back to the
/// native type to avoid overflowing Float16 intermediates.
impl<T> PercentileInterpolator<T> for FloatInterpolator
where
    T: ArrowNumericType,
    T::Native: AsPrimitive<f64>,
    f64: AsPrimitive<T::Native>,
{
    fn interpolate(
        lower: T::Native,
        upper: T::Native,
        fraction: f64,
    ) -> Result<T::Native> {
        // Linear interpolation.
        // We compute a quantized interpolation weight using `FLOAT_INTERPOLATION_PRECISION` because:
        // 1. Both values come from the input data, so (upper - lower) is bounded by the value range
        // 2. fraction is between 0 and 1; quantizing it provides stable, predictable results
        // 3. The result is guaranteed to be between lower_value and upper_value (modulo cast rounding)
        // 4. Arithmetic is performed in f64 and cast back to avoid overflowing Float16 intermediates
        let scaled = (fraction * (INTERPOLATION_PRECISION as f64)) as usize;
        let weight = scaled as f64 / (INTERPOLATION_PRECISION as f64);

        let lower_f: f64 = lower.as_();
        let upper_f: f64 = upper.as_();
        let interpolated_f = lower_f + (upper_f - lower_f) * weight;
        Ok(interpolated_f.as_())
    }
}

#[derive(Debug)]
struct DecimalInterpolator;

/// Compute a scaled value for interpolation using a formula `trunc(x * num / den)`
/// where den is a precisely chosen interpolation precision.
/// The numerical method is separating `q * num + trunc(r * num / den)`
/// where `q = x / den` and `r = x % den`
fn scale_by_num<T>(x: T::Native, num: i64) -> Result<T::Native>
where
    T: DecimalType,
    T::Native: DecimalCast,
{
    let den = INTERPOLATION_PRECISION as i64;

    debug_assert!(num >= 0);
    debug_assert!(num <= den);
    debug_assert!(den <= i32::MAX as i64);

    let num_native = T::Native::usize_as(num as usize);
    let den_native = T::Native::usize_as(den as usize);

    // q and r fit `den` and thus i32 (smallest Decimal32's native type)
    let q = x.div_wrapping(den_native);
    let r = x.mod_wrapping(den_native);
    let r_wide = ArrowNativeType::to_i64(r)
        .ok_or_else(|| exec_datafusion_err!("Arithmetic overflow in percentile_cont"))?;

    // `a = q * num` cannot exceed `x` and thus i32
    let a = q.mul_checked(num_native)?;

    // `r * num` cannot exceed `den^2`, and `r * num / den` cannot exceed `den`, fits i32
    let b_wide = r_wide.mul_checked(num)?.div_wrapping(den);
    let b = T::Native::from_decimal(b_wide)
        .ok_or_else(|| exec_datafusion_err!("Arithmetic overflow in percentile_cont"))?;

    a.add_checked(b)
        .map_err(|e| exec_datafusion_err!("Arithmetic overflow in percentile_cont: {e}"))
}

impl<T> PercentileInterpolator<T> for DecimalInterpolator
where
    T: DecimalType,
    T::Native: DecimalCast,
{
    fn interpolate(
        lower: T::Native,
        upper: T::Native,
        fraction: f64,
    ) -> Result<T::Native> {
        debug_assert!((0.0..=1.0).contains(&fraction));
        debug_assert!(lower <= upper);

        let num = (fraction * INTERPOLATION_PRECISION as f64) as i64;
        let den = INTERPOLATION_PRECISION as i64;

        // Happy path: `upper - lower` does not overflow
        // (could be a case for Decimal128 with max precision)
        if let Ok(delta) = upper.sub_checked(lower) {
            // Calculate the interpolation weight with the formula, where den is the precision:
            // `lower + (upper - lower) * num / den`
            let scaled: T::Native = scale_by_num::<T>(delta, num)?;
            lower.add_checked(scaled).map_err(|e| {
                exec_datafusion_err!("Arithmetic overflow in percentile_cont: {e}")
            })
        } else {
            // Avoid overflow with the subtraction - split to two additive parts
            // `a = lower * (precision-num) / precision`
            // `b = upper * num / precision`
            // The weights sum to 1, so the result is bounded by max(|lower|, |upper|)
            // and never overflows, at the cost of a second truncation (2 ULP not 1).
            let num_a = den.sub_wrapping(num);
            let a: T::Native = scale_by_num::<T>(lower, num_a)?;

            let b: T::Native = scale_by_num::<T>(upper, num)?;

            a.add_checked(b).map_err(|e| {
                exec_datafusion_err!("Arithmetic overflow in percentile_cont: {e}")
            })
        }
    }
}

/// Calculate the percentile value for a given set of values.
/// This function performs an exact calculation by sorting all values.
///
/// The percentile is calculated using linear interpolation between closest ranks.
/// For percentile p and n values:
/// - If p * (n-1) is an integer, return the value at that position
/// - Otherwise, interpolate between the two closest values
///
/// Note: This function takes a mutable slice and sorts it in place, but does not
/// consume the data. This is important for window frame queries where evaluate()
/// may be called multiple times on the same accumulator state.
fn calculate_percentile<T: ArrowPrimitiveType, I: PercentileInterpolator<T>>(
    values: &mut [T::Native],
    percentile: f64,
) -> Result<Option<T::Native>> {
    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    let len = values.len();
    if len == 0 {
        Ok(None)
    } else if len == 1 {
        Ok(Some(values[0]))
    } else if percentile == 0.0 {
        // Get minimum value
        Ok(Some(
            *values
                .iter()
                .min_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        ))
    } else if percentile == 1.0 {
        // Get maximum value
        Ok(Some(
            *values
                .iter()
                .max_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        ))
    } else {
        // Calculate the index using the formula: p * (n - 1)
        let index = percentile * ((len - 1) as f64);
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            // Exact index, return the value at that position
            let (_, value, _) = values.select_nth_unstable_by(lower_index, cmp);
            Ok(Some(*value))
        } else {
            // Need to interpolate between two values
            // First, partition at lower_index to get the lower value
            let (_, lower_value, _) = values.select_nth_unstable_by(lower_index, cmp);
            let lower_value = *lower_value;

            // Then partition at upper_index to get the upper value
            let (_, upper_value, _) = values.select_nth_unstable_by(upper_index, cmp);
            let upper_value = *upper_value;

            let fraction = index - (lower_index as f64);

            let interpolated = I::interpolate(lower_value, upper_value, fraction)?;

            Ok(Some(interpolated))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{Decimal64Type, Decimal128Type, Float16Type, Float64Type};
    use half::f16;

    #[test]
    fn retract_batch_errors_on_untracked_value() {
        let mut acc = PercentileContAccumulator::<Float64Type, FloatInterpolator>::new(
            0.5,
            DataType::Float64,
        );
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0]));
        acc.update_batch(std::slice::from_ref(&values)).unwrap();

        let retract: ArrayRef = Arc::new(Float64Array::from(vec![3.0]));
        let err = acc
            .retract_batch(std::slice::from_ref(&retract))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("not present in the window"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn update_batch_with_and_without_nulls_agree() {
        // The null-free fast path must accumulate the same values as the
        // general path.
        let dense: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let sparse: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(2.0),
            None,
            Some(3.0),
        ]));

        let mut dense_acc =
            PercentileContAccumulator::<Float64Type, FloatInterpolator>::new(
                0.5,
                DataType::Float64,
            );
        dense_acc
            .update_batch(std::slice::from_ref(&dense))
            .unwrap();
        let mut sparse_acc =
            PercentileContAccumulator::<Float64Type, FloatInterpolator>::new(
                0.5,
                DataType::Float64,
            );
        sparse_acc
            .update_batch(std::slice::from_ref(&sparse))
            .unwrap();

        assert_eq!(dense_acc.all_values, sparse_acc.all_values);
    }

    #[test]
    fn f16_interpolation_does_not_overflow_to_nan() {
        // Regression test for https://github.com/apache/datafusion/issues/18945
        // Interpolating between 0 and the max finite f16 value previously overflowed
        // intermediate f16 computations and produced NaN.
        let mut values = vec![f16::from_f32(0.0), f16::from_f32(65504.0)];
        let result =
            calculate_percentile::<Float16Type, FloatInterpolator>(&mut values, 0.5)
                .expect("non-empty input")
                .expect("non-empty result");
        let result_f = result.to_f32();
        assert!(
            !result_f.is_nan(),
            "expected non-NaN result, got {result_f}"
        );
        // 0.5 percentile should be close to midpoint
        assert!(
            (result_f - 32752.0).abs() < 1.0,
            "unexpected result {result_f}"
        );
    }

    #[test]
    fn percentile_cont_decimal64() {
        // Test values: [100.00, 200.00, 300.00, 400.00, 500.00]
        // These are stored as i64 values scaled by 10^2
        let mut values = vec![
            10000i64, // 100.00
            20000i64, // 200.00
            30000i64, // 300.00
            40000i64, // 400.00
            50000i64, // 500.00
        ];

        // Test 50th percentile (median)
        // Should return 300.00 (30000)
        let result =
            calculate_percentile::<Decimal64Type, DecimalInterpolator>(&mut values, 0.5)
                .expect("evaluate failed")
                .expect("expected Some value");

        assert_eq!(result, 30000i64, "50th percentile should be 300.00");

        // Test 15th percentile
        // Should return 160.00 (16000)
        let result =
            calculate_percentile::<Decimal64Type, DecimalInterpolator>(&mut values, 0.15)
                .expect("evaluate failed")
                .expect("expected Some value");

        assert_eq!(result, 16000i64, "15th percentile should be 160.00");

        // Test 0th percentile (minimum)
        let mut values = vec![10000i64, 20000i64, 30000i64];
        let result =
            calculate_percentile::<Decimal64Type, DecimalInterpolator>(&mut values, 0.0)
                .expect("evaluate failed")
                .expect("expected Some value");

        assert_eq!(
            result, 10000i64,
            "0th percentile should be minimum value 100.00"
        );

        // Test 100th percentile (maximum)
        let mut values = vec![10000i64, 20000i64, 30000i64];
        let result =
            calculate_percentile::<Decimal64Type, DecimalInterpolator>(&mut values, 1.0)
                .expect("evaluate failed")
                .expect("expected Some value");

        assert_eq!(
            result, 30000i64,
            "100th percentile should be maximum value 300.00"
        );
    }

    #[test]
    fn percentile_cont_decimal128_subtraction_overflow() {
        // Case for interpolation overflow (upper - lower cannot fit i128),
        // where `interpolate` takes the second branch
        let boundary = 100_000_000_000_000_000_000_000_000_000_000_000_000i128;
        let lower = -boundary;
        let upper = boundary;
        assert!(
            upper.checked_sub(lower).is_none(),
            "test premise: must overflow"
        );

        let mut values = vec![lower, upper];
        let result = calculate_percentile::<Decimal128Type, DecimalInterpolator>(
            &mut values,
            0.25,
        )
        .expect("evaluate failed")
        .expect("expected Some value");

        assert_eq!(
            result,
            -boundary / 2,
            "interpolation should split into two additive parts without overflowing"
        );
    }
}
