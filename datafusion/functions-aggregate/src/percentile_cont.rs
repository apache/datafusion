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
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    ArrowNumericType, BooleanArray, ListArray, PrimitiveArray, PrimitiveBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        ArrowNativeType, DataType, Field, FieldRef, Float16Type, Float32Type, Float64Type,
    },
};

use arrow::array::ArrowNativeTypeOp;
use datafusion_common::internal_err;
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_functions_aggregate_common::noop_accumulator::NoopAccumulator;

use crate::min_max::{max_udaf, min_udaf};
use datafusion_common::{
    Result, ScalarValue, internal_datafusion_err, utils::take_function_args,
};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Expr, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_expr::{
    expr::{AggregateFunction, Sort},
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
    simplify::SimplifyContext,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filtered_null_mask;
use datafusion_functions_aggregate_common::utils::{GenericDistinctBuffer, Hashable};
use datafusion_macros::user_doc;

use crate::utils::validate_percentile_expr;

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
+----------------------------------------------------------+
| percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) |
+----------------------------------------------------------+
| 45.5                                                     |
+----------------------------------------------------------+
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
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Float,
                        vec![TypeSignatureClass::Numeric],
                        NativeType::Float64,
                    ),
                    Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Numeric],
                        NativeType::Float64,
                    ),
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Float64),
            dt => Ok(dt.clone()),
        }
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
        let percentile = get_percentile(&args)?;

        let input_dt = args.expr_fields[0].data_type();
        if input_dt.is_null() {
            return Ok(Box::new(NoopAccumulator::new(ScalarValue::Float64(None))));
        }

        if args.is_distinct {
            match input_dt {
                DataType::Float16 => Ok(Box::new(DistinctPercentileContAccumulator::<
                    Float16Type,
                >::new(percentile))),
                DataType::Float32 => Ok(Box::new(DistinctPercentileContAccumulator::<
                    Float32Type,
                >::new(percentile))),
                DataType::Float64 => Ok(Box::new(DistinctPercentileContAccumulator::<
                    Float64Type,
                >::new(percentile))),
                dt => internal_err!("Unsupported datatype for percentile cont: {dt}"),
            }
        } else {
            match input_dt {
                DataType::Float16 => Ok(Box::new(
                    PercentileContAccumulator::<Float16Type>::new(percentile),
                )),
                DataType::Float32 => Ok(Box::new(
                    PercentileContAccumulator::<Float32Type>::new(percentile),
                )),
                DataType::Float64 => Ok(Box::new(
                    PercentileContAccumulator::<Float64Type>::new(percentile),
                )),
                dt => internal_err!("Unsupported datatype for percentile cont: {dt}"),
            }
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct && !args.expr_fields[0].data_type().is_null()
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let percentile = get_percentile(&args)?;

        let input_dt = args.expr_fields[0].data_type();
        match input_dt {
            DataType::Float16 => Ok(Box::new(PercentileContGroupsAccumulator::<
                Float16Type,
            >::new(percentile))),
            DataType::Float32 => Ok(Box::new(PercentileContGroupsAccumulator::<
                Float32Type,
            >::new(percentile))),
            DataType::Float64 => Ok(Box::new(PercentileContGroupsAccumulator::<
                Float64Type,
            >::new(percentile))),
            dt => internal_err!("Unsupported datatype for percentile cont: {dt}"),
        }
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
struct PercentileContAccumulator<T: ArrowNumericType + Debug> {
    all_values: Vec<T::Native>,
    percentile: f64,
}

impl<T: ArrowNumericType + Debug> PercentileContAccumulator<T> {
    fn new(percentile: f64) -> Self {
        Self {
            all_values: vec![],
            percentile,
        }
    }
}

impl<T: ArrowNumericType + Debug> Accumulator for PercentileContAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Convert `all_values` to `ListArray` and return a single List ScalarValue

        // Build offsets
        let offsets =
            OffsetBuffer::new(ScalarBuffer::from(vec![0, self.all_values.len() as i32]));

        // Build inner array
        let values_array = PrimitiveArray::<T>::new(
            ScalarBuffer::from(std::mem::take(&mut self.all_values)),
            None,
        );

        // Build the result list array
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
            offsets,
            Arc::new(values_array),
            None,
        );

        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.all_values.reserve(values.len() - values.null_count());
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        self.update_batch(&[array.value(0)])?;
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = calculate_percentile::<T>(&mut self.all_values, self.percentile);
        ScalarValue::new_primitive::<T>(value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let mut to_remove: HashMap<ScalarValue, usize> = HashMap::new();
        for i in 0..values[0].len() {
            let v = ScalarValue::try_from_array(&values[0], i)?;
            if !v.is_null() {
                *to_remove.entry(v).or_default() += 1;
            }
        }

        let mut i = 0;
        while i < self.all_values.len() {
            let k =
                ScalarValue::new_primitive::<T>(Some(self.all_values[i]), &T::DATA_TYPE)?;
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
struct PercentileContGroupsAccumulator<T: ArrowNumericType + Send> {
    group_values: Vec<Vec<T::Native>>,
    percentile: f64,
}

impl<T: ArrowNumericType + Send> PercentileContGroupsAccumulator<T> {
    fn new(percentile: f64) -> Self {
        Self {
            group_values: vec![],
            percentile,
        }
    }
}

impl<T: ArrowNumericType + Send> GroupsAccumulator
    for PercentileContGroupsAccumulator<T>
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
        // Since aggregate filter should be applied in partial stage, in final stage there should be no filter
        _opt_filter: Option<&BooleanArray>,
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
            PrimitiveArray::<T>::new(ScalarBuffer::from(flatten_group_values), None);

        // Build the result list array
        let result_list_array = ListArray::new(
            Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
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
            PrimitiveBuilder::<T>::with_capacity(emit_group_values.len());
        for values in &mut emit_group_values {
            let value = calculate_percentile::<T>(values.as_mut_slice(), self.percentile);
            evaluate_result_builder.append_option(value);
        }

        Ok(Arc::new(evaluate_result_builder.finish()))
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
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
        let values = PrimitiveArray::<T>::new(input_array.values().clone(), None);

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
            Arc::new(Field::new_list_field(T::DATA_TYPE, true)),
            offsets,
            Arc::new(values),
            nulls,
        );

        Ok(vec![Arc::new(converted_list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
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

#[derive(Debug)]
struct DistinctPercentileContAccumulator<T: ArrowNumericType> {
    distinct_values: GenericDistinctBuffer<T>,
    percentile: f64,
}

impl<T: ArrowNumericType + Debug> DistinctPercentileContAccumulator<T> {
    fn new(percentile: f64) -> Self {
        Self {
            distinct_values: GenericDistinctBuffer::new(T::DATA_TYPE),
            percentile,
        }
    }
}

impl<T: ArrowNumericType + Debug> Accumulator for DistinctPercentileContAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.distinct_values.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.distinct_values.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.distinct_values.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut values: Vec<T::Native> =
            self.distinct_values.values.iter().map(|v| v.0).collect();
        let value = calculate_percentile::<T>(&mut values, self.percentile);
        ScalarValue::new_primitive::<T>(value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.distinct_values.size()
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = values[0].as_primitive::<T>();
        for value in arr.iter().flatten() {
            self.distinct_values.values.remove(&Hashable(value));
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
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
fn calculate_percentile<T: ArrowNumericType>(
    values: &mut [T::Native],
    percentile: f64,
) -> Option<T::Native> {
    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    let len = values.len();
    if len == 0 {
        None
    } else if len == 1 {
        Some(values[0])
    } else if percentile == 0.0 {
        // Get minimum value
        Some(
            *values
                .iter()
                .min_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        )
    } else if percentile == 1.0 {
        // Get maximum value
        Some(
            *values
                .iter()
                .max_by(|a, b| cmp(a, b))
                .expect("we checked for len > 0 a few lines above"),
        )
    } else {
        // Calculate the index using the formula: p * (n - 1)
        let index = percentile * ((len - 1) as f64);
        let lower_index = index.floor() as usize;
        let upper_index = index.ceil() as usize;

        if lower_index == upper_index {
            // Exact index, return the value at that position
            let (_, value, _) = values.select_nth_unstable_by(lower_index, cmp);
            Some(*value)
        } else {
            // Need to interpolate between two values
            // First, partition at lower_index to get the lower value
            let (_, lower_value, _) = values.select_nth_unstable_by(lower_index, cmp);
            let lower_value = *lower_value;

            // Then partition at upper_index to get the upper value
            let (_, upper_value, _) = values.select_nth_unstable_by(upper_index, cmp);
            let upper_value = *upper_value;

            // Linear interpolation using wrapping arithmetic
            // We use wrapping operations here (matching the approach in median.rs) because:
            // 1. Both values come from the input data, so diff is bounded by the value range
            // 2. fraction is between 0 and 1, and INTERPOLATION_PRECISION is small enough
            //    to prevent overflow when combined with typical numeric ranges
            // 3. The result is guaranteed to be between lower_value and upper_value
            // 4. For floating-point types, wrapping ops behave the same as standard ops
            let fraction = index - (lower_index as f64);
            let diff = upper_value.sub_wrapping(lower_value);
            let interpolated = lower_value.add_wrapping(
                diff.mul_wrapping(T::Native::usize_as(
                    (fraction * INTERPOLATION_PRECISION as f64) as usize,
                ))
                .div_wrapping(T::Native::usize_as(INTERPOLATION_PRECISION)),
            );
            Some(interpolated)
        }
    }
}
