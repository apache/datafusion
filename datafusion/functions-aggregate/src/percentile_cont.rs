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

use std::fmt::{Debug, Formatter};
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    ArrowNumericType, ArrowPrimitiveType, BooleanArray, ListArray, PrimitiveArray,
    PrimitiveBuilder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::{
    array::{Array, ArrayRef, AsArray},
    datatypes::{
        ArrowNativeType, DataType, Decimal128Type, Decimal256Type, Decimal32Type,
        Decimal64Type, Field, FieldRef, Float16Type, Float32Type, Float64Type,
    },
};

use arrow::array::ArrowNativeTypeOp;
use datafusion_doc::Documentation;

use crate::min_max::{max_udaf, min_udaf};
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    assert_eq_or_internal_err, internal_datafusion_err, plan_err, DataFusionError,
    Result, ScalarValue,
};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    expr::{AggregateFunction, Cast, Sort},
    function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
    simplify::SimplifyInfo,
};

use datafusion_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, Expr, GroupsAccumulator, Signature, Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filtered_null_mask;
use datafusion_functions_aggregate_common::utils::GenericDistinctBuffer;
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
    description = "Returns the exact percentile(s) of input values, interpolating between values if needed. \
Supports both scalar percentiles and arrays of percentiles.",
    syntax_example = r#"percentile_cont(percentile) WITHIN GROUP (ORDER BY expression)
percentile_cont(expression, percentile)
percentile_cont(expression, ARRAY[percentile1, percentile2, ...])"#,
    sql_example = r#"```sql
-- Standard WITHIN GROUP syntax
> SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+----------------------------------------------------------+
| percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) |
+----------------------------------------------------------+
| 45.5                                                     |
+----------------------------------------------------------+

-- Alternate function-call syntax
> SELECT percentile_cont(column_name, 0.75) FROM table_name;
+---------------------------------------+
| percentile_cont(column_name, 0.75)    |
+---------------------------------------+
| 45.5                                  |
+---------------------------------------+

-- Multiple percentiles using an array
> SELECT percentile_cont(column_name, ARRAY[0.25, 0.5, 0.75]) FROM table_name;
+--------------------------------------------------------------+
| percentile_cont(column_name, ARRAY[0.25, 0.5, 0.75])         |
+--------------------------------------------------------------+
| [12.0, 30.0, 45.5]                                           |
+--------------------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "The"),
    argument(
        name = "percentile",
        description = "Percentile or list of percentiles to compute. \
Can be a single float between 0 and 1 (inclusive), or an array of float values in that range. \
When an array is provided, an array of results is returned in the same order."
    )
)]
/// PERCENTILE_CONT aggregate expression. This uses an exact calculation and stores all values
/// in memory before computing the result. If an approximation is sufficient, then
/// APPROX_PERCENTILE_CONT provides a much more efficient solution.
///
/// If using the DISTINCT variation, the memory usage will be similarly high when the
/// cardinality is high, as it must store all distinct values before computing the result.
/// For low-cardinality inputs, memory usage is correspondingly lower.
///
/// When percentile arguments are provided as an array, all percentiles are computed
/// from a single in-memory sort of the input values, making multi-percentile computation
/// significantly more efficient than issuing separate percentile_cont calls.

#[derive(PartialEq, Eq, Hash)]
pub struct PercentileCont {
    signature: Signature,
    aliases: Vec<String>,
}

impl Debug for PercentileCont {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("PercentileCont")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for PercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileCont {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable)
                .with_parameter_names(vec!["expr".to_string(), "percentile".to_string()])
                .expect("valid parameter names for percentile_cont"),
            aliases: vec![String::from("quantile_cont")],
        }
    }

    fn create_accumulator(&self, args: &AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let percentiles = validate_percentile_expr(&args.exprs[1], "PERCENTILE_CONT")?;

        let is_descending = args
            .order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        let percentiles = if is_descending {
            percentiles.iter().map(|&p| 1.0 - p).collect()
        } else {
            percentiles
        };

        let returns_list = matches!(
            args.exprs[1].data_type(args.schema)?,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        );

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                if args.is_distinct {
                    Ok(Box::new(DistinctPercentileContAccumulator::<$t> {
                        data_type: $dt.clone(),
                        distinct_values: GenericDistinctBuffer::new($dt),
                        percentiles,
                        returns_list,
                    }))
                } else {
                    Ok(Box::new(PercentileContAccumulator::<$t> {
                        data_type: $dt.clone(),
                        all_values: vec![],
                        percentiles,
                        returns_list,
                    }))
                }
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        match input_dt {
            // For integer types, use Float64 internally since percentile_cont returns Float64
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => helper!(Float64Type, DataType::Float64),
            DataType::Float16 => helper!(Float16Type, input_dt),
            DataType::Float32 => helper!(Float32Type, input_dt),
            DataType::Float64 => helper!(Float64Type, input_dt),
            DataType::Decimal32(_, _) => helper!(Decimal32Type, input_dt),
            DataType::Decimal64(_, _) => helper!(Decimal64Type, input_dt),
            DataType::Decimal128(_, _) => helper!(Decimal128Type, input_dt),
            DataType::Decimal256(_, _) => helper!(Decimal256Type, input_dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "PercentileContAccumulator not supported for {} with {}",
                args.name, input_dt,
            ))),
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

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // We expect exactly two arguments:
        //   0: value expression (any numeric)
        //   1: percentile (Float64 or List(Float64))
        let [value_ty, perc_ty] = take_function_args(self.name(), arg_types)?;
        // Coerce the value being aggregated to Float64
        let coerced_value_ty = match value_ty {
        // signed ints
         DataType::Int8 |  DataType::Int16 | DataType::Int32 | DataType::Int64 |
        // unsigned ints
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        // floats
        DataType::Float64 => DataType::Float64,
        DataType::Float32 => DataType::Float32,

        other => {
            return plan_err!(
                "PERCENTILE_CONT does not support value expression of type {other}."
            );
        }
    };

        // Percentile argument: Float64 or List(Float64)
        let coerced_perc_ty = match perc_ty {
            DataType::Float64 => DataType::Float64,

            DataType::List(field) if matches!(field.data_type(), DataType::Float64) => {
                // keep the list + its nullability as-is
                DataType::List(Arc::clone(field))
            }

            other => {
                return plan_err!(
                "PERCENTILE_CONT percentile argument must be Float64 or List(Float64), \
                 got {other}."
            );
            }
        };

        Ok(vec![coerced_value_ty, coerced_perc_ty])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let value_type = &arg_types[0];

        if !value_type.is_numeric() {
            return plan_err!("percentile_cont requires numeric input types");
        }

        // PERCENTILE_CONT performs linear interpolation and should return a float type
        // For integer inputs, return Float64 (matching PostgreSQL/DuckDB behavior)
        // For float inputs, preserve the float type
        // For float list inputs, return a list of the appropriate float type

        let elem_type = match value_type {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                value_type.clone()
            }
            DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _) => value_type.clone(),
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => DataType::Float64,
            dt => {
                return plan_err!(
                    "percentile_cont does not support input type {}, must be numeric",
                    dt
                )
            }
        };

        let percentile_type = &arg_types.get(1).unwrap_or(&elem_type);

        match percentile_type {
            DataType::List(field) => {
                if !field.data_type().is_numeric() {
                    return plan_err!(
                        "percentile_cont percentile list must be numeric, got {}",
                        field.data_type()
                    );
                }

                Ok(DataType::List(Arc::new(Field::new(
                    "item", elem_type, true,
                ))))
            }
            _ => Ok(elem_type),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        //Intermediate state is a list of the elements we have collected so far
        let input_type = args.input_fields[0].data_type().clone();
        // For integer types, we store as Float64 internally
        let storage_type = match &input_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DataType::Float64,
            _ => input_type,
        };

        let field = Field::new_list_field(storage_type, true);
        let state_name = if args.is_distinct {
            "distinct_percentile_cont"
        } else {
            "percentile_cont"
        };

        Ok(vec![Field::new(
            format_state_name(args.name, state_name),
            DataType::List(Arc::new(field)),
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.create_accumulator(&acc_args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let num_args = args.exprs.len();
        assert_eq_or_internal_err!(
            num_args,
            2,
            "percentile_cont should have 2 args, but found num args:{}",
            num_args
        );

        let percentiles = validate_percentile_expr(&args.exprs[1], "PERCENTILE_CONT")?;

        let is_descending = args
            .order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        let percentiles = if is_descending {
            percentiles.iter().map(|&p| 1.0 - p).collect()
        } else {
            percentiles
        };

        let returns_list = matches!(
            args.exprs[1].data_type(args.schema)?,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        );

        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PercentileContGroupsAccumulator::<$t>::new(
                    $dt,
                    percentiles,
                    returns_list,
                )))
            };
        }

        let input_dt = args.exprs[0].data_type(args.schema)?;
        match input_dt {
            // For integer types, use Float64 internally since percentile_cont returns Float64
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => helper!(Float64Type, DataType::Float64),
            DataType::Float16 => helper!(Float16Type, input_dt),
            DataType::Float32 => helper!(Float32Type, input_dt),
            DataType::Float64 => helper!(Float64Type, input_dt),
            DataType::Decimal32(_, _) => helper!(Decimal32Type, input_dt),
            DataType::Decimal64(_, _) => helper!(Decimal64Type, input_dt),
            DataType::Decimal128(_, _) => helper!(Decimal128Type, input_dt),
            DataType::Decimal256(_, _) => helper!(Decimal256Type, input_dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "PercentileContGroupsAccumulator not supported for {} with {}",
                args.name, input_dt,
            ))),
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

    fn schema_name(
        &self,
        params: &datafusion_expr::expr::AggregateFunctionParams,
    ) -> Result<String> {
        datafusion_expr::udaf_default_schema_name(self, params)
    }

    fn human_display(
        &self,
        params: &datafusion_expr::expr::AggregateFunctionParams,
    ) -> Result<String> {
        datafusion_expr::udaf_default_human_display(self, params)
    }

    fn window_function_schema_name(
        &self,
        params: &datafusion_expr::expr::WindowFunctionParams,
    ) -> Result<String> {
        datafusion_expr::udaf_default_window_function_schema_name(self, params)
    }

    fn display_name(
        &self,
        params: &datafusion_expr::expr::AggregateFunctionParams,
    ) -> Result<String> {
        datafusion_expr::udaf_default_display_name(self, params)
    }

    fn window_function_display_name(
        &self,
        params: &datafusion_expr::expr::WindowFunctionParams,
    ) -> Result<String> {
        datafusion_expr::udaf_default_window_function_display_name(self, params)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        datafusion_expr::udaf_default_return_field(self, arg_fields)
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        self.accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        if self.order_sensitivity().is_beneficial() {
            return datafusion_common::exec_err!(
                "Should implement with satisfied for aggregator :{:?}",
                self.name()
            );
        }
        Ok(None)
    }

    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        // We have hard ordering requirements by default, meaning that order
        // sensitive UDFs need their input orderings to satisfy their ordering
        // requirements to generate correct results.
        datafusion_expr::utils::AggregateOrderSensitivity::HardRequirement
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::NotSupported
    }

    fn is_descending(&self) -> Option<bool> {
        None
    }

    fn value_from_stats(
        &self,
        _statistics_args: &datafusion_expr::StatisticsArgs,
    ) -> Option<ScalarValue> {
        None
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        false
    }

    fn set_monotonicity(
        &self,
        _data_type: &DataType,
    ) -> datafusion_expr::SetMonotonicity {
        datafusion_expr::SetMonotonicity::NotMonotonic
    }
}

#[derive(Clone, Copy)]
enum PercentileRewriteTarget {
    Min,
    Max,
}

#[expect(clippy::needless_pass_by_value)]
fn simplify_percentile_cont_aggregate(
    aggregate_function: AggregateFunction,
    info: &dyn SimplifyInfo,
) -> Result<Expr> {
    let original_expr = Expr::AggregateFunction(aggregate_function.clone());
    let params = &aggregate_function.params;

    let [value, percentile] = take_function_args("percentile_cont", &params.args)?;

    let is_descending = params
        .order_by
        .first()
        .map(|sort| !sort.asc)
        .unwrap_or(false);

    let rewrite_target = match extract_percentile_literal(percentile) {
        Some(0.0) => {
            if is_descending {
                PercentileRewriteTarget::Max
            } else {
                PercentileRewriteTarget::Min
            }
        }
        Some(1.0) => {
            if is_descending {
                PercentileRewriteTarget::Min
            } else {
                PercentileRewriteTarget::Max
            }
        }
        _ => return Ok(original_expr),
    };

    let input_type = match info.get_data_type(value) {
        Ok(data_type) => data_type,
        Err(_) => return Ok(original_expr),
    };

    let expected_return_type =
        match percentile_cont_udaf().return_type(std::slice::from_ref(&input_type)) {
            Ok(data_type) => data_type,
            Err(_) => return Ok(original_expr),
        };

    let mut agg_arg = value.clone();
    if expected_return_type != input_type {
        // min/max return the same type as their input. percentile_cont widens
        // integers to Float64 (and preserves float/decimal types), so ensure the
        // rewritten aggregate sees an input of the final return type.
        agg_arg = Expr::Cast(Cast::new(Box::new(agg_arg), expected_return_type.clone()));
    }

    let udaf = match rewrite_target {
        PercentileRewriteTarget::Min => min_udaf(),
        PercentileRewriteTarget::Max => max_udaf(),
    };

    let rewritten = Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        vec![agg_arg],
        params.distinct,
        params.filter.clone(),
        vec![],
        params.null_treatment,
    ));
    Ok(rewritten)
}

fn extract_percentile_literal(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), _) => Some(*value),
        _ => None,
    }
}

/// The percentile_cont accumulator accumulates the raw input values
/// as native types.
///
/// The intermediate state is represented as a List of scalar values updated by
/// `merge_batch` and a `Vec` of native values that are converted to scalar values
/// in the final evaluation step so that we avoid expensive conversions and
/// allocations during `update_batch`.
struct PercentileContAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
    percentiles: Vec<f64>,
    returns_list: bool,
}

impl<T: ArrowNumericType> Debug for PercentileContAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PercentileContAccumulator({}, percentile={:?}, returns_list={})",
            self.data_type, self.percentiles, self.returns_list
        )
    }
}

impl<T: ArrowNumericType> Accumulator for PercentileContAccumulator<T> {
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
        // Cast to target type if needed (e.g., integer to Float64)
        let values = if values[0].data_type() != &self.data_type {
            arrow::compute::cast(&values[0], &self.data_type)?
        } else {
            Arc::clone(&values[0])
        };

        let values = values.as_primitive::<T>();
        self.all_values.reserve(values.len() - values.null_count());
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let d = std::mem::take(&mut self.all_values);
        let values = calculate_percentiles::<T>(d, &self.percentiles);

        if self.returns_list {
            let primitive_array = PrimitiveArray::<T>::from_iter(values)
                .with_data_type(self.data_type.clone());
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
                0,
                primitive_array.len() as i32,
            ]));
            let list_array = ListArray::new(
                Arc::new(Field::new_list_field(self.data_type.clone(), true)),
                offsets,
                Arc::new(primitive_array),
                None,
            );
            return Ok(ScalarValue::List(Arc::new(list_array)));
        }
        ScalarValue::new_primitive::<T>(values[0], &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.all_values.capacity() * size_of::<T::Native>()
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
    data_type: DataType,
    group_values: Vec<Vec<T::Native>>,
    percentiles: Vec<f64>,
    returns_list: bool,
}

impl<T: ArrowNumericType + Send> PercentileContGroupsAccumulator<T> {
    pub fn new(data_type: DataType, percentiles: Vec<f64>, returns_list: bool) -> Self {
        Self {
            data_type,
            group_values: Vec::new(),
            percentiles,
            returns_list,
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

        // Cast to target type if needed (e.g., integer to Float64)
        let values_array = if values[0].data_type() != &self.data_type {
            arrow::compute::cast(&values[0], &self.data_type)?
        } else {
            Arc::clone(&values[0])
        };

        let values = values_array.as_primitive::<T>();

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
        let emit_group_values = emit_to.take_needed(&mut self.group_values);

        // Calculate percentile for each group
        if self.returns_list {
            let mut result_builder = arrow::array::ListBuilder::new(
                PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone()),
            );
            for values in emit_group_values {
                let value = calculate_percentiles::<T>(values, &self.percentiles);
                for v in value {
                    result_builder.values().append_option(v);
                }
                result_builder.append(true);
            }
            Ok(Arc::new(result_builder.finish()))
        } else {
            let mut evaluate_result_builder =
                PrimitiveBuilder::<T>::new().with_data_type(self.data_type.clone());
            for values in emit_group_values {
                let value = calculate_percentiles::<T>(values, &self.percentiles);
                evaluate_result_builder.append_option(value[0]);
            }
            Ok(Arc::new(evaluate_result_builder.finish()))
        }
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");

        // Cast to target type if needed (e.g., integer to Float64)
        let values_array = if values[0].data_type() != &self.data_type {
            arrow::compute::cast(&values[0], &self.data_type)?
        } else {
            Arc::clone(&values[0])
        };

        let input_array = values_array.as_primitive::<T>();

        // Directly convert the input array to states, each row will be
        // seen as a respective group.
        // For detail, the `input_array` will be converted to a `ListArray`.
        // And if row is `not null + not filtered`, it will be converted to a list
        // with only one element; otherwise, this row in `ListArray` will be set
        // to null.

        // Reuse values buffer in `input_array` to build `values` in `ListArray`
        let values = PrimitiveArray::<T>::new(input_array.values().clone(), None)
            .with_data_type(self.data_type.clone());

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
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
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
    data_type: DataType,
    percentiles: Vec<f64>,
    returns_list: bool,
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
        let d: Vec<<T as ArrowPrimitiveType>::Native> =
            std::mem::take(&mut self.distinct_values.values)
                .into_iter()
                .map(|v| v.0)
                .collect();

        let values: Vec<Option<<T as ArrowPrimitiveType>::Native>> =
            calculate_percentiles::<T>(d, &self.percentiles);

        if self.returns_list {
            let primitive_array = PrimitiveArray::<T>::from_iter(values)
                .with_data_type(self.data_type.clone());
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![
                0,
                primitive_array.len() as i32,
            ]));
            let list_array = ListArray::new(
                Arc::new(Field::new_list_field(self.data_type.clone(), true)),
                offsets,
                Arc::new(primitive_array),
                None,
            );
            return Ok(ScalarValue::List(Arc::new(list_array)));
        }
        ScalarValue::new_primitive::<T>(values[0], &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.distinct_values.size()
    }
}

/// Calculate the percentile value for a given set of values.
/// This function performs an exact calculation by sorting all values.
///
/// The percentile is calculated using linear interpolation between closest ranks.
/// For percentile p and n values:
/// - If p * (n-1) is an integer, return the value at that position
/// - Otherwise, interpolate between the two closest values
fn calculate_percentiles<T: ArrowNumericType>(
    mut values: Vec<T::Native>,
    percentiles: &[f64],
) -> Vec<Option<T::Native>> {
    let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

    let len = values.len();

    // Fast path for empty and single-element arrays
    if len == 0 {
        return vec![None; percentiles.len()];
    } else if len == 1 {
        return vec![Some(values[0]); percentiles.len()];
    }

    // handle special cases for single percentile requests
    if percentiles.len() == 1 {
        if percentiles[0] == 0.0 {
            // Get minimum value
            return vec![
                Some(
                    *values
                        .iter()
                        .min_by(|a, b| cmp(a, b))
                        .expect("we checked for len > 0 a few lines above"),
                );
                percentiles.len()
            ];
        } else if percentiles[0] == 1.0 {
            // Get maximum value
            return vec![
                Some(
                    *values
                        .iter()
                        .max_by(|a, b| cmp(a, b))
                        .expect("we checked for len > 0 a few lines above"),
                );
                percentiles.len()
            ];
        }
    }

    // After special-case handling, a single value is treated as a 1-element array for uniform processing.
    let sorted_values = {
        values.sort_by(cmp);
        values
    };

    percentiles
        .iter()
        .map(|percentile| extract_percentile::<T>(&sorted_values, len, percentile))
        .collect::<Vec<Option<<T as ArrowPrimitiveType>::Native>>>()
}

fn extract_percentile<T: ArrowNumericType>(
    values: &[<T as ArrowPrimitiveType>::Native],
    len: usize,
    percentile: &f64,
) -> Option<<T as ArrowPrimitiveType>::Native> {
    // Calculate the index using the formula: p * (n - 1)
    let index = percentile * ((len - 1) as f64);
    let lower_index = index.floor() as usize;
    let upper_index = index.ceil() as usize;

    if lower_index == upper_index {
        // Exact index, return the value at that position
        values.get(lower_index).copied()
    } else {
        // Need to interpolate between two values
        // First, partition at lower_index to get the lower value
        let lower_value = values.get(lower_index)?;

        // Then partition at upper_index to get the upper value
        let upper_value = values.get(upper_index)?;

        // Linear interpolation using wrapping arithmetic
        // We use wrapping operations here (matching the approach in median.rs) because:
        // 1. Both values come from the input data, so diff is bounded by the value range
        // 2. fraction is between 0 and 1, and INTERPOLATION_PRECISION is small enough
        //    to prevent overflow when combined with typical numeric ranges
        // 3. The result is guaranteed to be between lower_value and upper_value
        // 4. For floating-point types, wrapping ops behave the same as standard ops
        let fraction = index - (lower_index as f64);
        let diff = upper_value.sub_wrapping(*lower_value);
        let interpolated = lower_value.add_wrapping(
            diff.mul_wrapping(T::Native::usize_as(
                (fraction * INTERPOLATION_PRECISION as f64) as usize,
            ))
            .div_wrapping(T::Native::usize_as(INTERPOLATION_PRECISION)),
        );
        Some(interpolated)
    }
}
