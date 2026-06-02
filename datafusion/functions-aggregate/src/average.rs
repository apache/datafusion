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

//! Defines `Avg` & `Mean` aggregate & accumulators

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType, AsArray,
    BooleanArray, PrimitiveArray, PrimitiveBuilder, UInt64Array,
};

use arrow::compute::sum;
use arrow::datatypes::{
    ArrowNativeType, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE,
    DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE, DataType,
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, FieldRef, Float64Type, TimeUnit, UInt64Type, i256,
};
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, EmitTo, Expr,
    GroupsAccumulator, ReversedUDAF, Signature, TypeSignature, TypeSignatureClass,
    Volatility,
};
use datafusion_functions_aggregate_common::aggregate::avg_distinct::{
    DecimalDistinctAvgAccumulator, Float64DistinctAvgAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::NullState;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::{
    filtered_null_mask, set_nulls,
};
use datafusion_functions_aggregate_common::utils::DecimalAverager;
use datafusion_macros::user_doc;
use log::debug;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

make_udaf_expr_and_func!(
    Avg,
    avg,
    expression,
    "Returns the avg of a group of values.",
    avg_udaf
);

pub fn avg_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        avg_udaf(),
        vec![expr],
        true,
        None,
        vec![],
        None,
    ))
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the average of numeric values in the specified column.",
    syntax_example = "avg(expression)",
    sql_example = r#"```sql
> SELECT avg(column_name) FROM table_name;
+---------------------------+
| avg(column_name)           |
+---------------------------+
| 42.75                      |
+---------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Avg {
    signature: Signature,
    aliases: Vec<String>,
}

impl Avg {
    pub fn new() -> Self {
        Self {
            // Supported types smallint, int, bigint, real, double precision, decimal, or interval
            // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Duration,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Integer, TypeSignatureClass::Float],
                        NativeType::Float64,
                    )]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("mean")],
        }
    }
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

fn avg_sum_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Decimal32(_, scale) => {
            DataType::Decimal64(DECIMAL64_MAX_PRECISION, *scale)
        }
        DataType::Decimal64(_, scale) => {
            DataType::Decimal128(DECIMAL128_MAX_PRECISION, *scale)
        }
        data_type => data_type.clone(),
    }
}

impl AggregateUDFImpl for Avg {
    fn name(&self) -> &str {
        "avg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Decimal32(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL32_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL32_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal32(new_precision, new_scale))
            }
            DataType::Decimal64(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL64_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL64_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal64(new_precision, new_scale))
            }
            DataType::Decimal128(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL128_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal128(new_precision, new_scale))
            }
            DataType::Decimal256(precision, scale) => {
                // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
                // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
                let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 4);
                let new_scale = DECIMAL256_MAX_SCALE.min(*scale + 4);
                Ok(DataType::Decimal256(new_precision, new_scale))
            }
            DataType::Duration(time_unit) => Ok(DataType::Duration(*time_unit)),
            _ => Ok(DataType::Float64),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = acc_args.expr_fields[0].data_type();
        use DataType::*;

        // instantiate specialized accumulator based for the type
        if acc_args.is_distinct {
            match (data_type, acc_args.return_type()) {
                // Numeric types are converted to Float64 via `coerce_avg_type` during logical plan creation
                (Float64, _) => Ok(Box::new(Float64DistinctAvgAccumulator::default())),
                (
                    Decimal32(_, scale),
                    Decimal32(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal32Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal64(_, scale),
                    Decimal64(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal64Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal128(_, scale),
                    Decimal128(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal128Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal256(_, scale),
                    Decimal256(target_precision, target_scale),
                ) => Ok(Box::new(DecimalDistinctAvgAccumulator::<Decimal256Type>::with_decimal_params(
                    *scale,
                    *target_precision,
                    *target_scale,
                ))),
                (dt, return_type) => exec_err!(
                    "AVG(DISTINCT) for ({} --> {}) not supported",
                    dt,
                    return_type
                ),
            }
        } else {
            match (&data_type, acc_args.return_type()) {
                (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
                (
                    Decimal32(_sum_precision, sum_scale),
                    Decimal32(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<
                    Decimal32Type,
                    Decimal64Type,
                >::new(
                    *sum_scale,
                    DECIMAL64_MAX_PRECISION,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal64(_sum_precision, sum_scale),
                    Decimal64(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<
                    Decimal64Type,
                    Decimal128Type,
                >::new(
                    *sum_scale,
                    DECIMAL128_MAX_PRECISION,
                    *target_precision,
                    *target_scale,
                ))),
                (
                    Decimal128(sum_precision, sum_scale),
                    Decimal128(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type>::new(
                    *sum_scale,
                    *sum_precision,
                    *target_precision,
                    *target_scale,
                ))),

                (
                    Decimal256(sum_precision, sum_scale),
                    Decimal256(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type>::new(
                    *sum_scale,
                    *sum_precision,
                    *target_precision,
                    *target_scale,
                ))),

                (Duration(time_unit), Duration(result_unit)) => {
                    Ok(Box::new(DurationAvgAccumulator {
                        sum: None,
                        count: 0,
                        time_unit: *time_unit,
                        result_unit: *result_unit,
                    }))
                }

                (dt, return_type) => {
                    exec_err!("AvgAccumulator for ({} --> {})", dt, return_type)
                }
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            // Decimal accumulator actually uses a different precision during accumulation,
            // see DecimalDistinctAvgAccumulator::with_decimal_params
            let dt = match args.input_fields[0].data_type() {
                DataType::Decimal32(_, scale) => {
                    DataType::Decimal32(DECIMAL32_MAX_PRECISION, *scale)
                }
                DataType::Decimal64(_, scale) => {
                    DataType::Decimal64(DECIMAL64_MAX_PRECISION, *scale)
                }
                DataType::Decimal128(_, scale) => {
                    DataType::Decimal128(DECIMAL128_MAX_PRECISION, *scale)
                }
                DataType::Decimal256(_, scale) => {
                    DataType::Decimal256(DECIMAL256_MAX_PRECISION, *scale)
                }
                _ => args.return_type().clone(),
            };
            // Similar to datafusion_functions_aggregate::sum::Sum::state_fields
            // since the accumulator uses DistinctSumAccumulator internally.
            Ok(vec![
                Field::new_list(
                    format_state_name(args.name, "avg distinct"),
                    Field::new_list_field(dt, true),
                    false,
                )
                .into(),
            ])
        } else {
            let sum_data_type = avg_sum_data_type(args.input_fields[0].data_type());
            Ok(vec![
                Field::new(
                    format_state_name(args.name, "count"),
                    DataType::UInt64,
                    true,
                ),
                Field::new(format_state_name(args.name, "sum"), sum_data_type, true),
            ]
            .into_iter()
            .map(Arc::new)
            .collect())
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        matches!(
            args.return_field.data_type(),
            DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
        ) && !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;

        let data_type = args.expr_fields[0].data_type();

        // instantiate specialized accumulator based for the type
        match (data_type, args.return_field.data_type()) {
            (Float64, Float64) => {
                Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                    data_type.clone(),
                    args.return_field.data_type(),
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (
                Decimal32(_sum_precision, sum_scale),
                Decimal32(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal64Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i64, count: u64| {
                    let avg = decimal_averager.avg(sum, count as i64)?;
                    if let Ok(avg) = i32::try_from(avg) {
                        Ok(avg)
                    } else {
                        exec_err!("Arithmetic Overflow in AvgAccumulator")
                    }
                };

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal32Type,
                    _,
                    Decimal64Type,
                >::new(
                    avg_sum_data_type(data_type),
                    args.return_field.data_type(),
                    avg_fn,
                )))
            }
            (
                Decimal64(_sum_precision, sum_scale),
                Decimal64(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal128Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i128, count: u64| {
                    let avg = decimal_averager.avg(sum, count as i128)?;
                    if let Ok(avg) = i64::try_from(avg) {
                        Ok(avg)
                    } else {
                        exec_err!("Arithmetic Overflow in AvgAccumulator")
                    }
                };

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal64Type,
                    _,
                    Decimal128Type,
                >::new(
                    avg_sum_data_type(data_type),
                    args.return_field.data_type(),
                    avg_fn,
                )))
            }
            (
                Decimal128(_sum_precision, sum_scale),
                Decimal128(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal128Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn =
                    move |sum: i128, count: u64| decimal_averager.avg(sum, count as i128);

                Ok(Box::new(AvgGroupsAccumulator::<Decimal128Type, _>::new(
                    data_type.clone(),
                    args.return_field.data_type(),
                    avg_fn,
                )))
            }

            (
                Decimal256(_sum_precision, sum_scale),
                Decimal256(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal256Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn = move |sum: i256, count: u64| {
                    decimal_averager.avg(sum, i256::from_usize(count as usize).unwrap())
                };

                Ok(Box::new(AvgGroupsAccumulator::<Decimal256Type, _>::new(
                    data_type.clone(),
                    args.return_field.data_type(),
                    avg_fn,
                )))
            }

            (Duration(time_unit), Duration(_result_unit)) => {
                let avg_fn = move |sum: i64, count: u64| Ok(sum / count as i64);

                match time_unit {
                    TimeUnit::Second => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationSecondType,
                        _,
                    >::new(
                        data_type.clone(),
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Millisecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMillisecondType,
                        _,
                    >::new(
                        data_type.clone(),
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Microsecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMicrosecondType,
                        _,
                    >::new(
                        data_type.clone(),
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Nanosecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationNanosecondType,
                        _,
                    >::new(
                        data_type.clone(),
                        args.return_type(),
                        avg_fn,
                    ))),
                }
            }

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                &data_type,
                args.return_field.data_type()
            ),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: u64,
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // In sliding-window mode `retract_batch` can bring `count` back to 0
        // while `sum` remains `Some(..)` (possibly zero or a floating-point
        // residual). Guard against that so the frame with no non-NULL values
        // yields NULL rather than NaN / ±Inf.
        let avg = if self.count == 0 {
            None
        } else {
            self.sum.map(|f| f / self.count as f64)
        };
        Ok(ScalarValue::Float64(avg))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::Float64(self.sum),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
            self.sum = Some(self.sum.unwrap() - x);
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average for decimals.
///
/// `I` is the input (and output) decimal type. `S` is a possibly wider decimal
/// type used to accumulate the sum so the running total does not overflow
/// (e.g. `Decimal32` values are summed as `Decimal64`).
#[derive(Debug)]
struct DecimalAvgAccumulator<I, S = I>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
{
    sum: Option<S::Native>,
    count: u64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
    _phantom: PhantomData<I>,
}

impl<I, S> DecimalAvgAccumulator<I, S>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
{
    fn new(
        sum_scale: i8,
        sum_precision: u8,
        target_precision: u8,
        target_scale: i8,
    ) -> Self {
        Self {
            sum: None,
            count: 0,
            sum_scale,
            sum_precision,
            target_precision,
            target_scale,
            _phantom: PhantomData,
        }
    }
}

fn decimal_sum_as<I, S>(values: &PrimitiveArray<I>) -> Option<S::Native>
where
    I: DecimalType + ArrowNumericType,
    S: DecimalType + ArrowNumericType,
    I::Native: Into<S::Native>,
{
    let mut sum: Option<S::Native> = None;

    for value in values.iter().flatten() {
        let value = value.into();
        sum = Some(match sum {
            Some(sum) => sum.add_wrapping(value),
            None => value,
        });
    }

    sum
}

impl<I, S> Accumulator for DecimalAvgAccumulator<I, S>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
    S::Native: TryInto<I::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<I>();
        self.count += (values.len() - values.null_count()) as u64;

        if let Some(x) = decimal_sum_as::<I, S>(values) {
            let v = self.sum.get_or_insert_with(S::Native::default);
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // `count == 0` can occur in sliding-window mode after `retract_batch`
        // removes every contributing value. Return NULL rather than dividing
        // by zero (which would panic for integer decimal types).
        let v = if self.count == 0 {
            None
        } else {
            self.sum
                .map(|v| {
                    let avg = DecimalAverager::<S>::try_new(
                        self.sum_scale,
                        self.target_precision,
                        self.target_scale,
                    )?
                    .avg(v, S::Native::from_usize(self.count as usize).unwrap())?;
                    if let Ok(avg) = avg.try_into() {
                        Ok(avg)
                    } else {
                        exec_err!("Arithmetic Overflow in AvgAccumulator")
                    }
                })
                .transpose()?
        };

        ScalarValue::new_primitive::<I>(
            v,
            &I::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<S>(
                self.sum,
                &S::TYPE_CONSTRUCTOR(self.sum_precision, self.sum_scale),
            )?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<S>()) {
            let v = self.sum.get_or_insert_with(S::Native::default);
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<I>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = decimal_sum_as::<I, S>(values) {
            self.sum = Some(self.sum.unwrap().sub_wrapping(x));
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average for duration values
#[derive(Debug)]
struct DurationAvgAccumulator {
    sum: Option<i64>,
    count: u64,
    time_unit: TimeUnit,
    result_unit: TimeUnit,
}

impl Accumulator for DurationAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - array.null_count()) as u64;

        let sum_value = match self.time_unit {
            TimeUnit::Second => sum(array.as_primitive::<DurationSecondType>()),
            TimeUnit::Millisecond => sum(array.as_primitive::<DurationMillisecondType>()),
            TimeUnit::Microsecond => sum(array.as_primitive::<DurationMicrosecondType>()),
            TimeUnit::Nanosecond => sum(array.as_primitive::<DurationNanosecondType>()),
        };

        if let Some(x) = sum_value {
            let v = self.sum.get_or_insert(0);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Guard against `count == 0` which can happen in sliding-window mode
        // after every contributing value has been retracted. Without this
        // check we would integer-divide by zero.
        let avg = if self.count == 0 {
            None
        } else {
            self.sum.map(|sum| sum / self.count as i64)
        };

        match self.result_unit {
            TimeUnit::Second => Ok(ScalarValue::DurationSecond(avg)),
            TimeUnit::Millisecond => Ok(ScalarValue::DurationMillisecond(avg)),
            TimeUnit::Microsecond => Ok(ScalarValue::DurationMicrosecond(avg)),
            TimeUnit::Nanosecond => Ok(ScalarValue::DurationNanosecond(avg)),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let duration_value = match self.time_unit {
            TimeUnit::Second => ScalarValue::DurationSecond(self.sum),
            TimeUnit::Millisecond => ScalarValue::DurationMillisecond(self.sum),
            TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(self.sum),
            TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(self.sum),
        };

        Ok(vec![ScalarValue::from(self.count), duration_value])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        let sum_value = match self.time_unit {
            TimeUnit::Second => sum(states[1].as_primitive::<DurationSecondType>()),
            TimeUnit::Millisecond => {
                sum(states[1].as_primitive::<DurationMillisecondType>())
            }
            TimeUnit::Microsecond => {
                sum(states[1].as_primitive::<DurationMicrosecondType>())
            }
            TimeUnit::Nanosecond => {
                sum(states[1].as_primitive::<DurationNanosecondType>())
            }
        };

        if let Some(x) = sum_value {
            let v = self.sum.get_or_insert(0);
            *v += x;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count -= (array.len() - array.null_count()) as u64;

        let sum_value = match self.time_unit {
            TimeUnit::Second => sum(array.as_primitive::<DurationSecondType>()),
            TimeUnit::Millisecond => sum(array.as_primitive::<DurationMillisecondType>()),
            TimeUnit::Microsecond => sum(array.as_primitive::<DurationMicrosecondType>()),
            TimeUnit::Nanosecond => sum(array.as_primitive::<DurationNanosecondType>()),
        };

        if let Some(x) = sum_value {
            self.sum = Some(self.sum.unwrap() - x);
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
/// S::Native and a total count
///
/// `I` is the input (and output) type. `S` is a possibly wider type used to
/// accumulate the sum so it does not overflow.
#[derive(Debug)]
struct AvgGroupsAccumulator<I, F, S = I>
where
    I: ArrowNumericType + Send,
    S: ArrowNumericType + Send,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native> + Send + 'static,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// Sums per group, stored as the native type
    sums: Vec<S::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
    avg_fn: F,

    _phantom: PhantomData<I>,
}

impl<I, F, S> AvgGroupsAccumulator<I, F, S>
where
    I: ArrowNumericType + Send,
    S: ArrowNumericType + Send,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native> + Send + 'static,
{
    pub fn new(
        sum_data_type: impl Into<DataType>,
        return_data_type: &DataType,
        avg_fn: F,
    ) -> Self {
        let sum_data_type = sum_data_type.into();
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type}) --> {return_data_type}",
            std::any::type_name::<I>()
        );

        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type,
            counts: vec![],
            sums: vec![],
            null_state: NullState::new(),
            avg_fn,
            _phantom: PhantomData,
        }
    }
}

impl<I, F, S> GroupsAccumulator for AvgGroupsAccumulator<I, F, S>
where
    I: ArrowNumericType + Send,
    S: ArrowNumericType + Send,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native> + Send + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<I>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, S::default_value());
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                // SAFETY: group_index is guaranteed to be in bounds
                let sum = unsafe { self.sums.get_unchecked_mut(group_index) };
                *sum = sum.add_wrapping(new_value.into());

                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);

        if let Some(nulls) = &nulls {
            assert_eq!(nulls.len(), sums.len());
        }
        assert_eq!(counts.len(), sums.len());

        // don't evaluate averages with null inputs to avoid errors on null values

        let array: PrimitiveArray<I> = if let Some(nulls) = &nulls
            && nulls.null_count() > 0
        {
            let mut builder = PrimitiveBuilder::<I>::with_capacity(nulls.len())
                .with_data_type(self.return_data_type.clone());
            let iter = sums.into_iter().zip(counts).zip(nulls.iter());

            for ((sum, count), is_valid) in iter {
                if is_valid {
                    builder.append_value((self.avg_fn)(sum, count)?)
                } else {
                    builder.append_null();
                }
            }
            builder.finish()
        } else {
            let averages: Vec<I::Native> = sums
                .into_iter()
                .zip(counts)
                .map(|(sum, count)| (self.avg_fn)(sum, count))
                .collect::<Result<Vec<_>>>()?;
            PrimitiveArray::new(averages.into(), nulls) // no copy
                .with_data_type(self.return_data_type.clone())
        };

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to);

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = UInt64Array::new(counts.into(), nulls.clone()); // zero copy

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<S>::new(sums.into(), nulls) // zero copy
            .with_data_type(self.sum_data_type.clone());

        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<S>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                // SAFETY: group_index is guaranteed to be in bounds
                let count = unsafe { self.counts.get_unchecked_mut(group_index) };
                *count += partial_count;
            },
        );

        // update sums
        self.sums.resize(total_num_groups, S::default_value());
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <S as ArrowPrimitiveType>::Native| {
                // SAFETY: group_index is guaranteed to be in bounds
                let sum = unsafe { self.sums.get_unchecked_mut(group_index) };
                *sum = sum.add_wrapping(new_value);
            },
        );

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = values[0].as_primitive::<I>();
        let mut sums = PrimitiveBuilder::<S>::with_capacity(values.len())
            .with_data_type(self.sum_data_type.clone());
        for value in values.iter() {
            if let Some(value) = value {
                sums.append_value(value.into());
            } else {
                sums.append_null();
            }
        }
        let sums = sums.finish();
        let counts = UInt64Array::from_value(1, sums.len());

        let nulls = filtered_null_mask(opt_filter, &sums);

        // set nulls on the arrays
        let counts = set_nulls(counts, nulls.clone());
        let sums = set_nulls(sums, nulls);

        Ok(vec![Arc::new(counts) as ArrayRef, Arc::new(sums)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<u64>()
            + self.sums.capacity() * size_of::<S::Native>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
        DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
        DurationSecondArray, Float64Array,
    };
    use arrow::datatypes::Schema;

    struct AvgCase {
        name: &'static str,
        values: ArrayRef,
        return_type: DataType,
        sum_type: DataType,
        expected: ScalarValue,
    }

    fn avg_groups_accumulator(
        input_type: &DataType,
        return_type: &DataType,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let schema = Schema::empty();
        let expr_field = Arc::new(Field::new("a", input_type.clone(), true));
        let return_field = Arc::new(Field::new("avg", return_type.clone(), true));

        Avg::new().create_groups_accumulator(AccumulatorArgs {
            return_field,
            schema: &schema,
            expr_fields: &[expr_field],
            ignore_nulls: false,
            order_bys: &[],
            is_distinct: false,
            name: "avg",
            is_reversed: false,
            exprs: &[],
        })
    }

    fn avg_accumulator(
        input_type: &DataType,
        return_type: &DataType,
    ) -> Result<Box<dyn Accumulator>> {
        let schema = Schema::empty();
        let expr_field = Arc::new(Field::new("a", input_type.clone(), true));
        let return_field = Arc::new(Field::new("avg", return_type.clone(), true));

        Avg::new().accumulator(AccumulatorArgs {
            return_field,
            schema: &schema,
            expr_fields: &[expr_field],
            ignore_nulls: false,
            order_bys: &[],
            is_distinct: false,
            name: "avg",
            is_reversed: false,
            exprs: &[],
        })
    }

    fn avg_state_fields(
        input_type: &DataType,
        return_type: &DataType,
    ) -> Result<Vec<FieldRef>> {
        let input_field = Arc::new(Field::new("a", input_type.clone(), true));
        let return_field = Arc::new(Field::new("avg", return_type.clone(), true));

        Avg::new().state_fields(StateFieldsArgs {
            name: "avg",
            input_fields: &[input_field],
            return_field,
            ordering_fields: &[],
            is_distinct: false,
        })
    }

    fn avg_cases() -> Result<Vec<AvgCase>> {
        const ROWS: usize = 21_476;
        const DECIMAL32_VALUE: i32 = 99_999;
        const DECIMAL64_ROWS: usize = 92_235;
        const DECIMAL64_VALUE: i64 = 99_999_999_999;

        Ok(vec![
            AvgCase {
                name: "float64",
                values: Arc::new(Float64Array::from(vec![10.0, 20.0])),
                return_type: DataType::Float64,
                sum_type: DataType::Float64,
                expected: ScalarValue::Float64(Some(15.0)),
            },
            AvgCase {
                name: "decimal32",
                values: Arc::new(
                    Decimal32Array::from(vec![Some(DECIMAL32_VALUE); ROWS])
                        .with_precision_and_scale(5, 0)?,
                ),
                return_type: DataType::Decimal32(9, 4),
                sum_type: DataType::Decimal64(18, 0),
                expected: ScalarValue::Decimal32(Some(DECIMAL32_VALUE * 10_000), 9, 4),
            },
            AvgCase {
                name: "decimal64",
                values: Arc::new(
                    Decimal64Array::from(vec![Some(DECIMAL64_VALUE); DECIMAL64_ROWS])
                        .with_precision_and_scale(11, 0)?,
                ),
                return_type: DataType::Decimal64(15, 4),
                sum_type: DataType::Decimal128(38, 0),
                expected: ScalarValue::Decimal64(Some(DECIMAL64_VALUE * 10_000), 15, 4),
            },
            AvgCase {
                name: "decimal128",
                values: Arc::new(
                    Decimal128Array::from(vec![10_i128, 20_i128])
                        .with_precision_and_scale(20, 0)?,
                ),
                return_type: DataType::Decimal128(24, 4),
                sum_type: DataType::Decimal128(20, 0),
                expected: ScalarValue::Decimal128(Some(150_000), 24, 4),
            },
            AvgCase {
                name: "decimal256",
                values: Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(10), i256::from_i128(20)])
                        .with_precision_and_scale(50, 0)?,
                ),
                return_type: DataType::Decimal256(54, 4),
                sum_type: DataType::Decimal256(50, 0),
                expected: ScalarValue::Decimal256(Some(i256::from_i128(150_000)), 54, 4),
            },
            AvgCase {
                name: "duration_second",
                values: Arc::new(DurationSecondArray::from(vec![10, 20])),
                return_type: DataType::Duration(TimeUnit::Second),
                sum_type: DataType::Duration(TimeUnit::Second),
                expected: ScalarValue::DurationSecond(Some(15)),
            },
            AvgCase {
                name: "duration_millisecond",
                values: Arc::new(DurationMillisecondArray::from(vec![10, 20])),
                return_type: DataType::Duration(TimeUnit::Millisecond),
                sum_type: DataType::Duration(TimeUnit::Millisecond),
                expected: ScalarValue::DurationMillisecond(Some(15)),
            },
            AvgCase {
                name: "duration_microsecond",
                values: Arc::new(DurationMicrosecondArray::from(vec![10, 20])),
                return_type: DataType::Duration(TimeUnit::Microsecond),
                sum_type: DataType::Duration(TimeUnit::Microsecond),
                expected: ScalarValue::DurationMicrosecond(Some(15)),
            },
            AvgCase {
                name: "duration_nanosecond",
                values: Arc::new(DurationNanosecondArray::from(vec![10, 20])),
                return_type: DataType::Duration(TimeUnit::Nanosecond),
                sum_type: DataType::Duration(TimeUnit::Nanosecond),
                expected: ScalarValue::DurationNanosecond(Some(15)),
            },
        ])
    }

    #[test]
    fn avg_accumulator_state_types_match_state_fields() -> Result<()> {
        for case in avg_cases()? {
            let input_type = case.values.data_type();
            let state_fields = avg_state_fields(input_type, &case.return_type)?;
            let mut acc = avg_accumulator(input_type, &case.return_type)?;
            acc.update_batch(std::slice::from_ref(&case.values))?;
            let state = acc.state()?;

            assert_eq!(
                &state[0].data_type(),
                state_fields[0].data_type(),
                "{}",
                case.name
            );
            assert_eq!(
                &state[1].data_type(),
                state_fields[1].data_type(),
                "{}",
                case.name
            );
        }

        Ok(())
    }

    #[test]
    fn avg_accumulator_evaluate() -> Result<()> {
        for case in avg_cases()? {
            let input_type = case.values.data_type();
            let mut acc = avg_accumulator(input_type, &case.return_type)?;
            acc.update_batch(std::slice::from_ref(&case.values))?;

            assert_eq!(acc.evaluate()?, case.expected, "{}", case.name);
        }

        Ok(())
    }

    #[test]
    fn avg_groups_state_types_match_state_fields() -> Result<()> {
        for case in avg_cases()? {
            let input_type = case.values.data_type();
            let state_fields = avg_state_fields(input_type, &case.return_type)?;
            let acc = avg_groups_accumulator(input_type, &case.return_type)?;
            let state = acc.convert_to_state(std::slice::from_ref(&case.values), None)?;

            assert_eq!(
                state_fields[0].data_type(),
                &DataType::UInt64,
                "{}",
                case.name
            );
            assert_eq!(state_fields[1].data_type(), &case.sum_type, "{}", case.name);
            assert_eq!(state[0].data_type(), &DataType::UInt64, "{}", case.name);
            assert_eq!(state[1].data_type(), &case.sum_type, "{}", case.name);
        }

        Ok(())
    }

    #[test]
    fn avg_groups_convert_to_state_roundtrip() -> Result<()> {
        for case in avg_cases()? {
            let input_type = case.values.data_type();
            let partial = avg_groups_accumulator(input_type, &case.return_type)?;
            let mut final_acc = avg_groups_accumulator(input_type, &case.return_type)?;
            let state =
                partial.convert_to_state(std::slice::from_ref(&case.values), None)?;
            final_acc.merge_batch(&state, &vec![0; case.values.len()], None, 1)?;

            let result = final_acc.evaluate(EmitTo::All)?;
            assert_eq!(result.data_type(), &case.return_type, "{}", case.name);
            assert_eq!(
                ScalarValue::try_from_array(result.as_ref(), 0)?,
                case.expected,
                "{}",
                case.name
            );
        }

        Ok(())
    }
}
