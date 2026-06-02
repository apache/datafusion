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
    BooleanArray, Float64Array, NullBufferBuilder, PrimitiveArray, PrimitiveBuilder,
    UInt64Array,
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
use datafusion_execution::memory_pool::proxy::VecAllocExt;
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
                    Decimal32(sum_precision, sum_scale),
                    Decimal32(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal32Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),
                (
                    Decimal64(sum_precision, sum_scale),
                    Decimal64(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal64Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),
                (
                    Decimal128(sum_precision, sum_scale),
                    Decimal128(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal128Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),

                (
                    Decimal256(sum_precision, sum_scale),
                    Decimal256(target_precision, target_scale),
                ) => Ok(Box::new(DecimalAvgAccumulator::<Decimal256Type> {
                    sum: None,
                    count: 0,
                    sum_scale: *sum_scale,
                    sum_precision: *sum_precision,
                    target_precision: *target_precision,
                    target_scale: *target_scale,
                })),

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
            Ok(vec![
                Field::new(
                    format_state_name(args.name, "count"),
                    DataType::UInt64,
                    true,
                ),
                Field::new(
                    format_state_name(args.name, "sum"),
                    args.input_fields[0].data_type().clone(),
                    true,
                ),
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
                    data_type,
                    args.return_field.data_type(),
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (
                Decimal32(_sum_precision, sum_scale),
                Decimal32(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal32Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn =
                    move |sum: i32, count: u64| decimal_averager.avg(sum, count as i32);

                Ok(Box::new(AvgGroupsAccumulator::<Decimal32Type, _>::new(
                    data_type,
                    args.return_field.data_type(),
                    avg_fn,
                )))
            }
            (
                Decimal64(_sum_precision, sum_scale),
                Decimal64(target_precision, target_scale),
            ) => {
                let decimal_averager = DecimalAverager::<Decimal64Type>::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn =
                    move |sum: i64, count: u64| decimal_averager.avg(sum, count as i64);

                Ok(Box::new(AvgGroupsAccumulator::<Decimal64Type, _>::new(
                    data_type,
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
                    data_type,
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
                    data_type,
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
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Millisecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMillisecondType,
                        _,
                    >::new(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Microsecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationMicrosecondType,
                        _,
                    >::new(
                        data_type,
                        args.return_type(),
                        avg_fn,
                    ))),
                    TimeUnit::Nanosecond => Ok(Box::new(AvgGroupsAccumulator::<
                        DurationNanosecondType,
                        _,
                    >::new(
                        data_type,
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

/// An accumulator to compute the average for decimals
#[derive(Debug)]
struct DecimalAvgAccumulator<T: DecimalType + ArrowNumericType + Debug> {
    sum: Option<T::Native>,
    count: u64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + ArrowNumericType + Debug> Accumulator for DecimalAvgAccumulator<T> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += (values.len() - values.null_count()) as u64;

        if let Some(x) = sum(values) {
            let v = self.sum.get_or_insert_with(T::Native::default);
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
                    DecimalAverager::<T>::try_new(
                        self.sum_scale,
                        self.target_precision,
                        self.target_scale,
                    )?
                    .avg(v, T::Native::from_usize(self.count as usize).unwrap())
                })
                .transpose()?
        };

        ScalarValue::new_primitive::<T>(
            v,
            &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<T>(
                self.sum,
                &T::TYPE_CONSTRUCTOR(self.sum_precision, self.sum_scale),
            )?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<T>()) {
            let v = self.sum.get_or_insert_with(T::Native::default);
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = sum(values) {
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
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send + 'static,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

/// Avg accumulator that stores its per-group state in fixed-size blocks.
#[derive(Debug)]
struct BlockedAvgGroupsAccumulator {
    block_size: usize,
    counts: Vec<Box<[u64]>>,
    sums: Vec<Box<[f64]>>,
    null_state: NullState,
    len: usize,
}

impl BlockedAvgGroupsAccumulator {
    fn new(block_size: usize) -> Self {
        assert!(block_size > 0);
        Self {
            block_size,
            counts: vec![],
            sums: vec![],
            null_state: NullState::new(),
            len: 0,
        }
    }

    fn ensure_capacity(&mut self, total_num_groups: usize) {
        let required_blocks = total_num_groups.div_ceil(self.block_size);
        while self.counts.len() < required_blocks {
            self.counts
                .push(vec![0; self.block_size].into_boxed_slice());
            self.sums
                .push(vec![0.0; self.block_size].into_boxed_slice());
        }
        self.len = self.len.max(total_num_groups);
    }

    fn values_range<T: Copy>(
        blocks: &[Box<[T]>],
        block_size: usize,
        start: usize,
        len: usize,
    ) -> Vec<T> {
        let mut output = Vec::with_capacity(len);
        let mut remaining = len;
        let mut group_id = start;

        while remaining > 0 {
            let block_idx = group_id / block_size;
            let offset = group_id % block_size;
            let take = remaining.min(block_size - offset);
            output.extend_from_slice(&blocks[block_idx][offset..offset + take]);
            remaining -= take;
            group_id += take;
        }

        output
    }

    fn rebuild_from_values(&mut self, counts: &[u64], sums: &[f64]) {
        debug_assert_eq!(counts.len(), sums.len());
        self.counts.clear();
        self.sums.clear();
        self.len = counts.len();

        for chunk in counts.chunks(self.block_size) {
            let mut block = vec![0; self.block_size].into_boxed_slice();
            block[..chunk.len()].copy_from_slice(chunk);
            self.counts.push(block);
        }

        for chunk in sums.chunks(self.block_size) {
            let mut block = vec![0.0; self.block_size].into_boxed_slice();
            block[..chunk.len()].copy_from_slice(chunk);
            self.sums.push(block);
        }
    }

    fn take_all_values(&mut self) -> (Vec<u64>, Vec<f64>) {
        let counts = Self::values_range(&self.counts, self.block_size, 0, self.len);
        let sums = Self::values_range(&self.sums, self.block_size, 0, self.len);
        self.counts.clear();
        self.sums.clear();
        self.len = 0;
        (counts, sums)
    }

    fn take_block_values(&mut self) -> (Vec<u64>, Vec<f64>) {
        let emit_len = self.len.min(self.block_size);

        let mut counts = self.counts.remove(0).into_vec();
        counts.truncate(emit_len);

        let mut sums = self.sums.remove(0).into_vec();
        sums.truncate(emit_len);

        self.len -= emit_len;
        (counts, sums)
    }

    fn take_first_values(&mut self, n: usize) -> (Vec<u64>, Vec<f64>) {
        let n = n.min(self.len);
        let counts = Self::values_range(&self.counts, self.block_size, 0, n);
        let sums = Self::values_range(&self.sums, self.block_size, 0, n);

        let remaining_counts =
            Self::values_range(&self.counts, self.block_size, n, self.len - n);
        let remaining_sums =
            Self::values_range(&self.sums, self.block_size, n, self.len - n);
        self.rebuild_from_values(&remaining_counts, &remaining_sums);

        (counts, sums)
    }

    fn take_values(&mut self, emit_to: EmitTo) -> (Vec<u64>, Vec<f64>) {
        self.null_state = NullState::new();
        match emit_to {
            EmitTo::All => self.take_all_values(),
            EmitTo::Block => self.take_block_values(),
            EmitTo::First(n) => self.take_first_values(n),
        }
    }

    fn nulls_for_counts(counts: &[u64]) -> Option<arrow::buffer::NullBuffer> {
        let mut nulls = NullBufferBuilder::new(counts.len());
        for count in counts {
            if *count == 0 {
                nulls.append_null();
            } else {
                nulls.append_non_null();
            }
        }
        nulls.finish()
    }
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send + 'static,
{
    pub fn new(sum_data_type: &DataType, return_data_type: &DataType, avg_fn: F) -> Self {
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type}) --> {return_data_type}",
            std::any::type_name::<T>()
        );

        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type: sum_data_type.clone(),
            counts: vec![],
            sums: vec![],
            null_state: NullState::new(),
            avg_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send + 'static,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                // SAFETY: group_index is guaranteed to be in bounds
                let sum = unsafe { self.sums.get_unchecked_mut(group_index) };
                *sum = sum.add_wrapping(new_value);

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

        let array: PrimitiveArray<T> = if let Some(nulls) = &nulls
            && nulls.null_count() > 0
        {
            let mut builder = PrimitiveBuilder::<T>::with_capacity(nulls.len())
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
            let averages: Vec<T::Native> = sums
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
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls) // zero copy
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
        let partial_sums = values[1].as_primitive::<T>();
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
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <T as ArrowPrimitiveType>::Native| {
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
        let sums = values[0]
            .as_primitive::<T>()
            .clone()
            .with_data_type(self.sum_data_type.clone());
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

    fn create_blocked_accumulator(
        &self,
        block_size: usize,
    ) -> Result<Option<Box<dyn GroupsAccumulator>>> {
        if self.sum_data_type == DataType::Float64
            && self.return_data_type == DataType::Float64
        {
            Ok(Some(Box::new(BlockedAvgGroupsAccumulator::new(block_size))))
        } else {
            Ok(None)
        }
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<u64>() + self.sums.capacity() * size_of::<T>()
    }
}

impl GroupsAccumulator for BlockedAvgGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Float64Type>();
        self.ensure_capacity(total_num_groups);

        let counts = &mut self.counts;
        let sums = &mut self.sums;
        let block_size = self.block_size;
        let len = self.len;

        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, value| {
                debug_assert!(group_index < len);
                let block_idx = group_index / block_size;
                let value_idx = group_index % block_size;
                counts[block_idx][value_idx] += 1;
                sums[block_idx][value_idx] += value;
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (counts, sums) = self.take_values(emit_to);
        let mut values = Vec::with_capacity(counts.len());
        let mut nulls = NullBufferBuilder::new(counts.len());

        for (count, sum) in counts.into_iter().zip(sums) {
            if count == 0 {
                values.push(0.0);
                nulls.append_null();
            } else {
                values.push(sum / count as f64);
                nulls.append_non_null();
            }
        }

        Ok(Arc::new(Float64Array::new(values.into(), nulls.finish())))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (counts, sums) = self.take_values(emit_to);
        let nulls = Self::nulls_for_counts(&counts);

        Ok(vec![
            Arc::new(UInt64Array::new(counts.into(), nulls.clone())) as ArrayRef,
            Arc::new(Float64Array::new(sums.into(), nulls)) as ArrayRef,
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
        self.ensure_capacity(total_num_groups);

        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<Float64Type>();

        let counts = &mut self.counts;
        let block_size = self.block_size;
        let len = self.len;
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                debug_assert!(group_index < len);
                let block_idx = group_index / block_size;
                let value_idx = group_index % block_size;
                counts[block_idx][value_idx] += partial_count;
            },
        );

        let sums = &mut self.sums;
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, partial_sum| {
                debug_assert!(group_index < len);
                let block_idx = group_index / block_size;
                let value_idx = group_index % block_size;
                sums[block_idx][value_idx] += partial_sum;
            },
        );

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert_eq!(values.len(), 1, "single argument to convert_to_state");
        let values = values[0].as_primitive::<Float64Type>();
        let mut counts = Vec::with_capacity(values.len());
        let mut sums = Vec::with_capacity(values.len());
        let mut nulls = NullBufferBuilder::new(values.len());

        for row in 0..values.len() {
            if opt_filter
                .map(|filter| filter.is_valid(row) && filter.value(row))
                .unwrap_or(true)
                && values.is_valid(row)
            {
                counts.push(1);
                sums.push(values.value(row));
                nulls.append_non_null();
            } else {
                counts.push(0);
                sums.push(0.0);
                nulls.append_null();
            }
        }

        let nulls = nulls.finish();
        Ok(vec![
            Arc::new(UInt64Array::new(counts.into(), nulls.clone())) as ArrayRef,
            Arc::new(Float64Array::new(sums.into(), nulls)) as ArrayRef,
        ])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn supports_blocked_emit(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.len() * self.block_size * size_of::<u64>()
            + self.sums.len() * self.block_size * size_of::<f64>()
            + self.counts.allocated_size()
            + self.sums.allocated_size()
            + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_avg_state_emit_blocks() -> Result<()> {
        let mut accumulator = BlockedAvgGroupsAccumulator::new(3);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(4.0),
            Some(5.0),
            Some(6.0),
            Some(7.0),
        ]));
        let group_indices = vec![0, 1, 2, 3, 4, 5, 6];

        accumulator.update_batch(&[values], &group_indices, None, 7)?;

        let state = accumulator.state(EmitTo::Block)?;
        let counts = state[0].as_primitive::<UInt64Type>();
        let sums = state[1].as_primitive::<Float64Type>();
        assert_eq!(counts.values().as_ref(), &[1, 1, 0]);
        assert_eq!(sums.values().as_ref(), &[1.0, 2.0, 0.0]);
        assert!(counts.is_null(2));
        assert!(sums.is_null(2));

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<UInt64Type>().values().as_ref(),
            &[1, 1, 1]
        );
        assert_eq!(
            state[1].as_primitive::<Float64Type>().values().as_ref(),
            &[4.0, 5.0, 6.0]
        );

        let state = accumulator.state(EmitTo::Block)?;
        assert_eq!(
            state[0].as_primitive::<UInt64Type>().values().as_ref(),
            &[1]
        );
        assert_eq!(
            state[1].as_primitive::<Float64Type>().values().as_ref(),
            &[7.0]
        );

        Ok(())
    }

    #[test]
    fn blocked_avg_merge_evaluate_blocks() -> Result<()> {
        let mut accumulator = BlockedAvgGroupsAccumulator::new(2);
        let counts: ArrayRef = Arc::new(UInt64Array::from(vec![2, 1, 3, 4, 5]));
        let sums: ArrayRef =
            Arc::new(Float64Array::from(vec![10.0, 4.0, 9.0, 20.0, 15.0]));
        let group_indices = vec![0, 1, 2, 3, 4];

        accumulator.merge_batch(&[counts, sums], &group_indices, None, 5)?;

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[5.0, 4.0]
        );

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[3.0, 5.0]
        );

        let output = accumulator.evaluate(EmitTo::Block)?;
        assert_eq!(
            output.as_primitive::<Float64Type>().values().as_ref(),
            &[3.0]
        );

        Ok(())
    }
}
