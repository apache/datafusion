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
    BooleanArray, PrimitiveArray, PrimitiveBuilder,
};

use arrow::compute::sum;
use arrow::datatypes::{
    ArrowNativeType, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE,
    DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE, DataType,
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, FieldRef, Float64Type, Int64Type, TimeUnit, UInt64Type,
    i256,
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
            (Float64, Float64) => Ok(Box::new(AvgGroupsAccumulator::<
                Float64Type,
                UInt64Type,
                _,
                true,
            >::new(
                data_type,
                args.return_field.data_type(),
                |sum: f64, count: u64| Ok(sum / count as f64),
            ))),
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

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal32Type,
                    UInt64Type,
                    _,
                    true,
                >::new(
                    data_type, args.return_field.data_type(), avg_fn
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

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal64Type,
                    UInt64Type,
                    _,
                    true,
                >::new(
                    data_type, args.return_field.data_type(), avg_fn
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

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal128Type,
                    UInt64Type,
                    _,
                    true,
                >::new(
                    data_type, args.return_field.data_type(), avg_fn
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

                Ok(Box::new(AvgGroupsAccumulator::<
                    Decimal256Type,
                    UInt64Type,
                    _,
                    true,
                >::new(
                    data_type, args.return_field.data_type(), avg_fn
                )))
            }

            (Duration(time_unit), Duration(_result_unit)) => {
                let avg_fn = move |sum: i64, count: u64| Ok(sum / count as i64);

                match time_unit {
                    TimeUnit::Second => {
                        Ok(Box::new(AvgGroupsAccumulator::<
                            DurationSecondType,
                            UInt64Type,
                            _,
                            true,
                        >::new(
                            data_type, args.return_type(), avg_fn
                        )))
                    }
                    TimeUnit::Millisecond => {
                        Ok(Box::new(AvgGroupsAccumulator::<
                            DurationMillisecondType,
                            UInt64Type,
                            _,
                            true,
                        >::new(
                            data_type, args.return_type(), avg_fn
                        )))
                    }
                    TimeUnit::Microsecond => {
                        Ok(Box::new(AvgGroupsAccumulator::<
                            DurationMicrosecondType,
                            UInt64Type,
                            _,
                            true,
                        >::new(
                            data_type, args.return_type(), avg_fn
                        )))
                    }
                    TimeUnit::Nanosecond => {
                        Ok(Box::new(AvgGroupsAccumulator::<
                            DurationNanosecondType,
                            UInt64Type,
                            _,
                            true,
                        >::new(
                            data_type, args.return_type(), avg_fn
                        )))
                    }
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
/// T::Native and a total count.
///
/// `COUNT_FIRST` controls the state field order:
/// * `true`: `[count, sum]` for built-in AVG
/// * `false`: `[sum, count]` for Spark AVG
#[derive(Debug)]
struct AvgGroupsAccumulator<T, CountType, F, const COUNT_FIRST: bool>
where
    T: ArrowNumericType + Send,
    CountType: ArrowPrimitiveType + Send,
    F: Fn(T::Native, CountType::Native) -> Result<T::Native> + Send + 'static,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group
    counts: Vec<CountType::Native>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

impl<T, CountType, F, const COUNT_FIRST: bool>
    AvgGroupsAccumulator<T, CountType, F, COUNT_FIRST>
where
    T: ArrowNumericType + Send,
    CountType: ArrowPrimitiveType + Send,
    F: Fn(T::Native, CountType::Native) -> Result<T::Native> + Send + 'static,
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

    fn state_arrays(
        sums: PrimitiveArray<T>,
        counts: PrimitiveArray<CountType>,
    ) -> Vec<ArrayRef> {
        if COUNT_FIRST {
            vec![Arc::new(counts) as ArrayRef, Arc::new(sums) as ArrayRef]
        } else {
            vec![Arc::new(sums) as ArrayRef, Arc::new(counts) as ArrayRef]
        }
    }
}

impl<T, CountType, F, const COUNT_FIRST: bool> GroupsAccumulator
    for AvgGroupsAccumulator<T, CountType, F, COUNT_FIRST>
where
    T: ArrowNumericType + Send,
    CountType: ArrowPrimitiveType + Send,
    F: Fn(T::Native, CountType::Native) -> Result<T::Native> + Send + 'static,
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
        self.counts
            .resize(total_num_groups, CountType::Native::usize_as(0));
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

                let count = unsafe { self.counts.get_unchecked_mut(group_index) };
                *count = count.add_wrapping(CountType::Native::usize_as(1));
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
        let counts = PrimitiveArray::<CountType>::new(counts.into(), nulls.clone());

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls) // zero copy
            .with_data_type(self.sum_data_type.clone());

        Ok(Self::state_arrays(sums, counts))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        let (partial_counts, partial_sums) = if COUNT_FIRST {
            (
                values[0].as_primitive::<CountType>(),
                values[1].as_primitive::<T>(),
            )
        } else {
            (
                values[1].as_primitive::<CountType>(),
                values[0].as_primitive::<T>(),
            )
        };

        // update counts with partial counts
        self.counts
            .resize(total_num_groups, CountType::Native::usize_as(0));
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                // SAFETY: group_index is guaranteed to be in bounds
                let count = unsafe { self.counts.get_unchecked_mut(group_index) };
                *count = count.add_wrapping(partial_count);
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
        let counts = PrimitiveArray::<CountType>::from_value(
            CountType::Native::usize_as(1),
            sums.len(),
        );

        let nulls = filtered_null_mask(opt_filter, &sums);
        let counts = set_nulls(counts, nulls.clone());
        let sums = set_nulls(sums, nulls);

        Ok(Self::state_arrays(sums, counts))
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<CountType::Native>()
            + self.sums.capacity() * size_of::<T::Native>()
            + self.null_state.size()
    }
}

/// Creates the Spark AVG grouped accumulator using the built-in AVG state
/// handling implementation while preserving Spark's `[sum, count]` state order
/// and signed count type.
#[doc(hidden)]
pub fn spark_avg_groups_accumulator(
    return_data_type: &DataType,
) -> Box<dyn GroupsAccumulator> {
    Box::new(
        AvgGroupsAccumulator::<Float64Type, Int64Type, _, false>::new(
            return_data_type,
            return_data_type,
            |sum: f64, count: i64| Ok(sum / count as f64),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array};
    use arrow::datatypes::{Decimal128Type, DurationNanosecondType};

    fn assert_validity(array: &dyn Array, expected: &[bool]) {
        assert_eq!(array.len(), expected.len());
        for (idx, expected_valid) in expected.iter().copied().enumerate() {
            assert_eq!(!array.is_null(idx), expected_valid, "validity at row {idx}");
        }
    }

    fn avg_state<T: ArrowPrimitiveType>(
        state: &[ArrayRef],
    ) -> (&PrimitiveArray<UInt64Type>, &PrimitiveArray<T>) {
        assert_eq!(state.len(), 2);
        (
            state[0].as_primitive::<UInt64Type>(),
            state[1].as_primitive::<T>(),
        )
    }

    fn float64_acc() -> AvgGroupsAccumulator<
        Float64Type,
        UInt64Type,
        impl Fn(f64, u64) -> Result<f64>,
        true,
    > {
        AvgGroupsAccumulator::<Float64Type, UInt64Type, _, true>::new(
            &DataType::Float64,
            &DataType::Float64,
            |sum, count| Ok(sum / count as f64),
        )
    }

    fn decimal128_acc() -> AvgGroupsAccumulator<
        Decimal128Type,
        UInt64Type,
        impl Fn(i128, u64) -> Result<i128>,
        true,
    > {
        AvgGroupsAccumulator::<Decimal128Type, UInt64Type, _, true>::new(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(14, 6),
            // convert_to_state does not evaluate averages, so avg_fn is unused here.
            |sum, _count| Ok(sum),
        )
    }

    fn duration_acc() -> AvgGroupsAccumulator<
        DurationNanosecondType,
        UInt64Type,
        impl Fn(i64, u64) -> Result<i64>,
        true,
    > {
        AvgGroupsAccumulator::<DurationNanosecondType, UInt64Type, _, true>::new(
            &DataType::Duration(TimeUnit::Nanosecond),
            &DataType::Duration(TimeUnit::Nanosecond),
            |sum, count| Ok(sum / count as i64),
        )
    }

    #[test]
    fn float64_convert_to_state_uses_count_sum_order_and_null_filter() {
        let acc = float64_acc();
        let values: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            Some(10.0),
            Some(20.0),
            None,
            Some(40.0),
        ]))];
        let filter = BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]);

        let state = acc.convert_to_state(&values, Some(&filter)).unwrap();

        let (counts, sums) = avg_state::<Float64Type>(&state);
        assert_eq!(counts.values().as_ref(), &[1, 1, 1, 1]);
        assert_validity(counts, &[true, false, false, false]);
        assert_validity(sums, &[true, false, false, false]);
    }

    #[test]
    fn decimal_convert_to_state_preserves_sum_type_and_nulls() {
        let acc = decimal128_acc();
        let values: Vec<ArrayRef> = vec![Arc::new(
            PrimitiveArray::<Decimal128Type>::from(vec![
                Some(100_i128),
                None,
                Some(300_i128),
            ])
            .with_data_type(DataType::Decimal128(10, 2)),
        )];

        let state = acc.convert_to_state(&values, None).unwrap();

        let (counts, sums) = avg_state::<Decimal128Type>(&state);
        assert_eq!(sums.data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(counts.values().as_ref(), &[1, 1, 1]);
        assert_validity(counts, &[true, false, true]);
        assert_validity(sums, &[true, false, true]);
    }

    #[test]
    fn duration_convert_to_state_preserves_sum_type_and_applies_filter() {
        let acc = duration_acc();
        let values: Vec<ArrayRef> = vec![Arc::new(
            PrimitiveArray::<DurationNanosecondType>::from(vec![
                Some(10_i64),
                Some(20_i64),
            ])
            .with_data_type(DataType::Duration(TimeUnit::Nanosecond)),
        )];
        let filter = BooleanArray::from(vec![Some(false), Some(true)]);

        let state = acc.convert_to_state(&values, Some(&filter)).unwrap();

        let (counts, sums) = avg_state::<DurationNanosecondType>(&state);
        assert_eq!(sums.data_type(), &DataType::Duration(TimeUnit::Nanosecond));
        assert_eq!(counts.values().as_ref(), &[1, 1]);
        assert_validity(counts, &[false, true]);
        assert_validity(sums, &[false, true]);
    }
}
