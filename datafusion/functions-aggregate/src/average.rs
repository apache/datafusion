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

use arrow::compute::{DecimalCast, sum};
use arrow::datatypes::{
    ArrowNativeType, DECIMAL32_MAX_PRECISION, DECIMAL32_MAX_SCALE,
    DECIMAL64_MAX_PRECISION, DECIMAL64_MAX_SCALE, DECIMAL128_MAX_PRECISION,
    DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE, DataType,
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, FieldRef, Float64Type, TimeUnit, UInt64Type,
};
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, exec_err, internal_err, not_impl_err,
};
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

/// Digits reserved above the input precision for `avg`'s intermediate sum: 4 for
/// the scale-up [`DecimalAverager`] applies before dividing (`Avg::return_type`
/// adds 4 to the scale), 9 for the row count.
///
/// The 9 is a row budget. A sum of `n` rows of `Decimal(p, _)` is bounded by
/// `n * 10^p`, so a sum type with `p + 4 + 9` digits holds `10^9` rows. The sum
/// wraps on overflow, like the `sum` aggregate; the budget is what puts that out
/// of reach. `Decimal256` input near max precision is the exception: no wider
/// type exists, so its sum keeps only whatever headroom `Decimal256(76, _)` has
/// left, as before this budget was introduced.
const AVG_SUM_HEADROOM_DIGITS: u8 = 13;

/// The narrowest decimal that can accumulate `avg`'s sum over `data_type`, never
/// narrower than `data_type` itself. Other types accumulate as themselves.
fn avg_sum_data_type(data_type: &DataType) -> DataType {
    let (precision, scale, input_max_precision) = match data_type {
        DataType::Decimal32(precision, scale) => {
            (*precision, *scale, DECIMAL32_MAX_PRECISION)
        }
        DataType::Decimal64(precision, scale) => {
            (*precision, *scale, DECIMAL64_MAX_PRECISION)
        }
        DataType::Decimal128(precision, scale) => {
            (*precision, *scale, DECIMAL128_MAX_PRECISION)
        }
        DataType::Decimal256(precision, scale) => {
            (*precision, *scale, DECIMAL256_MAX_PRECISION)
        }
        data_type => return data_type.clone(),
    };

    let required = precision
        .saturating_add(AVG_SUM_HEADROOM_DIGITS)
        .max(input_max_precision);

    // `required` always exceeds `DECIMAL32_MAX_PRECISION`, so a `Decimal32` sum is
    // never wide enough, not even for `Decimal32` input
    if required <= DECIMAL64_MAX_PRECISION {
        DataType::Decimal64(DECIMAL64_MAX_PRECISION, scale)
    } else if required <= DECIMAL128_MAX_PRECISION {
        DataType::Decimal128(DECIMAL128_MAX_PRECISION, scale)
    } else {
        DataType::Decimal256(DECIMAL256_MAX_PRECISION, scale)
    }
}

/// Instantiates `$builder::<Input, Sum>` for every decimal pair that
/// [`avg_sum_data_type`] can produce.
macro_rules! decimal_avg_dispatch {
    ($input:expr, $sum:expr, $builder:ident, $($arg:expr),*) => {
        match ($input, $sum) {
            (DataType::Decimal32(..), DataType::Decimal64(..)) => {
                $builder::<Decimal32Type, Decimal64Type>($($arg),*)
            }
            (DataType::Decimal32(..), DataType::Decimal128(..)) => {
                $builder::<Decimal32Type, Decimal128Type>($($arg),*)
            }
            (DataType::Decimal64(..), DataType::Decimal64(..)) => {
                $builder::<Decimal64Type, Decimal64Type>($($arg),*)
            }
            (DataType::Decimal64(..), DataType::Decimal128(..)) => {
                $builder::<Decimal64Type, Decimal128Type>($($arg),*)
            }
            (DataType::Decimal128(..), DataType::Decimal128(..)) => {
                $builder::<Decimal128Type, Decimal128Type>($($arg),*)
            }
            (DataType::Decimal128(..), DataType::Decimal256(..)) => {
                $builder::<Decimal128Type, Decimal256Type>($($arg),*)
            }
            (DataType::Decimal256(..), DataType::Decimal256(..)) => {
                $builder::<Decimal256Type, Decimal256Type>($($arg),*)
            }
            (input, sum) => {
                internal_err!("avg cannot accumulate {input} as {sum}")
            }
        }
    };
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

                (Decimal32(..), Decimal32(..))
                | (Decimal64(..), Decimal64(..))
                | (Decimal128(..), Decimal128(..))
                | (Decimal256(..), Decimal256(..)) => {
                    let sum_data_type = avg_sum_data_type(data_type);
                    decimal_avg_dispatch!(
                        data_type,
                        &sum_data_type,
                        decimal_distinct_avg_accumulator,
                        &sum_data_type,
                        acc_args.return_type()
                    )
                }

                (dt, return_type) => exec_err!(
                    "AVG(DISTINCT) for ({} --> {}) not supported",
                    dt,
                    return_type
                ),
            }
        } else {
            match (&data_type, acc_args.return_type()) {
                (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
                (Decimal32(..), Decimal32(..))
                | (Decimal64(..), Decimal64(..))
                | (Decimal128(..), Decimal128(..))
                | (Decimal256(..), Decimal256(..)) => {
                    let sum_data_type = avg_sum_data_type(data_type);
                    decimal_avg_dispatch!(
                        data_type,
                        &sum_data_type,
                        decimal_avg_accumulator,
                        sum_data_type.clone(),
                        acc_args.return_type().clone()
                    )
                }

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
                    data_type,
                    args.return_field.data_type(),
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (Decimal32(..), Decimal32(..))
            | (Decimal64(..), Decimal64(..))
            | (Decimal128(..), Decimal128(..))
            | (Decimal256(..), Decimal256(..)) => {
                let sum_data_type = avg_sum_data_type(data_type);
                decimal_avg_dispatch!(
                    data_type,
                    &sum_data_type,
                    decimal_avg_groups_accumulator,
                    &sum_data_type,
                    args.return_field.data_type()
                )
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

/// The precision and scale of a decimal `DataType`
fn decimal_parts(data_type: &DataType) -> Result<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Ok((*precision, *scale)),
        data_type => internal_err!("expected a decimal type, got {data_type}"),
    }
}

fn decimal_avg_fn<I, S>(
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
) -> Result<impl Fn(S::Native, u64) -> Result<I::Native> + Send + Sync + 'static>
where
    I: DecimalType,
    S: DecimalType,
    I::Native: DecimalCast,
    S::Native: DecimalCast,
{
    let decimal_averager =
        DecimalAverager::<S>::try_new(sum_scale, target_precision, target_scale)?;

    Ok(move |sum, count: u64| {
        let Some(count) = usize::try_from(count).ok().and_then(S::Native::from_usize)
        else {
            return exec_err!(
                "Arithmetic overflow in avg: the row count {count} cannot be \
                 represented in the sum type"
            );
        };

        // Narrowing the average back to the (never wider) output type cannot
        // fail in practice: `DecimalAverager::avg` validates the average
        // against the output precision, whose bound fits the output's native
        // type by construction
        I::Native::from_decimal(decimal_averager.avg(sum, count)?).ok_or_else(|| {
            exec_datafusion_err!(
                "Arithmetic overflow in avg: the computed average does not fit \
                 the output type"
            )
        })
    })
}

fn decimal_avg_accumulator<I, S>(
    sum_data_type: DataType,
    return_data_type: DataType,
) -> Result<Box<dyn Accumulator>>
where
    I: DecimalType + ArrowNumericType + Debug + Send + Sync,
    S: DecimalType + ArrowNumericType + Debug + Send + Sync,
    I::Native: Into<S::Native> + DecimalCast,
    S::Native: DecimalCast,
{
    let (_, sum_scale) = decimal_parts(&sum_data_type)?;
    let (target_precision, target_scale) = decimal_parts(&return_data_type)?;
    let avg_fn = decimal_avg_fn::<I, S>(sum_scale, target_precision, target_scale)?;

    Ok(Box::new(DecimalAvgAccumulator::<I, S, _>::new(
        sum_data_type,
        return_data_type,
        avg_fn,
    )))
}

fn decimal_distinct_avg_accumulator<I, S>(
    sum_data_type: &DataType,
    return_data_type: &DataType,
) -> Result<Box<dyn Accumulator>>
where
    I: DecimalType + ArrowNumericType + Debug + Send + Sync,
    S: DecimalType + ArrowNumericType + Debug + Send + Sync,
    I::Native: Into<S::Native> + DecimalCast,
    S::Native: DecimalCast,
{
    let (_, sum_scale) = decimal_parts(sum_data_type)?;
    let (target_precision, target_scale) = decimal_parts(return_data_type)?;

    Ok(Box::new(
        DecimalDistinctAvgAccumulator::<I, S>::with_decimal_params(
            sum_scale,
            target_precision,
            target_scale,
        ),
    ))
}

fn decimal_avg_groups_accumulator<I, S>(
    sum_data_type: &DataType,
    return_data_type: &DataType,
) -> Result<Box<dyn GroupsAccumulator>>
where
    I: DecimalType + ArrowNumericType + Debug + Send + Sync,
    S: DecimalType + ArrowNumericType + Debug + Send + Sync,
    I::Native: Into<S::Native> + DecimalCast,
    S::Native: DecimalCast,
{
    let (_, sum_scale) = decimal_parts(sum_data_type)?;
    let (target_precision, target_scale) = decimal_parts(return_data_type)?;
    let avg_fn = decimal_avg_fn::<I, S>(sum_scale, target_precision, target_scale)?;

    Ok(Box::new(AvgGroupsAccumulator::<I, _, S>::new(
        sum_data_type,
        return_data_type,
        avg_fn,
    )))
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
/// `I` is the input (and output) decimal type. `S` is the type used to accumulate
/// the sum, chosen by [`avg_sum_data_type`] so the running total does not overflow.
struct DecimalAvgAccumulator<I, S, F>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native>,
{
    sum: Option<S::Native>,
    count: u64,
    sum_data_type: DataType,
    return_data_type: DataType,
    avg_fn: F,
    _phantom: PhantomData<I>,
}

impl<I, S, F> Debug for DecimalAvgAccumulator<I, S, F>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecimalAvgAccumulator")
            .field("sum", &self.sum)
            .field("count", &self.count)
            .field("sum_data_type", &self.sum_data_type)
            .field("return_data_type", &self.return_data_type)
            .finish_non_exhaustive()
    }
}

impl<I, S, F> DecimalAvgAccumulator<I, S, F>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native>,
{
    fn new(sum_data_type: DataType, return_data_type: DataType, avg_fn: F) -> Self {
        Self {
            sum: None,
            count: 0,
            sum_data_type,
            return_data_type,
            avg_fn,
            _phantom: PhantomData,
        }
    }
}

/// Sums `values` into the wider `S`.
///
/// Wraps on overflow, matching the `sum` aggregate and [`arrow::compute::sum`].
/// [`avg_sum_data_type`] gives `S` enough headroom that this is unreachable for
/// any realistic row count.
fn decimal_sum_as<I, S>(values: &PrimitiveArray<I>) -> Option<S::Native>
where
    I: DecimalType + ArrowNumericType,
    S: DecimalType + ArrowNumericType,
    I::Native: Into<S::Native>,
{
    // Matches `arrow::compute::sum`: an empty or all-null input has no sum
    if values.null_count() == values.len() {
        return None;
    }

    let mut sum = S::Native::default();
    if values.null_count() == 0 {
        for value in values.values() {
            sum = sum.add_wrapping((*value).into());
        }
    } else {
        for value in values.iter().flatten() {
            sum = sum.add_wrapping(value.into());
        }
    }

    Some(sum)
}

impl<I, S, F> Accumulator for DecimalAvgAccumulator<I, S, F>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native>,
    F: Fn(S::Native, u64) -> Result<I::Native> + Send + Sync + 'static,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<I>();
        self.count += (values.len() - values.null_count()) as u64;

        if let Some(x) = decimal_sum_as::<I, S>(values) {
            let v = self.sum.unwrap_or_default();
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
            self.sum.map(|v| (self.avg_fn)(v, self.count)).transpose()?
        };

        ScalarValue::new_primitive::<I>(v, &self.return_data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.count),
            ScalarValue::new_primitive::<S>(self.sum, &self.sum_data_type)?,
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[0].as_primitive::<UInt64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[1].as_primitive::<S>()) {
            let v = self.sum.unwrap_or_default();
            self.sum = Some(v.add_wrapping(x));
        }
        Ok(())
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<I>();
        self.count -= (values.len() - values.null_count()) as u64;
        if let Some(x) = decimal_sum_as::<I, S>(values) {
            let v = self.sum.unwrap_or_default();
            self.sum = Some(v.sub_wrapping(x));
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

/// An accumulator to compute the average of `[PrimitiveArray<I>]`.
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
    pub fn new(sum_data_type: &DataType, return_data_type: &DataType, avg_fn: F) -> Self {
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type}) --> {return_data_type}",
            std::any::type_name::<I>()
        );

        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type: sum_data_type.clone(),
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
            None,
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
            None,
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
        // When the sum type equals the input type (`I == S`: `Float64`,
        // `Duration`, `Decimal256`, and any decimal whose precision already
        // leaves [`avg_sum_data_type`] enough headroom) the input is already a
        // valid sum array and is reused as is; the downcast is by Rust type, so
        // it succeeds even when precision differs. Otherwise every value is
        // widened.
        let sums = match values[0].as_any().downcast_ref::<PrimitiveArray<S>>() {
            Some(sums) => sums.clone().with_data_type(self.sum_data_type.clone()),
            None => {
                let values = values[0].as_primitive::<I>();
                // Values under null slots are widened too rather than branching per
                // element; `set_nulls` below masks them out again.
                let sums: Vec<S::Native> = values
                    .values()
                    .iter()
                    .map(|value| (*value).into())
                    .collect();
                PrimitiveArray::<S>::new(sums.into(), values.nulls().cloned())
                    .with_data_type(self.sum_data_type.clone())
            }
        };
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
        // Heap buffers
        self.counts.capacity() * size_of::<u64>()
        + self.sums.capacity() * size_of::<S::Native>()
        // Vec struct overhead (ptr, len, cap) for each field
        + size_of::<Vec<u64>>()
        + size_of::<Vec<S::Native>>()
        // Null tracking buffers
        + self.null_state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
        DurationSecondArray, Float64Array,
    };
    use arrow::datatypes::{Schema, i256};

    struct AvgCase {
        name: &'static str,
        values: ArrayRef,
        return_type: DataType,
        sum_type: DataType,
        expected: ScalarValue,
    }

    fn with_avg_args<R>(
        input_type: &DataType,
        return_type: &DataType,
        f: impl FnOnce(AccumulatorArgs) -> R,
    ) -> R {
        let schema = Schema::empty();
        let expr_field = Arc::new(Field::new("a", input_type.clone(), true));
        let return_field = Arc::new(Field::new("avg", return_type.clone(), true));

        f(AccumulatorArgs {
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

    fn avg_groups_accumulator(
        input_type: &DataType,
        return_type: &DataType,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        with_avg_args(input_type, return_type, |args| {
            Avg::new().create_groups_accumulator(args)
        })
    }

    fn avg_accumulator(
        input_type: &DataType,
        return_type: &DataType,
    ) -> Result<Box<dyn Accumulator>> {
        with_avg_args(input_type, return_type, |args| Avg::new().accumulator(args))
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
        const DECIMAL64_VALUE: i64 = 99_999_999_999_999;
        const DECIMAL128_ROWS: usize = 21_476;
        const DECIMAL128_VALUE: i128 = 9_999_999_999_999_999_999_999_999_999_999_999;

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
                        .with_precision_and_scale(14, 0)?,
                ),
                return_type: DataType::Decimal64(18, 4),
                sum_type: DataType::Decimal128(38, 0),
                expected: ScalarValue::Decimal64(Some(DECIMAL64_VALUE * 10_000), 18, 4),
            },
            AvgCase {
                name: "decimal128",
                values: Arc::new(
                    Decimal128Array::from(vec![Some(DECIMAL128_VALUE); DECIMAL128_ROWS])
                        .with_precision_and_scale(34, 0)?,
                ),
                return_type: DataType::Decimal128(38, 4),
                sum_type: DataType::Decimal256(76, 0),
                expected: ScalarValue::Decimal128(Some(DECIMAL128_VALUE * 10_000), 38, 4),
            },
            AvgCase {
                name: "decimal256",
                values: Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(10), i256::from_i128(20)])
                        .with_precision_and_scale(50, 0)?,
                ),
                return_type: DataType::Decimal256(54, 4),
                sum_type: DataType::Decimal256(76, 0),
                expected: ScalarValue::Decimal256(Some(i256::from_i128(150_000)), 54, 4),
            },
            // A `Decimal128` whose precision leaves room for the sum stays on
            // `i128` rather than widening to the emulated `i256` arithmetic
            AvgCase {
                name: "decimal128_with_headroom",
                values: Arc::new(
                    Decimal128Array::from(vec![100_000, 200_000])
                        .with_precision_and_scale(20, 4)?,
                ),
                return_type: DataType::Decimal128(24, 8),
                sum_type: DataType::Decimal128(38, 4),
                expected: ScalarValue::Decimal128(Some(1_500_000_000), 24, 8),
            },
            // A `Decimal32` at max precision needs more than `Decimal64` can hold
            // once `DecimalAverager` scales the sum up, so it accumulates as `i128`
            AvgCase {
                name: "decimal32_max_precision",
                values: Arc::new(
                    Decimal32Array::from(vec![10, 20]).with_precision_and_scale(9, 0)?,
                ),
                return_type: DataType::Decimal32(9, 4),
                sum_type: DataType::Decimal128(38, 0),
                expected: ScalarValue::Decimal32(Some(150_000), 9, 4),
            },
            // One duration unit suffices: all four units instantiate the same
            // `S = I` generic code
            AvgCase {
                name: "duration_second",
                values: Arc::new(DurationSecondArray::from(vec![10, 20])),
                return_type: DataType::Duration(TimeUnit::Second),
                sum_type: DataType::Duration(TimeUnit::Second),
                expected: ScalarValue::DurationSecond(Some(15)),
            },
        ])
    }

    #[test]
    fn avg_accumulator_evaluate_and_state_types() -> Result<()> {
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
            final_acc.merge_batch(&state, &vec![0; case.values.len()], 1)?;

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

    /// The widened sum fits, but the average does not fit the output type once
    /// `DecimalAverager` rescales it: avg must error rather than silently wrap
    #[test]
    fn avg_errors_when_average_exceeds_output_precision() -> Result<()> {
        let values: ArrayRef = Arc::new(
            Decimal32Array::from(vec![999_999_999]).with_precision_and_scale(9, 0)?,
        );
        let return_type = DataType::Decimal32(9, 4);
        let mut acc = avg_accumulator(values.data_type(), &return_type)?;

        acc.update_batch(&[values])?;
        assert!(acc.evaluate().is_err());

        Ok(())
    }
}
