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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::timezone::SparkTimeZone;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, PrimitiveBuilder, StringArrayType,
    new_empty_array,
};
use arrow::datatypes::TimeUnit;
use arrow::datatypes::{
    ArrowTimestampType, DataType, Field, FieldRef, Schema, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Apache Spark `from_utc_timestamp` function.
///
/// Interprets the given timestamp as UTC and converts it to the given timezone.
///
/// Timestamp in Apache Spark represents number of microseconds from the Unix epoch, which is not
/// timezone-agnostic. So in Apache Spark this function just shift the timestamp value from UTC timezone to
/// the given timezone.
///
/// # Compatibility
///
/// This function accepts Spark's additional offset spellings, Java short IDs, and
/// legacy SystemV zone IDs. They are parsed only when both arguments are non-null.
/// Spark's generated execution may validate a foldable timezone before processing
/// rows, so this implementation can return nulls where Spark raises an error.
///
/// Timezone conversion uses Chrono's calendar range, which is narrower than Spark's
/// implemented timestamp range, although it includes Spark's documented range.
/// Region timezone rules from `chrono-tz` may also differ after 2099 because it does
/// not extrapolate later daylight saving time transitions.
///
/// This UDF operates on already-evaluated arguments. Callers that need conditional
/// evaluation of computed timezone expressions should construct [`SparkFromUtcTimestampExpr`].
///
/// See <https://spark.apache.org/docs/latest/api/sql/index.html#from_utc_timestamp>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFromUtcTimestamp {
    signature: Signature,
}

impl Default for SparkFromUtcTimestamp {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFromUtcTimestamp {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Timestamp,
                        vec![TypeSignatureClass::Native(logical_string())],
                        NativeType::Timestamp(TimeUnit::Microsecond, None),
                    ),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkFromUtcTimestamp {
    fn name(&self) -> &str {
        "from_utc_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());

        Ok(Arc::new(Field::new(
            self.name(),
            args.arg_fields[0].data_type().clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_from_utc_timestamp, vec![])(&args.args)
    }
}

fn spark_from_utc_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [timestamp, timezone] = take_function_args("from_utc_timestamp", args)?;

    match timestamp.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampNanosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampMicrosecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampMillisecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        DataType::Timestamp(TimeUnit::Second, tz_opt) => {
            process_timestamp_with_tz_array::<TimestampSecondType>(
                timestamp,
                timezone,
                tz_opt.clone(),
            )
        }
        ts_type => {
            exec_err!("`from_utc_timestamp`: unsupported argument types: {ts_type}")
        }
    }
}

fn process_timestamp_with_tz_array<T: ArrowTimestampType>(
    ts_array: &ArrayRef,
    tz_array: &ArrayRef,
    tz_opt: Option<Arc<str>>,
) -> Result<ArrayRef> {
    match tz_array.data_type() {
        DataType::Utf8 => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string::<i32>())
        }
        DataType::LargeUtf8 => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string::<i64>())
        }
        DataType::Utf8View => {
            process_arrays::<T, _>(tz_opt, ts_array, tz_array.as_string_view())
        }
        other => {
            exec_err!("`from_utc_timestamp`: timezone must be a string type, got {other}")
        }
    }
}

fn process_arrays<'a, T: ArrowTimestampType, S>(
    return_tz_opt: Option<Arc<str>>,
    ts_array: &ArrayRef,
    tz_array: &'a S,
) -> Result<ArrayRef>
where
    &'a S: StringArrayType<'a>,
{
    let ts_primitive = ts_array.as_primitive::<T>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(ts_array.len());
    // Parse a scalar zone once per batch and avoid re-parsing consecutive column values
    let mut last_timezone = None;

    for (ts_opt, tz_opt) in ts_primitive.iter().zip(tz_array.iter()) {
        match (ts_opt, tz_opt) {
            (Some(ts), Some(tz_str)) => {
                let timezone = match last_timezone {
                    Some((previous, timezone)) if previous == tz_str => timezone,
                    _ => {
                        let timezone = parse_timezone(tz_str)?;
                        last_timezone = Some((tz_str, timezone));
                        timezone
                    }
                };
                let val = timezone.adjust_to_local_time::<T>(ts)?;
                builder.append_value(val);
            }
            _ => builder.append_null(),
        }
    }

    builder = builder.with_timezone_opt(return_tz_opt);
    Ok(Arc::new(builder.finish()))
}

fn parse_timezone(timezone: &str) -> Result<SparkTimeZone> {
    SparkTimeZone::parse(timezone).map_err(|e| {
        exec_datafusion_err!("`from_utc_timestamp`: invalid timezone '{timezone}': {e}")
    })
}

/// Physical `from_utc_timestamp` expression with conditional timezone evaluation.
///
/// The timestamp child is evaluated once for each non-empty batch. If every timestamp is null,
/// the timezone child is skipped. Otherwise, literal and column timezones are read directly, and
/// their values are ignored on null timestamp rows. Computed timezone expressions are evaluated
/// only for rows with non-null timestamps. Empty batches evaluate neither child.
///
/// This intentionally differs from Spark's generated evaluation of foldable timezones, which
/// evaluates the timezone and validates a non-null value before processing timestamp rows. This
/// expression can therefore skip an invalid timezone or an error in the timezone expression when
/// no timestamp is non-null. It also evaluates the timestamp first when the timezone is null, so a
/// timestamp error can be raised even though Spark's generated path would return null. Planners
/// using this expression should expose these differences as an incompatibility.
#[derive(Debug, Clone)]
pub struct SparkFromUtcTimestampExpr {
    timestamp: Arc<dyn PhysicalExpr>,
    timezone: Arc<dyn PhysicalExpr>,
}

impl SparkFromUtcTimestampExpr {
    /// Construct an expression that skips computed timezone evaluation for null timestamps.
    pub fn new(
        timestamp: Arc<dyn PhysicalExpr>,
        timezone: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            timestamp,
            timezone,
        }
    }
}

impl Display for SparkFromUtcTimestampExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "from_utc_timestamp({}, {})",
            self.timestamp, self.timezone
        )
    }
}

impl PartialEq for SparkFromUtcTimestampExpr {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp.eq(&other.timestamp) && self.timezone.eq(&other.timezone)
    }
}

impl Eq for SparkFromUtcTimestampExpr {}

impl Hash for SparkFromUtcTimestampExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.timestamp.hash(state);
        self.timezone.hash(state);
    }
}

impl PhysicalExpr for SparkFromUtcTimestampExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        let timestamp = self.timestamp.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            "from_utc_timestamp",
            timestamp.data_type().clone(),
            timestamp.is_nullable() || self.timezone.nullable(input_schema)?,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // There are no rows on which either child should run.
        if batch.num_rows() == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                &self.data_type(batch.schema_ref())?,
            )));
        }

        let timestamp = self.timestamp.evaluate(batch)?;
        let timezone = match &timestamp {
            ColumnarValue::Scalar(value) if value.is_null() => {
                return Ok(timestamp);
            }
            ColumnarValue::Scalar(_) => self.timezone.evaluate(batch)?,
            ColumnarValue::Array(array) if array.null_count() == array.len() => {
                return Ok(timestamp);
            }
            ColumnarValue::Array(_)
                if matches!(
                    self.timezone.placement(),
                    ExpressionPlacement::Literal | ExpressionPlacement::Column
                ) =>
            {
                // Literals and columns only read existing values. The conversion loop skips null
                // timestamps, so avoid filtering and copying unrelated batch columns.
                self.timezone.evaluate(batch)?
            }
            ColumnarValue::Array(array) => match array.nulls() {
                Some(nulls) => {
                    let selection = BooleanArray::new(nulls.inner().clone(), None);
                    self.timezone.evaluate_selection(batch, &selection)?
                }
                None => self.timezone.evaluate(batch)?,
            },
        };
        make_scalar_function(spark_from_utc_timestamp, vec![])(&[timestamp, timezone])
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.timestamp, &self.timezone]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [timestamp, timezone] = children.as_slice() else {
            return internal_err!(
                "SparkFromUtcTimestampExpr expected 2 children, got {}",
                children.len()
            );
        };
        Ok(Arc::new(Self::new(
            Arc::clone(timestamp),
            Arc::clone(timezone),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray,
    };
    use arrow::compute::CastOptions;
    use chrono::{DateTime, Utc};
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, CastExpr, Column, Literal,
    };
    use datafusion_common::{ScalarValue, config::ConfigOptions};
    use datafusion_expr::Operator;

    fn expression_batch(timestamps: Vec<Option<i64>>, values: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                    true,
                ),
                Field::new("value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(
                    TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(values)),
            ],
        )
        .unwrap()
    }

    fn throwing_timestamp() -> Arc<dyn PhysicalExpr> {
        Arc::new(CastExpr::new(
            Arc::new(Literal::new(ScalarValue::Utf8(Some("invalid".to_owned())))),
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            Some(CastOptions {
                safe: false,
                ..Default::default()
            }),
        ))
    }

    fn computed_timezone() -> Arc<dyn PhysicalExpr> {
        let value = Arc::new(CastExpr::new(
            Arc::new(Column::new("value", 1)),
            DataType::Int32,
            Some(CastOptions {
                safe: false,
                ..Default::default()
            }),
        ));
        let is_zero = Arc::new(BinaryExpr::new(
            value,
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        Arc::new(
            CaseExpr::try_new(
                None,
                vec![(
                    is_zero,
                    Arc::new(Literal::new(ScalarValue::Utf8(Some("UTC".to_owned())))),
                )],
                Some(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                    "PST".to_owned(),
                ))))),
            )
            .unwrap(),
        )
    }

    fn invoke(
        timestamp: ColumnarValue,
        timezone: ColumnarValue,
    ) -> Result<ColumnarValue> {
        let return_field = Arc::new(Field::new("result", timestamp.data_type(), true));
        SparkFromUtcTimestamp::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![timestamp, timezone],
            arg_fields: vec![],
            number_rows: 0,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn timestamps(values: Vec<Option<i64>>) -> ColumnarValue {
        ColumnarValue::Array(Arc::new(
            TimestampMicrosecondArray::from(values).with_timezone("UTC"),
        ))
    }

    fn scalar_zone(value: Option<&str>) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(value.map(str::to_owned)))
    }

    fn assert_timestamps(result: ColumnarValue, expected: &[Option<i64>]) {
        let ColumnarValue::Array(array) = result else {
            panic!("expected timestamp array");
        };
        let array = array.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(array.iter().collect::<Vec<_>>(), expected);
        assert_eq!(array.timezone(), Some("UTC"));
    }

    #[test]
    fn column_zones_preserve_values_nulls_and_timestamp_metadata() {
        let summer = 1_719_792_000_123_456;
        let zones = vec![
            Some("+00:00:01"),
            Some("GMT+1"),
            Some("PST"),
            Some("invalid"),
            None,
        ];
        let zone_arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(zones.clone())),
            Arc::new(LargeStringArray::from(zones.clone())),
            Arc::new(StringViewArray::from(zones)),
        ];
        for zones in zone_arrays {
            let result = invoke(
                timestamps(vec![Some(-1), Some(0), Some(summer), None, Some(summer)]),
                ColumnarValue::Array(zones),
            )
            .unwrap();
            assert_timestamps(
                result,
                &[
                    Some(999_999),
                    Some(3_600_000_000),
                    Some(summer - 25_200_000_000),
                    None,
                    None,
                ],
            );
        }
    }

    #[test]
    fn scalar_and_array_arguments_work_across_batches() {
        for values in [vec![Some(-1), None], vec![Some(123_456), Some(-1_000_001)]] {
            let expected = values
                .iter()
                .map(|value| value.map(|value| value + 3_723_000_000))
                .collect::<Vec<_>>();
            let result =
                invoke(timestamps(values), scalar_zone(Some("UTC+1:02:03"))).unwrap();
            assert_timestamps(result, &expected);
        }

        let timestamp = 1_719_792_000_123_456;
        let result = invoke(
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(timestamp),
                Some(Arc::from("UTC")),
            )),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("GMT+1"),
                Some("PST"),
                None,
                Some("GMT+1"),
            ]))),
        )
        .unwrap();
        assert_timestamps(
            result,
            &[
                Some(timestamp + 3_600_000_000),
                Some(timestamp - 25_200_000_000),
                None,
                Some(timestamp + 3_600_000_000),
            ],
        );
    }

    #[test]
    fn fixed_offset_seconds_preserve_each_timestamp_unit() {
        for (input, expected) in [
            (
                ScalarValue::TimestampSecond(Some(-1), None),
                ScalarValue::TimestampSecond(Some(0), None),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(-1), None),
                ScalarValue::TimestampMillisecond(Some(999), None),
            ),
            (
                ScalarValue::TimestampMicrosecond(Some(-1), None),
                ScalarValue::TimestampMicrosecond(Some(999_999), None),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(-1), None),
                ScalarValue::TimestampNanosecond(Some(999_999_999), None),
            ),
        ] {
            let result = invoke(
                ColumnarValue::Scalar(input.clone()),
                scalar_zone(Some("+00:00:01")),
            )
            .unwrap();
            let ColumnarValue::Scalar(actual) = result else {
                panic!("expected scalar timestamp");
            };
            assert_eq!(actual, expected);

            let expr = SparkFromUtcTimestampExpr::new(
                Arc::new(Literal::new(input)),
                Arc::new(Literal::new(ScalarValue::Utf8(Some(
                    "+00:00:01".to_owned(),
                )))),
            );
            let result = expr
                .evaluate(&expression_batch(vec![Some(0)], vec!["0"]))
                .unwrap();
            let ColumnarValue::Scalar(actual) = result else {
                panic!("expected scalar timestamp");
            };
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn invalid_literal_timezone_is_validated_lazily() {
        let expr = SparkFromUtcTimestampExpr::new(
            Arc::new(Column::new("timestamp", 0)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some("+19:00".to_owned())))),
        );
        assert_timestamps(
            expr.evaluate(&expression_batch(vec![None, None], vec!["0", "0"]))
                .unwrap(),
            &[None, None],
        );
        let error = expr
            .evaluate(&expression_batch(vec![Some(0)], vec!["0"]))
            .unwrap_err();
        assert!(error.to_string().contains("invalid timezone '+19:00'"));
    }

    #[test]
    fn null_timezone_does_not_skip_timestamp_evaluation() {
        let expr = SparkFromUtcTimestampExpr::new(
            throwing_timestamp(),
            Arc::new(Literal::new(ScalarValue::Utf8(None))),
        );
        let error = expr
            .evaluate(&expression_batch(vec![Some(0)], vec!["0"]))
            .unwrap_err();
        assert!(error.to_string().contains("invalid"));
    }

    #[test]
    fn timezone_evaluates_only_non_null_timestamp_rows() {
        let summer = 1_719_792_000_123_456;
        let expr = SparkFromUtcTimestampExpr::new(
            Arc::new(Column::new("timestamp", 0)),
            computed_timezone(),
        );
        let batch = expression_batch(
            vec![None, Some(0), None, Some(summer)],
            vec!["invalid", "0", "invalid", "1"],
        );
        assert_timestamps(
            expr.evaluate(&batch).unwrap(),
            &[None, Some(0), None, Some(summer - 25_200_000_000)],
        );

        // The same timezone child must still throw when its timestamp is non-null.
        let error = expr
            .evaluate(&expression_batch(vec![Some(0)], vec!["invalid"]))
            .unwrap_err();
        assert!(error.to_string().contains("invalid"));
    }

    #[test]
    fn timezone_column_ignores_values_on_null_timestamp_rows() {
        let summer = 1_719_792_000_123_456;
        let expr = SparkFromUtcTimestampExpr::new(
            Arc::new(Column::new("timestamp", 0)),
            Arc::new(Column::new("value", 1)),
        );
        let batch = expression_batch(
            vec![None, Some(0), None, Some(summer)],
            vec!["invalid", "UTC", "+19:00", "PST"],
        );
        assert_timestamps(
            expr.evaluate(&batch).unwrap(),
            &[None, Some(0), None, Some(summer - 25_200_000_000)],
        );

        let error = expr
            .evaluate(&expression_batch(vec![Some(0)], vec!["+19:00"]))
            .unwrap_err();
        assert!(error.to_string().contains("invalid timezone '+19:00'"));
    }

    #[test]
    fn timezone_skips_all_null_and_scalar_null_timestamps() {
        let batch = expression_batch(vec![None, None], vec!["invalid", "invalid"]);
        let expr = SparkFromUtcTimestampExpr::new(
            Arc::new(Column::new("timestamp", 0)),
            computed_timezone(),
        );
        assert_timestamps(expr.evaluate(&batch).unwrap(), &[None, None]);

        let expr = SparkFromUtcTimestampExpr::new(
            Arc::new(Literal::new(ScalarValue::TimestampMicrosecond(None, None))),
            computed_timezone(),
        );
        assert!(matches!(
            expr.evaluate(&batch).unwrap(),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, None))
        ));
    }

    #[test]
    fn empty_batch_skips_children() {
        let batch = expression_batch(vec![], vec![]);
        let expr =
            SparkFromUtcTimestampExpr::new(throwing_timestamp(), computed_timezone());
        assert_timestamps(expr.evaluate(&batch).unwrap(), &[]);
    }

    #[test]
    fn fixed_offset_outside_calendar_range_returns_error() {
        for (timestamp, timezone) in [
            (DateTime::<Utc>::MAX_UTC.timestamp_micros(), "+00:00:01"),
            (DateTime::<Utc>::MIN_UTC.timestamp_micros(), "-00:00:01"),
        ] {
            let error = invoke(
                ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                    Some(timestamp),
                    None,
                )),
                scalar_zone(Some(timezone)),
            )
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("exceeds Chrono's calendar range")
            );
        }
    }

    #[test]
    fn invalid_zones_are_parsed_only_for_non_null_pairs() {
        let zones =
            ColumnarValue::Array(Arc::new(StringArray::from(vec!["invalid", "+19:00"])));
        assert_timestamps(
            invoke(timestamps(vec![None, None]), zones).unwrap(),
            &[None, None],
        );

        let error = invoke(timestamps(vec![None, Some(0)]), scalar_zone(Some("+19:00")))
            .unwrap_err();
        assert!(error.to_string().contains("invalid timezone '+19:00'"));

        let result = invoke(
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, None)),
            scalar_zone(Some("invalid")),
        )
        .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, None))
        ));

        let result = invoke(
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(i64::MAX),
                None,
            )),
            scalar_zone(None),
        )
        .unwrap();
        assert!(matches!(
            result,
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(None, None))
        ));

        assert_timestamps(
            invoke(timestamps(vec![]), scalar_zone(Some("invalid"))).unwrap(),
            &[],
        );
    }
}
