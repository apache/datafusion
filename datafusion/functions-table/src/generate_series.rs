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

use arrow::array::timezone::Tz;
use arrow::array::types::TimestampNanosecondType;
use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_catalog::{Session, TableFunctionArgs};
use datafusion_common::{Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use parking_lot::RwLock;
use std::any::Any;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Empty generator that produces no rows - used when series arguments contain null values
#[derive(Debug, Clone)]
pub struct Empty {
    name: &'static str,
}

impl Empty {
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl LazyBatchGenerator for Empty {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(None)
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(Empty { name: self.name }))
    }
}

impl fmt::Display for Empty {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: empty", self.name)
    }
}

/// Trait for values that can be generated in a series
pub trait SeriesValue: fmt::Debug + Clone + Send + Sync + 'static {
    type StepType: fmt::Debug + Clone + Send + Sync;
    type ValueType: fmt::Debug + Clone + Send + Sync;

    /// Check if we've reached the end of the series
    fn should_stop(&self, end: Self, step: &Self::StepType, include_end: bool) -> bool;

    /// Advance to the next value in the series.
    fn advance(&mut self, step: &Self::StepType) -> Result<()>;

    /// Advance to the next value, adjusting the end of the series if needed.
    ///
    /// The default implementation preserves the behavior of [`Self::advance`].
    /// Implementations can override this method when they need to handle an
    /// overflow by terminating the series after the current value.
    fn advance_with_end(&mut self, _end: &mut Self, step: &Self::StepType) -> Result<()> {
        self.advance(step)
    }

    /// Create an Arrow array from a vector of values
    fn create_array(&self, values: Vec<Self::ValueType>) -> Result<ArrayRef>;

    /// Convert self to ValueType for array creation
    fn to_value_type(&self) -> Self::ValueType;

    /// Display the value for debugging
    fn display_value(&self) -> String;
}

impl SeriesValue for i64 {
    type StepType = i64;
    type ValueType = i64;

    fn should_stop(&self, end: Self, step: &Self::StepType, include_end: bool) -> bool {
        reach_end_int64(*self, end, *step, include_end)
    }

    fn advance(&mut self, step: &Self::StepType) -> Result<()> {
        *self += step;
        Ok(())
    }

    fn advance_with_end(&mut self, end: &mut Self, step: &Self::StepType) -> Result<()> {
        if let Some(next) = self.checked_add(*step) {
            *self = next;
        } else {
            // Advancing would overflow: clamp `end` so the series stops after
            // the current (last reachable) value instead of panicking or
            // wrapping around.
            *end = if *step > 0 {
                self.saturating_sub(1)
            } else {
                self.saturating_add(1)
            };
        }
        Ok(())
    }

    fn create_array(&self, values: Vec<Self::ValueType>) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from(values)))
    }

    fn to_value_type(&self) -> Self::ValueType {
        *self
    }

    fn display_value(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct TimestampValue {
    value: i64,
    parsed_tz: Option<Tz>,
    tz_str: Option<Arc<str>>,
}

impl TimestampValue {
    pub fn value(&self) -> i64 {
        self.value
    }

    pub fn tz_str(&self) -> Option<&Arc<str>> {
        self.tz_str.as_ref()
    }
}

impl SeriesValue for TimestampValue {
    type StepType = IntervalMonthDayNano;
    type ValueType = i64;

    fn should_stop(&self, end: Self, step: &Self::StepType, include_end: bool) -> bool {
        let step_negative = step.months < 0 || step.days < 0 || step.nanoseconds < 0;

        if include_end {
            if step_negative {
                self.value < end.value
            } else {
                self.value > end.value
            }
        } else if step_negative {
            self.value <= end.value
        } else {
            self.value >= end.value
        }
    }

    fn advance(&mut self, step: &Self::StepType) -> Result<()> {
        let tz = self
            .parsed_tz
            .unwrap_or_else(|| Tz::from_str("+00:00").unwrap());
        let Some(next_ts) =
            TimestampNanosecondType::add_month_day_nano(self.value, *step, tz)
        else {
            return plan_err!(
                "Failed to add interval {:?} to timestamp {}",
                step,
                self.value
            );
        };
        self.value = next_ts;
        Ok(())
    }

    fn advance_with_end(&mut self, end: &mut Self, step: &Self::StepType) -> Result<()> {
        let tz = self
            .parsed_tz
            .unwrap_or_else(|| Tz::from_str("+00:00").unwrap());
        if let Some(next_ts) =
            TimestampNanosecondType::add_month_day_nano(self.value, *step, tz)
        {
            self.value = next_ts;
        } else {
            // Advancing would exceed the timestamp range. Clamp `end` so the
            // series terminates after the current (last reachable) value.
            let step_negative = step.months < 0 || step.days < 0 || step.nanoseconds < 0;
            end.value = if step_negative {
                self.value.saturating_add(1)
            } else {
                self.value.saturating_sub(1)
            };
        }
        Ok(())
    }

    fn create_array(&self, values: Vec<Self::ValueType>) -> Result<ArrayRef> {
        let array = TimestampNanosecondArray::from(values);

        // Use timezone from self (now we have access to tz through &self)
        let array = match self.tz_str.as_ref() {
            Some(tz_str) => array.with_timezone(Arc::clone(tz_str)),
            None => array,
        };

        Ok(Arc::new(array))
    }

    fn to_value_type(&self) -> Self::ValueType {
        self.value
    }

    fn display_value(&self) -> String {
        self.value.to_string()
    }
}

/// Indicates the arguments used for generating a series.
#[derive(Debug, Clone)]
pub enum GenSeriesArgs {
    /// ContainsNull signifies that at least one argument(start, end, step) was null, thus no series will be generated.
    ContainsNull { name: &'static str },
    /// Int64Args holds the start, end, and step values for generating integer series when all arguments are not null.
    Int64Args {
        start: i64,
        end: i64,
        step: i64,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
    /// TimestampArgs holds the start, end, and step values for generating timestamp series when all arguments are not null.
    TimestampArgs {
        start: i64,
        end: i64,
        step: IntervalMonthDayNano,
        tz: Option<Arc<str>>,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
    /// DateArgs holds the start, end, and step values for generating date series when all arguments are not null.
    /// Internally, dates are converted to timestamps and use the timestamp logic.
    DateArgs {
        start: i64,
        end: i64,
        step: IntervalMonthDayNano,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
}

/// Table that generates a series of integers/timestamps from `start`(inclusive) to `end`, incrementing by step
#[derive(Debug, Clone)]
pub struct GenerateSeriesTable {
    schema: SchemaRef,
    args: GenSeriesArgs,
}

impl GenerateSeriesTable {
    pub fn new(schema: SchemaRef, args: GenSeriesArgs) -> Self {
        Self { schema, args }
    }

    pub fn as_generator(
        &self,
        batch_size: usize,
    ) -> Result<Arc<RwLock<dyn LazyBatchGenerator>>> {
        if batch_size == 0 {
            return plan_err!("GenerateSeriesTable: batch_size must be greater than 0");
        }
        let generator: Arc<RwLock<dyn LazyBatchGenerator>> = match &self.args {
            GenSeriesArgs::ContainsNull { name } => Arc::new(RwLock::new(Empty { name })),
            GenSeriesArgs::Int64Args {
                start,
                end,
                step,
                include_end,
                name,
            } => Arc::new(RwLock::new(GenericSeriesState {
                schema: self.schema(),
                start: *start,
                end: *end,
                step: *step,
                current: *start,
                finished: false,
                batch_size,
                include_end: *include_end,
                name,
            })),
            GenSeriesArgs::TimestampArgs {
                start,
                end,
                step,
                tz,
                include_end,
                name,
            } => {
                let parsed_tz = tz
                    .as_ref()
                    .map(|s| Tz::from_str(s.as_ref()))
                    .transpose()
                    .map_err(|e| {
                        datafusion_common::internal_datafusion_err!(
                            "Failed to parse timezone: {e}"
                        )
                    })?
                    .unwrap_or_else(|| Tz::from_str("+00:00").unwrap());
                Arc::new(RwLock::new(GenericSeriesState {
                    schema: self.schema(),
                    start: TimestampValue {
                        value: *start,
                        parsed_tz: Some(parsed_tz),
                        tz_str: tz.clone(),
                    },
                    end: TimestampValue {
                        value: *end,
                        parsed_tz: Some(parsed_tz),
                        tz_str: tz.clone(),
                    },
                    step: *step,
                    current: TimestampValue {
                        value: *start,
                        parsed_tz: Some(parsed_tz),
                        tz_str: tz.clone(),
                    },
                    finished: false,
                    batch_size,
                    include_end: *include_end,
                    name,
                }))
            }
            GenSeriesArgs::DateArgs {
                start,
                end,
                step,
                include_end,
                name,
            } => Arc::new(RwLock::new(GenericSeriesState {
                schema: self.schema(),
                start: TimestampValue {
                    value: *start,
                    parsed_tz: None,
                    tz_str: None,
                },
                end: TimestampValue {
                    value: *end,
                    parsed_tz: None,
                    tz_str: None,
                },
                step: *step,
                current: TimestampValue {
                    value: *start,
                    parsed_tz: None,
                    tz_str: None,
                },
                finished: false,
                batch_size,
                include_end: *include_end,
                name,
            })),
        };

        Ok(generator)
    }

    /// Detects output sort order to potentially remove `SortExec`.
    /// Only the `Int64` argument type is currently supported.
    fn output_ordering(&self, schema: &Schema) -> Option<PhysicalSortExpr> {
        let step = match &self.args {
            GenSeriesArgs::Int64Args { step, .. } => *step,
            _ => return None,
        };

        if schema.fields().is_empty() {
            return None;
        }

        let descending = step < 0;
        Some(PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(0).name(), 0)),
            SortOptions {
                descending,
                // this table function won't output nulls, so either is fine
                nulls_first: false,
            },
        ))
    }
}

#[derive(Debug, Clone)]
pub struct GenericSeriesState<T: SeriesValue> {
    schema: SchemaRef,
    start: T,
    end: T,
    step: T::StepType,
    batch_size: usize,
    current: T,
    finished: bool,
    include_end: bool,
    name: &'static str,
}

impl<T: SeriesValue> GenericSeriesState<T> {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn include_end(&self) -> bool {
        self.include_end
    }

    pub fn start(&self) -> &T {
        &self.start
    }

    pub fn end(&self) -> &T {
        &self.end
    }

    pub fn step(&self) -> &T::StepType {
        &self.step
    }

    pub fn current(&self) -> &T {
        &self.current
    }
}

impl<T: SeriesValue> LazyBatchGenerator for GenericSeriesState<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        let mut buf = Vec::with_capacity(self.batch_size);

        while buf.len() < self.batch_size
            && !self
                .current
                .should_stop(self.end.clone(), &self.step, self.include_end)
        {
            buf.push(self.current.to_value_type());
            if self
                .current
                .should_stop(self.end.clone(), &self.step, false)
            {
                self.finished = true;
                break;
            }

            let original_end = self.end.clone();
            self.current.advance_with_end(&mut self.end, &self.step)?;
            if self
                .current
                .should_stop(self.end.clone(), &self.step, self.include_end)
            {
                self.end = original_end;
                self.finished = true;
                break;
            }
        }

        if buf.is_empty() {
            return Ok(None);
        }

        let array = self.current.create_array(buf)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), vec![array])?;
        Ok(Some(batch))
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        let mut new = self.clone();
        new.current = new.start.clone();
        new.finished = false;
        Arc::new(RwLock::new(new))
    }
}

impl<T: SeriesValue> fmt::Display for GenericSeriesState<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: start={}, end={}, batch_size={}",
            self.name,
            self.start.display_value(),
            self.end.display_value(),
            self.batch_size
        )
    }
}

fn reach_end_int64(val: i64, end: i64, step: i64, include_end: bool) -> bool {
    if step > 0 {
        if include_end { val > end } else { val >= end }
    } else if include_end {
        val < end
    } else {
        val <= end
    }
}

/// `i64::MIN` nanoseconds since the Unix epoch, rendered as a timestamp.
const NANOS_RANGE_MIN: &str = "1677-09-21T00:12:43.145224192";
/// `i64::MAX` nanoseconds since the Unix epoch, rendered as a timestamp.
const NANOS_RANGE_MAX: &str = "2262-04-11T23:47:16.854775807";

/// Read one timestamp argument of `generate_series`/`range`, normalized to
/// nanoseconds since the Unix epoch, along with its timezone.
///
/// All four Arrow [`TimeUnit`]s are accepted. The series is always produced as
/// `Timestamp(Nanosecond, _)`, so coarser units are widened here rather than
/// rejected.
///
/// `Second`, `Millisecond` and `Microsecond` cover a far wider span than an
/// `i64` of nanoseconds (roughly 1677 to 2262), so the widening is a checked
/// multiplication: an input outside the nanosecond window is a planning error
/// naming the offending value, never a panic in debug builds or a silent wrap
/// in release builds.
///
/// A NULL argument yields `Ok((None, tz))`; the caller turns that into an
/// empty series.
fn timestamp_arg_to_nanos(
    expr: &Expr,
    arg_desc: &str,
    name: &str,
) -> Result<(Option<i64>, Option<Arc<str>>)> {
    let Expr::Literal(scalar, _) = expr else {
        return plan_err!(
            "{arg_desc} for {name} must be a literal TIMESTAMP or NULL, got {expr}"
        );
    };

    // Nanoseconds per unit of the argument's `TimeUnit`.
    let (value, nanos_per_unit, tz) = match scalar {
        // An untyped NULL carries no timezone, and the series is empty anyway.
        ScalarValue::Null => return Ok((None, None)),
        ScalarValue::TimestampSecond(v, tz) => (v, 1_000_000_000i64, tz),
        ScalarValue::TimestampMillisecond(v, tz) => (v, 1_000_000, tz),
        ScalarValue::TimestampMicrosecond(v, tz) => (v, 1_000, tz),
        ScalarValue::TimestampNanosecond(v, tz) => (v, 1, tz),
        other => {
            return plan_err!(
                "{arg_desc} for {name} must be a TIMESTAMP or NULL, got {:?}",
                other.data_type()
            );
        }
    };

    let Some(value) = value else {
        return Ok((None, tz.clone()));
    };

    let nanos = value.checked_mul(nanos_per_unit).ok_or_else(|| {
        plan_datafusion_err!(
            "{arg_desc} for {name} is out of range of nanosecond timestamps: \
             {value} ({:?}) is outside {NANOS_RANGE_MIN} to {NANOS_RANGE_MAX}",
            scalar.data_type()
        )
    })?;

    Ok((Some(nanos), tz.clone()))
}

fn validate_interval_step(step: IntervalMonthDayNano) -> Result<()> {
    if step.months == 0 && step.days == 0 && step.nanoseconds == 0 {
        return plan_err!("Step interval cannot be zero");
    }

    Ok(())
}

#[async_trait]
impl TableProvider for GenerateSeriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = state.config_options().execution.batch_size.get();
        let generator = self.as_generator(batch_size)?;
        let mut exec = LazyMemoryExec::try_new(self.schema(), vec![generator])?
            .with_projection(projection.map(|p| p.to_vec()));

        if let Some(ordering) = self.output_ordering(exec.schema().as_ref()) {
            exec.add_ordering([ordering]);
        }

        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct GenerateSeriesFuncImpl {
    name: &'static str,
    include_end: bool,
}

impl TableFunctionImpl for GenerateSeriesFuncImpl {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.is_empty() || exprs.len() > 3 {
            return plan_err!("{} function requires 1 to 3 arguments", self.name);
        }

        // Determine the data type from the first argument
        match &exprs[0] {
            Expr::Literal(
                // Default to int64 for null
                ScalarValue::Null | ScalarValue::Int64(_),
                _,
            ) => self.call_int64(exprs),
            Expr::Literal(s, _) if matches!(s.data_type(), DataType::Timestamp(_, _)) => {
                self.call_timestamp(exprs)
            }
            Expr::Literal(s, _) if matches!(s.data_type(), DataType::Date32) => {
                self.call_date(exprs)
            }
            Expr::Literal(scalar, _) => {
                plan_err!(
                    "Argument #1 must be an INTEGER, TIMESTAMP, DATE or NULL, got {:?}",
                    scalar.data_type()
                )
            }
            _ => plan_err!("Arguments must be literals"),
        }
    }
}

impl GenerateSeriesFuncImpl {
    fn call_int64(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let mut normalize_args = Vec::new();
        for (expr_index, expr) in exprs.iter().enumerate() {
            match expr {
                Expr::Literal(ScalarValue::Null, _) => {}
                Expr::Literal(ScalarValue::Int64(Some(n)), _) => normalize_args.push(*n),
                other => {
                    return plan_err!(
                        "Argument #{} must be an INTEGER or NULL, got {:?}",
                        expr_index + 1,
                        other
                    );
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        if normalize_args.len() != exprs.len() {
            // contain null
            return Ok(Arc::new(GenerateSeriesTable {
                schema,
                args: GenSeriesArgs::ContainsNull { name: self.name },
            }));
        }

        let (start, end, step) = match &normalize_args[..] {
            [end] => (0, *end, 1),
            [start, end] => (*start, *end, 1),
            [start, end, step] => (*start, *end, *step),
            _ => {
                return plan_err!("{} function requires 1 to 3 arguments", self.name);
            }
        };

        if step == 0 {
            return plan_err!("Step cannot be zero");
        }

        Ok(Arc::new(GenerateSeriesTable {
            schema,
            args: GenSeriesArgs::Int64Args {
                start,
                end,
                step,
                include_end: self.include_end,
                name: self.name,
            },
        }))
    }

    fn call_timestamp(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "{} function with timestamps requires exactly 3 arguments",
                self.name
            );
        }

        // Parse the start and end timestamps.
        //
        // Both are widened to nanoseconds, so the two arguments do not have to
        // agree on a `TimeUnit`: an Arrow timestamp denotes an instant
        // regardless of the unit it happens to be stored in, and the output is
        // nanoseconds either way (see the schema below).
        let (start_ts, tz) =
            timestamp_arg_to_nanos(&exprs[0], "First argument", self.name)?;
        let (end_ts, _end_tz) =
            timestamp_arg_to_nanos(&exprs[1], "Second argument", self.name)?;

        // `_end_tz` is deliberately discarded: the output timezone comes from
        // the start argument alone. A timezone on an Arrow timestamp does not
        // change which instant it denotes, only how that instant is rendered,
        // so a start and end carrying different timezones are still directly
        // comparable once both are nanoseconds since the epoch -- there is
        // nothing to reject. The start's zone is the one that is kept because
        // it also anchors the calendar arithmetic that advances the series:
        // month and day components of the step are applied in local time, so
        // they follow that zone's DST rules.

        // Parse step interval
        let step_interval = match &exprs[2] {
            Expr::Literal(ScalarValue::Null, _) => None,
            Expr::Literal(ScalarValue::IntervalMonthDayNano(interval), _) => *interval,
            Expr::Literal(scalar, _) => {
                return plan_err!(
                    "Third argument for {} must be an INTERVAL or NULL, got {:?}",
                    self.name,
                    scalar.data_type()
                );
            }
            other => {
                return plan_err!(
                    "Third argument for {} must be a literal INTERVAL or NULL, got {}",
                    self.name,
                    other
                );
            }
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            false,
        )]));

        // Check if any argument is null
        let (Some(start), Some(end), Some(step)) = (start_ts, end_ts, step_interval)
        else {
            return Ok(Arc::new(GenerateSeriesTable {
                schema,
                args: GenSeriesArgs::ContainsNull { name: self.name },
            }));
        };

        // Validate step interval
        validate_interval_step(step)?;

        Ok(Arc::new(GenerateSeriesTable {
            schema,
            args: GenSeriesArgs::TimestampArgs {
                start,
                end,
                step,
                tz,
                include_end: self.include_end,
                name: self.name,
            },
        }))
    }

    fn call_date(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "{} function with dates requires exactly 3 arguments",
                self.name
            );
        }

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        // Parse start date
        let start_date = match &exprs[0] {
            Expr::Literal(ScalarValue::Date32(Some(date)), _) => *date,
            Expr::Literal(ScalarValue::Date32(None), _)
            | Expr::Literal(ScalarValue::Null, _) => {
                return Ok(Arc::new(GenerateSeriesTable {
                    schema,
                    args: GenSeriesArgs::ContainsNull { name: self.name },
                }));
            }
            Expr::Literal(scalar, _) => {
                return plan_err!(
                    "First argument for {} must be a DATE or NULL, got {:?}",
                    self.name,
                    scalar.data_type()
                );
            }
            other => {
                return plan_err!(
                    "First argument for {} must be a literal DATE or NULL, got {}",
                    self.name,
                    other
                );
            }
        };

        // Parse end date
        let end_date = match &exprs[1] {
            Expr::Literal(ScalarValue::Date32(Some(date)), _) => *date,
            Expr::Literal(ScalarValue::Date32(None), _)
            | Expr::Literal(ScalarValue::Null, _) => {
                return Ok(Arc::new(GenerateSeriesTable {
                    schema,
                    args: GenSeriesArgs::ContainsNull { name: self.name },
                }));
            }
            Expr::Literal(scalar, _) => {
                return plan_err!(
                    "Second argument for {} must be a DATE or NULL, got {:?}",
                    self.name,
                    scalar.data_type()
                );
            }
            other => {
                return plan_err!(
                    "Second argument for {} must be a literal DATE or NULL, got {}",
                    self.name,
                    other
                );
            }
        };

        // Parse step interval
        let step_interval = match &exprs[2] {
            Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(interval)), _) => {
                *interval
            }
            Expr::Literal(ScalarValue::IntervalMonthDayNano(None), _)
            | Expr::Literal(ScalarValue::Null, _) => {
                return Ok(Arc::new(GenerateSeriesTable {
                    schema,
                    args: GenSeriesArgs::ContainsNull { name: self.name },
                }));
            }
            Expr::Literal(scalar, _) => {
                return plan_err!(
                    "Third argument for {} must be an INTERVAL or NULL, got {:?}",
                    self.name,
                    scalar.data_type()
                );
            }
            other => {
                return plan_err!(
                    "Third argument for {} must be a literal INTERVAL or NULL, got {}",
                    self.name,
                    other
                );
            }
        };

        // Convert Date32 (days since epoch) to timestamp nanoseconds (nanoseconds since epoch)
        // Date32 is days since 1970-01-01, so multiply by nanoseconds per day
        const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;

        // Dates outside the nanosecond timestamp range (1677-09-21 to
        // 2262-04-11) cannot be represented; return an error instead of
        // panicking (debug) or silently wrapping (release).
        let date_to_ts_nanos = |date: i32, arg: &str| {
            (date as i64).checked_mul(NANOS_PER_DAY).ok_or_else(|| {
                plan_datafusion_err!(
                    "{arg} for {} is out of range of nanosecond timestamps",
                    self.name
                )
            })
        };

        let start_ts = date_to_ts_nanos(start_date, "First argument")?;
        let end_ts = date_to_ts_nanos(end_date, "Second argument")?;

        // Validate step interval
        validate_interval_step(step_interval)?;

        Ok(Arc::new(GenerateSeriesTable {
            schema,
            args: GenSeriesArgs::DateArgs {
                start: start_ts,
                end: end_ts,
                step: step_interval,
                include_end: self.include_end,
                name: self.name,
            },
        }))
    }
}

#[derive(Debug)]
pub struct GenerateSeriesFunc {}

impl TableFunctionImpl for GenerateSeriesFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "generate_series",
            include_end: true,
        };
        impl_func.call_with_args(args)
    }
}

#[derive(Debug)]
pub struct RangeFunc {}

impl TableFunctionImpl for RangeFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "range",
            include_end: false,
        };
        impl_func.call_with_args(args)
    }
}

#[cfg(test)]
mod generate_series_tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::{
        DataType, Field, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit,
    };
    use datafusion_catalog::TableProvider;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Expr;
    use datafusion_physical_plan::memory::LazyBatchGenerator;

    use crate::generate_series::{
        GenSeriesArgs, GenerateSeriesFuncImpl, GenerateSeriesTable, GenericSeriesState,
        timestamp_arg_to_nanos,
    };

    /// Nanoseconds in a day, for readable expectations below.
    const DAY_NANOS: i64 = 24 * 60 * 60 * 1_000_000_000;

    fn lit(scalar: ScalarValue) -> Expr {
        Expr::Literal(scalar, None)
    }

    /// `2024-01-01T00:00:00Z` in seconds since the epoch.
    const JAN_1_2024_SECS: i64 = 1_704_067_200;

    fn tz(s: &str) -> Option<Arc<str>> {
        Some(Arc::from(s))
    }

    fn generate_series_impl() -> GenerateSeriesFuncImpl {
        GenerateSeriesFuncImpl {
            name: "generate_series",
            include_end: true,
        }
    }

    fn range_impl() -> GenerateSeriesFuncImpl {
        GenerateSeriesFuncImpl {
            name: "range",
            include_end: false,
        }
    }

    /// Run `call_timestamp` and return the resulting table's schema and args.
    fn call_timestamp(
        func: &GenerateSeriesFuncImpl,
        start: ScalarValue,
        end: ScalarValue,
        step_days: i32,
    ) -> Result<(SchemaRef, GenSeriesArgs)> {
        let exprs = vec![
            lit(start),
            lit(end),
            lit(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(0, step_days, 0),
            ))),
        ];
        let provider = func.call_timestamp(&exprs)?;
        let table = (provider.as_ref() as &dyn Any)
            .downcast_ref::<GenerateSeriesTable>()
            .expect("call_timestamp returns a GenerateSeriesTable");
        Ok((table.schema(), table.args.clone()))
    }

    fn timestamp_args(args: &GenSeriesArgs) -> (i64, i64, Option<Arc<str>>) {
        match args {
            GenSeriesArgs::TimestampArgs { start, end, tz, .. } => {
                (*start, *end, tz.clone())
            }
            other => panic!("expected TimestampArgs, got {other:?}"),
        }
    }

    /// Every `TimeUnit` is accepted and widened to nanoseconds, keeping its
    /// timezone. Regression test for
    /// <https://github.com/apache/datafusion/issues/25169>.
    #[test]
    fn timestamp_arg_accepts_all_time_units() -> Result<()> {
        let cases = [
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), tz("+02:00")),
            ScalarValue::TimestampMillisecond(
                Some(JAN_1_2024_SECS * 1_000),
                tz("+02:00"),
            ),
            ScalarValue::TimestampMicrosecond(
                Some(JAN_1_2024_SECS * 1_000_000),
                tz("+02:00"),
            ),
            ScalarValue::TimestampNanosecond(
                Some(JAN_1_2024_SECS * 1_000_000_000),
                tz("+02:00"),
            ),
        ];

        for scalar in cases {
            let described = format!("{:?}", scalar.data_type());
            let (value, zone) = timestamp_arg_to_nanos(
                &lit(scalar),
                "First argument",
                "generate_series",
            )?;
            assert_eq!(
                value,
                Some(JAN_1_2024_SECS * 1_000_000_000),
                "unexpected value for {described}"
            );
            assert_eq!(zone, tz("+02:00"), "unexpected timezone for {described}");
        }

        Ok(())
    }

    /// Naive (timezone-less) timestamps stay naive.
    #[test]
    fn timestamp_arg_keeps_naive_timestamps_naive() -> Result<()> {
        let (value, zone) = timestamp_arg_to_nanos(
            &lit(ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), None)),
            "First argument",
            "generate_series",
        )?;
        assert_eq!(value, Some(JAN_1_2024_SECS * 1_000_000_000));
        assert_eq!(zone, None);
        Ok(())
    }

    /// A NULL of any precision yields no value, but still carries its
    /// timezone so the output schema keeps it. An untyped NULL has none.
    #[test]
    fn timestamp_arg_handles_nulls() -> Result<()> {
        for scalar in [
            ScalarValue::TimestampSecond(None, tz("UTC")),
            ScalarValue::TimestampMillisecond(None, tz("UTC")),
            ScalarValue::TimestampMicrosecond(None, tz("UTC")),
            ScalarValue::TimestampNanosecond(None, tz("UTC")),
        ] {
            let (value, zone) =
                timestamp_arg_to_nanos(&lit(scalar), "First argument", "range")?;
            assert_eq!(value, None);
            assert_eq!(zone, tz("UTC"));
        }

        let (value, zone) =
            timestamp_arg_to_nanos(&lit(ScalarValue::Null), "Second argument", "range")?;
        assert_eq!(value, None);
        assert_eq!(zone, None);

        Ok(())
    }

    /// Coarse units span far more than an `i64` of nanoseconds, so the
    /// widening is checked in both directions rather than wrapping.
    #[test]
    fn timestamp_arg_overflow_boundary() -> Result<()> {
        // Largest/smallest second, millisecond and microsecond values that
        // still fit in an i64 of nanoseconds, and the first ones that do not.
        type TimestampCtor = fn(Option<i64>, Option<Arc<str>>) -> ScalarValue;
        let cases: [(TimestampCtor, i64); 3] = [
            (ScalarValue::TimestampSecond, 1_000_000_000),
            (ScalarValue::TimestampMillisecond, 1_000_000),
            (ScalarValue::TimestampMicrosecond, 1_000),
        ];

        for (ctor, nanos_per_unit) in cases {
            let max_ok = i64::MAX / nanos_per_unit;
            let min_ok = i64::MIN / nanos_per_unit;

            for value in [max_ok, min_ok] {
                let (nanos, _) = timestamp_arg_to_nanos(
                    &lit(ctor(Some(value), None)),
                    "First argument",
                    "generate_series",
                )?;
                assert_eq!(nanos, Some(value * nanos_per_unit));
            }

            for (value, direction) in [(max_ok + 1, "above"), (min_ok - 1, "below")] {
                let err = timestamp_arg_to_nanos(
                    &lit(ctor(Some(value), None)),
                    "First argument",
                    "generate_series",
                )
                .expect_err(&format!("{value} is {direction} the nanosecond range"))
                .to_string();
                assert!(
                    err.contains(
                        "First argument for generate_series is out of range of \
                         nanosecond timestamps"
                    ) && err.contains(&value.to_string())
                        && err.contains("1677-09-21T00:12:43.145224192"),
                    "unexpected error: {err}"
                );
            }
        }

        Ok(())
    }

    /// The start and end arguments are read independently, so they may use
    /// different precisions; both are widened to the same nanosecond scale.
    #[test]
    fn call_timestamp_accepts_mixed_precisions() -> Result<()> {
        let (schema, args) = call_timestamp(
            &generate_series_impl(),
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), None),
            ScalarValue::TimestampMicrosecond(
                Some((JAN_1_2024_SECS + 2 * 86_400) * 1_000_000),
                None,
            ),
            1,
        )?;

        let (start, end, zone) = timestamp_args(&args);
        assert_eq!(start, JAN_1_2024_SECS * 1_000_000_000);
        assert_eq!(end, start + 2 * DAY_NANOS);
        assert_eq!(zone, None);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        Ok(())
    }

    /// `range` (exclusive end) shares the same code path.
    #[test]
    fn call_timestamp_range_accepts_non_nanosecond_precision() -> Result<()> {
        let (_, args) = call_timestamp(
            &range_impl(),
            ScalarValue::TimestampMillisecond(Some(JAN_1_2024_SECS * 1_000), None),
            ScalarValue::TimestampMillisecond(
                Some((JAN_1_2024_SECS + 86_400) * 1_000),
                None,
            ),
            1,
        )?;

        match args {
            GenSeriesArgs::TimestampArgs {
                include_end, name, ..
            } => {
                assert!(!include_end);
                assert_eq!(name, "range");
            }
            other => panic!("expected TimestampArgs, got {other:?}"),
        }

        Ok(())
    }

    /// Both bounds denote instants, so differing timezones are not an error.
    /// The output keeps the start's zone.
    #[test]
    fn call_timestamp_takes_timezone_from_start() -> Result<()> {
        let (schema, args) = call_timestamp(
            &generate_series_impl(),
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), tz("+05:00")),
            ScalarValue::TimestampSecond(
                Some(JAN_1_2024_SECS + 86_400),
                tz("America/New_York"),
            ),
            1,
        )?;

        let (start, end, zone) = timestamp_args(&args);
        assert_eq!(zone, tz("+05:00"));
        assert_eq!(end - start, DAY_NANOS);
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, tz("+05:00"))
        );

        Ok(())
    }

    /// A NULL bound of any precision produces an empty series, keeping the
    /// start's timezone in the output schema.
    #[test]
    fn call_timestamp_null_bound_is_empty_series() -> Result<()> {
        let (schema, args) = call_timestamp(
            &generate_series_impl(),
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), tz("UTC")),
            ScalarValue::TimestampMicrosecond(None, None),
            1,
        )?;

        assert!(matches!(args, GenSeriesArgs::ContainsNull { .. }));
        assert_eq!(
            schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, tz("UTC"))
        );

        Ok(())
    }

    /// An out-of-range bound is a planning error naming the argument, the
    /// offending value and the representable range -- not a panic or a wrap.
    #[test]
    fn call_timestamp_reports_out_of_range_bound() {
        let err = call_timestamp(
            &generate_series_impl(),
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), None),
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
            1,
        )
        .expect_err("i64::MAX seconds is not a nanosecond timestamp")
        .to_string();

        assert!(
            err.contains(
                "Second argument for generate_series is out of range of nanosecond \
                 timestamps"
            ) && err.contains("9223372036854775807")
                && err.contains("Timestamp(Second, None)")
                && err.contains("2262-04-11T23:47:16.854775807"),
            "unexpected error: {err}"
        );
    }

    /// The rejection message names the offending type instead of dumping the
    /// whole expression.
    #[test]
    fn call_timestamp_rejects_non_timestamp_bound() {
        let err = call_timestamp(
            &generate_series_impl(),
            ScalarValue::TimestampSecond(Some(JAN_1_2024_SECS), None),
            ScalarValue::Int64(Some(5)),
            1,
        )
        .expect_err("an integer is not a timestamp bound")
        .to_string();

        assert!(
            err.contains(
                "Second argument for generate_series must be a TIMESTAMP or NULL, \
                 got Int64"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn generate_series_rejects_zero_batch_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let table = GenerateSeriesTable::new(
            schema,
            GenSeriesArgs::Int64Args {
                start: 1,
                end: 2,
                step: 1,
                include_end: true,
                name: "generate_series",
            },
        );

        assert!(table.as_generator(0).is_err());
    }

    #[test]
    fn test_generic_series_state_reset() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let mut state = GenericSeriesState::<i64> {
            schema,
            start: 1,
            end: 5,
            step: 1,
            current: 1,
            finished: false,
            batch_size: 8192,
            include_end: true,
            name: "test",
        };
        let batch = state.generate_next_batch()?.expect("missing batch");

        let state_reset = state.reset_state();
        let reset_batch = state_reset
            .write()
            .generate_next_batch()?
            .expect("missing reset batch");

        assert_eq!(batch, reset_batch);

        Ok(())
    }

    #[test]
    fn test_generic_series_state_reset_after_overflow() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let mut state = GenericSeriesState::<i64> {
            schema,
            start: i64::MAX - 1,
            end: i64::MAX,
            step: 2,
            current: i64::MAX - 1,
            finished: false,
            batch_size: 8192,
            include_end: true,
            name: "test",
        };
        let batch = state.generate_next_batch()?.expect("missing batch");
        assert!(state.generate_next_batch()?.is_none());

        let state_reset = state.reset_state();
        let reset_batch = state_reset
            .write()
            .generate_next_batch()?
            .expect("missing reset batch");

        assert_eq!(batch, reset_batch);

        Ok(())
    }
}
