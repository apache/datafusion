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
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::{Expr, TableType};
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

    /// Advance to the next value in the series
    fn advance(&mut self, step: &Self::StepType) -> Result<()>;

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
                batch_size,
                include_end: *include_end,
                name,
            })),
        };

        Ok(generator)
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
        let mut buf = Vec::with_capacity(self.batch_size);

        while buf.len() < self.batch_size
            && !self
                .current
                .should_stop(self.end.clone(), &self.step, self.include_end)
        {
            buf.push(self.current.to_value_type());
            self.current.advance(&self.step)?;
        }

        if buf.is_empty() {
            return Ok(None);
        }

        let array = self.current.create_array(buf)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), vec![array])?;
        Ok(Some(batch))
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

fn validate_interval_step(
    step: IntervalMonthDayNano,
    start: i64,
    end: i64,
) -> Result<()> {
    if step.months == 0 && step.days == 0 && step.nanoseconds == 0 {
        return plan_err!("Step interval cannot be zero");
    }

    let step_is_positive = step.months > 0 || step.days > 0 || step.nanoseconds > 0;
    let step_is_negative = step.months < 0 || step.days < 0 || step.nanoseconds < 0;

    if start > end && step_is_positive {
        return plan_err!(
            "Start is bigger than end, but increment is positive: Cannot generate infinite series"
        );
    }

    if start < end && step_is_negative {
        return plan_err!(
            "Start is smaller than end, but increment is negative: Cannot generate infinite series"
        );
    }

    Ok(())
}

#[async_trait]
impl TableProvider for GenerateSeriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch_size = state.config_options().execution.batch_size;
        let generator = self.as_generator(batch_size)?;

        Ok(Arc::new(
            LazyMemoryExec::try_new(self.schema(), vec![generator])?
                .with_projection(projection.cloned()),
        ))
    }
}

#[derive(Debug)]
struct GenerateSeriesFuncImpl {
    name: &'static str,
    include_end: bool,
}

impl TableFunctionImpl for GenerateSeriesFuncImpl {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
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
            };
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

        if start > end && step > 0 {
            return plan_err!(
                "Start is bigger than end, but increment is positive: Cannot generate infinite series"
            );
        }

        if start < end && step < 0 {
            return plan_err!(
                "Start is smaller than end, but increment is negative: Cannot generate infinite series"
            );
        }

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

        // Parse start timestamp
        let (start_ts, tz) = match &exprs[0] {
            Expr::Literal(ScalarValue::TimestampNanosecond(ts, tz), _) => {
                (*ts, tz.clone())
            }
            other => {
                return plan_err!(
                    "First argument must be a timestamp or NULL, got {:?}",
                    other
                );
            }
        };

        // Parse end timestamp
        let end_ts = match &exprs[1] {
            Expr::Literal(ScalarValue::Null, _) => None,
            Expr::Literal(ScalarValue::TimestampNanosecond(ts, _), _) => *ts,
            other => {
                return plan_err!(
                    "Second argument must be a timestamp or NULL, got {:?}",
                    other
                );
            }
        };

        // Parse step interval
        let step_interval = match &exprs[2] {
            Expr::Literal(ScalarValue::Null, _) => None,
            Expr::Literal(ScalarValue::IntervalMonthDayNano(interval), _) => *interval,
            other => {
                return plan_err!(
                    "Third argument must be an interval or NULL, got {:?}",
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
        validate_interval_step(step, start, end)?;

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
            other => {
                return plan_err!(
                    "First argument must be a date or NULL, got {:?}",
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
            other => {
                return plan_err!(
                    "Second argument must be a date or NULL, got {:?}",
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
            other => {
                return plan_err!(
                    "Third argument must be an interval or NULL, got {:?}",
                    other
                );
            }
        };

        // Convert Date32 (days since epoch) to timestamp nanoseconds (nanoseconds since epoch)
        // Date32 is days since 1970-01-01, so multiply by nanoseconds per day
        const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;

        let start_ts = start_date as i64 * NANOS_PER_DAY;
        let end_ts = end_date as i64 * NANOS_PER_DAY;

        // Validate step interval
        validate_interval_step(step_interval, start_ts, end_ts)?;

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
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "generate_series",
            include_end: true,
        };
        impl_func.call(exprs)
    }
}

#[derive(Debug)]
pub struct RangeFunc {}

impl TableFunctionImpl for RangeFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let impl_func = GenerateSeriesFuncImpl {
            name: "range",
            include_end: false,
        };
        impl_func.call(exprs)
    }
}
