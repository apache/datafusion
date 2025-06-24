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
use arrow::array::{Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{
    DataType, Field, IntervalMonthDayNano, Schema, SchemaRef, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::ExecutionPlan;
use parking_lot::RwLock;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

/// Indicates the arguments used for generating a series.
#[derive(Debug, Clone)]
enum GenSeriesArgs {
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
}

/// Table that generates a series of integers/timestamps from `start`(inclusive) to `end`, incrementing by step
#[derive(Debug, Clone)]
struct GenerateSeriesTable {
    schema: SchemaRef,
    args: GenSeriesArgs,
}

/// Table state that generates a series of values from `start`(inclusive) to `end`, incrementing by step
#[derive(Debug, Clone)]
enum GenerateSeriesState {
    Int64 {
        schema: SchemaRef,
        start: i64, // Kept for display
        end: i64,
        step: i64,
        batch_size: usize,
        /// Tracks current position when generating table
        current: i64,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
    Timestamp {
        schema: SchemaRef,
        start: i64,
        end: i64,
        step: IntervalMonthDayNano,
        tz: Option<Arc<str>>,
        parsed_tz: Tz,
        batch_size: usize,
        /// Tracks current position when generating table
        current: i64,
        /// Indicates whether the end value should be included in the series.
        include_end: bool,
        name: &'static str,
    },
    Empty {
        batch_size: usize,
        name: &'static str,
    },
}

/// Detail to display for 'Explain' plan
impl fmt::Display for GenerateSeriesState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GenerateSeriesState::Int64 {
                name,
                start,
                end,
                batch_size,
                ..
            }
            | GenerateSeriesState::Timestamp {
                name,
                start,
                end,
                batch_size,
                ..
            } => {
                write!(
                    f,
                    "{name}: start={start}, end={end}, batch_size={batch_size}"
                )
            }
            GenerateSeriesState::Empty {
                name, batch_size, ..
            } => {
                write!(f, "{name}: empty, batch_size={batch_size}")
            }
        }
    }
}

impl LazyBatchGenerator for GenerateSeriesState {
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self {
            GenerateSeriesState::Int64 {
                schema,
                end,
                step,
                batch_size,
                current,
                include_end,
                ..
            } => {
                let mut buf = Vec::with_capacity(*batch_size);
                let end_val = *end;
                let step_val = *step;
                let include_end_val = *include_end;

                while buf.len() < *batch_size
                    && !reach_end_int64(*current, end_val, step_val, include_end_val)
                {
                    buf.push(*current);
                    *current += step_val;
                }
                let array = Int64Array::from(buf);

                if array.is_empty() {
                    return Ok(None);
                }

                let batch =
                    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(array)])?;
                Ok(Some(batch))
            }
            GenerateSeriesState::Timestamp {
                schema,
                end,
                step,
                tz,
                parsed_tz,
                batch_size,
                current,
                include_end,
                start: _,
                name: _,
            } => {
                let mut buf = Vec::with_capacity(*batch_size);
                let step_val = *step;
                let include_end_val = *include_end;
                let step_negative =
                    step_val.months < 0 || step_val.days < 0 || step_val.nanoseconds < 0;

                while buf.len() < *batch_size {
                    let should_stop = if include_end_val {
                        if step_negative {
                            current < end
                        } else {
                            current > end
                        }
                    } else if step_negative {
                        current <= end
                    } else {
                        current >= end
                    };

                    if should_stop {
                        break;
                    }

                    // Store current value before advancing
                    let current_value = *current;

                    // Add interval using proper calendar arithmetic for next iteration
                    let Some(next_ts) = TimestampNanosecondType::add_month_day_nano(
                        *current, step_val, *parsed_tz,
                    ) else {
                        return plan_err!(
                            "Failed to add interval {:?} to timestamp {}",
                            step_val,
                            current_value
                        );
                    };

                    *current = next_ts;

                    // Push the current value after successfully advancing
                    buf.push(current_value);
                }

                let array = TimestampNanosecondArray::from(buf);
                // Create array with proper timezone
                let array = match tz {
                    Some(tz_str) => array.with_timezone(Arc::clone(tz_str)),
                    None => array,
                };

                if array.is_empty() {
                    return Ok(None);
                }

                let batch =
                    RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(array)])?;
                Ok(Some(batch))
            }
            GenerateSeriesState::Empty { .. } => Ok(None),
        }
    }
}

fn reach_end_int64(val: i64, end: i64, step: i64, include_end: bool) -> bool {
    if step > 0 {
        if include_end {
            val > end
        } else {
            val >= end
        }
    } else if include_end {
        val < end
    } else {
        val <= end
    }
}

#[async_trait]
impl TableProvider for GenerateSeriesTable {
    fn as_any(&self) -> &dyn std::any::Any {
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
        let schema = match projection {
            Some(projection) => Arc::new(self.schema.project(projection)?),
            None => self.schema(),
        };
        let series_state = match &self.args {
            // if args have null, then return 0 row
            GenSeriesArgs::ContainsNull { name } => {
                GenerateSeriesState::Empty { batch_size, name }
            }
            GenSeriesArgs::Int64Args {
                start,
                end,
                step,
                include_end,
                name,
            } => GenerateSeriesState::Int64 {
                schema: self.schema(),
                start: *start,
                end: *end,
                step: *step,
                current: *start,
                batch_size,
                include_end: *include_end,
                name,
            },
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
                        datafusion_common::DataFusionError::Internal(format!(
                            "Failed to parse timezone: {e}"
                        ))
                    })?
                    .unwrap_or_else(|| Tz::from_str("+00:00").unwrap());
                GenerateSeriesState::Timestamp {
                    schema: self.schema(),
                    start: *start,
                    end: *end,
                    step: *step,
                    tz: tz.clone(),
                    parsed_tz,
                    current: *start,
                    batch_size,
                    include_end: *include_end,
                    name,
                }
            }
        };

        Ok(Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(series_state))],
        )?))
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
            Expr::Literal(scalar, _) => {
                plan_err!(
                    "Argument #1 must be an INTEGER, TIMESTAMP or NULL, got {:?}",
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
                    )
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
            return plan_err!("Start is bigger than end, but increment is positive: Cannot generate infinite series");
        }

        if start < end && step < 0 {
            return plan_err!("Start is smaller than end, but increment is negative: Cannot generate infinite series");
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
                )
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
                )
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
                )
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

        // Basic validation
        if step.months == 0 && step.days == 0 && step.nanoseconds == 0 {
            return plan_err!("Step interval cannot be zero");
        }

        // Check for infinite series conditions with timestamps
        let step_is_positive = step.months > 0 || step.days > 0 || step.nanoseconds > 0;
        let step_is_negative = step.months < 0 || step.days < 0 || step.nanoseconds < 0;

        if start > end && step_is_positive {
            return plan_err!("Start is bigger than end, but increment is positive: Cannot generate infinite series");
        }

        if start < end && step_is_negative {
            return plan_err!("Start is smaller than end, but increment is negative: Cannot generate infinite series");
        }

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
