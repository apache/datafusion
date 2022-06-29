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

use crate::PhysicalExpr;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Duration, NaiveDate};
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator};
use std::any::Any;
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::ops::{Add, Sub};
use std::sync::Arc;

/// Perform DATE +/ INTERVAL math
#[derive(Debug)]
pub struct DateIntervalExpr {
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
}

impl DateIntervalExpr {
    /// Create a new instance of DateIntervalExpr
    pub fn try_new(
        lhs: Arc<dyn PhysicalExpr>,
        op: Operator,
        rhs: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Self> {
        match lhs.data_type(input_schema)? {
            DataType::Date32 | DataType::Date64 => match rhs.data_type(input_schema)? {
                DataType::Interval(_) => match &op {
                    Operator::Plus | Operator::Minus => Ok(Self { lhs, op, rhs }),
                    _ => Err(DataFusionError::Execution(format!(
                        "Invalid operator '{}' for DateIntervalExpr",
                        op
                    ))),
                },
                other => Err(DataFusionError::Execution(format!(
                    "Invalid rhs type '{}' for DateIntervalExpr",
                    other
                ))),
            },
            other => Err(DataFusionError::Execution(format!(
                "Invalid lhs type '{}' for DateIntervalExpr",
                other
            ))),
        }
    }
}

impl Display for DateIntervalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

impl PhysicalExpr for DateIntervalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.lhs.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.lhs.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let dates = self.lhs.evaluate(batch)?;
        let intervals = self.rhs.evaluate(batch)?;

        // Unwrap days since epoch
        let operand = match dates {
            ColumnarValue::Scalar(scalar) => scalar,
            _ => Err(DataFusionError::Execution(
                "Columnar execution is not yet supported for DateIntervalExpr"
                    .to_string(),
            ))?,
        };

        // Convert to NaiveDate
        let epoch = NaiveDate::from_ymd(1970, 1, 1);
        let prior = match operand {
            ScalarValue::Date32(Some(d)) => epoch.add(Duration::days(d as i64)),
            ScalarValue::Date64(Some(ms)) => epoch.add(Duration::milliseconds(ms)),
            _ => Err(DataFusionError::Execution(format!(
                "Invalid lhs type for DateIntervalExpr: {:?}",
                operand
            )))?,
        };

        // Unwrap interval to add
        let scalar = match &intervals {
            ColumnarValue::Scalar(interval) => interval,
            _ => Err(DataFusionError::Execution(
                "Columnar execution is not yet supported for DateIntervalExpr"
                    .to_string(),
            ))?,
        };

        // Invert sign for subtraction
        let sign = match &self.op {
            Operator::Plus => 1,
            Operator::Minus => -1,
            _ => {
                // this should be unreachable because we check the operators in `try_new`
                Err(DataFusionError::Execution(
                    "Invalid operator for DateIntervalExpr".to_string(),
                ))?
            }
        };

        // Do math
        let posterior = match scalar {
            ScalarValue::IntervalDayTime(Some(i)) => add_day_time(prior, *i, sign),
            ScalarValue::IntervalYearMonth(Some(i)) => add_months(prior, *i * sign),
            ScalarValue::IntervalMonthDayNano(Some(i)) => add_m_d_nano(prior, *i, sign),
            other => Err(DataFusionError::Execution(format!(
                "DateIntervalExpr does not support non-interval type {:?}",
                other
            )))?,
        };

        // convert back
        let res = match operand {
            ScalarValue::Date32(Some(_)) => {
                let days = posterior.sub(epoch).num_days() as i32;
                ColumnarValue::Scalar(ScalarValue::Date32(Some(days)))
            }
            ScalarValue::Date64(Some(_)) => {
                let ms = posterior.sub(epoch).num_milliseconds();
                ColumnarValue::Scalar(ScalarValue::Date64(Some(ms)))
            }
            _ => Err(DataFusionError::Execution(format!(
                "Invalid lhs type for DateIntervalExpr: {}",
                scalar
            )))?,
        };
        Ok(res)
    }
}

fn add_m_d_nano(prior: NaiveDate, interval: i128, sign: i32) -> NaiveDate {
    let interval = interval as u128;
    let months = (interval >> 96) as i32 * sign;
    let days = (interval >> 64) as i32 * sign;
    let nanos = interval as i64 * sign as i64;
    let a = add_months(prior, months);
    let b = a.add(Duration::days(days as i64));
    let c = b.add(Duration::nanoseconds(nanos));
    c
}

fn add_day_time(prior: NaiveDate, interval: i64, sign: i32) -> NaiveDate {
    let interval = interval as u64;
    let days = (interval >> 32) as i32 * sign;
    let ms = interval as i32 * sign;
    let intermediate = prior.add(Duration::days(days as i64));
    let posterior = intermediate.add(Duration::milliseconds(ms as i64));
    posterior
}

fn add_months(prior: NaiveDate, interval: i32) -> NaiveDate {
    let target = chrono_add_months(prior, interval);
    let target_plus = chrono_add_months(target, 1);
    let last_day = target_plus.sub(chrono::Duration::days(1));
    let day = min(prior.day(), last_day.day());
    NaiveDate::from_ymd(target.year(), target.month(), day)
}

fn chrono_add_months(dt: NaiveDate, delta: i32) -> NaiveDate {
    let ay = dt.year();
    let am = dt.month() as i32 - 1; // zero-based for modulo operations
    let bm = am + delta as i32;
    let by = ay + if bm < 0 { bm / 12 - 1 } else { bm / 12 };
    let cm = bm % 12;
    let dm = if cm < 0 { cm + 12 } else { cm };
    NaiveDate::from_ymd(by, dm as u32 + 1, 1)
}
