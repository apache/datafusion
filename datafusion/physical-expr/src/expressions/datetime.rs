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
use chrono::{Datelike, NaiveDate};
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

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        self.lhs.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion_common::Result<bool> {
        self.lhs.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
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
            ScalarValue::Date32(Some(date)) => {
                epoch.add(chrono::Duration::days(date as i64))
            }
            ScalarValue::Date64(Some(date)) => epoch.add(chrono::Duration::days(date)),
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

        // Negate for subtraction
        let interval = match &scalar {
            ScalarValue::IntervalDayTime(Some(interval)) => *interval,
            ScalarValue::IntervalYearMonth(Some(interval)) => *interval as i64,
            ScalarValue::IntervalMonthDayNano(Some(_interval)) => {
                Err(DataFusionError::Execution(
                    "DateIntervalExpr does not support IntervalMonthDayNano".to_string(),
                ))?
            }
            other => Err(DataFusionError::Execution(format!(
                "DateIntervalExpr does not support non-interval type {:?}",
                other
            )))?,
        };
        let interval = match &self.op {
            Operator::Plus => interval,
            Operator::Minus => interval * -1,
            _ => {
                // this should be unreachable because we check the operators in `try_new`
                Err(DataFusionError::Execution(
                    "Invalid operator for DateIntervalExpr".to_string(),
                ))?
            }
        };

        // Add interval
        let posterior = match scalar {
            ScalarValue::IntervalDayTime(Some(_)) => {
                prior.add(chrono::Duration::days(interval))
            }
            ScalarValue::IntervalYearMonth(Some(_)) => {
                let target = add_months(prior, interval);
                let target_plus = add_months(target, 1);
                let last_day = target_plus.sub(chrono::Duration::days(1));
                let day = min(prior.day(), last_day.day());
                NaiveDate::from_ymd(target.year(), target.month(), day)
            }
            ScalarValue::IntervalMonthDayNano(Some(_)) => {
                Err(DataFusionError::Execution(
                    "DateIntervalExpr does not support IntervalMonthDayNano".to_string(),
                ))?
            }
            other => Err(DataFusionError::Execution(format!(
                "DateIntervalExpr does not support non-interval type {:?}",
                other
            )))?,
        };

        // convert back
        let posterior = posterior.sub(epoch).num_days();
        let res = match operand {
            ScalarValue::Date32(Some(_)) => {
                let casted = i32::try_from(posterior).map_err(|_| {
                    DataFusionError::Execution(
                        "Date arithmetic out of bounds!".to_string(),
                    )
                })?;
                ColumnarValue::Scalar(ScalarValue::Date32(Some(casted)))
            }
            ScalarValue::Date64(Some(_)) => {
                ColumnarValue::Scalar(ScalarValue::Date64(Some(posterior)))
            }
            _ => Err(DataFusionError::Execution(format!(
                "Invalid lhs type for DateIntervalExpr: {}",
                scalar
            )))?,
        };
        Ok(res)
    }
}

fn add_months(dt: NaiveDate, delta: i64) -> NaiveDate {
    let ay = dt.year();
    let am = dt.month() as i32 - 1; // zero-based for modulo operations
    let bm = am + delta as i32;
    let by = ay + if bm < 0 { bm / 12 - 1 } else { bm / 12 };
    let cm = bm % 12;
    let dm = if cm < 0 { cm + 12 } else { cm };
    return NaiveDate::from_ymd(by, dm as u32 + 1, 1);
}
