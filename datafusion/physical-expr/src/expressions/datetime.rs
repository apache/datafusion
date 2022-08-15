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
use arrow::datatypes::{DataType, IntervalDayTimeType, IntervalMonthDayNanoType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Duration, Months, NaiveDate};
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator};
use std::any::Any;
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
            ScalarValue::IntervalDayTime(Some(i)) => {
                let (days, ms) = IntervalDayTimeType::to_parts(*i);
                let intermediate = prior.add(Duration::days(days as i64 * sign as i64));
                intermediate.add(Duration::milliseconds(ms as i64 * sign as i64))
            }
            ScalarValue::IntervalYearMonth(Some(months)) => {
                if months * sign > 0 {
                    prior + Months::new(months.abs() as u32)
                } else {
                    prior - Months::new(months.abs() as u32)
                }
            }
            ScalarValue::IntervalMonthDayNano(Some(i)) => {
                let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*i);
                let a = if months * sign > 0 {
                    prior + Months::new(months.abs() as u32)
                } else {
                    prior - Months::new(months.abs() as u32)
                };
                let b = a.add(Duration::days(days as i64));
                b.add(Duration::nanoseconds(nanos))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_physical_expr;
    use crate::execution_props::ExecutionProps;
    use arrow::array::{ArrayRef, Date32Builder};
    use arrow::datatypes::*;
    use datafusion_common::{Result, ToDFSchema};
    use datafusion_expr::Expr;

    #[test]
    fn add_32_day_time() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = IntervalDayTimeType::make_value(1, 0);
        let interval = Expr::Literal(ScalarValue::IntervalDayTime(Some(interval)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{:?}", res).as_str(), "1970-01-02");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn sub_32_year_month() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::IntervalYearMonth(Some(13)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{:?}", res).as_str(), "1968-12-01");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_64_day_time() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date64(Some(0)));
        let op = Operator::Plus;
        let interval = IntervalDayTimeType::make_value(-15, -24 * 60 * 60 * 1000);
        let interval = Expr::Literal(ScalarValue::IntervalDayTime(Some(interval)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date64(Some(d))) => {
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let res = epoch.add(Duration::milliseconds(d as i64));
                assert_eq!(format!("{:?}", res).as_str(), "1969-12-16");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_32_year_month() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::IntervalYearMonth(Some(1)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{:?}", res).as_str(), "1970-02-01");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn add_32_month_day_nano() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;

        let interval = IntervalMonthDayNanoType::make_value(-12, -15, -42);

        let interval = Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(interval)));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd(1970, 1, 1);
                let res = epoch.add(Duration::days(d as i64));
                assert_eq!(format!("{:?}", res).as_str(), "1968-12-17");
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }

        Ok(())
    }

    #[test]
    fn invalid_interval() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::Null);

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add a NULL interval");

        Ok(())
    }

    #[test]
    fn invalid_date() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Null);
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(0)));

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add to NULL date");

        Ok(())
    }

    #[test]
    fn invalid_op() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Eq;
        let interval = Expr::Literal(ScalarValue::IntervalMonthDayNano(Some(0)));

        // exercise
        let res = exercise(&dt, op, &interval);
        assert!(res.is_err(), "Can't add dates with == operator");

        Ok(())
    }

    fn exercise(dt: &Expr, op: Operator, interval: &Expr) -> Result<ColumnarValue> {
        let mut builder = Date32Builder::new(1);
        builder.append_value(0);
        let a: ArrayRef = Arc::new(builder.finish());
        let schema = Schema::new(vec![Field::new("a", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let dfs = schema.clone().to_dfschema()?;
        let props = ExecutionProps::new();

        let lhs = create_physical_expr(dt, &dfs, &schema, &props)?;
        let rhs = create_physical_expr(interval, &dfs, &schema, &props)?;

        let cut = DateIntervalExpr::try_new(lhs, op, rhs, &schema)?;
        let res = cut.evaluate(&batch)?;
        Ok(res)
    }
}
