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

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use arrow::array::{
    Array, ArrayRef, Date64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::unary;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Schema, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_date32_array;
use datafusion_common::scalar::{
    date32_add, date64_add, microseconds_add, milliseconds_add, nanoseconds_add,
    seconds_add,
};
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Perform DATE/TIME/TIMESTAMP +/ INTERVAL math
#[derive(Debug)]
pub struct DateTimeIntervalExpr {
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    // TODO: move type checking to the planning phase and not in the physical expr
    // so we can remove this
    input_schema: Schema,
}

impl DateTimeIntervalExpr {
    /// Create a new instance of DateIntervalExpr
    pub fn try_new(
        lhs: Arc<dyn PhysicalExpr>,
        op: Operator,
        rhs: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Result<Self> {
        match lhs.data_type(input_schema)? {
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
                match rhs.data_type(input_schema)? {
                    DataType::Interval(_) => match &op {
                        Operator::Plus | Operator::Minus => Ok(Self {
                            lhs,
                            op,
                            rhs,
                            input_schema: input_schema.clone(),
                        }),
                        _ => Err(DataFusionError::Execution(format!(
                            "Invalid operator '{}' for DateIntervalExpr",
                            op
                        ))),
                    },
                    other => Err(DataFusionError::Execution(format!(
                        "Operation '{}' not support for type {}",
                        op, other
                    ))),
                }
            }
            other => Err(DataFusionError::Execution(format!(
                "Invalid lhs type '{}' for DateIntervalExpr",
                other
            ))),
        }
    }

    /// Get the left-hand side expression
    pub fn lhs(&self) -> &Arc<dyn PhysicalExpr> {
        &self.lhs
    }

    /// Get the operator
    pub fn op(&self) -> &Operator {
        &self.op
    }

    /// Get the right-hand side expression
    pub fn rhs(&self) -> &Arc<dyn PhysicalExpr> {
        &self.rhs
    }
}

impl Display for DateTimeIntervalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.lhs, self.op, self.rhs)
    }
}

impl PhysicalExpr for DateTimeIntervalExpr {
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

        // Unwrap interval to add
        let intervals = match &intervals {
            ColumnarValue::Scalar(interval) => interval,
            _ => {
                let msg = "Columnar execution is not yet supported for DateIntervalExpr";
                return Err(DataFusionError::Execution(msg.to_string()));
            }
        };

        // Invert sign for subtraction
        let sign = match self.op {
            Operator::Plus => 1,
            Operator::Minus => -1,
            _ => {
                // this should be unreachable because we check the operators in `try_new`
                let msg = "Invalid operator for DateIntervalExpr";
                return Err(DataFusionError::Internal(msg.to_string()));
            }
        };

        match dates {
            ColumnarValue::Scalar(operand) => Ok(ColumnarValue::Scalar(if sign > 0 {
                operand.add(intervals)?
            } else {
                operand.sub(intervals)?
            })),
            ColumnarValue::Array(array) => evaluate_array(array, sign, intervals),
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.lhs.clone(), self.rhs.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(DateTimeIntervalExpr::try_new(
            children[0].clone(),
            self.op,
            children[1].clone(),
            &self.input_schema,
        )?))
    }
}

impl PartialEq<dyn Any> for DateTimeIntervalExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.lhs.eq(&x.lhs) && self.op == x.op && self.rhs.eq(&x.rhs))
            .unwrap_or(false)
    }
}

pub fn evaluate_array(
    array: ArrayRef,
    sign: i32,
    scalar: &ScalarValue,
) -> Result<ColumnarValue> {
    let ret = match array.data_type() {
        DataType::Date32 => {
            let array = as_date32_array(&array)?;
            Arc::new(unary::<Date32Type, _, Date32Type>(array, |days| {
                date32_add(days, scalar, sign).unwrap()
            })) as ArrayRef
        }
        DataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Arc::new(unary::<Date64Type, _, Date64Type>(array, |ms| {
                date64_add(ms, scalar, sign).unwrap()
            })) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Arc::new(unary::<TimestampSecondType, _, TimestampSecondType>(
                array,
                |ts_s| seconds_add(ts_s, scalar, sign).unwrap(),
            )) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Arc::new(
                unary::<TimestampMillisecondType, _, TimestampMillisecondType>(
                    array,
                    |ts_ms| milliseconds_add(ts_ms, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Arc::new(
                unary::<TimestampMicrosecondType, _, TimestampMicrosecondType>(
                    array,
                    |ts_us| microseconds_add(ts_us, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Arc::new(
                unary::<TimestampNanosecondType, _, TimestampNanosecondType>(
                    array,
                    |ts_ns| nanoseconds_add(ts_ns, scalar, sign).unwrap(),
                ),
            ) as ArrayRef
        }
        _ => Err(DataFusionError::Execution(format!(
            "Invalid lhs type for DateIntervalExpr: {}",
            array.data_type()
        )))?,
    };
    Ok(ColumnarValue::Array(ret))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_physical_expr;
    use crate::execution_props::ExecutionProps;
    use arrow::array::{ArrayRef, Date32Builder};
    use arrow::datatypes::*;
    use chrono::{Duration, NaiveDate};
    use datafusion_common::delta::shift_months;
    use datafusion_common::{Column, Result, ToDFSchema};
    use datafusion_expr::Expr;
    use std::ops::Add;

    #[test]
    fn add_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 11);
        assert_eq!(format!("{:?}", actual).as_str(), "2000-12-01");
    }

    #[test]
    fn add_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 12);
        assert_eq!(format!("{:?}", actual).as_str(), "2001-01-01");
    }

    #[test]
    fn add_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, 13);
        assert_eq!(format!("{:?}", actual).as_str(), "2001-02-01");
    }

    #[test]
    fn sub_11_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -11);
        assert_eq!(format!("{:?}", actual).as_str(), "1999-02-01");
    }

    #[test]
    fn sub_12_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -12);
        assert_eq!(format!("{:?}", actual).as_str(), "1999-01-01");
    }

    #[test]
    fn sub_13_months() {
        let prior = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let actual = shift_months(prior, -13);
        assert_eq!(format!("{:?}", actual).as_str(), "1998-12-01");
    }

    #[test]
    fn add_32_day_time() -> Result<()> {
        // setup
        let dt = Expr::Literal(ScalarValue::Date32(Some(0)));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(1, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
        let interval =
            Expr::Literal(ScalarValue::new_interval_dt(-15, -24 * 60 * 60 * 1000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date64(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let res = epoch.add(Duration::milliseconds(d));
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
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
        let interval = Expr::Literal(ScalarValue::new_interval_mdn(-12, -15, -42));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::Date32(Some(d))) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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
    fn add_1_millisecond() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 1));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns + 1_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn add_2_hours() -> Result<()> {
        // setup
        let now_ts_s = chrono::Utc::now().timestamp();
        let dt = Expr::Literal(ScalarValue::TimestampSecond(Some(now_ts_s), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 2 * 3600 * 1_000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_s + 2 * 3600);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn sub_4_hours() -> Result<()> {
        // setup
        let now_ts_s = chrono::Utc::now().timestamp();
        let dt = Expr::Literal(ScalarValue::TimestampSecond(Some(now_ts_s), None));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(0, 4 * 3600 * 1_000));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_s - 4 * 3600);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn add_8_days() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Plus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(8, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns + 8 * 86400 * 1_000_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn sub_16_days() -> Result<()> {
        // setup
        let now_ts_ns = chrono::Utc::now().timestamp_nanos();
        let dt = Expr::Literal(ScalarValue::TimestampNanosecond(Some(now_ts_ns), None));
        let op = Operator::Minus;
        let interval = Expr::Literal(ScalarValue::new_interval_dt(16, 0));

        // exercise
        let res = exercise(&dt, op, &interval)?;

        // assert
        match res {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(ts), None)) => {
                assert_eq!(ts, now_ts_ns - 16 * 86400 * 1_000_000_000);
            }
            _ => Err(DataFusionError::NotImplemented(
                "Unexpected result!".to_string(),
            ))?,
        }
        Ok(())
    }

    #[test]
    fn array_add_26_days() -> Result<()> {
        let mut builder = Date32Builder::with_capacity(8);
        builder.append_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let a: ArrayRef = Arc::new(builder.finish());

        let schema = Schema::new(vec![Field::new("a", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;
        let dfs = schema.clone().to_dfschema()?;
        let props = ExecutionProps::new();

        let dt = Expr::Column(Column::from_name("a"));
        let interval = Expr::Literal(ScalarValue::new_interval_dt(26, 0));
        let op = Operator::Plus;

        let lhs = create_physical_expr(&dt, &dfs, &schema, &props)?;
        let rhs = create_physical_expr(&interval, &dfs, &schema, &props)?;

        let cut = DateTimeIntervalExpr::try_new(lhs, op, rhs, &schema)?;
        let res = cut.evaluate(&batch)?;

        let mut builder = Date32Builder::with_capacity(8);
        builder.append_slice(&[26, 27, 28, 29, 30, 31, 32, 33]);
        let expected: ArrayRef = Arc::new(builder.finish());

        // assert
        match res {
            ColumnarValue::Array(array) => {
                assert_eq!(&array, &expected)
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
        let mut builder = Date32Builder::with_capacity(1);
        builder.append_value(0);
        let a: ArrayRef = Arc::new(builder.finish());
        let schema = Schema::new(vec![Field::new("a", DataType::Date32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a])?;

        let dfs = schema.clone().to_dfschema()?;
        let props = ExecutionProps::new();

        let lhs = create_physical_expr(dt, &dfs, &schema, &props)?;
        let rhs = create_physical_expr(interval, &dfs, &schema, &props)?;

        let lhs_str = format!("{}", lhs);
        let rhs_str = format!("{}", rhs);

        let cut = DateTimeIntervalExpr::try_new(lhs, op, rhs, &schema)?;

        assert_eq!(lhs_str, format!("{}", cut.lhs()));
        assert_eq!(op, cut.op().clone());
        assert_eq!(rhs_str, format!("{}", cut.rhs()));

        let res = cut.evaluate(&batch)?;
        Ok(res)
    }
}
