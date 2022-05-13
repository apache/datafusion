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
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, Operator};
use std::any::Any;
use std::fmt::{Display, Formatter};
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

        let interval = match intervals {
            ColumnarValue::Scalar(interval) => match interval {
                ScalarValue::IntervalDayTime(Some(interval)) => interval as i32,
                ScalarValue::IntervalYearMonth(Some(_)) => {
                    return Err(DataFusionError::Execution(
                        "DateIntervalExpr does not support IntervalYearMonth".to_string(),
                    ))
                }
                ScalarValue::IntervalMonthDayNano(Some(_)) => {
                    return Err(DataFusionError::Execution(
                        "DateIntervalExpr does not support IntervalMonthDayNano"
                            .to_string(),
                    ))
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "DateIntervalExpr does not support non-interval type {:?}",
                        other
                    )))
                }
            },
            _ => {
                return Err(DataFusionError::Execution(
                    "Columnar execution is not yet supported for DateIntervalExpr"
                        .to_string(),
                ))
            }
        };

        match dates {
            ColumnarValue::Scalar(scalar) => match scalar {
                ScalarValue::Date32(Some(date)) => match &self.op {
                    Operator::Plus => Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                        Some(date + interval),
                    ))),
                    Operator::Minus => Ok(ColumnarValue::Scalar(ScalarValue::Date32(
                        Some(date - interval),
                    ))),
                    _ => {
                        // this should be unreachable because we check the operators in `try_new`
                        Err(DataFusionError::Execution(
                            "Invalid operator for DateIntervalExpr".to_string(),
                        ))
                    }
                },
                ScalarValue::Date64(Some(date)) => match &self.op {
                    Operator::Plus => Ok(ColumnarValue::Scalar(ScalarValue::Date64(
                        Some(date + interval as i64),
                    ))),
                    Operator::Minus => Ok(ColumnarValue::Scalar(ScalarValue::Date64(
                        Some(date - interval as i64),
                    ))),
                    _ => {
                        // this should be unreachable because we check the operators in `try_new`
                        Err(DataFusionError::Execution(
                            "Invalid operator for DateIntervalExpr".to_string(),
                        ))
                    }
                },
                _ => {
                    // this should be unreachable because we check the types in `try_new`
                    Err(DataFusionError::Execution(
                        "Invalid lhs type for DateIntervalExpr".to_string(),
                    ))
                }
            },
            _ => Err(DataFusionError::Execution(
                "Columnar execution is not yet supported for DateIntervalExpr"
                    .to_string(),
            )),
        }
    }
}
