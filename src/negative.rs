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

use super::arithmetic_overflow_error;
use crate::SparkError;
use arrow::{compute::kernels::numeric::neg_wrapping, datatypes::IntervalDayTimeType};
use arrow_array::RecordBatch;
use arrow_buffer::IntervalDayTime;
use arrow_schema::{DataType, Schema};
use datafusion::{
    logical_expr::{interval_arithmetic::Interval, ColumnarValue},
    physical_expr::PhysicalExpr,
};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::sort_properties::ExprProperties;
use std::hash::Hash;
use std::{any::Any, sync::Arc};

pub fn create_negate_expr(
    expr: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    Ok(Arc::new(NegativeExpr::new(expr, fail_on_error)))
}

/// Negative expression
#[derive(Debug, Eq)]
pub struct NegativeExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
    fail_on_error: bool,
}

impl Hash for NegativeExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for NegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.arg.eq(&other.arg) && self.fail_on_error.eq(&other.fail_on_error)
    }
}

macro_rules! check_overflow {
    ($array:expr, $array_type:ty, $min_val:expr, $type_name:expr) => {{
        let typed_array = $array
            .as_any()
            .downcast_ref::<$array_type>()
            .expect(concat!(stringify!($array_type), " expected"));
        for i in 0..typed_array.len() {
            if typed_array.value(i) == $min_val {
                if $type_name == "byte" || $type_name == "short" {
                    let value = format!("{:?} caused", typed_array.value(i));
                    return Err(arithmetic_overflow_error(value.as_str()).into());
                }
                return Err(arithmetic_overflow_error($type_name).into());
            }
        }
    }};
}

impl NegativeExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, fail_on_error: bool) -> Self {
        Self { arg, fail_on_error }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for NegativeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(- {})", self.arg)
    }
}

impl PhysicalExpr for NegativeExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.arg.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.arg.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;

        // overflow checks only apply in ANSI mode
        // datatypes supported are byte, short, integer, long, float, interval
        match arg {
            ColumnarValue::Array(array) => {
                if self.fail_on_error {
                    match array.data_type() {
                        DataType::Int8 => {
                            check_overflow!(array, arrow::array::Int8Array, i8::MIN, "byte")
                        }
                        DataType::Int16 => {
                            check_overflow!(array, arrow::array::Int16Array, i16::MIN, "short")
                        }
                        DataType::Int32 => {
                            check_overflow!(array, arrow::array::Int32Array, i32::MIN, "integer")
                        }
                        DataType::Int64 => {
                            check_overflow!(array, arrow::array::Int64Array, i64::MIN, "long")
                        }
                        DataType::Interval(value) => match value {
                            arrow::datatypes::IntervalUnit::YearMonth => check_overflow!(
                                array,
                                arrow::array::IntervalYearMonthArray,
                                i32::MIN,
                                "interval"
                            ),
                            arrow::datatypes::IntervalUnit::DayTime => check_overflow!(
                                array,
                                arrow::array::IntervalDayTimeArray,
                                IntervalDayTime::MIN,
                                "interval"
                            ),
                            arrow::datatypes::IntervalUnit::MonthDayNano => {
                                // Overflow checks are not supported
                            }
                        },
                        _ => {
                            // Overflow checks are not supported for other datatypes
                        }
                    }
                }
                let result = neg_wrapping(array.as_ref())?;
                Ok(ColumnarValue::Array(result))
            }
            ColumnarValue::Scalar(scalar) => {
                if self.fail_on_error {
                    match scalar {
                        ScalarValue::Int8(value) => {
                            if value == Some(i8::MIN) {
                                return Err(arithmetic_overflow_error(" caused").into());
                            }
                        }
                        ScalarValue::Int16(value) => {
                            if value == Some(i16::MIN) {
                                return Err(arithmetic_overflow_error(" caused").into());
                            }
                        }
                        ScalarValue::Int32(value) => {
                            if value == Some(i32::MIN) {
                                return Err(arithmetic_overflow_error("integer").into());
                            }
                        }
                        ScalarValue::Int64(value) => {
                            if value == Some(i64::MIN) {
                                return Err(arithmetic_overflow_error("long").into());
                            }
                        }
                        ScalarValue::IntervalDayTime(value) => {
                            let (days, ms) =
                                IntervalDayTimeType::to_parts(value.unwrap_or_default());
                            if days == i32::MIN || ms == i32::MIN {
                                return Err(arithmetic_overflow_error("interval").into());
                            }
                        }
                        ScalarValue::IntervalYearMonth(value) => {
                            if value == Some(i32::MIN) {
                                return Err(arithmetic_overflow_error("interval").into());
                            }
                        }
                        _ => {
                            // Overflow checks are not supported for other datatypes
                        }
                    }
                }
                Ok(ColumnarValue::Scalar((scalar.arithmetic_negate())?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NegativeExpr::new(
            Arc::clone(&children[0]),
            self.fail_on_error,
        )))
    }

    /// Given the child interval of a NegativeExpr, it calculates the NegativeExpr's interval.
    /// It replaces the upper and lower bounds after multiplying them with -1.
    /// Ex: `(a, b]` => `[-b, -a)`
    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        Interval::try_new(
            children[0].upper().arithmetic_negate()?,
            children[0].lower().arithmetic_negate()?,
        )
    }

    /// Returns a new [`Interval`] of a NegativeExpr  that has the existing `interval` given that
    /// given the input interval is known to be `children`.
    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let child_interval = children[0];

        if child_interval.lower() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.upper() == &ScalarValue::Int32(Some(i32::MIN))
            || child_interval.lower() == &ScalarValue::Int64(Some(i64::MIN))
            || child_interval.upper() == &ScalarValue::Int64(Some(i64::MIN))
        {
            return Err(SparkError::ArithmeticOverflow {
                from_type: "long".to_string(),
            }
            .into());
        }

        let negated_interval = Interval::try_new(
            interval.upper().arithmetic_negate()?,
            interval.lower().arithmetic_negate()?,
        )?;

        Ok(child_interval
            .intersect(negated_interval)?
            .map(|result| vec![result]))
    }

    /// The ordering of a [`NegativeExpr`] is simply the reverse of its child.
    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let properties = children[0].clone().with_order(children[0].sort_properties);
        Ok(properties)
    }
}
