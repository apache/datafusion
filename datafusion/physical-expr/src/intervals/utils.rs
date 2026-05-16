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

//! Utility functions for the interval arithmetic library

use std::sync::Arc;

use crate::{
    PhysicalExpr,
    expressions::{BinaryExpr, CastExpr, Column, Literal, NegativeExpr},
    scalar_function::ScalarFunctionExpr,
};

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::Operator;
use datafusion_expr::interval_arithmetic::Interval;

/// Indicates whether interval arithmetic is supported for the given expression.
/// Currently, we do not support all [`PhysicalExpr`]s for interval calculations.
/// We do not support every type of [`Operator`]s either. Over time, this check
/// will relax as more types of `PhysicalExpr`s and `Operator`s are supported.
/// Currently, [`CastExpr`], [`NegativeExpr`], [`BinaryExpr`], [`Column`] and [`Literal`] are supported.
pub fn check_support(expr: &Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> bool {
    if let Some(binary_expr) = expr.downcast_ref::<BinaryExpr>() {
        is_operator_supported(binary_expr.op())
            && check_support(binary_expr.left(), schema)
            && check_support(binary_expr.right(), schema)
    } else if let Some(column) = expr.downcast_ref::<Column>() {
        if let Ok(field) = schema.field_with_name(column.name()) {
            is_datatype_supported(field.data_type())
        } else {
            false
        }
    } else if let Some(literal) = expr.downcast_ref::<Literal>() {
        if let Ok(dt) = literal.data_type(schema) {
            is_datatype_supported(&dt)
        } else {
            false
        }
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        check_support(cast.expr(), schema)
    } else if let Some(negative) = expr.downcast_ref::<NegativeExpr>() {
        check_support(negative.arg(), schema)
    } else if let Some(scalar_fn) = expr.downcast_ref::<ScalarFunctionExpr>() {
        is_datatype_supported(scalar_fn.return_type())
            && scalar_fn
                .args()
                .iter()
                .all(|arg| check_support(arg, schema))
    } else {
        false
    }
}

// This function returns the inverse operator of the given operator.
pub fn get_inverse_op(op: Operator) -> Result<Operator> {
    match op {
        Operator::Plus => Ok(Operator::Minus),
        Operator::Minus => Ok(Operator::Plus),
        Operator::Multiply => Ok(Operator::Divide),
        Operator::Divide => Ok(Operator::Multiply),
        _ => internal_err!("Interval arithmetic does not support the operator {}", op),
    }
}

/// Indicates whether interval arithmetic is supported for the given operator.
pub fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::GtEq
            | &Operator::Lt
            | &Operator::LtEq
            | &Operator::Eq
            | &Operator::Multiply
            | &Operator::Divide
    )
}

/// Indicates whether interval arithmetic is supported for the given data type.
pub fn is_datatype_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::Int64
            | &DataType::Int32
            | &DataType::Int16
            | &DataType::Int8
            | &DataType::UInt64
            | &DataType::UInt32
            | &DataType::UInt16
            | &DataType::UInt8
            | &DataType::Float64
            | &DataType::Float32
            | &DataType::Date32
            | &DataType::Date64
            | &DataType::Timestamp(_, _)
    )
}

/// Converts an [`Interval`] of time intervals to one of `Duration`s, if applicable. Otherwise, returns [`None`].
pub fn convert_interval_type_to_duration(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_interval_bound_to_duration(interval.lower()),
        convert_interval_bound_to_duration(interval.upper()),
    ) {
        Interval::try_new(lower, upper).ok()
    } else {
        None
    }
}

/// Converts an [`ScalarValue`] containing a time interval to one containing a `Duration`, if applicable. Otherwise, returns [`None`].
fn convert_interval_bound_to_duration(
    interval_bound: &ScalarValue,
) -> Option<ScalarValue> {
    match interval_bound {
        ScalarValue::IntervalMonthDayNano(Some(mdn)) => interval_mdn_to_duration_ns(mdn)
            .ok()
            .map(|duration| ScalarValue::DurationNanosecond(Some(duration))),
        ScalarValue::IntervalDayTime(Some(dt)) => interval_dt_to_duration_ms(dt)
            .ok()
            .map(|duration| ScalarValue::DurationMillisecond(Some(duration))),
        _ => None,
    }
}

/// Converts an [`Interval`] of `Duration`s to one of time intervals, if applicable. Otherwise, returns [`None`].
pub fn convert_duration_type_to_interval(interval: &Interval) -> Option<Interval> {
    if let (Some(lower), Some(upper)) = (
        convert_duration_bound_to_interval(interval.lower()),
        convert_duration_bound_to_interval(interval.upper()),
    ) {
        Interval::try_new(lower, upper).ok()
    } else {
        None
    }
}

/// Converts a [`ScalarValue`] containing a `Duration` to one containing a time interval, if applicable. Otherwise, returns [`None`].
fn convert_duration_bound_to_interval(
    interval_bound: &ScalarValue,
) -> Option<ScalarValue> {
    match interval_bound {
        ScalarValue::DurationNanosecond(Some(duration)) => {
            Some(ScalarValue::new_interval_mdn(0, 0, *duration))
        }
        ScalarValue::DurationMicrosecond(Some(duration)) => {
            Some(ScalarValue::new_interval_mdn(0, 0, *duration * 1000))
        }
        ScalarValue::DurationMillisecond(Some(duration)) => {
            Some(ScalarValue::new_interval_dt(0, *duration as i32))
        }
        ScalarValue::DurationSecond(Some(duration)) => {
            Some(ScalarValue::new_interval_dt(0, *duration as i32 * 1000))
        }
        _ => None,
    }
}

/// If both the month and day fields of [`ScalarValue::IntervalMonthDayNano`] are zero, this function returns the nanoseconds part.
/// Otherwise, it returns an error.
fn interval_mdn_to_duration_ns(mdn: &IntervalMonthDayNano) -> Result<i64> {
    if mdn.months == 0 && mdn.days == 0 {
        Ok(mdn.nanoseconds)
    } else {
        internal_err!(
            "The interval cannot have a non-zero month or day value for duration convertibility"
        )
    }
}

/// If the day field of the [`ScalarValue::IntervalDayTime`] is zero, this function returns the milliseconds part.
/// Otherwise, it returns an error.
fn interval_dt_to_duration_ms(dt: &IntervalDayTime) -> Result<i64> {
    if dt.days == 0 {
        // Safe to cast i32 to i64
        Ok(dt.milliseconds as i64)
    } else {
        internal_err!(
            "The interval cannot have a non-zero day value for duration convertibility"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{Column, Literal};
    use crate::scalar_function::ScalarFunctionExpr;
    use arrow::datatypes::{Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };

    fn f64_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, false)]))
    }

    fn utf8_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]))
    }

    fn col_x() -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new("x", 0))
    }

    fn lit_f64(v: f64) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Float64(Some(v))))
    }

    fn scalar_fn_expr(
        udf: Arc<datafusion_expr::ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Arc<dyn PhysicalExpr> {
        let name = udf.name().to_string();
        Arc::new(ScalarFunctionExpr::new(
            &name,
            udf,
            args,
            Field::new("result", return_type, true).into(),
            Arc::new(ConfigOptions::default()),
        ))
    }

    /// A minimal UDF whose declared return type is Utf8, used to test that
    /// check_support rejects functions with unsupported return types without
    /// relying on an invalid ceil-returns-Utf8 combination.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Utf8UDF {
        signature: Signature,
    }

    impl Utf8UDF {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Float64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for Utf8UDF {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "utf8_udf"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
    }

    fn utf8_udf() -> Arc<datafusion_expr::ScalarUDF> {
        Arc::new(datafusion_expr::ScalarUDF::from(Utf8UDF::new()))
    }

    #[test]
    fn test_check_support_scalar_fn_supported_return_type() {
        // ceil(x) returns Float64 — both return type and child are supported
        let schema = f64_schema();
        let expr = scalar_fn_expr(
            datafusion_functions::math::ceil(),
            vec![col_x()],
            DataType::Float64,
        );
        assert!(check_support(&expr, &schema));
    }

    #[test]
    fn test_check_support_scalar_fn_unsupported_return_type() {
        // utf8_udf(x) returns Utf8 — not in is_datatype_supported
        let schema = f64_schema();
        let expr = scalar_fn_expr(utf8_udf(), vec![col_x()], DataType::Utf8);
        assert!(!check_support(&expr, &schema));
    }

    #[test]
    fn test_check_support_scalar_fn_unsupported_child() {
        // ceil applied to a Utf8 column — child fails is_datatype_supported
        let schema = utf8_schema();
        let col_s = Arc::new(Column::new("s", 0)) as Arc<dyn PhysicalExpr>;
        let expr = scalar_fn_expr(
            datafusion_functions::math::ceil(),
            vec![col_s],
            DataType::Float64,
        );
        assert!(!check_support(&expr, &schema));
    }

    #[test]
    fn test_check_support_scalar_fn_in_binary_expr() {
        // ceil(x) > 5.0 — the main use case: ScalarFunctionExpr inside a BinaryExpr
        let schema = f64_schema();
        let ceil_x = scalar_fn_expr(
            datafusion_functions::math::ceil(),
            vec![col_x()],
            DataType::Float64,
        );
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(ceil_x, Operator::Gt, lit_f64(5.0)));
        assert!(check_support(&expr, &schema));
    }

    #[test]
    fn test_check_support_scalar_fn_in_binary_expr_unsupported_return() {
        // utf8_udf(x) > 5.0 where f returns Utf8 — should be false
        let schema = f64_schema();
        let fn_expr = scalar_fn_expr(utf8_udf(), vec![col_x()], DataType::Utf8);
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(fn_expr, Operator::Gt, lit_f64(5.0)));
        assert!(!check_support(&expr, &schema));
    }
}
