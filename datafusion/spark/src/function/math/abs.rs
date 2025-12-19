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

use arrow::array::*;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions::{
    downcast_named_arg, make_abs_function, make_wrapping_abs_function,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `abs` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#abs>
///
/// Returns the absolute value of input
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// TODOs:
///  - Spark's ANSI-compliant dialect, when off (i.e. `spark.sql.ansi.enabled=false`), taking absolute value on the minimal value of a signed integer returns the value as is. DataFusion's abs throws "DataFusion error: Arrow error: Compute error" on arithmetic overflow
///  - Spark's abs also supports ANSI interval types: YearMonthIntervalType and DayTimeIntervalType. DataFusion's abs doesn't.
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAbs {
    signature: Signature,
}

impl Default for SparkAbs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAbs {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkAbs {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "abs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "SparkAbs: return_type() is not used; return_field_from_args() is implemented"
        )
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.is_empty() {
            return internal_err!("abs expects at least 1 argument");
        }

        let input_field = &args.arg_fields[0];
        let out_dt = input_field.data_type().clone();
        let mut out_nullable = input_field.is_nullable();

        // If any scalar argument is explicitly null, output must be nullable
        let scalar_null_present = args
            .scalar_arguments
            .iter()
            .any(|opt_s| opt_s.is_some_and(|sv| sv.is_null()));

        if scalar_null_present {
            out_nullable = true;
        }

        Ok(Arc::new(Field::new(self.name(), out_dt, out_nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_abs(&args.args)
    }
}

macro_rules! scalar_compute_op {
    ($INPUT:ident, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_abs();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(Some(
            result,
        ))))
    }};
    ($INPUT:ident, $PRECISION:expr, $SCALE:expr, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_abs();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(
            Some(result),
            $PRECISION,
            $SCALE,
        )))
    }};
}

pub fn spark_abs(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return internal_err!("abs takes exactly 1 argument, but got: {}", args.len());
    }

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok(args[0].clone()),
            DataType::Int8 => {
                let abs_fun = make_wrapping_abs_function!(Int8Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Int16 => {
                let abs_fun = make_wrapping_abs_function!(Int16Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Int32 => {
                let abs_fun = make_wrapping_abs_function!(Int32Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Int64 => {
                let abs_fun = make_wrapping_abs_function!(Int64Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Float32 => {
                let abs_fun = make_abs_function!(Float32Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Float64 => {
                let abs_fun = make_abs_function!(Float64Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Decimal128(_, _) => {
                let abs_fun = make_wrapping_abs_function!(Decimal128Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            DataType::Decimal256(_, _) => {
                let abs_fun = make_wrapping_abs_function!(Decimal256Array);
                abs_fun(array).map(ColumnarValue::Array)
            }
            dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_) => Ok(args[0].clone()),
            sv if sv.is_null() => Ok(args[0].clone()),
            ScalarValue::Int8(Some(v)) => scalar_compute_op!(v, Int8),
            ScalarValue::Int16(Some(v)) => scalar_compute_op!(v, Int16),
            ScalarValue::Int32(Some(v)) => scalar_compute_op!(v, Int32),
            ScalarValue::Int64(Some(v)) => scalar_compute_op!(v, Int64),
            ScalarValue::Float32(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(v.abs()))))
            }
            ScalarValue::Float64(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(v.abs()))))
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                scalar_compute_op!(v, *precision, *scale, Decimal128)
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                scalar_compute_op!(v, *precision, *scale, Decimal256)
            }
            dt => internal_err!("Not supported datatype for Spark ABS: {dt}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;

    macro_rules! eval_legacy_mode {
        ($TYPE:ident, $VAL:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(result, $VAL);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $RESULT:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL)));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(result, $RESULT);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $VAL);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                _ => unreachable!(),
            }
        }};
        ($TYPE:ident, $VAL:expr, $PRECISION:expr, $SCALE:expr, $RESULT:expr) => {{
            let args =
                ColumnarValue::Scalar(ScalarValue::$TYPE(Some($VAL), $PRECISION, $SCALE));
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $RESULT);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                _ => unreachable!(),
            }
        }};
    }

    #[test]
    fn test_abs_scalar_legacy_mode() {
        // NumericType MIN
        eval_legacy_mode!(UInt8, u8::MIN);
        eval_legacy_mode!(UInt16, u16::MIN);
        eval_legacy_mode!(UInt32, u32::MIN);
        eval_legacy_mode!(UInt64, u64::MIN);
        eval_legacy_mode!(Int8, i8::MIN);
        eval_legacy_mode!(Int16, i16::MIN);
        eval_legacy_mode!(Int32, i32::MIN);
        eval_legacy_mode!(Int64, i64::MIN);
        eval_legacy_mode!(Float32, f32::MIN, f32::MAX);
        eval_legacy_mode!(Float64, f64::MIN, f64::MAX);
        eval_legacy_mode!(Decimal128, i128::MIN, 18, 10);
        eval_legacy_mode!(Decimal256, i256::MIN, 10, 2);

        // NumericType not MIN
        eval_legacy_mode!(Int8, -1i8, 1i8);
        eval_legacy_mode!(Int16, -1i16, 1i16);
        eval_legacy_mode!(Int32, -1i32, 1i32);
        eval_legacy_mode!(Int64, -1i64, 1i64);
        eval_legacy_mode!(Decimal128, -1i128, 18, 10, 1i128);
        eval_legacy_mode!(Decimal256, i256::from(-1i8), 10, 2, i256::from(1i8));

        // Float32, Float64
        eval_legacy_mode!(Float32, f32::NEG_INFINITY, f32::INFINITY);
        eval_legacy_mode!(Float32, f32::INFINITY, f32::INFINITY);
        eval_legacy_mode!(Float32, 0.0f32, 0.0f32);
        eval_legacy_mode!(Float32, -0.0f32, 0.0f32);
        eval_legacy_mode!(Float64, f64::NEG_INFINITY, f64::INFINITY);
        eval_legacy_mode!(Float64, f64::INFINITY, f64::INFINITY);
        eval_legacy_mode!(Float64, 0.0f64, 0.0f64);
        eval_legacy_mode!(Float64, -0.0f64, 0.0f64);
    }

    macro_rules! eval_array_legacy_mode {
        ($INPUT:expr, $OUTPUT:expr, $FUNC:ident) => {{
            let input = $INPUT;
            let args = ColumnarValue::Array(Arc::new(input));
            let expected = $OUTPUT;
            match spark_abs(&[args]) {
                Ok(ColumnarValue::Array(result)) => {
                    let actual = datafusion_common::cast::$FUNC(&result).unwrap();
                    assert_eq!(actual, &expected);
                }
                _ => unreachable!(),
            }
        }};
    }

    #[test]
    fn test_abs_array_legacy_mode() {
        eval_array_legacy_mode!(
            Int8Array::from(vec![Some(-1), Some(i8::MIN), Some(i8::MAX), None]),
            Int8Array::from(vec![Some(1), Some(i8::MIN), Some(i8::MAX), None]),
            as_int8_array
        );

        eval_array_legacy_mode!(
            Int16Array::from(vec![Some(-1), Some(i16::MIN), Some(i16::MAX), None]),
            Int16Array::from(vec![Some(1), Some(i16::MIN), Some(i16::MAX), None]),
            as_int16_array
        );

        eval_array_legacy_mode!(
            Int32Array::from(vec![Some(-1), Some(i32::MIN), Some(i32::MAX), None]),
            Int32Array::from(vec![Some(1), Some(i32::MIN), Some(i32::MAX), None]),
            as_int32_array
        );

        eval_array_legacy_mode!(
            Int64Array::from(vec![Some(-1), Some(i64::MIN), Some(i64::MAX), None]),
            Int64Array::from(vec![Some(1), Some(i64::MIN), Some(i64::MAX), None]),
            as_int64_array
        );

        eval_array_legacy_mode!(
            Float32Array::from(vec![
                Some(-1f32),
                Some(f32::MIN),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float32Array::from(vec![
                Some(1f32),
                Some(f32::MAX),
                Some(f32::MAX),
                None,
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float32_array
        );

        eval_array_legacy_mode!(
            Float64Array::from(vec![
                Some(-1f64),
                Some(f64::MIN),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
            ]),
            Float64Array::from(vec![
                Some(1f64),
                Some(f64::MAX),
                Some(f64::MAX),
                None,
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(0.0),
                Some(0.0),
            ]),
            as_float64_array
        );

        eval_array_legacy_mode!(
            Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)
                .unwrap(),
            Decimal128Array::from(vec![Some(i128::MIN), None])
                .with_precision_and_scale(38, 37)
                .unwrap(),
            as_decimal128_array
        );

        eval_array_legacy_mode!(
            Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
            Decimal256Array::from(vec![Some(i256::MIN), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
            as_decimal256_array
        );
    }

    #[test]
    fn test_abs_nullability() {
        use arrow::datatypes::{DataType, Field};
        use datafusion_expr::ReturnFieldArgs;
        use std::sync::Arc;

        let abs = SparkAbs::new();

        // --- non-nullable Int32 input ---
        let non_nullable_i32 = Arc::new(Field::new("c", DataType::Int32, false));
        let out_non_null = abs
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_i32)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be non-nullable and the same DataType as input
        assert!(!out_non_null.is_nullable());
        assert_eq!(out_non_null.data_type(), &DataType::Int32);

        // --- nullable Int32 input ---
        let nullable_i32 = Arc::new(Field::new("c", DataType::Int32, true));
        let out_nullable = abs
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_i32)],
                scalar_arguments: &[None],
            })
            .unwrap();

        // result should be nullable and the same DataType as input
        assert!(out_nullable.is_nullable());
        assert_eq!(out_nullable.data_type(), &DataType::Int32);

        // --- non-nullable Float64 input ---
        let non_nullable_f64 = Arc::new(Field::new("c", DataType::Float64, false));
        let out_f64 = abs
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(!out_f64.is_nullable());
        assert_eq!(out_f64.data_type(), &DataType::Float64);

        // --- nullable Float64 input ---
        let nullable_f64 = Arc::new(Field::new("c", DataType::Float64, true));
        let out_f64_null = abs
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&nullable_f64)],
                scalar_arguments: &[None],
            })
            .unwrap();

        assert!(out_f64_null.is_nullable());
        assert_eq!(out_f64_null.data_type(), &DataType::Float64);
    }

    #[test]
    fn test_abs_nullability_with_null_scalar() -> Result<()> {
        use arrow::datatypes::{DataType, Field};
        use datafusion_expr::ReturnFieldArgs;
        use std::sync::Arc;

        let func = SparkAbs::new();

        // Non-nullable field with non-null scalar argument -> non-nullable result
        let non_nullable: FieldRef = Arc::new(Field::new("col", DataType::Int32, false));
        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&non_nullable)],
            scalar_arguments: &[None],
        })?;
        assert!(!out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Int32);

        // Non-nullable field with null scalar argument -> nullable result
        let null_scalar = ScalarValue::Int32(None);
        let out_with_null_scalar = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&non_nullable)],
            scalar_arguments: &[Some(&null_scalar)],
        })?;
        assert!(out_with_null_scalar.is_nullable());
        assert_eq!(out_with_null_scalar.data_type(), &DataType::Int32);

        Ok(())
    }
}
