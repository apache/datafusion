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
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `negative` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#negative>
///
/// Returns the negation of input (equivalent to unary minus)
/// Returns NULL if input is NULL, returns NaN if input is NaN.
///
/// TODOs:
///  - Spark's ANSI-compliant dialect, when off (i.e. `spark.sql.ansi.enabled=false`),
///    negating the minimal value of a signed integer wraps around.
///    For example: negative(i32::MIN) returns i32::MIN (wraps instead of error).
///    This is the current implementation (legacy mode only).
///  - Spark's ANSI mode (when `spark.sql.ansi.enabled=true`) should throw an
///    ARITHMETIC_OVERFLOW error on integer overflow instead of wrapping.
///    This is not yet implemented - all operations currently use wrapping behavior.
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    signature: Signature,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNegative {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNegative {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "negative"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_negative(&args.args)
    }
}

/// Helper macro to generate wrapping negation for array types
macro_rules! wrapping_negative_array {
    ($INPUT:expr, $ARRAY_TYPE:ident) => {{
        let array = $INPUT
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected {}, got different type",
                    stringify!($ARRAY_TYPE)
                ))
            })?;
        let result: $ARRAY_TYPE = array.unary(|x| x.wrapping_neg());
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Helper macro to generate simple negation for floating point array types
macro_rules! simple_negative_array {
    ($INPUT:expr, $ARRAY_TYPE:ident) => {{
        let array = $INPUT
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Expected {}, got different type",
                    stringify!($ARRAY_TYPE)
                ))
            })?;
        let result: $ARRAY_TYPE = array.unary(|x| -x);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

/// Helper macro to generate wrapping negation for scalar types
macro_rules! wrapping_negative_scalar {
    ($INPUT:ident, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_neg();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(Some(
            result,
        ))))
    }};
}

/// Helper macro to generate wrapping negation for decimal scalar types
macro_rules! wrapping_negative_decimal_scalar {
    ($INPUT:ident, $PRECISION:expr, $SCALE:expr, $SCALAR_TYPE:ident) => {{
        let result = $INPUT.wrapping_neg();
        Ok(ColumnarValue::Scalar(ScalarValue::$SCALAR_TYPE(
            Some(result),
            $PRECISION,
            $SCALE,
        )))
    }};
}

/// Core implementation of Spark's negative function
pub fn spark_negative(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return internal_err!(
            "negative takes exactly 1 argument, but got: {}",
            args.len()
        );
    }

    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Null => Ok(args[0].clone()),

            // Signed integers - use wrapping negation (Spark legacy mode behavior)
            DataType::Int8 => wrapping_negative_array!(array, Int8Array),
            DataType::Int16 => wrapping_negative_array!(array, Int16Array),
            DataType::Int32 => wrapping_negative_array!(array, Int32Array),
            DataType::Int64 => wrapping_negative_array!(array, Int64Array),

            // Floating point - simple negation (no overflow possible)
            DataType::Float16 => simple_negative_array!(array, Float16Array),
            DataType::Float32 => simple_negative_array!(array, Float32Array),
            DataType::Float64 => simple_negative_array!(array, Float64Array),

            // Decimal types - wrapping negation
            DataType::Decimal128(_, _) => {
                wrapping_negative_array!(array, Decimal128Array)
            }
            DataType::Decimal256(_, _) => {
                wrapping_negative_array!(array, Decimal256Array)
            }

            dt => internal_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
        ColumnarValue::Scalar(sv) => match sv {
            ScalarValue::Null => Ok(args[0].clone()),
            sv if sv.is_null() => Ok(args[0].clone()),

            // Signed integers - wrapping negation
            ScalarValue::Int8(Some(v)) => wrapping_negative_scalar!(v, Int8),
            ScalarValue::Int16(Some(v)) => wrapping_negative_scalar!(v, Int16),
            ScalarValue::Int32(Some(v)) => wrapping_negative_scalar!(v, Int32),
            ScalarValue::Int64(Some(v)) => wrapping_negative_scalar!(v, Int64),

            // Floating point - simple negation
            ScalarValue::Float16(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float16(Some(-v))))
            }
            ScalarValue::Float32(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(-v))))
            }
            ScalarValue::Float64(Some(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(-v))))
            }

            // Decimal types - wrapping negation
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                wrapping_negative_decimal_scalar!(v, *precision, *scale, Decimal128)
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => {
                wrapping_negative_decimal_scalar!(v, *precision, *scale, Decimal256)
            }

            dt => internal_err!("Not supported datatype for Spark NEGATIVE: {dt}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;

    /// Helper macro for testing scalar values with wrapping behavior
    macro_rules! test_negative_scalar {
        ($TYPE:ident, $INPUT:expr, $EXPECTED:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(Some($INPUT)));
            match spark_negative(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(Some(result)))) => {
                    assert_eq!(
                        result,
                        $EXPECTED,
                        "Failed for {} input: {}",
                        stringify!($TYPE),
                        $INPUT
                    );
                }
                other => panic!("Unexpected result: {:?}", other),
            }
        }};
        ($TYPE:ident, $INPUT:expr, $PRECISION:expr, $SCALE:expr, $EXPECTED:expr) => {{
            let args = ColumnarValue::Scalar(ScalarValue::$TYPE(
                Some($INPUT),
                $PRECISION,
                $SCALE,
            ));
            match spark_negative(&[args]) {
                Ok(ColumnarValue::Scalar(ScalarValue::$TYPE(
                    Some(result),
                    precision,
                    scale,
                ))) => {
                    assert_eq!(result, $EXPECTED);
                    assert_eq!(precision, $PRECISION);
                    assert_eq!(scale, $SCALE);
                }
                other => panic!("Unexpected result: {:?}", other),
            }
        }};
    }

    #[test]
    fn test_negative_scalar_integers() {
        // Regular values
        test_negative_scalar!(Int8, 5i8, -5i8);
        test_negative_scalar!(Int16, 100i16, -100i16);
        test_negative_scalar!(Int32, 1000i32, -1000i32);
        test_negative_scalar!(Int64, 10000i64, -10000i64);

        // Zero
        test_negative_scalar!(Int8, 0i8, 0i8);
        test_negative_scalar!(Int16, 0i16, 0i16);
        test_negative_scalar!(Int32, 0i32, 0i32);
        test_negative_scalar!(Int64, 0i64, 0i64);

        // Negative values
        test_negative_scalar!(Int8, -5i8, 5i8);
        test_negative_scalar!(Int16, -100i16, 100i16);
        test_negative_scalar!(Int32, -1000i32, 1000i32);
        test_negative_scalar!(Int64, -10000i64, 10000i64);

        // MIN values - wrapping behavior (Spark legacy mode)
        test_negative_scalar!(Int8, i8::MIN, i8::MIN);
        test_negative_scalar!(Int16, i16::MIN, i16::MIN);
        test_negative_scalar!(Int32, i32::MIN, i32::MIN);
        test_negative_scalar!(Int64, i64::MIN, i64::MIN);

        // MAX values
        test_negative_scalar!(Int8, i8::MAX, -i8::MAX);
        test_negative_scalar!(Int16, i16::MAX, -i16::MAX);
        test_negative_scalar!(Int32, i32::MAX, -i32::MAX);
        test_negative_scalar!(Int64, i64::MAX, -i64::MAX);
    }

    #[test]
    fn test_negative_scalar_floats() {
        test_negative_scalar!(Float32, 5.5f32, -5.5f32);
        test_negative_scalar!(Float64, 10.25f64, -10.25f64);

        test_negative_scalar!(Float32, -5.5f32, 5.5f32);
        test_negative_scalar!(Float64, -10.25f64, 10.25f64);

        test_negative_scalar!(Float32, 0.0f32, -0.0f32);
        test_negative_scalar!(Float64, 0.0f64, -0.0f64);

        test_negative_scalar!(Float32, f32::INFINITY, f32::NEG_INFINITY);
        test_negative_scalar!(Float64, f64::INFINITY, f64::NEG_INFINITY);

        test_negative_scalar!(Float32, f32::NEG_INFINITY, f32::INFINITY);
        test_negative_scalar!(Float64, f64::NEG_INFINITY, f64::INFINITY);
    }

    #[test]
    fn test_negative_scalar_decimals() {
        test_negative_scalar!(Decimal128, 12345i128, 18, 10, -12345i128);
        test_negative_scalar!(Decimal128, -12345i128, 18, 10, 12345i128);
        test_negative_scalar!(Decimal128, i128::MIN, 38, 37, i128::MIN);

        test_negative_scalar!(
            Decimal256,
            i256::from(12345i32),
            10,
            2,
            i256::from(-12345i32)
        );
        test_negative_scalar!(
            Decimal256,
            i256::from(-12345i32),
            10,
            2,
            i256::from(12345i32)
        );
    }

    #[test]
    fn test_negative_array_integers() {
        // Test Int8Array with wrapping behavior
        let input = Int8Array::from(vec![
            Some(5),
            Some(-5),
            Some(0),
            Some(i8::MIN), // Key test: wraps to i8::MIN
            Some(i8::MAX),
            None,
        ]);
        let expected = Int8Array::from(vec![
            Some(-5),
            Some(5),
            Some(0),
            Some(i8::MIN), // Wraps!
            Some(-i8::MAX),
            None,
        ]);

        let args = vec![ColumnarValue::Array(Arc::new(input))];
        match spark_negative(&args) {
            Ok(ColumnarValue::Array(result)) => {
                let actual = result.as_any().downcast_ref::<Int8Array>().unwrap();
                assert_eq!(actual, &expected);
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    #[test]
    fn test_negative_array_floats() {
        let input = Float64Array::from(vec![
            Some(5.5),
            Some(-5.5),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
        ]);

        let args = vec![ColumnarValue::Array(Arc::new(input))];
        match spark_negative(&args) {
            Ok(ColumnarValue::Array(result)) => {
                let actual = result.as_any().downcast_ref::<Float64Array>().unwrap();
                assert_eq!(actual.value(0), -5.5);
                assert_eq!(actual.value(1), 5.5);
                assert_eq!(actual.value(2), -0.0);
                assert_eq!(actual.value(3), f64::NEG_INFINITY);
                assert_eq!(actual.value(4), f64::INFINITY);
                assert!(actual.value(5).is_nan());
                assert!(actual.is_null(6));
            }
            other => panic!("Unexpected result: {:?}", other),
        }
    }

    #[test]
    fn test_negative_null_handling() {
        // Null scalar
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(None))];
        match spark_negative(&args) {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))) => {}
            other => panic!("Expected null result, got: {:?}", other),
        }

        // Null type
        let args = vec![ColumnarValue::Scalar(ScalarValue::Null)];
        match spark_negative(&args) {
            Ok(ColumnarValue::Scalar(ScalarValue::Null)) => {}
            other => panic!("Expected null result, got: {:?}", other),
        }
    }

    #[test]
    fn test_negative_wrong_arg_count() {
        let result = spark_negative(&[]);
        assert!(result.is_err());

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int32(Some(5))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(10))),
        ];
        let result = spark_negative(&args);
        assert!(result.is_err());
    }
}
