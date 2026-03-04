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

use arrow::array::cast::AsArray;
use arrow::array::types::Decimal128Type;
use arrow::array::{Decimal128Array, Int64Array};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `ceil` function.
///
/// Returns the smallest integer greater than or equal to the input.
/// Unlike standard DataFusion ceil, this returns Int64 for float/integer
/// inputs (matching Spark behavior).
///
/// # Supported types
/// - Float32, Float64: returns Int64
/// - Int8, Int16, Int32, Int64: returns Int64
/// - Decimal128(p, s): returns Decimal128(p, s) (preserves precision and scale)
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion_common::Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(
        &self,
        args: ReturnFieldArgs,
    ) -> datafusion_common::Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let return_type = match args.arg_fields[0].data_type() {
            DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
            _ => DataType::Int64,
        };
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}

/// Computes the Spark-compatible ceiling of the input.
pub fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let input = array.as_primitive::<arrow::datatypes::Float32Type>();
                let result: Int64Array = unary(input, |x| x.ceil() as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Float64 => {
                let input = array.as_primitive::<arrow::datatypes::Float64Type>();
                let result: Int64Array = unary(input, |x| x.ceil() as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int8 => {
                let input = array.as_primitive::<arrow::datatypes::Int8Type>();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int16 => {
                let input = array.as_primitive::<arrow::datatypes::Int16Type>();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int32 => {
                let input = array.as_primitive::<arrow::datatypes::Int32Type>();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => Ok(ColumnarValue::Array(Arc::clone(array))),
            DataType::Decimal128(precision, scale) => {
                if *scale <= 0 {
                    Ok(ColumnarValue::Array(Arc::clone(array)))
                } else {
                    let f = decimal_ceil_f(*scale);
                    let input = array.as_primitive::<Decimal128Type>();
                    let result: Decimal128Array = unary(input, &f);
                    let result =
                        result.with_data_type(DataType::Decimal128(*precision, *scale));
                    Ok(ColumnarValue::Array(Arc::new(result)))
                }
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function ceil")
            }
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float32(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                v.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Float64(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                v.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Int8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                v.map(|x| x as i64),
            ))),
            ScalarValue::Int16(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                v.map(|x| x as i64),
            ))),
            ScalarValue::Int32(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                v.map(|x| x as i64),
            ))),
            ScalarValue::Int64(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(*v))),
            ScalarValue::Decimal128(v, precision, scale) => {
                if *scale <= 0 {
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        *v, *precision, *scale,
                    )))
                } else {
                    let f = decimal_ceil_f(*scale);
                    let result = v.map(f);
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        result, *precision, *scale,
                    )))
                }
            }
            other => {
                exec_err!(
                    "Unsupported data type {:?} for function ceil",
                    other.data_type()
                )
            }
        },
    }
}

/// Returns a closure that computes ceil for decimal values.
#[inline]
fn decimal_ceil_f(scale: i8) -> impl Fn(i128) -> i128 {
    let divisor = 10_i128.pow(scale as u32);
    move |x: i128| {
        let quotient = x / divisor;
        let remainder = x % divisor;
        if remainder > 0 {
            (quotient + 1) * divisor
        } else {
            quotient * divisor
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int64Array,
    };
    use datafusion_common::Result;
    use datafusion_common::cast::{as_decimal128_array, as_int64_array};

    #[test]
    fn test_ceil_float32_array() -> Result<()> {
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(1.9),
            Some(-1.1),
            Some(-1.9),
            Some(0.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 2);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), -1);
        assert_eq!(result.value(3), -1);
        assert_eq!(result.value(4), 0);
        assert!(result.is_null(5));
        Ok(())
    }

    #[test]
    fn test_ceil_float32_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) = spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, 2);
        Ok(())
    }

    #[test]
    fn test_ceil_float64_array() -> Result<()> {
        let array = Float64Array::from(vec![
            Some(1.1),
            Some(1.9),
            Some(-1.1),
            Some(-1.9),
            Some(0.0),
            Some(123.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 2);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), -1);
        assert_eq!(result.value(3), -1);
        assert_eq!(result.value(4), 0);
        assert_eq!(result.value(5), 123);
        assert!(result.is_null(6));
        Ok(())
    }

    #[test]
    fn test_ceil_float64_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.5)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) = spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, -1);
        Ok(())
    }

    #[test]
    fn test_ceil_float64_null_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(None))];
        let ColumnarValue::Scalar(ScalarValue::Int64(result)) = spark_ceil(&args)? else {
            unreachable!()
        };
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_ceil_int8_array() -> Result<()> {
        let array = Int8Array::from(vec![Some(1), Some(-1), Some(127), Some(-128), None]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), -1);
        assert_eq!(result.value(2), 127);
        assert_eq!(result.value(3), -128);
        assert!(result.is_null(4));
        Ok(())
    }

    #[test]
    fn test_ceil_int16_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int16(Some(100)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) = spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, 100);
        Ok(())
    }

    #[test]
    fn test_ceil_int32_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(-500)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) = spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, -500);
        Ok(())
    }

    #[test]
    fn test_ceil_int64_array() -> Result<()> {
        let array = Int64Array::from(vec![
            Some(1),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), -1);
        assert_eq!(result.value(2), i64::MAX);
        assert_eq!(result.value(3), i64::MIN);
        assert!(result.is_null(4));
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_array() -> Result<()> {
        let array =
            Decimal128Array::from(vec![Some(12345), Some(12500), Some(-12999), None])
                .with_precision_and_scale(5, 2)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let expected =
            Decimal128Array::from(vec![Some(12400), Some(12500), Some(-12900), None])
                .with_precision_and_scale(5, 2)?;
        let actual = as_decimal128_array(&result)?;
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(567),
            3,
            1,
        ))];
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 1)) =
            spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, 570);
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_negative_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(-567),
            3,
            1,
        ))];
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 1)) =
            spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, -560);
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_null_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(None, 5, 2))];
        let ColumnarValue::Scalar(ScalarValue::Decimal128(result, 5, 2)) =
            spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_scale_zero() -> Result<()> {
        let array = Decimal128Array::from(vec![Some(123), Some(-456), None])
            .with_precision_and_scale(10, 0)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let result = as_decimal128_array(&result)?;
        assert_eq!(result.value(0), 123);
        assert_eq!(result.value(1), -456);
        assert!(result.is_null(2));
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_scale_zero_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(12345),
            10,
            0,
        ))];
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 10, 0)) =
            spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, 12345);
        Ok(())
    }
}
