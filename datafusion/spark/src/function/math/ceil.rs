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

use std::sync::Arc;

use arrow::array::{ArrowNativeTypeOp, AsArray, Decimal128Array};
use arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int64Type};
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int32,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};

/// Spark-compatible `ceil` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
///
/// Differences with DataFusion ceil:
///  - Spark's ceil returns Int64 for float inputs; DataFusion preserves
///    the input type (Float32→Float32, Float64→Float64)
///  - Spark's ceil on Decimal128(p, s) returns Decimal128(p−s+1, 0), reducing scale
///    to 0; DataFusion preserves the original precision and scale
///  - Spark only supports Decimal128; DataFusion also supports Decimal32/64/256
///  - Spark does not check for decimal overflow; DataFusion errors on overflow
///
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCeil {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkCeil {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCeil {
    pub fn new() -> Self {
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let integer = Coercion::new_exact(TypeSignatureClass::Integer);
        let decimal_places = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        let float32 = Coercion::new_exact(TypeSignatureClass::Native(logical_float32()));
        let float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // ceil(decimal, scale)
                    TypeSignature::Coercible(vec![
                        decimal.clone(),
                        decimal_places.clone(),
                    ]),
                    // ceil(decimal)
                    TypeSignature::Coercible(vec![decimal]),
                    // ceil(integer, scale)
                    TypeSignature::Coercible(vec![
                        integer.clone(),
                        decimal_places.clone(),
                    ]),
                    // ceil(integer)
                    TypeSignature::Coercible(vec![integer]),
                    // ceil(float32, scale)
                    TypeSignature::Coercible(vec![
                        float32.clone(),
                        decimal_places.clone(),
                    ]),
                    // ceil(float32)
                    TypeSignature::Coercible(vec![float32]),
                    // ceil(float64, scale)
                    TypeSignature::Coercible(vec![float64.clone(), decimal_places]),
                    // ceil(float64)
                    TypeSignature::Coercible(vec![float64]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec!["ceiling".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkCeil {
    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let has_scale = arg_types.len() == 2;

        match &arg_types[0] {
            DataType::Decimal128(p, s) if has_scale => Ok(DataType::Decimal128(*p, *s)),
            DataType::Decimal128(p, s) => {
                if *s > 0 {
                    Ok(DataType::Decimal128(decimal128_ceil_precision(*p, *s), 0))
                } else {
                    // scale <= 0 means the value is already a whole number
                    // (or represents multiples of 10^(-scale)), so ceil is a no-op
                    Ok(DataType::Decimal128(*p, *s))
                }
            }
            DataType::Float32 if has_scale => Ok(DataType::Float32),
            DataType::Float64 if has_scale => Ok(DataType::Float64),
            dt if matches!(dt, DataType::Float32 | DataType::Float64)
                || dt.is_integer() =>
            {
                Ok(DataType::Int64)
            }
            other => exec_err!("Unsupported data type {other:?} for function ceil"),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}

/// Extract the scale (decimal places) from the second argument.
/// Returns `Some(0)` if no second argument is provided.
/// Returns `None` if the scale argument is NULL (Spark returns NULL for `round(expr, NULL)`).
fn get_scale(args: &[ColumnarValue]) -> Result<Option<i32>> {
    if args.len() < 2 {
        return Ok(Some(0));
    }

    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(Some(*v)),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(ScalarValue::UInt8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => {
            i32::try_from(*v).map(Some).map_err(|_| {
                (exec_err!("round scale {v} is out of supported i32 range")
                    as Result<(), _>)
                    .unwrap_err()
            })
        }
        ColumnarValue::Scalar(sv) if sv.is_null() => Ok(None),
        other => exec_err!("Unsupported type for round scale: {}", other.data_type()),
    }
}

fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() || args.len() > 2 {
        return exec_err!(
            "ceil function requires 1 or 2 arguments, got {}",
            args.len()
        );
    }

    let scale = match get_scale(args)? {
        Some(scale) => scale,
        None => {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                args[0].data_type(),
            )?));
        }
    };
    let input = &args[0];

    match input {
        ColumnarValue::Scalar(value) => spark_ceil_scalar(value, scale),
        ColumnarValue::Array(input) => spark_ceil_array(input, scale),
    }
}

/// Compute ceil for a single decimal128 value with the given scale.
#[inline]
fn decimal128_ceil(value: i128, scale: u32) -> i128 {
    let div = 10_i128.pow_wrapping(scale);
    let d = value / div;
    let r = value % div;
    if r > 0 { d + 1 } else { d }
}

/// Compute the return precision for a decimal128 ceil result.
#[inline]
fn decimal128_ceil_precision(precision: u8, scale: i8) -> u8 {
    ((precision as i64) - (scale as i64) + 1).clamp(1, 38) as u8
}

fn spark_ceil_scalar(value: &ScalarValue, scale: i32) -> Result<ColumnarValue> {
    let result = match value {
        // Floats without scale (scale=0) -> Int64 (original behaivour)
        ScalarValue::Float32(v) if scale == 0 => {
            ScalarValue::Int64(v.map(|x| x.ceil() as i64))
        }
        ScalarValue::Float64(v) if scale == 0 => {
            ScalarValue::Int64(v.map(|x| x.ceil() as i64))
        }
        // Floats with scale -> preserve type
        ScalarValue::Float32(v) => ScalarValue::Float32(v.map(|x| ceil_float(x, scale))),
        ScalarValue::Float64(v) => ScalarValue::Float64(v.map(|x| ceil_float(x, scale))),
        // Integers: negative scale rounds, positive is no-op
        v if v.data_type().is_integer() => v.cast_to(&DataType::Int64)?,
        // Decimal128 with positive scalar
        ScalarValue::Decimal128(v, p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            ScalarValue::Decimal128(v.map(|x| decimal128_ceil(x, *s as u32)), new_p, 0)
        }
        ScalarValue::Decimal128(_, _, _) => value.clone(),
        other => {
            return exec_err!(
                "Unsupported data type {:?} for function ceil",
                other.data_type()
            );
        }
    };
    Ok(ColumnarValue::Scalar(result))
}

fn spark_ceil_array(
    input: &Arc<dyn arrow::array::Array>,
    scale: i32,
) -> Result<ColumnarValue> {
    let result = match input.data_type() {
        DataType::Float32 if scale == 0 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        DataType::Float64 if scale == 0 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        DataType::Float32 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|x| ceil_float(x, scale)),
        ) as _,
        DataType::Float64 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|x| ceil_float(x, scale)),
        ) as _,
        dt if dt.is_integer() => arrow::compute::cast(input, &DataType::Int64)?,
        DataType::Decimal128(p, s) if *s > 0 => {
            let new_p = decimal128_ceil_precision(*p, *s);
            let result: Decimal128Array = input
                .as_primitive::<Decimal128Type>()
                .unary(|x| decimal128_ceil(x, *s as u32));
            Arc::new(result.with_data_type(DataType::Decimal128(new_p, 0)))
        }
        DataType::Decimal128(_, _) => Arc::clone(input),
        other => return exec_err!("Unsupported data type {other:?} for function ceil"),
    };

    Ok(ColumnarValue::Array(result))
}

fn ceil_float<T: num_traits::Float>(value: T, scale: i32) -> T {
    if scale >= 0 {
        let factor = T::from(10.0f64.powi(scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            return value;
        }
        (value * factor).ceil() / factor
    } else {
        let factor = T::from(10.0f64.powi(-scale)).unwrap_or_else(T::infinity);
        if factor.is_infinite() {
            return T::zero();
        }
        (value / factor).ceil() * factor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use datafusion_common::ScalarValue;

    #[test]
    fn test_ceil_float64() {
        let input = Float64Array::from(vec![
            Some(125.2345),
            Some(15.0001),
            Some(0.1),
            Some(-0.9),
            Some(-1.1),
            Some(123.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ])
        );
    }

    #[test]
    fn test_ceil_float32() {
        let input = Float32Array::from(vec![
            Some(125.2345f32),
            Some(15.0001f32),
            Some(0.1f32),
            Some(-0.9f32),
            Some(-1.1f32),
            Some(123.0f32),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![
                Some(126),
                Some(16),
                Some(1),
                Some(0),
                Some(-1),
                Some(123),
                None,
            ])
        );
    }

    #[test]
    fn test_ceil_int64() {
        let input = Int64Array::from(vec![Some(1), Some(-1), None]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result, &Int64Array::from(vec![Some(1), Some(-1), None]));
    }

    #[test]
    fn test_ceil_decimal128() {
        // Decimal128(10, 2): 150 = 1.50, -150 = -1.50, 100 = 1.00
        let return_type = DataType::Decimal128(9, 0);
        let input = Decimal128Array::from(vec![Some(150), Some(-150), Some(100), None])
            .with_data_type(DataType::Decimal128(10, 2));
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_ceil(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Decimal128Type>();
        let expected = Decimal128Array::from(vec![Some(2), Some(-1), Some(1), None])
            .with_data_type(return_type);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_ceil_float64_scalar() {
        let input = ScalarValue::Float64(Some(-1.1));
        let args = vec![ColumnarValue::Scalar(input)];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Int64(Some(-1)));
    }

    #[test]
    fn test_ceil_float32_scalar() {
        let input = ScalarValue::Float32(Some(125.2345f32));
        let args = vec![ColumnarValue::Scalar(input)];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Int64(Some(126)));
    }

    #[test]
    fn test_ceil_int64_scalar() {
        let input = ScalarValue::Int64(Some(48));
        let args = vec![ColumnarValue::Scalar(input)];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Int64(Some(48)));
    }

    #[test]
    fn test_ceil_float64_scalar_with_positive_scale() {
        // ceil(3.1411, 2) → 3.15
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.1411))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Float64(Some(3.15)));
    }

    #[test]
    fn test_ceil_float64_scalar_with_negative_scale() {
        // ceil(3345.1, -2) → 3400.0
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3345.1))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-2))),
        ];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Float64(Some(3400.0)));
    }

    #[test]
    fn test_ceil_float64_scalar_with_zero_scale() {
        // ceil(3.5, 0) → 4 as Int64 (same as 1-arg behavior)
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.5))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
        ];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert_eq!(result, ScalarValue::Int64(Some(4)));
    }

    #[test]
    fn test_ceil_float32_scalar_with_scale() {
        // ceil(3.1f32, 1) → 3.1 (already exact)
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(3.1f32))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        // 3.1f32 ceiling at 1 decimal place stays 3.1
        assert_eq!(result, ScalarValue::Float32(Some(3.1f32)));
    }

    #[test]
    fn test_ceil_float64_array_with_scale() {
        // ceil([3.1411, -1.001, 0.0, NULL], 2) → [3.15, -1.0, 0.0, NULL]
        let input = Float64Array::from(vec![Some(3.1411), Some(-1.001), Some(0.0), None]);
        let args = vec![
            ColumnarValue::Array(Arc::new(input)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        let result = spark_ceil(&args).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(
            result,
            &Float64Array::from(vec![Some(3.15), Some(-1.0), Some(0.0), None])
        );
    }

    #[test]
    fn test_ceil_null_scale() {
        // ceil(3.5, NULL) → NULL
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Float64(Some(3.5))),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
        ];
        let result = match spark_ceil(&args).unwrap() {
            ColumnarValue::Scalar(v) => v,
            _ => panic!("Expected scalar"),
        };
        assert!(result.is_null());
    }
}
