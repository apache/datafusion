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

use arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use arrow::datatypes::DataType::{
    Decimal128, Float32, Float64, Int16, Int32, Int64, Int8, UInt16, UInt32, UInt64,
    UInt8,
};
use arrow::datatypes::{ArrowNativeTypeOp, DataType, DECIMAL128_MAX_PRECISION};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible CEIL function implementation.
/// Returns the smallest integer that is greater than or equal to the input value.
/// Optionally takes a scale parameter to control decimal precision.
/// Reference: <https://spark.apache.org/docs/latest/api/sql/index.html#ceil>
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
            signature: Signature::one_of(
                vec![
                    // Single argument: ceil(expr) for basic numeric types
                    TypeSignature::Uniform(
                        1,
                        vec![Float32, Float64, Int64, Decimal128(38, 10)],
                    ),
                    // Two arguments: ceil(expr, scale) where scale can be any integer type
                    // Float32 with various integer scale types
                    TypeSignature::Exact(vec![Float32, Int8]),
                    TypeSignature::Exact(vec![Float32, Int16]),
                    TypeSignature::Exact(vec![Float32, Int32]),
                    TypeSignature::Exact(vec![Float32, Int64]),
                    // Float64 with various integer scale types
                    TypeSignature::Exact(vec![Float64, Int8]),
                    TypeSignature::Exact(vec![Float64, Int16]),
                    TypeSignature::Exact(vec![Float64, Int32]),
                    TypeSignature::Exact(vec![Float64, Int64]),
                    // Int64 with various integer scale types (scale has no effect on integers)
                    TypeSignature::Exact(vec![Int64, Int8]),
                    TypeSignature::Exact(vec![Int64, Int16]),
                    TypeSignature::Exact(vec![Int64, Int32]),
                    TypeSignature::Exact(vec![Int64, Int64]),
                    // Decimal128 with various integer scale types
                    TypeSignature::Exact(vec![Decimal128(38, 10), Int8]),
                    TypeSignature::Exact(vec![Decimal128(38, 10), Int16]),
                    TypeSignature::Exact(vec![Decimal128(38, 10), Int32]),
                    TypeSignature::Exact(vec![Decimal128(38, 10), Int64]),
                ],
                Volatility::Immutable,
            ),
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

    /// Determines the return type based on input argument types.
    /// For single argument (no scale): floats return Int64, integers stay Int64, decimals adjust precision/scale.
    /// For two arguments (with scale): floats keep their type, decimals become Float64.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("ceil expects at least 1 argument");
        }

        let value_type = &arg_types[0];
        let has_scale = arg_types.len() == 2;

        match (value_type, has_scale) {
            (Float32, false) => Ok(Int64),
            (Float32, true) => Ok(Float32),
            (Float64, false) => Ok(Int64),
            (Float64, true) => Ok(Float64),
            (Int64, _) => Ok(Int64),
            (Decimal128(precision, scale), false) => {
                // For decimals without scale, compute new precision/scale for integer result
                let (new_precision, new_scale) =
                    round_decimal_base(*precision as i32, *scale as i32, 0);
                Ok(Decimal128(new_precision, new_scale))
            }
            (Decimal128(_precision, _scale), true) => Ok(Float64), // With scale, convert to float
            _ => Ok(Int64), // Fallback for unsupported types
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_ceil, vec![])(&args.args)
    }
}

/// Calculates the new precision and scale for decimal operations.
/// Used to determine the appropriate decimal representation after ceiling operations.
/// Ensures the result fits within Decimal128 constraints.
fn round_decimal_base(precision: i32, _scale: i32, target_scale: i32) -> (u8, i8) {
    // Clamp target scale to valid range and ensure non-negative
    let scale = if target_scale < -38 {
        0
    } else {
        target_scale.max(0) as i8
    };
    // Calculate new precision based on target scale, ensuring it doesn't exceed max
    let new_precision = precision
        .max(target_scale + 1)
        .min(DECIMAL128_MAX_PRECISION as i32) as u8;
    (new_precision, scale)
}

/// Core implementation of the Spark CEIL function.
/// Handles ceiling operations for different data types with optional scale parameter.
/// Supports Float32, Float64, Int64, and Decimal128 types.
fn spark_ceil(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Validate argument count
    if args.is_empty() || args.len() > 2 {
        return exec_err!("ceil expects 1 or 2 arguments, got {}", args.len());
    }

    let value_array: &dyn Array = args[0].as_ref();

    // Extract scale parameter if provided (second argument)
    let scale = if args.len() == 2 {
        let scale_array = args[1].as_ref();
        // Scale must be a single scalar value, not an array
        if scale_array.is_empty() || scale_array.len() != 1 {
            return exec_err!(
                "scale parameter must be a single integer value, got array of length {}",
                scale_array.len()
            );
        }
        // Extract the scale value from the array, supporting various integer types
        let s = match scale_array.data_type() {
            Int8 => scale_array
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(0) as i32, // Cast to i32 for uniform handling
            Int16 => scale_array
                .as_primitive::<arrow::datatypes::Int16Type>()
                .value(0) as i32,
            Int32 => scale_array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(0), // Already i32
            Int64 => scale_array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0) as i32,
            UInt8 => scale_array
                .as_primitive::<arrow::datatypes::UInt8Type>()
                .value(0) as i32,
            UInt16 => scale_array
                .as_primitive::<arrow::datatypes::UInt16Type>()
                .value(0) as i32,
            UInt32 => scale_array
                .as_primitive::<arrow::datatypes::UInt32Type>()
                .value(0) as i32,
            UInt64 => scale_array
                .as_primitive::<arrow::datatypes::UInt64Type>()
                .value(0) as i32,
            other => {
                return exec_err!("scale parameter must be an integer, got {:?}", other)
            }
        };
        Some(s)
    } else {
        None // No scale provided
    };

    // Perform ceiling operation based on data type and scale parameter
    match (args[0].data_type(), scale) {
        // Float32 without scale: ceil to nearest integer, return as Int64
        (Float32, None) => {
            let array = value_array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f32| {
                value.ceil() as i64
            });
            Ok(Arc::new(array))
        }
        // Float32 with scale: ceil to specified decimal places, return as Float32
        (Float32, Some(s)) => {
            let scale_factor = 10_f32.powi(s); // 10^scale for decimal place adjustment
            let array = value_array
                .as_primitive::<arrow::datatypes::Float32Type>()
                .unary::<_, arrow::datatypes::Float32Type>(|value: f32| {
                (value * scale_factor).ceil() / scale_factor // Scale, ceil, then unscale
            });
            Ok(Arc::new(array))
        }
        // Float64 without scale: ceil to nearest integer, return as Int64
        (Float64, None) => {
            let array = value_array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .unary::<_, arrow::datatypes::Int64Type>(|value: f64| {
                value.ceil() as i64
            });
            Ok(Arc::new(array))
        }
        // Float64 with scale: ceil to specified decimal places, return as Float64
        (Float64, Some(s)) => {
            let scale_factor = 10_f64.powi(s); // 10^scale for decimal place adjustment
            let array = value_array
                .as_primitive::<arrow::datatypes::Float64Type>()
                .unary::<_, arrow::datatypes::Float64Type>(|value: f64| {
                (value * scale_factor).ceil() / scale_factor // Scale, ceil, then unscale
            });
            Ok(Arc::new(array))
        }
        // Int64: integers are already "ceiled", return unchanged regardless of scale
        (Int64, None) => Ok(Arc::clone(&args[0])),
        (Int64, Some(_)) => Ok(Arc::clone(&args[0])),
        // Decimal128: handle decimals with scale > 0 (fractional part exists)
        (Decimal128(precision, value_scale), scale_param) => {
            if *value_scale > 0 {
                match scale_param {
                    // Without scale parameter: ceil to integer
                    None => {
                        let decimal_array = value_array
                            .as_primitive::<arrow::datatypes::Decimal128Type>();
                        // Calculate divisor to separate integer and fractional parts
                        let div = 10_i128.pow_wrapping((*value_scale) as u32);
                        // Ceil by dividing, applying ceil to quotient, then converting to i64
                        let result_array = decimal_array
                            .unary::<_, arrow::datatypes::Int64Type>(|value: i128| {
                                div_ceil(value, div) as i64
                            });
                        Ok(Arc::new(result_array))
                    }
                    // With scale parameter: ceil to specified decimal places
                    Some(s) => {
                        // Validate that target scale doesn't exceed input scale
                        if s > *value_scale as i32 {
                            return exec_err!(
                                "scale {} cannot be greater than input scale {}",
                                s,
                                *value_scale
                            );
                        }
                        // Calculate new precision and scale for the result
                        let (new_precision, new_scale) =
                            round_decimal_base(*precision as i32, *value_scale as i32, s);
                        let decimal_array = value_array
                            .as_primitive::<arrow::datatypes::Decimal128Type>();

                        // Handle positive scale (decimal places)
                        if s >= 0 {
                            let s_i8 = s as i8;
                            if s_i8 > *value_scale {
                                return exec_err!(
                                    "output scale {} cannot exceed input scale {}",
                                    s_i8,
                                    *value_scale
                                );
                            }
                            let factor =
                                10_i128.pow_wrapping((*value_scale - s_i8) as u32);
                            let result_array = decimal_array
                                .unary::<_, arrow::datatypes::Decimal128Type>(
                                |value: i128| div_ceil(value, factor),
                            );
                            let decimal_result = result_array
                                .with_precision_and_scale(new_precision, new_scale)?;
                            let scale_factor = 10_f64.powi(new_scale as i32);
                            let float_values: Vec<Option<f64>> = decimal_result
                                .iter()
                                .map(|v| v.map(|x| (x as f64) / scale_factor))
                                .collect();
                            Ok(Arc::new(Float64Array::from(float_values)))
                        } else {
                            let s_i8 = s as i8;
                            let factor =
                                10_i128.pow_wrapping((*value_scale - s_i8) as u32);
                            let result_array = decimal_array
                                .unary::<_, arrow::datatypes::Decimal128Type>(
                                |value: i128| div_ceil(value, factor),
                            );
                            let decimal_result = result_array
                                .with_precision_and_scale(new_precision, 0)?;
                            let float_values: Vec<Option<f64>> = decimal_result
                                .iter()
                                .map(|v| v.map(|x| x as f64))
                                .collect();
                            Ok(Arc::new(Float64Array::from(float_values)))
                        }
                    }
                }
            } else {
                Ok(Arc::clone(&args[0]))
            }
        }
        _ => exec_err!(
            "ceil expects a numeric argument, got {}",
            args[0].data_type()
        ),
    }
}

#[inline]
fn div_ceil(a: i128, b: i128) -> i128 {
    if b == 0 {
        panic!("division by zero");
    }
    let div = a / b;
    let rem = a % b;
    if rem != 0 && ((b > 0 && a > 0) || (b < 0 && a < 0)) {
        div + 1
    } else {
        div
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use datafusion_common::Result;
    use std::sync::Arc;

    #[test]
    fn test_ceil_f32_array() -> Result<()> {
        let input = vec![Some(125.2345_f32), Some(-1.1_f32), None];
        let array = Arc::new(Float32Array::from(input)) as ArrayRef;
        let result = spark_ceil(&[array])?;
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 126);
        assert_eq!(result_array.value(1), -1);
        Ok(())
    }

    #[test]
    fn test_ceil_f64_array() -> Result<()> {
        let input = vec![Some(3.3281_f64), Some(-2.1_f64), None];
        let array = Arc::new(Float64Array::from(input)) as ArrayRef;
        let result = spark_ceil(&[array])?;
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 4);
        assert_eq!(result_array.value(1), -2);
        Ok(())
    }

    #[test]
    fn test_ceil_i64_array() -> Result<()> {
        let input = vec![Some(42_i64), Some(-15_i64), None];
        let array = Arc::new(Int64Array::from(input)) as ArrayRef;
        let result = spark_ceil(&[array])?;
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 42);
        assert_eq!(result_array.value(1), -15);
        Ok(())
    }

    #[test]
    fn test_ceil_decimal_array() -> Result<()> {
        let input = vec![Some(115_i128), Some(-267_i128), None];
        let array =
            Arc::new(Decimal128Array::from(input).with_precision_and_scale(10, 2)?)
                as ArrayRef;
        let result = spark_ceil(&[array])?;
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 2);
        assert_eq!(result_array.value(1), -2);
        Ok(())
    }

    #[test]
    fn test_ceil_with_scale() -> Result<()> {
        let input = vec![Some(3.24792_f64), Some(2.71324_f64)];
        let value_array = Arc::new(Float64Array::from(input)) as ArrayRef;
        let scale_array = Arc::new(Int64Array::from(vec![Some(2_i64)])) as ArrayRef;
        let result = spark_ceil(&[value_array, scale_array])?;
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_array.value(0), 3.25);
        assert_eq!(result_array.value(1), 2.72);
        Ok(())
    }

    #[test]
    fn test_ceil_float32_with_scale() -> Result<()> {
        let input = vec![Some(1.234_f32), Some(-2.567_f32)];
        let value_array = Arc::new(Float32Array::from(input)) as ArrayRef;
        let scale_array = Arc::new(Int64Array::from(vec![Some(1_i64)])) as ArrayRef;
        let result = spark_ceil(&[value_array, scale_array])?;
        let result_array = result.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(result_array.value(0), 1.3);
        assert_eq!(result_array.value(1), -2.5);
        Ok(())
    }

    #[test]
    fn test_ceil_float64_with_scale_3() -> Result<()> {
        let input = vec![Some(4.1418_f64)];
        let value_array = Arc::new(Float64Array::from(input)) as ArrayRef;
        let scale_array = Arc::new(Int64Array::from(vec![Some(3_i64)])) as ArrayRef;
        let result = spark_ceil(&[value_array, scale_array])?;
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_array.value(0), 4.142);
        Ok(())
    }

    #[test]
    fn test_ceil_with_negative_scale() -> Result<()> {
        let input = vec![Some(-12.345_f64)];
        let value_array = Arc::new(Float64Array::from(input)) as ArrayRef;
        let scale_array = Arc::new(Int64Array::from(vec![Some(1_i64)])) as ArrayRef;
        let result = spark_ceil(&[value_array, scale_array])?;
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_array.value(0), -12.3);
        Ok(())
    }

    #[test]
    // this scenario passes as UT but fails in slt.
    fn test_ceil_decimal_with_scale() -> Result<()> {
        let input = vec![Some(31411_i128), Some(-12345_i128)];
        let value_array =
            Arc::new(Decimal128Array::from(input).with_precision_and_scale(5, 4)?)
                as ArrayRef;
        let scale_array = Arc::new(Int64Array::from(vec![Some(3_i64)])) as ArrayRef;
        let result = spark_ceil(&[value_array, scale_array])?;
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_array.value(0), 3.142); // ceil(3.1411, 3) = 3.142
        assert_eq!(result_array.value(1), -1.234); // ceil(-1.2345, 3) = -1.234
        Ok(())
    }
}
