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
use arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float32Type, Float64Type, Int64Type,
};
use datafusion_common::types::{NativeType, logical_int32};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
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
/// `ceil(value, scale)` rounds up to `scale` decimal places. Spark declares this form as
/// `RoundCeil(DecimalType, IntegerType)`, so the value is a decimal and `scale` must be a
/// constant integer. The result type follows Spark's `RoundBase::dataType`:
///
/// - `scale < 0`  -> `Decimal128(min(max(p - s + 1, -scale + 1), 38), 0)`
/// - `scale >= 0` -> `Decimal128(min(p - s + 1 + min(s, scale), 38), min(s, scale))`
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
        let scale = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int32()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int32,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    // ceil(value)
                    TypeSignature::Numeric(1),
                    // ceil(decimal, scale)
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Decimal),
                        scale,
                    ]),
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let value_type = args.arg_fields[0].data_type();

        let data_type = if args.arg_fields.len() == 1 {
            self.return_type(std::slice::from_ref(value_type))?
        } else {
            // Spark requires a foldable scale: a non-constant one is a plan-time error there
            // (NON_FOLDABLE_INPUT) rather than something evaluated per row.
            let scale = match args.scalar_arguments.get(1).copied().flatten() {
                Some(ScalarValue::Int32(Some(scale))) => *scale,
                Some(ScalarValue::Int32(None)) => {
                    return plan_err!("Function ceil requires a non-null scale argument");
                }
                _ => {
                    return plan_err!(
                        "Function ceil requires a constant integer scale argument"
                    );
                }
            };

            match value_type {
                DataType::Decimal128(p, s) => DataType::Decimal128(
                    ceil_scaled_precision(*p, *s, scale),
                    ceil_scaled_scale(*s, scale),
                ),
                other => {
                    return plan_err!(
                        "Function ceil does not support a scale argument for {other:?}"
                    );
                }
            }
        };

        Ok(Arc::new(Field::new("ceil", data_type, nullable)))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Decimal128(p, s) => {
                if *s > 0 {
                    Ok(DataType::Decimal128(decimal128_ceil_precision(*p, *s), 0))
                } else {
                    // scale <= 0 means the value is already a whole number
                    // (or represents multiples of 10^(-scale)), so ceil is a no-op
                    Ok(DataType::Decimal128(*p, *s))
                }
            }
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

fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match args {
        [input] => match input {
            ColumnarValue::Scalar(value) => spark_ceil_scalar(value),
            ColumnarValue::Array(input) => spark_ceil_array(input),
        },
        [input, scale] => spark_ceil_with_scale(input, scale),
        _ => exec_err!("Function ceil expects one or two arguments"),
    }
}

/// Result scale for `ceil(decimal(p, s), scale)`, per Spark's `RoundBase::dataType`.
#[inline]
fn ceil_scaled_scale(input_scale: i8, scale: i32) -> i8 {
    if scale < 0 {
        0
    } else {
        (input_scale as i32).min(scale) as i8
    }
}

/// Result precision for `ceil(decimal(p, s), scale)`, per Spark's `RoundBase::dataType`.
///
/// Rounding up can carry into a new integral digit (`ceil(9.9, 0)` is `10`), so the integral
/// part always gets one digit more than the input had.
#[inline]
fn ceil_scaled_precision(precision: u8, input_scale: i8, scale: i32) -> u8 {
    let integral_least_num_digits = precision as i32 - input_scale as i32 + 1;
    let new_precision = if scale < 0 {
        integral_least_num_digits.max(-scale + 1)
    } else {
        integral_least_num_digits + (input_scale as i32).min(scale)
    };
    new_precision.clamp(1, 38) as u8
}

/// Round `value` up to `target_scale` decimal places, where `value` is held at `input_scale`.
#[inline]
fn decimal128_ceil_to_scale(value: i128, input_scale: i8, target_scale: i32) -> i128 {
    if target_scale >= input_scale as i32 {
        // Already finer-grained than the target, so there is nothing to drop.
        return value;
    }

    let dropped = (input_scale as i32 - target_scale) as u32;
    let rounded = decimal128_ceil(value, dropped);

    if target_scale < 0 {
        // The result is held at scale 0, so put the trailing zeros back.
        rounded.mul_wrapping(10_i128.pow_wrapping((-target_scale) as u32))
    } else {
        rounded
    }
}

fn spark_ceil_with_scale(
    input: &ColumnarValue,
    scale: &ColumnarValue,
) -> Result<ColumnarValue> {
    let ColumnarValue::Scalar(scale) = scale else {
        return exec_err!("Function ceil requires a constant integer scale argument");
    };
    let ScalarValue::Int32(Some(scale)) = scale else {
        return exec_err!("Function ceil requires a non-null integer scale argument");
    };
    let scale = *scale;

    match input {
        ColumnarValue::Scalar(ScalarValue::Decimal128(value, p, s)) => {
            let result = ScalarValue::Decimal128(
                value.map(|x| decimal128_ceil_to_scale(x, *s, scale)),
                ceil_scaled_precision(*p, *s, scale),
                ceil_scaled_scale(*s, scale),
            );
            Ok(ColumnarValue::Scalar(result))
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Decimal128(p, s) => {
                let (p, s) = (*p, *s);
                let result: Decimal128Array = array
                    .as_primitive::<Decimal128Type>()
                    .unary(|x| decimal128_ceil_to_scale(x, s, scale));
                Ok(ColumnarValue::Array(Arc::new(result.with_data_type(
                    DataType::Decimal128(
                        ceil_scaled_precision(p, s, scale),
                        ceil_scaled_scale(s, scale),
                    ),
                ))))
            }
            other => {
                exec_err!("Function ceil does not support a scale argument for {other:?}")
            }
        },
        other => exec_err!(
            "Function ceil does not support a scale argument for {:?}",
            other.data_type()
        ),
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

fn spark_ceil_scalar(value: &ScalarValue) -> Result<ColumnarValue> {
    let result = match value {
        ScalarValue::Float32(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        ScalarValue::Float64(v) => ScalarValue::Int64(v.map(|x| x.ceil() as i64)),
        v if v.data_type().is_integer() => v.cast_to(&DataType::Int64)?,
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

fn spark_ceil_array(input: &Arc<dyn arrow::array::Array>) -> Result<ColumnarValue> {
    let result = match input.data_type() {
        DataType::Float32 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
        ) as _,
        DataType::Float64 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Int64Type>(|x| x.ceil() as i64),
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

#[cfg(test)]
mod scale_tests {
    use super::*;

    /// Reference values captured from PySpark 3.5.5, as recorded in
    /// `datafusion/sqllogictest/test_files/spark/math/ceil.slt`.
    #[test]
    fn matches_pyspark_for_the_documented_cases() {
        // SELECT ceil(3.1411, 3) -> Decimal('3.142'), decimal(5,3)
        let value = 31411_i128; // 3.1411 at scale 4
        assert_eq!(decimal128_ceil_to_scale(value, 4, 3), 3142);
        assert_eq!(ceil_scaled_precision(5, 4, 3), 5);
        assert_eq!(ceil_scaled_scale(4, 3), 3);

        // SELECT ceil(3.1411, -3) -> Decimal('1000'), decimal(4,0)
        assert_eq!(decimal128_ceil_to_scale(value, 4, -3), 1000);
        assert_eq!(ceil_scaled_precision(5, 4, -3), 4);
        assert_eq!(ceil_scaled_scale(4, -3), 0);
    }

    #[test]
    fn rounds_up_toward_positive_infinity() {
        // -3.1411 to three places is -3.141, which is larger than the input.
        assert_eq!(decimal128_ceil_to_scale(-31411, 4, 3), -3141);
        // and to zero places it is -3.
        assert_eq!(decimal128_ceil_to_scale(-31411, 4, 0), -3);
    }

    #[test]
    fn a_scale_at_or_beyond_the_input_scale_is_a_no_op() {
        assert_eq!(decimal128_ceil_to_scale(31411, 4, 4), 31411);
        assert_eq!(decimal128_ceil_to_scale(31411, 4, 9), 31411);
        // the result keeps the input scale rather than growing to the requested one
        assert_eq!(ceil_scaled_scale(4, 9), 4);
        assert_eq!(ceil_scaled_precision(5, 4, 9), 6);
    }

    #[test]
    fn an_exact_value_does_not_round_up() {
        // 3.1400 to two places is exactly 3.14, no carry.
        assert_eq!(decimal128_ceil_to_scale(31400, 4, 2), 314);
        // 9.9 to zero places carries into a new integral digit.
        assert_eq!(decimal128_ceil_to_scale(99, 1, 0), 10);
        assert_eq!(ceil_scaled_precision(2, 1, 0), 2);
    }

    #[test]
    fn precision_is_capped_at_the_decimal128_maximum() {
        assert_eq!(ceil_scaled_precision(38, 0, -40), 38);
        assert_eq!(ceil_scaled_precision(38, 2, 30), 38);
    }

    #[test]
    fn precision_is_never_below_one() {
        assert_eq!(ceil_scaled_precision(1, 1, 0), 1);
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
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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
        let ColumnarValue::Array(result) = result else {
            panic!("Expected array")
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
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(-1)));
    }

    #[test]
    fn test_ceil_float32_scalar() {
        let input = ScalarValue::Float32(Some(125.2345f32));
        let args = vec![ColumnarValue::Scalar(input)];
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(126)));
    }

    #[test]
    fn test_ceil_int64_scalar() {
        let input = ScalarValue::Int64(Some(48));
        let args = vec![ColumnarValue::Scalar(input)];
        let ColumnarValue::Scalar(result) = spark_ceil(&args).unwrap() else {
            panic!("Expected scalar")
        };
        assert_eq!(result, ScalarValue::Int64(Some(48)));
    }
}
