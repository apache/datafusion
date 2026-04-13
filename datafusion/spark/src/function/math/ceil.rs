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
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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
/// 2-argument ceil(value, scale) is not yet implemented
/// <https://github.com/apache/datafusion/issues/21560>
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
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
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
    let [input] = take_function_args("ceil", args)?;

    match input {
        ColumnarValue::Scalar(value) => spark_ceil_scalar(value),
        ColumnarValue::Array(input) => spark_ceil_array(input),
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
}
