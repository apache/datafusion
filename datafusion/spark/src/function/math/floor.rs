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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{AsArray, Decimal128Array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark-compatible `floor` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#floor>
///
/// Differences with DataFusion floor:
///  - Spark's floor returns Int64 for float/integer types
///  - Spark's floor adjusts precision for Decimal128 types
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFloor {
    signature: Signature,
}

impl Default for SparkFloor {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkFloor {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkFloor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Decimal128(p, s) if *s > 0 => {
                let new_p = ((*p as i64) - (*s as i64) + 1).clamp(1, 38) as u8;
                Ok(DataType::Decimal128(new_p, 0))
            }
            DataType::Decimal128(p, s) => Ok(DataType::Decimal128(*p, *s)),
            _ => Ok(DataType::Int64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let return_type = args.return_type().clone();
        spark_floor(&args.args, &return_type)
    }
}

fn spark_floor(args: &[ColumnarValue], return_type: &DataType) -> Result<ColumnarValue> {
    let input = match take_function_args("floor", args)? {
        [ColumnarValue::Scalar(value)] => value.to_array()?,
        [ColumnarValue::Array(arr)] => Arc::clone(arr),
    };

    let result = match input.data_type() {
        DataType::Float32 => Arc::new(
            input
                .as_primitive::<Float32Type>()
                .unary::<_, Int64Type>(|x| x.floor() as i64),
        ) as _,
        DataType::Float64 => Arc::new(
            input
                .as_primitive::<Float64Type>()
                .unary::<_, Int64Type>(|x| x.floor() as i64),
        ) as _,
        dt if dt.is_integer() => cast(&input, &DataType::Int64)?,
        DataType::Decimal128(_, s) if *s > 0 => {
            let div = 10_i128.pow(*s as u32);
            let result: Decimal128Array =
                input.as_primitive::<Decimal128Type>().unary(|x| {
                    let d = x / div;
                    let r = x % div;
                    if r < 0 { d - 1 } else { d }
                });
            Arc::new(result.with_data_type(return_type.clone()))
        }
        DataType::Decimal128(_, _) => input,
        other => return exec_err!("Unsupported data type {other:?} for function floor"),
    };

    Ok(ColumnarValue::Array(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Float32Array, Float64Array, Int64Array};
    use datafusion_common::ScalarValue;

    #[test]
    fn test_floor_float64() {
        let input = Float64Array::from(vec![Some(1.9), Some(-1.1), Some(0.0), None]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_floor(&args, &DataType::Int64).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(
            result,
            &Int64Array::from(vec![Some(1), Some(-2), Some(0), None])
        );
    }

    #[test]
    fn test_floor_float32() {
        let input = Float32Array::from(vec![Some(1.5f32), Some(-1.5f32)]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_floor(&args, &DataType::Int64).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result, &Int64Array::from(vec![Some(1), Some(-2)]));
    }

    #[test]
    fn test_floor_int64() {
        let input = Int64Array::from(vec![Some(1), Some(-1), None]);
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_floor(&args, &DataType::Int64).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result, &Int64Array::from(vec![Some(1), Some(-1), None]));
    }

    #[test]
    fn test_floor_decimal128() {
        // Decimal128(10, 2): 150 = 1.50, -150 = -1.50, 100 = 1.00
        let return_type = DataType::Decimal128(9, 0);
        let input = Decimal128Array::from(vec![Some(150), Some(-150), Some(100), None])
            .with_data_type(DataType::Decimal128(10, 2));
        let args = vec![ColumnarValue::Array(Arc::new(input))];
        let result = spark_floor(&args, &return_type).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Decimal128Type>();
        let expected = Decimal128Array::from(vec![Some(1), Some(-2), Some(1), None])
            .with_data_type(return_type);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_floor_scalar() {
        let input = ScalarValue::Float64(Some(1.9));
        let args = vec![ColumnarValue::Scalar(input)];
        let result = spark_floor(&args, &DataType::Int64).unwrap();
        let result = match result {
            ColumnarValue::Array(arr) => arr,
            _ => panic!("Expected array"),
        };
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result, &Int64Array::from(vec![Some(1)]));
    }
}
