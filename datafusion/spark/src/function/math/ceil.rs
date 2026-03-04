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
use arrow::array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}

pub fn spark_ceil(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result =
                    downcast_compute_op!(array, "ceil", ceil, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result =
                    downcast_compute_op!(array, "ceil", ceil, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int8 => {
                let input = array.as_any().downcast_ref::<Int8Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int16 => {
                let input = array.as_any().downcast_ref::<Int16Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int32 => {
                let input = array.as_any().downcast_ref::<Int32Array>().unwrap();
                let result: Int64Array = unary(input, |x| x as i64);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::Int64 => {
                // Optimization: Int64 -> Int64 doesn't need conversion, just return same array
                Ok(ColumnarValue::Array(Arc::clone(array)))
            }
            DataType::Decimal128(precision, scale) if *scale > 0 => {
                let f = decimal_ceil_f(*scale);
                make_decimal_array(array, *precision, *scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function ceil",
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Int8(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int16(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(*a))),
            ScalarValue::Decimal128(a, precision, scale) if *scale > 0 => {
                let f = decimal_ceil_f(*scale);
                let result = a.map(f);
                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    result, *precision, *scale,
                )))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                value.data_type(),
            ))),
        },
    }
}

/// Computes ceil for a Decimal128 array, preserving the scale.
/// Divides by 10^scale, takes ceiling, then multiplies back.
#[inline]
fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Returns a closure that computes ceil for decimal values.
#[inline]
fn decimal_ceil_f(scale: i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow(scale as u32);
    move |x: i128| {
        let d = x / div;
        let r = x % div;
        // Ceiling: round up for positive remainders
        (if r > 0 { d + 1 } else { d }) * div
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCiel {
    signature: Signature,
}

impl Default for SparkCiel {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkCiel {
    pub fn new() -> Self {
        Self {
            signature: Signature::numeric(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkCiel {
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
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        spark_ceil(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use datafusion_common::Result;
    use datafusion_common::cast::as_decimal128_array;

    #[test]
    fn test_ceil_decimal128_array() -> Result<()> {
        let array = Decimal128Array::from(vec![
            Some(12345),  // 123.45
            Some(12500),  // 125.00
            Some(-12999), // -129.99
            None,
        ])
        .with_precision_and_scale(5, 2)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_ceil(&args)? else {
            unreachable!()
        };
        let expected = Decimal128Array::from(vec![
            Some(12400),  // 124.00
            Some(12500),  // 125.00
            Some(-12900), // -129.00
            None,
        ])
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
        ))]; // 56.7
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 1)) =
            spark_ceil(&args)?
        else {
            unreachable!()
        };
        assert_eq!(result, 570); // 57.0
        Ok(())
    }

    #[test]
    fn test_ceil_decimal128_negative_scalar() -> Result<()> {
        // -56.7 should ceil to -56.0
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
        assert_eq!(result, -560); // -56.0
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
}
