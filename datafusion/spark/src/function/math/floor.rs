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
use arrow::array::{ArrowNativeTypeOp, Decimal128Array, Int64Array};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use std::any::Any;
use std::sync::Arc;

/// Spark-compatible `floor` function.
///
/// Differences from DataFusion's floor:
/// - Returns Int64 for float and integer inputs (while DataFusion preserves input type)
/// - For Decimal128(p, s), returns Decimal128(p-s+1, 0) with scale 0
///   (DataFusion preserves original precision and scale)
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#floor>
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
            DataType::Decimal128(p, s) if *s > 0 => {
                let new_p = (*p - *s as u8 + 1).clamp(1, 38);
                DataType::Decimal128(new_p, 0)
            }
            DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
            DataType::Float32 | DataType::Float64 => DataType::Int64,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                DataType::Int64
            }
            _ => exec_err!(
                "found unsupported return type {:?}",
                args.arg_fields[0].data_type()
            )?,
        };
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        spark_floor(&args.args, args.return_field.data_type())
    }
}

macro_rules! apply_int64 {
    ($value:expr, $arr_type:ty, $scalar_variant:path, $f:expr) => {
        match $value {
            ColumnarValue::Array(array) => {
                let result: Int64Array = unary(array.as_primitive::<$arr_type>(), $f);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar($scalar_variant(v)) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v.map($f))))
            }
            other => internal_err!(
                "floor: expected {} but got {:?}",
                stringify!($scalar_variant),
                other.data_type()
            ),
        }
    };
}

fn spark_floor(
    args: &[ColumnarValue],
    return_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value.data_type() {
        DataType::Float32 => apply_int64!(
            value,
            arrow::datatypes::Float32Type,
            ScalarValue::Float32,
            |x| x.floor() as i64
        ),
        DataType::Float64 => apply_int64!(
            value,
            arrow::datatypes::Float64Type,
            ScalarValue::Float64,
            |x| x.floor() as i64
        ),
        DataType::Int8 => {
            apply_int64!(value, arrow::datatypes::Int8Type, ScalarValue::Int8, |x| x
                as i64)
        }
        DataType::Int16 => apply_int64!(
            value,
            arrow::datatypes::Int16Type,
            ScalarValue::Int16,
            |x| x as i64
        ),
        DataType::Int32 => apply_int64!(
            value,
            arrow::datatypes::Int32Type,
            ScalarValue::Int32,
            |x| x as i64
        ),
        DataType::Int64 => Ok(value.clone()),
        DataType::Decimal128(_, scale) if scale > 0 => {
            let divisor = 10_i128.pow_wrapping(scale as u32);
            let floor_decimal = |x: i128| {
                let (d, r) = (x / divisor, x % divisor);
                if r < 0 { d - 1 } else { d }
            };
            match value {
                ColumnarValue::Array(array) => {
                    let result: Decimal128Array =
                        unary(array.as_primitive::<Decimal128Type>(), floor_decimal);
                    Ok(ColumnarValue::Array(Arc::new(
                        result.with_data_type(return_type.clone()),
                    )))
                }
                ColumnarValue::Scalar(ScalarValue::Decimal128(v, _, _)) => {
                    let DataType::Decimal128(new_p, new_s) = return_type else {
                        return internal_err!(
                            "floor: expected Decimal128 return type but got {:?}",
                            return_type
                        );
                    };
                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        v.map(floor_decimal),
                        *new_p,
                        *new_s,
                    )))
                }
                other => internal_err!(
                    "floor: expected Decimal128 scalar but got {:?}",
                    other.data_type()
                ),
            }
        }
        DataType::Decimal128(_, _) => Ok(value.clone()),
        other => exec_err!("Unsupported data type {other:?} for function floor"),
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
    fn test_floor_float32_array() -> Result<()> {
        let array = Float32Array::from(vec![
            Some(1.1),
            Some(1.9),
            Some(-1.1),
            Some(-1.9),
            Some(0.0),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Int64)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), -2);
        assert_eq!(result.value(3), -2);
        assert_eq!(result.value(4), 0);
        assert!(result.is_null(5));
        Ok(())
    }

    #[test]
    fn test_floor_float32_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float32(Some(1.5)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, 1);
        Ok(())
    }

    #[test]
    fn test_floor_float64_array() -> Result<()> {
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
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Int64)? else {
            unreachable!()
        };
        let result = as_int64_array(&result)?;
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), -2);
        assert_eq!(result.value(3), -2);
        assert_eq!(result.value(4), 0);
        assert_eq!(result.value(5), 123);
        assert!(result.is_null(6));
        Ok(())
    }

    #[test]
    fn test_floor_float64_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(Some(-1.5)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, -2);
        Ok(())
    }

    #[test]
    fn test_floor_float64_null_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Float64(None))];
        let ColumnarValue::Scalar(ScalarValue::Int64(result)) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_floor_int8_array() -> Result<()> {
        let array = Int8Array::from(vec![Some(1), Some(-1), Some(127), Some(-128), None]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Int64)? else {
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
    fn test_floor_int16_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int16(Some(100)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, 100);
        Ok(())
    }

    #[test]
    fn test_floor_int32_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(-500)))];
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(result))) =
            spark_floor(&args, &DataType::Int64)?
        else {
            unreachable!()
        };
        assert_eq!(result, -500);
        Ok(())
    }

    #[test]
    fn test_floor_int64_array() -> Result<()> {
        let array = Int64Array::from(vec![
            Some(1),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]);
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let ColumnarValue::Array(result) = spark_floor(&args, &DataType::Int64)? else {
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
    fn test_floor_decimal128_array() -> Result<()> {
        let array =
            Decimal128Array::from(vec![Some(12345), Some(12500), Some(-12999), None])
                .with_precision_and_scale(5, 2)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let return_type = DataType::Decimal128(4, 0);
        let ColumnarValue::Array(result) = spark_floor(&args, &return_type)? else {
            unreachable!()
        };
        let expected =
            Decimal128Array::from(vec![Some(123), Some(125), Some(-130), None])
                .with_precision_and_scale(4, 0)?;
        let actual = as_decimal128_array(&result)?;
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(567),
            3,
            1,
        ))];
        let return_type = DataType::Decimal128(3, 0);
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 0)) =
            spark_floor(&args, &return_type)?
        else {
            unreachable!()
        };
        assert_eq!(result, 56);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_negative_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(-567),
            3,
            1,
        ))];
        let return_type = DataType::Decimal128(3, 0);
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 3, 0)) =
            spark_floor(&args, &return_type)?
        else {
            unreachable!()
        };
        assert_eq!(result, -57);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_null_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(None, 5, 2))];
        let return_type = DataType::Decimal128(4, 0);
        let ColumnarValue::Scalar(ScalarValue::Decimal128(result, 4, 0)) =
            spark_floor(&args, &return_type)?
        else {
            unreachable!()
        };
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_scale_zero() -> Result<()> {
        let array = Decimal128Array::from(vec![Some(123), Some(-456), None])
            .with_precision_and_scale(10, 0)?;
        let args = vec![ColumnarValue::Array(Arc::new(array))];
        let return_type = DataType::Decimal128(10, 0);
        let ColumnarValue::Array(result) = spark_floor(&args, &return_type)? else {
            unreachable!()
        };
        let result = as_decimal128_array(&result)?;
        assert_eq!(result.value(0), 123);
        assert_eq!(result.value(1), -456);
        assert!(result.is_null(2));
        Ok(())
    }

    #[test]
    fn test_floor_decimal128_scale_zero_scalar() -> Result<()> {
        let args = vec![ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(12345),
            10,
            0,
        ))];
        let return_type = DataType::Decimal128(10, 0);
        let ColumnarValue::Scalar(ScalarValue::Decimal128(Some(result), 10, 0)) =
            spark_floor(&args, &return_type)?
        else {
            unreachable!()
        };
        assert_eq!(result, 12345);
        Ok(())
    }
}
