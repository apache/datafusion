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
            DataType::Float32
            | DataType::Float64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => DataType::Int64,
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
                "floor: data type mismatch — expected scalar of type {} but got {:?}",
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
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            value.cast_to(&DataType::Int64, None)
        }
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
                            "floor: data type mismatch — expected Decimal128 return type but got {:?}",
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
                    "floor: data type mismatch — expected Decimal128 scalar but got {:?}",
                    other.data_type()
                ),
            }
        }
        DataType::Decimal128(_, _) => Ok(value.clone()),
        other => exec_err!("Unsupported data type {other:?} for function floor"),
    }
}
