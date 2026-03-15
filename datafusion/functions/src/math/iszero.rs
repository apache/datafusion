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

use arrow::array::{ArrowNativeTypeOp, AsArray, BooleanArray};
use arrow::datatypes::DataType::{
    Boolean, Decimal32, Decimal64, Decimal128, Decimal256, Float16, Float32, Float64,
    Int8, Int16, Int32, Int64, Null, UInt8, UInt16, UInt32, UInt64,
};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float16Type,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};

use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{Coercion, TypeSignatureClass};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns true if a given number is +0.0 or -0.0 otherwise returns false.",
    syntax_example = "iszero(numeric_expression)",
    sql_example = r#"```sql
> SELECT iszero(0);
+------------+
| iszero(0)  |
+------------+
| true       |
+------------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsZeroFunc {
    signature: Signature,
}

impl Default for IsZeroFunc {
    fn default() -> Self {
        IsZeroFunc::new()
    }
}

impl IsZeroFunc {
    pub fn new() -> Self {
        // Accept any numeric type (ints, uints, floats, decimals) without implicit casts.
        let numeric = Coercion::new_exact(TypeSignatureClass::Numeric);
        Self {
            signature: Signature::coercible(vec![numeric], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsZeroFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "iszero"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                match scalar {
                    ScalarValue::Float64(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0.0))))
                    }
                    ScalarValue::Float32(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0.0))))
                    }
                    ScalarValue::Float16(Some(v)) => Ok(ColumnarValue::Scalar(
                        ScalarValue::Boolean(Some(v.is_zero())),
                    )),

                    ScalarValue::Int8(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Int16(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Int32(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Int64(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::UInt8(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::UInt16(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::UInt32(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::UInt64(Some(v)) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }

                    ScalarValue::Decimal32(Some(v), ..) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Decimal64(Some(v), ..) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Decimal128(Some(v), ..) => {
                        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v == 0))))
                    }
                    ScalarValue::Decimal256(Some(v), ..) => Ok(ColumnarValue::Scalar(
                        ScalarValue::Boolean(Some(v.is_zero())),
                    )),

                    _ => {
                        internal_err!(
                            "Unexpected scalar type for iszero: {:?}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
                Null => Ok(ColumnarValue::Array(Arc::new(BooleanArray::new_null(
                    array.len(),
                )))),

                Float64 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Float64Type>(),
                    |x| x == 0.0,
                )))),
                Float32 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Float32Type>(),
                    |x| x == 0.0,
                )))),
                Float16 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Float16Type>(),
                    |x| x.is_zero(),
                )))),

                Int8 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Int8Type>(),
                    |x| x == 0,
                )))),
                Int16 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Int16Type>(),
                    |x| x == 0,
                )))),
                Int32 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Int32Type>(),
                    |x| x == 0,
                )))),
                Int64 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<Int64Type>(),
                    |x| x == 0,
                )))),
                UInt8 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<UInt8Type>(),
                    |x| x == 0,
                )))),
                UInt16 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<UInt16Type>(),
                    |x| x == 0,
                )))),
                UInt32 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<UInt32Type>(),
                    |x| x == 0,
                )))),
                UInt64 => Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                    array.as_primitive::<UInt64Type>(),
                    |x| x == 0,
                )))),

                Decimal32(_, _) => {
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal32Type>(),
                        |x| x == 0,
                    ))))
                }
                Decimal64(_, _) => {
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal64Type>(),
                        |x| x == 0,
                    ))))
                }
                Decimal128(_, _) => {
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal128Type>(),
                        |x| x == 0,
                    ))))
                }
                Decimal256(_, _) => {
                    Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal256Type>(),
                        |x| x.is_zero(),
                    ))))
                }

                other => {
                    internal_err!("Unexpected data type {other:?} for function iszero")
                }
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
