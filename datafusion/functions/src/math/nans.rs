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

//! Math function: `isnan()`.

use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::DataType::{
    Decimal32, Decimal64, Decimal128, Decimal256, Float16, Float32, Float64, Int8, Int16,
    Int32, Int64, Null, UInt8, UInt16, UInt32, UInt64,
};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float16Type,
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns true if a given number is +NaN or -NaN otherwise returns false.",
    syntax_example = "isnan(numeric_expression)",
    sql_example = r#"```sql
> SELECT isnan(1);
+----------+
| isnan(1) |
+----------+
| false    |
+----------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsNanFunc {
    signature: Signature,
}

impl Default for IsNanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNanFunc {
    pub fn new() -> Self {
        // Accept any numeric type (ints, uints, floats, decimals) without implicit casts.
        let numeric = Coercion::new_exact(TypeSignatureClass::Numeric);
        Self {
            signature: Signature::coercible(vec![numeric], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsNanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
                }

                let result = match scalar {
                    ScalarValue::Float64(Some(v)) => Some(v.is_nan()),
                    ScalarValue::Float32(Some(v)) => Some(v.is_nan()),
                    ScalarValue::Float16(Some(v)) => Some(v.is_nan()),

                    // Non-float numeric inputs are never NaN
                    ScalarValue::Int8(_)
                    | ScalarValue::Int16(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt8(_)
                    | ScalarValue::UInt16(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Decimal32(_, _, _)
                    | ScalarValue::Decimal64(_, _, _)
                    | ScalarValue::Decimal128(_, _, _)
                    | ScalarValue::Decimal256(_, _, _) => Some(false),

                    other => {
                        return exec_err!(
                            "Unsupported data type {other:?} for function {}",
                            self.name()
                        );
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(result)))
            }
            ColumnarValue::Array(array) => {
                // NOTE: BooleanArray::from_unary preserves nulls.
                let arr: ArrayRef = match array.data_type() {
                    Null => Arc::new(BooleanArray::new_null(array.len())) as ArrayRef,

                    Float64 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Float64Type>(),
                        f64::is_nan,
                    )) as ArrayRef,
                    Float32 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Float32Type>(),
                        f32::is_nan,
                    )) as ArrayRef,
                    Float16 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Float16Type>(),
                        |x| x.is_nan(),
                    )) as ArrayRef,

                    // Non-float numeric arrays are never NaN
                    Decimal32(_, _) => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal32Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Decimal64(_, _) => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal64Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Decimal128(_, _) => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal128Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Decimal256(_, _) => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Decimal256Type>(),
                        |_| false,
                    )) as ArrayRef,

                    Int8 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Int8Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Int16 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Int16Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Int32 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Int32Type>(),
                        |_| false,
                    )) as ArrayRef,
                    Int64 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<Int64Type>(),
                        |_| false,
                    )) as ArrayRef,
                    UInt8 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<UInt8Type>(),
                        |_| false,
                    )) as ArrayRef,
                    UInt16 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<UInt16Type>(),
                        |_| false,
                    )) as ArrayRef,
                    UInt32 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<UInt32Type>(),
                        |_| false,
                    )) as ArrayRef,
                    UInt64 => Arc::new(BooleanArray::from_unary(
                        array.as_primitive::<UInt64Type>(),
                        |_| false,
                    )) as ArrayRef,

                    other => {
                        return exec_err!(
                            "Unsupported data type {other:?} for function {}",
                            self.name()
                        );
                    }
                };

                Ok(ColumnarValue::Array(arr))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
