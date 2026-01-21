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
use arrow::datatypes::DataType::{Boolean, Float16, Float32, Float64};
use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};

use datafusion_common::types::NativeType;
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
        // Accept any numeric type and coerce to float
        let float = Coercion::new_implicit(
            TypeSignatureClass::Float,
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::coercible(vec![float], Volatility::Immutable),
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
                    _ => {
                        internal_err!(
                            "Unexpected scalar type for iszero: {:?}",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
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
