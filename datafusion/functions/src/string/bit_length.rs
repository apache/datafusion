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

use arrow::compute::kernels::length::bit_length;
use arrow::datatypes::DataType;
use std::any::Any;

use crate::utils::utf8_to_int_type;
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use arrow::array::{
    Array, StringArray, LargeStringArray, Int32Builder,
};
use std::sync::Arc;


#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the bit length of a string.",
    syntax_example = "bit_length(str)",
    sql_example = r#"```sql
> select bit_length('datafusion');
+--------------------------------+
| bit_length(Utf8("datafusion")) |
+--------------------------------+
| 80                             |
+--------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "length"),
    related_udf(name = "octet_length")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BitLengthFunc {
    signature: Signature,
}

impl Default for BitLengthFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl BitLengthFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![Coercion::new_exact(TypeSignatureClass::Native(
                    logical_string(),
                ))],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BitLengthFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_int_type(&arg_types[0], "bit_length")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [array] = take_function_args(self.name(), &args.args)?;

        match array {
            ColumnarValue::Array(v) => {
                if let Some(arr) = v.as_any().downcast_ref::<StringArray>() {
                    let mut builder = Int32Builder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let byte_len = arr.value(i).as_bytes().len();
                            builder.append_value((byte_len * 8) as i32);
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                } else if let Some(arr) = v.as_any().downcast_ref::<LargeStringArray>() {
                    let mut builder = Int32Builder::with_capacity(arr.len());
                    for i in 0..arr.len() {
                        if arr.is_null(i) {
                            builder.append_null();
                        } else {
                            let byte_len = arr.value(i).as_bytes().len();
                            builder.append_value((byte_len * 8) as i32);
                        }
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                } else {
                    // fallback for Utf8View, Dictionary, Binary, etc.
                    Ok(ColumnarValue::Array(bit_length(v.as_ref())?))
                }
            }
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    v.as_ref().map(|x| (x.len() * 8) as i64),
                ))),
                ScalarValue::Utf8View(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                _ => unreachable!("bit length"),
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
