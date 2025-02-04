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
use datafusion_common::types::{LogicalType, NativeType};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

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
    standard_argument(name = "str", prefix = "Coercible String"),
    related_udf(name = "length"),
    related_udf(name = "octet_length")
)]
#[derive(Debug)]
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
            signature: Signature::user_defined(Volatility::Immutable),
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

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        if args.len() != 1 {
            return exec_err!(
                "bit_length function requires 1 argument, got {}",
                args.len()
            );
        }

        match &args[0] {
            ColumnarValue::Array(v) => Ok(ColumnarValue::Array(bit_length(v.as_ref())?)),
            ColumnarValue::Scalar(v) => match v {
                ScalarValue::Utf8(v) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(
                    v.as_ref().map(|x| (x.len() * 8) as i32),
                ))),
                ScalarValue::LargeUtf8(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int64(v.as_ref().map(|x| (x.len() * 8) as i64)),
                )),
                ScalarValue::Utf8View(v) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int32(v.as_ref().map(|x| (x.len() * 8) as i32)),
                )),
                _ => unreachable!("bit length"),
            },
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!(
                "The {} function requires 1 argument, but got {}.",
                self.name(),
                arg_types.len()
            );
        }

        let arg_type = &arg_types[0];
        let current_native_type: NativeType = arg_type.into();
        let target_native_type = NativeType::String;
        if current_native_type.is_integer()
            || current_native_type.is_binary()
            || current_native_type == NativeType::String
            || current_native_type == NativeType::Null
        {
            Ok(vec![target_native_type.default_cast_for(arg_type)?])
        } else {
            plan_err!(
                "The first argument of the {} function can only be a string, integer, or binary but got {:?}.",
                self.name(),
                arg_type
            )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
