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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::utils::make_scalar_function;
use datafusion_common::types::{LogicalType, NativeType};
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_expr_common::type_coercion::binary::string_coercion;
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Tests if a string ends with a substring.",
    syntax_example = "ends_with(str, substr)",
    sql_example = r#"```sql
>  select ends_with('datafusion', 'soin');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("soin")) |
+--------------------------------------------+
| false                                      |
+--------------------------------------------+
> select ends_with('datafusion', 'sion');
+--------------------------------------------+
| ends_with(Utf8("datafusion"),Utf8("sion")) |
+--------------------------------------------+
| true                                       |
+--------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "Coercible String"),
    argument(name = "substr", description = "Coercible substring to test for.")
)]
#[derive(Debug)]
pub struct EndsWithFunc {
    signature: Signature,
}

impl Default for EndsWithFunc {
    fn default() -> Self {
        EndsWithFunc::new()
    }
}

impl EndsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for EndsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ends_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
                make_scalar_function(ends_with, vec![])(args)
            }
            other => {
                internal_err!("Unsupported data type {other:?} for function ends_with. Expected Utf8, LargeUtf8 or Utf8View")?
            }
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return plan_err!(
                "The {} function requires 2 arguments, but got {}.",
                self.name(),
                arg_types.len()
            );
        }

        let first_arg_type = &arg_types[0];
        let first_native_type: NativeType = first_arg_type.into();
        let second_arg_type = &arg_types[1];
        let second_native_type: NativeType = second_arg_type.into();
        let target_native_type = NativeType::String;

        let first_data_type = if first_native_type.is_integer()
            || first_native_type.is_binary()
            || first_native_type == NativeType::String
            || first_native_type == NativeType::Null
        {
            target_native_type.default_cast_for(first_arg_type)
        } else {
            plan_err!(
                "The first argument of the {} function can only be a string, integer, or binary but got {:?}.",
                self.name(),
                first_arg_type
            )
        }?;
        let second_data_type = if second_native_type.is_integer()
            || second_native_type.is_binary()
            || second_native_type == NativeType::String
            || second_native_type == NativeType::Null
        {
            target_native_type.default_cast_for(second_arg_type)
        } else {
            plan_err!(
                "The second argument of the {} function can only be a string, integer, or binary but got {:?}.",
                self.name(),
                second_arg_type
            )
        }?;

        if let Some(coerced_type) = string_coercion(&first_data_type, &second_data_type) {
            Ok(vec![coerced_type.clone(), coerced_type])
        } else {
            plan_err!(
                    "{first_data_type} and {second_data_type} are not coercible to a common string type"
                )
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
pub fn ends_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    let result = arrow::compute::kernels::comparison::ends_with(&args[0], &args[1])?;

    Ok(Arc::new(result) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;

    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::string::ends_with::EndsWithFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("bet")),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            EndsWithFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ],
            Ok(None),
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }
}
