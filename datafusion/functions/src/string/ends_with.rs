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
use datafusion_common::types::logical_string;
use datafusion_common::{Result, internal_err};
use datafusion_expr::binary::{binary_to_string_coercion, string_coercion};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
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
    standard_argument(name = "str", prefix = "String"),
    argument(name = "substr", description = "Substring to test for.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(ends_with, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Returns true if string ends with suffix.
/// ends_with('alphabet', 'abet') = 't'
fn ends_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    if let Some(coercion_data_type) =
        string_coercion(args[0].data_type(), args[1].data_type()).or_else(|| {
            binary_to_string_coercion(args[0].data_type(), args[1].data_type())
        })
    {
        let arg0 = if args[0].data_type() == &coercion_data_type {
            Arc::clone(&args[0])
        } else {
            arrow::compute::kernels::cast::cast(&args[0], &coercion_data_type)?
        };
        let arg1 = if args[1].data_type() == &coercion_data_type {
            Arc::clone(&args[1])
        } else {
            arrow::compute::kernels::cast::cast(&args[1], &coercion_data_type)?
        };
        let result = arrow::compute::kernels::comparison::ends_with(&arg0, &arg1)?;
        Ok(Arc::new(result) as ArrayRef)
    } else {
        internal_err!(
            "Unsupported data types for ends_with. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
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
