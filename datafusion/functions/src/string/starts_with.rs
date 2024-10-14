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
use std::sync::{Arc, OnceLock};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::utils::make_scalar_function;
use datafusion_common::{internal_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

/// Returns true if string starts with prefix.
/// starts_with('alphabet', 'alph') = 't'
pub fn starts_with(args: &[ArrayRef]) -> Result<ArrayRef> {
    let result = arrow::compute::kernels::comparison::starts_with(&args[0], &args[1])?;
    Ok(Arc::new(result) as ArrayRef)
}

#[derive(Debug)]
pub struct StartsWithFunc {
    signature: Signature,
}

impl Default for StartsWithFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl StartsWithFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StartsWithFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "starts_with"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 | DataType::LargeUtf8 => {
                make_scalar_function(starts_with, vec![])(args)
            }
            _ => internal_err!("Unsupported data types for starts_with. Expected Utf8, LargeUtf8 or Utf8View")?,
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_starts_with_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_starts_with_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Tests if a string starts with a substring.")
            .with_syntax_example("starts_with(str, substr)")
            .with_sql_example(
                r#"```sql
> select starts_with('datafusion','data');
+----------------------------------------------+
| starts_with(Utf8("datafusion"),Utf8("data")) |
+----------------------------------------------+
| true                                         |
+----------------------------------------------+
```"#,
            )
            .with_standard_argument("str", Some("String"))
            .with_argument("substr", "Substring to test for.")
            .build()
            .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use crate::utils::test::test_function;
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use super::*;

    #[test]
    fn test_functions() -> Result<()> {
        // Generate test cases for starts_with
        let test_cases = vec![
            (Some("alphabet"), Some("alph"), Some(true)),
            (Some("alphabet"), Some("bet"), Some(false)),
            (
                Some("somewhat large string"),
                Some("somewhat large"),
                Some(true),
            ),
            (Some("somewhat large string"), Some("large"), Some(false)),
        ]
        .into_iter()
        .flat_map(|(a, b, c)| {
            let utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(b.map(|s| s.to_string()))),
            ];

            let large_utf_8_args = vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(b.map(|s| s.to_string()))),
            ];

            let utf_8_view_args = vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(a.map(|s| s.to_string()))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(b.map(|s| s.to_string()))),
            ];

            vec![(utf_8_args, c), (large_utf_8_args, c), (utf_8_view_args, c)]
        });

        for (args, expected) in test_cases {
            test_function!(
                StartsWithFunc::new(),
                &args,
                Ok(expected),
                bool,
                Boolean,
                BooleanArray
            );
        }

        Ok(())
    }
}
