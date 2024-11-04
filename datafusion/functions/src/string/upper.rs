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

use crate::string::common::to_upper;
use crate::utils::utf8_to_str_type;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::scalar_doc_sections::DOC_SECTION_STRING;
use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct UpperFunc {
    signature: Signature,
}

impl Default for UpperFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl UpperFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for UpperFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "upper"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "upper")
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        to_upper(args, "upper")
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_upper_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_upper_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_STRING)
            .with_description("Converts a string to upper-case.")
            .with_syntax_example("upper(str)")
            .with_sql_example(
                r#"```sql
> select upper('dataFusion');
+---------------------------+
| upper(Utf8("dataFusion")) |
+---------------------------+
| DATAFUSION                |
+---------------------------+
```"#,
            )
            .with_standard_argument("str", Some("String"))
            .with_related_udf("initcap")
            .with_related_udf("lower")
            .build()
            .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use std::sync::Arc;

    fn to_upper(input: ArrayRef, expected: ArrayRef) -> Result<()> {
        let func = UpperFunc::new();
        let args = vec![ColumnarValue::Array(input)];
        let result = match func.invoke(&args)? {
            ColumnarValue::Array(result) => result,
            _ => unreachable!(),
        };
        assert_eq!(&expected, &result);
        Ok(())
    }

    #[test]
    fn upper_maybe_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_upper(input, expected)
    }

    #[test]
    fn upper_full_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_upper(input, expected)
    }

    #[test]
    fn upper_partial_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὀδυσσεύς"),
            Some("tschüß"),
            Some("ⱦ"), // Ⱦ: length change
            Some("农历新年"),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὈΔΥΣΣΕΎΣ"),
            Some("TSCHÜSS"),
            Some("Ⱦ"),
            Some("农历新年"),
        ])) as ArrayRef;

        to_upper(input, expected)
    }
}
