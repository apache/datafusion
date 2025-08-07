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

use arrow::datatypes::DataType;
use std::any::Any;

use crate::string::common::to_lower;
use crate::utils::utf8_to_str_type;
use datafusion_common::types::logical_string;
use datafusion_common::Result;
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Converts a string to lower-case.",
    syntax_example = "lower(str)",
    sql_example = r#"```sql
> select lower('Ångström');
+-------------------------+
| lower(Utf8("Ångström")) |
+-------------------------+
| ångström                |
+-------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    related_udf(name = "initcap"),
    related_udf(name = "upper")
)]
#[derive(Debug)]
pub struct LowerFunc {
    signature: Signature,
}

impl Default for LowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LowerFunc {
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

impl ScalarUDFImpl for LowerFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lower"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "lower")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        to_lower(&args.args, "lower")
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::DataType::Utf8;
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use std::sync::Arc;

    fn to_lower(input: ArrayRef, expected: ArrayRef) -> Result<()> {
        let func = LowerFunc::new();
        let arg_fields = vec![Field::new("a", input.data_type().clone(), true).into()];

        let args = ScalarFunctionArgs {
            number_rows: input.len(),
            args: vec![ColumnarValue::Array(input)],
            arg_fields,
            return_field: Field::new("f", Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = match func.invoke_with_args(args)? {
            ColumnarValue::Array(result) => result,
            _ => unreachable!("lower"),
        };
        assert_eq!(&expected, &result);
        Ok(())
    }

    #[test]
    fn lower_maybe_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("农历新年"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_full_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("0123456789"),
            Some(""),
        ])) as ArrayRef;

        to_lower(input, expected)
    }

    #[test]
    fn lower_partial_optimization() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("ARROW"),
            None,
            Some("DATAFUSION"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὈΔΥΣΣΕΎΣ"),
            Some("TSCHÜSS"),
            Some("Ⱦ"), // ⱦ: length change
            Some("农历新年"),
        ])) as ArrayRef;

        let expected = Arc::new(StringArray::from(vec![
            Some("arrow"),
            None,
            Some("datafusion"),
            Some("@_"),
            Some("0123456789"),
            Some(""),
            Some("\t\n"),
            Some("ὀδυσσεύς"),
            Some("tschüss"),
            Some("ⱦ"),
            Some("农历新年"),
        ])) as ArrayRef;

        to_lower(input, expected)
    }
}
