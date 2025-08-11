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

//! Regex expressions - regexp_extract function
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringViewArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::{as_generic_string_array, as_int64_array, as_string_view_array};
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;

use super::compile_and_cache_regex;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extract a substring that matches a [regular expression](https://docs.rs/regex/latest/regex/#syntax) from a string.",
    syntax_example = "regexp_extract(str, regexp, group)",
    sql_example = r#"```sql
> select regexp_extract('100-200', '(\\d+)-(\\d+)', 1);
+-------------------------------------------------------+
| regexp_extract(Utf8("100-200"),Utf8("(\\d+)-(\\d+)"),Int32(1)) |
+-------------------------------------------------------+
| 100                                                   |
+-------------------------------------------------------+
> select regexp_extract('foobarbequebaz', '(bar)(beque)', 2);
+----------------------------------------------------------------+
| regexp_extract(Utf8("foobarbequebaz"),Utf8("(bar)(beque)"),Int32(2)) |
+----------------------------------------------------------------+
| beque                                                          |
+----------------------------------------------------------------+
> select regexp_extract('abc', '(\\d+)', 1);
+-----------------------------------------------+
| regexp_extract(Utf8("abc"),Utf8("(\\d+)"),Int32(1)) |
+-----------------------------------------------+
|                                               |
+-----------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against.
Can be a constant, column, or function."
    ),
    argument(
        name = "group",
        description = r#"Group index to extract. Group 0 returns the entire match, 
group 1 returns the first capture group, etc. If the group index is invalid 
or no match is found, returns an empty string."#
    )
)]
#[derive(Debug)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match &arg_types[0] {
            DataType::Null => DataType::Null,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            DataType::Utf8 => DataType::Utf8,
            DataType::Utf8View => DataType::Utf8View,
            other => {
                return plan_err!(
                    "regexp_extract can only accept strings but got {:?}",
                    other
                );
            }
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_extract(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

pub fn regexp_extract(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 3 {
        return exec_err!(
            "regexp_extract was called with {} arguments. It requires exactly 3.",
            args.len()
        );
    }

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            let pattern_array = as_generic_string_array::<i32>(&args[1])?;
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_impl(string_array, pattern_array, group_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            let pattern_array = as_generic_string_array::<i64>(&args[1])?;
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_impl(string_array, pattern_array, group_array)
        }
        DataType::Utf8View => {
            let string_array = as_string_view_array(&args[0])?;
            let pattern_array = as_string_view_array(&args[1])?;
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_string_view_impl(string_array, pattern_array, group_array)
        }
        other => exec_err!(
            "regexp_extract was called with unexpected data type {:?}",
            other
        ),
    }
}

fn regexp_extract_impl<T>(
    string_array: &arrow::array::GenericStringArray<T>,
    pattern_array: &arrow::array::GenericStringArray<T>,
    group_array: &Int64Array,
) -> Result<ArrayRef>
where
    T: arrow::array::OffsetSizeTrait,
{
    let mut patterns: HashMap<(&str, Option<&str>), Regex> = HashMap::new();
    let mut result_builder = arrow::array::GenericStringBuilder::<T>::new();

    for i in 0..string_array.len() {
        let string_value = if string_array.is_null(i) { None } else { Some(string_array.value(i)) };
        let pattern_value = if pattern_array.is_null(i) { None } else { Some(pattern_array.value(i)) };
        let group_value = if group_array.is_null(i) { None } else { Some(group_array.value(i)) };

        match (string_value, pattern_value, group_value) {
            (Some(string), Some(pattern), Some(group)) => {
                if group < 0 {
                    result_builder.append_value("");
                    continue;
                }

                let group_idx = group as usize;
                
                // Get or compile regex pattern
                let regex = compile_and_cache_regex(pattern, None, &mut patterns)?;
                
                // Apply regex and extract group
                match regex.captures(string) {
                    Some(captures) => {
                        if let Some(matched_group) = captures.get(group_idx) {
                            result_builder.append_value(matched_group.as_str());
                        } else {
                            // Group index is valid but group doesn't exist in this match
                            result_builder.append_value("");
                        }
                    }
                    None => {
                        // No match found
                        result_builder.append_value("");
                    }
                }
            }
            _ => {
                // Any null input results in null output
                result_builder.append_null();
            }
        }
    }

    Ok(Arc::new(result_builder.finish()))
}

fn regexp_extract_string_view_impl(
    string_array: &StringViewArray,
    pattern_array: &StringViewArray,
    group_array: &Int64Array,
) -> Result<ArrayRef> {
    let mut patterns: HashMap<(&str, Option<&str>), Regex> = HashMap::new();
    let mut result_builder = arrow::array::StringViewBuilder::new();

    for i in 0..string_array.len() {
        let string_value = if string_array.is_null(i) { None } else { Some(string_array.value(i)) };
        let pattern_value = if pattern_array.is_null(i) { None } else { Some(pattern_array.value(i)) };
        let group_value = if group_array.is_null(i) { None } else { Some(group_array.value(i)) };

        match (string_value, pattern_value, group_value) {
            (Some(string), Some(pattern), Some(group)) => {
                if group < 0 {
                    result_builder.append_value("");
                    continue;
                }

                let group_idx = group as usize;
                
                // Get or compile regex pattern
                let regex = compile_and_cache_regex(pattern, None, &mut patterns)?;
                
                // Apply regex and extract group
                match regex.captures(string) {
                    Some(captures) => {
                        if let Some(matched_group) = captures.get(group_idx) {
                            result_builder.append_value(matched_group.as_str());
                        } else {
                            // Group index is valid but group doesn't exist in this match
                            result_builder.append_value("");
                        }
                    }
                    None => {
                        // No match found
                        result_builder.append_value("");
                    }
                }
            }
            _ => {
                // Any null input results in null output
                result_builder.append_null();
            }
        }
    }

    Ok(Arc::new(result_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    #[test]
    fn test_basic_extraction() {
        let strings = StringArray::from(vec!["100-200", "foo123bar", "no-match"]);
        let patterns = StringArray::from(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)([a-z]+)", r"(\d+)"]);
        let groups = Int64Array::from(vec![1, 2, 1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "100");  // First capture group from "100-200"
        assert_eq!(result.value(1), "123");  // Second capture group from "foo123bar"
        assert_eq!(result.value(2), "");     // No match for pattern (\d+) in "no-match"
    }

    #[test]
    fn test_group_zero_full_match() {
        let strings = StringArray::from(vec!["100-200", "abc123"]);
        let patterns = StringArray::from(vec![r"\d+-\d+", r"[a-z]+\d+"]);
        let groups = Int64Array::from(vec![0, 0]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "100-200");  // Full match
        assert_eq!(result.value(1), "abc123");   // Full match
    }

    #[test]
    fn test_invalid_group_index() {
        let strings = StringArray::from(vec!["100-200", "abc123"]);
        let patterns = StringArray::from(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)"]);
        let groups = Int64Array::from(vec![5, -1]);  // Group 5 doesn't exist, group -1 is negative

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "");  // Group 5 doesn't exist
        assert_eq!(result.value(1), "");  // Negative group index
    }

    #[test]
    fn test_null_values() {
        let strings = StringArray::from(vec![Some("100-200"), None, Some("abc123")]);
        let patterns = StringArray::from(vec![Some(r"(\d+)-(\d+)"), Some(r"(\d+)"), None]);
        let groups = Int64Array::from(vec![Some(1), Some(1), Some(1)]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "100");  // Valid extraction
        assert!(result.is_null(1));          // Null string input
        assert!(result.is_null(2));          // Null pattern input
    }

    #[test]
    fn test_complex_regex_patterns() {
        let strings = StringArray::from(vec![
            "user@example.com",
            "phone: (123) 456-7890",
            "Price: $99.99"
        ]);
        let patterns = StringArray::from(vec![
            r"([^@]+)@([^.]+)\.(.+)",    // Email parts
            r"phone: \((\d+)\) (\d+)-(\d+)", // Phone number parts
            r"Price: \$(\d+)\.(\d+)"     // Price parts
        ]);
        let groups = Int64Array::from(vec![2, 2, 1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "example");  // Domain name without TLD
        assert_eq!(result.value(1), "456");      // Middle part of phone number
        assert_eq!(result.value(2), "99");       // Dollar amount
    }

    #[test]
    fn test_empty_string_input() {
        let strings = StringArray::from(vec![""]);
        let patterns = StringArray::from(vec![r"(\d+)"]);
        let groups = Int64Array::from(vec![1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "");  // No match in empty string
    }

    #[test]
    fn test_invalid_regex_pattern() {
        let strings = StringArray::from(vec!["test"]);
        let patterns = StringArray::from(vec!["["]);  // Invalid regex - unclosed bracket
        let groups = Int64Array::from(vec![1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]);

        assert!(result.is_err());  // Should return an error for invalid regex
    }
}
