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

//! This file was created by AI(Claude 3.5 Sonnet) and was reviewed by a human (@pikerpoler)
//! all human-made changes have a comment explaining the change, and are marked with #HUMAN-MADE


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
                    // #HUMAN-MADE: the initial solution tried to support group indexes of both Int32 and Int64, which resulted in code duplication.
                    //              by coercing the group index to Int64, we can use the same code path for both Int32 and Int64.
                    
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
    use arrow::array::{Int64Array, StringArray, LargeStringArray, StringViewArray};

    // HUMAN-MADE: I opted to test all three string types.
    // to avoid code duplication, I created a trait that abstracts over the different string array types.

    // Trait to abstract over different string array types for testing
    trait StringArrayTestType: 'static {
        type ArrayType: Array + 'static;
        
        // Create array from string slice
        fn from_strings(values: Vec<&str>) -> Self::ArrayType;
        
        // Create array from optional string slice (supports nulls)
        fn from_optional_strings(values: Vec<Option<&str>>) -> Self::ArrayType;
        
        // Downcast result to correct type for assertions
        fn downcast_result(result: &ArrayRef) -> Option<&Self::ArrayType>;
        
        // Get string value at index
        fn get_value(array: &Self::ArrayType, index: usize) -> &str;
        
        // Check if value at index is null
        fn is_null(array: &Self::ArrayType, index: usize) -> bool;
    }

    // Implement for Utf8 (StringArray)
    struct Utf8TestType;
    impl StringArrayTestType for Utf8TestType {
        type ArrayType = StringArray;
        
        fn from_strings(values: Vec<&str>) -> Self::ArrayType {
            StringArray::from(values)
        }
        
        fn from_optional_strings(values: Vec<Option<&str>>) -> Self::ArrayType {
            StringArray::from(values)
        }
        
        fn downcast_result(result: &ArrayRef) -> Option<&Self::ArrayType> {
            result.as_any().downcast_ref::<StringArray>()
        }
        
        fn get_value(array: &Self::ArrayType, index: usize) -> &str {
            array.value(index)
        }
        
        fn is_null(array: &Self::ArrayType, index: usize) -> bool {
            array.is_null(index)
        }
    }

    // Implement for LargeUtf8 (LargeStringArray)
    struct LargeUtf8TestType;
    impl StringArrayTestType for LargeUtf8TestType {
        type ArrayType = LargeStringArray;
        
        fn from_strings(values: Vec<&str>) -> Self::ArrayType {
            LargeStringArray::from(values)
        }
        
        fn from_optional_strings(values: Vec<Option<&str>>) -> Self::ArrayType {
            LargeStringArray::from(values)
        }
        
        fn downcast_result(result: &ArrayRef) -> Option<&Self::ArrayType> {
            result.as_any().downcast_ref::<LargeStringArray>()
        }
        
        fn get_value(array: &Self::ArrayType, index: usize) -> &str {
            array.value(index)
        }
        
        fn is_null(array: &Self::ArrayType, index: usize) -> bool {
            array.is_null(index)
        }
    }

    // Implement for Utf8View (StringViewArray)
    struct Utf8ViewTestType;
    impl StringArrayTestType for Utf8ViewTestType {
        type ArrayType = StringViewArray;
        
        fn from_strings(values: Vec<&str>) -> Self::ArrayType {
            StringViewArray::from(values)
        }
        
        fn from_optional_strings(values: Vec<Option<&str>>) -> Self::ArrayType {
            StringViewArray::from(values)
        }
        
        fn downcast_result(result: &ArrayRef) -> Option<&Self::ArrayType> {
            result.as_any().downcast_ref::<StringViewArray>()
        }
        
        fn get_value(array: &Self::ArrayType, index: usize) -> &str {
            array.value(index)
        }
        
        fn is_null(array: &Self::ArrayType, index: usize) -> bool {
            array.is_null(index)
        }
    }

    // Generic test functions
    fn test_basic_extraction_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "foo123bar", "no-match"]);
        let patterns = T::from_strings(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)([a-z]+)", r"(\d+)"]);
        let groups = Int64Array::from(vec![1, 2, 1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100");  // First capture group from "100-200"
        assert_eq!(T::get_value(result, 1), "123");  // Second capture group from "foo123bar"
        assert_eq!(T::get_value(result, 2), "");     // No match for pattern (\d+) in "no-match"
    }

    fn test_group_zero_full_match_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "abc123"]);
        let patterns = T::from_strings(vec![r"\d+-\d+", r"[a-z]+\d+"]);
        let groups = Int64Array::from(vec![0, 0]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100-200");  // Full match
        assert_eq!(T::get_value(result, 1), "abc123");   // Full match
    }

    fn test_invalid_group_index_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "abc123"]);
        let patterns = T::from_strings(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)"]);
        let groups = Int64Array::from(vec![5, -1]);  // Group 5 doesn't exist, group -1 is negative

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "");  // Group 5 doesn't exist
        assert_eq!(T::get_value(result, 1), "");  // Negative group index
    }

    fn test_null_values_generic<T: StringArrayTestType>() {
        let strings = T::from_optional_strings(vec![Some("100-200"), None, Some("abc123")]);
        let patterns = T::from_optional_strings(vec![Some(r"(\d+)-(\d+)"), Some(r"(\d+)"), None]);
        let groups = Int64Array::from(vec![Some(1), Some(1), Some(1)]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100");  // Valid extraction
        assert!(T::is_null(result, 1));              // Null string input
        assert!(T::is_null(result, 2));              // Null pattern input
    }

    fn test_complex_regex_patterns_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec![
            "user@example.com",
            "phone: (123) 456-7890",
            "Price: $99.99"
        ]);
        let patterns = T::from_strings(vec![
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

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "example");  // Domain name without TLD
        assert_eq!(T::get_value(result, 1), "456");      // Middle part of phone number
        assert_eq!(T::get_value(result, 2), "99");       // Dollar amount
    }

    fn test_empty_string_input_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec![""]);
        let patterns = T::from_strings(vec![r"(\d+)"]);
        let groups = Int64Array::from(vec![1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ])
        .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "");  // No match in empty string
    }

    fn test_invalid_regex_pattern_generic<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["test"]);
        let patterns = T::from_strings(vec!["["]);  // Invalid regex - unclosed bracket
        let groups = Int64Array::from(vec![1]);

        let result = regexp_extract(&[
            Arc::new(strings),
            Arc::new(patterns),
            Arc::new(groups),
        ]);

        assert!(result.is_err());  // Should return an error for invalid regex
    }

    // Test all three string types - Basic extraction tests
    #[test]
    fn test_basic_extraction_utf8() {
        test_basic_extraction_generic::<Utf8TestType>();
    }

    #[test]
    fn test_basic_extraction_large_utf8() {
        test_basic_extraction_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_basic_extraction_utf8_view() {
        test_basic_extraction_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Group zero (full match) tests
    #[test]
    fn test_group_zero_full_match_utf8() {
        test_group_zero_full_match_generic::<Utf8TestType>();
    }

    #[test]
    fn test_group_zero_full_match_large_utf8() {
        test_group_zero_full_match_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_group_zero_full_match_utf8_view() {
        test_group_zero_full_match_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Invalid group index tests
    #[test]
    fn test_invalid_group_index_utf8() {
        test_invalid_group_index_generic::<Utf8TestType>();
    }

    #[test]
    fn test_invalid_group_index_large_utf8() {
        test_invalid_group_index_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_invalid_group_index_utf8_view() {
        test_invalid_group_index_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Null values tests
    #[test]
    fn test_null_values_utf8() {
        test_null_values_generic::<Utf8TestType>();
    }

    #[test]
    fn test_null_values_large_utf8() {
        test_null_values_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_null_values_utf8_view() {
        test_null_values_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Complex regex patterns tests
    #[test]
    fn test_complex_regex_patterns_utf8() {
        test_complex_regex_patterns_generic::<Utf8TestType>();
    }

    #[test]
    fn test_complex_regex_patterns_large_utf8() {
        test_complex_regex_patterns_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_complex_regex_patterns_utf8_view() {
        test_complex_regex_patterns_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Empty string input tests
    #[test]
    fn test_empty_string_input_utf8() {
        test_empty_string_input_generic::<Utf8TestType>();
    }

    #[test]
    fn test_empty_string_input_large_utf8() {
        test_empty_string_input_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_empty_string_input_utf8_view() {
        test_empty_string_input_generic::<Utf8ViewTestType>();
    }

    // Test all three string types - Invalid regex pattern tests
    #[test]
    fn test_invalid_regex_pattern_utf8() {
        test_invalid_regex_pattern_generic::<Utf8TestType>();
    }

    #[test]
    fn test_invalid_regex_pattern_large_utf8() {
        test_invalid_regex_pattern_generic::<LargeUtf8TestType>();
    }

    #[test]
    fn test_invalid_regex_pattern_utf8_view() {
        test_invalid_regex_pattern_generic::<Utf8ViewTestType>();
    }
}
