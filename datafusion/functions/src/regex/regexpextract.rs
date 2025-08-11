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
//! all design and implementation dilemas are accompanied by human-made comments and are marked with #HUMAN-MADE

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

use arrow::array::{
    Array, ArrayRef, AsArray, GenericStringArray, Int64Array, StringViewArray,
};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::{exec_err, plan_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, Documentation, TypeSignature};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use regex::Regex;

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
                    /*
                    #HUMAN-MADE:
                    the initial solution tried to support group indexes of both Int32 and Int64, which resulted in code duplication.
                    by coercing the group index to Int64, we can use the same code path for both Int32 and Int64.
                    */
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

    match (args[0].data_type(), args[1].data_type()) {
        (DataType::Utf8, DataType::Utf8) => {
            let string_array = args[0].as_string::<i32>();
            let pattern_array = args[1].as_string::<i32>();
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_inner(string_array, pattern_array, group_array, |values| {
                let arr = GenericStringArray::<i32>::from(values);
                Arc::new(arr) as ArrayRef
            })
        }
        (DataType::LargeUtf8, DataType::LargeUtf8) => {
            let string_array = args[0].as_string::<i64>();
            let pattern_array = args[1].as_string::<i64>();
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_inner(string_array, pattern_array, group_array, |values| {
                let arr = GenericStringArray::<i64>::from(values);
                Arc::new(arr) as ArrayRef
            })
        }
        (DataType::Utf8View, DataType::Utf8View) => {
            let string_array = args[0].as_string_view();
            let pattern_array = args[1].as_string_view();
            let group_array = as_int64_array(&args[2])?;
            regexp_extract_inner(string_array, pattern_array, group_array, |values| {
                let arr = StringViewArray::from(values);
                Arc::new(arr) as ArrayRef
            })
        }
        (string_type, pattern_type) => exec_err!(
            "regexp_extract() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the string and pattern to match, but got {string_type:?} and {pattern_type:?}"
        ),
    }
}
/*
#HUMAN-MADE:
This is the core logic for the regexp_extract function.
originally the AI agent made two implementations, one for GenericStringArray and one for StringViewArray.
both had the same logic, but different build_array functions.
inspired by the implementation of the regexp_match function, I decided to create a single implementation that works with any string array type.
this change took a significant amount of development time, but moving forward it should streamline future changes. so it should be worth it.
*/
fn regexp_extract_inner<S, F>(
    string_array: &S,
    pattern_array: &S,
    group_array: &Int64Array,
    build_array: F,
) -> Result<ArrayRef>
where
    S: Array,
    for<'a> &'a S: IntoIterator<Item = Option<&'a str>>,
    F: FnOnce(Vec<Option<String>>) -> ArrayRef,
{
    /*
    #HUMAN-MADE:
    after a few tries for parallelization, which all increased the execution time, I went for a different approach.
    analyzing benchmark results, I found that precompiling the patterns added only a small amount of time to the overall execution time. (600 µs -> 650 µs)
    then I opted to paralelize only the precompilation, which sifnificantly improved performance (650 µs -> 470 µs)
    after some more tweaking, I managed to optimize the serial part even further and get an overall execution time of 300 µs.
    */
    let compiled_patterns = precompile_patterns(pattern_array)?;

    /*
    #HUMAN-MADE:
    this might be wrong for larger inputs, but at this point, I opted to not parallelize the main loop as any attempts to parallelize it resulted in regressions for the current benchmark size.
     */
    let mut results: Vec<Option<String>> = Vec::with_capacity(string_array.len());
    let string_iter = string_array.into_iter();
    let pattern_iter = pattern_array.into_iter();
    for ((string_value, pattern_value), group_value) in
        string_iter.zip(pattern_iter).zip(group_array.iter())
    {
        let result = match (string_value, pattern_value, group_value) {
            (Some(string), Some(pattern), Some(group)) => {
                if group < 0 {
                    Some(String::new())
                } else {
                    let group_idx = group as usize;
                    let regex = compiled_patterns
                        .get(pattern)
                        .expect("Pattern should be pre-compiled");
                    if group_idx >= regex.captures_len() {
                        Some(String::new())
                    } else {
                        match regex.captures(string) {
                            Some(captures) => captures
                                .get(group_idx)
                                .map(|m| m.as_str().to_string())
                                .or_else(|| Some(String::new())),
                            None => Some(String::new()),
                        }
                    }
                }
            }
            _ => None,
        };
        results.push(result);
    }
    return Ok(build_array(results));
}

fn precompile_patterns<'a, S>(pattern_array: &'a S) -> Result<HashMap<&'a str, Regex>>
where
    S: Array,
    for<'b> &'b S: IntoIterator<Item = Option<&'b str>>,
{
    // First pass: collect all unique patterns and pre-compile them
    let mut unique_patterns: HashSet<&str> = HashSet::new();
    for pattern_value in pattern_array.into_iter() {
        if let Some(pattern) = pattern_value {
            unique_patterns.insert(pattern);
        }
    }
    let unique_patterns: Vec<&str> = unique_patterns.into_iter().collect();
    let mut compiled_patterns: HashMap<&str, Regex> =
        HashMap::with_capacity(unique_patterns.len());

    if !unique_patterns.is_empty() {
        let available = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let num_threads = available.min(unique_patterns.len());
        let chunk_size = (unique_patterns.len() + num_threads - 1) / num_threads;

        // Collect compiled outputs from threads
        let mut compiled_pairs: Vec<(&str, Regex)> =
            Vec::with_capacity(unique_patterns.len());
        let mut thread_error: Option<datafusion_common::DataFusionError> = None;

        thread::scope(|scope| {
            let mut handles = Vec::with_capacity(num_threads);

            for chunk_idx in 0..num_threads {
                let start = chunk_idx * chunk_size;
                if start >= unique_patterns.len() {
                    break;
                }
                let end = (start + chunk_size).min(unique_patterns.len());

                let slice = &unique_patterns[start..end];
                handles.push(scope.spawn(move || -> Result<Vec<(&str, Regex)>> {
                    let mut local: Vec<(&str, Regex)> = Vec::with_capacity(slice.len());
                    for &pattern in slice {
                        let regex = super::compile_regex(pattern, None).map_err(|e| {
                            datafusion_common::DataFusionError::ArrowError(
                                Box::new(e),
                                None,
                            )
                        })?;
                        local.push((pattern, regex));
                    }
                    Ok(local)
                }));
            }

            for handle in handles {
                match handle.join().expect("thread panicked") {
                    Ok(mut local) => compiled_pairs.append(&mut local),
                    Err(e) => {
                        thread_error = Some(e);
                    }
                }
            }
        });

        if let Some(e) = thread_error {
            return Err(e);
        }

        for (pattern, regex) in compiled_pairs {
            compiled_patterns.insert(pattern, regex);
        }
    }

    Ok(compiled_patterns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, LargeStringArray, StringArray, StringViewArray};
    /*
    #HUMAN-MADE:
    I opted to test all three string types.
    to avoid code duplication, I created a trait that abstracts over the different string array types.
    it isn't a common pattern in the codebase, but it is a good way to run the same test for all supported string array types.
    I could also have added rstest to the project's dependencies and used it to run the tests, but for this excercise implementing it myself is fine.
    */

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
    fn test_basic_extraction<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "foo123bar", "no-match"]);
        let patterns =
            T::from_strings(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)([a-z]+)", r"(\d+)"]);
        let groups = Int64Array::from(vec![1, 2, 1]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100"); // First capture group from "100-200"
        assert_eq!(T::get_value(result, 1), "123"); // Second capture group from "foo123bar"
        assert_eq!(T::get_value(result, 2), ""); // No match for pattern (\d+) in "no-match"
    }

    fn test_group_zero_full_match<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "abc123"]);
        let patterns = T::from_strings(vec![r"\d+-\d+", r"[a-z]+\d+"]);
        let groups = Int64Array::from(vec![0, 0]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100-200"); // Full match
        assert_eq!(T::get_value(result, 1), "abc123"); // Full match
    }

    fn test_invalid_group_index<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["100-200", "abc123"]);
        let patterns = T::from_strings(vec![r"(\d+)-(\d+)", r"([a-z]+)(\d+)"]);
        let groups = Int64Array::from(vec![5, -1]); // Group 5 doesn't exist, group -1 is negative

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), ""); // Group 5 doesn't exist
        assert_eq!(T::get_value(result, 1), ""); // Negative group index
    }

    fn test_null_values<T: StringArrayTestType>() {
        let strings =
            T::from_optional_strings(vec![Some("100-200"), None, Some("abc123")]);
        let patterns =
            T::from_optional_strings(vec![Some(r"(\d+)-(\d+)"), Some(r"(\d+)"), None]);
        let groups = Int64Array::from(vec![Some(1), Some(1), Some(1)]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "100"); // Valid extraction
        assert!(T::is_null(result, 1)); // Null string input
        assert!(T::is_null(result, 2)); // Null pattern input
    }

    fn test_complex_regex_patterns<T: StringArrayTestType>() {
        let strings = T::from_strings(vec![
            "user@example.com",
            "phone: (123) 456-7890",
            "Price: $99.99",
        ]);
        let patterns = T::from_strings(vec![
            r"([^@]+)@([^.]+)\.(.+)",        // Email parts
            r"phone: \((\d+)\) (\d+)-(\d+)", // Phone number parts
            r"Price: \$(\d+)\.(\d+)",        // Price parts
        ]);
        let groups = Int64Array::from(vec![2, 2, 1]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), "example"); // Domain name without TLD
        assert_eq!(T::get_value(result, 1), "456"); // Middle part of phone number
        assert_eq!(T::get_value(result, 2), "99"); // Dollar amount
    }

    fn test_empty_string_input<T: StringArrayTestType>() {
        let strings = T::from_strings(vec![""]);
        let patterns = T::from_strings(vec![r"(\d+)"]);
        let groups = Int64Array::from(vec![1]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)])
                .unwrap();

        let result = T::downcast_result(&result).expect("Failed to downcast result");
        assert_eq!(T::get_value(result, 0), ""); // No match in empty string
    }

    fn test_invalid_regex_pattern<T: StringArrayTestType>() {
        let strings = T::from_strings(vec!["test"]);
        let patterns = T::from_strings(vec!["["]); // Invalid regex - unclosed bracket
        let groups = Int64Array::from(vec![1]);

        let result =
            regexp_extract(&[Arc::new(strings), Arc::new(patterns), Arc::new(groups)]);

        assert!(result.is_err()); // Should return an error for invalid regex
    }

    macro_rules! test_for_all_types {
        ($modname:ident, $generic:ident) => {
            mod $modname {
                use super::*;
                #[test]
                fn utf8() {
                    $generic::<Utf8TestType>();
                }
                #[test]
                fn large_utf8() {
                    $generic::<LargeUtf8TestType>();
                }
                #[test]
                fn utf8_view() {
                    $generic::<Utf8ViewTestType>();
                }
            }
        };
    }

    test_for_all_types!(basic_extraction, test_basic_extraction);

    test_for_all_types!(group_zero_full_match, test_group_zero_full_match);

    test_for_all_types!(invalid_group_index, test_invalid_group_index);

    test_for_all_types!(null_values, test_null_values);

    test_for_all_types!(complex_regex_patterns, test_complex_regex_patterns);

    test_for_all_types!(empty_string_input, test_empty_string_input);

    test_for_all_types!(invalid_regex_pattern, test_invalid_regex_pattern);
}
