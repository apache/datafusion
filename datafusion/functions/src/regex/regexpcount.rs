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

use crate::regex::{compile_and_cache_regex, compile_regex};
use arrow::array::{Array, ArrayRef, AsArray, Datum, Int64Array, StringArrayType};
use arrow::datatypes::{DataType, Int64Type};
use arrow::datatypes::{
    DataType::Int64, DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View,
};
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature::Exact, TypeSignature::Uniform, Volatility,
};
use datafusion_macros::user_doc;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns the number of matches that a [regular expression](https://docs.rs/regex/latest/regex/#syntax) has in a string.",
    syntax_example = "regexp_count(str, regexp[, start, flags])",
    sql_example = r#"```sql
> select regexp_count('abcAbAbc', 'abc', 2, 'i');
+---------------------------------------------------------------+
| regexp_count(Utf8("abcAbAbc"),Utf8("abc"),Int64(2),Utf8("i")) |
+---------------------------------------------------------------+
| 1                                                             |
+---------------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    argument(
        name = "start",
        description = "- **start**: Optional start position (the first position is 1) to search for the regular expression. Can be a constant, column, or function."
    ),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpCountFunc {
    signature: Signature,
}

impl Default for RegexpCountFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpCountFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Uniform(2, vec![Utf8View, LargeUtf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![Utf8View, Utf8View, Int64, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, LargeUtf8]),
                    Exact(vec![Utf8, Utf8, Int64, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpCountFunc {
    fn name(&self) -> &str {
        "regexp_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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

        let result = regexp_count_func(&args);
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

pub fn regexp_count_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if !(2..=4).contains(&args_len) {
        return exec_err!(
            "regexp_count was called with {args_len} arguments. It requires at least 2 and at most 4."
        );
    }

    let values = &args[0];
    match values.data_type() {
        Utf8 | LargeUtf8 | Utf8View => (),
        other => {
            return internal_err!(
                "Unsupported data type {other:?} for function regexp_count"
            );
        }
    }

    regexp_count(
        values,
        &args[1],
        if args_len > 2 { Some(&args[2]) } else { None },
        if args_len > 3 { Some(&args[3]) } else { None },
    )
    .map_err(|e| e.into())
}

/// `arrow-rs` style implementation of `regexp_count` function.
/// This function `regexp_count` is responsible for counting the occurrences of a regular expression pattern
/// within a string array. It supports optional start positions and flags for case insensitivity.
///
/// The function accepts a variable number of arguments:
/// - `values`: The array of strings to search within.
/// - `regex_array`: The array of regular expression patterns to search for.
/// - `start_array` (optional): The array of start positions for the search.
/// - `flags_array` (optional): The array of flags to modify the search behavior (e.g., case insensitivity).
///
/// The function handles different combinations of scalar and array inputs for the regex patterns, start positions,
/// and flags. It uses a cache to store compiled regular expressions for efficiency.
///
/// # Errors
/// Returns an error if the input arrays have mismatched lengths or if the regular expression fails to compile.
fn regexp_count(
    values: &dyn Array,
    regex_array: &dyn Datum,
    start_array: Option<&dyn Datum>,
    flags_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (regex_array, is_regex_scalar) = regex_array.get();
    let (start_array, is_start_scalar) = start_array.map_or((None, true), |start| {
        let (start, is_start_scalar) = start.get();
        (Some(start), is_start_scalar)
    });
    let (flags_array, is_flags_scalar) = flags_array.map_or((None, true), |flags| {
        let (flags, is_flags_scalar) = flags.get();
        (Some(flags), is_flags_scalar)
    });

    match (values.data_type(), regex_array.data_type(), flags_array) {
        (Utf8, Utf8, None) => regexp_count_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            None,
            is_flags_scalar,
        ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 => regexp_count_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            Some(flags_array.as_string::<i32>()),
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, None) => regexp_count_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            None,
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 => regexp_count_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            Some(flags_array.as_string::<i64>()),
            is_flags_scalar,
        ),
        (Utf8View, Utf8View, None) => regexp_count_inner(
            values.as_string_view(),
            regex_array.as_string_view(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            None,
            is_flags_scalar,
        ),
        (Utf8View, Utf8View, Some(flags_array)) if *flags_array.data_type() == Utf8View => regexp_count_inner(
            values.as_string_view(),
            regex_array.as_string_view(),
            is_regex_scalar,
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            is_start_scalar,
            Some(flags_array.as_string_view()),
            is_flags_scalar,
        ),
        _ => Err(ArrowError::ComputeError(
            "regexp_count() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the values, regex_array, and flags_array to match".to_string(),
        )),
    }
}

fn regexp_count_inner<'a, S>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    start_array: Option<&'a Int64Array>,
    is_start_scalar: bool,
    flags_array: Option<S>,
    is_flags_scalar: bool,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a> + Copy,
{
    let values_len = values.len();
    let regex = StringValueSource::regex_arg(regex_array, is_regex_scalar);

    // Preserve existing short-circuit behavior: a scalar NULL regex produces zeros
    // for every row without validating start or flags.
    if regex.is_null_scalar() {
        return Ok(Arc::new(Int64Array::from(vec![0; values_len])));
    }

    let start = StartValueSource::new(start_array, is_start_scalar);
    let flags = StringValueSource::flags_arg(flags_array, is_flags_scalar);

    regex.validate_len("regex_array", values_len)?;

    let scalar_pattern = if regex.is_array() {
        start.validate_len(values_len)?;
        flags.validate_len("flags_array", values_len)?;
        None
    } else if flags.is_array() {
        flags.validate_len("flags_array", values_len)?;
        start.validate_len(values_len)?;
        None
    } else {
        let scalar_pattern = compile_scalar_pattern(&regex, &flags)?;
        start.validate_len(values_len)?;
        scalar_pattern
    };

    let mut regex_cache = HashMap::new();
    let counts = (0..values_len)
        .map(|row| {
            let regex = match regex.value(row) {
                None => return Ok(0),
                Some(regex) => regex,
            };
            let start = start.value(row);
            let flags = flags.value(row);
            let value = string_value_opt(values, row);

            if let Some(pattern) = &scalar_pattern {
                count_matches(value, pattern, start)
            } else {
                let pattern = compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                count_matches(value, pattern, start)
            }
        })
        .collect::<Result<Int64Array, ArrowError>>()?;

    Ok(Arc::new(counts))
}

fn string_value_opt<'a, S>(array: S, row: usize) -> Option<&'a str>
where
    S: StringArrayType<'a>,
{
    (!array.is_null(row)).then(|| array.value(row))
}

enum StringValueSource<'a, S> {
    Scalar(Option<&'a str>),
    Array(S),
}

impl<'a, S> StringValueSource<'a, S>
where
    S: StringArrayType<'a> + Copy,
{
    fn regex_arg(array: S, is_scalar: bool) -> Self {
        if is_scalar || array.len() == 1 {
            Self::Scalar(string_value_opt(array, 0))
        } else {
            Self::Array(array)
        }
    }

    fn flags_arg(array: Option<S>, is_scalar: bool) -> Self {
        match array {
            // Preserve prior behavior: scalar flags use value(0), not a null-aware
            // lookup, before compile_regex handles the resulting flag value.
            Some(array) if is_scalar || array.len() == 1 => {
                Self::Scalar(Some(array.value(0)))
            }
            Some(array) => Self::Array(array),
            None => Self::Scalar(None),
        }
    }

    fn is_null_scalar(&self) -> bool {
        matches!(self, Self::Scalar(None))
    }

    fn is_array(&self) -> bool {
        matches!(self, Self::Array(_))
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Scalar(value) => *value,
            Self::Array(array) => string_value_opt(*array, row),
        }
    }

    fn validate_len(&self, name: &str, values_len: usize) -> Result<(), ArrowError> {
        if let Self::Array(array) = self {
            validate_array_len(name, array.len(), values_len)?;
        }
        Ok(())
    }
}

fn compile_scalar_pattern<'a, S>(
    regex: &StringValueSource<'a, S>,
    flags: &StringValueSource<'a, S>,
) -> Result<Option<Regex>, ArrowError> {
    match (regex, flags) {
        (StringValueSource::Scalar(Some(regex)), StringValueSource::Scalar(flags)) => {
            compile_regex(regex, *flags).map(Some)
        }
        _ => Ok(None),
    }
}

enum StartValueSource<'a> {
    Scalar(i64),
    Array(&'a Int64Array),
}

impl<'a> StartValueSource<'a> {
    fn new(array: Option<&'a Int64Array>, is_scalar: bool) -> Self {
        match array {
            // Preserve prior behavior: scalar start uses value(0), not a null-aware
            // lookup, before count_matches validates the resulting start value.
            Some(array) if is_scalar || array.len() == 1 => Self::Scalar(array.value(0)),
            Some(array) => Self::Array(array),
            None => Self::Scalar(1),
        }
    }

    fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Scalar(value) => Some(*value),
            Self::Array(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }

    fn validate_len(&self, values_len: usize) -> Result<(), ArrowError> {
        if let Self::Array(array) = self {
            validate_array_len("start_array", array.len(), values_len)?;
        }
        Ok(())
    }
}

fn validate_array_len(
    array_name: &str,
    array_len: usize,
    values_len: usize,
) -> Result<(), ArrowError> {
    if values_len != array_len {
        return Err(ArrowError::ComputeError(format!(
            "{array_name} must be the same length as values array; got {array_len} and {values_len}",
        )));
    }
    Ok(())
}

fn count_matches(
    value: Option<&str>,
    pattern: &Regex,
    start: Option<i64>,
) -> Result<i64, ArrowError> {
    let value = match value {
        None => return Ok(0),
        Some(value) => value,
    };

    if let Some(start) = start {
        if start < 1 {
            return Err(ArrowError::ComputeError(
                "regexp_count() requires start to be 1 based".to_string(),
            ));
        }

        let char_len = value.chars().count();
        let start_index = (start as usize).saturating_sub(1);

        if start_index > char_len {
            return Ok(0);
        }

        // Find the byte offset for the start position (1-based character index)
        let byte_offset = if start_index == char_len {
            value.len()
        } else {
            value
                .char_indices()
                .nth(start_index)
                .map(|(idx, _)| idx)
                .unwrap_or(value.len())
        };

        // Use string slicing instead of collecting chars into a new String
        let find_slice = &value[byte_offset..];
        let count = pattern.find_iter(find_slice).count();
        Ok(count as i64)
    } else {
        let count = pattern.find_iter(value).count();
        Ok(count as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{GenericStringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn test_regexp_count() {
        test_case_sensitive_regexp_count_scalar();
        test_case_sensitive_regexp_count_empty_pattern_scalar();
        test_case_sensitive_regexp_count_scalar_start();
        test_case_insensitive_regexp_count_scalar_flags();
        test_case_sensitive_regexp_count_start_scalar_complex();

        test_case_sensitive_regexp_count_array::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_count_array::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_count_array::<StringViewArray>();

        test_case_sensitive_regexp_count_array_start::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_count_array_start::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_count_array_start::<StringViewArray>();

        test_case_insensitive_regexp_count_array_flags::<GenericStringArray<i32>>();
        test_case_insensitive_regexp_count_array_flags::<GenericStringArray<i64>>();
        test_case_insensitive_regexp_count_array_flags::<StringViewArray>();

        test_case_sensitive_regexp_count_array_complex::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_count_array_complex::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_count_array_complex::<StringViewArray>();

        test_case_regexp_count_cache_check::<GenericStringArray<i32>>();
        test_regexp_count_error_order_invalid_scalar_regex_before_start_len();
        test_regexp_count_error_order_flags_len_before_start_len();
    }

    fn regexp_count_with_scalar_values(args: &[ScalarValue]) -> Result<ColumnarValue> {
        let args_values = args
            .iter()
            .map(|sv| ColumnarValue::Scalar(sv.clone()))
            .collect();

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, a)| Field::new(format!("arg_{idx}"), a.data_type(), true).into())
            .collect::<Vec<_>>();

        RegexpCountFunc::new().invoke_with_args(ScalarFunctionArgs {
            args: args_values,
            arg_fields,
            number_rows: args.len(),
            return_field: Field::new("f", Int64, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn test_case_sensitive_regexp_count_scalar() {
        let values = ["", "aabca", "abcabc", "abcAbcab", "abcabcabc"];
        let regex = "abc";
        let expected: Vec<i64> = vec![0, 1, 2, 1, 3];

        values.iter().enumerate().for_each(|(pos, &v)| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(Some(regex.to_string()));
            let expected = expected.get(pos).cloned();
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(Some(regex.to_string()));
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(Some(regex.to_string()));
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn test_case_sensitive_regexp_count_empty_pattern_scalar() {
        let values = ["", "abc", "abc"];
        let start_positions = [1, 1, 2];
        let expected: Vec<i64> = vec![1, 4, 3];

        values
            .iter()
            .zip(start_positions.iter())
            .enumerate()
            .for_each(|(pos, (&value, &start))| {
                let expected = expected.get(pos).cloned();
                let start_sv = ScalarValue::Int64(Some(start));

                let re = regexp_count_with_scalar_values(&[
                    ScalarValue::Utf8(Some(value.to_string())),
                    ScalarValue::Utf8(Some("".to_string())),
                    start_sv.clone(),
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_count scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                let re = regexp_count_with_scalar_values(&[
                    ScalarValue::LargeUtf8(Some(value.to_string())),
                    ScalarValue::LargeUtf8(Some("".to_string())),
                    start_sv.clone(),
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_count scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                let re = regexp_count_with_scalar_values(&[
                    ScalarValue::Utf8View(Some(value.to_string())),
                    ScalarValue::Utf8View(Some("".to_string())),
                    start_sv,
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_count scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }
            });
    }

    fn test_case_sensitive_regexp_count_scalar_start() {
        let values = ["", "aabca", "abcabc", "abcAbcab", "abcabcabc"];
        let regex = "abc";
        let start = 2;
        let expected: Vec<i64> = vec![0, 1, 1, 0, 2];

        values.iter().enumerate().for_each(|(pos, &v)| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(Some(regex.to_string()));
            let start_sv = ScalarValue::Int64(Some(start));
            let expected = expected.get(pos).cloned();
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(Some(regex.to_string()));
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(Some(regex.to_string()));
            let re = regexp_count_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn test_case_insensitive_regexp_count_scalar_flags() {
        let values = ["", "aabca", "abcabc", "abcAbcab", "abcabcabc"];
        let regex = "abc";
        let start = 1;
        let flags = "i";
        let expected: Vec<i64> = vec![0, 1, 2, 2, 3];

        values.iter().enumerate().for_each(|(pos, &v)| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(Some(regex.to_string()));
            let start_sv = ScalarValue::Int64(Some(start));
            let flags_sv = ScalarValue::Utf8(Some(flags.to_string()));
            let expected = expected.get(pos).cloned();

            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(Some(regex.to_string()));
            let flags_sv = ScalarValue::LargeUtf8(Some(flags.to_string()));

            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(Some(regex.to_string()));
            let flags_sv = ScalarValue::Utf8View(Some(flags.to_string()));

            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn assert_regexp_count_error_contains(args: &[ArrayRef], expected: &str) {
        let err = regexp_count_func(args).unwrap_err().to_string();
        assert!(
            err.contains(expected),
            "expected error to contain {expected:?}, got {err:?}"
        );
    }

    fn test_regexp_count_error_order_invalid_scalar_regex_before_start_len() {
        let values = Arc::new(GenericStringArray::<i32>::from(vec!["a", "b"]));
        let regex = Arc::new(GenericStringArray::<i32>::from(vec!["["]));
        let start = Arc::new(Int64Array::from(vec![1, 1, 1]));

        assert_regexp_count_error_contains(
            &[values, regex, start],
            "Regular expression did not compile",
        );
    }

    fn test_regexp_count_error_order_flags_len_before_start_len() {
        let values = Arc::new(GenericStringArray::<i32>::from(vec!["a", "b"]));
        let regex = Arc::new(GenericStringArray::<i32>::from(vec!["a"]));
        let start = Arc::new(Int64Array::from(vec![1, 1, 1]));
        let flags = Arc::new(GenericStringArray::<i32>::from(vec!["", "", ""]));

        assert_regexp_count_error_contains(
            &[values, regex, start, flags],
            "flags_array must be the same length as values array; got 3 and 2",
        );
    }

    fn test_case_sensitive_regexp_count_array<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["", "aabca", "abcabc", "abcAbcab", "abcabcAbc"]);
        let regex = A::from(vec!["", "abc", "a", "bc", "ab"]);

        let expected = Int64Array::from(vec![1, 1, 2, 2, 2]);

        let re = regexp_count_func(&[Arc::new(values), Arc::new(regex)]).unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array_start<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["", "aAbca", "abcabc", "abcAbcab", "abcabcAbc"]);
        let regex = A::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let expected = Int64Array::from(vec![1, 0, 1, 1, 0]);

        let re = regexp_count_func(&[Arc::new(values), Arc::new(regex), Arc::new(start)])
            .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_insensitive_regexp_count_array_flags<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["", "aAbca", "abcabc", "abcAbcab", "abcabcAbc"]);
        let regex = A::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1]);
        let flags = A::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![1, 1, 2, 2, 3]);

        let re = regexp_count_func(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_start_scalar_complex() {
        let values = ["", "aabca", "abcabc", "abcAbcab", "abcabcabc"];
        let regex = ["", "abc", "a", "bc", "ab"];
        let start = 5;
        let flags = ["", "i", "", "", "i"];
        let expected: Vec<i64> = vec![0, 0, 0, 1, 1];

        values.iter().enumerate().for_each(|(pos, &v)| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(regex.get(pos).map(|s| (*s).to_string()));
            let start_sv = ScalarValue::Int64(Some(start));
            let flags_sv = ScalarValue::Utf8(flags.get(pos).map(|f| (*f).to_string()));
            let expected = expected.get(pos).cloned();
            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv =
                ScalarValue::LargeUtf8(regex.get(pos).map(|s| (*s).to_string()));
            let flags_sv =
                ScalarValue::LargeUtf8(flags.get(pos).map(|f| (*f).to_string()));
            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv =
                ScalarValue::Utf8View(regex.get(pos).map(|s| (*s).to_string()));
            let flags_sv =
                ScalarValue::Utf8View(flags.get(pos).map(|f| (*f).to_string()));
            let re = regexp_count_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                flags_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn test_case_sensitive_regexp_count_array_complex<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["", "aAbca", "abcabc", "abcAbcab", "abcabcAbc"]);
        let regex = A::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let flags = A::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![1, 1, 1, 1, 1]);

        let re = regexp_count_func(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_regexp_count_cache_check<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["aaa", "Aaa", "aaa"]);
        let regex = A::from(vec!["aaa", "aaa", "aaa"]);
        let start = Int64Array::from(vec![1, 1, 1]);
        let flags = A::from(vec!["", "i", ""]);

        let expected = Int64Array::from(vec![1, 1, 1]);

        let re = regexp_count_func(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }
}
