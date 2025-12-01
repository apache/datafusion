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

use arrow::array::{
    Array, ArrayRef, AsArray, Datum, Int64Array, PrimitiveArray, StringArrayType,
};
use arrow::datatypes::{DataType, Int64Type};
use arrow::datatypes::{
    DataType::Int64, DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View,
};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::Exact,
    TypeSignature::Uniform, Volatility,
};
use datafusion_macros::user_doc;
use itertools::izip;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

use crate::regex::compile_and_cache_regex;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns the position in a string where the specified occurrence of a POSIX regular expression is located.",
    syntax_example = "regexp_instr(str, regexp[, start[, N[, flags[, subexpr]]]])",
    sql_example = r#"```sql
> SELECT regexp_instr('ABCDEF', 'C(.)(..)');
+---------------------------------------------------------------+
| regexp_instr(Utf8("ABCDEF"),Utf8("C(.)(..)"))                 |
+---------------------------------------------------------------+
| 3                                                             |
+---------------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    argument(
        name = "start",
        description = "- **start**: Optional start position (the first position is 1) to search for the regular expression. Can be a constant, column, or function. Defaults to 1"
    ),
    argument(
        name = "N",
        description = "- **N**: Optional The N-th occurrence of pattern to find. Defaults to 1 (first match). Can be a constant, column, or function."
    ),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#
    ),
    argument(
        name = "subexpr",
        description = "Optional Specifies which capture group (subexpression) to return the position for. Defaults to 0, which returns the position of the entire match."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpInstrFunc {
    signature: Signature,
}

impl Default for RegexpInstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpInstrFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Uniform(2, vec![Utf8View, LargeUtf8, Utf8]),
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![Utf8View, Utf8View, Int64, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, Int64]),
                    Exact(vec![Utf8, Utf8, Int64, Int64]),
                    Exact(vec![Utf8View, Utf8View, Int64, Int64, Utf8View]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, Int64, LargeUtf8]),
                    Exact(vec![Utf8, Utf8, Int64, Int64, Utf8]),
                    Exact(vec![Utf8View, Utf8View, Int64, Int64, Utf8View, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, Int64, LargeUtf8, Int64]),
                    Exact(vec![Utf8, Utf8, Int64, Int64, Utf8, Int64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpInstrFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_instr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
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

        let result = regexp_instr_func(&args);
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

pub fn regexp_instr_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if !(2..=6).contains(&args_len) {
        return exec_err!("regexp_instr was called with {args_len} arguments. It requires at least 2 and at most 6.");
    }

    let values = &args[0];
    match values.data_type() {
        Utf8 | LargeUtf8 | Utf8View => (),
        other => {
            return internal_err!(
                "Unsupported data type {other:?} for function regexp_instr"
            );
        }
    }

    regexp_instr(
        values,
        &args[1],
        if args_len > 2 { Some(&args[2]) } else { None },
        if args_len > 3 { Some(&args[3]) } else { None },
        if args_len > 4 { Some(&args[4]) } else { None },
        if args_len > 5 { Some(&args[5]) } else { None },
    )
    .map_err(|e| e.into())
}

/// `arrow-rs` style implementation of `regexp_instr` function.
/// This function `regexp_instr` is responsible for returning the index of a regular expression pattern
/// within a string array. It supports optional start positions and flags for case insensitivity.
///
/// The function accepts a variable number of arguments:
/// - `values`: The array of strings to search within.
/// - `regex_array`: The array of regular expression patterns to search for.
/// - `start_array` (optional): The array of start positions for the search.
/// - `nth_array` (optional): The array of start nth for the search.
/// - `endoption_array` (optional): The array of endoption positions for the search.
/// - `flags_array` (optional): The array of flags to modify the search behavior (e.g., case insensitivity).
/// - `subexpr_array` (optional): The array of subexpr positions for the search.
///
/// The function handles different combinations of scalar and array inputs for the regex patterns, start positions,
/// and flags. It uses a cache to store compiled regular expressions for efficiency.
///
/// # Errors
/// Returns an error if the input arrays have mismatched lengths or if the regular expression fails to compile.
fn regexp_instr(
    values: &dyn Array,
    regex_array: &dyn Datum,
    start_array: Option<&dyn Datum>,
    nth_array: Option<&dyn Datum>,
    flags_array: Option<&dyn Datum>,
    subexpr_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (regex_array, _) = regex_array.get();
    let start_array = start_array.map(|start| {
        let (start, _) = start.get();
        start
    });
    let nth_array = nth_array.map(|nth| {
        let (nth, _) = nth.get();
        nth
    });
    let flags_array = flags_array.map(|flags| {
        let (flags, _) = flags.get();
        flags
    });
    let subexpr_array = subexpr_array.map(|subexpr| {
        let (subexpr, _) = subexpr.get();
        subexpr
    });

    match (values.data_type(), regex_array.data_type(), flags_array) {
        (Utf8, Utf8, None) => regexp_instr_inner(
            &values.as_string::<i32>(),
            &regex_array.as_string::<i32>(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            None,
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 => regexp_instr_inner(
            &values.as_string::<i32>(),
            &regex_array.as_string::<i32>(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            Some(flags_array.as_string::<i32>()),
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        (LargeUtf8, LargeUtf8, None) => regexp_instr_inner(
            &values.as_string::<i64>(),
            &regex_array.as_string::<i64>(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            None,
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 => regexp_instr_inner(
            &values.as_string::<i64>(),
            &regex_array.as_string::<i64>(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            Some(flags_array.as_string::<i64>()),
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        (Utf8View, Utf8View, None) => regexp_instr_inner(
            &values.as_string_view(),
            &regex_array.as_string_view(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            None,
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        (Utf8View, Utf8View, Some(flags_array)) if *flags_array.data_type() == Utf8View => regexp_instr_inner(
            &values.as_string_view(),
            &regex_array.as_string_view(),
            start_array.map(|start| start.as_primitive::<Int64Type>()),
            nth_array.map(|nth| nth.as_primitive::<Int64Type>()),
            Some(flags_array.as_string_view()),
            subexpr_array.map(|subexpr| subexpr.as_primitive::<Int64Type>()),
        ),
        _ => Err(ArrowError::ComputeError(
            "regexp_instr() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the values, regex_array, and flags_array to match".to_string(),
        )),
    }
}

fn regexp_instr_inner<'a, S>(
    values: &S,
    regex_array: &S,
    start_array: Option<&Int64Array>,
    nth_array: Option<&Int64Array>,
    flags_array: Option<S>,
    subexp_array: Option<&Int64Array>,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a>,
{
    let len = values.len();

    let default_start_array = PrimitiveArray::<Int64Type>::from(vec![1; len]);
    let start_array = start_array.unwrap_or(&default_start_array);
    let start_input: Vec<i64> = (0..start_array.len())
        .map(|i| start_array.value(i)) // handle nulls as 0
        .collect();

    let default_nth_array = PrimitiveArray::<Int64Type>::from(vec![1; len]);
    let nth_array = nth_array.unwrap_or(&default_nth_array);
    let nth_input: Vec<i64> = (0..nth_array.len())
        .map(|i| nth_array.value(i)) // handle nulls as 0
        .collect();

    let flags_input = match flags_array {
        Some(flags) => flags.iter().collect(),
        None => vec![None; len],
    };

    let default_subexp_array = PrimitiveArray::<Int64Type>::from(vec![0; len]);
    let subexp_array = subexp_array.unwrap_or(&default_subexp_array);
    let subexp_input: Vec<i64> = (0..subexp_array.len())
        .map(|i| subexp_array.value(i)) // handle nulls as 0
        .collect();

    let mut regex_cache = HashMap::new();

    let result: Result<Vec<Option<i64>>, ArrowError> = izip!(
        values.iter(),
        regex_array.iter(),
        start_input.iter(),
        nth_input.iter(),
        flags_input.iter(),
        subexp_input.iter()
    )
    .map(|(value, regex, start, nth, flags, subexp)| match regex {
        None => Ok(None),
        Some("") => Ok(Some(0)),
        Some(regex) => get_index(
            value,
            regex,
            *start,
            *nth,
            *subexp,
            *flags,
            &mut regex_cache,
        ),
    })
    .collect();
    Ok(Arc::new(Int64Array::from(result?)))
}

fn handle_subexp(
    pattern: &Regex,
    search_slice: &str,
    subexpr: i64,
    value: &str,
    byte_start_offset: usize,
) -> Result<Option<i64>, ArrowError> {
    if let Some(captures) = pattern.captures(search_slice) {
        if let Some(matched) = captures.get(subexpr as usize) {
            // Convert byte offset relative to search_slice back to 1-based character offset
            // relative to the original `value` string.
            let start_char_offset =
                value[..byte_start_offset + matched.start()].chars().count() as i64 + 1;
            return Ok(Some(start_char_offset));
        }
    }
    Ok(Some(0)) // Return 0 if the subexpression was not found
}

fn get_nth_match(
    pattern: &Regex,
    search_slice: &str,
    n: i64,
    byte_start_offset: usize,
    value: &str,
) -> Result<Option<i64>, ArrowError> {
    if let Some(mat) = pattern.find_iter(search_slice).nth((n - 1) as usize) {
        // Convert byte offset relative to search_slice back to 1-based character offset
        // relative to the original `value` string.
        let match_start_byte_offset = byte_start_offset + mat.start();
        let match_start_char_offset =
            value[..match_start_byte_offset].chars().count() as i64 + 1;
        Ok(Some(match_start_char_offset))
    } else {
        Ok(Some(0)) // Return 0 if the N-th match was not found
    }
}
fn get_index<'strings, 'cache>(
    value: Option<&str>,
    pattern: &'strings str,
    start: i64,
    n: i64,
    subexpr: i64,
    flags: Option<&'strings str>,
    regex_cache: &'cache mut HashMap<(&'strings str, Option<&'strings str>), Regex>,
) -> Result<Option<i64>, ArrowError>
where
    'strings: 'cache,
{
    let value = match value {
        None => return Ok(None),
        Some("") => return Ok(Some(0)),
        Some(value) => value,
    };
    let pattern: &Regex = compile_and_cache_regex(pattern, flags, regex_cache)?;
    // println!("get_index: value = {}, pattern = {}, start = {}, n = {}, subexpr = {}, flags = {:?}", value, pattern, start, n, subexpr, flags);
    if start < 1 {
        return Err(ArrowError::ComputeError(
            "regexp_instr() requires start to be 1-based".to_string(),
        ));
    }

    if n < 1 {
        return Err(ArrowError::ComputeError(
            "N must be 1 or greater".to_string(),
        ));
    }

    // --- Simplified byte_start_offset calculation ---
    let total_chars = value.chars().count() as i64;
    let byte_start_offset: usize = if start > total_chars {
        // If start is beyond the total characters, it means we start searching
        // after the string effectively. No matches possible.
        return Ok(Some(0));
    } else {
        // Get the byte offset for the (start - 1)-th character (0-based)
        value
            .char_indices()
            .nth((start - 1) as usize)
            .map(|(idx, _)| idx)
            .unwrap_or(0) // Should not happen if start is valid and <= total_chars
    };
    // --- End simplified calculation ---

    let search_slice = &value[byte_start_offset..];

    // Handle subexpression capturing first, as it takes precedence
    if subexpr > 0 {
        return handle_subexp(pattern, search_slice, subexpr, value, byte_start_offset);
    }

    // Use nth to get the N-th match (n is 1-based, nth is 0-based)
    get_nth_match(pattern, search_slice, n, byte_start_offset, value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::array::{GenericStringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;
    #[test]
    fn test_regexp_instr() {
        test_case_sensitive_regexp_instr_nulls();
        test_case_sensitive_regexp_instr_scalar();
        test_case_sensitive_regexp_instr_scalar_start();
        test_case_sensitive_regexp_instr_scalar_nth();
        test_case_sensitive_regexp_instr_scalar_subexp();

        test_case_sensitive_regexp_instr_array::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_instr_array::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_instr_array::<StringViewArray>();

        test_case_sensitive_regexp_instr_array_start::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_instr_array_start::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_instr_array_start::<StringViewArray>();

        test_case_sensitive_regexp_instr_array_nth::<GenericStringArray<i32>>();
        test_case_sensitive_regexp_instr_array_nth::<GenericStringArray<i64>>();
        test_case_sensitive_regexp_instr_array_nth::<StringViewArray>();
    }

    fn regexp_instr_with_scalar_values(args: &[ScalarValue]) -> Result<ColumnarValue> {
        let args_values: Vec<ColumnarValue> = args
            .iter()
            .map(|sv| ColumnarValue::Scalar(sv.clone()))
            .collect();

        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, a)| {
                Arc::new(Field::new(format!("arg_{idx}"), a.data_type(), true))
            })
            .collect::<Vec<_>>();

        RegexpInstrFunc::new().invoke_with_args(ScalarFunctionArgs {
            args: args_values,
            arg_fields,
            number_rows: args.len(),
            return_field: Arc::new(Field::new("f", Int64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn test_case_sensitive_regexp_instr_nulls() {
        let v = "";
        let r = "";
        let expected = 0;
        let regex_sv = ScalarValue::Utf8(Some(r.to_string()));
        let re = regexp_instr_with_scalar_values(&[v.to_string().into(), regex_sv]);
        // let res_exp = re.unwrap();
        match re {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                assert_eq!(v, Some(expected), "regexp_instr scalar test failed");
            }
            _ => panic!("Unexpected result"),
        }
    }
    fn test_case_sensitive_regexp_instr_scalar() {
        let values = [
            "hello world",
            "abcdefg",
            "xyz123xyz",
            "no match here",
            "abc",
            "ДатаФусион数据融合📊🔥",
        ];
        let regex = ["o", "d", "123", "z", "gg", "📊"];

        let expected: Vec<i64> = vec![5, 4, 4, 0, 0, 15];

        izip!(values.iter(), regex.iter())
            .enumerate()
            .for_each(|(pos, (&v, &r))| {
                // utf8
                let v_sv = ScalarValue::Utf8(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8(Some(r.to_string()));
                let expected = expected.get(pos).cloned();
                let re = regexp_instr_with_scalar_values(&[v_sv, regex_sv]);
                // let res_exp = re.unwrap();
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // largeutf8
                let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
                let regex_sv = ScalarValue::LargeUtf8(Some(r.to_string()));
                let re = regexp_instr_with_scalar_values(&[v_sv, regex_sv]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // utf8view
                let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8View(Some(r.to_string()));
                let re = regexp_instr_with_scalar_values(&[v_sv, regex_sv]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }
            });
    }

    fn test_case_sensitive_regexp_instr_scalar_start() {
        let values = ["abcabcabc", "abcabcabc", ""];
        let regex = ["abc", "abc", "gg"];
        let start = [4, 5, 5];
        let expected: Vec<i64> = vec![4, 7, 0];

        izip!(values.iter(), regex.iter(), start.iter())
            .enumerate()
            .for_each(|(pos, (&v, &r, &s))| {
                // utf8
                let v_sv = ScalarValue::Utf8(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let expected = expected.get(pos).cloned();
                let re =
                    regexp_instr_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // largeutf8
                let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
                let regex_sv = ScalarValue::LargeUtf8(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let re =
                    regexp_instr_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // utf8view
                let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8View(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let re =
                    regexp_instr_with_scalar_values(&[v_sv, regex_sv, start_sv.clone()]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }
            });
    }

    fn test_case_sensitive_regexp_instr_scalar_nth() {
        let values = ["abcabcabc", "abcabcabc", "abcabcabc", "abcabcabc"];
        let regex = ["abc", "abc", "abc", "abc"];
        let start = [1, 1, 1, 1];
        let nth = [1, 2, 3, 4];
        let expected: Vec<i64> = vec![1, 4, 7, 0];

        izip!(values.iter(), regex.iter(), start.iter(), nth.iter())
            .enumerate()
            .for_each(|(pos, (&v, &r, &s, &n))| {
                // utf8
                let v_sv = ScalarValue::Utf8(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let nth_sv = ScalarValue::Int64(Some(n));
                let expected = expected.get(pos).cloned();
                let re = regexp_instr_with_scalar_values(&[
                    v_sv,
                    regex_sv,
                    start_sv.clone(),
                    nth_sv.clone(),
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // largeutf8
                let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
                let regex_sv = ScalarValue::LargeUtf8(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let nth_sv = ScalarValue::Int64(Some(n));
                let re = regexp_instr_with_scalar_values(&[
                    v_sv,
                    regex_sv,
                    start_sv.clone(),
                    nth_sv.clone(),
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }

                // utf8view
                let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
                let regex_sv = ScalarValue::Utf8View(Some(r.to_string()));
                let start_sv = ScalarValue::Int64(Some(s));
                let nth_sv = ScalarValue::Int64(Some(n));
                let re = regexp_instr_with_scalar_values(&[
                    v_sv,
                    regex_sv,
                    start_sv.clone(),
                    nth_sv.clone(),
                ]);
                match re {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                        assert_eq!(v, expected, "regexp_instr scalar test failed");
                    }
                    _ => panic!("Unexpected result"),
                }
            });
    }

    fn test_case_sensitive_regexp_instr_scalar_subexp() {
        let values = ["12 abc def ghi 34"];
        let regex = ["(abc) (def) (ghi)"];
        let start = [1];
        let nth = [1];
        let flags = ["i"];
        let subexps = [2];
        let expected: Vec<i64> = vec![8];

        izip!(
            values.iter(),
            regex.iter(),
            start.iter(),
            nth.iter(),
            flags.iter(),
            subexps.iter()
        )
        .enumerate()
        .for_each(|(pos, (&v, &r, &s, &n, &flag, &subexp))| {
            // utf8
            let v_sv = ScalarValue::Utf8(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8(Some(r.to_string()));
            let start_sv = ScalarValue::Int64(Some(s));
            let nth_sv = ScalarValue::Int64(Some(n));
            let flags_sv = ScalarValue::Utf8(Some(flag.to_string()));
            let subexp_sv = ScalarValue::Int64(Some(subexp));
            let expected = expected.get(pos).cloned();
            let re = regexp_instr_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                nth_sv.clone(),
                flags_sv,
                subexp_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_instr scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(Some(r.to_string()));
            let start_sv = ScalarValue::Int64(Some(s));
            let nth_sv = ScalarValue::Int64(Some(n));
            let flags_sv = ScalarValue::LargeUtf8(Some(flag.to_string()));
            let subexp_sv = ScalarValue::Int64(Some(subexp));
            let re = regexp_instr_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                nth_sv.clone(),
                flags_sv,
                subexp_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_instr scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(Some(r.to_string()));
            let start_sv = ScalarValue::Int64(Some(s));
            let nth_sv = ScalarValue::Int64(Some(n));
            let flags_sv = ScalarValue::Utf8View(Some(flag.to_string()));
            let subexp_sv = ScalarValue::Int64(Some(subexp));
            let re = regexp_instr_with_scalar_values(&[
                v_sv,
                regex_sv,
                start_sv.clone(),
                nth_sv.clone(),
                flags_sv,
                subexp_sv.clone(),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_instr scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn test_case_sensitive_regexp_instr_array<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec![
            "hello world",
            "abcdefg",
            "xyz123xyz",
            "no match here",
            "",
        ]);
        let regex = A::from(vec!["o", "d", "123", "z", "gg"]);

        let expected = Int64Array::from(vec![5, 4, 4, 0, 0]);
        let re = regexp_instr_func(&[Arc::new(values), Arc::new(regex)]).unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_instr_array_start<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["abcabcabc", "abcabcabc", ""]);
        let regex = A::from(vec!["abc", "abc", "gg"]);
        let start = Int64Array::from(vec![4, 5, 5]);
        let expected = Int64Array::from(vec![4, 7, 0]);

        let re = regexp_instr_func(&[Arc::new(values), Arc::new(regex), Arc::new(start)])
            .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_instr_array_nth<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["abcabcabc", "abcabcabc", "abcabcabc", "abcabcabc"]);
        let regex = A::from(vec!["abc", "abc", "abc", "abc"]);
        let start = Int64Array::from(vec![1, 1, 1, 1]);
        let nth = Int64Array::from(vec![1, 2, 3, 4]);
        let expected = Int64Array::from(vec![1, 4, 7, 0]);

        let re = regexp_instr_func(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(nth),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }
}
