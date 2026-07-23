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
    Array, ArrayRef, AsArray, Datum, Int64Array, Int64Builder, StringArrayType,
};
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
use std::collections::hash_map::Entry;
use std::sync::Arc;

use crate::regex::compile_regex;

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
    fn name(&self) -> &str {
        "regexp_instr"
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
        return exec_err!(
            "regexp_instr was called with {args_len} arguments. It requires at least 2 and at most 6."
        );
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
            Some(&flags_array.as_string::<i32>()),
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
            Some(&flags_array.as_string::<i64>()),
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
            Some(&flags_array.as_string_view()),
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
    flags_array: Option<&S>,
    subexp_array: Option<&Int64Array>,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a>,
{
    let len = values.len();
    let mut regex_cache = RegexCache::default();
    let mut result = Int64Builder::with_capacity(len);

    for i in 0..len {
        if regex_array.is_null(i) {
            result.append_null();
            continue;
        }
        let regex = regex_array.value(i);
        if regex.is_empty() {
            result.append_value(0);
            continue;
        }

        if values.is_null(i) {
            result.append_null();
            continue;
        }
        let value = values.value(i);
        if value.is_empty() {
            result.append_value(0);
            continue;
        }

        let flags = match flags_array {
            Some(flags) if !flags.is_null(i) => Some(flags.value(i)),
            _ => None,
        };
        let pattern = regex_cache.get_or_compile(regex, flags)?;

        // The defaults apply when the optional argument was not supplied at
        // all. A supplied but null slot reads through as its raw buffer value.
        let start = start_array.map_or(1, |array| array.value(i));
        let nth = nth_array.map_or(1, |array| array.value(i));
        let subexp = subexp_array.map_or(0, |array| array.value(i));

        result.append_value(get_index(value, pattern, start, nth, subexp)?);
    }

    Ok(Arc::new(result.finish()))
}

/// Compiles the patterns seen so far, keyed by `(pattern, flags)`.
///
/// Patterns are addressed by index rather than by reference so that `last` can
/// memoize the previous row's pattern without holding a borrow of `indices`
/// across rows. A literal pattern yields the same string on every row, so that
/// memo means the common case never hashes a key.
#[derive(Default)]
struct RegexCache<'a> {
    compiled: Vec<Regex>,
    indices: HashMap<(&'a str, Option<&'a str>), usize>,
    last: Option<((&'a str, Option<&'a str>), usize)>,
}

impl<'a> RegexCache<'a> {
    fn get_or_compile(
        &mut self,
        regex: &'a str,
        flags: Option<&'a str>,
    ) -> Result<&Regex, ArrowError> {
        let key = (regex, flags);
        let index = match self.last {
            Some((last_key, index)) if last_key == key => index,
            _ => {
                let index = match self.indices.entry(key) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        self.compiled.push(compile_regex(regex, flags)?);
                        *entry.insert(self.compiled.len() - 1)
                    }
                };
                self.last = Some((key, index));
                index
            }
        };
        Ok(&self.compiled[index])
    }
}

/// Returns the 1-based character position of the `n`-th match of `pattern` in
/// `value`, or 0 if there is no such match. The search begins at the 1-based
/// character position `start`. A positive `subexpr` selects that capture group
/// of the first match instead of the `n`-th match. `value` is non-empty.
fn get_index(
    value: &str,
    pattern: &Regex,
    start: i64,
    n: i64,
    subexpr: i64,
) -> Result<i64, ArrowError> {
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

    // Byte offset of the `start`-th character. A `start` past the end of the
    // string leaves nothing to search, so no match is possible.
    let Some((byte_start_offset, _)) = value.char_indices().nth((start - 1) as usize)
    else {
        return Ok(0);
    };
    let search_slice = &value[byte_start_offset..];

    // A subexpression, when requested, takes precedence over the N-th match.
    let match_start = if subexpr > 0 {
        pattern
            .captures(search_slice)
            .and_then(|captures| captures.get(subexpr as usize))
            .map(|matched| matched.start())
    } else {
        // `n` is 1-based, `nth` is 0-based.
        pattern
            .find_iter(search_slice)
            .nth((n - 1) as usize)
            .map(|matched| matched.start())
    };

    // Convert the byte offset within `search_slice` back to a 1-based character
    // offset within `value`.
    Ok(match_start.map_or(0, |offset| {
        value[..byte_start_offset + offset].chars().count() as i64 + 1
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{GenericStringArray, StringViewArray};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use itertools::izip;
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
