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

use crate::strings::StringArrayType;
use arrow::array::{Array, ArrayRef, AsArray, Datum, Int64Array};
use arrow::datatypes::{DataType, Int64Type};
use arrow::datatypes::{
    DataType::Int64, DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View,
};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_REGEX;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::Exact,
    TypeSignature::Uniform, Volatility,
};
use itertools::izip;
use regex::Regex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
            .map(|arg| arg.clone().into_array(inferred_length))
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
        Some(get_regexp_count_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_regexp_count_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_REGEX)
            .with_description("Returns the number of matches that a [regular expression](https://docs.rs/regex/latest/regex/#syntax) has in a string.")
            .with_syntax_example("regexp_count(str, regexp[, start, flags])")
            .with_sql_example(r#"```sql
> select regexp_count('abcAbAbc', 'abc', 2, 'i');
+---------------------------------------------------------------+
| regexp_count(Utf8("abcAbAbc"),Utf8("abc"),Int64(2),Utf8("i")) |
+---------------------------------------------------------------+
| 1                                                             |
+---------------------------------------------------------------+
```"#)
            .with_standard_argument("str", Some("String"))
            .with_standard_argument("regexp",Some("Regular"))
            .with_argument("start", "- **start**: Optional start position (the first position is 1) to search for the regular expression. Can be a constant, column, or function.")
            .with_argument("flags",
                           r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#)
            .build()
            .unwrap()
    })
}

pub fn regexp_count_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if !(2..=4).contains(&args_len) {
        return exec_err!("regexp_count was called with {args_len} arguments. It requires at least 2 and at most 4.");
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
pub fn regexp_count(
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

pub fn regexp_count_inner<'a, S>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    start_array: Option<&Int64Array>,
    is_start_scalar: bool,
    flags_array: Option<S>,
    is_flags_scalar: bool,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a>,
{
    let (regex_scalar, is_regex_scalar) = if is_regex_scalar || regex_array.len() == 1 {
        (Some(regex_array.value(0)), true)
    } else {
        (None, false)
    };

    let (start_array, start_scalar, is_start_scalar) =
        if let Some(start_array) = start_array {
            if is_start_scalar || start_array.len() == 1 {
                (None, Some(start_array.value(0)), true)
            } else {
                (Some(start_array), None, false)
            }
        } else {
            (None, Some(1), true)
        };

    let (flags_array, flags_scalar, is_flags_scalar) =
        if let Some(flags_array) = flags_array {
            if is_flags_scalar || flags_array.len() == 1 {
                (None, Some(flags_array.value(0)), true)
            } else {
                (Some(flags_array), None, false)
            }
        } else {
            (None, None, true)
        };

    let mut regex_cache = HashMap::new();

    match (is_regex_scalar, is_start_scalar, is_flags_scalar) {
        (true, true, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            Ok(Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .map(|value| count_matches(value, &pattern, start_scalar))
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (true, true, false) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .zip(flags_array.iter())
                    .map(|(value, flags)| {
                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                        count_matches(value, &pattern, start_scalar)
                    })
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (true, false, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            let start_array = start_array.unwrap();

            Ok(Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .zip(start_array.iter())
                    .map(|(value, start)| count_matches(value, &pattern, start))
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (true, false, false) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                izip!(
                    values.iter(),
                    start_array.unwrap().iter(),
                    flags_array.iter()
                )
                .map(|(value, start, flags)| {
                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                    count_matches(value, &pattern, start)
                })
                .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (false, true, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .zip(regex_array.iter())
                    .map(|(value, regex)| {
                        let regex = match regex {
                            None | Some("") => return Ok(0),
                            Some(regex) => regex,
                        };

                        let pattern = compile_and_cache_regex(
                            regex,
                            flags_scalar,
                            &mut regex_cache,
                        )?;
                        count_matches(value, &pattern, start_scalar)
                    })
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (false, true, false) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                izip!(values.iter(), regex_array.iter(), flags_array.iter())
                    .map(|(value, regex, flags)| {
                        let regex = match regex {
                            None | Some("") => return Ok(0),
                            Some(regex) => regex,
                        };

                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                        count_matches(value, &pattern, start_scalar)
                    })
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (false, false, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let start_array = start_array.unwrap();
            if values.len() != start_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "start_array must be the same length as values array; got {} and {}",
                    start_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                izip!(values.iter(), regex_array.iter(), start_array.iter())
                    .map(|(value, regex, start)| {
                        let regex = match regex {
                            None | Some("") => return Ok(0),
                            Some(regex) => regex,
                        };

                        let pattern = compile_and_cache_regex(
                            regex,
                            flags_scalar,
                            &mut regex_cache,
                        )?;
                        count_matches(value, &pattern, start)
                    })
                    .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
        (false, false, false) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            let start_array = start_array.unwrap();
            if values.len() != start_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "start_array must be the same length as values array; got {} and {}",
                    start_array.len(),
                    values.len(),
                )));
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(Int64Array::from_iter_values(
                izip!(
                    values.iter(),
                    regex_array.iter(),
                    start_array.iter(),
                    flags_array.iter()
                )
                .map(|(value, regex, start, flags)| {
                    let regex = match regex {
                        None | Some("") => return Ok(0),
                        Some(regex) => regex,
                    };

                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                    count_matches(value, &pattern, start)
                })
                .collect::<Result<Vec<i64>, ArrowError>>()?,
            )))
        }
    }
}

fn compile_and_cache_regex(
    regex: &str,
    flags: Option<&str>,
    regex_cache: &mut HashMap<String, Regex>,
) -> Result<Regex, ArrowError> {
    match regex_cache.entry(regex.to_string()) {
        Entry::Vacant(entry) => {
            let compiled = compile_regex(regex, flags)?;
            entry.insert(compiled.clone());
            Ok(compiled)
        }
        Entry::Occupied(entry) => Ok(entry.get().to_owned()),
    }
}

fn compile_regex(regex: &str, flags: Option<&str>) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") {
                return Err(ArrowError::ComputeError(
                    "regexp_count() does not support global flag".to_string(),
                ));
            }
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
    })
}

fn count_matches(
    value: Option<&str>,
    pattern: &Regex,
    start: Option<i64>,
) -> Result<i64, ArrowError> {
    let value = match value {
        None | Some("") => return Ok(0),
        Some(value) => value,
    };

    if let Some(start) = start {
        if start < 1 {
            return Err(ArrowError::ComputeError(
                "regexp_count() requires start to be 1 based".to_string(),
            ));
        }

        let find_slice = value.chars().skip(start as usize - 1).collect::<String>();
        let count = pattern.find_iter(find_slice.as_str()).count();
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

    #[test]
    fn test_regexp_count() {
        test_case_sensitive_regexp_count_scalar();
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

            let re = RegexpCountFunc::new()
                .invoke(&[ColumnarValue::Scalar(v_sv), ColumnarValue::Scalar(regex_sv)]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(Some(regex.to_string()));

            let re = RegexpCountFunc::new()
                .invoke(&[ColumnarValue::Scalar(v_sv), ColumnarValue::Scalar(regex_sv)]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(Some(regex.to_string()));

            let re = RegexpCountFunc::new()
                .invoke(&[ColumnarValue::Scalar(v_sv), ColumnarValue::Scalar(regex_sv)]);
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv),
            ]);
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
                ColumnarValue::Scalar(flags_sv.clone()),
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
                ColumnarValue::Scalar(flags_sv.clone()),
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

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv),
                ColumnarValue::Scalar(flags_sv.clone()),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }
        });
    }

    fn test_case_sensitive_regexp_count_array<A>()
    where
        A: From<Vec<&'static str>> + Array + 'static,
    {
        let values = A::from(vec!["", "aabca", "abcabc", "abcAbcab", "abcabcAbc"]);
        let regex = A::from(vec!["", "abc", "a", "bc", "ab"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 2]);

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

        let expected = Int64Array::from(vec![0, 0, 1, 1, 0]);

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

        let expected = Int64Array::from(vec![0, 1, 2, 2, 3]);

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
            let regex_sv = ScalarValue::Utf8(regex.get(pos).map(|s| s.to_string()));
            let start_sv = ScalarValue::Int64(Some(start));
            let flags_sv = ScalarValue::Utf8(flags.get(pos).map(|f| f.to_string()));
            let expected = expected.get(pos).cloned();

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
                ColumnarValue::Scalar(flags_sv.clone()),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // largeutf8
            let v_sv = ScalarValue::LargeUtf8(Some(v.to_string()));
            let regex_sv = ScalarValue::LargeUtf8(regex.get(pos).map(|s| s.to_string()));
            let flags_sv = ScalarValue::LargeUtf8(flags.get(pos).map(|f| f.to_string()));

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv.clone()),
                ColumnarValue::Scalar(flags_sv.clone()),
            ]);
            match re {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                    assert_eq!(v, expected, "regexp_count scalar test failed");
                }
                _ => panic!("Unexpected result"),
            }

            // utf8view
            let v_sv = ScalarValue::Utf8View(Some(v.to_string()));
            let regex_sv = ScalarValue::Utf8View(regex.get(pos).map(|s| s.to_string()));
            let flags_sv = ScalarValue::Utf8View(flags.get(pos).map(|f| f.to_string()));

            let re = RegexpCountFunc::new().invoke(&[
                ColumnarValue::Scalar(v_sv),
                ColumnarValue::Scalar(regex_sv),
                ColumnarValue::Scalar(start_sv),
                ColumnarValue::Scalar(flags_sv.clone()),
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

        let expected = Int64Array::from(vec![0, 1, 1, 1, 1]);

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
