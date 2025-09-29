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
use arrow::array::{
    Array, ArrayRef, AsArray, Datum, GenericStringArray, Int64Array, StringArrayType,
    StringViewArray,
};
use arrow::datatypes::{DataType, Int64Type};
use arrow::datatypes::{
    DataType::Int64, DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View,
};
use arrow::error::ArrowError;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature::Exact,
    Volatility,
};
use datafusion_macros::user_doc;
use itertools::izip;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extracts a specific group matched by a [regular expression](https://docs.rs/regex/latest/regex/#syntax) in a string.",
    syntax_example = "regexp_extract(str, regexp, idx[, flags])",
    sql_example = r#"```sql
> select regexp_extract('abcAbAbc', 'abc', 2, 'i');
+-------------------------------------------------------------------+
| regexp_extract(Utf8("abcAbAbc"),Utf8("(abc)"),Int64(1),Utf8("i")) |
+-------------------------------------------------------------------+
| abc                                                               |
+-------------------------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    argument(
        name = "idx",
        description = "- **idx**: The index of the matched group to extract."
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
        Self {
            signature: Signature::one_of(
                vec![
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

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
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

        let result = regexp_extract_func(&args);
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

pub fn regexp_extract_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if !(3..=4).contains(&args_len) {
        return exec_err!("regexp_extract was called with {args_len} arguments. It requires at least 3 and at most 4.");
    }

    let values = &args[0];
    match values.data_type() {
        Utf8 | LargeUtf8 | Utf8View => (),
        other => {
            return internal_err!(
                "Unsupported data type {other:?} for function regexp_extract"
            );
        }
    }

    regexp_extract(
        values,
        &args[1],
        &args[2],
        if args_len > 3 { Some(&args[3]) } else { None },
    )
    .map_err(|e| e.into())
}

/// `arrow-rs` style implementation of `regexp_extract` function.
/// This function `regexp_extract` is responsible for extracting a single matching group of a regular expression pattern
/// within a string array. It supports optional flags for case insensitivity.
///
/// The function accepts a variable number of arguments:
/// - `values`: The array of strings to search within.
/// - `regex_array`: The array of regular expression patterns to search for.
/// - `idx`: The index of a group inside the regular expression to extract.
/// - `flags_array` (optional): The array of flags to modify the search behavior (e.g., case insensitivity).
///
/// The function handles different combinations of scalar and array inputs for the regex patterns, group ids,
/// and flags. It uses a cache to store compiled regular expressions for efficiency.
///
/// # Errors
/// Returns an error if the input arrays have mismatched lengths or if the regular expression fails to compile.
pub fn regexp_extract(
    values: &dyn Array,
    regex_array: &dyn Datum,
    idx_array: &dyn Datum,
    flags_array: Option<&dyn Datum>,
) -> Result<ArrayRef, ArrowError> {
    let (regex_array, is_regex_scalar) = regex_array.get();
    let (idx_array, is_idx_scalar) = idx_array.get();
    let (flags_array, is_flags_scalar) = flags_array.map_or((None, true), |flags| {
        let (flags, is_flags_scalar) = flags.get();
        (Some(flags), is_flags_scalar)
    });

    match (values.data_type(), regex_array.data_type(), flags_array) {
        (Utf8, Utf8, None) => regexp_extract_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            None,
            is_flags_scalar,
        ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 => regexp_extract_inner(
            values.as_string::<i32>(),
            regex_array.as_string::<i32>(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            Some(flags_array.as_string::<i32>()),
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, None) => regexp_extract_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            None,
            is_flags_scalar,
        ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 => regexp_extract_inner(
            values.as_string::<i64>(),
            regex_array.as_string::<i64>(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            Some(flags_array.as_string::<i64>()),
            is_flags_scalar,
        ),
        (Utf8View, Utf8View, None) => regexp_extract_inner(
            values.as_string_view(),
            regex_array.as_string_view(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            None,
            is_flags_scalar,
        ),
        (Utf8View, Utf8View, Some(flags_array)) if *flags_array.data_type() == Utf8View => regexp_extract_inner(
            values.as_string_view(),
            regex_array.as_string_view(),
            is_regex_scalar,
            idx_array.as_primitive::<Int64Type>(),
            is_idx_scalar,
            Some(flags_array.as_string_view()),
            is_flags_scalar,
        ),
        _ => Err(ArrowError::ComputeError(
            "regexp_extract() expected the input arrays to be of type Utf8, LargeUtf8, or Utf8View and the data types of the values, regex_array, and flags_array to match".to_string(),
        )),
    }
}

pub fn regexp_extract_inner<'a, S>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    idx_array: &Int64Array,
    is_idx_scalar: bool,
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

    let (idx_scalar, is_idx_scalar) = if is_idx_scalar || idx_array.len() == 1 {
        (Some(idx_array.value(0)), true)
    } else {
        (None, false)
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

    let datatype = values.data_type();

    match (is_regex_scalar, is_idx_scalar, is_flags_scalar) {
        (true, true, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            collect_to_array(
                datatype,
                values
                    .iter()
                    .map(|value| extract_match(value, &pattern, idx_scalar)),
            )
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

            collect_to_array(
                datatype,
                values.iter().zip(flags_array.iter()).map(|(value, flags)| {
                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                    extract_match(value, pattern, idx_scalar)
                }),
            )
        }
        (true, false, true) => {
            let regex = match regex_scalar {
                None | Some("") => {
                    return Ok(Arc::new(Int64Array::from(vec![0; values.len()])))
                }
                Some(regex) => regex,
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            collect_to_array(
                datatype,
                values
                    .iter()
                    .zip(idx_array.iter())
                    .map(|(value, idx)| extract_match(value, &pattern, idx)),
            )
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

            collect_to_array(
                datatype,
                izip!(values.iter(), idx_array.iter(), flags_array.iter()).map(
                    |(value, idx, flags)| {
                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                        extract_match(value, pattern, idx)
                    },
                ),
            )
        }
        (false, true, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            collect_to_array(
                datatype,
                values.iter().zip(regex_array.iter()).map(|(value, regex)| {
                    let regex = match regex {
                        None | Some("") => return Ok(""),
                        Some(regex) => regex,
                    };

                    let pattern =
                        compile_and_cache_regex(regex, flags_scalar, &mut regex_cache)?;
                    extract_match(value, pattern, idx_scalar)
                }),
            )
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

            collect_to_array(
                datatype,
                izip!(values.iter(), regex_array.iter(), flags_array.iter()).map(
                    |(value, regex, flags)| {
                        let regex = match regex {
                            None | Some("") => return Ok(""),
                            Some(regex) => regex,
                        };

                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                        extract_match(value, pattern, idx_scalar)
                    },
                ),
            )
        }
        (false, false, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            if values.len() != idx_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "idx_array must be the same length as values array; got {} and {}",
                    idx_array.len(),
                    values.len(),
                )));
            }

            collect_to_array(
                datatype,
                izip!(values.iter(), regex_array.iter(), idx_array.iter()).map(
                    |(value, regex, start)| {
                        let regex = match regex {
                            None | Some("") => return Ok(""),
                            Some(regex) => regex,
                        };

                        let pattern = compile_and_cache_regex(
                            regex,
                            flags_scalar,
                            &mut regex_cache,
                        )?;
                        extract_match(value, pattern, start)
                    },
                ),
            )
        }
        (false, false, false) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            if values.len() != idx_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "idx_array must be the same length as values array; got {} and {}",
                    idx_array.len(),
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

            collect_to_array(
                datatype,
                izip!(
                    values.iter(),
                    regex_array.iter(),
                    idx_array.iter(),
                    flags_array.iter()
                )
                .map(|(value, regex, idx, flags)| {
                    let regex = match regex {
                        None | Some("") => return Ok(""),
                        Some(regex) => regex,
                    };

                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                    extract_match(value, pattern, idx)
                }),
            )
        }
    }
}

fn extract_match<'v>(
    value: Option<&'v str>,
    pattern: &Regex,
    idx: Option<i64>,
) -> Result<&'v str, ArrowError> {
    let value = match value {
        None | Some("") => return Ok(""),
        Some(value) => value,
    };

    let idx = match idx {
        None => return Ok(""),
        Some(idx) => idx,
    };

    if idx < 1 {
        return Err(ArrowError::ComputeError(
            "regexp_extract() requires idx to be 1 based".to_string(),
        ));
    }

    let capture = extract_match_inner(value, pattern, idx);
    Ok(capture.unwrap_or(""))
}

fn extract_match_inner<'v>(value: &'v str, pattern: &Regex, idx: i64) -> Option<&'v str> {
    Some(pattern.captures(value)?.get(idx as usize)?.as_str())
}

fn collect_to_array<'a>(
    datatype: &DataType,
    result_iter: impl Iterator<Item = Result<&'a str, ArrowError>>,
) -> Result<ArrayRef, ArrowError> {
    let result_iter = result_iter.map(|i| Some(i).transpose());
    match datatype {
        Utf8 => {
            let result =
                result_iter.collect::<Result<GenericStringArray<i32>, ArrowError>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        LargeUtf8 => {
            let result =
                result_iter.collect::<Result<GenericStringArray<i64>, ArrowError>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        Utf8View => {
            let result = result_iter.collect::<Result<StringViewArray, ArrowError>>()?;
            Ok(Arc::new(result) as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function regex_replace")
            .map_err(Into::into),
    }
}
