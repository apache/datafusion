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

use arrow::array::{Array, ArrayRef, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::Int64, DataType::LargeUtf8, DataType::Utf8, DataType::Utf8View,
};
use arrow::error::ArrowError;
use datafusion_common::cast::{as_generic_string_array, as_int64_array};
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature::Exact,
    TypeSignature::Uniform, Volatility,
};
use itertools::izip;
use regex::Regex;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

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
                    Uniform(2, vec![Utf8, LargeUtf8, Utf8View]),
                    Exact(vec![Utf8, Utf8, Int64]),
                    Exact(vec![Utf8, Utf8, Int64, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    Exact(vec![LargeUtf8, LargeUtf8, Int64, LargeUtf8]),
                    Exact(vec![Utf8View, Utf8View, Int64]),
                    Exact(vec![Utf8View, Utf8View, Int64, Utf8View]),
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

    fn invoke(&self, args: &[datafusion_expr::ColumnarValue]) -> Result<ColumnarValue> {
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
}

fn regexp_count_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Utf8 => regexp_count::<i32>(args),
        LargeUtf8 => regexp_count::<i64>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_count")
        }
    }
}

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
pub fn regexp_count<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let args_len = args.len();
    if !(2..=4).contains(&args_len) {
        return exec_err!("regexp_count was called with {args_len} arguments. It requires at least 2 and at most 4.");
    }

    let values = as_generic_string_array::<O>(&args[0])?;
    let regex_array = as_generic_string_array::<O>(&args[1])?;

    let (regex_scalar, is_regex_scalar) = if regex_array.len() == 1 {
        (Some(regex_array.value(0)), true)
    } else {
        (None, false)
    };

    let (start_array, start_scalar, is_start_scalar) = if args.len() > 2 {
        let start = as_int64_array(&args[2])?;
        if start.len() == 1 {
            (None, Some(start.value(0)), true)
        } else {
            (Some(start), None, false)
        }
    } else {
        (None, Some(1), true)
    };

    let (flags_array, flags_scalar, is_flags_scalar) = if args.len() > 3 {
        let flags = as_generic_string_array::<O>(&args[3])?;
        if flags.len() == 1 {
            (None, Some(flags.value(0)), true)
        } else {
            (Some(flags), None, false)
        }
    } else {
        (None, None, true)
    };

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
                    .collect::<Result<Vec<i64>>>()?,
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
                return exec_err!(
                    "flags_array must be the same length as values array; got {} and {}",
                    values.len(),
                    flags_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
            Ok(Arc::new(Int64Array::from_iter_values(
                values
                    .iter()
                    .zip(flags_array.iter())
                    .map(|(value, flags)| {
                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                        count_matches(value, &pattern, start_scalar)
                    })
                    .collect::<Result<Vec<i64>>>()?,
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
                    .collect::<Result<Vec<i64>>>()?,
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
                return exec_err!(
                    "flags_array must be the same length as values array; got {} and {}",
                    values.len(),
                    flags_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
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
                .collect::<Result<Vec<i64>>>()?,
            )))
        }
        (false, true, true) => {
            if values.len() != regex_array.len() {
                return exec_err!(
                    "regex_array must be the same length as values array; got {} and {}",
                    values.len(),
                    regex_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
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
                    .collect::<Result<Vec<i64>>>()?,
            )))
        }
        (false, true, false) => {
            if values.len() != regex_array.len() {
                return exec_err!(
                    "regex_array must be the same length as values array; got {} and {}",
                    values.len(),
                    regex_array.len()
                );
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return exec_err!(
                    "flags_array must be the same length as values array; got {} and {}",
                    values.len(),
                    flags_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
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
                    .collect::<Result<Vec<i64>>>()?,
            )))
        }
        (false, false, true) => {
            if values.len() != regex_array.len() {
                return exec_err!(
                    "regex_array must be the same length as values array; got {} and {}",
                    values.len(),
                    regex_array.len()
                );
            }

            let start_array = start_array.unwrap();
            if values.len() != start_array.len() {
                return exec_err!(
                    "start_array must be the same length as values array; got {} and {}",
                    values.len(),
                    start_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
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
                    .collect::<Result<Vec<i64>>>()?,
            )))
        }
        (false, false, false) => {
            if values.len() != regex_array.len() {
                return exec_err!(
                    "regex_array must be the same length as values array; got {} and {}",
                    values.len(),
                    regex_array.len()
                );
            }

            let start_array = start_array.unwrap();
            if values.len() != start_array.len() {
                return exec_err!(
                    "start_array must be the same length as values array; got {} and {}",
                    values.len(),
                    start_array.len()
                );
            }

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return exec_err!(
                    "flags_array must be the same length as values array; got {} and {}",
                    values.len(),
                    flags_array.len()
                );
            }

            let mut regex_cache = HashMap::new();
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
                .collect::<Result<Vec<i64>>>()?,
            )))
        }
    }
}

fn compile_and_cache_regex(
    regex: &str,
    flags: Option<&str>,
    regex_cache: &mut HashMap<String, Regex>,
) -> Result<Regex> {
    match regex_cache.entry(regex.to_string()) {
        Entry::Vacant(entry) => {
            let compiled = compile_regex(regex, flags)?;
            entry.insert(compiled.clone());
            Ok(compiled)
        }
        Entry::Occupied(entry) => Ok(entry.get().to_owned()),
    }
}

fn compile_regex(regex: &str, flags: Option<&str>) -> Result<Regex> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") {
                return Err(ArrowError::ComputeError(
                    "regexp_count() does not support global flag".to_string(),
                )
                .into());
            }
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
        .into()
    })
}

fn count_matches(
    value: Option<&str>,
    pattern: &Regex,
    start: Option<i64>,
) -> Result<i64> {
    let value = match value {
        None | Some("") => return Ok(0),
        Some(value) => value,
    };

    if let Some(start) = start {
        if start < 1 {
            return Err(ArrowError::ComputeError(
                "regexp_count() requires start to be 1 based".to_string(),
            )
            .into());
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
    use arrow::array::GenericStringArray;

    #[test]
    fn test_regexp_count() {
        test_case_sensitive_regexp_count_scalar::<i32>();
        test_case_sensitive_regexp_count_scalar::<i64>();

        test_case_sensitive_regexp_count_scalar_start::<i32>();
        test_case_sensitive_regexp_count_scalar_start::<i64>();

        test_case_insensitive_regexp_count_scalar_flags::<i32>();
        test_case_insensitive_regexp_count_scalar_flags::<i64>();

        test_case_sensitive_regexp_count_array::<i32>();
        test_case_sensitive_regexp_count_array::<i64>();

        test_case_sensitive_regexp_count_array_start::<i32>();
        test_case_sensitive_regexp_count_array_start::<i64>();

        test_case_insensitive_regexp_count_array_flags::<i32>();
        test_case_insensitive_regexp_count_array_flags::<i64>();

        test_case_sensitive_regexp_count_start_scalar_complex::<i32>();
        test_case_sensitive_regexp_count_start_scalar_complex::<i64>();

        test_case_sensitive_regexp_count_array_complex::<i32>();
        test_case_sensitive_regexp_count_array_complex::<i64>();
    }

    fn test_case_sensitive_regexp_count_scalar<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);

        let expected = Int64Array::from(vec![0, 1, 2, 1, 3]);

        let re = regexp_count::<O>(&[Arc::new(values), Arc::new(regex)]).unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_scalar_start<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);
        let start = Int64Array::from(vec![2]);

        let expected = Int64Array::from(vec![0, 1, 1, 0, 2]);

        let re = regexp_count::<O>(&[Arc::new(values), Arc::new(regex), Arc::new(start)])
            .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_insensitive_regexp_count_scalar_flags<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcabc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["abc"; 1]);
        let start = Int64Array::from(vec![1]);
        let flags = GenericStringArray::<O>::from(vec!["i"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 3]);

        let re = regexp_count::<O>(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aabca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 2]);

        let re = regexp_count::<O>(&[Arc::new(values), Arc::new(regex)]).unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array_start<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let expected = Int64Array::from(vec![0, 0, 1, 1, 0]);

        let re = regexp_count::<O>(&[Arc::new(values), Arc::new(regex), Arc::new(start)])
            .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_insensitive_regexp_count_array_flags<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 1, 2, 2, 3]);

        let re = regexp_count::<O>(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_start_scalar_complex<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcabc",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![5]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 0, 0, 2, 1]);

        let re = regexp_count::<O>(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }

    fn test_case_sensitive_regexp_count_array_complex<O: OffsetSizeTrait>() {
        let values = GenericStringArray::<O>::from(vec![
            "",
            "aAbca",
            "abcabc",
            "abcAbcab",
            "abcabcAbc",
        ]);
        let regex = GenericStringArray::<O>::from(vec!["", "abc", "a", "bc", "ab"]);
        let start = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let flags = GenericStringArray::<O>::from(vec!["", "i", "", "", "i"]);

        let expected = Int64Array::from(vec![0, 1, 1, 1, 1]);

        let re = regexp_count::<O>(&[
            Arc::new(values),
            Arc::new(regex),
            Arc::new(start),
            Arc::new(flags),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
    }
}
