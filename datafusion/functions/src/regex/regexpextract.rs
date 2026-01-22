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
use arrow::array::new_null_array;
use arrow::array::{
    Array, ArrayRef, AsArray, Datum, Int64Array, LargeStringArray, StringArray,
    StringArrayType, StringViewArray,
};
use arrow::datatypes::DataType::{Int64, LargeUtf8, Utf8, Utf8View};
use arrow::datatypes::{DataType, Int64Type};
use arrow::error::ArrowError;
use datafusion_common::{Result, ScalarValue};
use datafusion_common::{exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use itertools::izip;
use std::sync::Arc;

#[datafusion_macros::user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns specific group matched by [regular expression](https://docs.rs/regex/latest/regex/#syntax).",
    syntax_example = "regexp_extract(str, regexp, idx[, flags])",
    sql_example = r#"```sql
            > select regexp_extract('100-200', '(\\d+)-(\\d+)', 2);
            +--------------------------------------------------------------+
            | regexp_extract(Utf8("100-200"),Utf8("(\d+)-(\d+)"),Int64(2)) |
            +--------------------------------------------------------------+
            | 200                                                          |
            +--------------------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/regexp.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to match against. Can be a constant, column, or function."
    ),
    argument(
        name = "idx",
        description = "Matched group id. Can be a constant, column, or function."
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
                    // Planner attempts coercion to the target type starting with the most preferred candidate.
                    // For example, given input `(Utf8View, Utf8)`, it first tries coercing to `(Utf8View, Utf8View)`.
                    // If that fails, it proceeds to `(Utf8, Utf8)`.
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64]),
                    TypeSignature::Exact(vec![Utf8View, Utf8View, Int64, Utf8View]),
                    TypeSignature::Exact(vec![Utf8, Utf8, Int64, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, LargeUtf8, Int64, LargeUtf8]),
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
    if args_len != 3 && args_len != 4 {
        return exec_err!(
            "regexp_extract was called with {args_len} arguments. It requires 3 or 4."
        );
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

fn regexp_extract(
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
        (Utf8, Utf8, None) =>
            regexp_extract_inner::<_, StringArray>(
                values.as_string::<i32>(),
                regex_array.as_string::<i32>(),
                is_regex_scalar,
                idx_array.as_primitive::<Int64Type>(),
                is_idx_scalar,
                None,
                is_flags_scalar
            ),
        (Utf8, Utf8, Some(flags_array)) if *flags_array.data_type() == Utf8 =>
            regexp_extract_inner::<_, StringArray>(
                values.as_string::<i32>(),
                regex_array.as_string::<i32>(),
                is_regex_scalar,
                idx_array.as_primitive::<Int64Type>(),
                is_idx_scalar,
                Some(flags_array.as_string::<i32>()),
                is_flags_scalar,
            ),
        (LargeUtf8, LargeUtf8, None) =>
            regexp_extract_inner::<_, LargeStringArray>(
                values.as_string::<i64>(),
                regex_array.as_string::<i64>(),
                is_regex_scalar,
                idx_array.as_primitive::<Int64Type>(),
                is_idx_scalar,
                None,
                is_flags_scalar
            ),
        (LargeUtf8, LargeUtf8, Some(flags_array)) if *flags_array.data_type() == LargeUtf8 =>
            regexp_extract_inner::<_, LargeStringArray>(
                values.as_string::<i64>(),
                regex_array.as_string::<i64>(),
                is_regex_scalar,
                idx_array.as_primitive::<Int64Type>(),
                is_idx_scalar,
                Some(flags_array.as_string::<i64>()),
                is_flags_scalar,
            ),
        (Utf8View, Utf8View, None) =>
            regexp_extract_inner::<_, StringViewArray>(
                values.as_string_view(),
                regex_array.as_string_view(),
                is_regex_scalar,
                idx_array.as_primitive::<Int64Type>(),
                is_idx_scalar,
                None,
                is_flags_scalar
            ),
        (Utf8View, Utf8View, Some(flags_array)) if *flags_array.data_type() == Utf8View =>
            regexp_extract_inner::<_, StringViewArray>(
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

fn regexp_extract_inner<'a, S, O>(
    values: S,
    regex_array: S,
    is_regex_scalar: bool,
    idx_array: &Int64Array,
    is_idx_scalar: bool,
    flags_array: Option<S>,
    is_flags_scalar: bool,
) -> Result<ArrayRef, ArrowError>
where
    S: StringArrayType<'a> + Copy + 'a,
    O: FromIterator<Option<&'a str>> + Array + 'static,
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

    let mut regex_cache = std::collections::HashMap::new();

    match (is_regex_scalar, is_idx_scalar, is_flags_scalar) {
        (true, true, true) => {
            let Some(regex) = regex_scalar.filter(|s| !s.is_empty()) else {
                return Ok(new_null_array(values.data_type(), values.len()));
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            Ok(Arc::new(
                values
                    .iter()
                    .map(|value| extract_matches(value, &pattern, idx_scalar))
                    .collect::<Result<O, ArrowError>>()?,
            ))
        }
        (true, true, false) => {
            let Some(regex) = regex_scalar.filter(|s| !s.is_empty()) else {
                return Ok(new_null_array(values.data_type(), values.len()));
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(
                values
                    .iter()
                    .zip(flags_array.iter())
                    .map(|(value, flags)| {
                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                        extract_matches(value, pattern, idx_scalar)
                    })
                    .collect::<Result<O, ArrowError>>()?,
            ))
        }
        (true, false, true) => {
            let Some(regex) = regex_scalar.filter(|s| !s.is_empty()) else {
                return Ok(new_null_array(values.data_type(), values.len()));
            };

            let pattern = compile_regex(regex, flags_scalar)?;

            Ok(Arc::new(
                values
                    .iter()
                    .zip(idx_array.iter())
                    .map(|(value, idx)| extract_matches(value, &pattern, idx))
                    .collect::<Result<O, ArrowError>>()?,
            ))
        }
        (true, false, false) => {
            let Some(regex) = regex_scalar.filter(|s| !s.is_empty()) else {
                return Ok(new_null_array(values.data_type(), values.len()));
            };

            let flags_array = flags_array.unwrap();
            if values.len() != flags_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "flags_array must be the same length as values array; got {} and {}",
                    flags_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(
                izip!(values.iter(), idx_array.iter(), flags_array.iter())
                    .map(|(value, idx, flags)| {
                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                        extract_matches(value, pattern, idx)
                    })
                    .collect::<Result<O, ArrowError>>()?,
            ))
        }
        (false, true, true) => {
            if values.len() != regex_array.len() {
                return Err(ArrowError::ComputeError(format!(
                    "regex_array must be the same length as values array; got {} and {}",
                    regex_array.len(),
                    values.len(),
                )));
            }

            Ok(Arc::new(
                values
                    .iter()
                    .zip(regex_array.iter())
                    .map(|(value, regex)| {
                        let regex = match regex {
                            None | Some("") => return Ok(None),
                            Some(regex) => regex,
                        };

                        let pattern = compile_and_cache_regex(
                            regex,
                            flags_scalar,
                            &mut regex_cache,
                        )?;
                        extract_matches(value, pattern, idx_scalar)
                    })
                    .collect::<Result<O, ArrowError>>()?,
            ))
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

            Ok(Arc::new(
                izip!(values.iter(), regex_array.iter(), flags_array.iter())
                    .map(|(value, regex, flags)| {
                        let regex = match regex {
                            None | Some("") => return Ok(None),
                            Some(regex) => regex,
                        };

                        let pattern =
                            compile_and_cache_regex(regex, flags, &mut regex_cache)?;

                        extract_matches(value, pattern, idx_scalar)
                    })
                    .collect::<Result<O, ArrowError>>()?,
            ))
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

            Ok(Arc::new(
                izip!(values.iter(), regex_array.iter(), idx_array.iter())
                    .map(|(value, regex, idx)| {
                        let regex = match regex {
                            None | Some("") => return Ok(None),
                            Some(regex) => regex,
                        };

                        let pattern = compile_and_cache_regex(
                            regex,
                            flags_scalar,
                            &mut regex_cache,
                        )?;
                        extract_matches(value, pattern, idx)
                    })
                    .collect::<Result<O, ArrowError>>()?,
            ))
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

            Ok(Arc::new(
                izip!(
                    values.iter(),
                    regex_array.iter(),
                    idx_array.iter(),
                    flags_array.iter()
                )
                .map(|(value, regex, idx, flags)| {
                    let regex = match regex {
                        None | Some("") => return Ok(None),
                        Some(regex) => regex,
                    };

                    let pattern =
                        compile_and_cache_regex(regex, flags, &mut regex_cache)?;
                    extract_matches(value, pattern, idx)
                })
                .collect::<Result<O, ArrowError>>()?,
            ))
        }
    }
}

fn extract_matches<'a>(
    value: Option<&'a str>,
    pattern: &regex::Regex,
    idx: Option<i64>,
) -> Result<Option<&'a str>, ArrowError> {
    let value = match value {
        None => return Ok(None),
        Some(value) => value,
    };

    let idx = idx.unwrap_or(0);
    if idx < 0 {
        return Err(ArrowError::ComputeError(
            "regexp_extract() requires idx to be non-negative".to_string(),
        ));
    }

    if let Some(caps) = pattern.captures(value) {
        Ok(caps.get(idx as usize).map(|m| m.as_str()))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regexp_extract_simple_match() {
        let values = StringArray::from(vec!["100-200", "300-400"]);
        let regex = StringArray::from(vec![r"(\d+)-(\d+)", r"(\d+)-(\d+)"]);
        let idx = Int64Array::from(vec![1, 2]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_string::<i32>();

        assert_eq!(result.value(0), "100");
        assert_eq!(result.value(1), "400");
    }

    #[test]
    fn test_regexp_extract_scalar_regex_and_index() {
        let values = StringArray::from(vec![Some("100-200"), Some("300-400"), None]);
        let regex = StringArray::from(vec![r"(\d+)-(\d+)"]);
        let idx = Int64Array::from(vec![2]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_string::<i32>();

        assert_eq!(result.value(0), "200");
        assert_eq!(result.value(1), "400");
        assert!(result.is_null(2));
    }

    #[test]
    fn test_regexp_extract_no_match() {
        let values = StringArray::from(vec!["apple", "banana"]);
        let regex = StringArray::from(vec![r"\d+"]); // Look for digits
        let idx = Int64Array::from(vec![0]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        // Should return NULL for no match
        assert!(result.is_null(0));
        assert!(result.is_null(1));
    }

    #[test]
    fn test_regexp_extract_empty_string_behavior() {
        // Ensure we distinguish between NULL input and Empty String input
        let values = StringArray::from(vec![Some(""), None]);
        let regex = StringArray::from(vec![r"^$"]); // Matches empty string
        let idx = Int64Array::from(vec![0]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_string::<i32>();

        // Empty string input matches empty regex -> returns empty string
        assert_eq!(result.value(0), "");
        // Null input -> returns Null
        assert!(result.is_null(1));
    }

    #[test]
    fn test_regexp_extract_flags() {
        let values = StringArray::from(vec!["Foo", "bar"]);
        let regex = StringArray::from(vec!["foo"]);
        let idx = Int64Array::from(vec![0]);
        let flags = StringArray::from(vec!["i"]); // Case insensitive

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
            Arc::new(flags) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_string::<i32>();

        assert_eq!(result.value(0), "Foo"); // Matches because of 'i' flag
        assert!(result.is_null(1)); // 'bar' does not match 'foo' even with 'i'
    }

    #[test]
    fn test_regexp_extract_large_utf8() {
        let values = LargeStringArray::from(vec!["100-200"]);
        let regex = LargeStringArray::from(vec![r"(\d+)-(\d+)"]);
        let idx = Int64Array::from(vec![1]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_any().downcast_ref::<LargeStringArray>().unwrap();

        assert_eq!(result.value(0), "100");
    }

    #[test]
    fn test_regexp_extract_utf8view() {
        let values = StringViewArray::from(vec!["100-200"]);
        let regex = StringViewArray::from(vec![r"(\d+)-(\d+)"]);
        let idx = Int64Array::from(vec![1]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args).unwrap();
        let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();

        assert_eq!(result.value(0), "100");
    }

    #[test]
    fn test_error_negative_index() {
        let values = StringArray::from(vec!["abc"]);
        let regex = StringArray::from(vec![r"(a)"]);
        let idx = Int64Array::from(vec![-1]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-negative"));
    }

    #[test]
    fn test_error_array_length_mismatch() {
        let values = StringArray::from(vec!["a", "b"]);
        let regex = StringArray::from(vec![r"(a)", r"(b)", r"(c)"]); // Length 3 vs 2
        let idx = Int64Array::from(vec![0]);

        let args = vec![
            Arc::new(values) as ArrayRef,
            Arc::new(regex) as ArrayRef,
            Arc::new(idx) as ArrayRef,
        ];

        let result = regexp_extract_func(&args);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same length"));
    }
}
