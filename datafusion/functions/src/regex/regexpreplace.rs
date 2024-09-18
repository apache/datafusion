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

//! Regx expressions
use arrow::array::ArrayDataBuilder;
use arrow::array::BufferBuilder;
use arrow::array::GenericStringArray;
use arrow::array::StringViewBuilder;
use arrow::array::{new_null_array, ArrayIter, AsArray};
use arrow::array::{Array, ArrayRef, OffsetSizeTrait};
use arrow::array::{ArrayAccessor, StringViewArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::as_string_view_array;
use datafusion_common::exec_err;
use datafusion_common::plan_err;
use datafusion_common::ScalarValue;
use datafusion_common::{
    cast::as_generic_string_array, internal_err, DataFusionError, Result,
};
use datafusion_expr::function::Hint;
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use regex::Regex;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
#[derive(Debug)]
pub struct RegexpReplaceFunc {
    signature: Signature,
}
impl Default for RegexpReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpReplaceFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8, Utf8]),
                    Exact(vec![Utf8View, Utf8, Utf8]),
                    Exact(vec![Utf8, Utf8, Utf8, Utf8]),
                    Exact(vec![Utf8View, Utf8, Utf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpReplaceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => LargeUtf8,
            Utf8 | Binary => Utf8,
            Utf8View | BinaryView => Utf8View,
            Null => Null,
            Dictionary(_, t) => match **t {
                LargeUtf8 | LargeBinary => LargeUtf8,
                Utf8 | Binary => Utf8,
                Null => Null,
                _ => {
                    return plan_err!(
                        "the regexp_replace can only accept strings but got {:?}",
                        **t
                    );
                }
            },
            other => {
                return plan_err!(
                    "The regexp_replace function can only accept strings. Got {other}"
                );
            }
        })
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let result = regexp_replace_func(args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

fn regexp_replace_func(args: &[ColumnarValue]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => specialize_regexp_replace::<i32>(args),
        DataType::LargeUtf8 => specialize_regexp_replace::<i64>(args),
        DataType::Utf8View => specialize_regexp_replace::<i32>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_replace")
        }
    }
}

/// replace POSIX capture groups (like \1) with Rust Regex group (like ${1})
/// used by regexp_replace
fn regex_replace_posix_groups(replacement: &str) -> String {
    fn capture_groups_re() -> &'static Regex {
        static CAPTURE_GROUPS_RE_LOCK: OnceLock<Regex> = OnceLock::new();
        CAPTURE_GROUPS_RE_LOCK.get_or_init(|| Regex::new(r"(\\)(\d*)").unwrap())
    }
    capture_groups_re()
        .replace_all(replacement, "$${$2}")
        .into_owned()
}

/// Replaces substring(s) matching a PCRE-like regular expression.
///
/// The full list of supported features and syntax can be found at
/// <https://docs.rs/regex/latest/regex/#syntax>
///
/// Supported flags with the addition of 'g' can be found at
/// <https://docs.rs/regex/latest/regex/#grouping-and-flags>
///
/// # Examples
///
/// ```ignore
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/data/regex.csv", CsvReadOptions::new()).await?;
///
/// // use the regexp_replace function to replace substring(s) without flags
/// let df = df.with_column(
///     "a",
///     regexp_replace(vec![col("values"), col("patterns"), col("replacement")])
/// )?;
/// // use the regexp_replace function to replace substring(s) with flags
/// let df = df.with_column(
///     "b",
///     regexp_replace(vec![col("values"), col("patterns"), col("replacement"), col("flags")]),
/// )?;
///
/// // literals can be used as well
/// let df = df.with_column(
///     "c",
///     regexp_replace(vec![lit("foobarbequebaz"), lit("(bar)(beque)"), lit(r"\2")]),
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn regexp_replace<'a, T: OffsetSizeTrait, V, B>(
    string_array: V,
    pattern_array: B,
    replacement_array: B,
    flags: Option<&ArrayRef>,
) -> Result<ArrayRef>
where
    V: ArrayAccessor<Item = &'a str>,
    B: ArrayAccessor<Item = &'a str>,
{
    // Default implementation for regexp_replace, assumes all args are arrays
    // and args is a sequence of 3 or 4 elements.

    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    let datatype = string_array.data_type().to_owned();

    let string_array_iter = ArrayIter::new(string_array);
    let pattern_array_iter = ArrayIter::new(pattern_array);
    let replacement_array_iter = ArrayIter::new(replacement_array);

    match flags {
        None => {
            let result_iter = string_array_iter
                .zip(pattern_array_iter)
                .zip(replacement_array_iter)
                .map(|((string, pattern), replacement)| {
                    match (string, pattern, replacement) {
                        (Some(string), Some(pattern), Some(replacement)) => {
                            let replacement = regex_replace_posix_groups(replacement);
                            // if patterns hashmap already has regexp then use else create and return
                            let re = match patterns.get(pattern) {
                                Some(re) => Ok(re),
                                None => match Regex::new(pattern) {
                                    Ok(re) => {
                                        patterns.insert(pattern.to_string(), re);
                                        Ok(patterns.get(pattern).unwrap())
                                    }
                                    Err(err) => {
                                        Err(DataFusionError::External(Box::new(err)))
                                    }
                                },
                            };

                            Some(re.map(|re| re.replace(string, replacement.as_str())))
                                .transpose()
                        }
                        _ => Ok(None),
                    }
                });

            match datatype {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let result =
                        result_iter.collect::<Result<GenericStringArray<T>>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                DataType::Utf8View => {
                    let result = result_iter.collect::<Result<StringViewArray>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
        Some(flags) => {
            let flags_array = as_generic_string_array::<T>(flags)?;

            let result_iter = string_array_iter
                .zip(pattern_array_iter)
                .zip(replacement_array_iter)
                .zip(flags_array.iter())
                .map(|(((string, pattern), replacement), flags)| {
                    match (string, pattern, replacement, flags) {
                        (Some(string), Some(pattern), Some(replacement), Some(flags)) => {
                            let replacement = regex_replace_posix_groups(replacement);

                            // format flags into rust pattern
                            let (pattern, replace_all) = if flags == "g" {
                                (pattern.to_string(), true)
                            } else if flags.contains('g') {
                                (
                                    format!(
                                        "(?{}){}",
                                        flags.to_string().replace('g', ""),
                                        pattern
                                    ),
                                    true,
                                )
                            } else {
                                (format!("(?{flags}){pattern}"), false)
                            };

                            // if patterns hashmap already has regexp then use else create and return
                            let re = match patterns.get(&pattern) {
                                Some(re) => Ok(re),
                                None => match Regex::new(pattern.as_str()) {
                                    Ok(re) => {
                                        patterns.insert(pattern.clone(), re);
                                        Ok(patterns.get(&pattern).unwrap())
                                    }
                                    Err(err) => {
                                        Err(DataFusionError::External(Box::new(err)))
                                    }
                                },
                            };

                            Some(re.map(|re| {
                                if replace_all {
                                    re.replace_all(string, replacement.as_str())
                                } else {
                                    re.replace(string, replacement.as_str())
                                }
                            }))
                            .transpose()
                        }
                        _ => Ok(None),
                    }
                });

            match datatype {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let result =
                        result_iter.collect::<Result<GenericStringArray<T>>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                DataType::Utf8View => {
                    let result = result_iter.collect::<Result<StringViewArray>>()?;
                    Ok(Arc::new(result) as ArrayRef)
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
    }
}

fn _regexp_replace_early_abort<T: ArrayAccessor>(
    input_array: T,
    sz: usize,
) -> Result<ArrayRef> {
    // Mimicking the existing behavior of regexp_replace, if any of the scalar arguments
    // are actually null, then the result will be an array of the same size as the first argument with all nulls.
    //
    // Also acts like an early abort mechanism when the input array is empty.
    Ok(new_null_array(input_array.data_type(), sz))
}

/// Get the first argument from the given string array.
///
/// Note: If the array is empty or the first argument is null,
/// then calls the given early abort function.
macro_rules! fetch_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident, $EARLY_ABORT:ident, $ARRAY_SIZE:expr) => {{
        let array = as_generic_string_array::<$T>($ARG)?;
        if array.len() == 0 || array.is_null(0) {
            return $EARLY_ABORT(array, $ARRAY_SIZE);
        } else {
            array.value(0)
        }
    }};
}
/// Special cased regex_replace implementation for the scenario where
/// the pattern, replacement and flags are static (arrays that are derived
/// from scalars). This means we can skip regex caching system and basically
/// hold a single Regex object for the replace operation. This also speeds
/// up the pre-processing time of the replacement string, since it only
/// needs to processed once.
fn _regexp_replace_static_pattern_replace<T: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let array_size = args[0].len();
    let pattern = fetch_string_arg!(
        &args[1],
        "pattern",
        i32,
        _regexp_replace_early_abort,
        array_size
    );
    let replacement = fetch_string_arg!(
        &args[2],
        "replacement",
        i32,
        _regexp_replace_early_abort,
        array_size
    );
    let flags = match args.len() {
        3 => None,
        4 => Some(fetch_string_arg!(&args[3], "flags", i32, _regexp_replace_early_abort, array_size)),
        other => {
            return exec_err!(
                "regexp_replace was called with {other} arguments. It requires at least 3 and at most 4."
            )
        }
    };

    // Embed the flag (if it exists) into the pattern. Limit will determine
    // whether this is a global match (as in replace all) or just a single
    // replace operation.
    let (pattern, limit) = match flags {
        Some("g") => (pattern.to_string(), 0),
        Some(flags) => (
            format!("(?{}){}", flags.to_string().replace('g', ""), pattern),
            !flags.contains('g') as usize,
        ),
        None => (pattern.to_string(), 1),
    };

    let re =
        Regex::new(&pattern).map_err(|err| DataFusionError::External(Box::new(err)))?;

    // Replaces the posix groups in the replacement string
    // with rust ones.
    let replacement = regex_replace_posix_groups(replacement);

    let string_array_type = args[0].data_type();
    match string_array_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;

            // We are going to create the underlying string buffer from its parts
            // to be able to re-use the existing null buffer for sparse arrays.
            let mut vals = BufferBuilder::<u8>::new({
                let offsets = string_array.value_offsets();
                (offsets[string_array.len()] - offsets[0])
                    .to_usize()
                    .unwrap()
            });
            let mut new_offsets = BufferBuilder::<T>::new(string_array.len() + 1);
            new_offsets.append(T::zero());

            string_array.iter().for_each(|val| {
                if let Some(val) = val {
                    let result = re.replacen(val, limit, replacement.as_str());
                    vals.append_slice(result.as_bytes());
                }
                new_offsets.append(T::from_usize(vals.len()).unwrap());
            });

            let data = ArrayDataBuilder::new(GenericStringArray::<T>::DATA_TYPE)
                .len(string_array.len())
                .nulls(string_array.nulls().cloned())
                .buffers(vec![new_offsets.finish(), vals.finish()])
                .build()?;
            let result_array = GenericStringArray::<T>::from(data);
            Ok(Arc::new(result_array) as ArrayRef)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;

            let mut builder = StringViewBuilder::with_capacity(string_view_array.len());

            for val in string_view_array.iter() {
                if let Some(val) = val {
                    let result = re.replacen(val, limit, replacement.as_str());
                    builder.append_value(result);
                } else {
                    builder.append_null();
                }
            }

            let result = builder.finish();
            Ok(Arc::new(result) as ArrayRef)
        }
        _ => unreachable!(
            "Invalid data type for regexp_replace: {}",
            string_array_type
        ),
    }
}

/// Determine which implementation of the regexp_replace to use based
/// on the given set of arguments.
pub fn specialize_regexp_replace<T: OffsetSizeTrait>(
    args: &[ColumnarValue],
) -> Result<ArrayRef> {
    // This will serve as a dispatch table where we can
    // leverage it in order to determine whether the scalarity
    // of the given set of arguments fits a better specialized
    // function.
    let (is_source_scalar, is_pattern_scalar, is_replacement_scalar, is_flags_scalar) = (
        matches!(args[0], ColumnarValue::Scalar(_)),
        matches!(args[1], ColumnarValue::Scalar(_)),
        matches!(args[2], ColumnarValue::Scalar(_)),
        // The forth argument (flags) is optional; so in the event that
        // it is not available, we'll claim that it is scalar.
        matches!(args.get(3), Some(ColumnarValue::Scalar(_)) | None),
    );
    let len = args
        .iter()
        .fold(Option::<usize>::None, |acc, arg| match arg {
            ColumnarValue::Scalar(_) => acc,
            ColumnarValue::Array(a) => Some(a.len()),
        });
    let inferred_length = len.unwrap_or(1);
    match (
        is_source_scalar,
        is_pattern_scalar,
        is_replacement_scalar,
        is_flags_scalar,
    ) {
        // This represents a very hot path for the case where the there is
        // a single pattern that is being matched against and a single replacement.
        // This is extremely important to specialize on since it removes the overhead
        // of DF's in-house regex pattern cache (since there will be at most a single
        // pattern) and the pre-processing of the same replacement pattern at each
        // query.
        //
        // The flags needs to be a scalar as well since each pattern is actually
        // constructed with the flags embedded into the pattern itself. This means
        // even if the pattern itself is scalar, if the flags are an array then
        // we will create many regexes and it is best to use the implementation
        // that caches it. If there are no flags, we can simply ignore it here,
        // and let the specialized function handle it.
        (_, true, true, true) => {
            let hints = [
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
            ];
            let args = args
                .iter()
                .zip(hints.iter().chain(std::iter::repeat(&Hint::Pad)))
                .map(|(arg, hint)| {
                    // Decide on the length to expand this scalar to depending
                    // on the given hints.
                    let expansion_len = match hint {
                        Hint::AcceptsSingular => 1,
                        Hint::Pad => inferred_length,
                    };
                    arg.clone().into_array(expansion_len)
                })
                .collect::<Result<Vec<_>>>()?;
            _regexp_replace_static_pattern_replace::<T>(&args)
        }

        // If there are no specialized implementations, we'll fall back to the
        // generic implementation.
        (_, _, _, _) => {
            let args = args
                .iter()
                .map(|arg| arg.clone().into_array(inferred_length))
                .collect::<Result<Vec<_>>>()?;

            match args[0].data_type() {
                DataType::Utf8View => {
                    let string_array = args[0].as_string_view();
                    let pattern_array = args[1].as_string::<i32>();
                    let replacement_array = args[2].as_string::<i32>();
                    regexp_replace::<i32, _, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        args.get(3),
                    )
                }
                DataType::Utf8 => {
                    let string_array = args[0].as_string::<i32>();
                    let pattern_array = args[1].as_string::<i32>();
                    let replacement_array = args[2].as_string::<i32>();
                    regexp_replace::<i32, _, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        args.get(3),
                    )
                }
                DataType::LargeUtf8 => {
                    let string_array = args[0].as_string::<i64>();
                    let pattern_array = args[1].as_string::<i64>();
                    let replacement_array = args[2].as_string::<i64>();
                    regexp_replace::<i64, _, _>(
                        string_array,
                        pattern_array,
                        replacement_array,
                        args.get(3),
                    )
                }
                other => {
                    exec_err!(
                        "Unsupported data type {other:?} for function regex_replace"
                    )
                }
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use arrow::array::*;

    use super::*;

    macro_rules! static_pattern_regexp_replace {
        ($name:ident, $T:ty, $O:ty) => {
            #[test]
            fn $name() {
                let values = vec!["abc", "acd", "abcd1234567890123", "123456789012abc"];
                let patterns = vec!["b"; 4];
                let replacement = vec!["foo"; 4];
                let expected =
                    vec!["afooc", "acd", "afoocd1234567890123", "123456789012afooc"];

                let values = <$T>::from(values);
                let patterns = StringArray::from(patterns);
                let replacements = StringArray::from(replacement);
                let expected = <$T>::from(expected);

                let re = _regexp_replace_static_pattern_replace::<$O>(&[
                    Arc::new(values),
                    Arc::new(patterns),
                    Arc::new(replacements),
                ])
                .unwrap();

                assert_eq!(re.as_ref(), &expected);
            }
        };
    }

    static_pattern_regexp_replace!(string_array, StringArray, i32);
    static_pattern_regexp_replace!(string_view_array, StringViewArray, i32);
    static_pattern_regexp_replace!(large_string_array, LargeStringArray, i64);

    macro_rules! static_pattern_regexp_replace_with_flags {
        ($name:ident, $T:ty, $O: ty) => {
            #[test]
            fn $name() {
                let values = vec![
                    "abc",
                    "aBc",
                    "acd",
                    "abcd1234567890123",
                    "aBcd1234567890123",
                    "123456789012abc",
                    "123456789012aBc",
                ];
                let expected = vec![
                    "afooc",
                    "afooc",
                    "acd",
                    "afoocd1234567890123",
                    "afoocd1234567890123",
                    "123456789012afooc",
                    "123456789012afooc",
                ];

                let values = <$T>::from(values);
                let patterns = StringArray::from(vec!["b"; 7]);
                let replacements = StringArray::from(vec!["foo"; 7]);
                let flags = StringArray::from(vec!["i"; 5]);
                let expected = <$T>::from(expected);

                let re = _regexp_replace_static_pattern_replace::<$O>(&[
                    Arc::new(values),
                    Arc::new(patterns),
                    Arc::new(replacements),
                    Arc::new(flags),
                ])
                .unwrap();

                assert_eq!(re.as_ref(), &expected);
            }
        };
    }

    static_pattern_regexp_replace_with_flags!(string_array_with_flags, StringArray, i32);
    static_pattern_regexp_replace_with_flags!(
        string_view_array_with_flags,
        StringViewArray,
        i32
    );
    static_pattern_regexp_replace_with_flags!(
        large_string_array_with_flags,
        LargeStringArray,
        i64
    );

    #[test]
    fn test_static_pattern_regexp_replace_early_abort() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec![None::<&str>; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let expected = StringArray::from(vec![None::<&str>; 5]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_early_abort_when_empty() {
        let values = StringArray::from(Vec::<Option<&str>>::new());
        let patterns = StringArray::from(Vec::<Option<&str>>::new());
        let replacements = StringArray::from(Vec::<Option<&str>>::new());
        let expected = StringArray::from(Vec::<Option<&str>>::new());

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_early_abort_flags() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec!["a"; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let flags = StringArray::from(vec![None::<&str>; 5]);
        let expected = StringArray::from(vec![None::<&str>; 5]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
            Arc::new(flags),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_pattern_error() {
        let values = StringArray::from(vec!["abc"; 5]);
        // Deliberately using an invalid pattern to see how the single pattern
        // error is propagated on regexp_replace.
        let patterns = StringArray::from(vec!["["; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ]);
        let pattern_err = re.expect_err("broken pattern should have failed");
        assert_eq!(
            pattern_err.strip_backtrace(),
            "External error: regex parse error:\n    [\n    ^\nerror: unclosed character class"
        );
    }

    #[test]
    fn test_static_pattern_regexp_replace_with_null_buffers() {
        let values = StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            None,
            Some("a"),
            None,
            None,
            Some("c"),
        ]);
        let patterns = StringArray::from(vec!["a"; 1]);
        let replacements = StringArray::from(vec!["foo"; 1]);
        let expected = StringArray::from(vec![
            Some("foo"),
            None,
            Some("b"),
            None,
            Some("foo"),
            None,
            None,
            Some("c"),
        ]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
        assert_eq!(re.null_count(), 4);
    }

    #[test]
    fn test_static_pattern_regexp_replace_with_sliced_null_buffer() {
        let values = StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            None,
            Some("a"),
            None,
            None,
            Some("c"),
        ]);
        let values = values.slice(2, 5);
        let patterns = StringArray::from(vec!["a"; 1]);
        let replacements = StringArray::from(vec!["foo"; 1]);
        let expected = StringArray::from(vec![Some("b"), None, Some("foo"), None, None]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();
        assert_eq!(re.as_ref(), &expected);
        assert_eq!(re.null_count(), 3);
    }
}
