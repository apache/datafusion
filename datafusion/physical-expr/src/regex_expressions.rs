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

// Some of these functions reference the Postgres documentation
// or implementation to ensure compatibility and are subject to
// the Postgres license.

//! Regex expressions

use arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, BufferBuilder, GenericStringArray,
    OffsetSizeTrait,
};
use arrow::compute;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};
use hashbrown::HashMap;
use lazy_static::lazy_static;
use regex::Regex;
use std::any::type_name;
use std::sync::Arc;

use crate::functions::{make_scalar_function, make_scalar_function_with_hints, Hint};

/// Get the first argument from the given string array.
///
/// Note: If the array is empty or the first argument is null,
/// then calls the given early abort function.
macro_rules! fetch_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident, $EARLY_ABORT:ident) => {{
        let array = downcast_string_array_arg!($ARG, $NAME, $T);
        if array.len() == 0 || array.is_null(0) {
            return $EARLY_ABORT(array);
        } else {
            array.value(0)
        }
    }};
}

macro_rules! downcast_string_array_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<GenericStringArray<T>>()
                ))
            })?
    }};
}

/// extract a specific group from a string column, using a regular expression
pub fn regexp_match<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let values = downcast_string_array_arg!(args[0], "string", T);
            let regex = downcast_string_array_arg!(args[1], "pattern", T);
            compute::regexp_match(values, regex, None).map_err(DataFusionError::ArrowError)
        }
        3 => {
            let values = downcast_string_array_arg!(args[0], "string", T);
            let regex = downcast_string_array_arg!(args[1], "pattern", T);
            let flags = Some(downcast_string_array_arg!(args[2], "flags", T));

            match flags {
                Some(f) if f.iter().any(|s| s == Some("g")) => {
                    Err(DataFusionError::Plan("regexp_match() does not support the \"global\" option".to_owned()))
                },
                _ => compute::regexp_match(values, regex, flags).map_err(DataFusionError::ArrowError),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "regexp_match was called with {} arguments. It requires at least 2 and at most 3.",
            other
        ))),
    }
}

/// replace POSIX capture groups (like \1) with Rust Regex group (like ${1})
/// used by regexp_replace
fn regex_replace_posix_groups(replacement: &str) -> String {
    lazy_static! {
        static ref CAPTURE_GROUPS_RE: Regex = Regex::new(r"(\\)(\d*)").unwrap();
    }
    CAPTURE_GROUPS_RE
        .replace_all(replacement, "$${$2}")
        .into_owned()
}

/// Replaces substring(s) matching a POSIX regular expression.
///
/// example: `regexp_replace('Thomas', '.[mN]a.', 'M') = 'ThM'`
pub fn regexp_replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Default implementation for regexp_replace, assumes all args are arrays
    // and args is a sequence of 3 or 4 elements.

    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    match args.len() {
        3 => {
            let string_array = downcast_string_array_arg!(args[0], "string", T);
            let pattern_array = downcast_string_array_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_array_arg!(args[2], "replacement", T);

            let result = string_array
            .iter()
            .zip(pattern_array.iter())
            .zip(replacement_array.iter())
            .map(|((string, pattern), replacement)| match (string, pattern, replacement) {
                (Some(string), Some(pattern), Some(replacement)) => {
                    let replacement = regex_replace_posix_groups(replacement);

                    // if patterns hashmap already has regexp then use else else create and return
                    let re = match patterns.get(pattern) {
                        Some(re) => Ok(re.clone()),
                        None => {
                            match Regex::new(pattern) {
                                Ok(re) => {
                                    patterns.insert(pattern.to_string(), re.clone());
                                    Ok(re)
                                },
                                Err(err) => Err(DataFusionError::Execution(err.to_string())),
                            }
                        }
                    };

                    Some(re.map(|re| re.replace(string, replacement.as_str()))).transpose()
                }
            _ => Ok(None)
            })
            .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        4 => {
            let string_array = downcast_string_array_arg!(args[0], "string", T);
            let pattern_array = downcast_string_array_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_array_arg!(args[2], "replacement", T);
            let flags_array = downcast_string_array_arg!(args[3], "flags", T);

            let result = string_array
            .iter()
            .zip(pattern_array.iter())
            .zip(replacement_array.iter())
            .zip(flags_array.iter())
            .map(|(((string, pattern), replacement), flags)| match (string, pattern, replacement, flags) {
                (Some(string), Some(pattern), Some(replacement), Some(flags)) => {
                    let replacement = regex_replace_posix_groups(replacement);

                    // format flags into rust pattern
                    let (pattern, replace_all) = if flags == "g" {
                        (pattern.to_string(), true)
                    } else if flags.contains('g') {
                        (format!("(?{}){}", flags.to_string().replace('g', ""), pattern), true)
                    } else {
                        (format!("(?{}){}", flags, pattern), false)
                    };

                    // if patterns hashmap already has regexp then use else else create and return
                    let re = match patterns.get(&pattern) {
                        Some(re) => Ok(re.clone()),
                        None => {
                            match Regex::new(pattern.as_str()) {
                                Ok(re) => {
                                    patterns.insert(pattern, re.clone());
                                    Ok(re)
                                },
                                Err(err) => Err(DataFusionError::Execution(err.to_string())),
                            }
                        }
                    };

                    Some(re.map(|re| {
                        if replace_all {
                            re.replace_all(string, replacement.as_str())
                        } else {
                            re.replace(string, replacement.as_str())
                        }
                    })).transpose()
                }
            _ => Ok(None)
            })
            .collect::<Result<GenericStringArray<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "regexp_replace was called with {} arguments. It requires at least 3 and at most 4.",
            other
        ))),
    }
}

fn _regexp_replace_early_abort<T: OffsetSizeTrait>(
    input_array: &GenericStringArray<T>,
) -> Result<ArrayRef> {
    // Mimicking the existing behavior of regexp_replace, if any of the scalar arguments
    // are actuall null, then the result will be an array of the same size but with nulls.
    //
    // Also acts like an early abort mechanism when the input array is empty.
    Ok(new_null_array(input_array.data_type(), input_array.len()))
}

/// Special cased regex_replace implementation for the scenerio where
/// the pattern, replacement and flags are static (arrays that are derived
/// from scalars). This means we can skip regex caching system and basically
/// hold a single Regex object for the replace operation. This also speeds
/// up the pre-processing time of the replacement string, since it only
/// needs to processed once.
fn _regexp_replace_static_pattern_replace<T: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let string_array = downcast_string_array_arg!(args[0], "string", T);
    let pattern = fetch_string_arg!(args[1], "pattern", T, _regexp_replace_early_abort);
    let replacement =
        fetch_string_arg!(args[2], "replacement", T, _regexp_replace_early_abort);
    let flags = match args.len() {
        3 => None,
        4 => Some(fetch_string_arg!(args[3], "flags", T, _regexp_replace_early_abort)),
        other => {
            return Err(DataFusionError::Internal(format!(
                "regexp_replace was called with {} arguments. It requires at least 3 and at most 4.",
                other
            )))
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

    let re = Regex::new(&pattern)
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

    // Replaces the posix groups in the replacement string
    // with rust ones.
    let replacement = regex_replace_posix_groups(replacement);

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

    let data = ArrayData::try_new(
        GenericStringArray::<T>::DATA_TYPE,
        string_array.len(),
        string_array
            .data_ref()
            .null_buffer()
            .map(|b| b.bit_slice(string_array.offset(), string_array.len())),
        0,
        vec![new_offsets.finish(), vals.finish()],
        vec![],
    )?;
    let result_array = GenericStringArray::<T>::from(data);
    Ok(Arc::new(result_array) as ArrayRef)
}

/// Determine which implementation of the regexp_replace to use based
/// on the given set of arguments.
pub fn specialize_regexp_replace<T: OffsetSizeTrait>(
    args: &[ColumnarValue],
) -> Result<ScalarFunctionImplementation> {
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
        (_, true, true, true) => Ok(make_scalar_function_with_hints(
            _regexp_replace_static_pattern_replace::<T>,
            vec![
                Hint::Pad,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
                Hint::AcceptsSingular,
            ],
        )),

        // If there are no specialized implementations, we'll fall back to the
        // generic implementation.
        (_, _, _, _) => Ok(make_scalar_function(regexp_replace::<T>)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_case_sensitive_regexp_match() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.append(false);
        let expected = expected_builder.finish();

        let re = regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns)]).unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_match() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);
        let flags = StringArray::from(vec!["i"; 5]);

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.append(false);
        let expected = expected_builder.finish();

        let re =
            regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_unsupported_global_flag_regexp_match() {
        let values = StringArray::from(vec!["abc"]);
        let patterns = StringArray::from(vec!["^(a)"]);
        let flags = StringArray::from(vec!["g"]);

        let re_err =
            regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .expect_err("unsupported flag should have failed");

        assert_eq!(re_err.to_string(), "Error during planning: regexp_match() does not support the \"global\" option");
    }

    #[test]
    fn test_static_pattern_regexp_replace() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec!["b"; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let expected = StringArray::from(vec!["afooc"; 5]);

        let re = _regexp_replace_static_pattern_replace::<i32>(&[
            Arc::new(values),
            Arc::new(patterns),
            Arc::new(replacements),
        ])
        .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_static_pattern_regexp_replace_with_flags() {
        let values = StringArray::from(vec!["abc", "ABC", "aBc", "AbC", "aBC"]);
        let patterns = StringArray::from(vec!["b"; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let flags = StringArray::from(vec!["i"; 5]);
        let expected =
            StringArray::from(vec!["afooc", "AfooC", "afooc", "AfooC", "afooC"]);

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
    fn test_static_pattern_regexp_replace_early_abort() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns = StringArray::from(vec![None; 5]);
        let replacements = StringArray::from(vec!["foo"; 5]);
        let expected = StringArray::from(vec![None; 5]);

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
        let flags = StringArray::from(vec![None; 5]);
        let expected = StringArray::from(vec![None; 5]);

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
        // Delibaretely using an invalid pattern to see how the single pattern
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
            pattern_err.to_string(),
            "Execution error: regex parse error:\n    [\n    ^\nerror: unclosed character class"
        );
    }

    #[test]
    fn test_regexp_can_specialize_all_cases() {
        macro_rules! make_scalar {
            () => {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("foo".to_string())))
            };
        }

        macro_rules! make_array {
            () => {
                ColumnarValue::Array(
                    Arc::new(StringArray::from(vec!["bar"; 2])) as ArrayRef
                )
            };
        }

        for source in [make_scalar!(), make_array!()] {
            for pattern in [make_scalar!(), make_array!()] {
                for replacement in [make_scalar!(), make_array!()] {
                    for flags in [Some(make_scalar!()), Some(make_array!()), None] {
                        let mut args =
                            vec![source.clone(), pattern.clone(), replacement.clone()];
                        if let Some(flags) = flags {
                            args.push(flags.clone());
                        }
                        let regex_func = specialize_regexp_replace::<i32>(&args);
                        assert!(regex_func.is_ok());
                    }
                }
            }
        }
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
