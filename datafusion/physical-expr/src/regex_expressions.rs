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

use std::sync::{Arc, OnceLock};

use arrow::array::{
    new_null_array, Array, ArrayDataBuilder, ArrayRef, BufferBuilder, GenericStringArray,
    OffsetSizeTrait,
};
use hashbrown::HashMap;
use regex::Regex;

use datafusion_common::{arrow_datafusion_err, exec_err, plan_err};
use datafusion_common::{cast::as_generic_string_array, DataFusionError, Result};
use datafusion_expr::{ColumnarValue, ScalarFunctionImplementation};

use crate::functions::{
    make_scalar_function_inner, make_scalar_function_with_hints, Hint,
};

/// Get the first argument from the given string array.
///
/// Note: If the array is empty or the first argument is null,
/// then calls the given early abort function.
macro_rules! fetch_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident, $EARLY_ABORT:ident) => {{
        let array = as_generic_string_array::<T>($ARG)?;
        if array.len() == 0 || array.is_null(0) {
            return $EARLY_ABORT(array);
        } else {
            array.value(0)
        }
    }};
}

/// Tests a string using a regular expression returning true if at
/// least one match, false otherwise.
///
/// The full list of supported features and syntax can be found at
/// <https://docs.rs/regex/latest/regex/#syntax>
///
/// Supported flags can be found at
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
/// // use the regexp_like function to test col 'values',
/// // against patterns in col 'patterns' without flags
/// let df = df.with_column(
///     "a",
///     regexp_like(vec![col("values"), col("patterns")])
/// )?;
/// // use the regexp_like function to test col 'values',
/// // against patterns in col 'patterns' with flags
/// let df = df.with_column(
///     "b",
///     regexp_like(vec![col("values"), col("patterns"), col("flags")])
/// )?;
/// // literals can be used as well with dataframe calls
/// let df = df.with_column(
///     "c",
///     regexp_like(vec![lit("foobarbequebaz"), lit("(bar)(beque)")])
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn regexp_like<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            let array = arrow_string::regexp::regexp_is_match_utf8(values, regex, None)
                .map_err(|e| arrow_datafusion_err!(e))?;

            Ok(Arc::new(array) as ArrayRef)
        }
        3 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            let flags = as_generic_string_array::<T>(&args[2])?;

            if flags.iter().any(|s| s == Some("g")) {
                return plan_err!("regexp_like() does not support the \"global\" option");
            }

            let array = arrow_string::regexp::regexp_is_match_utf8(values, regex, Some(flags))
                .map_err(|e| arrow_datafusion_err!(e))?;

            Ok(Arc::new(array) as ArrayRef)
        }
        other => exec_err!(
            "regexp_like was called with {other} arguments. It requires at least 2 and at most 3."
        ),
    }
}

/// Extract a specific group from a string column, using a regular expression.
///
/// The full list of supported features and syntax can be found at
/// <https://docs.rs/regex/latest/regex/#syntax>
///
/// Supported flags can be found at
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
/// // use  the regexp_match function to test col 'values',
/// // against patterns in col 'patterns' without flags
/// let df = df.with_column(
///     "a",
///     regexp_match(vec![col("values"), col("patterns")])
/// )?;
/// // use the regexp_match function to test col 'values',
/// // against patterns in col 'patterns' with flags
/// let df = df.with_column(
///     "b",
///     regexp_match(vec![col("values"), col("patterns"), col("flags")]),
/// )?;
///
/// // literals can be used as well with dataframe calls
/// let df = df.with_column(
///     "c",
///     regexp_match(vec![lit("foobarbequebaz"), lit("(bar)(beque)")]),
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn regexp_match<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            arrow_string::regexp::regexp_match(values, regex, None)
                .map_err(|e| arrow_datafusion_err!(e))
        }
        3 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            let flags = as_generic_string_array::<T>(&args[2])?;

            if flags.iter().any(|s| s == Some("g")) {
                return plan_err!("regexp_match() does not support the \"global\" option")
            }

            arrow_string::regexp::regexp_match(values, regex, Some(flags))
                .map_err(|e| arrow_datafusion_err!(e))
        }
        other => exec_err!(
            "regexp_match was called with {other} arguments. It requires at least 2 and at most 3."
        ),
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
pub fn regexp_replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Default implementation for regexp_replace, assumes all args are arrays
    // and args is a sequence of 3 or 4 elements.

    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    match args.len() {
        3 => {
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let pattern_array = as_generic_string_array::<T>(&args[1])?;
            let replacement_array = as_generic_string_array::<T>(&args[2])?;

            let result = string_array
            .iter()
            .zip(pattern_array.iter())
            .zip(replacement_array.iter())
            .map(|((string, pattern), replacement)| match (string, pattern, replacement) {
                (Some(string), Some(pattern), Some(replacement)) => {
                    let replacement = regex_replace_posix_groups(replacement);

                    // if patterns hashmap already has regexp then use else create and return
                    let re = match patterns.get(pattern) {
                        Some(re) => Ok(re),
                        None => {
                            match Regex::new(pattern) {
                                Ok(re) => {
                                    patterns.insert(pattern.to_string(), re);
                                    Ok(patterns.get(pattern).unwrap())
                                },
                                Err(err) => Err(DataFusionError::External(Box::new(err))),
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
            let string_array = as_generic_string_array::<T>(&args[0])?;
            let pattern_array = as_generic_string_array::<T>(&args[1])?;
            let replacement_array = as_generic_string_array::<T>(&args[2])?;
            let flags_array = as_generic_string_array::<T>(&args[3])?;

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
                        (format!("(?{flags}){pattern}"), false)
                    };

                    // if patterns hashmap already has regexp then use else create and return
                    let re = match patterns.get(&pattern) {
                        Some(re) => Ok(re),
                        None => {
                            match Regex::new(pattern.as_str()) {
                                Ok(re) => {
                                    patterns.insert(pattern.clone(), re);
                                    Ok(patterns.get(&pattern).unwrap())
                                },
                                Err(err) => Err(DataFusionError::External(Box::new(err))),
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
        other => exec_err!(
            "regexp_replace was called with {other} arguments. It requires at least 3 and at most 4."
        ),
    }
}

fn _regexp_replace_early_abort<T: OffsetSizeTrait>(
    input_array: &GenericStringArray<T>,
) -> Result<ArrayRef> {
    // Mimicking the existing behavior of regexp_replace, if any of the scalar arguments
    // are actually null, then the result will be an array of the same size but with nulls.
    //
    // Also acts like an early abort mechanism when the input array is empty.
    Ok(new_null_array(input_array.data_type(), input_array.len()))
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
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let pattern = fetch_string_arg!(&args[1], "pattern", T, _regexp_replace_early_abort);
    let replacement =
        fetch_string_arg!(&args[2], "replacement", T, _regexp_replace_early_abort);
    let flags = match args.len() {
        3 => None,
        4 => Some(fetch_string_arg!(&args[3], "flags", T, _regexp_replace_early_abort)),
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
        (_, _, _, _) => Ok(make_scalar_function_inner(regexp_replace::<T>)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::*;

    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn test_case_sensitive_regexp_like() {
        let values = StringArray::from(vec!["abc"; 5]);

        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re = regexp_like::<i32>(&[Arc::new(values), Arc::new(patterns)]).unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_like() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);
        let flags = StringArray::from(vec!["i"; 5]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re =
            regexp_like::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_unsupported_global_flag_regexp_like() {
        let values = StringArray::from(vec!["abc"]);
        let patterns = StringArray::from(vec!["^(a)"]);
        let flags = StringArray::from(vec!["g"]);

        let re_err =
            regexp_like::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .expect_err("unsupported flag should have failed");

        assert_eq!(
            re_err.strip_backtrace(),
            "Error during planning: regexp_like() does not support the \"global\" option"
        );
    }

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

        assert_eq!(re_err.strip_backtrace(), "Error during planning: regexp_match() does not support the \"global\" option");
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
            pattern_err.strip_backtrace(),
            "External error: regex parse error:\n    [\n    ^\nerror: unclosed character class"
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
