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

use std::any::type_name;
use std::sync::Arc;

use crate::error::{DataFusionError, Result};
use arrow::array::*;
use arrow::error::ArrowError;
use hashbrown::HashMap;
use regex::Regex;

macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<Utf8Array<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<Utf8Array<T>>()
                ))
            })?
    }};
}

/// extract a specific group from a string column, using a regular expression
pub fn regexp_match<T: Offset>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let values = downcast_string_arg!(args[0], "string", T);
            let regex = downcast_string_arg!(args[1], "pattern", T);
            Ok(regexp_matches(values, regex, None).map(|x| Arc::new(x) as Arc<dyn Array>)?)
        },
        3 => {
            let values = downcast_string_arg!(args[0], "string", T);
            let regex = downcast_string_arg!(args[1], "pattern", T);
            let flags = Some(downcast_string_arg!(args[2], "flags", T));
            Ok(regexp_matches(values, regex, flags).map(|x| Arc::new(x) as Arc<dyn Array>)?)
        },
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
pub fn regexp_replace<T: Offset>(args: &[ArrayRef]) -> Result<ArrayRef> {
    // creating Regex is expensive so create hashmap for memoization
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    match args.len() {
        3 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let pattern_array = downcast_string_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_arg!(args[2], "replacement", T);

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
            .collect::<Result<Utf8Array<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        4 => {
            let string_array = downcast_string_arg!(args[0], "string", T);
            let pattern_array = downcast_string_arg!(args[1], "pattern", T);
            let replacement_array = downcast_string_arg!(args[2], "replacement", T);
            let flags_array = downcast_string_arg!(args[3], "flags", T);

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
                        (format!("(?{}){}", flags.to_string().replace("g", ""), pattern), true)
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
            .collect::<Result<Utf8Array<T>>>()?;

            Ok(Arc::new(result) as ArrayRef)
        }
        other => Err(DataFusionError::Internal(format!(
            "regexp_replace was called with {} arguments. It requires at least 3 and at most 4.",
            other
        ))),
    }
}

/// Extract all groups matched by a regular expression for a given String array.
pub fn regexp_matches<O: Offset>(
    array: &Utf8Array<O>,
    regex_array: &Utf8Array<O>,
    flags_array: Option<&Utf8Array<O>>,
) -> Result<ListArray<O>> {
    let mut patterns: HashMap<String, Regex> = HashMap::new();

    let complete_pattern = match flags_array {
        Some(flags) => Box::new(regex_array.iter().zip(flags.iter()).map(
            |(pattern, flags)| {
                pattern.map(|pattern| match flags {
                    Some(value) => format!("(?{}){}", value, pattern),
                    None => pattern.to_string(),
                })
            },
        )) as Box<dyn Iterator<Item = Option<String>>>,
        None => Box::new(
            regex_array
                .iter()
                .map(|pattern| pattern.map(|pattern| pattern.to_string())),
        ),
    };
    let iter = array.iter().zip(complete_pattern).map(|(value, pattern)| {
        match (value, pattern) {
            // Required for Postgres compatibility:
            // SELECT regexp_match('foobarbequebaz', ''); = {""}
            (Some(_), Some(pattern)) if pattern == *"" => {
                Result::Ok(Some(vec![Some("")].into_iter()))
            }
            (Some(value), Some(pattern)) => {
                let existing_pattern = patterns.get(&pattern);
                let re = match existing_pattern {
                    Some(re) => re.clone(),
                    None => {
                        let re = Regex::new(pattern.as_str()).map_err(|e| {
                            ArrowError::InvalidArgumentError(format!(
                                "Regular expression did not compile: {:?}",
                                e
                            ))
                        })?;
                        patterns.insert(pattern, re.clone());
                        re
                    }
                };
                match re.captures(value) {
                    Some(caps) => {
                        let a = caps
                            .iter()
                            .skip(1)
                            .map(|x| x.map(|x| x.as_str()))
                            .collect::<Vec<_>>()
                            .into_iter();
                        Ok(Some(a))
                    }
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    });
    let mut array = MutableListArray::<O, MutableUtf8Array<O>>::new();
    for items in iter {
        if let Some(items) = items? {
            let values = array.mut_values();
            values.try_extend(items)?;
            array.try_push_valid()?;
        } else {
            array.push_null();
        }
    }

    Ok(array.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_single_group() -> Result<()> {
        let array = Utf8Array::<i32>::from(&[
            Some("abc-005-def"),
            Some("X-7-5"),
            Some("X545"),
            None,
            Some("foobarbequebaz"),
            Some("foobarbequebaz"),
        ]);

        let patterns = Utf8Array::<i32>::from_slice(&[
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r".*-(\d*)-.*",
            r"(bar)(bequ1e)",
            "",
        ]);

        let result = regexp_matches(&array, &patterns, None)?;

        let expected = vec![
            Some(vec![Some("005")]),
            Some(vec![Some("7")]),
            None,
            None,
            None,
            Some(vec![Some("")]),
        ];

        let mut array = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
        array.try_extend(expected)?;
        let expected: ListArray<i32> = array.into();

        assert_eq!(expected, result);
        Ok(())
    }

    #[test]
    fn match_single_group_with_flags() -> Result<()> {
        let array = Utf8Array::<i32>::from(&[
            Some("abc-005-def"),
            Some("X-7-5"),
            Some("X545"),
            None,
        ]);

        let patterns = Utf8Array::<i32>::from_slice(&vec![r"x.*-(\d*)-.*"; 4]);
        let flags = Utf8Array::<i32>::from_slice(vec!["i"; 4]);

        let result = regexp_matches(&array, &patterns, Some(&flags))?;

        let expected = vec![None, Some(vec![Some("7")]), None, None];
        let mut array = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
        array.try_extend(expected)?;
        let expected: ListArray<i32> = array.into();

        assert_eq!(expected, result);
        Ok(())
    }
}
