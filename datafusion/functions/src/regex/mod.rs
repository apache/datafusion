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

//! "regex" DataFusion functions

use arrow::array::ArrayRef;
use arrow::compute::kernels::{cmp::eq, nullif::nullif};
use datafusion_common::{Result, ScalarValue};
use regex::Regex;
use std::collections::{HashMap, hash_map::Entry};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub(crate) use datafusion_physical_expr_common::regex::explain_regexp_kernel_error;
// The compilation of a regular expression is shared with the physical
// expressions, so that every caller reports a failure in the same way. These
// re-exports keep the paths that callers of this crate already use.
pub use datafusion_physical_expr_common::regex::{
    compile_and_cache_regex, compile_regex,
};
pub mod regexpcount;
pub mod regexpinstr;
pub mod regexplike;
pub mod regexpmatch;
pub mod regexpreplace;

/// Patterns are addressed by index rather than by reference so that `last` can
/// memoize the previous row's pattern without holding a borrow of `indices`
/// across rows. Repeated patterns yield the same key on consecutive rows, so
/// the memo avoids hashing in that case.
///
/// Criterion benchmarks for each function using this cache are in
/// `datafusion/functions/benches/regex_expressions/cache.rs`.
pub(crate) struct RegexCache<'a> {
    function_name: &'static str,
    compiled: Vec<Regex>,
    indices: HashMap<RegexKey<'a>, usize>,
    last: Option<(RegexKey<'a>, usize)>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct RegexKey<'a> {
    regex: &'a str,
    flags: Option<&'a str>,
}

impl Hash for RegexKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.regex.hash(state);
        // String hashing includes a delimiter, so absent flags need no
        // additional discriminator. Equality still distinguishes every key.
        if let Some(flags) = self.flags {
            flags.hash(state);
        }
    }
}

impl<'a> RegexCache<'a> {
    fn new(function_name: &'static str) -> Self {
        Self {
            function_name,
            compiled: Vec::new(),
            indices: HashMap::new(),
            last: None,
        }
    }

    #[inline]
    pub(crate) fn get_or_compile(
        &mut self,
        regex: &'a str,
        flags: Option<&'a str>,
    ) -> Result<&Regex> {
        let key = RegexKey { regex, flags };
        let index = match self.last {
            Some((last_key, index)) if last_key == key => index,
            _ => {
                let index = match self.indices.entry(key) {
                    Entry::Occupied(entry) => *entry.get(),
                    Entry::Vacant(entry) => {
                        self.compiled.push(Self::compile(
                            self.function_name,
                            regex,
                            flags,
                        )?);
                        *entry.insert(self.compiled.len() - 1)
                    }
                };
                self.last = Some((key, index));
                index
            }
        };
        Ok(&self.compiled[index])
    }
    #[cold]
    fn compile(function_name: &str, regex: &str, flags: Option<&str>) -> Result<Regex> {
        // Replacement's global flag chooses the replacement limit. Other
        // functions reject it through the standard compilation helper.
        if function_name == "regexp_replace" {
            let flags = flags.map(|flags| flags.replace('g', ""));
            compile_regex(function_name, regex, flags.as_deref())
        } else {
            compile_regex(function_name, regex, flags)
        }
    }
}

/// Arrow's regex kernels treat null flags as no flags, but reject empty flags.
/// Normalize empty strings without copying the string buffers.
fn normalize_empty_flags(flags: &ArrayRef) -> Result<ArrayRef> {
    let empty =
        ScalarValue::try_from_string(String::new(), flags.data_type())?.to_scalar()?;
    Ok(nullif(flags, &eq(flags, &empty)?)?)
}

// create UDFs
make_udf_function!(regexpcount::RegexpCountFunc, regexp_count);
make_udf_function!(regexpinstr::RegexpInstrFunc, regexp_instr);
make_udf_function!(regexpmatch::RegexpMatchFunc, regexp_match);
make_udf_function!(regexplike::RegexpLikeFunc, regexp_like);
make_udf_function!(regexpreplace::RegexpReplaceFunc, regexp_replace);

pub mod expr_fn {
    use datafusion_expr::Expr;

    /// Returns the number of consecutive occurrences of a regular expression in a string.
    pub fn regexp_count(
        values: Expr,
        regex: Expr,
        start: Option<Expr>,
        flags: Option<Expr>,
    ) -> Expr {
        let mut args = vec![values, regex];
        if let Some(start) = start {
            args.push(start);
        }

        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_count().call(args)
    }

    /// Returns a list of regular expression matches in a string.
    pub fn regexp_match(values: Expr, regex: Expr, flags: Option<Expr>) -> Expr {
        let mut args = vec![values, regex];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_match().call(args)
    }

    /// Returns index of regular expression matches in a string.
    pub fn regexp_instr(
        values: Expr,
        regex: Expr,
        start: Option<Expr>,
        n: Option<Expr>,
        endoption: Option<Expr>,
        flags: Option<Expr>,
        subexpr: Option<Expr>,
    ) -> Expr {
        let mut args = vec![values, regex];
        if let Some(start) = start {
            args.push(start);
        }
        if let Some(n) = n {
            args.push(n);
        }
        if let Some(endoption) = endoption {
            args.push(endoption);
        }
        if let Some(flags) = flags {
            args.push(flags);
        }
        if let Some(subexpr) = subexpr {
            args.push(subexpr);
        }
        super::regexp_instr().call(args)
    }
    /// Returns true if a regex has at least one match in a string, false otherwise.
    pub fn regexp_like(values: Expr, regex: Expr, flags: Option<Expr>) -> Expr {
        let mut args = vec![values, regex];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_like().call(args)
    }

    /// Replaces substrings in a string that match.
    pub fn regexp_replace(
        string: Expr,
        pattern: Expr,
        replacement: Expr,
        flags: Option<Expr>,
    ) -> Expr {
        let mut args = vec![string, pattern, replacement];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_replace().call(args)
    }
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<datafusion_expr::ScalarUDF>> {
    vec![
        regexp_count(),
        regexp_match(),
        regexp_instr(),
        regexp_like(),
        regexp_replace(),
    ]
}

/// Maps `start`, a 1-based character position, to a byte offset in `value`.
/// Positions `1..=n` (for an `n`-character string) map to the corresponding
/// character's first byte; position `n + 1`, the end of the string, maps to
/// `value.len()`. Returns `None` for larger positions. Callers must validate
/// `start >= 1`.
pub(crate) fn start_to_byte_offset(value: &str, start: i64) -> Option<usize> {
    // If `start - 1` does not fit in `usize`, it is necessarily past the end
    // of the string.
    let start_index = usize::try_from(start - 1).ok()?;
    value
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(value.len()))
        .nth(start_index)
}

#[cfg(test)]
mod tests {
    use super::start_to_byte_offset;

    #[test]
    fn regex_cache_compiles_each_key_once() {
        let mut cache = super::RegexCache::new("regexp_instr");
        for (pattern, flags) in [
            ("a", None),
            ("a", None),
            ("b", None),
            ("a", None),
            ("a", Some("i")),
            ("a", Some("i")),
            ("a", None),
        ] {
            let regex = cache.get_or_compile(pattern, flags).unwrap();
            assert!(regex.is_match(pattern));
        }
        assert_eq!(cache.compiled.len(), 3);
        assert!(cache.get_or_compile("[", None).is_err());
        assert!(cache.get_or_compile("a", None).unwrap().is_match("a"));
    }

    #[test]
    fn regex_cache_preserves_flag_semantics() {
        let mut cache = super::RegexCache::new("regexp_instr");
        let mut replacement_cache = super::RegexCache::new("regexp_replace");
        assert!(!cache.get_or_compile("a", None).unwrap().is_match("A"));
        assert!(cache.get_or_compile("a", Some("i")).unwrap().is_match("A"));
        assert!(!cache.get_or_compile("a", None).unwrap().is_match("A"));
        assert!(cache.get_or_compile("a", Some("g")).is_err());
        assert!(
            replacement_cache
                .get_or_compile("a", Some("gi"))
                .unwrap()
                .is_match("A")
        );
        assert!(
            !replacement_cache
                .get_or_compile("a", Some("g"))
                .unwrap()
                .is_match("A")
        );
        assert!(cache.get_or_compile("a", Some("g")).is_err());
    }

    #[test]
    fn empty_flags_match_omitted_flags() {
        use arrow::array::StringArray;
        use arrow::compute::cast;
        use arrow::datatypes::{DataType, Field};
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

        use super::*;

        for udf in [regexp_like(), regexp_match()] {
            for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                // Exercise every scalar/array combination, bypassing simplification.
                for shape in 0..8 {
                    for pattern in ["b..", "B..", "", "(b)(..)"] {
                        let args = ["foobarbaz", pattern, ""]
                            .into_iter()
                            .enumerate()
                            .map(|(i, value)| {
                                let scalar = ScalarValue::try_from_string(
                                    value.to_string(),
                                    &data_type,
                                )
                                .unwrap();
                                if shape & (1 << i) == 0 {
                                    ColumnarValue::Scalar(scalar)
                                } else {
                                    ColumnarValue::Array(
                                        scalar.to_array_of_size(3).unwrap(),
                                    )
                                }
                            })
                            .collect::<Vec<_>>();
                        let invoke = |args: Vec<ColumnarValue>| {
                            let types = args
                                .iter()
                                .map(ColumnarValue::data_type)
                                .collect::<Vec<_>>();
                            let return_type = udf.return_type(&types).unwrap();
                            udf.invoke_with_args(ScalarFunctionArgs {
                                arg_fields: types
                                    .into_iter()
                                    .map(|t| Arc::new(Field::new("arg", t, true)))
                                    .collect(),
                                args,
                                number_rows: 3,
                                return_field: Arc::new(Field::new(
                                    "result",
                                    return_type,
                                    true,
                                )),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap()
                            .to_array(3)
                            .unwrap()
                        };
                        assert_eq!(
                            &invoke(args.clone()),
                            &invoke(args[..2].to_vec()),
                            "{} {data_type:?} shape={shape} pattern={pattern:?}",
                            udf.name()
                        );
                    }
                }

                // Empty and null flags both mean no flags; preserve nonempty flags
                // and null values/patterns in the same batch.
                let arrays = [
                    vec![
                        Some("abc"),
                        Some("ABC"),
                        Some("ABC"),
                        None,
                        Some("abc"),
                        Some("abc"),
                    ],
                    vec![Some("a"), Some("a"), Some("a"), Some("a"), None, Some("a")],
                    vec![Some(""), Some("i"), Some(""), Some(""), Some(""), None],
                ]
                .map(|values| cast(&StringArray::from(values), &data_type).unwrap());
                let expected_flags = cast(
                    &StringArray::from(vec![None, Some("i"), None, None, None, None]),
                    &data_type,
                )
                .unwrap();
                let kernel = if udf.name() == "regexp_like" {
                    regexplike::regexp_like
                } else {
                    regexpmatch::regexp_match
                };
                let expected = kernel(&[
                    Arc::clone(&arrays[0]),
                    Arc::clone(&arrays[1]),
                    expected_flags,
                ])
                .unwrap();
                assert_eq!(&kernel(&arrays).unwrap(), &expected);
            }
        }
    }

    #[test]
    fn start_to_byte_offset_ascii() {
        assert_eq!(start_to_byte_offset("abc", 1), Some(0));
        assert_eq!(start_to_byte_offset("abc", 3), Some(2));
        // The end of the string is a valid position.
        assert_eq!(start_to_byte_offset("abc", 4), Some(3));
        assert_eq!(start_to_byte_offset("abc", 5), None);
        assert_eq!(start_to_byte_offset("abc", i64::MAX), None);
    }

    #[test]
    fn start_to_byte_offset_empty_string() {
        assert_eq!(start_to_byte_offset("", 1), Some(0));
        assert_eq!(start_to_byte_offset("", 2), None);
    }

    #[test]
    fn start_to_byte_offset_multibyte() {
        assert_eq!(start_to_byte_offset("😀a", 1), Some(0));
        assert_eq!(start_to_byte_offset("😀a", 2), Some(4));
        assert_eq!(start_to_byte_offset("😀a", 3), Some(5));
        assert_eq!(start_to_byte_offset("😀a", 4), None);
    }
}
