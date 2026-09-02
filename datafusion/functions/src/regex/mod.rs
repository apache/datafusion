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

use arrow::error::ArrowError;
use regex::Regex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
pub mod regexpcount;
pub mod regexpinstr;
pub mod regexplike;
pub mod regexpmatch;
pub mod regexpreplace;

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

pub fn compile_and_cache_regex<'strings, 'cache>(
    regex: &'strings str,
    flags: Option<&'strings str>,
    regex_cache: &'cache mut HashMap<(&'strings str, Option<&'strings str>), Regex>,
) -> Result<&'cache Regex, ArrowError>
where
    'strings: 'cache,
{
    let result = match regex_cache.entry((regex, flags)) {
        Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
        Entry::Vacant(vacant_entry) => {
            let compiled = compile_regex(regex, flags)?;
            vacant_entry.insert(compiled)
        }
    };
    Ok(result)
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

pub fn compile_regex(regex: &str, flags: Option<&str>) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains('g') {
                return Err(ArrowError::ComputeError(
                    "regexp_count()/regexp_instr() does not support the global flag"
                        .to_string(),
                ));
            }
            format!("(?{flags}){regex}")
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!("Regular expression did not compile: {pattern}"))
    })
}

#[cfg(test)]
mod tests {
    use super::start_to_byte_offset;

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
