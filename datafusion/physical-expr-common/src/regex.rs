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

//! Compilation of the regular expressions of SQL.
//!
//! The regexp functions and the `~` family of operators share this module, so
//! that a pattern that does not compile is reported in one way, wherever the
//! pattern came from.

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::{
    DataFusionError, Result, arrow_datafusion_err, exec_datafusion_err, plan_err,
};
use regex::Regex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

/// Compiles `regex` with [`compile_regex`], keeping the compiled pattern in
/// `regex_cache` under the key `(regex, flags)`.
pub fn compile_and_cache_regex<'strings, 'cache>(
    function_name: &str,
    regex: &'strings str,
    flags: Option<&'strings str>,
    regex_cache: &'cache mut HashMap<(&'strings str, Option<&'strings str>), Regex>,
) -> Result<&'cache Regex>
where
    'strings: 'cache,
{
    let result = match regex_cache.entry((regex, flags)) {
        Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
        Entry::Vacant(vacant_entry) => {
            let compiled = compile_regex(function_name, regex, flags)?;
            vacant_entry.insert(compiled)
        }
    };
    Ok(result)
}

/// Compiles `regex`, applying `flags` as inline regex flags.
///
/// `function_name` names the SQL function that the user called. It appears in
/// the error that reports an unsupported flag, so that every function reports
/// its own name.
///
/// A pattern that does not compile is reported as
/// [`DataFusionError::Execution`] carrying the diagnosis of the `regex` crate,
/// which names the position and the reason the pattern was rejected.
pub fn compile_regex(
    function_name: &str,
    regex: &str,
    flags: Option<&str>,
) -> Result<Regex> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains('g') {
                return plan_err!(
                    "{function_name}() does not support the \"global\" option"
                );
            }
            format!("(?{flags}){regex}")
        }
    };

    Regex::new(&pattern)
        .map_err(|e| exec_datafusion_err!("Regular expression did not compile: {e}"))
}

/// Explains a failure reported by one of the arrow regexp kernels.
///
/// The kernels compile the patterns themselves and report a pattern that does
/// not compile as an opaque [`ArrowError::ComputeError`]. This compiles the
/// patterns that were given to the kernel and reports the first one that does
/// not compile, with the diagnosis of the `regex` crate. The diagnosis of a
/// syntax error quotes the pattern that caused it.
///
/// This runs only after the kernel has failed, so a query that succeeds never
/// compiles a pattern twice.
///
/// `patterns` and `flags` are the arrays that the kernel received. An argument
/// that was a scalar is held as an array of one value, which applies to every
/// row. If every pattern compiles, the failure has a different cause and the
/// original error is kept.
pub fn explain_regexp_kernel_error(
    function_name: &str,
    error: ArrowError,
    patterns: &dyn Array,
    flags: Option<&dyn Array>,
) -> DataFusionError {
    let Some(patterns) = string_values(patterns) else {
        return arrow_datafusion_err!(error);
    };
    let flags = match flags.map(string_values) {
        None => None,
        Some(Some(flags)) => Some(flags),
        // Flags of some other type are not what made the kernel fail.
        Some(None) => return arrow_datafusion_err!(error),
    };

    let rows = patterns.len().max(flags.as_ref().map_or(0, Vec::len));
    for row in 0..rows {
        // A NULL pattern or NULL flags produce a NULL result, not an error.
        let Some(pattern) = broadcast_value(&patterns, row) else {
            continue;
        };
        let flags = flags.as_ref().and_then(|flags| broadcast_value(flags, row));
        if let Err(error) = compile_regex(function_name, pattern, flags) {
            return error;
        }
    }

    arrow_datafusion_err!(error)
}

/// Borrows the values of a string array of any of the three string types.
/// Returns `None` for an array of any other type.
fn string_values(array: &dyn Array) -> Option<Vec<Option<&str>>> {
    match array.data_type() {
        DataType::Utf8 => Some(array.as_string::<i32>().iter().collect()),
        DataType::LargeUtf8 => Some(array.as_string::<i64>().iter().collect()),
        DataType::Utf8View => Some(array.as_string_view().iter().collect()),
        _ => None,
    }
}

/// Reads the value of `row`, treating an array of a single value as a scalar
/// that applies to every row.
fn broadcast_value<'a>(values: &[Option<&'a str>], row: usize) -> Option<&'a str> {
    if values.len() == 1 {
        values[0]
    } else {
        values.get(row).copied().flatten()
    }
}
