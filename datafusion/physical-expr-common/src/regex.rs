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

use arrow::array::{Array, AsArray, LargeStringArray, StringArray, StringViewArray};
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
///
/// This is `pub` only so that the crates that call the kernels can reach it.
// Not public API.
#[doc(hidden)]
pub fn explain_regexp_kernel_error(
    function_name: &str,
    error: ArrowError,
    patterns: &dyn Array,
    flags: Option<&dyn Array>,
) -> DataFusionError {
    let Some(patterns) = StringValues::new(patterns) else {
        return arrow_datafusion_err!(error);
    };
    let flags = match flags.map(StringValues::new) {
        None => None,
        Some(Some(flags)) => Some(flags),
        // Flags of some other type are not what made the kernel fail.
        Some(None) => return arrow_datafusion_err!(error),
    };

    let rows = patterns
        .len()
        .max(flags.as_ref().map_or(0, StringValues::len));
    for row in 0..rows {
        // A NULL pattern or NULL flags produce a NULL result, not an error.
        let Some(pattern) = patterns.broadcast_value(row) else {
            continue;
        };
        let flags = flags.as_ref().and_then(|flags| flags.broadcast_value(row));
        if let Err(error) = compile_regex(function_name, pattern, flags) {
            return error;
        }
    }

    arrow_datafusion_err!(error)
}

/// A string array of any of the three string types, read by row.
///
/// This borrows the array that the kernel received, so that explaining an
/// error reads the rows it needs and allocates nothing, however long the
/// array is.
enum StringValues<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> StringValues<'a> {
    /// Borrows `array`, or returns `None` for an array of any other type.
    fn new(array: &'a dyn Array) -> Option<Self> {
        match array.data_type() {
            DataType::Utf8 => Some(Self::Utf8(array.as_string::<i32>())),
            DataType::LargeUtf8 => Some(Self::LargeUtf8(array.as_string::<i64>())),
            DataType::Utf8View => Some(Self::Utf8View(array.as_string_view())),
            _ => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Utf8(array) => array.len(),
            Self::LargeUtf8(array) => array.len(),
            Self::Utf8View(array) => array.len(),
        }
    }

    /// Reads the value of `row`, treating an array of a single value as a
    /// scalar that applies to every row.
    fn broadcast_value(&self, row: usize) -> Option<&'a str> {
        let row = if self.len() == 1 { 0 } else { row };
        if row >= self.len() {
            return None;
        }
        match *self {
            Self::Utf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::LargeUtf8(array) => (!array.is_null(row)).then(|| array.value(row)),
            Self::Utf8View(array) => (!array.is_null(row)).then(|| array.value(row)),
        }
    }
}
