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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow::error::ArrowError;

use regex::Regex;

pub(crate) fn compile_and_cache_regex(
    regex: &str,
    flags: Option<&str>,
    supports_global: bool,
    regex_cache: &mut HashMap<String, Regex>,
) -> Result<Regex, ArrowError> {
    match regex_cache.entry(regex.to_string()) {
        Entry::Vacant(entry) => {
            let compiled = compile_regex(regex, flags, supports_global)?;
            entry.insert(compiled.clone());
            Ok(compiled)
        }
        Entry::Occupied(entry) => Ok(entry.get().to_owned()),
    }
}

pub(crate) fn compile_regex(
    regex: &str,
    flags: Option<&str>,
    supports_global: bool,
) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") && !supports_global {
                return Err(ArrowError::ComputeError(
                    "regexp function does not support global flag".to_string(),
                ));
            }
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
    })
}
