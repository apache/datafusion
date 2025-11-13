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

use datafusion::common::{exec_datafusion_err, Result};
use itertools::Itertools;
use log::Level::Warn;
use log::{info, log_enabled, warn};
use sqllogictest::Normalizer;
use std::fs;
use std::path::{Path, PathBuf};

/// Sets up an empty directory at `test_files/scratch/<name>`
/// creating it if needed and clearing any file contents if it exists
/// This allows tests for inserting to external tables or copy to
/// persist data to disk and have consistent state when running
/// a new test
pub fn setup_scratch_dir(name: &Path) -> Result<()> {
    // go from copy.slt --> copy
    let file_stem = name.file_stem().expect("File should have a stem");
    let path = PathBuf::from("test_files").join("scratch").join(file_stem);

    info!("Creating scratch dir in {path:?}");
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    Ok(())
}

/// Trailing whitespace from lines in SLT will typically be removed, but do not fail if it is not
/// If particular test wants to cover trailing whitespace on a value,
/// it should project additional non-whitespace column on the right.
#[allow(clippy::ptr_arg)]
pub fn value_normalizer(s: &String) -> String {
    s.trim_end().to_string()
}

pub fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref())?;
    Ok(dst)
}

/// Append all paths recursively to dst
fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) -> Result<()> {
    let entries = fs::read_dir(path)
        .map_err(|e| exec_datafusion_err!("Error reading directory {path:?}: {e}"))?;
    for entry in entries {
        let path = entry
            .map_err(|e| {
                exec_datafusion_err!("Error reading entry in directory {path:?}: {e}")
            })?
            .path();

        if path.is_dir() {
            read_dir_recursive_impl(dst, &path)?;
        } else {
            dst.push(path);
        }
    }

    Ok(())
}

/// Validate the actual and expected values.
pub fn df_value_validator(
    normalizer: Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    let normalized_expected = expected.iter().map(normalizer).collect::<Vec<_>>();
    let normalized_actual = actual
        .iter()
        .map(|strs| strs.iter().join(" "))
        .map(|str| str.trim_end().to_string())
        .collect_vec();

    if log_enabled!(Warn) && normalized_actual != normalized_expected {
        warn!("df validation failed. actual vs expected:");
        for i in 0..normalized_actual.len() {
            warn!("[{i}] {}<eol>", normalized_actual[i]);
            warn!(
                "[{i}] {}<eol>",
                if normalized_expected.len() > i {
                    &normalized_expected[i]
                } else {
                    "No more results"
                }
            );
        }
    }

    normalized_actual == normalized_expected
}

pub fn is_spark_path(relative_path: &Path) -> bool {
    relative_path.starts_with("spark/")
}
