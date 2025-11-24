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
use std::cell::RefCell;
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
    reset_validation_failure();

    let contains_ignore_marker = expected.iter().any(|line| line.contains(IGNORE_MARKER));

    let normalized_expected: Vec<String> =
        expected.iter().map(normalizer).collect::<Vec<_>>();
    let normalized_actual: Vec<String> = actual
        .iter()
        .map(|strs| strs.iter().join(" "))
        .map(|str| str.trim_end().to_string())
        .collect_vec();

    if contains_ignore_marker {
        let expected_snapshot = normalized_expected.join("\n");
        let actual_snapshot = normalized_actual.join("\n");
        match compare_with_ignore_markers(&expected_snapshot, &actual_snapshot) {
            Ok(()) => true,
            Err(note) => {
                record_validation_failure(ValidationFailure {
                    expected_snapshot,
                    actual_snapshot,
                    note: Some(note),
                });
                log_value_mismatch(&normalized_actual, &normalized_expected);
                false
            }
        }
    } else if normalized_actual == normalized_expected {
        true
    } else {
        log_value_mismatch(&normalized_actual, &normalized_expected);
        false
    }
}

pub fn is_spark_path(relative_path: &Path) -> bool {
    relative_path.starts_with("spark/")
}

/// Details captured when validation fails so that the error reporter can render a consistent
/// message (e.g. suppressing `<slt:ignore>` false positives).
#[derive(Debug, Clone)]
pub struct ValidationFailure {
    pub expected_snapshot: String,
    pub actual_snapshot: String,
    pub note: Option<String>,
}

thread_local! {
    static LAST_VALIDATION_FAILURE: RefCell<Option<ValidationFailure>> = const { RefCell::new(None) };
}

pub fn take_last_validation_failure() -> Option<ValidationFailure> {
    LAST_VALIDATION_FAILURE.with(|cell| cell.borrow_mut().take())
}

fn record_validation_failure(failure: ValidationFailure) {
    LAST_VALIDATION_FAILURE.with(|cell| {
        *cell.borrow_mut() = Some(failure);
    });
}

fn reset_validation_failure() {
    LAST_VALIDATION_FAILURE.with(|cell| {
        cell.borrow_mut().take();
    });
}

fn log_value_mismatch(actual: &[String], expected: &[String]) {
    if !log_enabled!(Warn) {
        return;
    }

    warn!("df validation failed. actual vs expected:");
    for i in 0..actual.len() {
        warn!("[{i}] {}<eol>", actual[i]);
        warn!(
            "[{i}] {}<eol>",
            if expected.len() > i {
                &expected[i]
            } else {
                "No more results"
            }
        );
    }
}

const IGNORE_MARKER: &str = "<slt:ignore>";

fn compare_with_ignore_markers(
    expected_snapshot: &str,
    actual_snapshot: &str,
) -> Result<(), String> {
    let fragments: Vec<&str> = expected_snapshot.split(IGNORE_MARKER).collect();
    let mut pos = 0usize;
    let trailing_wildcard = expected_snapshot.ends_with(IGNORE_MARKER);
    for (i, frag) in fragments.iter().enumerate() {
        if frag.is_empty() {
            continue;
        }

        if let Some(idx) = actual_snapshot[pos..].find(frag) {
            if i == 0 && idx != 0 {
                let unexpected = actual_snapshot[pos..pos + idx]
                    .chars()
                    .take(32)
                    .collect::<String>();
                return Err(format!(
                    "expected output should start with fragment \"{frag}\" but actual output begins with \"{unexpected}\""
                ));
            }
            pos += idx + frag.len();
        } else {
            let tail = &actual_snapshot[pos..];
            let preview = tail.chars().take(32).collect::<String>();
            return Err(format!(
                "fragment \"{frag}\" not found in actual output after byte offset {pos} (remaining output starts with \"{preview}\")"
            ));
        }
    }

    if !trailing_wildcard && pos != actual_snapshot.len() {
        let preview = actual_snapshot[pos..].chars().take(32).collect::<String>();
        return Err(format!(
            "actual output contains additional trailing data starting with \"{preview}\""
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Validation should fail for the below case:
    // Actual - 'foo bar baz'
    // Expected - 'bar <slt:ignore>'
    #[test]
    fn ignore_marker_does_not_skip_leading_text() {
        // Actual snapshot contains unexpected prefix before the expected fragment.
        let actual = vec![vec!["foo bar baz".to_string()]];
        let expected = vec!["bar <slt:ignore>".to_string()];

        assert!(!df_value_validator(value_normalizer, &actual, &expected));
    }

    #[test]
    fn ignore_marker_failure_records_context() {
        let actual = vec![
            vec!["0".to_string()],
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        let expected = vec![
            "<slt:ignore>".to_string(),
            "1".to_string(),
            "3<slt:ignore>".to_string(),
        ];

        assert!(!df_value_validator(value_normalizer, &actual, &expected));
        let failure = take_last_validation_failure().expect("failure recorded");
        assert!(failure.expected_snapshot.contains("<slt:ignore>"));
        assert!(failure.actual_snapshot.contains("2"));
        assert!(failure.note.expect("note").contains("fragment"));
    }

    #[test]
    fn ignore_marker_requires_exact_end_without_trailing_wildcard() {
        let actual = vec![
            vec!["0".to_string()],
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        let expected = vec!["<slt:ignore>".to_string(), "2".to_string()];

        assert!(!df_value_validator(value_normalizer, &actual, &expected));
        let failure = take_last_validation_failure().expect("failure recorded");
        assert!(failure.note.expect("note").contains("trailing data"));
    }
}
