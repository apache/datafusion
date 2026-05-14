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

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

/// Represents a parsed test file
///
/// Note there is a custom Ord implementation that sorts test files by:
/// 1. Hard coded test priority (lower runs first),
/// 2. Relative path as deterministic tie-breaker.
#[derive(Debug, PartialEq, Eq)]
pub struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    /// Create a new [`TestFile`] from the given path, stripping any of the
    /// known test directory prefixes for the relative path.
    pub fn new(path: PathBuf, prefixes: &[&str]) -> Self {
        let p = path.to_string_lossy();
        for prefix in prefixes {
            if p.starts_with(prefix) {
                let relative_path = PathBuf::from(p.strip_prefix(prefix).unwrap());
                return Self {
                    path,
                    relative_path,
                };
            }
        }
        let relative_path = PathBuf::from("");

        Self {
            path,
            relative_path,
        }
    }

    /// Returns true if the file has a .slt extension, indicating it is a sqllogictest file.
    pub fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }

    /// Returns true if the relative path starts with the given prefix, which
    /// can be used to filter tests by subdirectory or filename patterns.
    pub fn relative_path_starts_with(&self, prefix: impl AsRef<Path>) -> bool {
        self.relative_path.starts_with(prefix)
    }
}

impl PartialOrd for TestFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TestFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_path = &self.relative_path;
        let other_path = &other.relative_path;

        let priority_self = TEST_PRIORITY.get(self_path).unwrap_or(&DEFAULT_PRIORITY);
        let priority_other = TEST_PRIORITY.get(other_path).unwrap_or(&DEFAULT_PRIORITY);

        priority_self
            .cmp(priority_other)
            .then_with(|| self_path.cmp(other_path)) // Tie-breaker: lexicographic order of relative paths.
            // Final tie-breaker keeps Ord consistent with Eq when relative paths collide.
            .then_with(|| self.path.cmp(&other.path))
    }
}

/// TEST PRIORITY
///
/// Heuristically prioritize some test to run earlier.
///
/// Prioritizes test to run earlier if they are known to be long running (as
/// each test file itself is run sequentially, but multiple test files are run
/// in parallel.
///
/// Tests not listed here will run after the listed tests in deterministic
/// lexicographic order by relative path.
///
/// You can find the top longest running tests by running `--timing-summary`
/// mode. For example
///
/// ```shell
/// $ cargo test --profile=ci --test sqllogictests -- --timing-summary top
/// ...
/// Per-file elapsed summary (deterministic):
/// 1.    3.568s  aggregate.slt
/// 2.    3.464s  joins.slt
/// 3.    3.336s  imdb.slt
/// 4.    3.085s  push_down_filter_regression.slt
/// 5.    2.926s  aggregate_skip_partial.slt
/// 6.    2.399s  window.slt
/// 7.    2.198s  group_by.slt
/// 8.    1.281s  clickbench.slt
/// 9.    1.058s  datetime/timestamps.slt
/// ```
const TEST_PRIORITY_ENTRIES: &[&str] = &[
    "aggregate.slt", //  longest-running files go first
    "joins.slt",
    "imdb.slt",
    "push_down_filter_regression.slt",
    "aggregate_skip_partial.slt",
    "window.slt",
    "group_by.slt",
    "clickbench.slt",
    "datetime/timestamps.slt",
];

/// Default priority for tests not in the priority map. Tests with lower
/// priority values run first.
const DEFAULT_PRIORITY: usize = 100;

static TEST_PRIORITY: LazyLock<HashMap<PathBuf, usize>> = LazyLock::new(|| {
    TEST_PRIORITY_ENTRIES
        .iter()
        .enumerate()
        .map(|(priority, path)| (PathBuf::from(path), priority))
        .collect()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prioritized_files_are_first() {
        let mut input = vec!["z_unlisted.slt", "a_unlisted.slt"];
        input.extend(TEST_PRIORITY_ENTRIES.iter());
        input.push("q_unlisted.slt");

        let mut sorted = to_test_files(input);
        sorted.sort_unstable();

        println!("Sorted input: {sorted:?}");

        // the prioritized files should be first, in the order specified by TEST_PRIORITY_ENTRIES
        for file in sorted.iter().take(TEST_PRIORITY_ENTRIES.len()) {
            assert!(
                TEST_PRIORITY.contains_key(&file.relative_path),
                "Expected prioritized file {file:?} not found in input {sorted:?}"
            );
        }
        // last three files should be the unlisted ones in deterministic order
        let expected_files =
            to_test_files(["a_unlisted.slt", "q_unlisted.slt", "z_unlisted.slt"]);
        assert!(
            sorted.ends_with(&expected_files),
            "Expected unlisted files {expected_files:?} at the end in deterministic order of {sorted:?}"
        );
    }

    fn to_test_files<'a>(files: impl IntoIterator<Item = &'a str>) -> Vec<TestFile> {
        files
            .into_iter()
            .map(|f| TestFile {
                path: PathBuf::from(f),
                relative_path: PathBuf::from(f),
            })
            .collect()
    }
}
