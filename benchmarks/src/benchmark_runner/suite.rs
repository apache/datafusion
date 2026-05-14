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

//! Suite-file loading and validation.
//!
//! A suite is described by a text `.suite` file that declares which options
//! the runner should display. Query discovery recursively scans from the suite
//! file's directory for `qNN.benchmark` files. Discovered queries are cached
//! lazily because they are reused by listing during a single CLI run.

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
use std::cell::OnceCell;
use std::collections::HashSet;
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteQuery {
    /// Numeric query id parsed from a `qNN.benchmark` file.
    pub id: usize,
    /// File name as it appears on disk, for example `q01.benchmark`.
    pub file_name: String,
    /// Full path to the benchmark file.
    pub path: PathBuf,
}

/// Parsed `.suite` file plus runtime metadata derived from its location.
///
/// The serialized fields define the text configuration format. The skipped
/// fields are populated from the suite file path and used for discovery and
/// caching during a single runner invocation.
#[derive(Debug, Deserialize, Serialize)]
pub struct SuiteConfig {
    /// Suite selector used on the command line, such as `tpch`.
    pub name: String,
    /// Human-readable suite description shown by `list`.
    pub description: String,
    /// Suite-specific options shown by `list`.
    #[serde(default)]
    pub options: Vec<SuiteOption>,
    /// Path to the `.suite` file that produced this config.
    #[serde(skip)]
    pub suite_path: PathBuf,
    /// Directory containing the `.suite` file.
    #[serde(skip)]
    pub suite_dir: PathBuf,
    /// Lazily discovered benchmark query files for this suite.
    #[serde(skip)]
    pub(crate) query_cache: OnceCell<Vec<SuiteQuery>>,
}

impl Clone for SuiteConfig {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            description: self.description.clone(),
            options: self.options.clone(),
            suite_path: self.suite_path.clone(),
            suite_dir: self.suite_dir.clone(),
            query_cache: OnceCell::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SuiteOption {
    /// Long option name, without the leading `--`.
    pub name: String,
    /// Optional short alias, without the leading `-`.
    #[serde(default)]
    pub short: Option<String>,
    /// Default option value.
    pub default: String,
    /// Allowed option values.
    pub values: Vec<String>,
    /// Help text shown in command output.
    pub help: String,
}

/// Discovered suite metadata, sorted by suite name.
#[derive(Debug, Clone)]
pub struct SuiteRegistry {
    suites: Vec<SuiteConfig>,
}

impl SuiteConfig {
    /// Loads, parses, and validates one `.suite` file.
    ///
    /// The suite file path and containing directory are stored on the returned
    /// config so later discovery can be resolved relative to the suite file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let suite_path = path.as_ref().to_path_buf();
        let contents = fs::read_to_string(&suite_path).map_err(|e| {
            DataFusionError::External(
                format!("failed to read suite file {}: {e}", suite_path.display()).into(),
            )
        })?;
        let mut suite: Self = toml::from_str(&contents).map_err(|e| {
            DataFusionError::External(
                format!("failed to parse suite file {}: {e}", suite_path.display())
                    .into(),
            )
        })?;

        suite.suite_dir = suite_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        suite.suite_path = suite_path;
        suite.validate()?;

        Ok(suite)
    }

    /// Returns the directory recursively searched for query benchmark files.
    pub fn query_search_root(&self) -> &Path {
        &self.suite_dir
    }

    /// Discovers and caches the suite's benchmark query files.
    ///
    /// Query files are found by recursively scanning from the suite directory,
    /// accepting only `qNN.benchmark` files, sorting by numeric query id, and
    /// rejecting duplicate ids.
    pub fn discover_queries(&self) -> Result<Vec<SuiteQuery>> {
        if let Some(queries) = self.query_cache.get() {
            return Ok(queries.clone());
        }

        let queries = self.scan_queries()?;
        let _ = self.query_cache.set(queries.clone());
        Ok(queries)
    }

    /// Performs uncached query discovery and duplicate-id validation.
    fn scan_queries(&self) -> Result<Vec<SuiteQuery>> {
        let mut queries = Vec::new();

        self.scan_query_dir(self.query_search_root(), &mut queries)?;
        queries.sort_by(|left, right| {
            left.id
                .cmp(&right.id)
                .then_with(|| left.path.cmp(&right.path))
        });

        for pair in queries.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            if left.id == right.id {
                return Err(DataFusionError::Configuration(format!(
                    "duplicate QUERY_ID {} in suite '{}': {} and {}",
                    left.id,
                    self.name,
                    left.path.display(),
                    right.path.display()
                )));
            }
        }

        Ok(queries)
    }

    /// Recursively scans a directory and appends valid query benchmark files to
    /// the provided collection.
    fn scan_query_dir(&self, dir: &Path, queries: &mut Vec<SuiteQuery>) -> Result<()> {
        let mut entries = read_dir_entries(dir, "benchmark query directory")?;
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| {
                DataFusionError::External(
                    format!(
                        "failed to read benchmark query entry type {}: {e}",
                        path.display()
                    )
                    .into(),
                )
            })?;

            if file_type.is_dir() {
                self.scan_query_dir(&path, queries)?;
                continue;
            }

            if path
                .extension()
                .is_none_or(|extension| extension != "benchmark")
            {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().into_owned();
            if let Some(id) = parse_query_file_name(&file_name) {
                queries.push(SuiteQuery {
                    id,
                    file_name,
                    path,
                });
            }
        }

        Ok(())
    }

    /// Validates suite metadata that cannot be enforced by TOML deserialization.
    fn validate(&self) -> Result<()> {
        self.validate_suite_fields()?;
        self.validate_options()
    }

    /// Validates required suite-level fields.
    fn validate_suite_fields(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(DataFusionError::Configuration(
                "suite name cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Validates suite-defined option declarations.
    fn validate_options(&self) -> Result<()> {
        let mut option_names = HashSet::new();
        for option in &self.options {
            if !option_names.insert(option.name.as_str()) {
                return Err(DataFusionError::Configuration(format!(
                    "duplicate option name '{}'",
                    option.name
                )));
            }

            option.validate()?;
        }

        Ok(())
    }
}

impl SuiteOption {
    /// Validates one suite-defined option.
    fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(DataFusionError::Configuration(
                "option name cannot be empty".to_string(),
            ));
        }

        if !is_valid_cli_option_name(&self.name) {
            return Err(DataFusionError::Configuration(format!(
                "invalid option name '{}'; expected lowercase ASCII letters, digits, and hyphens",
                self.name
            )));
        }

        self.validate_short_alias()?;

        if self.help.trim().is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "help for option '{}' cannot be empty",
                self.name
            )));
        }

        if self.values.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "values for option '{}' cannot be empty",
                self.name
            )));
        }

        let mut values = HashSet::new();
        for value in &self.values {
            if !values.insert(value.as_str()) {
                return Err(DataFusionError::Configuration(format!(
                    "duplicate value '{}' for option '{}'",
                    value, self.name
                )));
            }
        }

        if !self.values.contains(&self.default) {
            return Err(DataFusionError::Configuration(format!(
                "default value '{}' for option '{}' must be present in values",
                self.default, self.name
            )));
        }

        Ok(())
    }

    /// Validates the optional short alias for one suite-defined option.
    fn validate_short_alias(&self) -> Result<()> {
        let Some(short) = &self.short else {
            return Ok(());
        };

        if short.trim().is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "short alias for option '{}' cannot be empty",
                self.name
            )));
        }

        if !is_valid_cli_option_name(short) {
            return Err(DataFusionError::Configuration(format!(
                "invalid short alias '{short}' for option '{}'; expected lowercase ASCII letters, digits, and hyphens",
                self.name
            )));
        }

        Ok(())
    }
}

impl SuiteRegistry {
    /// Discovers all suite files below the SQL benchmark root.
    ///
    /// Each direct child directory is searched for `.suite` files. Suite names
    /// must be unique across the registry so command selectors are
    /// unambiguous.
    pub fn discover(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let mut suites = Vec::new();
        let mut suite_names = HashSet::new();

        for entry in read_dir_entries(root, "benchmark suite root")? {
            if !entry
                .file_type()
                .map_err(|e| {
                    DataFusionError::External(
                        format!(
                            "failed to read benchmark suite entry type {}: {e}",
                            entry.path().display()
                        )
                        .into(),
                    )
                })?
                .is_dir()
            {
                continue;
            }

            let mut suite_files = Vec::new();
            let suite_dir = entry.path();
            for suite_entry in read_dir_entries(&suite_dir, "benchmark suite directory")?
            {
                let path = suite_entry.path();
                if path
                    .extension()
                    .is_some_and(|extension| extension == "suite")
                {
                    suite_files.push(path);
                }
            }
            suite_files.sort();

            for suite_file in suite_files {
                let suite = SuiteConfig::from_file(suite_file)?;
                if !suite_names.insert(suite.name.clone()) {
                    return Err(DataFusionError::Configuration(format!(
                        "duplicate suite name '{}'",
                        suite.name
                    )));
                }
                suites.push(suite);
            }
        }

        suites.sort_by(|left, right| left.name.cmp(&right.name));

        Ok(Self { suites })
    }

    /// Returns discovered suites sorted by selector name.
    pub fn suites(&self) -> &[SuiteConfig] {
        &self.suites
    }
}

fn parse_query_file_name(file_name: &str) -> Option<usize> {
    let query_id = file_name.strip_prefix('q')?.strip_suffix(".benchmark")?;

    if query_id.len() < 2 || !query_id.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    query_id.parse().ok()
}

fn is_valid_cli_option_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

fn read_dir_entries(dir: &Path, label: &str) -> Result<Vec<DirEntry>> {
    let entries = fs::read_dir(dir).map_err(|e| {
        DataFusionError::External(
            format!("failed to read {label} {}: {e}", dir.display()).into(),
        )
    })?;

    entries
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    }

    #[test]
    fn discovers_tpch_suite_file() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();

        assert_eq!(registry.suites().len(), 1);
        assert_eq!(registry.suites()[0].name, "tpch");
        assert_eq!(registry.suites()[0].options.len(), 2);
    }

    #[test]
    fn discovers_query_ids_in_numeric_order() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let suite = &registry.suites()[0];
        let queries = suite.discover_queries().unwrap();
        let ids = queries.iter().map(|query| query.id).collect::<Vec<_>>();

        assert_eq!(ids.first(), Some(&1));
        assert_eq!(ids.last(), Some(&22));
        assert!(!ids.contains(&0));
    }

    #[test]
    fn rejects_duplicate_suite_names() {
        let dir = tempfile::tempdir().unwrap();
        let one = dir.path().join("one");
        let two = dir.path().join("two");
        fs::create_dir_all(&one).unwrap();
        fs::create_dir_all(&two).unwrap();
        fs::write(
            one.join("suite.suite"),
            "name = \"dup\"\ndescription = \"one\"\n",
        )
        .unwrap();
        fs::write(
            two.join("suite.suite"),
            "name = \"dup\"\ndescription = \"two\"\n",
        )
        .unwrap();

        let err = SuiteRegistry::discover(dir.path()).unwrap_err();
        assert!(err.to_string().contains("duplicate suite name"));
    }

    #[test]
    fn rejects_invalid_option_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let suite_dir = dir.path().join("suite");
        fs::create_dir_all(&suite_dir).unwrap();
        fs::write(
            suite_dir.join("bad.suite"),
            r#"
name = "bad"
description = "bad suite"

[[options]]
name = "BAD"
default = "one"
values = ["one"]
help = "bad"
"#,
        )
        .unwrap();

        let err = SuiteRegistry::discover(dir.path()).unwrap_err();
        assert!(err.to_string().contains("invalid option name"));
    }
}
