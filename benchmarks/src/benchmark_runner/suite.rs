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
//! the runner should expose and how selected option values map to SQL
//! replacement variables. Query discovery recursively scans from the suite
//! file's directory for `qNN.benchmark` files. Discovered queries are cached
//! lazily because they are reused by listing and target resolution during a
//! single CLI run.

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};
use std::cell::OnceCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};

const DEFAULT_QUERY_PATTERN: &str = "q{QUERY_ID_PADDED}.benchmark";
const QUERY_ID_TOKEN: &str = "{QUERY_ID}";
const QUERY_ID_PADDED_TOKEN: &str = "{QUERY_ID_PADDED}";
const RESERVED_CLI_OPTION_NAMES: &[&str] = &[
    "batch-size",
    "criterion",
    "debug",
    "help",
    "iterations",
    "mem-pool-type",
    "memory-limit",
    "output",
    "partitions",
    "persist-results",
    "query_id",
    "save-baseline",
    "selector",
    "simulate-latency",
    "sort-spill-reservation-bytes",
    "validate-results",
];
const RESERVED_CLI_SHORT_ALIASES: &[&str] = &["d", "h", "i", "n", "o", "p", "s"];

/// Discovered query benchmark file belonging to one suite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuiteQuery {
    /// Numeric query id parsed from the configured query file pattern.
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
    /// Suite selector inferred from the `.suite` file name, such as `tpch`.
    #[serde(skip)]
    pub name: String,
    /// Human-readable suite description shown by `list` and `info`.
    pub description: String,
    /// Query file naming pattern. Defaults to `q{QUERY_ID_PADDED}.benchmark`.
    #[serde(default = "default_query_pattern")]
    pub query_pattern: String,
    /// Path-like SQL replacement values resolved relative to the suite file.
    #[serde(default)]
    pub path_replacements: BTreeMap<String, PathBuf>,
    /// Suite-specific CLI options and their SQL replacement variables.
    #[serde(default)]
    pub options: Vec<SuiteOption>,
    /// Example commands shown by `list` and `info`.
    #[serde(default)]
    pub examples: Vec<SuiteExample>,
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

/// Raw TOML shape accepted from a `.suite` file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SuiteFileConfig {
    /// Human-readable suite description shown by list and info.
    description: String,
    /// Query file naming pattern used for discovery and query-id lookup.
    #[serde(default = "default_query_pattern")]
    query_pattern: String,
    /// SQL replacement paths resolved relative to the suite file.
    #[serde(default)]
    path_replacements: BTreeMap<String, PathBuf>,
    /// Suite-defined command-line options and their replacement variables.
    #[serde(default)]
    options: Vec<SuiteOption>,
    /// Example commands shown by suite info output.
    #[serde(default)]
    examples: Vec<SuiteExample>,
}

/// Clones suite metadata while intentionally resetting the lazy query cache.
impl Clone for SuiteConfig {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            description: self.description.clone(),
            query_pattern: self.query_pattern.clone(),
            path_replacements: self.path_replacements.clone(),
            options: self.options.clone(),
            examples: self.examples.clone(),
            suite_path: self.suite_path.clone(),
            suite_dir: self.suite_dir.clone(),
            query_cache: OnceCell::new(),
        }
    }
}

/// Suite-defined command-line option and its SQL replacement behavior.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteOption {
    /// Long CLI option name, without the leading `--`.
    pub name: String,
    /// Optional short CLI alias, without the leading `-`.
    #[serde(default)]
    pub short: Option<String>,
    /// SQL replacement variable set when this option is selected.
    pub env: String,
    /// Default option value used when the CLI does not provide one.
    pub default: String,
    /// Allowed option values.
    pub values: Vec<String>,
    /// Help text shown in command output.
    pub help: String,
}

/// Example command configured in a `.suite` file.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteExample {
    /// Example command to display.
    pub command: String,
    /// Explanation of what the example command runs.
    pub description: String,
}

/// Discovered suite metadata, sorted by suite name.
#[derive(Debug, Clone)]
pub struct SuiteRegistry {
    suites: Vec<SuiteConfig>,
}

/// Loads, validates, and resolves metadata for one suite file.
impl SuiteConfig {
    /// Loads, parses, and validates one `.suite` file.
    ///
    /// The suite file path and containing directory are stored on the returned
    /// config so later discovery and path replacements can be resolved relative
    /// to the suite file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let suite_path = path.as_ref().to_path_buf();
        let suite_name = suite_name_from_path(&suite_path)?;
        let contents = fs::read_to_string(&suite_path).map_err(|e| {
            DataFusionError::External(
                format!("failed to read suite file {}: {e}", suite_path.display()).into(),
            )
        })?;
        let raw: SuiteFileConfig = toml::from_str(&contents).map_err(|e| {
            DataFusionError::External(
                format!("failed to parse suite file {}: {e}", suite_path.display())
                    .into(),
            )
        })?;

        let suite_dir = suite_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let suite = Self {
            name: suite_name,
            description: raw.description,
            query_pattern: raw.query_pattern,
            path_replacements: raw.path_replacements,
            options: raw.options,
            examples: raw.examples,
            suite_path,
            suite_dir,
            query_cache: OnceCell::new(),
        };

        suite.validate()?;

        Ok(suite)
    }

    /// Returns a configured suite option by command-line name.
    pub fn option(&self, name: &str) -> Option<&SuiteOption> {
        self.options.iter().find(|option| option.name == name)
    }

    /// Returns the directory recursively searched for query benchmark files.
    pub fn query_search_root(&self) -> &Path {
        &self.suite_dir
    }

    /// Discovers and caches the suite's benchmark query files.
    ///
    /// Query files are found by recursively scanning from the suite directory,
    /// accepting only files matching this suite's `query_pattern`, sorting by
    /// numeric query id, and rejecting duplicate ids.
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
        let query_pattern = QueryPattern::new(&self.query_pattern)?;

        self.scan_query_dir(self.query_search_root(), &query_pattern, &mut queries)?;
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
    fn scan_query_dir(
        &self,
        dir: &Path,
        query_pattern: &QueryPattern,
        queries: &mut Vec<SuiteQuery>,
    ) -> Result<()> {
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
                self.scan_query_dir(&path, query_pattern, queries)?;
                continue;
            }

            if path
                .extension()
                .is_none_or(|extension| extension != "benchmark")
            {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().into_owned();
            if let Some(id) = query_pattern.parse_file_name(&file_name) {
                queries.push(SuiteQuery {
                    id,
                    file_name,
                    path,
                });
            }
        }

        Ok(())
    }

    /// Merges supplied suite option values with defaults and validates that all
    /// values are allowed for this suite.
    pub fn resolve_option_values(
        &self,
        supplied: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        for key in supplied.keys() {
            if self.option(key).is_none() {
                return Err(DataFusionError::Configuration(format!(
                    "unknown option '--{}' for suite '{}'",
                    key, self.name
                )));
            }
        }

        let mut values = BTreeMap::new();
        for option in &self.options {
            let value = supplied
                .get(&option.name)
                .map(String::as_str)
                .unwrap_or(option.default.as_str());

            if !option.values.iter().any(|allowed| allowed == value) {
                return Err(DataFusionError::Configuration(format!(
                    "invalid value '{}' for --{}; expected one of: {}",
                    value,
                    option.name,
                    option.values.join(", ")
                )));
            }

            values.insert(option.name.clone(), value.to_string());
        }

        Ok(values)
    }

    /// Converts selected suite option values and path replacements into the
    /// replacement map passed to `SqlBenchmark`.
    ///
    /// Path replacement values are resolved relative to the suite file unless
    /// they are already absolute.
    pub fn option_replacements(
        &self,
        values: &BTreeMap<String, String>,
    ) -> HashMap<String, String> {
        let mut replacements =
            HashMap::with_capacity(self.path_replacements.len() + self.options.len());

        for (key, value) in &self.path_replacements {
            let value = if value.is_absolute() {
                value.to_string_lossy().into_owned()
            } else {
                self.suite_dir.join(value).to_string_lossy().into_owned()
            };

            replacements.insert(key.clone(), value);
        }

        replacements.extend(self.options.iter().filter_map(|option| {
            values
                .get(&option.name)
                .map(|value| (option.env.clone(), value.clone()))
        }));

        replacements
    }

    /// Validates suite metadata that cannot be enforced by TOML deserialization.
    fn validate(&self) -> Result<()> {
        self.validate_suite_fields()?;
        self.validate_path_replacements()?;
        self.validate_options()
    }

    /// Validates required suite-level fields.
    fn validate_suite_fields(&self) -> Result<()> {
        QueryPattern::new(&self.query_pattern)?;

        Ok(())
    }

    /// Validates static path replacement declarations.
    fn validate_path_replacements(&self) -> Result<()> {
        for key in self.path_replacements.keys() {
            if key.trim().is_empty() {
                return Err(DataFusionError::Configuration(
                    "path replacement name cannot be empty".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Validates suite-defined CLI option declarations.
    fn validate_options(&self) -> Result<()> {
        let mut option_names = HashSet::new();
        let mut short_aliases = HashSet::new();

        for option in &self.options {
            if !option_names.insert(option.name.as_str()) {
                return Err(DataFusionError::Configuration(format!(
                    "duplicate option name '{}'",
                    option.name
                )));
            }

            option.validate()?;

            if let Some(short) = &option.short
                && !short_aliases.insert(short.as_str())
            {
                return Err(DataFusionError::Configuration(format!(
                    "duplicate short alias '{short}'"
                )));
            }
        }

        Ok(())
    }
}

/// Parsed suite query filename pattern used for discovery and lookup.
#[derive(Debug)]
struct QueryPattern<'a> {
    /// File name prefix before the query-id token.
    prefix: &'a str,
    /// File name suffix after the query-id token.
    suffix: &'a str,
    /// Whether file names use padded or unpadded query ids.
    query_id_format: QueryIdFormat,
}

/// Query id formatting mode selected by the suite query pattern token.
#[derive(Debug, Clone, Copy)]
enum QueryIdFormat {
    /// Formats query ids as at least two digits.
    Padded,
    /// Formats query ids without padding.
    Unpadded,
}

/// Parses query filename patterns and applies them in both directions.
impl<'a> QueryPattern<'a> {
    /// Creates a validated pattern from one `.suite` `query_pattern` value.
    fn new(pattern: &'a str) -> Result<Self> {
        if pattern.trim().is_empty() {
            return Err(DataFusionError::Configuration(
                "query_pattern cannot be empty".to_string(),
            ));
        }

        if !pattern.ends_with(".benchmark") {
            return Err(DataFusionError::Configuration(
                "query_pattern must name .benchmark files".to_string(),
            ));
        }

        let padded_count = pattern.matches(QUERY_ID_PADDED_TOKEN).count();
        let unpadded_count = pattern.matches(QUERY_ID_TOKEN).count();

        if padded_count + unpadded_count != 1 {
            return Err(DataFusionError::Configuration(format!(
                "query_pattern must contain exactly one {QUERY_ID_TOKEN} or {QUERY_ID_PADDED_TOKEN} token"
            )));
        }

        if padded_count == 1 {
            let (prefix, suffix) =
                pattern.split_once(QUERY_ID_PADDED_TOKEN).ok_or_else(|| {
                    DataFusionError::Configuration(format!(
                        "query_pattern must contain {QUERY_ID_PADDED_TOKEN}"
                    ))
                })?;
            Ok(Self {
                prefix,
                suffix,
                query_id_format: QueryIdFormat::Padded,
            })
        } else {
            let (prefix, suffix) =
                pattern.split_once(QUERY_ID_TOKEN).ok_or_else(|| {
                    DataFusionError::Configuration(format!(
                        "query_pattern must contain {QUERY_ID_TOKEN}"
                    ))
                })?;
            Ok(Self {
                prefix,
                suffix,
                query_id_format: QueryIdFormat::Unpadded,
            })
        }
    }

    /// Extracts a query id from a file name when it matches this pattern.
    fn parse_file_name(&self, file_name: &str) -> Option<usize> {
        let query_id = file_name
            .strip_prefix(self.prefix)?
            .strip_suffix(self.suffix)?;

        if query_id.is_empty() || !query_id.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }

        if matches!(self.query_id_format, QueryIdFormat::Padded) && query_id.len() < 2 {
            return None;
        }

        query_id.parse().ok()
    }
}

/// Returns the default padded `qNN.benchmark` query filename pattern.
fn default_query_pattern() -> String {
    DEFAULT_QUERY_PATTERN.to_string()
}

/// Infers and validates a suite selector from a `.suite` file path.
fn suite_name_from_path(path: &Path) -> Result<String> {
    let file_stem = path.file_stem().ok_or_else(|| {
        DataFusionError::Configuration(format!(
            "suite file {} has no file name stem",
            path.display()
        ))
    })?;
    let name = file_stem.to_str().ok_or_else(|| {
        DataFusionError::Configuration(format!(
            "suite file name {} is not valid UTF-8",
            path.display()
        ))
    })?;

    if !is_valid_suite_name(name) {
        return Err(DataFusionError::Configuration(format!(
            "invalid suite file name '{name}'; expected lowercase ASCII letters, digits, hyphens, and underscores"
        )));
    }

    Ok(name.to_string())
}

/// Validates suite-defined CLI option metadata.
impl SuiteOption {
    /// Validates one suite-defined CLI option.
    fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            return Err(DataFusionError::Configuration(
                "option name cannot be empty".to_string(),
            ));
        }

        if !is_valid_cli_option_name(&self.name) {
            return Err(DataFusionError::Configuration(format!(
                "invalid CLI option name '{}'; expected lowercase ASCII letters, digits, and hyphens",
                self.name
            )));
        }

        if RESERVED_CLI_OPTION_NAMES.contains(&self.name.as_str()) {
            return Err(DataFusionError::Configuration(format!(
                "reserved CLI option name '{}'",
                self.name
            )));
        }

        self.validate_short_alias()?;

        if self.env.trim().is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "env for option '{}' cannot be empty",
                self.name
            )));
        }

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

    /// Validates the optional short alias for one suite-defined CLI option.
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

        if short.chars().count() == 1
            && RESERVED_CLI_SHORT_ALIASES.contains(&short.as_str())
        {
            return Err(DataFusionError::Configuration(format!(
                "reserved short alias '{short}' for option '{}'",
                self.name
            )));
        }

        Ok(())
    }
}

/// Discovers suite files and provides lookup by selector name.
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

    /// Returns a suite by its command-line selector name.
    pub fn get(&self, name: &str) -> Option<&SuiteConfig> {
        self.suites.iter().find(|suite| suite.name == name)
    }

    /// Returns discovered suites in display order.
    pub fn suites(&self) -> &[SuiteConfig] {
        &self.suites
    }
}

/// Reads directory entries and wraps filesystem errors with runner-specific context.
fn read_dir_entries(path: &Path, description: &str) -> Result<Vec<DirEntry>> {
    let entries = fs::read_dir(path).map_err(|e| {
        DataFusionError::External(
            format!("failed to read {description} {}: {e}", path.display()).into(),
        )
    })?;

    collect_dir_entries(path, description, entries)
}

/// Collects directory entries while preserving contextual filesystem errors.
fn collect_dir_entries(
    path: &Path,
    description: &str,
    entries: impl IntoIterator<Item = std::io::Result<DirEntry>>,
) -> Result<Vec<DirEntry>> {
    entries
        .into_iter()
        .map(|entry| {
            entry.map_err(|e| {
                DataFusionError::External(
                    format!("failed to read {description} entry {}: {e}", path.display())
                        .into(),
                )
            })
        })
        .collect()
}

/// Checks the restricted option-name syntax accepted for suite-defined CLI flags and aliases.
fn is_valid_cli_option_name(name: &str) -> bool {
    let mut previous_was_hyphen = false;
    for c in name.chars() {
        let is_valid = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-';
        if !is_valid || (c == '-' && previous_was_hyphen) {
            return false;
        }
        previous_was_hyphen = c == '-';
    }

    !name.starts_with('-') && !name.ends_with('-')
}

/// Checks the restricted filename syntax accepted for suite selectors.
fn is_valid_suite_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    }

    fn suite_option_with_short(short: Option<&str>) -> SuiteOption {
        SuiteOption {
            name: "format".to_string(),
            short: short.map(str::to_string),
            env: "DATA_FORMAT".to_string(),
            default: "parquet".to_string(),
            values: vec!["parquet".to_string()],
            help: "Selects the data format.".to_string(),
        }
    }

    fn suite_with_format_option() -> SuiteConfig {
        SuiteConfig {
            name: "example".to_string(),
            description: "Example benchmark suite".to_string(),
            query_pattern: DEFAULT_QUERY_PATTERN.to_string(),
            path_replacements: BTreeMap::new(),
            options: vec![suite_option_with_short(None)],
            examples: vec![],
            suite_path: PathBuf::from("example.suite"),
            suite_dir: PathBuf::from("."),
            query_cache: OnceCell::new(),
        }
    }

    fn suite_file_error_message(suite_contents: impl AsRef<str>) -> String {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("invalid.suite");
        fs::write(&suite_path, suite_contents.as_ref()).unwrap();

        SuiteConfig::from_file(&suite_path).unwrap_err().to_string()
    }

    fn assert_suite_file_error_contains(
        suite_contents: impl AsRef<str>,
        expected_message: &str,
    ) {
        let message = suite_file_error_message(suite_contents);

        assert!(
            message.contains(expected_message),
            "expected error containing {expected_message:?}, got {message:?}"
        );
    }

    #[test]
    fn read_dir_entries_reports_directory_open_errors() {
        let tempdir = tempfile::tempdir().unwrap();
        let missing_dir = tempdir.path().join("missing");

        let err = read_dir_entries(&missing_dir, "test directory")
            .expect_err("read should fail");
        let message = err.to_string();

        assert!(
            message.contains("failed to read test directory"),
            "{message}"
        );
        assert!(
            message.contains(&missing_dir.display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn read_dir_entries_reports_directory_entry_errors() {
        let tempdir = tempfile::tempdir().unwrap();
        let err = collect_dir_entries(
            tempdir.path(),
            "test directory",
            std::iter::once(Err::<DirEntry, _>(io::Error::other("entry failed"))),
        )
        .expect_err("read should fail");
        let message = err.to_string();

        assert!(
            message.contains("failed to read test directory entry"),
            "{message}"
        );
        assert!(
            message.contains(&tempdir.path().display().to_string()),
            "{message}"
        );
        assert!(message.contains("entry failed"), "{message}");
    }

    #[test]
    fn from_file_reports_missing_file_errors() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("missing.suite");

        let err = SuiteConfig::from_file(&suite_path).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("failed to read suite file"), "{message}");
        assert!(
            message.contains(&suite_path.display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn from_file_reports_toml_parse_errors() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("invalid.suite");
        fs::write(&suite_path, "name = [").unwrap();

        let err = SuiteConfig::from_file(&suite_path).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("failed to parse suite file"), "{message}");
        assert!(
            message.contains(&suite_path.display().to_string()),
            "{message}"
        );
    }

    #[test]
    fn query_pattern_new_rejects_missing_query_id_token() {
        let err = QueryPattern::new("query.benchmark").unwrap_err();

        assert!(
            err.to_string()
                .contains("query_pattern must contain exactly one"),
            "{err}"
        );
    }

    #[test]
    fn query_pattern_new_rejects_multiple_query_id_tokens() {
        let err =
            QueryPattern::new("q{QUERY_ID}-{QUERY_ID_PADDED}.benchmark").unwrap_err();

        assert!(
            err.to_string()
                .contains("query_pattern must contain exactly one"),
            "{err}"
        );
    }

    #[test]
    fn resolve_option_values_rejects_unknown_option() {
        let suite = suite_with_format_option();
        let supplied = BTreeMap::from([("unknown".to_string(), "parquet".to_string())]);

        let err = suite.resolve_option_values(&supplied).unwrap_err();
        let message = err.to_string();

        assert!(message.contains("unknown option '--unknown'"), "{message}");
        assert!(message.contains("suite 'example'"), "{message}");
    }

    #[test]
    fn resolve_option_values_rejects_invalid_value() {
        let suite = suite_with_format_option();
        let supplied = BTreeMap::from([("format".to_string(), "csv".to_string())]);

        let err = suite.resolve_option_values(&supplied).unwrap_err();
        let message = err.to_string();

        assert!(
            message.contains("invalid value 'csv' for --format"),
            "{message}"
        );
        assert!(message.contains("expected one of: parquet"), "{message}");
    }

    #[test]
    fn parses_suite_file_with_multiple_options() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet", "csv"]
help = "Selects the data format."

[[options]]
name = "scale-factor"
env = "SCALE_FACTOR"
default = "1"
values = ["1", "10"]
help = "Selects the scale factor."

[[examples]]
command = "cargo run --release --bin benchmark_runner -- run example"
description = "Run the example suite."
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();

        assert_eq!(suite.name, "example");
        assert_eq!(suite.description, "Example benchmark suite");
        assert_eq!(suite.query_pattern, "q{QUERY_ID_PADDED}.benchmark");
        assert_eq!(suite.suite_path, suite_path);
        assert_eq!(suite.suite_dir, tempdir.path());
        assert_eq!(suite.query_search_root(), tempdir.path());
        assert_eq!(suite.options.len(), 2);
        assert_eq!(suite.option("format").unwrap().default, "parquet");
        assert_eq!(suite.option("scale-factor").unwrap().values, ["1", "10"]);
        assert!(suite.option("missing").is_none());
        assert_eq!(suite.examples.len(), 1);
        assert_eq!(
            suite.examples[0].command,
            "cargo run --release --bin benchmark_runner -- run example"
        );
    }

    #[test]
    fn suite_name_is_inferred_from_file_stem() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example_suite.suite");
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();

        assert_eq!(suite.name, "example_suite");
    }

    #[test]
    fn validation_rejects_top_level_suite_name_field() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        fs::write(
            &suite_path,
            r#"
name = "example"
description = "Example benchmark suite"
"#,
        )
        .unwrap();

        let err = SuiteConfig::from_file(&suite_path).unwrap_err();

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn path_replacements_are_resolved_relative_to_suite_dir() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[path_replacements]
DATA_DIR = "../data"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();
        let replacements = suite.option_replacements(&BTreeMap::new());
        let expected_data_dir = tempdir
            .path()
            .join("../data")
            .to_string_lossy()
            .into_owned();

        assert_eq!(
            replacements.get("DATA_DIR").map(String::as_str),
            Some(expected_data_dir.as_str())
        );
    }

    #[test]
    fn suite_file_without_query_pattern_uses_default_pattern() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();

        assert_eq!(suite.query_pattern, "q{QUERY_ID_PADDED}.benchmark");
    }

    #[test]
    fn checked_in_tpch_suite_has_expected_options() {
        let suite =
            SuiteConfig::from_file(manifest_path("sql_benchmarks/tpch/tpch.suite"))
                .unwrap();

        assert_eq!(suite.name, "tpch");
        assert_eq!(
            suite.query_search_root(),
            manifest_path("sql_benchmarks/tpch")
        );
        assert_eq!(suite.option("format").unwrap().env, "TPCH_FILE_TYPE");
        assert_eq!(suite.option("format").unwrap().short.as_deref(), Some("f"));
        assert_eq!(
            suite.option("format").unwrap().values,
            ["parquet", "csv", "mem"]
        );
        assert_eq!(suite.option("scale-factor").unwrap().env, "BENCH_SIZE");
        assert_eq!(suite.option("scale-factor").unwrap().values, ["1", "10"]);
    }

    #[test]
    fn discover_queries_finds_tpch_queries_sorted_by_id() {
        let suite =
            SuiteConfig::from_file(manifest_path("sql_benchmarks/tpch/tpch.suite"))
                .unwrap();

        let queries = suite.discover_queries().unwrap();

        let query_ids = queries.iter().map(|query| query.id).collect::<Vec<_>>();
        assert!(query_ids.starts_with(&[1, 2, 3]));
        assert!(query_ids.ends_with(&[20, 21, 22]));

        let q01 = queries.iter().find(|query| query.id == 1).unwrap();
        assert_eq!(q01.file_name, "q01.benchmark");
        assert_eq!(
            q01.path,
            manifest_path("sql_benchmarks/tpch/benchmarks/q01.benchmark")
        );

        let q15 = queries.iter().find(|query| query.id == 15).unwrap();
        assert_eq!(q15.file_name, "q15.benchmark");

        let q22 = queries.iter().find(|query| query.id == 22).unwrap();
        assert_eq!(q22.file_name, "q22.benchmark");
    }

    #[test]
    fn discover_queries_recursively_from_suite_dir_without_benchmark_dir() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        let nested_dir = tempdir.path().join("nested/queries");
        fs::create_dir_all(&nested_dir).unwrap();
        fs::write(nested_dir.join("q02.benchmark"), "run\nSELECT 2\n").unwrap();
        fs::write(tempdir.path().join("q01.benchmark"), "run\nSELECT 1\n").unwrap();
        fs::write(nested_dir.join("notes.benchmark"), "run\nSELECT 3\n").unwrap();
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();
        let queries = suite.discover_queries().unwrap();

        assert_eq!(
            queries
                .iter()
                .map(|query| (query.id, query.file_name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "q01.benchmark"), (2, "q02.benchmark")]
        );
        assert_eq!(queries[0].path, tempdir.path().join("q01.benchmark"));
        assert_eq!(queries[1].path, nested_dir.join("q02.benchmark"));
    }

    #[test]
    fn discover_queries_uses_unpadded_query_pattern() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        let nested_dir = tempdir.path().join("nested");
        fs::create_dir_all(&nested_dir).unwrap();
        fs::write(tempdir.path().join("query-1.benchmark"), "run\nSELECT 1\n").unwrap();
        fs::write(nested_dir.join("query-12.benchmark"), "run\nSELECT 12\n").unwrap();
        fs::write(tempdir.path().join("q01.benchmark"), "run\nSELECT 99\n").unwrap();
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "query-{QUERY_ID}.benchmark"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();
        let queries = suite.discover_queries().unwrap();

        assert_eq!(
            queries
                .iter()
                .map(|query| (query.id, query.file_name.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "query-1.benchmark"), (12, "query-12.benchmark")]
        );
    }

    #[test]
    fn discover_queries_uses_custom_padded_query_pattern() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        fs::write(tempdir.path().join("case-03.benchmark"), "run\nSELECT 3\n").unwrap();
        fs::write(tempdir.path().join("case-3.benchmark"), "run\nSELECT 99\n").unwrap();
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "case-{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();
        let queries = suite.discover_queries().unwrap();

        assert_eq!(
            queries
                .iter()
                .map(|query| (query.id, query.file_name.as_str()))
                .collect::<Vec<_>>(),
            vec![(3, "case-03.benchmark")]
        );
    }

    #[test]
    fn discover_queries_rejects_duplicate_query_ids() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("example.suite");
        let nested_dir = tempdir.path().join("nested");
        fs::create_dir_all(&nested_dir).unwrap();
        fs::write(tempdir.path().join("q01.benchmark"), "run\nSELECT 1\n").unwrap();
        fs::write(nested_dir.join("q01.benchmark"), "run\nSELECT 1\n").unwrap();
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();
        let err = suite.discover_queries().unwrap_err();

        assert!(err.to_string().contains("duplicate QUERY_ID 1"));
    }

    #[test]
    fn cloning_suite_config_does_not_copy_runtime_caches() {
        let suite =
            SuiteConfig::from_file(manifest_path("sql_benchmarks/tpch/tpch.suite"))
                .unwrap();

        let queries = suite.discover_queries().unwrap();
        assert!(!queries.is_empty());
        assert!(suite.query_cache.get().is_some());

        let cloned = suite.clone();

        assert!(cloned.query_cache.get().is_none());
    }

    #[test]
    fn validation_rejects_duplicate_option_names() {
        assert_suite_file_error_contains(
            r#"
description = "Duplicate options"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Selects the data format."

[[options]]
name = "format"
env = "OTHER_DATA_FORMAT"
default = "csv"
values = ["csv"]
help = "Selects another data format."
"#,
            "duplicate option name",
        );
    }

    #[test]
    fn validation_rejects_default_values_missing_from_values() {
        let message = suite_file_error_message(
            r#"
description = "Missing default"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["csv"]
help = "Selects the data format."
"#,
        );

        assert!(message.contains("default"));
        assert!(message.contains("values"));
    }

    #[test]
    fn validation_rejects_invalid_suite_file_name() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("Invalid.suite");
        fs::write(
            &suite_path,
            r#"
description = "Invalid suite file name"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();

        let err = SuiteConfig::from_file(&suite_path).unwrap_err();
        assert!(err.to_string().contains("invalid suite file name"));
    }

    #[test]
    fn suite_file_name_allows_any_valid_characters() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_path = tempdir.path().join("-example__suite-.suite");
        fs::write(
            &suite_path,
            r#"
description = "Example benchmark suite"
"#,
        )
        .unwrap();

        let suite = SuiteConfig::from_file(&suite_path).unwrap();

        assert_eq!(suite.name, "-example__suite-");
    }

    #[test]
    fn validation_rejects_unknown_top_level_fields() {
        assert_suite_file_error_contains(
            r#"
description = "Example benchmark suite"
unexpected = "value"
"#,
            "unknown field",
        );
    }

    #[test]
    fn validation_rejects_unknown_option_fields() {
        assert_suite_file_error_contains(
            r#"
description = "Example benchmark suite"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Selects the data format."
unexpected = "value"
"#,
            "unknown field",
        );
    }

    #[test]
    fn validation_rejects_query_pattern_without_query_id_token() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid pattern"
query_pattern = "query.benchmark"
"#,
            "query_pattern",
        );
    }

    #[test]
    fn validation_rejects_query_pattern_with_multiple_query_id_tokens() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid pattern"
query_pattern = "q{QUERY_ID}-{QUERY_ID_PADDED}.benchmark"
"#,
            "query_pattern",
        );
    }

    #[test]
    fn validation_rejects_empty_query_pattern() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid pattern"
query_pattern = ""
"#,
            "query_pattern cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_query_pattern_without_benchmark_extension() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid pattern"
query_pattern = "q{QUERY_ID}.sql"
"#,
            "query_pattern must name .benchmark files",
        );
    }

    #[test]
    fn validation_rejects_empty_path_replacement_name() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid path replacement"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[path_replacements]
"" = "../data"
"#,
            "path replacement name cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_empty_option_name() {
        assert_suite_file_error_contains(
            r#"
description = "Empty option name"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = ""
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Selects the data format."
"#,
            "option name cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_invalid_option_name() {
        assert_suite_file_error_contains(
            r#"
description = "Invalid option name"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "Format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Selects the data format."
"#,
            "invalid CLI option name 'Format'",
        );
    }

    #[test]
    fn validation_rejects_empty_short_alias() {
        let option = suite_option_with_short(Some(" "));

        let err = option.validate_short_alias().unwrap_err();

        assert!(
            err.to_string()
                .contains("short alias for option 'format' cannot be empty")
        );
    }

    #[test]
    fn validation_rejects_invalid_short_alias() {
        let option = suite_option_with_short(Some("F"));

        let err = option.validate_short_alias().unwrap_err();
        let message = err.to_string();

        assert!(message.contains("invalid short alias 'F'"), "{message}");
        assert!(message.contains("option 'format'"), "{message}");
    }

    #[test]
    fn validation_rejects_duplicate_short_aliases() {
        assert_suite_file_error_contains(
            r#"
description = "Duplicate short aliases"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
short = "f"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Selects the data format."

[[options]]
name = "filter"
short = "f"
env = "FILTER"
default = "none"
values = ["none"]
help = "Selects the filter."
"#,
            "duplicate short alias 'f'",
        );
    }

    #[test]
    fn validation_rejects_empty_option_env() {
        assert_suite_file_error_contains(
            r#"
description = "Empty option env"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = ""
default = "parquet"
values = ["parquet"]
help = "Selects the data format."
"#,
            "env for option 'format' cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_empty_option_help() {
        assert_suite_file_error_contains(
            r#"
description = "Empty option help"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = ""
"#,
            "help for option 'format' cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_empty_option_values() {
        assert_suite_file_error_contains(
            r#"
description = "Empty option values"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = []
help = "Selects the data format."
"#,
            "values for option 'format' cannot be empty",
        );
    }

    #[test]
    fn validation_rejects_reserved_cli_option_names() {
        assert_suite_file_error_contains(
            r#"
description = "Reserved option name"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "iterations"
env = "ITERATIONS"
default = "1"
values = ["1"]
help = "Conflicts with CommonOpt."
"#,
            "reserved CLI option name",
        );
    }

    #[test]
    fn validation_rejects_new_runner_option_names() {
        for option_name in ["criterion", "save-baseline"] {
            assert_suite_file_error_contains(
                format!(
                    r#"
description = "Reserved runner option"
query_pattern = "q{{QUERY_ID_PADDED}}.benchmark"

[[options]]
name = "{option_name}"
env = "RESERVED"
default = "value"
values = ["value"]
help = "Conflicts with benchmark_runner run options."
"#
                ),
                "reserved CLI option name",
            );
        }
    }

    #[test]
    fn validation_rejects_reserved_short_aliases() {
        for short in ["h", "p", "o", "i", "n", "s", "d"] {
            assert_suite_file_error_contains(
                format!(
                    r#"
description = "Reserved short alias"
query_pattern = "q{{QUERY_ID_PADDED}}.benchmark"

[[options]]
name = "format"
short = "{short}"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet"]
help = "Conflicts with benchmark_runner short options."
"#
                ),
                "reserved short alias",
            );
        }
    }

    #[test]
    fn validation_rejects_duplicate_option_values() {
        assert_suite_file_error_contains(
            r#"
description = "Duplicate option values"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"

[[options]]
name = "format"
env = "DATA_FORMAT"
default = "parquet"
values = ["parquet", "parquet"]
help = "Selects the data format."
"#,
            "duplicate value",
        );
    }

    #[test]
    fn registry_discovery_rejects_duplicate_suite_names() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_one_dir = tempdir.path().join("one");
        let suite_two_dir = tempdir.path().join("two");
        fs::create_dir(&suite_one_dir).unwrap();
        fs::create_dir(&suite_two_dir).unwrap();
        fs::write(
            suite_one_dir.join("duplicate.suite"),
            r#"
description = "First duplicate suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();
        fs::write(
            suite_two_dir.join("duplicate.suite"),
            r#"
description = "Second duplicate suite"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
"#,
        )
        .unwrap();

        let err = SuiteRegistry::discover(tempdir.path()).unwrap_err();
        assert!(err.to_string().contains("duplicate suite name"));
    }

    #[test]
    fn registry_discovery_finds_tpch() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let tpch = registry.get("tpch").unwrap();

        assert_eq!(tpch.name, "tpch");
    }
}
