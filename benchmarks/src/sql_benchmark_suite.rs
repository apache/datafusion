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

//! Metadata parsing and validation for SQL benchmark suites.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use datafusion_common::{DataFusionError, Result};
use serde::Deserialize;

const DEFAULT_QUERY_PATTERN: &str = "q{QUERY_ID_PADDED}.benchmark";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSuite {
    description: String,
    query_pattern: Option<String>,
    #[serde(default)]
    path_replacements: BTreeMap<String, String>,
    #[serde(default)]
    options: Vec<RawSuiteOption>,
    #[serde(default)]
    examples: Vec<SuiteExample>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSuiteOption {
    name: String,
    short: Option<String>,
    env: String,
    default: String,
    values: Option<Vec<String>>,
    help: String,
}

/// Validated metadata for one benchmark suite.
#[derive(Debug, Clone)]
pub struct SuiteMetadata {
    name: String,
    directory: PathBuf,
    description: String,
    query_pattern: String,
    path_replacements: BTreeMap<String, PathBuf>,
    options: Vec<SuiteOption>,
    examples: Vec<SuiteExample>,
    benchmark_count: usize,
}

/// A suite-specific command-line option.
#[derive(Debug, Clone)]
pub struct SuiteOption {
    name: String,
    short: Option<char>,
    env: String,
    default: String,
    values: Option<Vec<String>>,
    help: String,
}

/// An example invocation from a suite metadata file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SuiteExample {
    command: String,
    description: String,
}

/// Global option names unavailable to suite-specific options.
pub struct ReservedOptions<'a> {
    pub long: &'a BTreeSet<String>,
    pub short: &'a BTreeSet<char>,
}

/// Where a resolved option value originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueSource {
    CommandLine,
    Environment,
    Default,
}

/// An option value together with its origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedValue {
    pub value: String,
    pub source: ValueSource,
}

fn metadata_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Configuration(message.into())
}

impl SuiteMetadata {
    /// Loads and validates `<root>/<name>/<name>.suite`.
    pub fn load(root: &Path, name: &str, reserved: &ReservedOptions) -> Result<Self> {
        let directory = root.join(name);
        let metadata_path = directory.join(format!("{name}.suite"));
        let contents = fs::read_to_string(&metadata_path)?;
        let raw: RawSuite = toml::from_str(&contents).map_err(|error| {
            metadata_error(format!("{}: {error}", metadata_path.display()))
        })?;
        Self::from_raw(name, directory, raw, reserved)
    }

    fn from_raw(
        name: &str,
        directory: PathBuf,
        raw: RawSuite,
        reserved: &ReservedOptions,
    ) -> Result<Self> {
        if raw.description.trim().is_empty() {
            return Err(metadata_error("suite description must not be empty"));
        }

        let query_pattern = raw
            .query_pattern
            .clone()
            .unwrap_or_else(|| DEFAULT_QUERY_PATTERN.to_string());

        validate_query_pattern(&query_pattern)?;

        let mut long_names = BTreeSet::new();
        let mut short_names = BTreeSet::new();
        let mut env_names = BTreeSet::new();
        let mut options = Vec::with_capacity(raw.options.len());

        for option in raw.options {
            Self::validate_option(
                reserved,
                &mut long_names,
                &mut short_names,
                &mut env_names,
                &raw.path_replacements,
                &option,
            )?;

            let suite_option = SuiteOption {
                name: option.name,
                short: option.short.as_deref().map(parse_short).transpose()?,
                env: option.env,
                default: option.default,
                values: option.values,
                help: option.help,
            };

            if !suite_option.accepts(&suite_option.default) {
                return Err(metadata_error(format!(
                    "default value '{}' is not accepted by option '{}'",
                    suite_option.default, suite_option.name
                )));
            }

            options.push(suite_option);
        }

        for example in &raw.examples {
            if example.command.trim().is_empty() {
                return Err(metadata_error("example command must not be empty"));
            }
            if example.description.trim().is_empty() {
                return Err(metadata_error("example description must not be empty"));
            }
        }

        let path_replacements = raw
            .path_replacements
            .into_iter()
            .map(|(key, path)| {
                let path = PathBuf::from(path);
                let path = if path.is_relative() {
                    directory.join(path)
                } else {
                    path
                };
                (key, path)
            })
            .collect();
        let benchmark_count = count_benchmarks(&directory)?;

        Ok(Self {
            name: name.to_string(),
            directory,
            description: raw.description,
            query_pattern,
            path_replacements,
            options,
            examples: raw.examples,
            benchmark_count,
        })
    }

    fn validate_option(
        reserved: &ReservedOptions,
        long_names: &mut BTreeSet<String>,
        short_names: &mut BTreeSet<char>,
        env_names: &mut BTreeSet<String>,
        path_replacements: &BTreeMap<String, String>,
        option: &RawSuiteOption,
    ) -> Result<()> {
        if !valid_long_name(&option.name) {
            return Err(metadata_error(format!(
                "invalid option name '{}'",
                option.name
            )));
        }
        if reserved.long.contains(&option.name) || !long_names.insert(option.name.clone())
        {
            return Err(metadata_error(format!(
                "option name '{}' is reserved or duplicated",
                option.name
            )));
        }
        let short = option.short.as_deref().map(parse_short).transpose()?;
        if let Some(short) = short
            && (reserved.short.contains(&short) || !short_names.insert(short))
        {
            return Err(metadata_error(format!(
                "option short name '{short}' is reserved or duplicated"
            )));
        }
        if !env_names.insert(option.env.clone()) {
            return Err(metadata_error(format!(
                "option environment key '{}' is duplicated",
                option.env
            )));
        }
        if path_replacements.contains_key(&option.env) {
            return Err(metadata_error(format!(
                "environment key '{}' is used by both an option and a path replacement",
                option.env
            )));
        }
        if option.help.trim().is_empty() {
            return Err(metadata_error(format!(
                "help for option '{}' must not be empty",
                option.name
            )));
        }

        Ok(())
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn query_pattern(&self) -> &str {
        &self.query_pattern
    }

    pub fn path_replacements(&self) -> &BTreeMap<String, PathBuf> {
        &self.path_replacements
    }

    pub fn options(&self) -> &[SuiteOption] {
        &self.options
    }

    pub fn examples(&self) -> &[SuiteExample] {
        &self.examples
    }

    pub fn benchmark_count(&self) -> usize {
        self.benchmark_count
    }

    /// Formats a query identifier using this suite's query pattern.
    pub fn query_filename(&self, query: &str) -> Result<String> {
        let query = query.strip_prefix(['q', 'Q']).unwrap_or(query);
        let digit_count = query.bytes().take_while(u8::is_ascii_digit).count();

        if digit_count == 0 || !query.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(metadata_error(format!(
                "invalid query identifier '{query}'"
            )));
        }

        let (digits, suffix) = query.split_at(digit_count);
        let replacement = if self.query_pattern.contains("{QUERY_ID_PADDED}") {
            let digits = digits.trim_start_matches('0');
            let digits = if digits.is_empty() { "0" } else { digits };
            format!("{digits:0>2}{suffix}")
        } else {
            query.to_string()
        };

        Ok(self
            .query_pattern
            .replace("{QUERY_ID_PADDED}", &replacement)
            .replace("{QUERY_ID}", &replacement))
    }
}

impl SuiteOption {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn short(&self) -> Option<char> {
        self.short
    }

    pub fn env(&self) -> &str {
        &self.env
    }

    pub fn default(&self) -> &str {
        &self.default
    }

    pub fn values(&self) -> Option<&[String]> {
        self.values.as_deref()
    }

    pub fn help(&self) -> &str {
        &self.help
    }

    /// Whether `value` belongs to this option's configured value set.
    pub fn accepts(&self, value: &str) -> bool {
        self.values.as_ref().is_none_or(|values| {
            values
                .iter()
                .any(|allowed| allowed == value || allowed == "...")
        })
    }
}

impl SuiteExample {
    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Finds and loads suite metadata immediately below `root`, sorted by name.
pub fn discover_suites(
    root: &Path,
    reserved: &ReservedOptions,
) -> Result<Vec<SuiteMetadata>> {
    let mut suites = Vec::new();

    for entry in collect_sorted_entries(fs::read_dir(root)?)? {
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().into_owned();
        let expected = entry.path().join(format!("{name}.suite"));
        let suite_files = collect_sorted_entries(fs::read_dir(entry.path())?)?
            .into_iter()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "suite"))
            .collect::<Vec<_>>();

        if suite_files.is_empty() {
            continue;
        }
        if suite_files.len() != 1 || suite_files[0].path() != expected {
            return Err(metadata_error(format!(
                "suite metadata filename must match directory name '{name}'"
            )));
        }

        suites.push(SuiteMetadata::load(root, &name, reserved)?);
    }

    suites.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(suites)
}

fn valid_long_name(name: &str) -> bool {
    name.bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
        })
}

fn parse_short(short: &str) -> Result<char> {
    let mut chars = short.chars();
    let value = chars.next().filter(char::is_ascii_alphanumeric);

    match (value, chars.next()) {
        (Some(value), None) => Ok(value),
        _ => Err(metadata_error(format!(
            "invalid option short name '{short}': expected one ASCII alphanumeric character"
        ))),
    }
}

fn validate_query_pattern(pattern: &str) -> Result<()> {
    let path = Path::new(pattern);
    if path.is_absolute() {
        return Err(metadata_error("query pattern must not be absolute"));
    }
    if path
        .components()
        .any(|component| component == Component::ParentDir)
    {
        return Err(metadata_error(
            "query pattern must not contain a parent component",
        ));
    }

    let placeholders = pattern.matches("{QUERY_ID}").count()
        + pattern.matches("{QUERY_ID_PADDED}").count();
    if placeholders != 1 {
        return Err(metadata_error(
            "query pattern must contain exactly one query identifier placeholder",
        ));
    }

    Ok(())
}

fn count_benchmarks(directory: &Path) -> Result<usize> {
    let mut count = 0;
    for entry in collect_sorted_entries(fs::read_dir(directory)?)? {
        if entry.file_type()?.is_dir() {
            count += count_benchmarks(&entry.path())?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|ext| ext == "benchmark")
        {
            count += 1;
        }
    }

    Ok(count)
}

fn collect_sorted_entries(
    entries: impl IntoIterator<Item = io::Result<fs::DirEntry>>,
) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = entries.into_iter().collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql_benchmark_runner::default_sql_benchmark_directory;
    use std::collections::BTreeSet;
    use std::fs;
    use std::io;
    use std::path::Path;

    fn reserved() -> ReservedOptions<'static> {
        let long = Box::leak(Box::new(BTreeSet::from([
            "help".to_string(),
            "query".to_string(),
        ])));
        let short = Box::leak(Box::new(BTreeSet::from(['h', 'q'])));
        ReservedOptions { long, short }
    }

    fn write_suite(root: &Path, name: &str, metadata: &str) {
        let directory = root.join(name);
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join(format!("{name}.suite")), metadata).unwrap();
    }

    fn minimal(extra: &str) -> String {
        format!("description = \"Benchmark\"\n{extra}")
    }

    #[test]
    fn loads_complete_suite() {
        let temp = tempfile::tempdir().unwrap();
        write_suite(
            temp.path(),
            "alpha",
            r#"
description = "Alpha benchmark"
query_pattern = "q{QUERY_ID_PADDED}.benchmark"
[path_replacements]
DATA_DIR = "../../data"
[[options]]
name = "format"
short = "f"
env = "ALPHA_FORMAT"
default = "parquet"
values = ["parquet", "csv"]
help = "Select the file format."
[[examples]]
command = "benchmark_runner alpha -q 1 -f csv"
description = "Run query 1 against CSV."
"#,
        );

        let suite = SuiteMetadata::load(temp.path(), "alpha", &reserved()).unwrap();
        assert_eq!(suite.name(), "alpha");
        assert_eq!(suite.description(), "Alpha benchmark");
        assert_eq!(suite.options()[0].short(), Some('f'));
        assert!(suite.options()[0].accepts("csv"));
        assert!(!suite.options()[0].accepts("json"));
        assert_eq!(
            suite.path_replacements()["DATA_DIR"],
            temp.path().join("alpha/../../data")
        );
        assert_eq!(suite.examples().len(), 1);
    }

    #[test]
    fn rejects_unknown_field() {
        let temp = tempfile::tempdir().unwrap();
        write_suite(
            temp.path(),
            "alpha",
            "description = \"Alpha\"\ndescripton = \"bad\"\n",
        );
        let error = SuiteMetadata::load(temp.path(), "alpha", &reserved()).unwrap_err();
        assert!(error.to_string().contains("descripton"));
        assert!(error.to_string().contains("alpha.suite"));
    }

    #[test]
    fn validates_value_sets() {
        let closed = suite_option(Some(vec!["csv", "parquet"]));
        assert!(closed.accepts("csv"));
        assert!(!closed.accepts("json"));
        assert!(suite_option(Some(vec!["1", "10", "..."])).accepts("100"));
        assert!(suite_option(None).accepts("anything"));
    }

    fn suite_option(values: Option<Vec<&str>>) -> SuiteOption {
        SuiteOption {
            name: "format".to_string(),
            short: Some('f'),
            env: "FORMAT".to_string(),
            default: "csv".to_string(),
            values: values.map(|values| values.into_iter().map(str::to_string).collect()),
            help: "Format".to_string(),
        }
    }

    #[test]
    fn rejects_invalid_metadata() {
        let cases = [
            ("empty description", "description = \" \"\n", "description"),
            (
                "invalid long",
                &minimal(
                    "[[options]]\nname = \"Bad_name\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "Bad_name",
            ),
            (
                "long starts hyphen",
                &minimal(
                    "[[options]]\nname = \"-bad\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "-bad",
            ),
            (
                "long reserved",
                &minimal(
                    "[[options]]\nname = \"query\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "query",
            ),
            (
                "short long",
                &minimal(
                    "[[options]]\nname = \"format\"\nshort = \"ff\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "ff",
            ),
            (
                "short invalid",
                &minimal(
                    "[[options]]\nname = \"format\"\nshort = \"-\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "short",
            ),
            (
                "short reserved",
                &minimal(
                    "[[options]]\nname = \"format\"\nshort = \"q\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \"help\"\n",
                ),
                "q",
            ),
            (
                "empty help",
                &minimal(
                    "[[options]]\nname = \"format\"\nenv = \"ENV\"\ndefault = \"x\"\nhelp = \" \"\n",
                ),
                "help",
            ),
            (
                "bad default",
                &minimal(
                    "[[options]]\nname = \"format\"\nenv = \"ENV\"\ndefault = \"json\"\nvalues = [\"csv\"]\nhelp = \"help\"\n",
                ),
                "json",
            ),
            (
                "absolute pattern",
                "description = \"Benchmark\"\nquery_pattern = \"/q{QUERY_ID}.benchmark\"\n",
                "absolute",
            ),
            (
                "parent pattern",
                "description = \"Benchmark\"\nquery_pattern = \"../q{QUERY_ID}.benchmark\"\n",
                "parent",
            ),
            (
                "no placeholder",
                "description = \"Benchmark\"\nquery_pattern = \"q.benchmark\"\n",
                "placeholder",
            ),
            (
                "two placeholders",
                "description = \"Benchmark\"\nquery_pattern = \"{QUERY_ID}-{QUERY_ID_PADDED}.benchmark\"\n",
                "exactly one",
            ),
            (
                "empty example command",
                &minimal("[[examples]]\ncommand = \" \"\ndescription = \"example\"\n"),
                "command",
            ),
            (
                "empty example description",
                &minimal("[[examples]]\ncommand = \"runner\"\ndescription = \" \"\n"),
                "description",
            ),
        ];

        for (name, metadata, expected) in cases {
            let temp = tempfile::tempdir().unwrap();
            write_suite(temp.path(), "alpha", metadata);
            let error =
                SuiteMetadata::load(temp.path(), "alpha", &reserved()).unwrap_err();
            assert!(error.to_string().contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn rejects_duplicate_and_colliding_options() {
        let fields = [("name", "format"), ("short", "f"), ("env", "FORMAT")];
        for (field, value) in fields {
            let temp = tempfile::tempdir().unwrap();
            write_suite(
                temp.path(),
                "alpha",
                &minimal(&format!(
                    r#"
[[options]]
name = "format"
short = "f"
env = "FORMAT"
default = "x"
help = "help"
[[options]]
name = "{name}"
short = "{short}"
env = "{env}"
default = "x"
help = "help"
"#,
                    name = if field == "name" { value } else { "other" },
                    short = if field == "short" { value } else { "o" },
                    env = if field == "env" { value } else { "OTHER" }
                )),
            );
            let error =
                SuiteMetadata::load(temp.path(), "alpha", &reserved()).unwrap_err();
            assert!(error.to_string().contains(value), "{field}: {error}");
        }

        let temp = tempfile::tempdir().unwrap();
        write_suite(
            temp.path(),
            "alpha",
            &minimal(
                "[path_replacements]\nFORMAT = \"data\"\n[[options]]\nname = \"format\"\nenv = \"FORMAT\"\ndefault = \"x\"\nhelp = \"help\"\n",
            ),
        );
        let error = SuiteMetadata::load(temp.path(), "alpha", &reserved()).unwrap_err();
        assert!(error.to_string().contains("FORMAT"));
    }

    #[test]
    fn discovers_sorted_suites_and_counts_benchmarks() {
        let temp = tempfile::tempdir().unwrap();
        write_suite(temp.path(), "zeta", "description = \"Zeta\"\n");
        write_suite(temp.path(), "alpha", "description = \"Alpha\"\n");
        fs::create_dir_all(temp.path().join("alpha/nested")).unwrap();
        fs::write(temp.path().join("alpha/q01.benchmark"), "").unwrap();
        fs::write(temp.path().join("alpha/nested/q02.benchmark"), "").unwrap();
        fs::write(temp.path().join("alpha/ignored.sql"), "").unwrap();
        let suites = discover_suites(temp.path(), &reserved()).unwrap();
        assert_eq!(
            suites.iter().map(SuiteMetadata::name).collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert_eq!(suites[0].benchmark_count(), 2);
    }

    #[test]
    fn checked_in_suites_cover_benchmark_directories() {
        let root = default_sql_benchmark_directory();
        for entry in fs::read_dir(&root).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_dir() {
                continue;
            }
            let directory = entry.path();
            let has_benchmark = count_benchmarks(&directory).unwrap() > 0;
            if has_benchmark {
                let name = entry.file_name().to_string_lossy().into_owned();
                assert!(
                    directory.join(format!("{name}.suite")).is_file(),
                    "benchmark directory {name} is missing {name}.suite"
                );
            }
        }

        let long = BTreeSet::from([
            "batch-size".to_string(),
            "debug".to_string(),
            "iterations".to_string(),
            "output".to_string(),
            "partitions".to_string(),
            "path".to_string(),
            "query".to_string(),
        ]);
        let short = BTreeSet::from(['q', 'i', 'n', 's', 'd', 'p', 'o']);
        let suites = discover_suites(
            &root,
            &ReservedOptions {
                long: &long,
                short: &short,
            },
        )
        .unwrap();
        let by_name = suites
            .iter()
            .map(|suite| (suite.name(), suite))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(
            by_name["imdb"].query_filename("1a").unwrap(),
            "01a.benchmark"
        );
        assert_eq!(
            by_name["imdb"].query_filename("01a").unwrap(),
            "01a.benchmark"
        );
        assert_eq!(by_name["clickbench"].options()[0].name(), "partitioning");
        assert!(
            by_name["tpch"]
                .options()
                .iter()
                .all(|option| option.short() != Some('s'))
        );
    }

    #[test]
    fn rejects_mismatched_suite_filename() {
        let temp = tempfile::tempdir().unwrap();
        fs::create_dir_all(temp.path().join("wrong")).unwrap();
        fs::write(
            temp.path().join("wrong/other.suite"),
            "description = \"Wrong\"",
        )
        .unwrap();

        let error = discover_suites(temp.path(), &reserved()).unwrap_err();
        assert!(error.to_string().contains("wrong"));
    }

    #[test]
    fn propagates_directory_entry_errors() {
        let entries = std::iter::once(Err::<fs::DirEntry, _>(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "entry denied",
        )));

        let error = collect_sorted_entries(entries).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "entry denied");
    }

    #[test]
    fn formats_query_filenames() {
        let temp = tempfile::tempdir().unwrap();
        write_suite(temp.path(), "padded", "description = \"Padded\"\n");
        write_suite(
            temp.path(),
            "plain",
            "description = \"Plain\"\nquery_pattern = \"{QUERY_ID}.benchmark\"\n",
        );
        let padded = SuiteMetadata::load(temp.path(), "padded", &reserved()).unwrap();
        let plain = SuiteMetadata::load(temp.path(), "plain", &reserved()).unwrap();

        assert_eq!(padded.query_filename("7").unwrap(), "q07.benchmark");
        assert_eq!(padded.query_filename("07").unwrap(), "q07.benchmark");
        assert_eq!(
            padded.query_filename("Q1").unwrap(),
            padded.query_filename("q1").unwrap()
        );
        assert_eq!(
            plain.query_filename("Q01a").unwrap(),
            plain.query_filename("q01a").unwrap()
        );
        assert_eq!(
            padded.query_filename("184467440737095516160").unwrap(),
            "q184467440737095516160.benchmark"
        );
        assert_eq!(plain.query_filename("01a").unwrap(), "01a.benchmark");
        assert!(plain.query_filename("abc").is_err());
        assert!(plain.query_filename("1-a").is_err());
    }
}
