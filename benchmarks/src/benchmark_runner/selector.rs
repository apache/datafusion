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

//! Benchmark target resolution.
//!
//! Selectors name a suite, such as `tpch`. Optional query IDs are normalized
//! so values like `1`, `01`, and `001` resolve through the suite's query file
//! pattern.

use crate::benchmark_runner::suite::{
    SuiteConfig, SuiteExample, SuiteOption, SuiteRegistry,
};
use datafusion_common::{DataFusionError, Result};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

/// Concrete benchmark file selected by suite and optional query id.
#[derive(Debug, Clone)]
pub struct ResolvedBenchmark {
    /// Benchmark file selected for inspection or execution.
    pub path: PathBuf,
    /// Numeric query id when it can be inferred from a `qNN.benchmark` file.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the stacked run command branch")
    )]
    pub query_id: Option<usize>,
    /// Human-readable benchmark label used in run output and JSON.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the stacked run command branch")
    )]
    pub label: String,
}

/// Suite metadata needed after selector resolution.
///
/// This intentionally carries only display and suite-option metadata. It does
/// not include the full suite config, filesystem paths, or discovery caches.
#[derive(Debug, Clone)]
pub struct ResolvedSuite {
    /// Suite selector used on the command line.
    pub name: String,
    /// Human-readable suite description.
    pub description: String,
    /// Suite-specific CLI options shown by `info`.
    pub options: Vec<SuiteOption>,
    /// Example commands shown by `info`.
    pub examples: Vec<SuiteExample>,
}

/// Copies display metadata from a suite config into a resolved suite value.
impl From<&SuiteConfig> for ResolvedSuite {
    fn from(suite: &SuiteConfig) -> Self {
        Self {
            name: suite.name.clone(),
            description: suite.description.clone(),
            options: suite.options.clone(),
            examples: suite.examples.clone(),
        }
    }
}

/// Fully resolved SQL benchmark target ready for inspection or execution.
#[derive(Debug, Clone)]
pub struct ResolvedBenchmarkTarget {
    /// Containing suite metadata, when the target came from or is inside a
    /// discovered suite.
    pub suite: Option<ResolvedSuite>,
    /// Final suite option values after applying defaults and CLI overrides.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the stacked run command branch")
    )]
    pub option_values: BTreeMap<String, String>,
    /// SQL replacement values passed to `SqlBenchmark`.
    pub replacement_values: HashMap<String, String>,
    /// Concrete benchmark files selected for inspection or execution.
    pub benchmarks: Vec<ResolvedBenchmark>,
}

/// Resolves suite selectors, query ids, and suite option values.
impl ResolvedBenchmarkTarget {
    /// Resolves a user selector into the concrete benchmark files to inspect or run.
    ///
    /// Suite selectors apply validated suite options and optional query-id
    /// filtering.
    pub fn resolve(
        registry: &SuiteRegistry,
        selector: &str,
        query_id: Option<&str>,
        suite_options: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let suite = registry.get(selector).ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "unknown benchmark suite '{selector}'"
            ))
        })?;
        let option_values = suite.resolve_option_values(suite_options)?;
        let replacement_values = suite.option_replacements(&option_values);
        let queries = suite.discover_queries()?;
        let benchmarks = if let Some(query_id) = query_id {
            let id = parse_query_id(query_id)?;
            let query = queries
                .into_iter()
                .find(|query| query.id == id)
                .ok_or_else(|| {
                    DataFusionError::Configuration(format!(
                        "suite '{}' has no QUERY_ID {}",
                        suite.name, id
                    ))
                })?;
            vec![ResolvedBenchmark {
                path: query.path,
                query_id: Some(id),
                label: format!("{}/q{id:02}", suite.name),
            }]
        } else {
            queries
                .into_iter()
                .map(|query| ResolvedBenchmark {
                    label: format!("{}/q{:02}", suite.name, query.id),
                    path: query.path,
                    query_id: Some(query.id),
                })
                .collect()
        };

        if benchmarks.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "suite '{}' has no benchmark files",
                suite.name
            )));
        }

        Ok(Self {
            suite: Some(ResolvedSuite::from(suite)),
            option_values,
            replacement_values,
            benchmarks,
        })
    }

    /// Returns the single resolved benchmark required by parse-only inspection
    /// commands.
    pub fn single_benchmark_for_inspection(&self) -> Result<&ResolvedBenchmark> {
        match self.benchmarks.as_slice() {
            [benchmark] => Ok(benchmark),
            _ => Err(DataFusionError::Configuration(
                "query requires exactly one benchmark; provide QUERY_ID".to_string(),
            )),
        }
    }
}

/// Parses a user-provided query id while accepting zero-padded forms.
fn parse_query_id(input: &str) -> Result<usize> {
    let input = input.trim();

    input.parse::<usize>().map_err(|_| {
        DataFusionError::Configuration(
            "QUERY_ID must be a numeric query id such as 1, 01, or 15".to_string(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark_runner::suite::SuiteRegistry;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    }

    fn registry() -> SuiteRegistry {
        SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap()
    }

    #[test]
    fn parses_query_ids() {
        assert_eq!(parse_query_id("1").unwrap(), 1);
        assert_eq!(parse_query_id("01").unwrap(), 1);
        assert_eq!(parse_query_id("001").unwrap(), 1);
        assert_eq!(parse_query_id("15").unwrap(), 15);

        let err = parse_query_id("").unwrap_err().to_string();
        assert!(err.contains("QUERY_ID"));

        let err = parse_query_id("q15").unwrap_err().to_string();
        assert!(err.contains("QUERY_ID"));

        assert_eq!(parse_query_id("0").unwrap(), 0);
        assert_eq!(parse_query_id("00").unwrap(), 0);
        assert_eq!(parse_query_id("000").unwrap(), 0);
    }

    #[test]
    fn resolves_tpch_query_id_to_q15() {
        let registry = registry();
        let resolved = ResolvedBenchmarkTarget::resolve(
            &registry,
            "tpch",
            Some("15"),
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(resolved.benchmarks.len(), 1);
        assert!(resolved.benchmarks[0].path.ends_with("q15.benchmark"));
        assert_eq!(resolved.benchmarks[0].query_id, Some(15));
        assert_eq!(resolved.benchmarks[0].label, "tpch/q15");
    }

    #[test]
    fn selected_suite_options_override_defaults() {
        let registry = registry();
        let mut options = BTreeMap::new();
        options.insert("format".to_string(), "csv".to_string());
        options.insert("scale-factor".to_string(), "10".to_string());

        let resolved =
            ResolvedBenchmarkTarget::resolve(&registry, "tpch", Some("1"), &options)
                .unwrap();

        assert_eq!(
            resolved.replacement_values.get("TPCH_FILE_TYPE").unwrap(),
            "csv"
        );
        assert_eq!(resolved.replacement_values.get("BENCH_SIZE").unwrap(), "10");
        assert_eq!(
            resolved.option_values.get("format").map(String::as_str),
            Some("csv")
        );
    }

    #[test]
    fn suite_without_query_id_resolves_all_queries_in_numeric_order() {
        let registry = registry();
        let resolved =
            ResolvedBenchmarkTarget::resolve(&registry, "tpch", None, &BTreeMap::new())
                .unwrap();

        let ids = resolved
            .benchmarks
            .iter()
            .map(|benchmark| benchmark.query_id.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids.first(), Some(&1));
        assert_eq!(ids.last(), Some(&22));
        assert_eq!(
            resolved.replacement_values.get("TPCH_FILE_TYPE").unwrap(),
            "parquet"
        );
        assert_eq!(resolved.replacement_values.get("BENCH_SIZE").unwrap(), "1");
    }

    #[test]
    fn query_id_resolution_uses_suite_query_pattern() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_dir = tempdir.path().join("custom");
        std::fs::create_dir(&suite_dir).unwrap();
        std::fs::write(suite_dir.join("query-7.benchmark"), "run\nSELECT 7\n").unwrap();
        std::fs::write(
            suite_dir.join("custom.suite"),
            r#"
description = "Custom query pattern"
query_pattern = "query-{QUERY_ID}.benchmark"
"#,
        )
        .unwrap();
        let registry = SuiteRegistry::discover(tempdir.path()).unwrap();

        let resolved = ResolvedBenchmarkTarget::resolve(
            &registry,
            "custom",
            Some("7"),
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(resolved.benchmarks.len(), 1);
        assert_eq!(resolved.benchmarks[0].query_id, Some(7));
        assert!(resolved.benchmarks[0].path.ends_with("query-7.benchmark"));
    }

    #[test]
    fn query_id_resolution_uses_discovered_numeric_id() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_dir = tempdir.path().join("custom");
        std::fs::create_dir(&suite_dir).unwrap();
        std::fs::write(suite_dir.join("q001.benchmark"), "run\nSELECT 1\n").unwrap();
        std::fs::write(
            suite_dir.join("custom.suite"),
            r#"
description = "Custom padded query id"
"#,
        )
        .unwrap();
        let registry = SuiteRegistry::discover(tempdir.path()).unwrap();

        let resolved = ResolvedBenchmarkTarget::resolve(
            &registry,
            "custom",
            Some("1"),
            &BTreeMap::new(),
        )
        .unwrap();

        assert_eq!(resolved.benchmarks.len(), 1);
        assert_eq!(resolved.benchmarks[0].query_id, Some(1));
        assert!(resolved.benchmarks[0].path.ends_with("q001.benchmark"));
    }

    #[test]
    fn inspection_requires_single_benchmark() {
        let registry = registry();
        let resolved =
            ResolvedBenchmarkTarget::resolve(&registry, "tpch", None, &BTreeMap::new())
                .unwrap();

        let err = resolved.single_benchmark_for_inspection().unwrap_err();
        assert!(err.to_string().contains("QUERY_ID"));
    }

    #[test]
    fn benchmark_file_path_is_rejected() {
        let registry = registry();
        let path = manifest_path("sql_benchmarks/tpch/benchmarks/q15.benchmark");
        let err = ResolvedBenchmarkTarget::resolve(
            &registry,
            path.to_str().unwrap(),
            None,
            &BTreeMap::new(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("unknown benchmark suite"));
    }
}
