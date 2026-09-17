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

//! Shared SQL benchmark runner used by `benchmark_runner` and the Criterion
//! SQL benchmark harness.

use crate::sql_benchmark::SqlBenchmark;
use crate::util::{CommonOpt, print_memory_stats};
use clap::Parser;
use criterion::{Criterion, SamplingMode};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, exec_datafusion_err};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;

const CRITERION_MAX_DIRECTORY_NAME_LEN: usize = 64;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BenchmarkFilter {
    pub name: Option<String>,
    pub subgroup: Option<String>,
    pub query: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SqlRunConfig {
    pub common: CommonOpt,
    pub filter: BenchmarkFilter,
    pub replacements: HashMap<String, String>,
    pub query_filename: Option<String>,
    pub persist_results: bool,
    pub validate_results: bool,
    pub output: Option<PathBuf>,
}

#[derive(Debug, Parser)]
#[command(ignore_errors = true)]
struct CriterionHarnessEnv {
    #[command(flatten)]
    options: CommonOpt,

    #[arg(
        env = "BENCH_PERSIST_RESULTS",
        long = "persist_results",
        default_value = "false",
        action = clap::ArgAction::SetTrue
    )]
    persist_results: bool,

    #[arg(
        env = "BENCH_VALIDATE",
        long = "validate_results",
        default_value = "false",
        action = clap::ArgAction::SetTrue
    )]
    validate: bool,

    #[arg(env = "BENCH_NAME")]
    name: Option<String>,

    #[arg(env = "BENCH_SUBGROUP")]
    subgroup: Option<String>,

    #[arg(env = "BENCH_QUERY")]
    query: Option<String>,

    #[arg(env = "BENCH_NAMESPACE")]
    criterion_namespace: Option<String>,
}

/// Builds the direct Criterion harness configuration from its `BENCH_*`
/// environment variables.
pub fn criterion_harness_config_from_env() -> (SqlRunConfig, Option<String>) {
    let args = CriterionHarnessEnv::parse();
    let config = SqlRunConfig {
        common: args.options,
        filter: BenchmarkFilter {
            name: args.name,
            subgroup: args.subgroup,
            query: args.query,
        },
        replacements: default_criterion_replacements(),
        query_filename: None,
        persist_results: args.persist_results,
        validate_results: args.validate,
        output: None,
    };

    (config, args.criterion_namespace)
}

/// Runs the selected SQL benchmarks through a caller-provided Criterion instance.
pub fn run_criterion_benchmarks_impl(
    benchmark_dir: &Path,
    config: &SqlRunConfig,
    criterion: &mut Criterion,
) -> Result<()> {
    run_criterion_benchmarks_impl_with_namespace(benchmark_dir, config, None, criterion)
}

/// Runs the selected SQL benchmarks through a caller-provided Criterion instance,
/// optionally appending a safe invocation namespace to each benchmark group.
pub fn run_criterion_benchmarks_impl_with_namespace(
    benchmark_dir: &Path,
    config: &SqlRunConfig,
    namespace: Option<&str>,
    criterion: &mut Criterion,
) -> Result<()> {
    validate_criterion_namespace(namespace)?;

    let rt = make_tokio_runtime()?;
    let listing_ctx = make_ctx(&config.common)?;
    let all_benchmarks = rt.block_on(load_benchmark_definitions_for_query(
        &config.filter,
        &listing_ctx,
        benchmark_dir,
        &config.replacements,
        config.query_filename.as_deref(),
    ))?;
    let selected = filter_benchmarks(&config.filter, all_benchmarks.clone());

    ensure_selection(&config.filter, &all_benchmarks, &selected)?;

    let mut named_benchmarks = Vec::with_capacity(selected.len());
    for (group_name, benchmarks) in selected {
        named_benchmarks
            .push((criterion_group_name(&group_name, namespace)?, benchmarks));
    }

    for (group_name, benchmarks) in named_benchmarks {
        let mut group = criterion.benchmark_group(group_name);

        group.sample_size(10);
        group.sampling_mode(SamplingMode::Flat);

        for mut benchmark in benchmarks {
            let ctx = make_ctx(&config.common)?;
            let result =
                run_criterion_benchmark(&rt, &ctx, &mut benchmark, config, &mut group);
            let cleanup_result = rt.block_on(benchmark.cleanup(&ctx));

            finish_benchmark(result, cleanup_result)?;
        }

        group.finish();
    }

    Ok(())
}

fn validate_criterion_namespace(namespace: Option<&str>) -> Result<()> {
    let Some(namespace) = namespace else {
        return Ok(());
    };

    if namespace.is_empty()
        || !namespace.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || "_-".contains(character)
        })
    {
        return Err(exec_datafusion_err!(
            "criterion namespace must be nonempty and contain only lowercase ASCII letters, digits, '_', or '-'"
        ));
    }

    Ok(())
}

fn criterion_group_name(group_name: &str, namespace: Option<&str>) -> Result<String> {
    validate_criterion_namespace(namespace)?;

    let Some(namespace) = namespace else {
        return Ok(group_name.to_string());
    };

    let group_name = format!("{group_name}__{namespace}");
    if group_name.len() > CRITERION_MAX_DIRECTORY_NAME_LEN {
        return Err(exec_datafusion_err!(
            "criterion group with namespace must not exceed {CRITERION_MAX_DIRECTORY_NAME_LEN} bytes"
        ));
    }

    Ok(group_name)
}

/// Runs one benchmark case inside Criterion and converts benchmark panics to errors.
fn run_criterion_benchmark(
    rt: &Runtime,
    ctx: &SessionContext,
    benchmark: &mut SqlBenchmark,
    config: &SqlRunConfig,
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
) -> Result<()> {
    rt.block_on(prepare_benchmark(ctx, benchmark, config))?;

    let name = criterion_function_name(benchmark);
    let result = catch_unwind(AssertUnwindSafe(|| {
        group.bench_function(name.clone(), |b| {
            b.iter(|| {
                let _ = rt.block_on(async {
                    benchmark.run(ctx, false).await.unwrap_or_else(|err| {
                        panic!("Failed to run benchmark {name}: {err:?}")
                    })
                });
            });
        });
    }));

    match result {
        Ok(()) => {
            print_memory_stats(&*ctx.runtime_env().memory_pool);
            Ok(())
        }
        Err(payload) => Err(panic_payload_to_error(payload.as_ref())),
    }
}

/// Extracts a readable message from a panic payload.
fn panic_payload_to_error(payload: &(dyn Any + Send)) -> DataFusionError {
    let message = if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else {
        "unknown panic"
    };

    exec_datafusion_err!("criterion benchmark failed: {message}")
}

pub fn default_sql_benchmark_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sql_benchmarks")
}

/// Replacements used by the Criterion SQL benchmark harness.
pub fn default_criterion_replacements() -> HashMap<String, String> {
    criterion_replacements(std::env::var("DATA_DIR").ok())
}

fn criterion_replacements(data_dir: Option<String>) -> HashMap<String, String> {
    HashMap::from([(
        "data_dir".to_string(),
        data_dir.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("data")
                .to_string_lossy()
                .into_owned()
        }),
    )])
}

fn make_tokio_runtime() -> Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

pub fn make_ctx(common: &CommonOpt) -> Result<SessionContext> {
    let config = common.config()?;
    let rt = common.build_runtime()?;

    Ok(SessionContext::new_with_config_rt(config, rt))
}

/// Discovers benchmark definition files in stable path order.
fn discover_benchmark_paths(path: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();

    collect_benchmark_paths(path, &mut paths)?;
    paths.sort();

    Ok(paths)
}

/// Loads all benchmark definitions with replacements derived from the filter.
pub async fn load_benchmark_definitions(
    filter: &BenchmarkFilter,
    ctx: &SessionContext,
    benchmark_dir: &Path,
    replacements: &HashMap<String, String>,
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    load_benchmark_definitions_for_query(filter, ctx, benchmark_dir, replacements, None)
        .await
}

/// Loads benchmark definitions, optionally limiting discovery to one filename.
pub async fn load_benchmark_definitions_for_query(
    filter: &BenchmarkFilter,
    ctx: &SessionContext,
    benchmark_dir: &Path,
    replacements: &HashMap<String, String>,
    query_filename: Option<&str>,
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    let mut benches = BTreeMap::new();
    let mut replacements = replacements.clone();
    let selected_suite_dir = filter
        .name
        .as_ref()
        .map(|name| benchmark_dir.join(name.to_ascii_lowercase()))
        .filter(|path| path.is_dir());
    let discovery_dir = selected_suite_dir.as_deref().unwrap_or(benchmark_dir);
    if let Some(subgroup) = &filter.subgroup {
        replacements.insert("bench_subgroup".to_string(), subgroup.to_string());
    }

    for path in discover_benchmark_paths(discovery_dir)?
        .into_iter()
        .filter(|path| {
            query_filename.is_none_or(|filename| {
                path.file_name()
                    .is_some_and(|candidate| candidate.eq_ignore_ascii_case(filename))
            })
        })
    {
        let benchmark = SqlBenchmark::new_with_replacements(
            ctx,
            &path,
            benchmark_dir,
            replacements.clone(),
        )
        .await?;
        benches
            .entry(benchmark.group().to_string())
            .or_insert_with(Vec::new)
            .push(benchmark);
    }

    sort_benchmarks(&mut benches);

    Ok(benches)
}

pub fn sort_benchmarks(benchmarks: &mut BTreeMap<String, Vec<SqlBenchmark>>) {
    for benchmarks in benchmarks.values_mut() {
        benchmarks.sort_by(|a, b| a.name().cmp(b.name()));
    }
}

/// Applies benchmark, subgroup, and query filters to discovered benchmark groups.
pub fn filter_benchmarks(
    filter: &BenchmarkFilter,
    benchmarks: BTreeMap<String, Vec<SqlBenchmark>>,
) -> BTreeMap<String, Vec<SqlBenchmark>> {
    match &filter.name {
        Some(bench_name) => benchmarks
            .into_iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(bench_name))
            .map(|(key, mut value)| {
                if let Some(subgroup) = &filter.subgroup {
                    value.retain(|bench| bench.subgroup().eq_ignore_ascii_case(subgroup));
                }
                if let Some(query) = &filter.query {
                    retain_query_matches(&mut value, query);
                }
                (key, value)
            })
            .filter(|(_, value)| !value.is_empty())
            .collect(),
        None => benchmarks,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum QueryMatchRank {
    Exact,
    StartsWith,
    TokenStartsWith,
    Contains,
}

/// Retains the best benchmark name matches for a query selector like `1` or `Q01`.
///
/// Exact matches keep all matching benchmarks; fallback matches keep one stable
/// best match to avoid running adjacent query variants unexpectedly.
fn retain_query_matches(benchmarks: &mut Vec<SqlBenchmark>, query: &str) {
    let normalized = normalize_query(query);
    let best_rank = benchmarks
        .iter()
        .filter_map(|bench| query_match_rank(bench.name(), &normalized))
        .min();
    let Some(best_rank) = best_rank else {
        benchmarks.clear();
        return;
    };

    // if exact match retain all matches
    if best_rank == QueryMatchRank::Exact {
        benchmarks.retain(|bench| {
            query_match_rank(bench.name(), &normalized) == Some(QueryMatchRank::Exact)
        });
        return;
    }

    let selected = benchmarks
        .iter()
        .filter(|bench| query_match_rank(bench.name(), &normalized) == Some(best_rank))
        .min_by(|left, right| {
            left.name()
                .cmp(right.name())
                .then_with(|| left.subgroup().cmp(right.subgroup()))
        })
        .cloned();

    benchmarks.clear();

    if let Some(benchmark) = selected {
        benchmarks.push(benchmark);
    }
}

/// Ranks query-name matches, preferring direct `Q01...` names before fallback
/// matches inside descriptive names such as `costsel_q01...`.
fn query_match_rank(name: &str, normalized_query: &str) -> Option<QueryMatchRank> {
    let name = name.to_ascii_uppercase();
    let normalized_query = normalized_query.to_ascii_uppercase();

    if name == normalized_query {
        Some(QueryMatchRank::Exact)
    } else if name.starts_with(&normalized_query) {
        Some(QueryMatchRank::StartsWith)
    } else if name
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|token| token.starts_with(&normalized_query))
    {
        Some(QueryMatchRank::TokenStartsWith)
    } else if name.contains(&normalized_query) {
        Some(QueryMatchRank::Contains)
    } else {
        None
    }
}

/// Converts user query selectors into the SQL benchmark `QNN` naming form.
fn normalize_query(query: &str) -> String {
    let query = query.trim_start_matches(['Q', 'q']);
    let split = query
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(query.len());
    let (number, suffix) = query.split_at(split);

    format!("Q{number:0>2}{suffix}")
}

pub fn format_benchmark_list(benchmarks: &BTreeMap<String, Vec<SqlBenchmark>>) -> String {
    let mut output = String::from("SQL benchmarks:\n");

    for (name, benchmarks) in benchmarks {
        let query_word = if benchmarks.len() == 1 {
            "query"
        } else {
            "queries"
        };
        writeln!(output, "  {name:<24} {} {query_word}", benchmarks.len()).ok();
    }

    output.trim_end().to_string()
}

/// Recursively collects `.benchmark` files below `path`.
fn collect_benchmark_paths(path: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    let mut entries = fs::read_dir(path)?
        .filter_map(std::result::Result::ok)
        .collect::<Vec<_>>();

    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect_benchmark_paths(&path, paths)?;
        } else if path.extension().is_some_and(|ext| ext == "benchmark") {
            paths.push(path);
        }
    }

    Ok(())
}

pub fn unknown_benchmark_error(
    requested: &str,
    benchmarks: &BTreeMap<String, Vec<SqlBenchmark>>,
) -> DataFusionError {
    exec_datafusion_err!(
        "unknown benchmark '{requested}'\n\n{}",
        format_benchmark_list(benchmarks)
    )
}

fn unknown_subgroup_error(
    benchmark_name: &str,
    subgroup: &str,
    benchmarks: &[SqlBenchmark],
) -> DataFusionError {
    exec_datafusion_err!(
        "no SQL benchmark subgroup matched benchmark '{benchmark_name}' with subgroup '{subgroup}'\n\n{}",
        format_subgroup_list(benchmark_name, benchmarks)
    )
}

fn unknown_query_error(
    benchmark_name: &str,
    query: &str,
    subgroup: Option<&str>,
    benchmarks: &[SqlBenchmark],
) -> DataFusionError {
    let normalized = normalize_query(query);

    exec_datafusion_err!(
        "no SQL benchmark query matched benchmark '{benchmark_name}' with query '{query}' (normalized: '{normalized}')\n\n{}",
        format_query_list(benchmark_name, subgroup, benchmarks)
    )
}

fn format_subgroup_list(benchmark_name: &str, benchmarks: &[SqlBenchmark]) -> String {
    let mut entries = benchmarks
        .iter()
        .map(|bench| {
            if bench.subgroup().is_empty() {
                "<none>".to_string()
            } else {
                bench.subgroup().to_string()
            }
        })
        .collect::<Vec<_>>();

    entries.sort();
    entries.dedup();

    let mut output = format!("Available {benchmark_name} subgroups:\n");

    if entries.is_empty() {
        output.push_str("  <none>");
    } else {
        for entry in entries {
            writeln!(output, "  {entry}").ok();
        }
    }

    output.trim_end().to_string()
}

/// Formats available query names for an unknown-query error message.
fn format_query_list(
    benchmark_name: &str,
    subgroup: Option<&str>,
    benchmarks: &[SqlBenchmark],
) -> String {
    let mut entries = benchmarks
        .iter()
        .filter(|bench| {
            subgroup
                .is_none_or(|subgroup| bench.subgroup().eq_ignore_ascii_case(subgroup))
        })
        .map(|bench| {
            if bench.subgroup().is_empty() {
                bench.name().to_string()
            } else {
                format!("{}/{} ", bench.subgroup(), bench.name())
            }
        })
        .take(10)
        .collect::<Vec<_>>();

    entries.sort();
    entries.dedup();
    if entries.len() == 10 {
        entries.push("...".to_string());
    }

    let mut output = match subgroup {
        Some(subgroup) => {
            format!("Available {benchmark_name} queries in subgroup '{subgroup}':\n")
        }
        None => format!("Available {benchmark_name} queries:\n"),
    };

    if entries.is_empty() {
        output.push_str("  <none>");
    } else {
        for entry in entries {
            writeln!(output, "  {entry}").ok();
        }
    }

    output.trim_end().to_string()
}

/// Initializes a benchmark and performs any configured assertion or validation step.
pub async fn prepare_benchmark(
    ctx: &SessionContext,
    benchmark: &mut SqlBenchmark,
    config: &SqlRunConfig,
) -> Result<()> {
    benchmark.initialize(ctx).await?;
    benchmark.assert(ctx).await?;

    if config.persist_results {
        benchmark.persist(ctx).await?;
    } else if config.validate_results {
        let _ = benchmark.run(ctx, true).await?;
        benchmark.verify(ctx).await?;
    }

    Ok(())
}

/// Ensures filtering selected at least one benchmark and emits targeted errors.
pub fn ensure_selection(
    filter: &BenchmarkFilter,
    all_benchmarks: &BTreeMap<String, Vec<SqlBenchmark>>,
    selected: &BTreeMap<String, Vec<SqlBenchmark>>,
) -> Result<()> {
    if selected.is_empty() {
        if let Some(name) = &filter.name {
            if let Some((benchmark_name, benchmarks)) = all_benchmarks
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
            {
                if let Some(subgroup) = &filter.subgroup {
                    let has_subgroup = benchmarks
                        .iter()
                        .any(|bench| bench.subgroup().eq_ignore_ascii_case(subgroup));

                    if !has_subgroup {
                        return Err(unknown_subgroup_error(
                            benchmark_name,
                            subgroup,
                            benchmarks,
                        ));
                    }
                }

                if let Some(query) = &filter.query {
                    return Err(unknown_query_error(
                        benchmark_name,
                        query,
                        filter.subgroup.as_deref(),
                        benchmarks,
                    ));
                }
            }
            return Err(unknown_benchmark_error(name, all_benchmarks));
        }
        return Err(exec_datafusion_err!("no SQL benchmarks discovered"));
    }

    Ok(())
}

/// Combines benchmark and cleanup results without hiding cleanup failures.
pub fn finish_benchmark(result: Result<()>, cleanup_result: Result<()>) -> Result<()> {
    match (result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(exec_datafusion_err!(
            "{error}; cleanup also failed: {cleanup_error}"
        )),
    }
}

fn criterion_function_name(benchmark: &SqlBenchmark) -> String {
    let mut name = benchmark.name().to_string();

    if !benchmark.subgroup().is_empty() {
        name.push('_');
        name.push_str(benchmark.subgroup());
    }

    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use std::path::{Path, PathBuf};

    fn write_benchmark(root: &Path, relative_path: &str, contents: &str) -> PathBuf {
        let path = root.join(relative_path);

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();

        path
    }

    #[tokio::test]
    async fn caller_replacements_reach_parser() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nload\nSELECT '${ALPHA_FORMAT}'\n\nrun\nSELECT 1\n",
        );
        let replacements =
            HashMap::from([("alpha_format".to_string(), "csv".to_string())]);

        let result = load_benchmark_definitions(
            &BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("1".to_string()),
            },
            &SessionContext::new(),
            temp.path(),
            &replacements,
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn query_filename_filters_paths_before_parsing() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q07.benchmark",
            "name Q07\n\nrun\nSELECT 7\n",
        );
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q08.benchmark",
            "this is not a benchmark definition",
        );
        write_benchmark(
            temp.path(),
            "beta/benchmarks/q07.benchmark",
            "this is not a benchmark definition",
        );

        let benches = load_benchmark_definitions_for_query(
            &BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("7".to_string()),
            },
            &SessionContext::new(),
            temp.path(),
            &HashMap::new(),
            Some("q07.benchmark"),
        )
        .await
        .unwrap();

        assert_eq!(benches["alpha"].len(), 1);
        assert_eq!(benches["alpha"][0].name(), "Q07");
    }

    #[test]
    fn criterion_replacements_use_benchmarks_data_directory() {
        let expected = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .to_string_lossy()
            .into_owned();

        assert_eq!(criterion_replacements(None)["data_dir"], expected);
    }

    #[test]
    fn criterion_replacements_use_explicit_data_directory() {
        let replacements = criterion_replacements(Some("/custom/data".to_string()));

        assert_eq!(replacements["data_dir"], "/custom/data");
    }

    #[tokio::test]
    async fn query_filename_keeps_matches_in_multiple_subgroups() {
        let temp = tempfile::tempdir().unwrap();
        for subgroup in ["aggregate", "window"] {
            write_benchmark(
                temp.path(),
                &format!("alpha/benchmarks/{subgroup}/q03.benchmark"),
                &format!("name Q03\nsubgroup {subgroup}\n\nrun\nSELECT 3\n"),
            );
        }

        let filter = BenchmarkFilter {
            name: Some("alpha".to_string()),
            subgroup: None,
            query: Some("3".to_string()),
        };
        let benches = load_benchmark_definitions_for_query(
            &filter,
            &SessionContext::new(),
            temp.path(),
            &HashMap::new(),
            Some("q03.benchmark"),
        )
        .await
        .unwrap();
        assert_eq!(filter_benchmarks(&filter, benches)["alpha"].len(), 2);

        let filter = BenchmarkFilter {
            subgroup: Some("window".to_string()),
            ..filter
        };
        let benches = load_benchmark_definitions_for_query(
            &filter,
            &SessionContext::new(),
            temp.path(),
            &HashMap::new(),
            Some("q03.benchmark"),
        )
        .await
        .unwrap();
        assert_eq!(filter_benchmarks(&filter, benches)["alpha"].len(), 1);
    }

    #[tokio::test]
    async fn query_filename_accepts_alphanumeric_pattern() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "imdb/benchmarks/01a.benchmark",
            "name Q01a\n\nrun\nSELECT 1\n",
        );

        let benches = load_benchmark_definitions_for_query(
            &BenchmarkFilter {
                name: Some("imdb".to_string()),
                subgroup: None,
                query: Some("1a".to_string()),
            },
            &SessionContext::new(),
            temp.path(),
            &HashMap::new(),
            Some("01a.benchmark"),
        )
        .await
        .unwrap();

        assert_eq!(benches["imdb"][0].name(), "Q01a");
    }

    #[test]
    fn normalizes_query_like_existing_sql_harness() {
        assert_eq!(normalize_query("1"), "Q01");
        assert_eq!(normalize_query("01"), "Q01");
        assert_eq!(normalize_query("6a"), "Q06a");
        assert_eq!(normalize_query("Q06a"), "Q06a");
    }

    #[test]
    fn criterion_names_match_existing_sql_harness() {
        let temp = tempfile::tempdir().unwrap();
        let benchmark_path = write_benchmark(
            temp.path(),
            "tpch/benchmarks/q01.benchmark",
            "name Q01\nsubgroup sf1\n\nrun\nSELECT 1\n",
        );
        let ctx = SessionContext::new();
        let rt = make_tokio_runtime().unwrap();
        let benchmark = rt
            .block_on(SqlBenchmark::new(&ctx, &benchmark_path, temp.path()))
            .unwrap();

        assert_eq!(benchmark.group(), "tpch");
        assert_eq!(criterion_function_name(&benchmark), "Q01_sf1");
    }

    #[test]
    fn criterion_group_names_include_safe_namespaces() {
        assert_eq!(criterion_group_name("tpch", None).unwrap(), "tpch");
        assert_eq!(
            criterion_group_name("tpch", Some("parquet-sf1")).unwrap(),
            "tpch__parquet-sf1"
        );
        assert_eq!(
            criterion_group_name("tpch", Some("memory_sf1")).unwrap(),
            "tpch__memory_sf1"
        );
    }

    #[test]
    fn criterion_group_names_reject_unsafe_namespaces() {
        for namespace in ["", "csv/sf1", "csv sf1", "csv.sf1", "parquét"] {
            let error = criterion_group_name("tpch", Some(namespace)).unwrap_err();

            assert!(error.to_string().contains("namespace"), "{error}");
        }
    }

    #[test]
    fn criterion_group_names_reject_windows_case_collisions() {
        let error = criterion_group_name("tpch", Some("Parquet")).unwrap_err();

        assert!(error.to_string().contains("lowercase"), "{error}");
        assert_eq!(
            criterion_group_name("tpch", Some("parquet")).unwrap(),
            "tpch__parquet"
        );
    }

    #[test]
    fn criterion_group_names_reject_components_criterion_would_truncate() {
        let group_name = "g".repeat(55);

        assert_eq!(
            criterion_group_name(&group_name, Some("1234567"))
                .unwrap()
                .len(),
            64
        );

        let first = criterion_group_name(&group_name, Some("12345678"));
        let second = criterion_group_name(&group_name, Some("12345679"));

        assert!(first.unwrap_err().to_string().contains("64 bytes"));
        assert!(second.unwrap_err().to_string().contains("64 bytes"));
    }

    #[test]
    fn criterion_harness_reads_namespace_from_env_in_subprocess() {
        const CHILD_ENV: &str = "DATAFUSION_CRITERION_HARNESS_ENV_TEST_CHILD";

        if std::env::var_os(CHILD_ENV).is_some() {
            let (_, namespace) = criterion_harness_config_from_env();

            assert_eq!(namespace.as_deref(), Some("parquet_sf1"));
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sql_benchmark_runner::tests::criterion_harness_reads_namespace_from_env_in_subprocess",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .env("BENCH_NAMESPACE", "parquet_sf1")
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "child failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
