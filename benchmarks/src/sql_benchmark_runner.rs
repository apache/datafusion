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
use criterion::{Criterion, SamplingMode};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, exec_datafusion_err};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;

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
    pub persist_results: bool,
    pub validate_results: bool,
    pub output: Option<PathBuf>,
}

/// Runs the selected SQL benchmarks through a caller-provided Criterion instance.
pub fn run_criterion_benchmarks_impl(
    benchmark_dir: &Path,
    config: &SqlRunConfig,
    criterion: &mut Criterion,
) -> Result<()> {
    let rt = make_tokio_runtime()?;
    let listing_ctx = make_ctx(&config.common)?;
    let all_benchmarks = rt.block_on(load_benchmark_definitions(
        &config.filter,
        &listing_ctx,
        benchmark_dir,
    ))?;
    let selected = filter_benchmarks(&config.filter, all_benchmarks.clone());

    ensure_selection(&config.filter, &all_benchmarks, &selected)?;

    for (group_name, benchmarks) in selected {
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
            print_memory_stats();
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
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    let mut benches = BTreeMap::new();
    let replacements = benchmark_replacements(filter);

    for path in discover_benchmark_paths(benchmark_dir)? {
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

/// Builds template replacements from CLI values that also appear in benchmark files.
fn benchmark_replacements(filter: &BenchmarkFilter) -> HashMap<String, String> {
    let mut replacements = HashMap::new();
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .to_string_lossy()
            .into_owned()
    });

    replacements.insert("data_dir".to_string(), data_dir);

    if let Some(subgroup) = &filter.subgroup {
        replacements.insert("bench_subgroup".to_string(), subgroup.to_string());
    }

    replacements
}

pub fn sort_benchmarks(benchmarks: &mut BTreeMap<String, Vec<SqlBenchmark>>) {
    benchmarks
        .values_mut()
        .for_each(|benchmarks| benchmarks.sort_by(|a, b| a.name().cmp(b.name())));
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
        output.push_str(&format!("  {name:<24} {} {query_word}\n", benchmarks.len()));
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
            output.push_str(&format!("  {entry}\n"));
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
            output.push_str(&format!("  {entry}\n"));
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
}
