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
use crate::util::{BenchmarkRun, CommonOpt, print_memory_stats};
use clap::{ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser};
use criterion::{Criterion, SamplingMode};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, exec_datafusion_err, instant::Instant};
use datafusion_common_runtime::SpawnedTask;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::IsTerminal;
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

#[derive(Debug)]
pub enum CliAction {
    List,
    Simple(SqlRunConfig),
    Criterion {
        config: SqlRunConfig,
        save_baseline: Option<String>,
    },
}

#[derive(Debug, Parser)]
#[command(
    name = "benchmark_runner",
    about = "Run DataFusion SQL benchmarks",
    styles = criterion_like_styles(),
)]
pub struct Cli {
    #[arg(value_name = "BENCHMARK", help = "SQL benchmark group to run")]
    pub benchmark: Option<String>,

    #[arg(short = 'q', long = "query", env = "BENCH_QUERY")]
    pub query: Option<String>,

    #[arg(long = "subgroup", env = "BENCH_SUBGROUP")]
    pub subgroup: Option<String>,

    #[command(flatten)]
    pub common: CommonOpt,

    #[arg(
        long = "criterion",
        action = ArgAction::SetTrue,
        help = "Run benchmarks with Criterion"
    )]
    pub criterion: bool,

    #[arg(
        short = 'o',
        long = "output",
        help = "Write simple runner results as JSON to this path"
    )]
    pub output: Option<PathBuf>,

    #[arg(
        long = "save-baseline",
        value_name = "BASELINE",
        help = "Save Criterion measurements to the named baseline"
    )]
    pub save_baseline: Option<String>,
}

/// Parses CLI arguments, runs the selected action, and prints any list output.
pub async fn run_cli() -> Result<()> {
    let matches = Cli::command().get_matches();
    let action = cli_action_from_matches(matches)?;
    let output = run_cli_action(action, &default_sql_benchmark_directory()).await?;

    if !output.is_empty() {
        println!("{output}");
    }

    Ok(())
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

pub fn default_sql_benchmark_directory() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sql_benchmarks")
}

fn make_tokio_runtime() -> Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

fn make_ctx(common: &CommonOpt) -> Result<SessionContext> {
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

/// Loads benchmark definitions, applies CLI-style filters, and sorts each group.
async fn load_benchmarks(
    filter: &BenchmarkFilter,
    ctx: &SessionContext,
    benchmark_dir: &Path,
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    let benches = load_benchmark_definitions(filter, ctx, benchmark_dir).await?;
    let mut benches = filter_benchmarks(filter, benches);

    sort_benchmarks(&mut benches);

    Ok(benches)
}

/// Loads all benchmark definitions with replacements derived from the filter.
async fn load_benchmark_definitions(
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

    if let Some(subgroup) = &filter.subgroup {
        replacements.insert("bench_subgroup".to_string(), subgroup.to_string());
    }

    replacements
}

fn sort_benchmarks(benchmarks: &mut BTreeMap<String, Vec<SqlBenchmark>>) {
    benchmarks
        .values_mut()
        .for_each(|benchmarks| benchmarks.sort_by(|a, b| a.name().cmp(b.name())));
}

/// Applies benchmark, subgroup, and query filters to discovered benchmark groups.
fn filter_benchmarks(
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

fn format_benchmark_list(benchmarks: &BTreeMap<String, Vec<SqlBenchmark>>) -> String {
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

/// Runs selected benchmarks with fixed iteration counts and optional JSON output.
async fn run_simple_benchmarks(benchmark_dir: &Path, config: SqlRunConfig) -> Result<()> {
    if config.common.iterations == 0 {
        return Err(exec_datafusion_err!("iterations must be greater than zero"));
    }

    let listing_ctx = make_ctx(&config.common)?;
    let all_benchmarks =
        load_benchmark_definitions(&config.filter, &listing_ctx, benchmark_dir).await?;
    let selected = filter_benchmarks(&config.filter, all_benchmarks.clone());
    let mut run = BenchmarkRun::new();

    ensure_selection(&config.filter, &all_benchmarks, &selected)?;

    for (_group, benchmarks) in selected {
        for mut benchmark in benchmarks {
            let ctx = make_ctx(&config.common)?;
            let result =
                run_simple_benchmark(&ctx, &mut benchmark, &config, &mut run).await;
            let cleanup_result = benchmark.cleanup(&ctx).await;

            finish_benchmark(result, cleanup_result)?;
        }
    }

    run.maybe_write_json(config.output.as_ref())?;

    Ok(())
}

/// Builds the default Criterion runner and optionally records a named baseline.
fn run_criterion_benchmarks(
    benchmark_dir: &Path,
    config: &SqlRunConfig,
    save_baseline: Option<&str>,
) -> Result<()> {
    let mut criterion = Criterion::default()
        .sample_size(10)
        .with_output_color(std::io::stdout().is_terminal());

    if let Some(save_baseline) = save_baseline {
        criterion = criterion.save_baseline(save_baseline.to_string());
    }

    run_criterion_benchmarks_impl(benchmark_dir, config, &mut criterion)?;
    criterion.final_summary();

    Ok(())
}

/// Converts parsed arguments into an executable action and validates mode options.
fn cli_action_from_matches(matches: ArgMatches) -> Result<CliAction> {
    let cli = Cli::from_arg_matches(&matches)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if cli.benchmark.is_none() {
        return Ok(CliAction::List);
    }

    if cli.criterion && cli.output.is_some() {
        return Err(exec_datafusion_err!(
            "--output cannot be used with --criterion"
        ));
    }
    if !cli.criterion && cli.save_baseline.is_some() {
        return Err(exec_datafusion_err!(
            "--save-baseline cannot be used without --criterion"
        ));
    }

    // we need to know if iterations was set on the command line, not the default value
    let iterations_from_cli = matches.value_source("iterations")
        == Some(clap::parser::ValueSource::CommandLine);

    if cli.criterion && iterations_from_cli {
        return Err(exec_datafusion_err!(
            "--iterations cannot be used with --criterion"
        ));
    }
    if !cli.criterion && cli.common.iterations == 0 {
        return Err(exec_datafusion_err!("iterations must be greater than zero"));
    }

    let config = SqlRunConfig {
        common: cli.common,
        filter: BenchmarkFilter {
            name: cli.benchmark,
            subgroup: cli.subgroup,
            query: cli.query,
        },
        persist_results: false,
        validate_results: false,
        output: cli.output,
    };

    if cli.criterion {
        Ok(CliAction::Criterion {
            config,
            save_baseline: cli.save_baseline,
        })
    } else {
        Ok(CliAction::Simple(config))
    }
}

fn criterion_like_styles() -> clap::builder::Styles {
    use clap::builder::styling::AnsiColor;

    clap::builder::Styles::styled()
        .header(AnsiColor::Green.on_default().bold())
        .usage(AnsiColor::Green.on_default().bold())
        .literal(AnsiColor::Cyan.on_default().bold())
        .placeholder(AnsiColor::Cyan.on_default())
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

fn unknown_benchmark_error(
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

/// Runs one benchmark case, recording each timed iteration.
async fn run_simple_benchmark(
    ctx: &SessionContext,
    benchmark: &mut SqlBenchmark,
    config: &SqlRunConfig,
    run: &mut BenchmarkRun,
) -> Result<()> {
    prepare_benchmark(ctx, benchmark, config).await?;

    let case_name = benchmark_case_name(benchmark);

    run.start_new_case(&case_name);

    for iteration in 0..config.common.iterations {
        let start = Instant::now();
        let row_count = benchmark.run(ctx, false).await?;
        let elapsed = start.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;

        println!("{case_name} iteration {iteration}: {ms:.1} ms, {row_count} rows");

        run.write_iter(elapsed, row_count);
    }

    print_memory_stats();

    Ok(())
}

/// Initializes a benchmark and performs any configured assertion or validation step.
async fn prepare_benchmark(
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
fn ensure_selection(
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

fn benchmark_case_name(benchmark: &SqlBenchmark) -> String {
    let mut name = format!("{}/{}", benchmark.group(), benchmark.name());

    if !benchmark.subgroup().is_empty() {
        name.push('/');
        name.push_str(benchmark.subgroup());
    }

    name
}

/// Combines benchmark and cleanup results without hiding cleanup failures.
fn finish_benchmark(result: Result<()>, cleanup_result: Result<()>) -> Result<()> {
    match (result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(cleanup_error)) => Err(cleanup_error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(exec_datafusion_err!(
            "{error}; cleanup also failed: {cleanup_error}"
        )),
    }
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

fn criterion_function_name(benchmark: &SqlBenchmark) -> String {
    let mut name = benchmark.name().to_string();

    if !benchmark.subgroup().is_empty() {
        name.push('_');
        name.push_str(benchmark.subgroup());
    }

    name
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

/// Executes a parsed CLI action and returns any text that should be printed.
async fn run_cli_action(action: CliAction, benchmark_dir: &Path) -> Result<String> {
    match action {
        CliAction::List => {
            let ctx = SessionContext::new();
            let benchmarks =
                load_benchmarks(&BenchmarkFilter::default(), &ctx, benchmark_dir).await?;

            Ok(format_benchmark_list(&benchmarks))
        }
        CliAction::Simple(config) => {
            run_simple_benchmarks(benchmark_dir, config).await?;
            Ok(String::new())
        }
        CliAction::Criterion {
            config,
            save_baseline,
        } => {
            if config.output.is_some() {
                return Err(exec_datafusion_err!(
                    "--output cannot be used with --criterion"
                ));
            }
            let benchmark_dir = benchmark_dir.to_path_buf();

            SpawnedTask::spawn_blocking(move || {
                run_criterion_benchmarks(
                    &benchmark_dir,
                    &config,
                    save_baseline.as_deref(),
                )
            })
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;

            Ok(String::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use criterion::Criterion;
    use datafusion::prelude::SessionContext;
    use std::path::{Path, PathBuf};

    fn write_benchmark(root: &Path, relative_path: &str, contents: &str) -> PathBuf {
        let path = root.join(relative_path);

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();

        path
    }

    fn common(iterations: usize) -> CommonOpt {
        CommonOpt {
            iterations,
            partitions: None,
            batch_size: None,
            mem_pool_type: "fair".to_string(),
            memory_limit: None,
            sort_spill_reservation_bytes: None,
            debug: false,
            simulate_latency: false,
        }
    }

    fn parse_cli_from<I, T>(args: I) -> Result<CliAction>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let matches = Cli::command()
            .try_get_matches_from(args)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        cli_action_from_matches(matches)
    }

    async fn run_cli_with_dir<I, T>(args: I, benchmark_dir: &Path) -> Result<String>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        run_cli_action(parse_cli_from(args)?, benchmark_dir).await
    }

    #[test]
    fn cli_lists_when_benchmark_is_omitted() {
        let action = parse_cli_from(["benchmark_runner"]).unwrap();

        assert!(matches!(action, CliAction::List));
    }

    #[test]
    fn cli_defaults_to_basic_runner() {
        let action =
            parse_cli_from(["benchmark_runner", "tpch", "--query", "1"]).unwrap();
        let CliAction::Simple(config) = action else {
            panic!("expected basic runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("tpch"));
        assert_eq!(config.filter.query.as_deref(), Some("1"));
    }

    #[test]
    fn cli_reads_query_from_env() {
        let previous = std::env::var_os("BENCH_QUERY");
        // SAFETY: This test restores BENCH_QUERY before returning and does not
        // spawn threads while the environment variable is overridden.
        unsafe {
            std::env::set_var("BENCH_QUERY", "8");
        }

        let action = parse_cli_from(["benchmark_runner", "tpch"]);

        unsafe {
            match previous {
                Some(value) => std::env::set_var("BENCH_QUERY", value),
                None => std::env::remove_var("BENCH_QUERY"),
            }
        }

        let action = action.unwrap();
        let CliAction::Simple(config) = action else {
            panic!("expected basic runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("tpch"));
        assert_eq!(config.filter.query.as_deref(), Some("8"));
    }

    #[test]
    fn cli_accepts_criterion_runner() {
        let action = parse_cli_from([
            "benchmark_runner",
            "tpch",
            "--criterion",
            "--save-baseline",
            "main",
        ])
        .unwrap();

        let CliAction::Criterion {
            config,
            save_baseline,
        } = action
        else {
            panic!("expected criterion runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("tpch"));
        assert_eq!(save_baseline.as_deref(), Some("main"));
    }

    #[test]
    fn cli_rejects_output_with_criterion() {
        let err = parse_cli_from([
            "benchmark_runner",
            "tpch",
            "--criterion",
            "--output",
            "results.json",
        ])
        .unwrap_err();

        assert!(err.to_string().contains("--output"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_save_baseline_without_criterion() {
        let err = parse_cli_from(["benchmark_runner", "tpch", "--save-baseline", "main"])
            .unwrap_err();

        assert!(err.to_string().contains("--save-baseline"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_iterations_with_criterion() {
        let err = parse_cli_from([
            "benchmark_runner",
            "tpch",
            "--criterion",
            "--iterations",
            "3",
        ])
        .unwrap_err();

        assert!(err.to_string().contains("--iterations"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_zero_basic_iterations() {
        let err = parse_cli_from(["benchmark_runner", "tpch", "--iterations", "0"])
            .unwrap_err();

        assert!(err.to_string().contains("iterations"));
    }

    #[tokio::test]
    async fn discovery_lists_groups_from_directories() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        write_benchmark(
            temp.path(),
            "beta/benchmarks/q02.benchmark",
            "name Q02\n\nrun\nSELECT 2\n",
        );
        let ctx = SessionContext::new();
        let benches = load_benchmarks(&BenchmarkFilter::default(), &ctx, temp.path())
            .await
            .unwrap();

        assert_eq!(benches["alpha"].len(), 1);
        assert_eq!(benches["beta"].len(), 1);
    }

    #[tokio::test]
    async fn discovery_filters_benchmark_subgroup_and_query() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\nsubgroup wide\n\nrun\nSELECT 1\n",
        );
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q02.benchmark",
            "name Q02\nsubgroup narrow\n\nrun\nSELECT 2\n",
        );

        let ctx = SessionContext::new();
        let filter = BenchmarkFilter {
            name: Some("alpha".to_string()),
            subgroup: Some("wide".to_string()),
            query: Some("1".to_string()),
        };
        let benches = load_benchmarks(&filter, &ctx, temp.path()).await.unwrap();

        assert_eq!(benches.len(), 1);
        assert_eq!(benches["alpha"].len(), 1);
        assert_eq!(benches["alpha"][0].name(), "Q01");
    }

    #[tokio::test]
    async fn cli_subgroup_filter_is_used_for_benchmark_replacements() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "wide_schema/benchmarks/q01.benchmark",
            "name Q01\nsubgroup ${BENCH_SUBGROUP:-wide}\n\nrun\nSELECT '${BENCH_SUBGROUP:-wide}'\n",
        );

        let ctx = SessionContext::new();
        let filter = BenchmarkFilter {
            name: Some("wide_schema".to_string()),
            subgroup: Some("narrow".to_string()),
            query: None,
        };
        let benches = load_benchmarks(&filter, &ctx, temp.path()).await.unwrap();

        assert_eq!(benches["wide_schema"].len(), 1);
        assert_eq!(benches["wide_schema"][0].subgroup(), "narrow");
    }

    #[tokio::test]
    async fn query_filter_matches_starts_with_when_exact_match_is_absent() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01a.benchmark",
            "name Q01a\n\nrun\nSELECT 1\n",
        );

        let ctx = SessionContext::new();
        let filter = BenchmarkFilter {
            name: Some("alpha".to_string()),
            subgroup: None,
            query: Some("1".to_string()),
        };
        let benches = load_benchmarks(&filter, &ctx, temp.path()).await.unwrap();

        assert_eq!(benches["alpha"].len(), 1);
        assert_eq!(benches["alpha"][0].name(), "Q01a");
    }

    #[tokio::test]
    async fn query_filter_matches_token_start_when_exact_match_is_absent() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "predicate_eval/benchmarks/costsel/q01.benchmark",
            "name costsel_q01_regexp_selective_last\n\nrun\nSELECT 1\n",
        );

        let ctx = SessionContext::new();
        let filter = BenchmarkFilter {
            name: Some("predicate_eval".to_string()),
            subgroup: None,
            query: Some("1".to_string()),
        };
        let benches = load_benchmarks(&filter, &ctx, temp.path()).await.unwrap();

        assert_eq!(benches["predicate_eval"].len(), 1);
        assert_eq!(
            benches["predicate_eval"][0].name(),
            "costsel_q01_regexp_selective_last"
        );
    }

    #[tokio::test]
    async fn query_filter_prefers_starts_with_match_over_token_match() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/token.benchmark",
            "name costsel_q01_regexp_selective_last\n\nrun\nSELECT 1\n",
        );
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01a.benchmark",
            "name Q01a\n\nrun\nSELECT 2\n",
        );

        let ctx = SessionContext::new();
        let filter = BenchmarkFilter {
            name: Some("alpha".to_string()),
            subgroup: None,
            query: Some("1".to_string()),
        };
        let benches = load_benchmarks(&filter, &ctx, temp.path()).await.unwrap();

        assert_eq!(benches["alpha"].len(), 1);
        assert_eq!(benches["alpha"][0].name(), "Q01a");
    }

    #[test]
    fn normalizes_query_like_existing_sql_harness() {
        assert_eq!(normalize_query("1"), "Q01");
        assert_eq!(normalize_query("01"), "Q01");
        assert_eq!(normalize_query("6a"), "Q06a");
        assert_eq!(normalize_query("Q06a"), "Q06a");
    }

    #[tokio::test]
    async fn list_output_is_sorted_and_includes_counts() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "beta/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q02.benchmark",
            "name Q02\n\nrun\nSELECT 2\n",
        );

        let ctx = SessionContext::new();
        let benches = load_benchmarks(&BenchmarkFilter::default(), &ctx, temp.path())
            .await
            .unwrap();
        let output = format_benchmark_list(&benches);

        assert!(output.starts_with("SQL benchmarks:\n  alpha"));
        assert!(output.contains("alpha                    2 queries"));
        assert!(output.contains("beta                     1 query"));
    }

    #[tokio::test]
    async fn unknown_benchmark_error_includes_available_benchmarks() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        let ctx = SessionContext::new();
        let benches = load_benchmarks(&BenchmarkFilter::default(), &ctx, temp.path())
            .await
            .unwrap();
        let message = unknown_benchmark_error("missing", &benches).to_string();

        assert!(message.contains("unknown benchmark 'missing'"), "{message}");
        assert!(message.contains("alpha"), "{message}");
    }

    #[tokio::test]
    async fn run_cli_reports_unknown_query_for_known_benchmark() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let err = run_cli_with_dir(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "9",
                "--iterations",
                "1",
            ],
            temp.path(),
        )
        .await
        .unwrap_err();
        let message = err.to_string();

        assert!(
            message.contains("no SQL benchmark query matched benchmark 'alpha'"),
            "{message}"
        );
        assert!(message.contains("query '9'"), "{message}");
        assert!(message.contains("normalized: 'Q09'"), "{message}");
        assert!(message.contains("Available alpha queries:"), "{message}");
        assert!(message.contains("Q01"), "{message}");
        assert!(!message.contains("unknown benchmark 'alpha'"), "{message}");
    }

    #[tokio::test]
    async fn run_cli_reports_unknown_subgroup_for_known_benchmark() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\nsubgroup wide\n\nrun\nSELECT 1\n",
        );

        let err = run_cli_with_dir(
            [
                "benchmark_runner",
                "alpha",
                "--subgroup",
                "narrow",
                "--iterations",
                "1",
            ],
            temp.path(),
        )
        .await
        .unwrap_err();
        let message = err.to_string();

        assert!(
            message.contains(
                "no SQL benchmark subgroup matched benchmark 'alpha' with subgroup 'narrow'"
            ),
            "{message}"
        );
        assert!(message.contains("Available alpha subgroups:"), "{message}");
        assert!(message.contains("wide"), "{message}");
        assert!(!message.contains("unknown benchmark 'alpha'"), "{message}");
    }

    #[tokio::test]
    async fn basic_runner_executes_iterations_and_writes_json() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("results.json");

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT * FROM (VALUES (1), (2)) AS t(v)\n",
        );

        let config = SqlRunConfig {
            common: common(2),
            filter: BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("1".to_string()),
            },
            persist_results: false,
            validate_results: false,
            output: Some(output.clone()),
        };

        run_simple_benchmarks(temp.path(), config).await.unwrap();

        let json = fs::read_to_string(output).unwrap();

        assert!(json.contains("\"query\": \"alpha/Q01\""));
        assert!(json.contains("\"row_count\": 2"));
        assert_eq!(json.matches("\"row_count\": 2").count(), 2);
    }

    #[tokio::test]
    async fn basic_runner_reports_run_and_cleanup_failures() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT * FROM missing_run_table\n\ncleanup\nSELECT * FROM missing_cleanup_table\n",
        );

        let config = SqlRunConfig {
            common: common(1),
            filter: BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("1".to_string()),
            },
            persist_results: false,
            validate_results: false,
            output: None,
        };
        let err = run_simple_benchmarks(temp.path(), config)
            .await
            .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("missing_run_table"), "{message}");
        assert!(message.contains("cleanup also failed"), "{message}");
        assert!(message.contains("missing_cleanup_table"), "{message}");
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
    fn criterion_runner_saves_named_baseline() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let output = tempfile::tempdir().unwrap();
        let mut criterion = Criterion::default()
            .sample_size(10)
            .warm_up_time(std::time::Duration::from_millis(1))
            .measurement_time(std::time::Duration::from_millis(10))
            .without_plots()
            .output_directory(output.path())
            .save_baseline("acceptance".to_string());
        let config = SqlRunConfig {
            common: common(3),
            filter: BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("1".to_string()),
            },
            persist_results: false,
            validate_results: false,
            output: None,
        };

        run_criterion_benchmarks_impl(temp.path(), &config, &mut criterion).unwrap();
        criterion.final_summary();

        assert!(
            output
                .path()
                .join("alpha")
                .join("Q01")
                .join("acceptance")
                .join("estimates.json")
                .exists()
        );
    }

    #[tokio::test]
    async fn run_cli_lists_when_no_benchmark_is_supplied() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let output = run_cli_with_dir(["benchmark_runner"], temp.path())
            .await
            .unwrap();

        assert!(output.contains("SQL benchmarks:"));
        assert!(output.contains("alpha"));
    }

    #[tokio::test]
    async fn run_cli_reports_unknown_benchmark_with_list() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let err = run_cli_with_dir(["benchmark_runner", "missing"], temp.path())
            .await
            .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("unknown benchmark 'missing'"), "{message}");
        assert!(message.contains("alpha"), "{message}");
    }
}
