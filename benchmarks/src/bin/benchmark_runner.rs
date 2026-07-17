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

//! DataFusion SQL benchmark runner.

use clap::{ArgAction, ArgMatches, CommandFactory, FromArgMatches, Parser};
use criterion::Criterion;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_benchmarks::sql_benchmark::SqlBenchmark;
use datafusion_benchmarks::sql_benchmark_runner::{
    BenchmarkFilter, SqlRunConfig, default_sql_benchmark_directory, ensure_selection,
    filter_benchmarks, finish_benchmark, format_benchmark_list,
    load_benchmark_definitions, make_ctx, prepare_benchmark,
    run_criterion_benchmarks_impl, sort_benchmarks,
};
use datafusion_benchmarks::util::{BenchmarkRun, CommonOpt, print_memory_stats};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, exec_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::Path;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();
    if let Err(error) = run_cli().await {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

#[derive(Debug)]
enum CliAction {
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
struct Cli {
    #[arg(value_name = "BENCHMARK", help = "SQL benchmark group to run")]
    benchmark: Option<String>,

    #[arg(short = 'q', long = "query", env = "BENCH_QUERY")]
    query: Option<String>,

    #[arg(long = "subgroup", env = "BENCH_SUBGROUP")]
    subgroup: Option<String>,

    #[command(flatten)]
    common: CommonOpt,

    #[arg(
        long = "criterion",
        action = ArgAction::SetTrue,
        help = "Run benchmarks with Criterion"
    )]
    criterion: bool,

    #[arg(
        long = "list",
        action = ArgAction::SetTrue,
        help = "List available SQL benchmark groups"
    )]
    list: bool,

    #[arg(
        short = 'o',
        long = "output",
        help = "Write simple runner results as JSON to this path"
    )]
    output: Option<std::path::PathBuf>,

    #[arg(
        long = "save-baseline",
        value_name = "BASELINE",
        help = "Save Criterion measurements to the named baseline"
    )]
    save_baseline: Option<String>,
}

/// Parses CLI arguments, runs the selected action, and prints any   output.
async fn run_cli() -> Result<()> {
    let matches = Cli::command().get_matches();
    let action = cli_action_from_matches(&matches)?;
    let output = run_cli_action(action, &default_sql_benchmark_directory()).await?;

    if !output.is_empty() {
        println!("{output}");
    }

    Ok(())
}

/// Converts parsed arguments into an executable action and validates mode options.
fn cli_action_from_matches(matches: &ArgMatches) -> Result<CliAction> {
    let cli = Cli::from_arg_matches(matches)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if cli.list || cli.benchmark.is_none() {
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

/// Loads benchmark definitions, applies CLI-style filters, and sorts each group.
pub async fn load_benchmarks(
    filter: &BenchmarkFilter,
    ctx: &SessionContext,
    benchmark_dir: &Path,
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    let benches = load_benchmark_definitions(filter, ctx, benchmark_dir).await?;
    let mut benches = filter_benchmarks(filter, benches);

    sort_benchmarks(&mut benches);

    Ok(benches)
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

/// Runs selected benchmarks with fixed iteration counts and optional JSON output.
pub async fn run_simple_benchmarks(
    benchmark_dir: &Path,
    config: SqlRunConfig,
) -> Result<()> {
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

fn benchmark_case_name(benchmark: &SqlBenchmark) -> String {
    let mut name = format!("{}/{}", benchmark.group(), benchmark.name());

    if !benchmark.subgroup().is_empty() {
        name.push('/');
        name.push_str(benchmark.subgroup());
    }

    name
}

fn criterion_like_styles() -> clap::builder::Styles {
    use clap::builder::styling::AnsiColor;

    clap::builder::Styles::styled()
        .header(AnsiColor::Green.on_default().bold())
        .usage(AnsiColor::Green.on_default().bold())
        .literal(AnsiColor::Cyan.on_default().bold())
        .placeholder(AnsiColor::Cyan.on_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_benchmarks::sql_benchmark_runner::unknown_benchmark_error;
    use std::fs;
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

        cli_action_from_matches(&matches)
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
    fn cli_lists_with_explicit_list_flag() {
        let action = parse_cli_from(["benchmark_runner", "--list"]).unwrap();

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
    async fn run_cli_lists_with_explicit_list_flag() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let output = run_cli_with_dir(["benchmark_runner", "--list"], temp.path())
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
    async fn simple_runner_reports_unknown_query_for_known_benchmark() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );

        let config = SqlRunConfig {
            common: common(1),
            filter: BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: None,
                query: Some("9".to_string()),
            },
            persist_results: false,
            validate_results: false,
            output: None,
        };
        let err = run_simple_benchmarks(temp.path(), config)
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
    async fn simple_runner_reports_unknown_subgroup_for_known_benchmark() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\nsubgroup wide\n\nrun\nSELECT 1\n",
        );

        let config = SqlRunConfig {
            common: common(1),
            filter: BenchmarkFilter {
                name: Some("alpha".to_string()),
                subgroup: Some("narrow".to_string()),
                query: None,
            },
            persist_results: false,
            validate_results: false,
            output: None,
        };
        let err = run_simple_benchmarks(temp.path(), config)
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
    async fn benchmark_replacements_default_data_dir_to_benchmarks_data() {
        let temp = tempfile::tempdir().unwrap();
        let previous = std::env::var_os("DATA_DIR");

        unsafe {
            std::env::remove_var("DATA_DIR");
        }

        write_benchmark(
            temp.path(),
            "clickbench/benchmarks/q01.benchmark",
            "name Q01\nsubgroup ${DATA_DIR:-data}\n\nrun\nSELECT 1\n",
        );

        let ctx = SessionContext::new();
        let benches =
            load_benchmarks(&BenchmarkFilter::default(), &ctx, temp.path()).await;

        unsafe {
            match previous {
                Some(value) => std::env::set_var("DATA_DIR", value),
                None => std::env::remove_var("DATA_DIR"),
            }
        }

        let benches = benches.unwrap();

        let expected = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .to_string_lossy()
            .into_owned();

        assert_eq!(benches["clickbench"][0].subgroup(), expected);
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
}
