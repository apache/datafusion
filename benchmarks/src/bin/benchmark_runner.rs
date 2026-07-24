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

use clap::{
    Arg, ArgAction, ArgMatches, Command, CommandFactory, FromArgMatches, Parser,
    ValueEnum,
};
use criterion::Criterion;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_benchmarks::sql_benchmark::SqlBenchmark;
use datafusion_benchmarks::sql_benchmark_runner::{
    BenchmarkFilter, SqlRunConfig, default_sql_benchmark_directory, ensure_selection,
    filter_benchmarks, finish_benchmark, load_benchmark_definitions_for_query, make_ctx,
    prepare_benchmark, run_criterion_benchmarks_impl,
};
use datafusion_benchmarks::sql_benchmark_suite::{
    ReservedOptions, SuiteExample, SuiteMetadata, discover_suites,
};
use datafusion_benchmarks::util::{BenchmarkRun, CommonOpt, print_memory_stats};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, exec_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use serde::{Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
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
    DryRun(DryRunOutput),
}

#[derive(Debug, Serialize)]
struct ResolvedSuiteValue {
    value: String,
    #[serde(serialize_with = "serialize_value_source")]
    source: datafusion_benchmarks::sql_benchmark_suite::ValueSource,
    environment: String,
}

#[derive(Debug, Serialize)]
struct ResolvedPathValue {
    value: String,
    #[serde(serialize_with = "serialize_value_source")]
    source: datafusion_benchmarks::sql_benchmark_suite::ValueSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunMode {
    Simple,
    Criterion,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ResultMode {
    #[default]
    None,
    Persist,
    Validate,
}

impl ResultMode {
    fn config_flags(self) -> (bool, bool) {
        match self {
            Self::None => (false, false),
            Self::Persist => (true, false),
            Self::Validate => (false, true),
        }
    }
}

#[derive(Debug, Serialize)]
struct DryRunCommonOptions {
    iterations: usize,
    partitions: Option<usize>,
    batch_size: Option<usize>,
}

#[derive(Debug, Serialize)]
struct DryRunOutput {
    suite: String,
    query: Option<String>,
    subgroup: Option<String>,
    mode: RunMode,
    result_mode: ResultMode,
    common_options: DryRunCommonOptions,
    suite_options: BTreeMap<String, ResolvedSuiteValue>,
    path_replacements: BTreeMap<String, ResolvedPathValue>,
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

    #[arg(short = 'p', long = "path", value_name = "PATH")]
    path: Option<std::path::PathBuf>,

    #[arg(
        long = "result-mode",
        value_enum,
        value_name = "MODE",
        help = "Handle expected results: none, persist, or validate"
    )]
    result_mode: Option<ResultMode>,

    #[arg(long = "dry-run", action = ArgAction::SetTrue)]
    dry_run: bool,
}

/// Parses CLI arguments, runs the selected action, and prints any   output.
async fn run_cli() -> Result<()> {
    let benchmark_dir = default_sql_benchmark_directory();
    let output = run_cli_from(std::env::args_os(), &benchmark_dir).await?;

    if !output.is_empty() {
        println!("{output}");
    }

    Ok(())
}

fn serialize_value_source<S>(
    source: &datafusion_benchmarks::sql_benchmark_suite::ValueSource,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let value = match source {
        datafusion_benchmarks::sql_benchmark_suite::ValueSource::CommandLine => {
            "command_line"
        }
        datafusion_benchmarks::sql_benchmark_suite::ValueSource::Environment => {
            "environment"
        }
        datafusion_benchmarks::sql_benchmark_suite::ValueSource::Default => "default",
    };
    serializer.serialize_str(value)
}

fn clap_display_output(error: &DataFusionError) -> Option<String> {
    let DataFusionError::External(error) = error else {
        return None;
    };
    let error = error.downcast_ref::<clap::Error>()?;
    matches!(
        error.kind(),
        clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
    )
    .then(|| error.to_string())
}

async fn run_cli_from<I, T>(args: I, benchmark_dir: &Path) -> Result<String>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    match parse_cli_from(args, benchmark_dir) {
        Ok(action) => run_cli_action(action, benchmark_dir).await,
        Err(error) => clap_display_output(&error).ok_or(error),
    }
}

fn format_examples(examples: &[SuiteExample]) -> String {
    if examples.is_empty() {
        return String::new();
    }

    let mut output = String::from("Examples:\n");
    for example in examples {
        output.push_str("  ");
        output.push_str(example.command());
        output.push_str("\n      ");
        output.push_str(example.description());
        output.push('\n');
    }
    output
}

fn build_cli(suite: Option<&SuiteMetadata>) -> Command {
    let mut command = Cli::command();

    if let Some(suite) = suite {
        command = command.about(suite.description().to_string());

        for option in suite.options() {
            let mut arg = Arg::new(option.name().to_string())
                .long(option.name().to_string())
                .help(option.help().to_string())
                .env(option.env().to_string())
                .default_value(option.default().to_string());

            if let Some(short) = option.short() {
                arg = arg.short(short);
            }
            if let Some(values) = option
                .values()
                .filter(|values| !values.iter().any(|value| value == "..."))
            {
                arg = arg.value_parser(values.to_vec());
            }

            command = command.arg(arg);
        }

        let examples = format_examples(suite.examples());

        if !examples.is_empty() {
            command = command.after_help(examples);
        }
    }

    command
}

fn reserved_options() -> (BTreeSet<String>, BTreeSet<char>) {
    let command = Cli::command();
    let long = command
        .get_arguments()
        .filter_map(|arg| arg.get_long().map(ToOwned::to_owned))
        .collect();
    let short = command
        .get_arguments()
        .filter_map(|arg| arg.get_short())
        .collect();
    (long, short)
}

fn suite_metadata(benchmark_dir: &Path) -> Result<Vec<SuiteMetadata>> {
    let (long, short) = reserved_options();
    discover_suites(
        benchmark_dir,
        &ReservedOptions {
            long: &long,
            short: &short,
        },
    )
}

fn format_suite_list(suites: &[SuiteMetadata]) -> String {
    let mut output = String::from("SQL benchmarks:\n");
    for suite in suites {
        let query_word = if suite.benchmark_count() == 1 {
            "query  "
        } else {
            "queries "
        };
        output.push_str(&format!(
            "  {:<24} {} {query_word}{}\n",
            suite.name(),
            suite.benchmark_count(),
            suite.description()
        ));
    }
    output.trim_end().to_string()
}

fn locate_suite_arg(args: &[OsString]) -> Result<Option<&str>> {
    let Some(argument) = args.get(1) else {
        return Ok(None);
    };
    if argument == OsStr::new("--help")
        || argument == OsStr::new("-h")
        || argument == OsStr::new("--list")
        || argument == OsStr::new("--dry-run")
    {
        return Ok(None);
    }
    let suite = argument.to_str().ok_or_else(|| {
        DataFusionError::External("suite name is not valid Unicode".into())
    })?;
    if suite.starts_with('-') {
        return Err(exec_datafusion_err!(
            "suite must be the first argument; options must follow the suite"
        ));
    }
    Ok(Some(suite))
}

fn try_parse_cli_from<I, T>(args: I, benchmark_dir: &Path) -> Result<CliAction>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let suite = locate_suite_arg(&args)?;
    let (long, short) = reserved_options();
    let reserved = ReservedOptions {
        long: &long,
        short: &short,
    };
    let suite = suite
        .map(|name| {
            if !benchmark_dir.join(name).is_dir() {
                let available = discover_suites(benchmark_dir, &reserved)?;
                return Err(exec_datafusion_err!(
                    "unknown benchmark '{name}'\n\n{}",
                    format_suite_list(&available)
                ));
            }
            SuiteMetadata::load(benchmark_dir, name, &reserved)
        })
        .transpose()?;
    let matches = build_cli(suite.as_ref())
        .try_get_matches_from(args)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;

    cli_action_from_matches(&matches, suite.as_ref())
}

fn parse_cli_from<I, T>(args: I, benchmark_dir: &Path) -> Result<CliAction>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    try_parse_cli_from(args, benchmark_dir)
}

/// Converts parsed arguments into an executable action and validates mode options.
fn cli_action_from_matches(
    matches: &ArgMatches,
    suite: Option<&SuiteMetadata>,
) -> Result<CliAction> {
    let cli = Cli::from_arg_matches(matches)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    if cli.dry_run && cli.list {
        return Err(exec_datafusion_err!("--list cannot be used with --dry-run"));
    }
    if cli.dry_run && cli.benchmark.is_none() {
        return Err(exec_datafusion_err!("--dry-run requires a benchmark suite"));
    }
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
    if !cli.criterion && cli.common.iterations == 0 {
        return Err(exec_datafusion_err!("iterations must be greater than zero"));
    }

    // we need to know if iterations was set on the command line, not the default value
    let iterations_from_cli = matches.value_source("iterations")
        == Some(clap::parser::ValueSource::CommandLine);

    if cli.criterion && iterations_from_cli {
        return Err(exec_datafusion_err!(
            "--iterations cannot be used with --criterion"
        ));
    }

    let suite =
        suite.ok_or_else(|| exec_datafusion_err!("benchmark suite is required"))?;

    if cli.path.is_some() && !suite.path_replacements().contains_key("DATA_DIR") {
        return Err(exec_datafusion_err!(
            "--path cannot be used because suite '{}' does not declare DATA_DIR",
            suite.name()
        ));
    }

    let result_mode = resolve_result_mode(cli.result_mode)?;
    let (persist_results, validate_results) = result_mode.config_flags();
    let mut config = SqlRunConfig {
        common: cli.common,
        filter: BenchmarkFilter {
            name: cli.benchmark,
            subgroup: cli.subgroup,
            query: cli.query,
        },
        replacements: Default::default(),
        query_filename: None,
        persist_results,
        validate_results,
        output: cli.output,
    };
    let suite_options: BTreeMap<String, ResolvedSuiteValue> = suite
        .options()
        .iter()
        .map(|option| {
            let value = matches
                .get_one::<String>(option.name())
                .expect("suite options always have defaults")
                .clone();
            let source = match matches.value_source(option.name()) {
                Some(clap::parser::ValueSource::CommandLine) => {
                    datafusion_benchmarks::sql_benchmark_suite::ValueSource::CommandLine
                }
                Some(clap::parser::ValueSource::EnvVariable) => {
                    datafusion_benchmarks::sql_benchmark_suite::ValueSource::Environment
                }
                Some(clap::parser::ValueSource::DefaultValue) => {
                    datafusion_benchmarks::sql_benchmark_suite::ValueSource::Default
                }
                vs => unreachable!("unexpected suite option source: {vs:?}"),
            };
            (
                option.name().to_string(),
                ResolvedSuiteValue {
                    value,
                    source,
                    environment: option.env().to_string(),
                },
            )
        })
        .collect();
    let path_replacements: BTreeMap<String, ResolvedPathValue> = suite
        .path_replacements()
        .iter()
        .map(|(key, default)| {
            let (value, source) = if key == "DATA_DIR" {
                cli.path.as_ref().map_or_else(
                    || {
                        (
                            default.display().to_string(),
                            datafusion_benchmarks::sql_benchmark_suite::ValueSource::Default,
                        )
                    },
                    |path| {
                        (
                            path.display().to_string(),
                            datafusion_benchmarks::sql_benchmark_suite::ValueSource::CommandLine,
                        )
                    },
                )
            } else {
                (
                    default.display().to_string(),
                    datafusion_benchmarks::sql_benchmark_suite::ValueSource::Default,
                )
            };
            (
                key.to_ascii_lowercase(),
                ResolvedPathValue { value, source },
            )
        })
        .collect();

    config.replacements = suite_options
        .values()
        .map(|resolved| {
            (
                resolved.environment.to_ascii_lowercase(),
                resolved.value.clone(),
            )
        })
        .chain(
            path_replacements
                .iter()
                .map(|(key, resolved)| (key.clone(), resolved.value.clone())),
        )
        .collect();
    config.query_filename = config
        .filter
        .query
        .as_deref()
        .map(|query| suite.query_filename(query))
        .transpose()?;

    if cli.dry_run {
        let mode = if cli.criterion {
            RunMode::Criterion
        } else {
            RunMode::Simple
        };
        return Ok(CliAction::DryRun(DryRunOutput {
            suite: suite.name().to_string(),
            query: config.filter.query.clone(),
            subgroup: config.filter.subgroup.clone(),
            mode,
            result_mode,
            common_options: DryRunCommonOptions {
                iterations: config.common.iterations,
                partitions: config.common.partitions,
                batch_size: config.common.batch_size,
            },
            suite_options,
            path_replacements,
        }));
    }

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
        CliAction::List => Ok(format_suite_list(&suite_metadata(benchmark_dir)?)),
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
        CliAction::DryRun(output) => serde_json::to_string_pretty(&output)
            .map_err(|error| DataFusionError::External(Box::new(error))),
    }
}

fn resolve_result_mode(explicit: Option<ResultMode>) -> Result<ResultMode> {
    if let Some(mode) = explicit {
        return Ok(mode);
    }

    let persist = parse_compat_bool("BENCH_PERSIST_RESULTS")?;
    let validate = parse_compat_bool("BENCH_VALIDATE")?;

    Ok(if persist {
        ResultMode::Persist
    } else if validate {
        ResultMode::Validate
    } else {
        ResultMode::None
    })
}

fn parse_compat_bool(name: &str) -> Result<bool> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(false);
    };
    let value = value
        .into_string()
        .map_err(|_| exec_datafusion_err!("{name} contains invalid UTF-8"))?;

    value.parse::<bool>().map_err(|_| {
        exec_datafusion_err!("invalid value '{value}' for {name}; expected true or false")
    })
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
    let all_benchmarks = load_benchmark_definitions_for_query(
        &config.filter,
        &listing_ctx,
        benchmark_dir,
        &config.replacements,
        config.query_filename.as_deref(),
    )
    .await?;
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
    use datafusion_benchmarks::sql_benchmark_runner::{
        load_benchmark_definitions, sort_benchmarks, unknown_benchmark_error,
    };
    use datafusion_benchmarks::sql_benchmark_suite::ValueSource;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    struct ScopedEnv {
        previous: Vec<(&'static str, Option<OsString>)>,
        _lock: MutexGuard<'static, ()>,
    }

    impl ScopedEnv {
        fn set(name: &'static str, value: impl Into<OsString>) -> Self {
            let lock = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
            let previous = std::env::var_os(name);
            // SAFETY: ENV_MUTEX serializes changes made through ScopedEnv in this
            // test module; it does not synchronize environment access elsewhere.
            unsafe { std::env::set_var(name, value.into()) };
            Self {
                previous: vec![(name, previous)],
                _lock: lock,
            }
        }

        fn remove(name: &'static str) -> Self {
            let lock = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
            let previous = std::env::var_os(name);
            // SAFETY: ENV_MUTEX serializes changes made through ScopedEnv in this
            // test module; it does not synchronize environment access elsewhere.
            unsafe { std::env::remove_var(name) };
            Self {
                previous: vec![(name, previous)],
                _lock: lock,
            }
        }

        fn set_many<const N: usize>(changes: [(&'static str, Option<&str>); N]) -> Self {
            let lock = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
            let mut previous = Vec::with_capacity(N);
            for (name, value) in changes {
                previous.push((name, std::env::var_os(name)));
                // SAFETY: this guard holds ENV_MUTEX until it restores all entries.
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
            Self {
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for ScopedEnv {
        fn drop(&mut self) {
            // SAFETY: this guard holds ENV_MUTEX until after all entries are restored.
            unsafe {
                for (name, previous) in self.previous.drain(..).rev() {
                    match previous {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    /// Loads benchmark definitions, applies CLI-style filters, and sorts each group.
    async fn load_benchmarks(
        filter: &BenchmarkFilter,
        ctx: &SessionContext,
        benchmark_dir: &Path,
    ) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
        let benches =
            load_benchmark_definitions(filter, ctx, benchmark_dir, &Default::default())
                .await?;
        let mut benches = filter_benchmarks(filter, benches);

        sort_benchmarks(&mut benches);

        Ok(benches)
    }

    fn write_benchmark(root: &Path, relative_path: &str, contents: &str) -> PathBuf {
        let path = root.join(relative_path);

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, contents).unwrap();

        path
    }

    fn write_suite(root: &Path, name: &str, description: &str) -> PathBuf {
        let path = root.join(name).join(format!("{name}.suite"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, format!("description = {description:?}\n")).unwrap();
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

    async fn run_cli_with_dir<I, T>(args: I, benchmark_dir: &Path) -> Result<String>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        run_cli_from(args, benchmark_dir).await
    }

    fn suite_root() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        fs::write(
            temp.path().join("alpha/alpha.suite"),
            r#"description = "Alpha benchmark"

[path_replacements]
DATA_DIR = "data"

[[options]]
name = "format"
short = "f"
env = "ALPHA_FORMAT"
default = "parquet"
values = ["parquet", "csv"]
help = "Alpha input format"

[[examples]]
command = "benchmark_runner alpha -q 1 -f csv"
description = "Run query one against CSV data."
"#,
        )
        .unwrap();
        temp
    }

    #[test]
    fn suite_help_contains_metadata() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let error =
            try_parse_cli_from(["benchmark_runner", "alpha", "--help"], temp.path())
                .unwrap_err();
        let help = error.to_string();

        assert!(help.contains("Alpha benchmark"), "{help}");
        assert!(help.contains("--format"), "{help}");
        assert!(help.contains("ALPHA_FORMAT"), "{help}");
        assert!(
            help.contains("benchmark_runner alpha -q 1 -f csv"),
            "{help}"
        );
    }

    #[test]
    fn accepts_interleaved_named_options() {
        let temp = suite_root();
        let action = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--partitions",
                "2",
                "--format",
                "csv",
                "--query",
                "5",
            ],
            temp.path(),
        )
        .unwrap();
        let CliAction::Simple(config) = action else {
            panic!("expected simple run")
        };

        assert_eq!(config.common.partitions, Some(2));
        assert_eq!(config.filter.query.as_deref(), Some("5"));
        assert_eq!(config.query_filename.as_deref(), Some("q05.benchmark"));
        assert_eq!(config.replacements["alpha_format"], "csv");
        assert_eq!(
            config.replacements["data_dir"],
            temp.path().join("alpha/data").display().to_string()
        );
    }

    #[test]
    fn dry_run_uses_suite_default() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let action =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap();
        let CliAction::DryRun(output) = action else {
            panic!("expected dry run")
        };

        assert_eq!(output.suite_options["format"].value, "parquet");
        assert_eq!(output.suite_options["format"].source, ValueSource::Default);
    }

    #[test]
    fn result_mode_defaults_to_none() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", None),
            ("BENCH_VALIDATE", None),
        ]);
        let temp = suite_root();
        let CliAction::DryRun(output) =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap()
        else {
            panic!("expected dry run");
        };
        assert_eq!(output.result_mode, ResultMode::None);
    }

    #[tokio::test]
    async fn result_mode_persist_writes_expected_results() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", None),
            ("BENCH_VALIDATE", None),
        ]);
        let temp = suite_root();
        let result_path = temp.path().join("expected.csv");
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            &format!(
                "name Q01\ngroup alpha\n\nresult {}\n\nrun\nSELECT 1 AS value\n",
                result_path.display()
            ),
        );

        run_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "1",
                "--result-mode",
                "persist",
            ],
            temp.path(),
        )
        .await
        .unwrap();

        let persisted = fs::read_to_string(result_path).unwrap();
        assert!(persisted.contains("value"), "{persisted}");
        assert!(persisted.contains('1'), "{persisted}");
    }

    #[tokio::test]
    async fn result_mode_validate_accepts_expected_results() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", None),
            ("BENCH_VALIDATE", None),
        ]);
        let temp = suite_root();
        let result_path = temp.path().join("expected.csv");
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            &format!(
                "name Q01\ngroup alpha\n\nresult {}\n\nrun\nSELECT 1 AS value\n",
                result_path.display()
            ),
        );
        fs::write(&result_path, "value\n1\n").unwrap();

        run_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "1",
                "--result-mode",
                "validate",
            ],
            temp.path(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn result_mode_validate_reports_mismatched_results() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", None),
            ("BENCH_VALIDATE", None),
        ]);
        let temp = suite_root();
        let result_path = temp.path().join("expected.csv");
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            &format!(
                "name Q01\ngroup alpha\n\nresult {}\n\nrun\nSELECT 1 AS value\n",
                result_path.display()
            ),
        );
        fs::write(&result_path, "value\n2\n").unwrap();

        let error = run_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "1",
                "--result-mode",
                "validate",
            ],
            temp.path(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("expected value"), "{error}");
    }

    #[test]
    fn explicit_result_modes_populate_config() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", Some("invalid")),
            ("BENCH_VALIDATE", Some("invalid")),
        ]);
        let temp = suite_root();
        for (value, expected, persist, validate) in [
            ("none", ResultMode::None, false, false),
            ("persist", ResultMode::Persist, true, false),
            ("validate", ResultMode::Validate, false, true),
        ] {
            let CliAction::DryRun(output) = parse_cli_from(
                [
                    "benchmark_runner",
                    "alpha",
                    "--result-mode",
                    value,
                    "--dry-run",
                ],
                temp.path(),
            )
            .unwrap() else {
                panic!("expected dry run");
            };
            assert_eq!(output.result_mode, expected);

            let CliAction::Simple(config) = parse_cli_from(
                ["benchmark_runner", "alpha", "--result-mode", value],
                temp.path(),
            )
            .unwrap() else {
                panic!("expected simple run");
            };
            assert_eq!(config.persist_results, persist);
            assert_eq!(config.validate_results, validate);
        }
    }

    #[test]
    fn compatibility_environment_resolves_result_mode() {
        for (persist, validate, expected) in [
            (Some("true"), None, ResultMode::Persist),
            (None, Some("true"), ResultMode::Validate),
            (Some("true"), Some("true"), ResultMode::Persist),
            (Some("false"), Some("false"), ResultMode::None),
        ] {
            let _env = ScopedEnv::set_many([
                ("BENCH_PERSIST_RESULTS", persist),
                ("BENCH_VALIDATE", validate),
            ]);
            let temp = suite_root();
            let CliAction::DryRun(output) =
                parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                    .unwrap()
            else {
                panic!("expected dry run");
            };
            assert_eq!(output.result_mode, expected);
        }
    }

    #[test]
    fn invalid_result_mode_environment_is_rejected_without_cli_override() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", Some("invalid")),
            ("BENCH_VALIDATE", Some("false")),
        ]);
        let temp = suite_root();
        let error =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap_err();
        assert!(
            error.to_string().contains("BENCH_PERSIST_RESULTS"),
            "{error}"
        );
    }

    #[test]
    fn invalid_result_mode_cli_value_lists_allowed_values() {
        let _env = ScopedEnv::set_many([
            ("BENCH_PERSIST_RESULTS", None),
            ("BENCH_VALIDATE", None),
        ]);
        let temp = suite_root();
        let error = parse_cli_from(
            ["benchmark_runner", "alpha", "--result-mode", "invalid"],
            temp.path(),
        )
        .unwrap_err();
        let message = error.to_string();
        for allowed in ["none", "persist", "validate"] {
            assert!(message.contains(allowed), "{message}");
        }
    }

    #[test]
    fn environment_beats_suite_default() {
        let _env = ScopedEnv::set("ALPHA_FORMAT", "csv");
        let temp = suite_root();
        let action =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap();
        let CliAction::DryRun(output) = action else {
            panic!("expected dry run")
        };

        assert_eq!(output.suite_options["format"].value, "csv");
        assert_eq!(
            output.suite_options["format"].source,
            ValueSource::Environment
        );
    }

    #[test]
    fn cli_equals_syntax_beats_environment_and_default() {
        let _env = ScopedEnv::set("ALPHA_FORMAT", "parquet");
        let temp = suite_root();
        let action = parse_cli_from(
            ["benchmark_runner", "alpha", "--format=csv", "--dry-run"],
            temp.path(),
        )
        .unwrap();
        let CliAction::DryRun(output) = action else {
            panic!("expected dry run")
        };

        assert_eq!(output.suite_options["format"].value, "csv");
        assert_eq!(
            output.suite_options["format"].source,
            ValueSource::CommandLine
        );
    }

    #[test]
    fn empty_environment_value_is_validated() {
        let _env = ScopedEnv::set("ALPHA_FORMAT", "");
        let temp = suite_root();
        let error =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("a value is required"), "{message}");
        assert!(message.contains("parquet, csv"), "{message}");
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_value_is_rejected_by_clap() {
        use std::os::unix::ffi::OsStringExt;

        let _env = ScopedEnv::set("ALPHA_FORMAT", OsString::from_vec(vec![0xff]));
        let temp = suite_root();
        let error =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap_err();

        assert!(error.to_string().contains("invalid UTF-8"), "{error}");
    }

    #[test]
    fn attached_short_cli_value_beats_invalid_environment() {
        let _env = ScopedEnv::set("ALPHA_FORMAT", "invalid");
        let temp = suite_root();
        let action = parse_cli_from(
            ["benchmark_runner", "alpha", "-fcsv", "--dry-run"],
            temp.path(),
        )
        .unwrap();
        let CliAction::DryRun(output) = action else {
            panic!("expected dry run")
        };

        assert_eq!(output.suite_options["format"].value, "csv");
        assert_eq!(
            output.suite_options["format"].source,
            ValueSource::CommandLine
        );
    }

    #[test]
    fn invalid_suite_environment_does_not_block_help() {
        let _env = ScopedEnv::set("ALPHA_FORMAT", "invalid");
        let temp = suite_root();
        let error =
            try_parse_cli_from(["benchmark_runner", "alpha", "--help"], temp.path())
                .unwrap_err();
        let help = error.to_string();

        assert!(help.contains("Alpha benchmark"), "{help}");
        assert!(help.contains("--format"), "{help}");
    }

    #[test]
    fn dry_run_rejects_list_instead_of_listing() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let error = parse_cli_from(
            ["benchmark_runner", "alpha", "--list", "--dry-run"],
            temp.path(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("--list"), "{error}");
        assert!(error.to_string().contains("--dry-run"), "{error}");
    }

    #[test]
    fn dry_run_requires_suite() {
        let temp = suite_root();
        let error =
            parse_cli_from(["benchmark_runner", "--dry-run"], temp.path()).unwrap_err();

        assert!(error.to_string().contains("--dry-run"), "{error}");
        assert!(error.to_string().contains("suite"), "{error}");
    }

    #[test]
    fn dry_run_resolves_default_and_overridden_path() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let default_action =
            parse_cli_from(["benchmark_runner", "alpha", "--dry-run"], temp.path())
                .unwrap();
        let CliAction::DryRun(default_output) = default_action else {
            panic!("expected dry run")
        };
        assert_eq!(
            default_output.path_replacements["data_dir"].source,
            ValueSource::Default
        );

        let override_action = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--path",
                "/tmp/alpha-data",
                "--dry-run",
            ],
            temp.path(),
        )
        .unwrap();
        let CliAction::DryRun(override_output) = override_action else {
            panic!("expected dry run")
        };
        assert_eq!(
            override_output.path_replacements["data_dir"].value,
            "/tmp/alpha-data"
        );
        assert_eq!(
            override_output.path_replacements["data_dir"].source,
            ValueSource::CommandLine
        );
    }

    #[test]
    fn path_is_rejected_without_data_dir_replacement() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        fs::write(
            temp.path().join("alpha/alpha.suite"),
            fs::read_to_string(temp.path().join("alpha/alpha.suite"))
                .unwrap()
                .replace("[path_replacements]\nDATA_DIR = \"data\"\n\n", ""),
        )
        .unwrap();

        let error =
            parse_cli_from(["benchmark_runner", "alpha", "--path", "data"], temp.path())
                .unwrap_err();
        assert!(error.to_string().contains("--path"), "{error}");
        assert!(error.to_string().contains("DATA_DIR"), "{error}");
    }

    #[test]
    fn criterion_dry_run_reports_mode_and_keeps_cross_checks() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let action = parse_cli_from(
            ["benchmark_runner", "alpha", "--criterion", "--dry-run"],
            temp.path(),
        )
        .unwrap();
        let CliAction::DryRun(output) = action else {
            panic!("expected dry run")
        };
        assert_eq!(output.mode, RunMode::Criterion);

        let error = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--criterion",
                "--iterations",
                "2",
                "--dry-run",
            ],
            temp.path(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("--iterations"), "{error}");
    }

    #[test]
    fn dry_run_rejects_invalid_query_form() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let error = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "../secret",
                "--dry-run",
            ],
            temp.path(),
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("invalid query identifier"),
            "{error}"
        );
    }

    #[test]
    fn uppercase_q_dry_run_uses_same_query_filename_as_lowercase_q() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();

        assert!(matches!(
            parse_cli_from(
                ["benchmark_runner", "alpha", "--query", "Q1", "--dry-run",],
                temp.path(),
            )
            .unwrap(),
            CliAction::DryRun(_)
        ));

        let filename = |query| {
            let CliAction::Simple(config) = parse_cli_from(
                ["benchmark_runner", "alpha", "--query", query],
                temp.path(),
            )
            .unwrap() else {
                panic!("expected simple run")
            };
            config.query_filename
        };

        assert_eq!(filename("Q1"), filename("q1"));
    }

    #[tokio::test]
    async fn dry_run_returns_deterministic_json_without_reading_benchmarks() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        fs::write(
            temp.path().join("alpha/benchmarks/q01.benchmark"),
            "this benchmark is intentionally invalid",
        )
        .unwrap();

        let output = run_cli_with_dir(
            [
                "benchmark_runner",
                "alpha",
                "--query",
                "7",
                "--partitions",
                "2",
                "--dry-run",
            ],
            temp.path(),
        )
        .await
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(json["suite"], "alpha");
        assert_eq!(json["query"], "7");
        assert_eq!(json["mode"], "simple");
        assert_eq!(json["common_options"]["partitions"], 2);
        assert_eq!(json["suite_options"]["format"]["source"], "default");
    }

    #[test]
    fn cli_lists_when_benchmark_is_omitted() {
        let temp = suite_root();
        let action = parse_cli_from(["benchmark_runner"], temp.path()).unwrap();

        assert!(matches!(action, CliAction::List));
    }

    #[test]
    fn cli_lists_with_explicit_list_flag() {
        let temp = suite_root();
        let action = parse_cli_from(["benchmark_runner", "--list"], temp.path()).unwrap();

        assert!(matches!(action, CliAction::List));
    }

    #[test]
    fn cli_defaults_to_basic_runner() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let action =
            parse_cli_from(["benchmark_runner", "alpha", "--query", "1"], temp.path())
                .unwrap();
        let CliAction::Simple(config) = action else {
            panic!("expected basic runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("alpha"));
        assert_eq!(config.filter.query.as_deref(), Some("1"));
    }

    #[test]
    fn cli_reads_query_from_env() {
        let _env = ScopedEnv::set("BENCH_QUERY", "8");
        let temp = suite_root();
        let action = parse_cli_from(
            ["benchmark_runner", "alpha", "--format", "parquet"],
            temp.path(),
        );
        let action = action.unwrap();
        let CliAction::Simple(config) = action else {
            panic!("expected basic runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("alpha"));
        assert_eq!(config.filter.query.as_deref(), Some("8"));
    }

    #[test]
    fn cli_accepts_criterion_runner() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let action = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--criterion",
                "--save-baseline",
                "main",
            ],
            temp.path(),
        )
        .unwrap();

        let CliAction::Criterion {
            config,
            save_baseline,
        } = action
        else {
            panic!("expected criterion runner");
        };

        assert_eq!(config.filter.name.as_deref(), Some("alpha"));
        assert_eq!(save_baseline.as_deref(), Some("main"));
    }

    #[test]
    fn cli_rejects_output_with_criterion() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let err = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--criterion",
                "--output",
                "results.json",
            ],
            temp.path(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("--output"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_save_baseline_without_criterion() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let err = parse_cli_from(
            ["benchmark_runner", "alpha", "--save-baseline", "main"],
            temp.path(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("--save-baseline"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_iterations_with_criterion() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let err = parse_cli_from(
            [
                "benchmark_runner",
                "alpha",
                "--criterion",
                "--iterations",
                "3",
            ],
            temp.path(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("--iterations"));
        assert!(err.to_string().contains("--criterion"));
    }

    #[test]
    fn cli_rejects_zero_basic_iterations() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let err = parse_cli_from(
            ["benchmark_runner", "alpha", "--iterations", "0"],
            temp.path(),
        )
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
        write_suite(temp.path(), "alpha", "Alpha workload");

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
        write_suite(temp.path(), "alpha", "Alpha workload");

        let output = run_cli_with_dir(["benchmark_runner", "--list"], temp.path())
            .await
            .unwrap();

        assert!(output.contains("SQL benchmarks:"));
        assert!(output.contains("alpha"));
    }

    #[tokio::test]
    async fn run_cli_top_level_help_is_successful_output() {
        let temp = suite_root();
        let output = run_cli_with_dir(["benchmark_runner", "--help"], temp.path())
            .await
            .unwrap();

        assert!(output.contains("Run DataFusion SQL benchmarks"), "{output}");
        assert!(output.contains("Usage:"), "{output}");
    }

    #[tokio::test]
    async fn run_cli_suite_help_is_successful_output() {
        let _env = ScopedEnv::remove("ALPHA_FORMAT");
        let temp = suite_root();
        let output =
            run_cli_with_dir(["benchmark_runner", "alpha", "--help"], temp.path())
                .await
                .unwrap();

        assert!(output.contains("Alpha benchmark"), "{output}");
        assert!(output.contains("--format"), "{output}");
    }

    #[tokio::test]
    async fn run_cli_real_parse_error_remains_an_error() {
        let temp = suite_root();
        let error = run_cli_with_dir(
            ["benchmark_runner", "alpha", "--not-an-option"],
            temp.path(),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("unexpected argument"), "{error}");
    }

    #[tokio::test]
    async fn run_cli_reports_unknown_benchmark_with_list() {
        let temp = suite_root();

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
            replacements: HashMap::new(),
            query_filename: None,
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
            replacements: HashMap::new(),
            query_filename: None,
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
            replacements: HashMap::new(),
            query_filename: None,
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
            replacements: HashMap::new(),
            query_filename: None,
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
            replacements: HashMap::new(),
            query_filename: None,
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
    async fn benchmark_replacements_use_explicit_data_dir() {
        let temp = tempfile::tempdir().unwrap();

        write_benchmark(
            temp.path(),
            "clickbench/benchmarks/q01.benchmark",
            "name Q01\nsubgroup ${DATA_DIR:-data}\n\nrun\nSELECT 1\n",
        );

        let ctx = SessionContext::new();
        let expected = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .to_string_lossy()
            .into_owned();
        let replacements = HashMap::from([("data_dir".to_string(), expected.clone())]);
        let benches = load_benchmark_definitions(
            &BenchmarkFilter::default(),
            &ctx,
            temp.path(),
            &replacements,
        )
        .await
        .unwrap();

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
    async fn list_output_is_sorted_and_includes_counts_and_descriptions() {
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
            "beta/benchmarks/q02.benchmark",
            "name Q02\n\nrun\nSELECT 2\n",
        );
        write_suite(temp.path(), "alpha", "Alpha workload");
        write_suite(temp.path(), "beta", "Beta workload");

        let output = run_cli_with_dir(["benchmark_runner", "--list"], temp.path())
            .await
            .unwrap();

        assert_eq!(
            output,
            "SQL benchmarks:\n  alpha                    1 query  Alpha workload\n  beta                     2 queries Beta workload"
        );
    }

    #[tokio::test]
    async fn list_does_not_parse_benchmark_sql() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "not valid benchmark syntax",
        );
        write_suite(temp.path(), "alpha", "Alpha workload");

        let output = run_cli_with_dir(["benchmark_runner", "--list"], temp.path())
            .await
            .unwrap();

        assert_eq!(
            output,
            "SQL benchmarks:\n  alpha                    1 query  Alpha workload"
        );
    }

    #[tokio::test]
    async fn list_malformed_metadata_names_its_file() {
        let temp = tempfile::tempdir().unwrap();
        write_benchmark(
            temp.path(),
            "alpha/benchmarks/q01.benchmark",
            "name Q01\n\nrun\nSELECT 1\n",
        );
        let metadata_path = temp.path().join("alpha/alpha.suite");
        fs::write(&metadata_path, "not valid metadata").unwrap();

        let error = run_cli_with_dir(["benchmark_runner", "--list"], temp.path())
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&metadata_path.display().to_string()),
            "{error}"
        );
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
