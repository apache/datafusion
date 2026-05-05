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

//! Command-line inspection for DataFusion SQL and native benchmarks.
//!
//! This module backs the `benchmark_runner` binary. SQL suites and their
//! configurable options are discovered from text `.suite` files, individual
//! `.benchmark` files are parsed through [`SqlBenchmark`], and selected
//! benchmarks can be inspected as metadata or parsed SQL. Native benchmarks are
//! exposed through their existing Rust `RunOpt` metadata while they are migrated
//! to SQL suites.
//!
//! Common invocations:
//!
//! ```text
//! cargo run --bin benchmark_runner -- --help
//! cargo run --release --bin benchmark_runner -- list
//! cargo run --release --bin benchmark_runner -- info tpch
//! cargo run --release --bin benchmark_runner -- info tpch 15
//! cargo run --release --bin benchmark_runner -- info hj
//! cargo run --release --bin benchmark_runner -- query tpch 15
//! ```
//!
//! In suite-based `query` commands, the final numeric argument is the benchmark
//! query id. For example, `15` targets `q15.benchmark`, and `1` or `01` targets
//! `q01.benchmark`. For `info`, the query id is optional:
//! `info tpch` reports suite metadata, while `info tpch 15` also reports
//! metadata for `q15.benchmark`.
//!
//! The public entry point is [`run_cli`]. The submodules are kept private so
//! the command-line flow remains the single supported API:
//!
//! - `cli` builds the clap command tree, parses inspection flags, and handles
//!   dynamic suite options from suite files.
//! - `native` describes Rust `RunOpt` benchmarks, filters entries shadowed by
//!   SQL suites, and formats native benchmark metadata.
//! - `suite` loads `.suite` metadata, validates suite options, discovers
//!   benchmark query files recursively, and maps chosen options to SQL
//!   replacements.
//! - `selector` resolves a suite name and optional query id into the concrete
//!   benchmark files to inspect.
//! - `output` formats colored `list`, `info`, and `query` command output.

mod cli;
mod native;
mod output;
mod selector;
mod style;
mod suite;

use crate::benchmark_runner::cli::{
    InspectArgs, RunnerCommand, SuiteCliOptions, build_cli_for_args_with_options,
    command_from_matches_with_options, format_sql_run_help_sections_styled,
    normalize_suite_option_aliases_with_options,
};
use crate::benchmark_runner::native::{NativeBenchmark, NativeBenchmarks};
use crate::benchmark_runner::output::{
    format_benchmark_list_styled, write_info_styled, write_queries_styled,
    write_suite_info_styled,
};
use crate::benchmark_runner::selector::{ResolvedBenchmarkTarget, ResolvedSuite};
use crate::benchmark_runner::suite::SuiteRegistry;
use crate::sql_benchmark::SqlBenchmark;
use clap::error::ErrorKind;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Runs the benchmark runner command-line flow for the provided argument list.
///
/// This discovers suite metadata, normalizes suite option aliases, builds the
/// appropriate clap command for the requested help context, and dispatches to
/// the selected `list`, `info`, or `query` implementation.
pub async fn run_cli<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let benchmark_dir = default_benchmark_dir();
    let registry = SuiteRegistry::discover(&benchmark_dir)?;
    let native = NativeBenchmarks::new(&registry);
    let suite_cli_options = SuiteCliOptions::new(&registry);
    let args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    let args = normalize_suite_option_aliases_with_options(&suite_cli_options, args);

    let root_help_requested = is_root_help_request(&args);
    let mut cli =
        build_cli_for_args_with_options(&registry, &suite_cli_options, &native, &args);
    let matches = match cli.try_get_matches_from_mut(args) {
        Ok(matches) => matches,
        Err(e) if e.kind() == ErrorKind::DisplayHelp => {
            if root_help_requested {
                print_root_help(&cli)?;
            } else {
                e.print()?;
            }
            return Ok(());
        }
        Err(e) => return Err(DataFusionError::External(Box::new(e))),
    };

    let command =
        command_from_matches_with_options(&suite_cli_options, &native, &matches)?;

    match command {
        RunnerCommand::Help => {
            print_root_help(&cli)?;
        }
        RunnerCommand::List => {
            print_styled(&format_benchmark_list_styled(&registry, &native)?)?;
        }
        RunnerCommand::InfoNative(name) => {
            let benchmark = native.get(&name).ok_or_else(|| {
                DataFusionError::Configuration(format!(
                    "unknown native benchmark '{name}'"
                ))
            })?;
            print_native_help(benchmark)?;
        }
        RunnerCommand::Info(args) => {
            print_styled(&load_info_output(&registry, &benchmark_dir, &args).await?)?;
        }
        RunnerCommand::Query(args) => {
            if native.contains(&args.selector) {
                return Err(native_query_error(&args.selector));
            }

            let (_target, sql) = load_benchmark(&registry, &benchmark_dir, &args).await?;

            print_styled(&write_queries_styled(&sql)?)?;
        }
    }

    Ok(())
}

/// Returns whether the raw args request root help directly.
fn is_root_help_request(args: &[OsString]) -> bool {
    matches!(
        args.get(1).and_then(|arg| arg.to_str()),
        Some("-h" | "--help")
    ) && args.len() == 2
}

/// Prints root help with the explicit `help` command hidden from options.
fn print_root_help(cli: &clap::Command) -> Result<()> {
    let mut cli = cli.clone().mut_arg("help", |arg| arg.hide(true));
    cli.print_long_help()?;
    println!();
    Ok(())
}

/// Prints compact native benchmark help shared by native info selectors.
fn print_native_help(benchmark: &NativeBenchmark) -> Result<()> {
    print_styled(&benchmark.info_text()?)?;
    println!();
    Ok(())
}

/// Builds the error returned when `query` is used with a native benchmark.
fn native_query_error(selector: &str) -> DataFusionError {
    DataFusionError::Configuration(format!(
        "query is only supported for SQL benchmark suites; '{selector}' is a native benchmark"
    ))
}

/// Resolves a selector and parses the single benchmark it names.
///
/// The `info` and `query` subcommands are parse-only operations: they load the
/// benchmark with suite replacement values but do not initialize benchmark SQL.
async fn load_benchmark(
    registry: &SuiteRegistry,
    benchmark_dir: &Path,
    args: &InspectArgs,
) -> Result<(ResolvedBenchmarkTarget, SqlBenchmark)> {
    let target = ResolvedBenchmarkTarget::resolve(
        registry,
        &args.selector,
        args.query_id.as_deref(),
        &args.suite_options,
    )?;
    let benchmark_path = target.single_benchmark_for_inspection()?.path.clone();
    let ctx = SessionContext::new();
    let sql = SqlBenchmark::new_with_replacements(
        &ctx,
        &benchmark_path,
        benchmark_dir,
        &target.replacement_values,
        None,
    )
    .await?;

    Ok((target, sql))
}

/// Formats SQL `info` output.
///
/// A suite selector without `QUERY_ID` reports suite metadata, options, and
/// examples. A selector with `QUERY_ID` additionally parses the single
/// benchmark file and reports benchmark metadata.
async fn load_info_output(
    registry: &SuiteRegistry,
    benchmark_dir: &Path,
    args: &InspectArgs,
) -> Result<String> {
    if args.query_id.is_none() {
        let suite = registry.get(&args.selector).ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "unknown benchmark suite '{}'",
                args.selector
            ))
        })?;
        suite.resolve_option_values(&args.suite_options)?;
        let run_help_sections = format_sql_run_help_sections_styled(registry)?;
        return write_suite_info_styled(&ResolvedSuite::from(suite), &run_help_sections);
    }

    let (target, sql) = load_benchmark(registry, benchmark_dir, args).await?;
    let run_help_sections = format_sql_run_help_sections_styled(registry)?;

    write_info_styled(target.suite.as_ref(), &sql, &run_help_sections)
}

/// Writes already styled output through `anstream` so ANSI color handling
/// matches clap help output on supported terminals.
fn print_styled(output: &str) -> Result<()> {
    let mut stdout = anstream::stdout();

    write!(&mut stdout, "{output}")
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

/// Resolves the SQL benchmark root from either the repository root or the
/// benchmarks crate manifest directory.
fn default_benchmark_dir() -> PathBuf {
    let repo_root_path = PathBuf::from("benchmarks/sql_benchmarks");

    if repo_root_path.exists() {
        repo_root_path
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("sql_benchmarks")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn strip_ansi(input: &str) -> String {
        let mut output = String::new();
        let mut chars = input.chars();

        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c in chars.by_ref() {
                    if c.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                output.push(c);
            }
        }

        output
    }

    #[test]
    fn native_query_error_mentions_sql_only_query() {
        let error = native_query_error("hj").to_string();

        assert!(error.contains("query is only supported for SQL benchmark suites"));
        assert!(error.contains("hj"));
    }

    #[tokio::test]
    async fn sql_info_without_query_id_formats_suite_info() {
        let benchmark_dir = default_benchmark_dir();
        let registry = SuiteRegistry::discover(&benchmark_dir).unwrap();
        let args = InspectArgs {
            selector: "tpch".to_string(),
            query_id: None,
            suite_options: BTreeMap::new(),
        };

        let output = load_info_output(&registry, &benchmark_dir, &args)
            .await
            .unwrap();
        let plain = strip_ansi(&output);

        assert!(plain.contains("Benchmark suite"));
        assert!(plain.contains("suite"));
        assert!(plain.contains("tpch"));
        assert!(
            plain.contains("Usage: benchmark_runner run [OPTIONS] <SUITE> [QUERY_ID]")
        );
        assert!(output.contains("\u{1b}[1m\u{1b}[32mUsage:"));
        assert!(plain.contains("SQL Benchmark Target:"));
        assert!(output.contains("\u{1b}[1m\u{1b}[32mSQL Benchmark Target:"));
        assert!(plain.contains("Runner Options:"));
        assert!(output.contains("\u{1b}[1m\u{1b}[32mRunner Options:"));
        assert!(plain.contains("--save-baseline <BASELINE>"));
        assert!(plain.contains("DataFusion Options:"));
        assert!(output.contains("\u{1b}[1m\u{1b}[32mDataFusion Options:"));
        assert!(plain.contains("Suite Options (tpch):"));
        assert!(plain.contains("--format"));
        assert!(plain.contains("--scale-factor"));
        assert!(plain.contains("Examples"));
        assert!(!plain.contains("    value:"));
        assert!(!plain.contains("q01.benchmark"));
    }

    #[tokio::test]
    async fn sql_info_with_query_id_includes_run_help_sections() {
        let benchmark_dir = default_benchmark_dir();
        let registry = SuiteRegistry::discover(&benchmark_dir).unwrap();
        let args = InspectArgs {
            selector: "tpch".to_string(),
            query_id: Some("1".to_string()),
            suite_options: BTreeMap::new(),
        };

        let output = load_info_output(&registry, &benchmark_dir, &args)
            .await
            .unwrap();
        let plain = strip_ansi(&output);

        assert!(plain.contains("Benchmark"));
        assert!(plain.contains("q01.benchmark"));
        assert!(
            plain.contains("Usage: benchmark_runner run [OPTIONS] <SUITE> [QUERY_ID]")
        );
        assert!(plain.contains("SQL Benchmark Target:"));
        assert!(plain.contains("Runner Options:"));
        assert!(plain.contains("DataFusion Options:"));
        assert!(plain.contains("Suite Options (tpch):"));
        assert!(plain.contains("Examples"));
        assert!(!plain.contains("    value:"));
    }

    #[tokio::test]
    async fn sql_info_without_query_id_does_not_require_queries() {
        let tempdir = tempfile::tempdir().unwrap();
        let suite_dir = tempdir.path().join("empty");
        std::fs::create_dir(&suite_dir).unwrap();
        std::fs::write(
            suite_dir.join("empty.suite"),
            r#"
description = "Empty SQL benchmark suite"
"#,
        )
        .unwrap();
        let registry = SuiteRegistry::discover(tempdir.path()).unwrap();
        let args = InspectArgs {
            selector: "empty".to_string(),
            query_id: None,
            suite_options: BTreeMap::new(),
        };

        let output = load_info_output(&registry, tempdir.path(), &args)
            .await
            .unwrap();

        assert!(output.contains("Benchmark suite"));
        assert!(output.contains("empty"));
        assert!(output.contains("Empty SQL benchmark suite"));
    }
}
