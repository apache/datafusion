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

//! Formatting helpers for human-readable benchmark runner output.
//!
//! The runner intentionally uses the same colored style for `list`, `info`,
//! and `query` output as clap uses for help text. These functions only format
//! already parsed suite or benchmark metadata; they should not execute
//! benchmarks or gather runtime-derived details.

use crate::benchmark_runner::native::{NativeBenchmark, NativeBenchmarks};
use crate::benchmark_runner::selector::ResolvedSuite;
use crate::benchmark_runner::style::{header, label, literal, placeholder, value};
use crate::benchmark_runner::suite::{
    SuiteConfig, SuiteExample, SuiteOption, SuiteRegistry,
};
use crate::sql_benchmark::{QueryDirective, SqlBenchmark};
use clap::{Arg, Command};
use datafusion_common::Result;
use std::fmt::Write as _;

/// Formats the `list` command output with benchmark names and descriptions.
pub fn format_benchmark_list_styled(
    registry: &SuiteRegistry,
    native: &NativeBenchmarks,
) -> Result<String> {
    let mut output = String::new();

    for suite in registry.suites() {
        write_suite_list_entry(&mut output, suite)?;
    }

    for benchmark in native.visible() {
        write_native_list_entry(&mut output, benchmark)?;
    }

    Ok(output)
}

/// Writes one suite entry for the `list` command.
fn write_suite_list_entry(output: &mut String, suite: &SuiteConfig) -> Result<()> {
    writeln!(output, "{}", header(&suite.name))?;
    writeln!(output, "  {}", suite.description)?;

    Ok(())
}

/// Writes one native benchmark entry for the `list` command.
fn write_native_list_entry(
    output: &mut String,
    benchmark: &NativeBenchmark,
) -> Result<()> {
    writeln!(output, "{}", header(benchmark.name()))?;
    writeln!(output, "  {}", benchmark.description())?;

    Ok(())
}

/// Writes parse-time metadata for one benchmark and its containing suite.
///
/// This intentionally avoids runtime-derived details such as row counts so the
/// `info` command can remain a cheap inspection operation.
pub fn write_info_styled(
    suite: Option<&ResolvedSuite>,
    benchmark: &SqlBenchmark,
    run_help_sections: &str,
) -> Result<String> {
    let mut output = String::new();

    writeln!(output, "{}", header("Benchmark"))?;

    if let Some(suite) = suite {
        writeln!(output, "  {}: {}", label("suite"), value(&suite.name))?;
    }

    writeln!(output, "  {}: {}", label("name"), value(benchmark.name()))?;
    writeln!(output, "  {}: {}", label("group"), value(benchmark.group()))?;
    writeln!(
        output,
        "  {}: {}",
        label("subgroup"),
        value(benchmark.subgroup())
    )?;
    writeln!(
        output,
        "  {}: {}",
        label("file"),
        benchmark.benchmark_path().display()
    )?;

    if let Some(suite) = suite {
        writeln!(output, "  {}: {}", label("description"), suite.description)?;
    }

    if let Some(suite) = suite {
        write_run_help_sections(&mut output, run_help_sections)?;
        write_suite_sections(&mut output, suite)?;
    }

    Ok(output)
}

/// Writes suite-level metadata without requiring a single benchmark query file.
pub fn write_suite_info_styled(
    suite: &ResolvedSuite,
    run_help_sections: &str,
) -> Result<String> {
    let mut output = String::new();

    writeln!(output, "{}", header("Benchmark suite"))?;
    writeln!(output, "  {}: {}", label("suite"), value(&suite.name))?;
    writeln!(output, "  {}: {}", label("description"), suite.description)?;
    write_run_help_sections(&mut output, run_help_sections)?;
    write_suite_sections(&mut output, suite)?;

    Ok(output)
}

/// Writes one named clap help section in the runner's compact styled format.
pub fn write_help_section(
    output: &mut String,
    command: &Command,
    heading: &str,
) -> Result<()> {
    writeln!(output, "{}", header(format!("{heading}:")))?;

    let entries = command
        .get_arguments()
        .filter_map(|arg| {
            if arg.is_hide_set() || arg.get_help_heading() != Some(heading) {
                return None;
            }

            let help = arg.get_help()?;
            Some((arg_display(arg), help.to_string()))
        })
        .collect::<Vec<_>>();
    let flag_width = entries
        .iter()
        .map(|(flag, _help)| flag.len())
        .max()
        .unwrap_or(0);

    for (flag, help) in entries {
        write_help_entry(output, &flag, flag_width, &help)?;
    }

    writeln!(output)?;

    Ok(())
}

/// Writes all visible options for a command in compact single-line form.
pub fn write_options_section(output: &mut String, command: &Command) -> Result<()> {
    writeln!(output, "{}", header("Options:"))?;

    let entries = command
        .get_arguments()
        .filter_map(|arg| {
            if arg.is_hide_set() {
                return None;
            }

            Some((arg_display(arg), arg_help(arg)?))
        })
        .collect::<Vec<_>>();
    let flag_width = entries
        .iter()
        .map(|(flag, _help)| flag.len())
        .max()
        .unwrap_or(0);

    for (flag, help) in entries {
        write_help_entry(output, &flag, flag_width, &help)?;
    }

    Ok(())
}

/// Builds help text for one clap argument, including defaults and values.
fn arg_help(arg: &Arg) -> Option<String> {
    let mut help = arg.get_help()?.to_string();

    if arg.get_action().takes_values() {
        let defaults = arg
            .get_default_values()
            .iter()
            .map(|value| value.to_string_lossy())
            .collect::<Vec<_>>();
        if !defaults.is_empty() {
            write!(help, " [default: {}]", defaults.join(", ")).ok()?;
        }

        let possible_values = arg
            .get_possible_values()
            .into_iter()
            .map(|value| value.get_name().to_string())
            .collect::<Vec<_>>();
        if !possible_values.is_empty() {
            write!(help, " [possible values: {}]", possible_values.join(", ")).ok()?;
        }
    }

    Some(help)
}

/// Builds the display form for one clap argument.
fn arg_display(arg: &Arg) -> String {
    match arg.get_id().as_str() {
        "selector" => return "<SUITE>".to_string(),
        "query_id" => return "[QUERY_ID]".to_string(),
        _ => {}
    }

    let mut display = String::new();

    if let Some(short) = arg.get_short() {
        display.push('-');
        display.push(short);
    }

    if let Some(long) = arg.get_long() {
        if !display.is_empty() {
            display.push_str(", ");
        }
        display.push_str("--");
        display.push_str(long);
    }

    if arg.get_action().takes_values()
        && let Some(value_names) = arg.get_value_names()
    {
        display.push(' ');
        display.push_str(&format_value_names(value_names.iter()));
    }

    display
}

/// Formats one or more clap value names as angle-bracket placeholders.
fn format_value_names<'a>(
    value_names: impl Iterator<Item = &'a clap::builder::Str>,
) -> String {
    value_names
        .map(|value| format!("<{value}>"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Writes one aligned help entry.
fn write_help_entry(
    output: &mut String,
    flag: &str,
    flag_width: usize,
    help: &str,
) -> Result<()> {
    let padding = " ".repeat(flag_width.saturating_sub(flag.len()) + 2);

    writeln!(output, "  {}{padding}{help}", literal(flag))?;

    Ok(())
}

/// Appends preformatted run-help sections to info output.
fn write_run_help_sections(output: &mut String, run_help_sections: &str) -> Result<()> {
    if !run_help_sections.is_empty() {
        writeln!(output, "\n{}", run_help_sections.trim_end())?;
    }

    Ok(())
}

/// Writes suite options and examples sections for info output.
fn write_suite_sections(output: &mut String, suite: &ResolvedSuite) -> Result<()> {
    writeln!(
        output,
        "\n{}",
        header(format!("Suite Options ({}):", suite.name))
    )?;

    for option in &suite.options {
        write_suite_option_details(output, option)?;
    }

    if suite.examples.is_empty() {
        return Ok(());
    }

    writeln!(output, "\n{}", header("Examples"))?;

    for example in &suite.examples {
        write_example_details(output, example)?;
    }

    Ok(())
}

/// Writes one suite option for `info`, including default, replacement
/// variable, allowed values, and help text.
fn write_suite_option_details(output: &mut String, option: &SuiteOption) -> Result<()> {
    writeln!(
        output,
        "  {}\n    {}: {}\n    {}: {}\n    {}: {}\n    {}: {}\n",
        literal(option_display(option)),
        label("default"),
        value(&option.default),
        label("variable"),
        placeholder(&option.env),
        label("values"),
        option
            .values
            .iter()
            .map(value)
            .collect::<Vec<_>>()
            .join(", "),
        label("help"),
        option.help
    )?;

    Ok(())
}

/// Formats one suite option with its short alias when present.
fn option_display(option: &SuiteOption) -> String {
    match &option.short {
        Some(short) => format!("-{short}, --{}", option.name),
        None => format!("--{}", option.name),
    }
}

/// Writes one suite example command and its description for `info`.
fn write_example_details(output: &mut String, example: &SuiteExample) -> Result<()> {
    writeln!(
        output,
        "  {}\n    {}\n",
        literal(&example.command),
        example.description
    )?;

    Ok(())
}

/// Writes the SQL query text parsed from a benchmark file by directive.
///
/// Only directives that contain queries are emitted, and each query is numbered
/// within its directive for easier inspection.
pub fn write_queries_styled(benchmark: &SqlBenchmark) -> Result<String> {
    let mut output = String::new();

    for directive in [
        QueryDirective::Init,
        QueryDirective::Load,
        QueryDirective::Run,
        QueryDirective::Cleanup,
    ] {
        if let Some(queries) = benchmark.queries().get(&directive) {
            writeln!(output, "{}", header(directive.as_str()))?;

            for (idx, query) in queries.iter().enumerate() {
                writeln!(
                    output,
                    "{}\n{}",
                    literal(format!("-- query {}", idx + 1)),
                    query.trim()
                )?;
            }

            output.push('\n');
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark_runner::native::NativeBenchmarks;
    use datafusion::prelude::SessionContext;
    use std::cell::OnceCell;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    }

    fn example_suite() -> ResolvedSuite {
        ResolvedSuite::from(&SuiteConfig {
            name: "example-suite".to_string(),
            description: "Example suite description".to_string(),
            query_pattern: "q{QUERY_ID_PADDED}.benchmark".to_string(),
            path_replacements: BTreeMap::new(),
            options: vec![],
            examples: vec![],
            suite_path: PathBuf::from("example.suite"),
            suite_dir: PathBuf::from("."),
            query_cache: OnceCell::new(),
        })
    }

    fn assert_in_order(output: &str, expected: &[&str]) {
        let mut offset = 0;
        for expected in expected {
            let Some(index) = output[offset..].find(expected) else {
                panic!("expected '{expected}' after byte {offset} in:\n{output}");
            };
            offset += index + expected.len();
        }
    }

    fn strip_ansi(input: &str) -> String {
        let mut output = String::new();
        let mut chars = input.chars();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                for c in chars.by_ref() {
                    if c == 'm' {
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
    fn list_output_only_includes_benchmark_names_and_descriptions() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let native = NativeBenchmarks::new(&registry);
        let output =
            strip_ansi(&format_benchmark_list_styled(&registry, &native).unwrap());

        assert!(output.contains("tpch\n  TPC-H SQL benchmarks"));
        assert!(output.contains("hj\n  Run the Hash Join benchmark"));
        assert!(!output.contains("description:"));
        assert!(!output.contains("options:"));
        assert!(!output.contains("examples:"));
    }

    #[test]
    fn styled_list_output_includes_ansi_sequences() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let native = NativeBenchmarks::new(&registry);
        let output = format_benchmark_list_styled(&registry, &native).unwrap();

        assert!(output.contains("\u{1b}["));
        assert!(output.contains("tpch"));
    }

    #[test]
    fn list_output_excludes_native_benchmarks_shadowed_by_sql_suites() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let native = NativeBenchmarks::new(&registry);
        let output =
            strip_ansi(&format_benchmark_list_styled(&registry, &native).unwrap());
        let lines = output.lines().collect::<Vec<_>>();

        assert!(output.contains("tpch\n  TPC-H SQL benchmarks"));
        assert_eq!(lines.iter().filter(|line| **line == "tpch").count(), 1);
    }

    #[tokio::test]
    async fn info_output_is_parse_only() {
        let ctx = SessionContext::new();
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let benchmark_path = dir.path().join("q01.benchmark");
        std::fs::write(
            &benchmark_path,
            "name Q01\ngroup example\nsubgroup default\nrun\nSELECT 1\n",
        )
        .expect("benchmark file should be written");
        let benchmark = SqlBenchmark::new(&ctx, &benchmark_path, dir.path(), None)
            .await
            .expect("benchmark should parse");

        let suite = example_suite();
        let output = write_info_styled(
            Some(&suite),
            &benchmark,
            "Usage: benchmark_runner run [OPTIONS] <SUITE> [QUERY_ID]",
        )
        .unwrap();

        assert!(output.contains("Benchmark"));
        assert_in_order(
            &output,
            &[
                "suite",
                "example-suite",
                "name",
                "Q01",
                "group",
                "example",
                "subgroup",
                "default",
                "file",
                "q01.benchmark",
                "description",
                "Example suite description",
            ],
        );
        assert!(!output.contains("query id"));
        assert!(!output.contains("Parsed benchmark"));
        assert!(!output.contains("initialized"));
        assert!(!output.contains("expected plan strings"));
        assert!(!output.contains("query directives"));
        assert!(!output.contains("assert queries"));
        assert!(!output.contains("result queries"));
        assert!(!output.contains("row count"));
        assert!(!output.contains("rows returned"));
    }

    #[tokio::test]
    async fn styled_info_output_includes_ansi_sequences() {
        let ctx = SessionContext::new();
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let benchmark_path = dir.path().join("q01.benchmark");
        std::fs::write(
            &benchmark_path,
            "name Q01\ngroup example\nsubgroup default\nrun\nSELECT 1\n",
        )
        .expect("benchmark file should be written");
        let benchmark = SqlBenchmark::new(&ctx, &benchmark_path, dir.path(), None)
            .await
            .expect("benchmark should parse");

        let suite = example_suite();
        let output = write_info_styled(
            Some(&suite),
            &benchmark,
            "Usage: benchmark_runner run [OPTIONS] <SUITE> [QUERY_ID]",
        )
        .unwrap();

        assert!(output.contains("\u{1b}["));
        assert!(output.contains("Benchmark"));
        assert!(output.contains("suite"));
        assert!(output.contains("name"));
        assert!(output.contains("Suite Options (example-suite):"));
        assert!(output.contains("group"));
        assert!(output.contains("subgroup"));
        assert!(output.contains("file"));
        assert!(output.contains("description"));
        assert!(!output.contains("query id"));
        assert!(!output.contains("Parsed benchmark"));
        assert!(!output.contains("initialized"));
        assert!(!output.contains("expected plan strings"));
        assert!(!output.contains("query directives"));
        assert!(!output.contains("assert queries"));
        assert!(!output.contains("result queries"));
    }

    #[test]
    fn suite_info_omits_empty_examples_section() {
        let suite = example_suite();
        let output = write_suite_info_styled(&suite, "").unwrap();

        assert!(output.contains("Benchmark suite"));
        assert!(!output.contains("Examples"));
    }

    #[tokio::test]
    async fn query_output_formats_loaded_query_directives() {
        let ctx = SessionContext::new();
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let benchmark_path = dir.path().join("q01.benchmark");
        std::fs::write(
            &benchmark_path,
            "init\nCREATE TEMP TABLE t AS SELECT 1 AS value;\n\nrun\nSELECT value FROM t\n\ncleanup\nDROP TABLE t\n",
        )
        .expect("benchmark file should be written");
        let benchmark = SqlBenchmark::new(&ctx, &benchmark_path, dir.path(), None)
            .await
            .expect("benchmark should parse");

        let output = write_queries_styled(&benchmark).unwrap();

        assert_in_order(
            &output,
            &[
                "init",
                "-- query 1",
                "CREATE TEMP TABLE t AS SELECT 1 AS value;",
                "run",
                "-- query 1",
                "SELECT value FROM t",
                "cleanup",
                "-- query 1",
                "DROP TABLE t",
            ],
        );
    }

    #[tokio::test]
    async fn styled_query_output_includes_ansi_sequences() {
        let ctx = SessionContext::new();
        let dir = tempfile::tempdir().expect("temp dir should be created");
        let benchmark_path = dir.path().join("q01.benchmark");
        std::fs::write(&benchmark_path, "run\nSELECT 1\n")
            .expect("benchmark file should be written");
        let benchmark = SqlBenchmark::new(&ctx, &benchmark_path, dir.path(), None)
            .await
            .expect("benchmark should parse");

        let output = write_queries_styled(&benchmark).unwrap();

        assert!(output.contains("\u{1b}["));
        assert!(output.contains("run"));
        assert!(output.contains("SELECT 1"));
    }
}
