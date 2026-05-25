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
//! The runner intentionally uses the same colored style for `list` output as
//! clap uses for help text.

use crate::benchmark_runner::suite::{SuiteConfig, SuiteOption, SuiteRegistry};
use clap::builder::styling::{AnsiColor, Style};
use datafusion_common::Result;
use std::fmt::{Display, Write as _};

/// Formats the `list` command output with suite summaries, query-id hints, and
/// configurable suite options.
pub fn format_suite_list_styled(registry: &SuiteRegistry) -> Result<String> {
    let mut output = String::new();

    for suite in registry.suites() {
        write_suite_list_entry(&mut output, suite)?;
    }

    Ok(output)
}

/// Writes one suite entry for the `list` command.
fn write_suite_list_entry(output: &mut String, suite: &SuiteConfig) -> Result<()> {
    writeln!(output, "{}", header(&suite.name))?;
    writeln!(output, "  {}: {}", label("description"), suite.description)?;

    let queries = suite.discover_queries()?;

    if let (Some(first), Some(last)) = (queries.first(), queries.last()) {
        writeln!(
            output,
            "  {}: {}-{} discovered under {} as {}",
            label("query ids"),
            value(first.id),
            value(last.id),
            suite.query_search_root().display(),
            literal("qNN.benchmark")
        )?;
    }
    writeln!(output, "  {}:", label("options"))?;

    for option in &suite.options {
        write_suite_list_option(output, option)?;
    }

    Ok(())
}

/// Writes one suite option summary for the `list` command.
fn write_suite_list_option(output: &mut String, option: &SuiteOption) -> Result<()> {
    let values = option
        .values
        .iter()
        .map(|v| {
            if v == &option.default {
                format!("{} ({})", value(v), label("default"))
            } else {
                value(v)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");

    writeln!(
        output,
        "    {} {}  {}",
        literal(option_display(option)),
        placeholder("<VALUE>"),
        values
    )?;

    Ok(())
}

fn option_display(option: &SuiteOption) -> String {
    match &option.short {
        Some(short) => format!("-{short}, --{}", option.name),
        None => format!("--{}", option.name),
    }
}

fn header(text: impl Display) -> String {
    styled(AnsiColor::Green.on_default().bold(), text)
}

fn literal(text: impl Display) -> String {
    styled(AnsiColor::Cyan.on_default().bold(), text)
}

fn placeholder(text: impl Display) -> String {
    styled(AnsiColor::Cyan.on_default(), text)
}

fn value(text: impl Display) -> String {
    styled(AnsiColor::Green.on_default(), text)
}

fn label(text: impl Display) -> String {
    styled(Style::new().bold(), text)
}

fn styled(style: Style, text: impl Display) -> String {
    format!("{style}{text}{style:#}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
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
    fn list_output_mentions_tpch_options() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let output = strip_ansi(&format_suite_list_styled(&registry).unwrap());

        assert!(output.contains("tpch\n  description: TPC-H SQL benchmarks"));
        assert!(output.contains("-f, --format <VALUE>  parquet (default), csv, mem"));
        assert!(output.contains("-sf, --scale-factor <VALUE>  1 (default), 10"));
    }

    #[test]
    fn styled_list_output_includes_ansi_sequences() {
        let registry = SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap();
        let output = format_suite_list_styled(&registry).unwrap();

        assert!(output.contains("\u{1b}["));
        assert!(output.contains("tpch"));
        assert!(output.contains("-f"));
        assert!(output.contains("--format"));
        assert!(output.contains("-sf"));
        assert!(output.contains("--scale-factor"));
        assert!(output.contains("<VALUE>"));
    }
}
