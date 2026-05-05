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

//! Native benchmark metadata for `benchmark_runner`.
//!
//! Native benchmarks are the Rust `RunOpt`-based benchmarks historically
//! exposed through `dfbench.rs`. SQL suites take precedence, so native entries
//! whose selector matches a discovered `.suite` are hidden from this runner.

use crate::benchmark_runner::output::write_options_section;
use crate::benchmark_runner::style::{HELP_STYLES, header};
use crate::benchmark_runner::suite::SuiteRegistry;
use crate::{
    cancellation, clickbench, h2o, hj, imdb, nlj, smj, sort_pushdown, sort_tpch, tpcds,
};
use clap::{Arg, ArgAction, Args, ColorChoice, Command};
use datafusion::error::Result;
use std::collections::BTreeSet;
use std::fmt::Write as _;

/// Metadata and clap wiring for one native Rust benchmark.
#[derive(Debug, Clone)]
pub(crate) struct NativeBenchmark {
    kind: NativeBenchmarkKind,
}

/// Builds commands and formatted help for one native benchmark.
impl NativeBenchmark {
    /// Creates metadata for one native benchmark kind.
    fn new(kind: NativeBenchmarkKind) -> Self {
        Self { kind }
    }

    /// Returns the command-line selector for this benchmark.
    pub(crate) fn name(&self) -> &'static str {
        self.kind.name()
    }

    /// Returns the description shown by `list` and help output.
    pub(crate) fn description(&self) -> &'static str {
        self.kind.description()
    }

    /// Builds the clap command used to render native benchmark options.
    pub(crate) fn command(&self) -> Command {
        configure_help(
            self.kind.augment_args(
                Command::new(self.name())
                    .about(self.description())
                    .styles(HELP_STYLES)
                    .color(ColorChoice::Always),
            ),
        )
    }

    /// Builds the hidden clap subcommand used to recognize native info selectors.
    pub(crate) fn info_command(&self) -> Command {
        Command::new(self.name())
            .about(self.description())
            .styles(HELP_STYLES)
            .color(ColorChoice::Always)
    }

    /// Renders compact native help text for the `info` command.
    pub(crate) fn info_text(&self) -> Result<String> {
        let mut command = self
            .command()
            .bin_name(format!("benchmark_runner info {}", self.name()));
        format_native_help(&mut command)
    }
}

/// Formats native benchmark help in the compact runner style.
fn format_native_help(command: &mut Command) -> Result<String> {
    let mut output = String::new();

    if let Some(about) = command.get_about() {
        writeln!(output, "{about}\n")?;
    }

    writeln!(
        output,
        "{}",
        header(command.render_usage().to_string().trim_end())
    )?;
    writeln!(output)?;
    write_options_section(&mut output, command)?;

    Ok(output)
}

/// Applies benchmark-runner help behavior to a native `RunOpt` command.
fn configure_help(command: Command) -> Command {
    hide_env_annotations(command).disable_help_flag(true).arg(
        Arg::new("help")
            .short('h')
            .long("help")
            .action(ArgAction::HelpShort)
            .help("Print help"),
    )
}

/// Hides environment variable annotations inherited from `RunOpt` arguments.
fn hide_env_annotations(mut command: Command) -> Command {
    let arg_ids = command
        .get_arguments()
        .map(|arg| arg.get_id().to_string())
        .collect::<Vec<_>>();

    for id in arg_ids {
        command = command.mut_arg(id, |arg| arg.hide_env(true));
    }

    command
}

/// Visible native benchmark registry after excluding names shadowed by suites.
#[derive(Debug, Clone)]
pub(crate) struct NativeBenchmarks {
    visible: Vec<NativeBenchmark>,
}

/// Discovers and looks up native benchmarks exposed by the runner.
impl NativeBenchmarks {
    /// Builds the visible native benchmark list for the current suite registry.
    pub(crate) fn new(suites: &SuiteRegistry) -> Self {
        let suite_names = suites
            .suites()
            .iter()
            .map(|suite| suite.name.as_str())
            .collect::<BTreeSet<_>>();
        let visible = native_benchmarks()
            .into_iter()
            .filter(|benchmark| !suite_names.contains(benchmark.name()))
            .collect();

        Self { visible }
    }

    /// Returns visible native benchmarks in display order.
    pub(crate) fn visible(&self) -> &[NativeBenchmark] {
        &self.visible
    }

    /// Returns whether a native benchmark selector is visible.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Finds one visible native benchmark by selector.
    pub(crate) fn get(&self, name: &str) -> Option<&NativeBenchmark> {
        self.visible
            .iter()
            .find(|benchmark| benchmark.name() == name)
    }
}

/// Enumerates the native `RunOpt` benchmarks that can be exposed.
#[derive(Debug, Clone, Copy)]
enum NativeBenchmarkKind {
    Cancellation,
    Clickbench,
    H2o,
    Hj,
    Imdb,
    Nlj,
    Smj,
    SortPushdown,
    SortTpch,
    Tpcds,
}

const NATIVE_BENCHMARKS: &[NativeBenchmarkKind] = &[
    NativeBenchmarkKind::Cancellation,
    NativeBenchmarkKind::Clickbench,
    NativeBenchmarkKind::H2o,
    NativeBenchmarkKind::Hj,
    NativeBenchmarkKind::Imdb,
    NativeBenchmarkKind::Nlj,
    NativeBenchmarkKind::Smj,
    NativeBenchmarkKind::SortPushdown,
    NativeBenchmarkKind::SortTpch,
    NativeBenchmarkKind::Tpcds,
];

/// Provides metadata and clap augmentation for native benchmark kinds.
impl NativeBenchmarkKind {
    /// Returns the command-line selector for this native benchmark kind.
    fn name(self) -> &'static str {
        match self {
            Self::Cancellation => "cancellation",
            Self::Clickbench => "clickbench",
            Self::H2o => "h2o",
            Self::Hj => "hj",
            Self::Imdb => "imdb",
            Self::Nlj => "nlj",
            Self::Smj => "smj",
            Self::SortPushdown => "sort-pushdown",
            Self::SortTpch => "sort-tpch",
            Self::Tpcds => "tpcds",
        }
    }

    /// Returns the description shown in list and help output.
    fn description(self) -> &'static str {
        match self {
            Self::Cancellation => "Test performance of cancelling queries",
            Self::Clickbench => "Driver program to run the ClickBench benchmark",
            Self::H2o => "Run the H2O benchmark",
            Self::Hj => "Run the Hash Join benchmark",
            Self::Imdb => "Run the Join Order Benchmark using the IMDB dataset",
            Self::Nlj => "Run the Nested Loop Join benchmark",
            Self::Smj => "Run the Sort Merge Join benchmark",
            Self::SortPushdown => "Benchmark sort pushdown optimization",
            Self::SortTpch => "Run end-to-end sort queries on a TPC-H dataset",
            Self::Tpcds => "Run the TPC-DS benchmark",
        }
    }

    /// Adds this benchmark's `RunOpt` arguments to a clap command for display.
    fn augment_args(self, command: Command) -> Command {
        match self {
            Self::Cancellation => augment::<cancellation::RunOpt>(command),
            Self::Clickbench => augment::<clickbench::RunOpt>(command),
            Self::H2o => augment::<h2o::RunOpt>(command),
            Self::Hj => augment::<hj::RunOpt>(command),
            Self::Imdb => augment::<imdb::RunOpt>(command),
            Self::Nlj => augment::<nlj::RunOpt>(command),
            Self::Smj => augment::<smj::RunOpt>(command),
            Self::SortPushdown => augment::<sort_pushdown::RunOpt>(command),
            Self::SortTpch => augment::<sort_tpch::RunOpt>(command),
            Self::Tpcds => augment::<tpcds::RunOpt>(command),
        }
    }
}

/// Builds metadata for every native benchmark kind before suite filtering.
fn native_benchmarks() -> Vec<NativeBenchmark> {
    NATIVE_BENCHMARKS
        .iter()
        .copied()
        .map(NativeBenchmark::new)
        .collect()
}

/// Adds one concrete native `RunOpt` type's clap arguments to a command.
fn augment<T>(command: Command) -> Command
where
    T: Args,
{
    T::augment_args(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark_runner::suite::SuiteRegistry;
    use std::path::PathBuf;

    fn manifest_path(path: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
    }

    fn registry() -> SuiteRegistry {
        SuiteRegistry::discover(manifest_path("sql_benchmarks")).unwrap()
    }

    #[test]
    fn visible_native_benchmarks_include_hj() {
        let suites = registry();
        let native = NativeBenchmarks::new(&suites);

        assert!(native.contains("hj"));
        assert!(native.get("hj").is_some());
    }

    #[test]
    fn visible_native_benchmarks_exclude_suite_names() {
        let suites = registry();
        let native = NativeBenchmarks::new(&suites);

        assert!(suites.get("tpch").is_some());
        assert!(!native.contains("tpch"));
        assert!(native.get("tpch").is_none());
    }

    #[test]
    fn native_help_for_hj_contains_runopt_options() {
        let suites = registry();
        let native = NativeBenchmarks::new(&suites);
        let hj = native.get("hj").unwrap();
        let mut command = hj
            .command()
            .bin_name(format!("benchmark_runner info {}", hj.name()));
        let help = command
            .try_get_matches_from_mut(["benchmark_runner info hj", "--help"])
            .unwrap_err()
            .to_string();

        assert!(help.contains("Run the Hash Join benchmark"));
        assert!(help.contains("--query"));
        assert!(help.contains("--path"));
        assert!(help.contains("--iterations"));
        assert!(help.contains("Number of iterations of each test run [default: 3]"));
        assert!(!help.contains("This micro-benchmark focuses"));
        assert!(!help.contains("[env:"));
    }

    #[test]
    fn native_info_text_matches_condensed_help() {
        let suites = registry();
        let native = NativeBenchmarks::new(&suites);
        let hj = native.get("hj").unwrap();
        let info = hj.info_text().unwrap();

        assert!(info.contains("Usage:"));
        assert!(info.contains("\u{1b}["));
        assert!(info.contains("--iterations"));
        assert!(info.contains("Number of iterations of each test run"));
        assert!(info.lines().any(|line| {
            line.contains("-i, --iterations <ITERATIONS>")
                && line.contains("Number of iterations of each test run")
        }));
        assert!(
            !info.contains("--iterations <ITERATIONS>\n          Number of iterations")
        );
        assert!(info.contains("-d, --debug"));
        assert!(!info.contains("-d, --debug <DEBUG>"));
        assert!(!info.contains("This micro-benchmark focuses"));
        assert!(!info.contains("[env:"));
    }

    #[test]
    fn native_info_text_preserves_required_args_in_compact_usage() {
        let suites = registry();
        let native = NativeBenchmarks::new(&suites);
        let tpcds = native.get("tpcds").unwrap();
        let info = tpcds.info_text().unwrap();

        assert!(info.contains(
            "Usage: benchmark_runner info tpcds [OPTIONS] --path <PATH> --query_path <QUERY_PATH>"
        ));
        assert!(info.contains("-p, --path <PATH>"));
        assert!(info.contains("-Q, --query_path <QUERY_PATH>"));
        assert!(info.contains("Path to data files"));
        assert!(!info.contains("--path <PATH>\n          Path to data files"));
    }
}
