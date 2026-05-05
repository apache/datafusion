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

//! CLI construction and argument conversion for `benchmark_runner`.
//!
//! This module owns the clap command tree for `list`, `info`, and `query`.
//! Suite options are loaded from `.suite` files and registered as dynamic clap
//! arguments so help output can describe suite-specific flags. Short aliases
//! with more than one character, such as `-sf`, are normalized before clap
//! parses the command because clap only supports one-character short flags
//! directly.

use crate::benchmark_runner::native::NativeBenchmarks;
use crate::benchmark_runner::output::write_help_section;
use crate::benchmark_runner::style::{HELP_STYLES, header};
use crate::benchmark_runner::suite::{SuiteConfig, SuiteOption, SuiteRegistry};
use crate::util::CommonOpt;
use clap::{Arg, ArgAction, ArgMatches, Args, Command};
use datafusion_common::{Result, exec_datafusion_err};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fmt::Write as _;

const BENCHMARK_TARGET_HEADING: &str = "SQL Benchmark Target";
const RUNNER_OPTIONS_HEADING: &str = "Runner Options";
const DATAFUSION_OPTIONS_HEADING: &str = "DataFusion Options";
const SUITE_OPTIONS_HEADING: &str = "Suite Options";
const DATAFUSION_ARG_IDS: &[&str] = &[
    "partitions",
    "batch_size",
    "mem_pool_type",
    "memory_limit",
    "sort_spill_reservation_bytes",
    "debug",
    "simulate_latency",
];
const RUN_ENV_ARG_IDS: &[&str] = &[
    "iterations",
    "partitions",
    "batch_size",
    "mem_pool_type",
    "memory_limit",
    "sort_spill_reservation_bytes",
    "debug",
    "simulate_latency",
];
const HIDDEN_RUN_ARG_IDS: &[&str] = &["debug", "sort_spill_reservation_bytes"];

/// Parsed top-level command selected by clap and dynamic suite metadata.
#[derive(Debug)]
pub enum RunnerCommand {
    /// Print root command help.
    Help,
    /// List SQL suites and visible native benchmarks.
    List,
    /// Show SQL suite or query metadata.
    Info(InspectArgs),
    /// Show native benchmark help-style metadata.
    InfoNative(String),
    /// Print SQL parsed from one benchmark query.
    Query(InspectArgs),
}

/// Suite-option visibility used while constructing selector subcommands.
#[derive(Clone, Copy)]
enum SelectorSuiteOptions<'a> {
    /// Include every discovered suite option.
    All,
    /// Include only options from the selected suite.
    Suite(&'a SuiteConfig),
    /// Do not include suite options.
    None,
}

/// Selector subcommands that accept a SQL suite name.
#[derive(Clone, Copy)]
enum SelectorCommand {
    /// The `info` subcommand.
    Info,
    /// The `query` subcommand.
    Query,
}

/// Common selector arguments for SQL inspection and execution commands.
#[derive(Debug, Clone)]
pub struct InspectArgs {
    /// Suite selector provided on the command line.
    pub selector: String,
    /// Optional query id provided on the command line.
    pub query_id: Option<String>,
    /// Suite option values supplied by the user.
    pub suite_options: BTreeMap<String, String>,
}

/// Per-subcommand suite-option visibility used for help rendering.
#[derive(Clone, Copy)]
struct SelectorOptionVisibility<'a> {
    info: SelectorSuiteOptions<'a>,
    query: SelectorSuiteOptions<'a>,
}

/// Constructs visibility rules for normal and help-specific command trees.
impl<'a> SelectorOptionVisibility<'a> {
    /// Shows all suite options for each selector command.
    fn all() -> Self {
        Self {
            info: SelectorSuiteOptions::All,
            query: SelectorSuiteOptions::All,
        }
    }

    /// Shows a selected option set for one selector command's help output.
    fn for_help(command: SelectorCommand, options: SelectorSuiteOptions<'a>) -> Self {
        let mut visibility = Self::all();
        match command {
            SelectorCommand::Info => visibility.info = options,
            SelectorCommand::Query => visibility.query = options,
        };
        visibility
    }
}

/// Precomputed suite option metadata used while parsing dynamic CLI options.
pub(crate) struct SuiteCliOptions {
    /// All suite option long names.
    names: BTreeSet<String>,
    /// All long option spellings, including the leading `--`.
    long_args: BTreeSet<OsString>,
    /// All short alias spellings, including the leading `-`.
    alias_args: BTreeSet<OsString>,
    /// Per-suite short alias to long option name mappings.
    aliases_by_suite: BTreeMap<String, BTreeMap<OsString, String>>,
}

/// Collects dynamic suite option metadata from the registry.
impl SuiteCliOptions {
    /// Collects suite option names and aliases used by clap parsing and
    /// pre-parse alias normalization.
    pub(crate) fn new(registry: &SuiteRegistry) -> Self {
        let mut names = BTreeSet::new();
        let mut long_args = BTreeSet::new();
        let mut alias_args = BTreeSet::new();
        let mut aliases_by_suite = BTreeMap::new();

        for suite in registry.suites() {
            let mut aliases = BTreeMap::new();

            for option in &suite.options {
                names.insert(option.name.clone());
                long_args.insert(OsString::from(format!("--{}", option.name)));

                if let Some(short) = &option.short {
                    let alias = OsString::from(format!("-{short}"));
                    alias_args.insert(alias.clone());
                    aliases.insert(alias, option.name.clone());
                }
            }
            aliases_by_suite.insert(suite.name.clone(), aliases);
        }

        Self {
            names,
            long_args,
            alias_args,
            aliases_by_suite,
        }
    }
}

/// Builds the full command tree with all discovered suite options available.
pub(crate) fn build_cli(registry: &SuiteRegistry, native: &NativeBenchmarks) -> Command {
    build_cli_with_selector_options(registry, native, SelectorOptionVisibility::all())
}

/// Builds a command tree using precomputed suite option metadata.
pub(crate) fn build_cli_for_args_with_options(
    registry: &SuiteRegistry,
    suite_cli_options: &SuiteCliOptions,
    native: &NativeBenchmarks,
    args: &[OsString],
) -> Command {
    match selector_help_request(suite_cli_options, args) {
        Some((command, Some(selector))) => {
            let options = registry
                .get(&selector)
                .map(SelectorSuiteOptions::Suite)
                .unwrap_or(SelectorSuiteOptions::None);

            build_cli_with_help_options(registry, native, command, options)
        }
        Some((command, None)) => build_cli_with_help_options(
            registry,
            native,
            command,
            SelectorSuiteOptions::None,
        ),
        None => build_cli(registry, native),
    }
}

/// Builds a help-oriented command tree for one selector subcommand.
fn build_cli_with_help_options(
    registry: &SuiteRegistry,
    native: &NativeBenchmarks,
    command: SelectorCommand,
    options: SelectorSuiteOptions<'_>,
) -> Command {
    build_cli_with_selector_options(
        registry,
        native,
        SelectorOptionVisibility::for_help(command, options),
    )
}

/// Builds the clap command tree with configurable suite-option visibility per selector subcommand.
fn build_cli_with_selector_options(
    registry: &SuiteRegistry,
    native: &NativeBenchmarks,
    visibility: SelectorOptionVisibility<'_>,
) -> Command {
    let info = add_configured_suite_options(
        base_selector_command("info", "Show benchmark metadata")
            .override_usage(
                "benchmark_runner info <SUITE> [QUERY_ID]\n       benchmark_runner info <NATIVE_BENCHMARK>",
            ),
        registry,
        visibility.info,
    );
    let info = add_native_info_subcommands(info, native);
    let query = add_configured_suite_options(
        base_selector_command("query", "Show parsed benchmark SQL"),
        registry,
        visibility.query,
    );

    Command::new("benchmark_runner")
        .about("Inspect DataFusion benchmarks.")
        .styles(HELP_STYLES)
        .subcommand_required(false)
        .arg_required_else_help(false)
        .disable_help_subcommand(true)
        .subcommand(
            Command::new("help")
                .about("Print help")
                .disable_help_flag(true),
        )
        .subcommand(
            Command::new("list")
                .about("List benchmark suites")
                .disable_help_flag(true),
        )
        .subcommand(info)
        .subcommand(query)
}

/// Builds the shared `run` command metadata reused by SQL `info` output.
fn base_run_command() -> Command {
    let run = CommonOpt::augment_args(
        base_selector_command("run", "Run benchmarks").override_usage(run_usage()),
    )
    .mut_arg("iterations", |arg| arg.help_heading(RUNNER_OPTIONS_HEADING))
    .arg(
        Arg::new("persist-results")
            .long("persist-results")
            .action(ArgAction::SetTrue)
            .help("Persist benchmark results before the timed run (SQL suites only.)")
            .help_heading(RUNNER_OPTIONS_HEADING),
    )
    .arg(
        Arg::new("validate-results")
            .long("validate-results")
            .action(ArgAction::SetTrue)
            .help("Validate benchmark results before the timed run (SQL suites only.)")
            .help_heading(RUNNER_OPTIONS_HEADING),
    )
    .arg(
        Arg::new("criterion")
            .long("criterion")
            .action(ArgAction::SetTrue)
            .help(
                "Run selected benchmarks with Criterion instead of fixed iterations (SQL suites only.)",
            )
            .help_heading(RUNNER_OPTIONS_HEADING),
    )
    .arg(
        Arg::new("save-baseline")
            .long("save-baseline")
            .value_name("BASELINE")
            .help("Save Criterion results under a named baseline (SQL suites only.)")
            .help_heading(RUNNER_OPTIONS_HEADING),
    )
    .arg(
        Arg::new("path")
            .long("path")
            .short('p')
            .value_name("PATH")
            .help("Path to benchmark data (default: benchmarks/data)")
            .help_heading(RUNNER_OPTIONS_HEADING),
    )
    .arg(
        Arg::new("output")
            .long("output")
            .short('o')
            .value_name("PATH")
            .help("Write run timings and row counts as JSON")
            .help_heading(RUNNER_OPTIONS_HEADING),
    );
    let run = set_help_heading(run, DATAFUSION_ARG_IDS, DATAFUSION_OPTIONS_HEADING);
    let run = hide_env_annotations(run, RUN_ENV_ARG_IDS);

    hide_help_entries(run, HIDDEN_RUN_ARG_IDS)
}

/// Returns the condensed SQL-only usage string for run help.
fn run_usage() -> &'static str {
    "benchmark_runner run [OPTIONS] <SUITE> [QUERY_ID]"
}

/// Formats the SQL benchmark `run` help sections reused by SQL `info`.
pub(crate) fn format_sql_run_help_sections_styled(
    registry: &SuiteRegistry,
) -> Result<String> {
    let run = add_configured_suite_options(
        base_run_command(),
        registry,
        SelectorSuiteOptions::None,
    );
    let mut output = String::new();

    writeln!(output, "{}", header(format!("Usage: {}", run_usage())))?;
    writeln!(output)?;
    write_help_section(&mut output, &run, BENCHMARK_TARGET_HEADING)?;
    write_help_section(&mut output, &run, RUNNER_OPTIONS_HEADING)?;
    write_help_section(&mut output, &run, DATAFUSION_OPTIONS_HEADING)?;

    Ok(output.trim_end().to_string())
}

/// Converts clap matches into a typed command using precomputed suite option metadata.
pub(crate) fn command_from_matches_with_options(
    suite_cli_options: &SuiteCliOptions,
    native: &NativeBenchmarks,
    matches: &ArgMatches,
) -> Result<RunnerCommand> {
    match matches.subcommand() {
        None | Some(("help", _)) => Ok(RunnerCommand::Help),
        Some(("list", _)) => Ok(RunnerCommand::List),
        Some(("info", submatches)) => {
            if let Some((name, _native_matches)) = submatches.subcommand() {
                if !native.contains(name) {
                    return Err(exec_datafusion_err!(
                        "unknown native benchmark '{name}'"
                    ));
                }
                reject_suite_options_for_native(suite_cli_options, submatches, name)?;

                return Ok(RunnerCommand::InfoNative(name.to_string()));
            }

            Ok(RunnerCommand::Info(inspect_args_with_missing_selector(
                suite_cli_options,
                submatches,
                "Missing benchmark name",
            )?))
        }
        Some(("query", submatches)) => Ok(RunnerCommand::Query(inspect_args(
            suite_cli_options,
            submatches,
        )?)),
        Some((name, _)) => Err(exec_datafusion_err!("Unknown command '{name}'")),
    }
}

/// Creates the common `info` and `query` selector command shape.
fn base_selector_command(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .styles(HELP_STYLES)
        .arg(
            Arg::new("selector")
                .value_name("SUITE")
                .help("SQL benchmark suite name")
                .help_heading(BENCHMARK_TARGET_HEADING)
                .display_order(1),
        )
        .arg(
            Arg::new("query_id")
                .value_name("QUERY_ID")
                .help("Optional SQL query id, such as 1, 01, 15, or 00")
                .help_heading(BENCHMARK_TARGET_HEADING)
                .display_order(2),
        )
}

/// Adds each distinct suite-defined option to a command.
fn add_suite_options(mut command: Command, registry: &SuiteRegistry) -> Command {
    let mut seen = BTreeSet::new();

    for suite in registry.suites() {
        for option in &suite.options {
            if seen.insert(option.name.clone()) {
                command = add_suite_option(command, option, SUITE_OPTIONS_HEADING, false);
            }
        }
    }
    command
}

/// Applies the requested suite-option visibility mode to a selector command.
fn add_configured_suite_options(
    command: Command,
    registry: &SuiteRegistry,
    suite_options: SelectorSuiteOptions<'_>,
) -> Command {
    match suite_options {
        SelectorSuiteOptions::All => add_suite_options(command, registry),
        SelectorSuiteOptions::Suite(suite) => add_suite_options_for(command, suite),
        SelectorSuiteOptions::None => command,
    }
}

/// Adds each visible native benchmark as an info-only selector subcommand.
fn add_native_info_subcommands(
    mut command: Command,
    native: &NativeBenchmarks,
) -> Command {
    for benchmark in native.visible() {
        command = command.subcommand(benchmark.info_command().hide(true));
    }

    command
}

/// Adds only one suite's options under a suite-specific help heading.
fn add_suite_options_for(mut command: Command, suite: &SuiteConfig) -> Command {
    let heading = suite_options_heading(&suite.name);

    for option in &suite.options {
        command = add_suite_option(command, option, heading.clone(), true);
    }

    command
}

/// Registers one suite-defined option with clap, using direct short aliases
/// when clap supports them and documenting multi-character aliases otherwise.
fn add_suite_option(
    command: Command,
    option: &SuiteOption,
    heading: impl Into<clap::builder::Str>,
    register_short_alias: bool,
) -> Command {
    let name = option.name.clone();
    let (short, help) = match &option.short {
        Some(short) if register_short_alias && short.chars().count() == 1 => {
            (short.chars().next(), option.help.clone())
        }
        Some(short) => (None, format!("{} Alias: -{short}.", option.help)),
        None => (None, option.help.clone()),
    };
    let mut arg = Arg::new(name.clone())
        .long(name)
        .value_name("VALUE")
        .help(help)
        .help_heading(heading);
    if let Some(short) = short {
        arg = arg.short(short);
    }

    command.arg(arg)
}

/// Formats the suite-specific heading used for selected suite options.
fn suite_options_heading(suite_name: &str) -> String {
    format!("{SUITE_OPTIONS_HEADING} ({suite_name})")
}

/// Detects whether the provided arguments are asking for help on a selector
/// subcommand and, when possible, extracts the suite name preceding `--help`.
fn selector_help_request(
    suite_cli_options: &SuiteCliOptions,
    args: &[OsString],
) -> Option<(SelectorCommand, Option<String>)> {
    if !args.iter().any(|arg| is_help_arg(arg)) {
        return None;
    }

    selector_request(suite_cli_options, args, true)
}

/// Scans raw CLI arguments to find a selector before clap parses them.
fn selector_request(
    suite_cli_options: &SuiteCliOptions,
    args: &[OsString],
    stop_at_help: bool,
) -> Option<(SelectorCommand, Option<String>)> {
    let (command_position, command) =
        args.iter().enumerate().find_map(|(position, arg)| {
            selector_command(arg).map(|command| (position, command))
        })?;
    let mut skip_next = false;

    for arg in &args[command_position + 1..] {
        if skip_next {
            skip_next = false;
            continue;
        }

        if stop_at_help && is_help_arg(arg) {
            return Some((command, None));
        }

        if is_value_taking_selector_option(suite_cli_options, command, arg) {
            skip_next = true;
            continue;
        }

        if arg.to_string_lossy().starts_with('-') {
            continue;
        }

        return Some((command, Some(arg.to_string_lossy().into_owned())));
    }

    Some((command, None))
}

/// Converts a raw argument into a selector command name when it matches.
fn selector_command(arg: &OsStr) -> Option<SelectorCommand> {
    match arg.to_str()? {
        "info" => Some(SelectorCommand::Info),
        "query" => Some(SelectorCommand::Query),
        _ => None,
    }
}

/// Returns whether a raw argument requests clap help.
fn is_help_arg(arg: &OsStr) -> bool {
    arg == "--help" || arg == "-h"
}

/// Returns whether an argument is an option whose next token should be skipped
/// while looking for a suite selector before `--help`.
fn is_value_taking_selector_option(
    suite_cli_options: &SuiteCliOptions,
    _command: SelectorCommand,
    arg: &OsStr,
) -> bool {
    let arg_text = arg.to_string_lossy();
    if arg_text.contains('=') {
        return false;
    }

    suite_cli_options.long_args.contains(arg)
        || suite_cli_options.alias_args.contains(arg)
}

/// Rewrites suite option aliases using precomputed suite option metadata.
pub(crate) fn normalize_suite_option_aliases_with_options<I, T>(
    suite_cli_options: &SuiteCliOptions,
    args: I,
) -> Vec<OsString>
where
    I: IntoIterator<Item = T>,
    T: AsRef<OsStr>,
{
    let args = args
        .into_iter()
        .map(|arg| arg.as_ref().to_os_string())
        .collect::<Vec<_>>();
    let aliases = selector_request(suite_cli_options, &args, false)
        .and_then(|(_, selector)| selector)
        .and_then(|selector| suite_cli_options.aliases_by_suite.get(&selector));

    args.into_iter()
        .map(|arg| {
            aliases
                .and_then(|aliases| aliases.get(&arg))
                .map(|option| OsString::from(format!("--{option}")))
                .unwrap_or(arg)
        })
        .collect()
}

/// Extracts selector, optional query id, and suite option values from a
/// selector subcommand's clap matches.
fn inspect_args(
    suite_cli_options: &SuiteCliOptions,
    matches: &ArgMatches,
) -> Result<InspectArgs> {
    inspect_args_with_missing_selector(suite_cli_options, matches, "Missing SUITE")
}

/// Extracts inspection args using a caller-specific missing-selector message.
fn inspect_args_with_missing_selector(
    suite_cli_options: &SuiteCliOptions,
    matches: &ArgMatches,
    missing_selector_message: &'static str,
) -> Result<InspectArgs> {
    let selector = matches
        .get_one::<String>("selector")
        .cloned()
        .ok_or_else(|| exec_datafusion_err!("{missing_selector_message}"))?;
    let query_id = matches.get_one::<String>("query_id").cloned();
    let suite_options = suite_options_from_matches(suite_cli_options, matches);

    Ok(InspectArgs {
        selector,
        query_id,
        suite_options,
    })
}

/// Rejects SQL suite options that clap accepted before native selection.
fn reject_suite_options_for_native(
    suite_cli_options: &SuiteCliOptions,
    matches: &ArgMatches,
    native_name: &str,
) -> Result<()> {
    let suite_options = suite_options_from_matches(suite_cli_options, matches);

    if let Some(option) = suite_options.keys().next() {
        return Err(exec_datafusion_err!(
            "suite option '--{option}' cannot be used with native benchmark '{native_name}'"
        ));
    }

    Ok(())
}

/// Collects only suite-defined option values that were present in clap matches.
fn suite_options_from_matches(
    suite_cli_options: &SuiteCliOptions,
    matches: &ArgMatches,
) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();

    for option_name in &suite_cli_options.names {
        if let Some(value) = matches.get_one::<String>(option_name) {
            values.insert(option_name.clone(), value.clone());
        }
    }

    values
}

/// Moves existing clap arguments into a shared help section.
fn set_help_heading(
    mut command: Command,
    arg_ids: &[&str],
    heading: &'static str,
) -> Command {
    for id in arg_ids {
        command = command.mut_arg(id, |arg| arg.help_heading(heading));
    }

    command
}

/// Hides environment-variable annotations from help while preserving the
/// underlying argument behavior.
fn hide_env_annotations(mut command: Command, arg_ids: &[&str]) -> Command {
    for id in arg_ids {
        command = command.mut_arg(id, |arg| arg.hide_env(true));
    }

    command
}

/// Hides selected inherited options from help while keeping them parseable.
fn hide_help_entries(mut command: Command, arg_ids: &[&str]) -> Command {
    for id in arg_ids {
        command = command.mut_arg(id, |arg| arg.hide(true));
    }

    command
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark_runner::native::NativeBenchmarks;
    use std::fs;

    fn registry() -> SuiteRegistry {
        SuiteRegistry::discover(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sql_benchmarks"),
        )
        .unwrap()
    }

    fn native_registry(registry: &SuiteRegistry) -> NativeBenchmarks {
        NativeBenchmarks::new(registry)
    }

    fn build_cli_with_native(registry: &SuiteRegistry) -> Command {
        build_cli(registry, &native_registry(registry))
    }

    fn os_args<I, T>(args: I) -> Vec<OsString>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<OsStr>,
    {
        args.into_iter()
            .map(|arg| arg.as_ref().to_os_string())
            .collect()
    }

    fn build_cli_for_args<I, T>(registry: &SuiteRegistry, args: I) -> Command
    where
        I: IntoIterator<Item = T>,
        T: AsRef<OsStr>,
    {
        let suite_cli_options = SuiteCliOptions::new(registry);
        let args = os_args(args);
        let native = native_registry(registry);
        build_cli_for_args_with_options(registry, &suite_cli_options, &native, &args)
    }

    fn command_from_matches(
        registry: &SuiteRegistry,
        matches: &ArgMatches,
    ) -> Result<RunnerCommand> {
        let suite_cli_options = SuiteCliOptions::new(registry);
        let native = native_registry(registry);
        command_from_matches_with_options(&suite_cli_options, &native, matches)
    }

    fn parse_command<I, T>(registry: &SuiteRegistry, args: I) -> Result<RunnerCommand>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<OsStr>,
    {
        let args = os_args(args);
        let matches = build_cli_with_native(registry)
            .try_get_matches_from(args)
            .unwrap();

        command_from_matches(registry, &matches)
    }

    fn normalize_suite_option_aliases<I, T>(
        registry: &SuiteRegistry,
        args: I,
    ) -> Vec<OsString>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<OsStr>,
    {
        let suite_cli_options = SuiteCliOptions::new(registry);
        normalize_suite_option_aliases_with_options(&suite_cli_options, args)
    }

    fn registry_with_suite_aliases() -> SuiteRegistry {
        let tempdir = tempfile::tempdir().unwrap();

        for (suite_name, option_name, value) in [
            ("left", "left-option", "left-value"),
            ("right", "right-option", "right-value"),
        ] {
            let suite_dir = tempdir.path().join(suite_name);
            fs::create_dir(&suite_dir).unwrap();
            fs::write(
                suite_dir.join(format!("{suite_name}.suite")),
                format!(
                    r#"
description = "{suite_name} benchmark suite"

[[options]]
name = "{option_name}"
short = "x"
env = "SUITE_OPTION"
default = "{value}"
values = ["{value}"]
help = "Selects the suite option."
"#
                ),
            )
            .unwrap();
        }

        SuiteRegistry::discover(tempdir.path()).unwrap()
    }

    #[test]
    fn help_and_list_subcommands_reject_help_flags() {
        for command in ["help", "list"] {
            for help_flag in ["-h", "--help"] {
                let err = build_cli_with_native(&registry())
                    .try_get_matches_from(["benchmark_runner", command, help_flag])
                    .unwrap_err();

                assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
            }
        }
    }

    #[test]
    fn help_and_list_subcommands_accept_no_options() {
        for (command, expected) in
            [("help", RunnerCommand::Help), ("list", RunnerCommand::List)]
        {
            let registry = registry();
            let actual = parse_command(&registry, ["benchmark_runner", command]).unwrap();

            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected)
            );
        }
    }

    #[test]
    fn list_rejects_suite_options_because_they_are_ignored() {
        let matches = build_cli_with_native(&registry()).try_get_matches_from([
            "benchmark_runner",
            "list",
            "--format",
            "csv",
        ]);

        assert!(matches.is_err(), "{matches:?}");
    }

    #[test]
    fn help_mentions_query_id() {
        let help =
            build_cli_for_args(&registry(), ["benchmark_runner", "query", "--help"])
                .try_get_matches_from(["benchmark_runner", "query", "--help"])
                .unwrap_err()
                .to_string();

        assert!(help.contains("SUITE"));
        assert!(help.contains("SQL benchmark suite name"));
        assert!(help.contains("QUERY_ID"));
        assert!(help.contains("Optional SQL query id"));
    }

    #[test]
    fn info_without_selector_reports_missing_benchmark_name() {
        let registry = registry();
        let matches = build_cli_with_native(&registry)
            .try_get_matches_from(["benchmark_runner", "info"])
            .unwrap();

        let err = command_from_matches(&registry, &matches).unwrap_err();

        assert!(
            err.to_string()
                .contains("Execution error: Missing benchmark name")
        );
    }

    #[test]
    fn sql_run_help_sections_use_run_command_metadata() {
        let output = format_sql_run_help_sections_styled(&registry()).unwrap();

        assert!(output.contains("Usage:"));
        assert!(output.contains("SQL Benchmark Target:"));
        assert!(output.contains("Runner Options:"));
        assert!(output.contains("DataFusion Options:"));
        assert!(output.contains("--save-baseline <BASELINE>"));
        assert!(output.lines().any(|line| {
            line.contains("--persist-results")
                && line.contains(
                    "Persist benchmark results before the timed run (SQL suites only.)",
                )
        }));
        assert!(output.contains(
            "Validate benchmark results before the timed run (SQL suites only.)"
        ));
        assert!(output.contains(
            "Run selected benchmarks with Criterion instead of fixed iterations (SQL suites only.)"
        ));
        assert!(output.contains(
            "Save Criterion results under a named baseline (SQL suites only.)"
        ));
        assert!(!output.contains(
            "--persist-results\n          Persist benchmark results before the timed run"
        ));
    }

    #[test]
    fn info_native_selector_parses_as_native_info() {
        let registry = registry();
        let command =
            parse_command(&registry, ["benchmark_runner", "info", "hj"]).unwrap();

        assert!(matches!(command, RunnerCommand::InfoNative(name) if name == "hj"));
    }

    #[test]
    fn info_native_selector_does_not_require_run_options() {
        let registry = registry();
        let command =
            parse_command(&registry, ["benchmark_runner", "info", "tpcds"]).unwrap();

        assert!(matches!(command, RunnerCommand::InfoNative(name) if name == "tpcds"));
    }

    #[test]
    fn info_native_rejects_parent_suite_options() {
        let registry = registry();
        let matches = build_cli_with_native(&registry)
            .try_get_matches_from(["benchmark_runner", "info", "--format", "csv", "hj"])
            .unwrap();

        let err = command_from_matches(&registry, &matches).unwrap_err();

        assert!(err.to_string().contains("--format"));
        assert!(err.to_string().contains("native benchmark"));
    }

    #[test]
    fn parses_suite_option_short_alias() {
        let registry = registry();
        let args = normalize_suite_option_aliases(
            &registry,
            [
                "benchmark_runner",
                "info",
                "tpch",
                "15",
                "-f",
                "csv",
                "-sf",
                "10",
            ],
        );
        let matches = build_cli_with_native(&registry)
            .try_get_matches_from(args)
            .unwrap();
        let command = command_from_matches(&registry, &matches).unwrap();

        let RunnerCommand::Info(args) = command else {
            panic!("expected info command");
        };
        assert_eq!(
            args.suite_options.get("format").map(String::as_str),
            Some("csv")
        );
        assert_eq!(
            args.suite_options.get("scale-factor").map(String::as_str),
            Some("10")
        );
    }

    #[test]
    fn normalizes_suite_option_aliases_for_selected_suite() {
        for (suite_name, option_name, value) in [
            ("left", "left-option", "left-value"),
            ("right", "right-option", "right-value"),
        ] {
            let registry = registry_with_suite_aliases();
            let args = normalize_suite_option_aliases(
                &registry,
                ["benchmark_runner", "info", suite_name, "-x", value],
            );
            let matches = build_cli_with_native(&registry)
                .try_get_matches_from(args)
                .unwrap();
            let command = command_from_matches(&registry, &matches).unwrap();

            let RunnerCommand::Info(args) = command else {
                panic!("expected info command");
            };
            assert_eq!(
                args.suite_options.get(option_name).map(String::as_str),
                Some(value)
            );
            assert_eq!(args.suite_options.len(), 1);
        }
    }
}
