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
//! This module owns the clap command tree for the initial runner surface:
//! top-level help and suite listing.

use clap::builder::styling::{AnsiColor, Styles};
use clap::{ArgMatches, Command};
use datafusion_common::{Result, exec_datafusion_err};

const HELP_STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Cyan.on_default());

#[derive(Debug)]
pub enum RunnerCommand {
    Help,
    List,
}

/// Builds the command tree for help and suite listing.
pub fn build_cli() -> Command {
    Command::new("benchmark_runner")
        .about("Inspect DataFusion SQL benchmark suites.")
        .styles(HELP_STYLES)
        .subcommand_required(false)
        .arg_required_else_help(false)
        .disable_help_subcommand(true)
        .subcommand(Command::new("help").about("Print help"))
        .subcommand(Command::new("list").about("List SQL benchmark suites"))
}

/// Converts clap matches into a typed command.
pub(crate) fn command_from_matches(matches: &ArgMatches) -> Result<RunnerCommand> {
    match matches.subcommand() {
        None | Some(("help", _)) => Ok(RunnerCommand::Help),
        Some(("list", _)) => Ok(RunnerCommand::List),
        Some((name, _)) => Err(exec_datafusion_err!("Unknown command '{name}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_rejects_unrecognized_options() {
        let matches =
            build_cli().try_get_matches_from(["benchmark_runner", "list", "--format"]);

        assert!(matches.is_err(), "{matches:?}");
    }

    #[test]
    fn help_mentions_list_command() {
        let err = build_cli()
            .try_get_matches_from(["benchmark_runner", "--help"])
            .unwrap_err();
        let help = err.to_string();

        assert!(help.contains("list"));
    }
}
