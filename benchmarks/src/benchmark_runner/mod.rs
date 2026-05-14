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

//! Command-line inspection for SQL benchmark suites.
//!
//! This module backs the `benchmark_runner` binary. The initial command
//! surface lists discovered SQL benchmark suites from `.suite` files and
//! prints the top-level help.
//!
//! Common invocations:
//!
//! ```text
//! cargo run --bin benchmark_runner -- --help
//! cargo run --release --bin benchmark_runner -- list
//! ```
//!
//! The public entry point is [`run_cli`]. The submodules are kept private so
//! the command-line flow remains the single supported API:
//!
//! - `cli` builds the clap command tree and parses the selected command.
//! - `suite` loads `.suite` metadata and discovers benchmark query files.
//! - `output` formats colored `list` command output.

mod cli;
mod output;
mod suite;

use crate::benchmark_runner::cli::{RunnerCommand, build_cli, command_from_matches};
use crate::benchmark_runner::output::format_suite_list_styled;
use crate::benchmark_runner::suite::SuiteRegistry;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use std::io::Write;
use std::path::PathBuf;

/// Runs the benchmark runner command-line flow for the provided argument list.
///
/// This discovers suite metadata, parses the help/list command, and dispatches
/// to the selected implementation.
pub fn run_cli<I, T>(args: I) -> Result<()>
where
    I: IntoIterator<Item = T>,
    T: Clone + Into<std::ffi::OsString>,
{
    let benchmark_dir = default_benchmark_dir();
    let registry = SuiteRegistry::discover(&benchmark_dir)?;
    let mut cli = build_cli();
    let matches = match cli.try_get_matches_from_mut(args) {
        Ok(matches) => matches,
        Err(e) if e.kind() == clap::error::ErrorKind::DisplayHelp => {
            e.print()?;
            return Ok(());
        }
        Err(e) => return Err(DataFusionError::External(Box::new(e))),
    };
    let command = command_from_matches(&matches)?;

    match command {
        RunnerCommand::Help => {
            cli.print_long_help()?;
            println!();
        }
        RunnerCommand::List => {
            print_styled(&format_suite_list_styled(&registry)?)?;
        }
    }

    Ok(())
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
