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

//! # DataFusion repository development tools
//!
//! ## Example
//!
//! Run the default CI test suite locally:
//!
//! ```sh
//! cargo xtask ci step test workspace
//! ```
//!
//! Implementing the command inside `xtask` can make it easier to maintain. For example,
//! the local test runs and CI runs can keep in sync.
//!
//! ## Mechanism
//!
//! `xtask` is an internal Rust binary in this workspace. The `cargo xtask` alias
//! builds and runs that binary, forwarding the remaining arguments to it.
//!
//! There is no external dependency required.
//!
//! ## Reference
//!
//! The [`xtask` convention](https://github.com/matklad/cargo-xtask) keeps project
//! automation in ordinary Rust code. It is used by projects such as
//! [rust-analyzer](https://github.com/rust-lang/rust-analyzer/tree/master/xtask).
//! Cargo uses the same pattern for several
//! [`xtask-*` maintenance commands](https://github.com/rust-lang/cargo/blob/master/.cargo/config.toml).
//!
//! # Supported Commands
//!
//! ## CI steps
//!
//! A CI step is one command used by DataFusion's CI that can also be run
//! locally. `--explain` prints the complete shell command without executing it.
//!
//! Use `cargo xtask ci step help` to list the available steps and
//! `cargo xtask ci step <step-name> help` for step-specific arguments.
//!
//! # Examples
//!
//! Show all available CI steps:
//!
//! ```sh
//! cargo xtask help
//! ```
//!
//! Show the arguments and examples for the `test` step:
//!
//! ```sh
//! cargo xtask ci step test help
//! ```
//!
//! Run the `cli` variant of the `test` step:
//!
//! ```sh
//! cargo xtask ci step test cli
//! ```
//!
//! Print that step's shell command without running it:
//!
//! ```sh
//! cargo xtask ci step test cli --explain
//! ```

mod ci_steps;

use std::env;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, String>;

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let result = Xtask::new().and_then(|xtask| xtask.run(&args));

    if let Err(error) = result {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

struct Xtask {
    root: PathBuf,
}

impl Xtask {
    fn new() -> Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let root = manifest_dir
            .parent()
            .ok_or_else(|| {
                format!(
                    "could not find workspace root from {}",
                    manifest_dir.display()
                )
            })?
            .to_path_buf();
        Ok(Self { root })
    }

    fn run(&self, args: &[String]) -> Result<()> {
        match args {
            [] => {
                print!("{}", ci_steps::help());
                Ok(())
            }
            [help_arg] if ci_steps::is_help_arg(help_arg) => {
                print!("{}", ci_steps::help());
                Ok(())
            }
            [ci, step, step_args @ ..] if ci == "ci" && step == "step" => {
                ci_steps::run(&self.root, step_args)
            }
            _ => Err(format!("unknown command\n\n{}", ci_steps::help())),
        }
    }
}
