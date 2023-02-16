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

use std::error::Error;
use std::path::{Path, PathBuf};

use log::info;
use sqllogictest::strict_column_validator;

use datafusion::prelude::{SessionConfig, SessionContext};

use crate::engines::datafusion::DataFusion;
use crate::engines::postgres::Postgres;

mod engines;
mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files/";
const PG_COMPAT_FILE_PREFIX: &str = "pg_compat_";

#[tokio::main]
#[cfg(target_family = "windows")]
pub async fn main() -> Result<(), Box<dyn Error>> {
    println!("Skipping test on windows");
    Ok(())
}

#[tokio::main]
#[cfg(not(target_family = "windows"))]
pub async fn main() -> Result<(), Box<dyn Error>> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)
    env_logger::init();

    let options = Options::new();

    for (path, relative_path) in read_test_files(&options) {
        if options.complete_mode {
            run_complete_file(&path, relative_path).await?;
        } else if options.postgres_runner {
            run_test_file_with_postgres(&path, relative_path).await?;
        } else {
            run_test_file(&path, relative_path).await?;
        }
    }

    Ok(())
}

async fn run_test_file(
    path: &Path,
    relative_path: PathBuf,
) -> Result<(), Box<dyn Error>> {
    info!("Running with DataFusion runner: {}", path.display());
    let ctx = context_for_test_file(&relative_path).await;
    let mut runner = sqllogictest::Runner::new(DataFusion::new(ctx, relative_path));
    runner.with_column_validator(strict_column_validator);
    runner.run_file_async(path).await?;
    Ok(())
}

async fn run_test_file_with_postgres(
    path: &Path,
    relative_path: PathBuf,
) -> Result<(), Box<dyn Error>> {
    info!("Running with Postgres runner: {}", path.display());
    let postgres_client = Postgres::connect(relative_path).await?;
    let mut runner = sqllogictest::Runner::new(postgres_client);
    runner.with_column_validator(strict_column_validator);
    runner.run_file_async(path).await?;
    Ok(())
}

async fn run_complete_file(
    path: &Path,
    relative_path: PathBuf,
) -> Result<(), Box<dyn Error>> {
    use sqllogictest::default_validator;

    info!("Using complete mode to complete: {}", path.display());

    let ctx = context_for_test_file(&relative_path).await;
    let mut runner = sqllogictest::Runner::new(DataFusion::new(ctx, relative_path));
    let col_separator = " ";
    runner
        .update_test_file(
            path,
            col_separator,
            default_validator,
            strict_column_validator,
        )
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn read_test_files<'a>(
    options: &'a Options,
) -> Box<dyn Iterator<Item = (PathBuf, PathBuf)> + 'a> {
    Box::new(
        read_dir_recursive(TEST_DIRECTORY)
            .map(|path| {
                (
                    path.clone(),
                    PathBuf::from(
                        path.to_string_lossy().strip_prefix(TEST_DIRECTORY).unwrap(),
                    ),
                )
            })
            .filter(|(_, relative_path)| options.check_test_file(relative_path))
            .filter(|(path, _)| options.check_pg_compat_file(path.as_path())),
    )
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = PathBuf>> {
    Box::new(
        std::fs::read_dir(path)
            .expect("Readable directory")
            .map(|path| path.expect("Readable entry").path())
            .flat_map(|path| {
                if path.is_dir() {
                    read_dir_recursive(path)
                } else {
                    Box::new(std::iter::once(path))
                }
            }),
    )
}

/// Create a SessionContext, configured for the specific test
async fn context_for_test_file(relative_path: &Path) -> SessionContext {
    let config = SessionConfig::new()
        // hardcode target partitions so plans are deterministic
        .with_target_partitions(4);

    let ctx = SessionContext::with_config(config);

    match relative_path.file_name().unwrap().to_str().unwrap() {
        "aggregate.slt" | "select.slt" => {
            info!("Registering aggregate tables");
            setup::register_aggregate_tables(&ctx).await;
        }
        _ => {
            info!("Using default SessionContext");
        }
    };
    ctx
}

/// Parsed command line options
struct Options {
    // regex like
    /// arguments passed to the program which are treated as
    /// cargo test filter (substring match on filenames)
    filters: Vec<String>,

    /// Auto complete mode to fill out expected results
    complete_mode: bool,

    /// Run Postgres compatibility tests with Postgres runner
    postgres_runner: bool,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");
        let postgres_runner = std::env::var("PG_COMPAT").map_or(false, |_| true);

        // treat args after the first as filters to run (substring matching)
        let filters = if !args.is_empty() {
            args.into_iter()
                .skip(1)
                // ignore command line arguments like `--complete`
                .filter(|arg| !arg.as_str().starts_with("--"))
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        Self {
            filters,
            complete_mode,
            postgres_runner,
        }
    }

    /// Because this test can be run as a cargo test, commands like
    ///
    /// ```shell
    /// cargo test foo
    /// ```
    ///
    /// Will end up passing `foo` as a command line argument.
    ///
    /// To be compatible with this, treat the command line arguments as a
    /// filter and that does a substring match on each input.  returns
    /// true f this path should be run
    fn check_test_file(&self, relative_path: &Path) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // otherwise check if any filter matches
        self.filters
            .iter()
            .any(|filter| relative_path.to_string_lossy().contains(filter))
    }

    /// Postgres runner executes only tests in files with specific names
    fn check_pg_compat_file(&self, path: &Path) -> bool {
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        !self.postgres_runner || file_name.starts_with(PG_COMPAT_FILE_PREFIX)
    }
}
