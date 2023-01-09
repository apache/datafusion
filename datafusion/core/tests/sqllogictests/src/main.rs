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
use std::fs::copy;
use std::path::{Path, PathBuf};

use log::info;
use tempfile::tempdir;
use testcontainers::clients::Cli as Docker;

use datafusion::prelude::SessionContext;

use crate::engines::datafusion::DataFusion;
use crate::engines::postgres;
use crate::engines::postgres::{Postgres};
use crate::engines::postgres::image::{PG_DB, PG_PASSWORD, PG_PORT, PG_USER};

mod engines;
mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files";
const TEST_DIRECTORY_POSTGRES: &str = "tests/sqllogictests/postgres/test_files";

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

    let files: Vec<_> = read_test_files(&options);

    info!("Running test files {:?}", files);

    for path in files {
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

        if options.complete_mode {
            run_complete_file(&path, file_name).await?;
        } else if options.postgres_mode {
            run_postgres_test_file(&path, file_name).await?;
        } else {
            run_test_file(&path, file_name).await?;
        }
    }

    Ok(())
}

async fn run_test_file(path: &PathBuf, file_name: String) -> Result<(), Box<dyn Error>> {
    println!("Running: {}", path.display());
    let ctx = context_for_test_file(&file_name).await;
    let mut runner = sqllogictest::Runner::new(DataFusion::new(ctx, file_name));
    runner.run_file_async(path).await?;
    Ok(())
}

async fn run_postgres_test_file(
    path: &PathBuf,
    file_name: String,
) -> Result<(), Box<dyn Error>> {
    use sqllogictest::{default_validator, update_test_file};
    info!("Running postgres test: {}", path.display());

    let docker = Docker::default();
    let postgres_container = docker.run(postgres::image::postgres_docker_image());

    let postgres_client = Postgres::connect_with_retry(
        "127.0.0.1",
        postgres_container.get_host_port_ipv4(PG_PORT),
        PG_DB,
        PG_USER,
        PG_PASSWORD,
    )
    .await?;
    let postgres_runner = sqllogictest::Runner::new(postgres_client);

    // let temp_dir = tempdir()?;
    // let copy_path = temp_dir.path().join(&file_name);

    // copy(path, &copy_path)?;
    update_test_file(&path, postgres_runner, " ", default_validator).await?;

    let ctx = SessionContext::new();
    setup::register_aggregate_csv_by_sql(&ctx).await;
    let mut df_runner = sqllogictest::Runner::new(DataFusion::new(ctx, file_name));

    df_runner.run_file_async(path).await?;
    Ok(())
}

async fn run_complete_file(
    path: &PathBuf,
    file_name: String,
) -> Result<(), Box<dyn Error>> {
    use sqllogictest::{default_validator, update_test_file};

    info!("Using complete mode to complete: {}", path.display());

    let ctx = context_for_test_file(&file_name).await;
    let runner = sqllogictest::Runner::new(DataFusion::new(ctx, file_name));

    let col_separator = " ";
    let validator = default_validator;
    update_test_file(path, runner, col_separator, validator)
        .await
        .map_err(|e| e.to_string())?;

    Ok(())
}

fn read_test_files(options: &Options) -> Vec<PathBuf> {
    let path = if options.postgres_mode {
        TEST_DIRECTORY_POSTGRES
    } else {
        TEST_DIRECTORY
    };
    std::fs::read_dir(path)
        .unwrap()
        .map(|path| path.unwrap().path())
        .filter(|path| options.check_test_file(path.as_path()))
        .collect()
}

/// Create a SessionContext, configured for the specific test
async fn context_for_test_file(file_name: &str) -> SessionContext {
    match file_name {
        "aggregate.slt" | "select.slt" => {
            info!("Registering aggregate tables");
            let ctx = SessionContext::new();
            setup::register_aggregate_tables(&ctx).await;
            ctx
        }
        _ => {
            info!("Using default SessionContext");
            SessionContext::new()
        }
    }
}

/// Parsed command line options
struct Options {
    // regex like
    /// arguments passed to the program which are treated as
    /// cargo test filter (substring match on filenames)
    filters: Vec<String>,

    /// Auto complete mode to fill out expected results
    complete_mode: bool,

    /// Run Postgres compatibility tests
    postgres_mode: bool,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");
        let postgres_mode = args.iter().any(|a| a == "--postgres");

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
            postgres_mode,
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
    fn check_test_file(&self, path: &Path) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // otherwise check if any filter matches
        let path_str = path.to_string_lossy();
        self.filters.iter().any(|filter| path_str.contains(filter))
    }
}
