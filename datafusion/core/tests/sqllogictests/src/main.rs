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

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
#[cfg(target_family = "windows")]
use std::thread;

use futures::stream::StreamExt;
use log::info;
use sqllogictest::strict_column_validator;
use tempfile::TempDir;

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DataFusionError, Result};

use crate::engines::datafusion::DataFusion;
use crate::engines::postgres::Postgres;

mod engines;
mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files/";
const PG_COMPAT_FILE_PREFIX: &str = "pg_compat_";

#[cfg(target_family = "windows")]
pub fn main() {
    // Tests from `tpch/tpch.slt` fail with stackoverflow with the default stack size.
    thread::Builder::new()
        .stack_size(2 * 1024 * 1024) // 2 MB
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { run_tests().await })
                .unwrap()
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::main]
#[cfg(not(target_family = "windows"))]
pub async fn main() -> Result<()> {
    run_tests().await
}

async fn run_tests() -> Result<()> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)
    env_logger::init();

    let options = Options::new();

    // Run all tests in parallel, reporting failures at the end
    //
    // Doing so is safe because each slt file runs with its own
    // `SessionContext` and should not have side effects (like
    // modifying shared state like `/tmp/`)
    let errors: Vec<_> = futures::stream::iter(read_test_files(&options))
        .map(|test_file| {
            tokio::task::spawn(async move {
                println!("Running {:?}", test_file.relative_path);
                if options.complete_mode {
                    run_complete_file(test_file).await?;
                } else if options.postgres_runner {
                    run_test_file_with_postgres(test_file).await?;
                } else {
                    run_test_file(test_file).await?;
                }
                Ok(()) as Result<()>
            })
        })
        // run up to num_cpus streams in parallel
        .buffer_unordered(num_cpus::get())
        .flat_map(|result| {
            // Filter out any Ok() leaving only the DataFusionErrors
            futures::stream::iter(match result {
                // Tokio panic error
                Err(e) => Some(DataFusionError::External(Box::new(e))),
                Ok(thread_result) => match thread_result {
                    // Test run error
                    Err(e) => Some(e),
                    // success
                    Ok(_) => None,
                },
            })
        })
        .collect()
        .await;

    // report on any errors
    if !errors.is_empty() {
        for e in &errors {
            println!("{e}");
        }
        Err(DataFusionError::Execution(format!(
            "{} failures",
            errors.len()
        )))
    } else {
        Ok(())
    }
}

async fn run_test_file(test_file: TestFile) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    info!("Running with DataFusion runner: {}", path.display());
    let Some(test_ctx) = context_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    let ctx = test_ctx.session_ctx().clone();
    let mut runner = sqllogictest::Runner::new(DataFusion::new(ctx, relative_path));
    runner.with_column_validator(strict_column_validator);
    runner
        .run_file_async(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

async fn run_test_file_with_postgres(test_file: TestFile) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    info!("Running with Postgres runner: {}", path.display());
    let postgres_client = Postgres::connect(relative_path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut runner = sqllogictest::Runner::new(postgres_client);
    runner.with_column_validator(strict_column_validator);
    runner
        .run_file_async(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

async fn run_complete_file(test_file: TestFile) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    use sqllogictest::default_validator;

    info!("Using complete mode to complete: {}", path.display());

    let Some(test_ctx) = context_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    let ctx = test_ctx.session_ctx().clone();
    let mut runner =
        sqllogictest::Runner::new(DataFusion::new(ctx, relative_path.clone()));
    let col_separator = " ";
    runner
        .update_test_file(
            path,
            col_separator,
            default_validator,
            strict_column_validator,
        )
        .await
        // Can't use e directly because it isn't marked Send, so turn it into a string.
        .map_err(|e| {
            DataFusionError::Execution(format!("Error completing {relative_path:?}: {e}"))
        })
}

/// Represents a parsed test file
#[derive(Debug)]
struct TestFile {
    /// The absolute path to the file
    pub path: PathBuf,
    /// The relative path of the file (used for display)
    pub relative_path: PathBuf,
}

impl TestFile {
    fn new(path: PathBuf) -> Self {
        let relative_path = PathBuf::from(
            path.to_string_lossy()
                .strip_prefix(TEST_DIRECTORY)
                .unwrap_or(""),
        );

        Self {
            path,
            relative_path,
        }
    }

    fn is_slt_file(&self) -> bool {
        self.path.extension() == Some(OsStr::new("slt"))
    }

    fn check_tpch(&self, options: &Options) -> bool {
        if !self.relative_path.starts_with("tpch") {
            return true;
        }

        options.include_tpch
    }
}

fn read_test_files<'a>(options: &'a Options) -> Box<dyn Iterator<Item=TestFile> + 'a> {
    Box::new(
        read_dir_recursive(TEST_DIRECTORY)
            .map(TestFile::new)
            .filter(|f| options.check_test_file(&f.relative_path))
            .filter(|f| f.is_slt_file())
            .filter(|f| f.check_tpch(options))
            .filter(|f| options.check_pg_compat_file(f.path.as_path())),
    )
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item=PathBuf>> {
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

/// Create a SessionContext, configured for the specific test, if
/// possible.
///
/// If `None` is returned (e.g. because some needed feature is not
/// enabled), the file should be skipped
async fn context_for_test_file(relative_path: &Path) -> Option<TestContext> {
    let config = SessionConfig::new()
        // hardcode target partitions so plans are deterministic
        .with_target_partitions(4);

    let test_ctx = TestContext::new(SessionContext::with_config(config));

    let file_name = relative_path.file_name().unwrap().to_str().unwrap();
    match file_name {
        "aggregate.slt" => {
            info!("Registering aggregate tables");
            setup::register_aggregate_tables(test_ctx.session_ctx()).await;
        }
        "scalar.slt" => {
            info!("Registering scalar tables");
            setup::register_scalar_tables(test_ctx.session_ctx()).await;
        }
        "avro.slt" => {
            #[cfg(feature = "avro")]
            {
                let mut test_ctx = test_ctx;
                info!("Registering avro tables");
                setup::register_avro_tables(&mut test_ctx).await;
                return Some(test_ctx);
            }
            #[cfg(not(feature = "avro"))]
            {
                info!("Skipping {file_name} because avro feature is not enabled");
                return None;
            }
        }
        _ => {
            info!("Using default SessionContext");
        }
    };
    Some(test_ctx)
}

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            test_dir: None,
        }
    }

    /// Enables the test directory feature. If not enabled,
    /// calling `testdir_path` will result in a panic.
    pub fn enable_testdir(&mut self) {
        if self.test_dir.is_none() {
            self.test_dir = Some(TempDir::new().expect("failed to create testdir"));
        }
    }

    /// Returns the path to the test directory. Panics if the test
    /// directory feature is not enabled via `enable_testdir`.
    pub fn testdir_path(&self) -> &Path {
        self.test_dir.as_ref().expect("testdir not enabled").path()
    }

    /// Returns a reference to the internal SessionContext
    fn session_ctx(&self) -> &SessionContext {
        &self.ctx
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

    /// Run Postgres compatibility tests with Postgres runner
    postgres_runner: bool,

    /// Include tpch files
    include_tpch: bool,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");
        let postgres_runner = std::env::var("PG_COMPAT").map_or(false, |_| true);
        let include_tpch = std::env::var("INCLUDE_TPCH").map_or(false, |_| true);

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
            include_tpch,
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
