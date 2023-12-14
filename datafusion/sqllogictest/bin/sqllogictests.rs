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
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(target_family = "windows")]
use std::thread;

use datafusion_sqllogictest::{DataFusion, TestContext};
use futures::stream::StreamExt;
use log::info;
use sqllogictest::strict_column_validator;

use datafusion_common::{exec_datafusion_err, exec_err, DataFusionError, Result};

const TEST_DIRECTORY: &str = "test_files/";
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

/// Sets up an empty directory at test_files/scratch/<name>
/// creating it if needed and clearing any file contents if it exists
/// This allows tests for inserting to external tables or copy to
/// to persist data to disk and have consistent state when running
/// a new test
fn setup_scratch_dir(name: &Path) -> Result<()> {
    // go from copy.slt --> copy
    let file_stem = name.file_stem().expect("File should have a stem");
    let path = PathBuf::from("test_files").join("scratch").join(file_stem);

    info!("Creating scratch dir in {path:?}");
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    Ok(())
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
    let errors: Vec<_> = futures::stream::iter(read_test_files(&options)?)
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
        exec_err!("{} failures", errors.len())
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
    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;
    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(DataFusion::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
        ))
    });
    runner.with_column_validator(strict_column_validator);
    runner
        .run_file_async(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

#[cfg(feature = "postgres")]
async fn run_test_file_with_postgres(test_file: TestFile) -> Result<()> {
    use datafusion_sqllogictest::Postgres;
    let TestFile {
        path,
        relative_path,
    } = test_file;
    info!("Running with Postgres runner: {}", path.display());
    setup_scratch_dir(&relative_path)?;
    let mut runner =
        sqllogictest::Runner::new(|| Postgres::connect(relative_path.clone()));
    runner.with_column_validator(strict_column_validator);
    runner
        .run_file_async(path)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(())
}

#[cfg(not(feature = "postgres"))]
async fn run_test_file_with_postgres(_test_file: TestFile) -> Result<()> {
    use datafusion_common::plan_err;
    plan_err!("Can not run with postgres as postgres feature is not enabled")
}

async fn run_complete_file(test_file: TestFile) -> Result<()> {
    let TestFile {
        path,
        relative_path,
    } = test_file;
    use sqllogictest::default_validator;

    info!("Using complete mode to complete: {}", path.display());

    let Some(test_ctx) = TestContext::try_new_for_test_file(&relative_path).await else {
        info!("Skipping: {}", path.display());
        return Ok(());
    };
    setup_scratch_dir(&relative_path)?;
    let mut runner = sqllogictest::Runner::new(|| async {
        Ok(DataFusion::new(
            test_ctx.session_ctx().clone(),
            relative_path.clone(),
        ))
    });
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

fn read_test_files<'a>(
    options: &'a Options,
) -> Result<Box<dyn Iterator<Item = TestFile> + 'a>> {
    Ok(Box::new(
        read_dir_recursive(TEST_DIRECTORY)?
            .into_iter()
            .map(TestFile::new)
            .filter(|f| options.check_test_file(&f.relative_path))
            .filter(|f| f.is_slt_file())
            .filter(|f| f.check_tpch(options))
            .filter(|f| options.check_pg_compat_file(f.path.as_path())),
    ))
}

fn read_dir_recursive<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>> {
    let mut dst = vec![];
    read_dir_recursive_impl(&mut dst, path.as_ref())?;
    Ok(dst)
}

/// Append all paths recursively to dst
fn read_dir_recursive_impl(dst: &mut Vec<PathBuf>, path: &Path) -> Result<()> {
    let entries = std::fs::read_dir(path)
        .map_err(|e| exec_datafusion_err!("Error reading directory {path:?}: {e}"))?;
    for entry in entries {
        let path = entry
            .map_err(|e| {
                exec_datafusion_err!("Error reading entry in directory {path:?}: {e}")
            })?
            .path();

        if path.is_dir() {
            read_dir_recursive_impl(dst, &path)?;
        } else {
            dst.push(path);
        }
    }

    Ok(())
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
