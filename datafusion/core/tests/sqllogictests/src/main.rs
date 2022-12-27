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

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::{DFParser, Statement};
use log::info;
use normalize::convert_batches;
use sqllogictest::DBOutput;
use sqlparser::ast::Statement as SQLStatement;
use std::path::Path;
use std::time::Duration;

use crate::error::{DFSqlLogicTestError, Result};
use crate::insert::insert;

mod error;
mod insert;
mod normalize;
mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files";

pub struct DataFusion {
    ctx: SessionContext,
    file_name: String,
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        println!("[{}] Running query: \"{}\"", self.file_name, sql);
        let result = run_query(&self.ctx, sql).await?;
        Ok(result)
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universial to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[tokio::main]
#[cfg(target_family = "windows")]
pub async fn main() -> Result<()> {
    println!("Skipping test on windows");
    Ok(())
}

#[tokio::main]
#[cfg(not(target_family = "windows"))]
pub async fn main() -> Result<()> {
    // Enable logging (e.g. set RUST_LOG=debug to see debug logs)

    use sqllogictest::{default_validator, update_test_file};
    env_logger::init();

    let options = Options::new();

    // default to all files in test directory filtering based on name
    let files: Vec<_> = std::fs::read_dir(TEST_DIRECTORY)
        .unwrap()
        .map(|path| path.unwrap().path())
        .filter(|path| options.check_test_file(path.as_path()))
        .collect();

    info!("Running test files {:?}", files);

    for path in files {
        println!("Running: {}", path.display());

        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

        // Create the test runner
        let ctx = context_for_test_file(&file_name).await;
        let mut runner = sqllogictest::Runner::new(DataFusion { ctx, file_name });

        // run each file using its own new DB
        //
        // We could run these tests in parallel eventually if we wanted.
        if options.complete_mode {
            info!("Using complete mode to complete {}", path.display());
            let col_separator = " ";
            let validator = default_validator;
            update_test_file(path, runner, col_separator, validator)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            // run the test normally:
            runner.run_file_async(path).await?;
        }
    }

    Ok(())
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

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DBOutput> {
    let sql = sql.into();
    // Check if the sql is `insert`
    if let Ok(mut statements) = DFParser::parse_sql(&sql) {
        let statement0 = statements.pop_front().expect("at least one SQL statement");
        if let Statement::Statement(statement) = statement0 {
            let statement = *statement;
            if matches!(&statement, SQLStatement::Insert { .. }) {
                return insert(ctx, statement).await;
            }
        }
    }
    let df = ctx.sql(sql.as_str()).await?;
    let results: Vec<RecordBatch> = df.collect().await?;
    let formatted_batches = convert_batches(results)?;
    Ok(formatted_batches)
}

/// Parsed command line options
struct Options {
    // regex like
    /// arguments passed to the program which are treated as
    /// cargo test filter (substring match on filenames)
    filters: Vec<String>,

    /// Auto complete mode to fill out expected results
    complete_mode: bool,
}

impl Options {
    fn new() -> Self {
        let args: Vec<_> = std::env::args().collect();

        let complete_mode = args.iter().any(|a| a == "--complete");

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
