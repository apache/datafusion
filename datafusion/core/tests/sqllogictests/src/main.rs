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
use datafusion::arrow::csv::WriterBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_sql::parser::{DFParser, Statement};
use log::info;
use normalize::normalize_batch;
use sqlparser::ast::Statement as SQLStatement;
use std::path::{Path, PathBuf};
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

    async fn run(&mut self, sql: &str) -> Result<String> {
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
    env_logger::init();

    // run each file using its own new DB
    //
    // Note: can't use tester.run_parallel_async()
    // as that will reuse the same SessionContext
    //
    // We could run these tests in parallel eventually if we wanted.

    let files = get_test_files();
    info!("Running test files {:?}", files);

    for path in files {
        println!("Running: {}", path.display());

        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

        let ctx = context_for_test_file(&file_name).await;

        let mut tester = sqllogictest::Runner::new(DataFusion { ctx, file_name });
        tester.run_file_async(path).await?;
    }

    Ok(())
}

/// Gets a list of test files to execute. If there were arguments
/// passed to the program treat it as a cargo test filter (substring match on filenames)
fn get_test_files() -> Vec<PathBuf> {
    info!("Test directory: {}", TEST_DIRECTORY);

    let args: Vec<_> = std::env::args().collect();

    // treat args after the first as filters to run (substring matching)
    let filters = if !args.is_empty() {
        args.iter()
            .skip(1)
            .map(|arg| arg.as_str())
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    // default to all files in test directory filtering based on name
    std::fs::read_dir(TEST_DIRECTORY)
        .unwrap()
        .map(|path| path.unwrap().path())
        .filter(|path| check_test_file(&filters, path.as_path()))
        .collect()
}

/// because this test can be run as a cargo test, commands like
///
/// ```shell
/// cargo test foo
/// ```
///
/// Will end up passing `foo` as a command line argument.
///
/// be compatible with this, treat the command line arguments as a
/// filter and that does a substring match on each input.
/// returns true f this path should be run
fn check_test_file(filters: &[&str], path: &Path) -> bool {
    if filters.is_empty() {
        return true;
    }

    // otherwise check if any filter matches
    let path_str = path.to_string_lossy();
    filters.iter().any(|filter| path_str.contains(filter))
}

/// Create a SessionContext, configured for the specific test
async fn context_for_test_file(file_name: &str) -> SessionContext {
    match file_name {
        "aggregate.slt" => {
            info!("Registering aggregate tables");
            let ctx = SessionContext::new();
            setup::register_aggregate_tables(&ctx).await;
            ctx
        }
        "information_schema.slt" => {
            info!("Enabling information schema");
            SessionContext::with_config(
                SessionConfig::new().with_information_schema(true),
            )
        }
        _ => {
            info!("Using default SessionContext");
            SessionContext::new()
        }
    }
}

fn format_batches(batches: Vec<RecordBatch>) -> Result<String> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new().has_headers(false).with_delimiter(b' ');
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(&normalize_batch(batch)).unwrap();
        }
    }
    Ok(String::from_utf8(bytes).unwrap())
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<String> {
    let sql = sql.into();
    // Check if the sql is `insert`
    if let Ok(statements) = DFParser::parse_sql(&sql) {
        if let Statement::Statement(statement) = &statements[0] {
            if let SQLStatement::Insert { .. } = &**statement {
                return insert(ctx, statement).await;
            }
        }
    }
    let df = ctx.sql(sql.as_str()).await.unwrap();
    let results: Vec<RecordBatch> = df.collect().await.unwrap();
    let formatted_batches = format_batches(results)?;
    Ok(formatted_batches)
}
