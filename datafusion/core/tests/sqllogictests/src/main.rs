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
use datafusion::prelude::{SessionContext, SessionConfig};
use std::path::Path;
use std::time::Duration;

use sqllogictest::TestError;
pub type Result<T> = std::result::Result<T, TestError>;

mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files";

pub struct DataFusion {
    ctx: SessionContext,
    file_name: String,
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = TestError;

    async fn run(&mut self, sql: &str) -> Result<String> {
        println!(
            "[{}] Running query: \"{}\"",
            self.file_name,
            sql
        );
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
    if cfg!(target_os = "windows") {
        println!("Skipping test on windows");
    }
}



#[tokio::main]
#[cfg(not(target_family = "windows"))]
pub async fn main() -> Result<()> {
    let paths = std::fs::read_dir(TEST_DIRECTORY).unwrap();

    // run each file using its own new SessionContext
    //
    // Note: can't use tester.run_parallel_async()
    // as that will reuse the same SessionContext

    for path in paths {
        // TODO better error handling
        let path = path.unwrap().path();

        run_file(&path).await?;
    }

    Ok(())
}


async fn run_file(path: &Path) -> Result<()> {
    println!("Running: {}", path.display());

    let file_name =  path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let ctx = context_for_test_file(&file_name).await;

    let mut tester = sqllogictest::Runner::new(DataFusion { ctx, file_name });
    tester.run_file_async(path).await?;

    Ok(())
}

async fn context_for_test_file(file_name: &str) -> SessionContext {

    // find relevant test_category, if any, based on file name
    match file_name {
        "aggregate.slt" => {
            println!("Registering aggregate tables");
            let ctx = SessionContext::new();
            setup::register_aggregate_tables(&ctx).await;
            ctx
        }
        "information_schema.slt" => {
            println!("Enabling information schema");
            SessionContext::with_config(SessionConfig::new().with_information_schema(true))
        }
        _ => {
            println!("No extra context");
            SessionContext::new()
        }
    }

}

fn format_batches(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new().has_headers(false).with_delimiter(b' ');
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch).unwrap();
        }
    }
    Ok(String::from_utf8(bytes).unwrap())
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<String> {
    let df = ctx.sql(&sql.into()).await.unwrap();
    let results: Vec<RecordBatch> = df.collect().await.unwrap();
    let formatted_batches = format_batches(&results)?;
    Ok(formatted_batches)
}
