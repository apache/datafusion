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
use datafusion::prelude::SessionContext;
use std::path::PathBuf;

use sqllogictest::TestError;
pub type Result<T> = std::result::Result<T, TestError>;

mod setup;
mod utils;

const TEST_DIRECTORY: &str = "tests/sqllogictests/test_files";
const TEST_CATEGORIES: [TestCategory; 2] =
    [TestCategory::Aggregate, TestCategory::ArrowTypeOf];

pub enum TestCategory {
    Aggregate,
    ArrowTypeOf,
}

impl TestCategory {
    fn as_str(&self) -> &'static str {
        match self {
            TestCategory::Aggregate => "Aggregate",
            TestCategory::ArrowTypeOf => "ArrowTypeOf",
        }
    }

    fn test_filename(&self) -> &'static str {
        match self {
            TestCategory::Aggregate => "aggregate.slt",
            TestCategory::ArrowTypeOf => "arrow_typeof.slt",
        }
    }

    async fn register_test_tables(&self, ctx: &SessionContext) {
        println!("[{}] Registering tables", self.as_str());
        match self {
            TestCategory::Aggregate => setup::register_aggregate_tables(&ctx).await,
            TestCategory::ArrowTypeOf => (),
        }
    }
}

pub struct DataFusion {
    ctx: SessionContext,
    test_category: TestCategory,
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = TestError;

    async fn run(&mut self, sql: &str) -> Result<String> {
        println!(
            "[{}] Running query: \"{}\"",
            self.test_category.as_str(),
            sql
        );
        let result = run_query(&self.ctx, sql).await?;
        Ok(result)
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    for test_category in TEST_CATEGORIES {
        let filename = PathBuf::from(format!(
            "{}/{}",
            TEST_DIRECTORY,
            test_category.test_filename()
        ));
        let ctx = SessionContext::new();
        test_category.register_test_tables(&ctx).await;

        let mut tester = sqllogictest::Runner::new(DataFusion { ctx, test_category });
        // TODO: use tester.run_parallel_async()
        tester.run_file_async(filename).await.unwrap();
    }

    Ok(())
}

fn format_batches(batches: &[RecordBatch]) -> Result<String> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new().has_headers(false).with_delimiter(b',');
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch).unwrap();
        }
    }

    let formatted = String::from_utf8(bytes).unwrap().replace(",", " ");
    Ok(formatted)
}

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<String> {
    let df = ctx.sql(&sql.into()).await.unwrap();
    let results: Vec<RecordBatch> = df.collect().await.unwrap();
    Ok(format_batches(&results)?)
}
