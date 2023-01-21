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

use std::time::Duration;

use sqllogictest::DBOutput;

use self::error::{DFSqlLogicTestError, Result};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::{DFParser, Statement};
use insert::insert;
use sqlparser::ast::Statement as SQLStatement;

mod error;
mod insert;
mod normalize;

pub struct DataFusion {
    ctx: SessionContext,
    file_name: String,
    is_pg_compatibility_test: bool,
}

impl DataFusion {
    pub fn new(
        ctx: SessionContext,
        file_name: String,
        postgres_compatible: bool,
    ) -> Self {
        Self {
            ctx,
            file_name,
            is_pg_compatibility_test: postgres_compatible,
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;

    async fn run(&mut self, sql: &str) -> Result<DBOutput> {
        println!("[{}] Running query: \"{}\"", self.file_name, sql);
        let result = run_query(&self.ctx, sql, self.is_pg_compatibility_test).await?;
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

async fn run_query(
    ctx: &SessionContext,
    sql: impl Into<String>,
    is_pg_compatibility_test: bool,
) -> Result<DBOutput> {
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
    let formatted_batches =
        normalize::convert_batches(results, is_pg_compatibility_test)?;
    Ok(formatted_batches)
}
