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

use std::path::PathBuf;
use std::time::Duration;

use crate::engines::output::{DFColumnType, DFOutput};

use self::error::{DFSqlLogicTestError, Result};
use async_trait::async_trait;
use create_table::create_table;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_sql::parser::{DFParser, Statement};
use insert::insert;
use sqllogictest::DBOutput;
use sqlparser::ast::Statement as SQLStatement;

mod create_table;
mod error;
mod insert;
mod normalize;
mod util;

pub struct DataFusion {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl DataFusion {
    pub fn new(ctx: SessionContext, relative_path: PathBuf) -> Self {
        Self { ctx, relative_path }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        println!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
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

async fn run_query(ctx: &SessionContext, sql: impl Into<String>) -> Result<DFOutput> {
    let sql = sql.into();
    // Check if the sql is `insert`
    if let Ok(mut statements) = DFParser::parse_sql(&sql) {
        let statement0 = statements.pop_front().expect("at least one SQL statement");
        if let Statement::Statement(statement) = statement0 {
            let statement = *statement;
            match statement {
                SQLStatement::Insert { .. } => return insert(ctx, statement).await,
                SQLStatement::CreateTable {
                    query,
                    constraints,
                    table_properties,
                    with_options,
                    name,
                    columns,
                    if_not_exists,
                    or_replace,
                    ..
                } if query.is_none()
                    && constraints.is_empty()
                    && table_properties.is_empty()
                    && with_options.is_empty() =>
                {
                    return create_table(ctx, name, columns, if_not_exists, or_replace)
                        .await
                }
                _ => {}
            };
        }
    }
    let df = ctx.sql(sql.as_str()).await?;

    let types = normalize::convert_schema_to_types(df.schema().fields());
    let results: Vec<RecordBatch> = df.collect().await?;
    let rows = normalize::convert_batches(results)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}
