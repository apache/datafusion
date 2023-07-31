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

use std::{time::Duration, path::PathBuf};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use sqllogictest::DBOutput;
use log::info;

use super::{error::Result, DFSqlLogicTestError};

use crate::engines::output::{DFColumnType, DFOutput};

use super::{convert_batches, convert_schema_to_types};

pub struct DataFusionTestEngine {
    ctx: SessionContext,
    relative_path: PathBuf,
}

impl DataFusionTestEngine {
    pub fn new(context: SessionContext, relative_path: PathBuf) -> Self {
        Self { ctx: context, relative_path }
    }
}

async fn run_query(
    ctx: &SessionContext,
    sql: impl Into<String>,
) -> Result<DFOutput> {
    let df = ctx.sql(sql.into().as_str()).await?;

    let types = convert_schema_to_types(df.schema().fields().as_slice());
    let results: Vec<RecordBatch> = df.collect().await?;
    let rows = convert_batches(results)?;

    if rows.is_empty() && types.is_empty() {
        Ok(DBOutput::StatementComplete(0))
    } else {
        Ok(DBOutput::Rows { types, rows })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusionTestEngine {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        info!(
            "[{}] Running query: \"{}\"",
            self.relative_path.display(),
            sql
        );
        run_query(&self.ctx, sql).await
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusionTestEngine"
    }

    /// [`Runner`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
