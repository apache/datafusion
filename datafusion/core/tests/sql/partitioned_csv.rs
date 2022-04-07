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

//! Utility functions for creating and running with a partitioned csv dataset.

use std::{io::Write, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    error::Result,
    prelude::{CsvReadOptions, SessionConfig, SessionContext},
};
use tempfile::TempDir;

/// Execute SQL and return results
async fn plan_and_collect(
    ctx: &mut SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

/// Execute SQL and return results
pub async fn execute(sql: &str, partition_count: usize) -> Result<Vec<RecordBatch>> {
    let tmp_dir = TempDir::new()?;
    let mut ctx = create_ctx(&tmp_dir, partition_count).await?;
    plan_and_collect(&mut ctx, sql).await
}

/// Generate CSV partitions within the supplied directory
fn populate_csv_partitions(
    tmp_dir: &TempDir,
    partition_count: usize,
    file_extension: &str,
) -> Result<SchemaRef> {
    // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));

    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{}.{}", partition, file_extension);
        let file_path = tmp_dir.path().join(&filename);
        let mut file = std::fs::File::create(file_path)?;

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes())?;
        }
    }

    Ok(schema)
}

/// Generate a partitioned CSV file and register it with an execution context
pub async fn create_ctx(
    tmp_dir: &TempDir,
    partition_count: usize,
) -> Result<SessionContext> {
    let ctx = SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

    let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

    // register csv file with the execution context
    ctx.register_csv(
        "test",
        tmp_dir.path().to_str().unwrap(),
        CsvReadOptions::new().schema(&schema),
    )
    .await?;

    Ok(ctx)
}
