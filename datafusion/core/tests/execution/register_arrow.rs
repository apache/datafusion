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

//! Integration tests for register_arrow API

use datafusion::{execution::options::ArrowReadOptions, prelude::*};
use datafusion_common::Result;

#[tokio::test]
async fn test_register_arrow_auto_detects_format() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_arrow(
        "file_format",
        "../../datafusion/datasource-arrow/tests/data/example.arrow",
        ArrowReadOptions::default(),
    )
    .await?;

    ctx.register_arrow(
        "stream_format",
        "../../datafusion/datasource-arrow/tests/data/example_stream.arrow",
        ArrowReadOptions::default(),
    )
    .await?;

    let file_result = ctx.sql("SELECT * FROM file_format ORDER BY f0").await?;
    let stream_result = ctx.sql("SELECT * FROM stream_format ORDER BY f0").await?;

    let file_batches = file_result.collect().await?;
    let stream_batches = stream_result.collect().await?;

    assert_eq!(file_batches.len(), stream_batches.len());
    assert_eq!(file_batches[0].schema(), stream_batches[0].schema());

    let file_rows: usize = file_batches.iter().map(|b| b.num_rows()).sum();
    let stream_rows: usize = stream_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(file_rows, stream_rows);

    Ok(())
}

#[tokio::test]
async fn test_register_arrow_join_file_and_stream() -> Result<()> {
    let ctx = SessionContext::new();

    ctx.register_arrow(
        "file_table",
        "../../datafusion/datasource-arrow/tests/data/example.arrow",
        ArrowReadOptions::default(),
    )
    .await?;

    ctx.register_arrow(
        "stream_table",
        "../../datafusion/datasource-arrow/tests/data/example_stream.arrow",
        ArrowReadOptions::default(),
    )
    .await?;

    let result = ctx
        .sql(
            "SELECT a.f0, a.f1, b.f0, b.f1
             FROM file_table a
             JOIN stream_table b ON a.f0 = b.f0
             WHERE a.f0 <= 2
             ORDER BY a.f0",
        )
        .await?;
    let batches = result.collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);

    Ok(())
}
