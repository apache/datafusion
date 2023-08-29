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

use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use datafusion_common::test_util::parquet_test_data;

#[tokio::test]
async fn describe_plan() {
    let ctx = parquet_context().await;

    let query = "describe alltypes_tiny_pages";
    let results = ctx.sql(query).await.unwrap().collect().await.unwrap();

    let expected = vec![
        "+-----------------+-----------------------------+-------------+",
        "| column_name     | data_type                   | is_nullable |",
        "+-----------------+-----------------------------+-------------+",
        "| id              | Int32                       | YES         |",
        "| bool_col        | Boolean                     | YES         |",
        "| tinyint_col     | Int8                        | YES         |",
        "| smallint_col    | Int16                       | YES         |",
        "| int_col         | Int32                       | YES         |",
        "| bigint_col      | Int64                       | YES         |",
        "| float_col       | Float32                     | YES         |",
        "| double_col      | Float64                     | YES         |",
        "| date_string_col | Utf8                        | YES         |",
        "| string_col      | Utf8                        | YES         |",
        "| timestamp_col   | Timestamp(Nanosecond, None) | YES         |",
        "| year            | Int32                       | YES         |",
        "| month           | Int32                       | YES         |",
        "+-----------------+-----------------------------+-------------+",
    ];

    assert_batches_eq!(expected, &results);

    // also ensure we plan Describe via SessionState
    let state = ctx.state();
    let plan = state.create_logical_plan(query).await.unwrap();
    let df = DataFrame::new(state, plan);
    let results = df.collect().await.unwrap();

    assert_batches_eq!(expected, &results);
}

/// Return a SessionContext with parquet file registered
async fn parquet_context() -> SessionContext {
    let ctx = SessionContext::new();
    let testdata = parquet_test_data();
    ctx.register_parquet(
        "alltypes_tiny_pages",
        &format!("{testdata}/alltypes_tiny_pages.parquet"),
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx
}
