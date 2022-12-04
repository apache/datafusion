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

use super::*;

async fn register_alltypes_avro(ctx: &SessionContext) {
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_avro(
        "alltypes_plain",
        &format!("{}/avro/alltypes_plain.avro", testdata),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn avro_query() {
    let ctx = SessionContext::new();
    register_alltypes_avro(&ctx).await;
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------------------------+",
        "| id | alltypes_plain.string_col |",
        "+----+---------------------------+",
        "| 4  | 0                         |",
        "| 5  | 1                         |",
        "| 6  | 0                         |",
        "| 7  | 1                         |",
        "| 2  | 0                         |",
        "| 3  | 1                         |",
        "| 0  | 0                         |",
        "| 1  | 1                         |",
        "+----+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn avro_query_multiple_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let table_path = tempdir.path();
    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{}/avro/alltypes_plain.avro", testdata);
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    let ctx = SessionContext::new();
    ctx.register_avro(
        "alltypes_plain",
        table_path.display().to_string().as_str(),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
    // NOTE that string_col is actually a binary column and does not have the UTF8 logical type
    // so we need an explicit cast
    let sql = "SELECT id, CAST(string_col AS varchar) FROM alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+---------------------------+",
        "| id | alltypes_plain.string_col |",
        "+----+---------------------------+",
        "| 4  | 0                         |",
        "| 5  | 1                         |",
        "| 6  | 0                         |",
        "| 7  | 1                         |",
        "| 2  | 0                         |",
        "| 3  | 1                         |",
        "| 0  | 0                         |",
        "| 1  | 1                         |",
        "| 4  | 0                         |",
        "| 5  | 1                         |",
        "| 6  | 0                         |",
        "| 7  | 1                         |",
        "| 2  | 0                         |",
        "| 3  | 1                         |",
        "| 0  | 0                         |",
        "| 1  | 1                         |",
        "+----+---------------------------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn avro_single_nan_schema() {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_avro(
        "single_nan",
        &format!("{}/avro/single_nan.avro", testdata),
        AvroReadOptions::default(),
    )
    .await
    .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let runtime = ctx.task_ctx();
    let results = collect(plan, runtime).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
async fn avro_explain() {
    let ctx = SessionContext::new();
    register_alltypes_avro(&ctx).await;

    let sql = "EXPLAIN SELECT count(*) from alltypes_plain";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: COUNT(UInt8(1))\
            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n    TableScan: alltypes_plain projection=[id]",
        ],
        vec![
            "physical_plan",
            "ProjectionExec: expr=[COUNT(UInt8(1))@0 as COUNT(UInt8(1))]\
            \n  AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]\
            \n    CoalescePartitionsExec\
            \n      AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]\
            \n        RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES)\
            \n          AvroExec: files={1 group: [[ARROW_TEST_DATA/avro/alltypes_plain.avro]]}, limit=None\
            \n",
        ],
    ];
    assert_eq!(expected, actual);
}
