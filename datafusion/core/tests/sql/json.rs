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

const TEST_DATA_BASE: &str = "tests/jsons";

#[tokio::test]
async fn json_query() {
    let ctx = SessionContext::new();
    let path = format!("{}/2.json", TEST_DATA_BASE);
    ctx.register_json("t1", &path, NdJsonReadOptions::default())
        .await
        .unwrap();

    let sql = "SELECT a, b FROM t1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+------+",
        "| a               | b    |",
        "+-----------------+------+",
        "| 1               | 2    |",
        "| -10             | -3.5 |",
        "| 2               | 0.6  |",
        "| 1               | 2    |",
        "| 7               | -3.5 |",
        "| 1               | 0.6  |",
        "| 1               | 2    |",
        "| 5               | -3.5 |",
        "| 1               | 0.6  |",
        "| 1               | 2    |",
        "| 1               | -3.5 |",
        "| 100000000000000 | 0.6  |",
        "+-----------------+------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
#[should_panic]
async fn json_single_nan_schema() {
    let ctx = SessionContext::new();
    let path = format!("{}/3.json", TEST_DATA_BASE);
    ctx.register_json("single_nan", &path, NdJsonReadOptions::default())
        .await
        .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let plan = ctx.create_physical_plan(&plan).await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn json_explain() {
    let ctx = SessionContext::new();
    let path = format!("{}/2.json", TEST_DATA_BASE);
    ctx.register_json("t1", &path, NdJsonReadOptions::default())
        .await
        .unwrap();

    let sql = "EXPLAIN SELECT count(*) from t1";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: #COUNT(UInt8(1))\
            \n  Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n    TableScan: t1 projection=[a]",
        ],
        vec![
            "physical_plan",
            "ProjectionExec: expr=[COUNT(UInt8(1))@0 as COUNT(UInt8(1))]\
            \n  AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]\
            \n    CoalescePartitionsExec\
            \n      AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]\
            \n        RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES)\
            \n          JsonExec: limit=None, files=[WORKING_DIR/tests/jsons/2.json]\n",
        ],
    ];
    assert_eq!(expected, actual);
}
