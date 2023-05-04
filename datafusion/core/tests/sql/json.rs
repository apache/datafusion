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
    let path = format!("{TEST_DATA_BASE}/2.json");
    ctx.register_json("t1", &path, NdJsonReadOptions::default())
        .await
        .unwrap();

    let sql = "SELECT a, b FROM t1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+------+",
        "| a               | b    |",
        "+-----------------+------+",
        "| 1               | 2.0  |",
        "| -10             | -3.5 |",
        "| 2               | 0.6  |",
        "| 1               | 2.0  |",
        "| 7               | -3.5 |",
        "| 1               | 0.6  |",
        "| 1               | 2.0  |",
        "| 5               | -3.5 |",
        "| 1               | 0.6  |",
        "| 1               | 2.0  |",
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
    let path = format!("{TEST_DATA_BASE}/3.json");
    ctx.register_json("single_nan", &path, NdJsonReadOptions::default())
        .await
        .unwrap();
    let sql = "SELECT mycol FROM single_nan";
    let dataframe = ctx.sql(sql).await.unwrap();
    let results = dataframe.collect().await.unwrap();
    for batch in results {
        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.num_columns());
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn json_explain() {
    let ctx = SessionContext::new();
    let path = format!("{TEST_DATA_BASE}/2.json");
    ctx.register_json("t1", &path, NdJsonReadOptions::default())
        .await
        .unwrap();

    let sql = "EXPLAIN SELECT count(*) from t1";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let expected = vec![
        vec![
            "logical_plan",
            "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1))]]\
            \n  TableScan: t1 projection=[a]",
        ],
        vec![
            "physical_plan",
            "AggregateExec: mode=Final, gby=[], aggr=[COUNT(UInt8(1))]\
            \n  CoalescePartitionsExec\
            \n    AggregateExec: mode=Partial, gby=[], aggr=[COUNT(UInt8(1))]\
            \n      RepartitionExec: partitioning=RoundRobinBatch(NUM_CORES), input_partitions=1\
            \n        JsonExec: file_groups={1 group: [[WORKING_DIR/tests/jsons/2.json]]}, projection=[a]\n",
        ],
    ];
    assert_eq!(expected, actual);
}
