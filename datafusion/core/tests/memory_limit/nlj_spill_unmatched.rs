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

//! Memory-limited `NestedLoopJoinExec` must not drop unmatched rows.
//!
//! When the join spills, per-right-batch match bitmaps are accumulated across
//! left chunks and their emission is deferred until every chunk has been
//! probed. The load that follows the final chunk finds no left batches left,
//! and that exit used to end the stream immediately — discarding the
//! accumulated bitmaps, so rows that no chunk ever matched were never emitted.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

/// Deterministic table whose join keys repeat, so most rows match and only a
/// few remain unmatched.
fn table(rows: usize, value_column: &str, seed: u64) -> RecordBatch {
    let mut state = seed;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new(value_column, DataType::Int32, true),
    ]));
    let keys: Vec<Option<i32>> = (0..rows).map(|_| Some((next() % 10) as i32)).collect();
    let values: Vec<Option<i32>> =
        (0..rows).map(|_| Some((next() % 60) as i32)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

/// Run `sql`, optionally under a memory limit, returning the sorted rows.
async fn run(sql: &str, memory_limit: Option<usize>) -> Vec<String> {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(16);
    let runtime = match memory_limit {
        Some(bytes) => RuntimeEnvBuilder::new()
            .with_memory_limit(bytes, 1.0)
            .build_arc()
            .unwrap(),
        None => RuntimeEnvBuilder::new().build_arc().unwrap(),
    };
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_batch("l", table(200, "v", 11)).unwrap();
    ctx.register_batch("r", table(90, "w", 16)).unwrap();

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = vec![];
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|col| {
                    datafusion_common::ScalarValue::try_from_array(batch.column(col), row)
                        .unwrap()
                        .to_string()
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// A non-equi `LEFT JOIN` is planned as a `NestedLoopJoinExec`; the optimizer
/// swaps the inputs, so the rows needing unmatched emission end up on the
/// join's probe side. Spilling must not lose them.
#[tokio::test]
async fn left_join_keeps_unmatched_rows_when_spilling() {
    let sql = "SELECT l.k, r.w FROM l LEFT JOIN r ON l.v > r.w";

    let ample = run(sql, None).await;
    let limited = run(sql, Some(64)).await;

    assert_eq!(
        limited,
        ample,
        "memory-limited execution returned {} rows instead of {}",
        limited.len(),
        ample.len()
    );
}

/// `LEFT ANTI` returns only unmatched rows, so losing them empties the result.
#[tokio::test]
async fn left_anti_join_keeps_unmatched_rows_when_spilling() {
    let sql = "SELECT l.k, l.v FROM l \
               WHERE NOT EXISTS (SELECT 1 FROM r WHERE l.v > r.w)";

    let ample = run(sql, None).await;
    let limited = run(sql, Some(64)).await;

    assert!(!ample.is_empty(), "expected the query to match some rows");
    assert_eq!(
        limited,
        ample,
        "memory-limited execution returned {} rows instead of {}",
        limited.len(),
        ample.len()
    );
}

/// `FULL JOIN` needs unmatched emission on both sides.
#[tokio::test]
async fn full_join_keeps_unmatched_rows_when_spilling() {
    let sql = "SELECT l.k, r.w FROM l FULL JOIN r ON l.v > r.w";

    let ample = run(sql, None).await;
    let limited = run(sql, Some(64)).await;

    assert_eq!(
        limited,
        ample,
        "memory-limited execution returned {} rows instead of {}",
        limited.len(),
        ample.len()
    );
}
