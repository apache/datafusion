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

//! `count(distinct)` over integers under a memory limit.
//!
//! The integer distinct-count groups accumulator reports the capacity of its
//! buffers in `size()`. After an aggregate stream emits all groups, either to
//! emit partial state early or to spill, it resizes its reservation to the
//! table's reported size and expects it to have shrunk. If the accumulator
//! keeps its capacity, that resize is a grow against an exhausted pool and the
//! query fails although everything was already written out.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::assert_batches_sorted_eq;
use datafusion_execution::disk_manager::DiskManagerBuilder;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

const ROWS: usize = 200_000;
const GROUPS: i64 = 64;
const BATCH_ROWS: usize = 8_192;

/// Far below the distinct sets (200k values, several megabytes across the
/// partial and final tables), far above the fixed cost of the stages. With the
/// accumulator releasing its buffers the query passes from 2 MB upwards;
/// without, it fails up to 4 MB with "Decreasing allocation after spilling
/// should succeed" in the final stage or a failed emit in the partial stage.
const MEMORY_LIMIT: usize = 4 * 1024 * 1024;

/// `g` has 64 groups, `v` is unique, so every group holds 3125 distinct values.
fn table() -> MemTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let batches = (0..ROWS)
        .step_by(BATCH_ROWS)
        .map(|start| {
            let rows = start..(start + BATCH_ROWS).min(ROWS);
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        rows.clone().map(|row| row as i64 % GROUPS),
                    )),
                    Arc::new(Int64Array::from_iter_values(rows.map(|row| row as i64))),
                ],
            )
            .unwrap()
        })
        .collect();
    MemTable::try_new(schema, vec![batches]).unwrap()
}

/// Four partial stages emit their state early and four hash-partitioned final
/// stages spill; every one of them must see the accumulator memory drop after
/// emitting all groups.
#[tokio::test]
async fn count_distinct_releases_memory_after_emitting_all() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(MEMORY_LIMIT, 1.0)
        .with_disk_manager_builder(DiskManagerBuilder::default())
        .build_arc()
        .unwrap();
    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table())).unwrap();

    let batches = ctx
        .sql("select count(distinct v) as d, count(*) as n from t group by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap_or_else(|error| panic!("query failed under the memory limit: {error}"));

    let per_group = (ROWS as i64 / GROUPS).to_string();
    let row = format!("| {per_group} | {per_group} |");
    let mut expected = vec!["+------+------+", "| d    | n    |", "+------+------+"];
    expected.extend(std::iter::repeat_n(row.as_str(), GROUPS as usize));
    expected.push("+------+------+");
    assert_batches_sorted_eq!(expected, &batches);
}
