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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use datafusion::{
    assert_batches_sorted_eq,
    prelude::{SessionConfig, SessionContext},
};
use datafusion_catalog::MemTable;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_plan::repartition::RepartitionExec;
use futures::TryStreamExt;
use itertools::Itertools;

/// End to end test for spilling in RepartitionExec.
/// The idea is to make a real world query with a relatively low memory limit and
/// then drive one partition at a time, simulating dissimlar execution speed in partitions.
/// Just as some examples of real world scenarios where this can happen consider
/// lopsided groups in a group by especially if one partitions spills and others dont',
/// or in distributed systems if one upstream node is slower than others.
#[tokio::test]
async fn test_repartition_memory_limit() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(1024 * 1024, 1.0)
        .build()
        .unwrap();
    let config = SessionConfig::new()
        .with_batch_size(32)
        .with_target_partitions(2);
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    let batches = vec![RecordBatch::try_from_iter(vec![(
        "c1",
        Arc::new(Int32Array::from_iter_values(
            (0..10).cycle().take(1_000_000),
        )) as ArrayRef,
    )])
    .unwrap()];
    let table = Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap());
    ctx.register_table("t", table).unwrap();
    let plan = ctx
        .state()
        .create_logical_plan("SELECT c1, count(*) as c FROM t GROUP BY c1;")
        .await
        .unwrap();
    let plan = ctx.state().create_physical_plan(&plan).await.unwrap();
    // Execute partition 0, this should cause items going into the rest of the partitions to queue up and because
    // of the low memory limit should spill to disk.
    let batches0 = Arc::clone(&plan)
        .execute(0, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let mut metrics = None;
    Arc::clone(&plan)
        .transform_down(|node| {
            if node.as_any().is::<RepartitionExec>() {
                metrics = node.metrics();
            }
            Ok(Transformed::no(node))
        })
        .unwrap();

    let metrics = metrics.unwrap();
    assert!(metrics.spilled_bytes().unwrap() > 0);
    assert!(metrics.spilled_rows().unwrap() > 0);
    assert!(metrics.spill_count().unwrap() > 0);

    // Execute the other partition
    let batches1 = Arc::clone(&plan)
        .execute(1, ctx.task_ctx())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let all_batches = batches0
        .into_iter()
        .chain(batches1.into_iter())
        .collect_vec();
    #[rustfmt::skip]
    let expected = &[
    "+----+--------+",
    "| c1 | c      |",
    "+----+--------+",
    "| 0  | 100000 |",
    "| 1  | 100000 |",
    "| 2  | 100000 |",
    "| 3  | 100000 |",
    "| 4  | 100000 |",
    "| 5  | 100000 |",
    "| 6  | 100000 |",
    "| 7  | 100000 |",
    "| 8  | 100000 |",
    "| 9  | 100000 |",
    "+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &all_batches);
}
