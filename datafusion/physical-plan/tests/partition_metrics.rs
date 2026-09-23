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

#[path = "metrics/plan.rs"]
mod plan;

use std::sync::Arc;

use arrow::array::Int32Array;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::{FutureExt, StreamExt, TryStreamExt};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_tree_partition_metrics() -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(30), run_shared_tree())
        .await
        .expect("shared tree did not complete")
}

async fn run_shared_tree() -> Result<()> {
    let (nodes, gate) = plan::shared_plan(16)?;
    let context = Arc::new(TaskContext::default());
    for node in &nodes {
        assert_eq!(node.metrics().unwrap().for_partition(0).iter().count(), 0);
    }
    let mut long_stream = nodes[0].execute(0, Arc::clone(&context))?;
    let first = long_stream.next().await.unwrap()?;
    assert_eq!(first.num_rows(), 16);
    assert!(long_stream.next().now_or_never().is_none());
    let early = nodes[0].metrics().unwrap().for_partition(0);
    assert_eq!(early.output_rows(), Some(16));

    // Concurrent execution and registration on the same Arc<dyn ExecutionPlan>.
    let mut tasks = tokio::task::JoinSet::new();
    for partition in 1..16 {
        let root = Arc::clone(&nodes[0]);
        let context = Arc::clone(&context);
        tasks.spawn(async move {
            let batches: Vec<_> = root.execute(partition, context)?.try_collect().await?;
            for batch in &batches {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(values.values().as_ref(), &(1..17).collect::<Vec<_>>());
            }
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 32);
            Ok::<_, datafusion_common::DataFusionError>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap()?;
    }
    // Other completed partitions do not affect the still-running partition.
    assert_eq!(early.output_rows(), Some(16));
    assert_eq!(
        nodes[0].metrics().unwrap().for_partition(0).output_rows(),
        Some(16)
    );
    gate.notify_one();
    let remaining: Vec<_> = long_stream.try_collect().await?;
    assert_eq!(remaining.iter().map(|b| b.num_rows()).sum::<usize>(), 16);
    assert_eq!(early.output_rows(), Some(32));

    for (node_index, node) in nodes.iter().enumerate() {
        let rows = if node_index == 2 { 64 } else { 32 };
        let full = node.metrics().unwrap();
        assert_eq!(full.output_rows(), Some(rows * 16));
        for partition in 0..16 {
            let selected = node.metrics().unwrap().for_partition(partition);
            assert_eq!(selected.output_rows(), Some(rows));
            assert_eq!(selected.aggregate_by_name().output_rows(), Some(rows));
            let expected: Vec<_> = full
                .iter()
                .filter(|m| m.partition() == Some(partition))
                .collect();
            assert_eq!(selected.iter().count(), expected.len());
            for (actual, expected) in selected.iter().zip(expected) {
                assert!(Arc::ptr_eq(actual, expected));
            }
        }
        assert_eq!(node.metrics().unwrap().for_partition(16).iter().count(), 0);
    }
    Ok(())
}
