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

//! Metrics coverage for PartitionedTopKExec.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::sorts::partitioned_topk::PartitionedTopKExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::*;

/// The `output_batches` metric of `PartitionedTopKExec` should equal the
/// number of batches the operator emits to its consumer.
///
/// Regression test for <https://github.com/apache/datafusion/issues/24470>.
#[tokio::test]
async fn partitioned_topk_output_batches_metric_matches_emitted_batches() -> Result<()> {
    // Top-1 per partition over 50 partition keys, at batch_size 10, so the 50
    // result rows are emitted as several coalesced batches.
    let mut config = SessionConfig::new()
        .with_batch_size(10)
        .with_target_partitions(1);
    config.options_mut().optimizer.enable_window_topn = true;
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE t AS SELECT value % 50 AS pk, value AS val FROM range(0, 150)")
        .await?
        .collect()
        .await?;
    let df = ctx
        .sql(
            "SELECT * FROM ( \
                 SELECT pk, val, row_number() OVER (PARTITION BY pk ORDER BY val) AS rn \
                 FROM t \
             ) WHERE rn <= 1",
        )
        .await?;
    let plan = df.create_physical_plan().await?;

    // `PartitionedTopKExec` sits below the window operator, so execute it
    // directly to observe the batches it emits.
    let topk =
        find_partitioned_topk(&plan).expect("plan should contain PartitionedTopKExec");
    let batches = collect(Arc::clone(&topk), ctx.task_ctx()).await?;
    let emitted_sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();

    // The 50 result rows arrive as five coalesced batches of batch_size rows
    assert_eq!(emitted_sizes, vec![10, 10, 10, 10, 10]);

    // The operator should expose its metrics (e.g. for EXPLAIN ANALYZE) ...
    let metrics = topk
        .metrics()
        .expect("PartitionedTopKExec should expose metrics");
    // ... and its output_batches metric should match the emitted batches
    let output_batches = metrics
        .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
        .expect("output_batches metric should be present")
        .as_usize();
    assert_eq!(
        output_batches,
        emitted_sizes.len(),
        "output_batches metric disagrees with the number of emitted batches"
    );
    Ok(())
}

fn find_partitioned_topk(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<PartitionedTopKExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_partitioned_topk)
}
