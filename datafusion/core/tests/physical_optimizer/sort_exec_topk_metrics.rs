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

//! Metrics coverage for SortExec in TopK mode.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::*;

/// The `output_batches` metric of a TopK sort should equal the number of
/// batches the operator emits to its consumer.
///
/// Regression test for <https://github.com/apache/datafusion/issues/24468>.
#[tokio::test]
async fn topk_output_batches_metric_matches_emitted_batches() -> Result<()> {
    // A top-25 over 100 unsorted rows, at batch_size 10, so the TopK result
    // must be emitted as multiple batches.
    let config = SessionConfig::new()
        .with_batch_size(10)
        .with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.sql("CREATE TABLE t AS SELECT value FROM range(0, 100)")
        .await?
        .collect()
        .await?;
    let df = ctx
        .sql("SELECT value FROM t ORDER BY value DESC LIMIT 25")
        .await?;
    let plan = df.create_physical_plan().await?;

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let emitted_sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();

    let sort = find_sort(&plan).expect("plan should contain SortExec");
    let metrics = sort.metrics().expect("SortExec should have metrics");
    let output_batches = metrics
        .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
        .expect("output_batches metric should be present")
        .as_usize();

    // The 25 result rows arrive as three batches of at most batch_size rows
    assert_eq!(emitted_sizes, vec![10, 10, 5]);
    // ... so the metric should report three output batches
    assert_eq!(
        output_batches,
        emitted_sizes.len(),
        "output_batches metric disagrees with the number of emitted batches"
    );
    Ok(())
}

fn find_sort(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.downcast_ref::<SortExec>().is_some() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_sort)
}
