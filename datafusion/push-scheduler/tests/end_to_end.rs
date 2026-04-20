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

//! End-to-end tests driving SQL through `datafusion_push_scheduler::Scheduler`
//! and comparing results against the default pull-based path.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionConfig;
use datafusion_common::Result;
use datafusion_push_scheduler::{PipelinePlanner, Scheduler};
use futures::StreamExt;

/// Build a small test dataset with multiple partitions so repartition /
/// sort paths get exercised.
fn sample_context() -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("grp", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ]));

    let make_batch = |ids: &[i32], grps: &[&str], vs: &[i32]| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(grps.to_vec())),
                Arc::new(Int32Array::from(vs.to_vec())),
            ],
        )
        .unwrap()
    };

    let b1 = make_batch(&[1, 3, 5, 7], &["a", "b", "a", "b"], &[10, 20, 30, 40]);
    let b2 = make_batch(&[2, 4, 6, 8], &["a", "b", "a", "b"], &[11, 21, 31, 41]);
    let b3 = make_batch(&[9, 11, 13, 15], &["a", "b", "a", "b"], &[12, 22, 32, 42]);

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .with_batch_size(2);
    let ctx = SessionContext::new_with_config(config);

    // Register the table with 2 pre-existing partitions so the pipeline
    // sees >1 input partition at the source.
    let table = MemTable::try_new(Arc::clone(&schema), vec![vec![b1, b2], vec![b3]])?;
    ctx.register_table("t", Arc::new(table))?;
    Ok(ctx)
}

async fn collect_default(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    collect(plan, ctx.task_ctx()).await
}

async fn collect_scheduler(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    let scheduler = Scheduler::new(num_cpus())?;
    let mut stream = scheduler.schedule(plan, ctx.task_ctx())?.stream();

    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn normalise(batches: &[RecordBatch]) -> String {
    // Compare as sorted text to paper over partition / batch-boundary
    // differences between the two execution paths.
    let formatted = pretty_format_batches(batches).unwrap().to_string();
    let mut lines: Vec<&str> = formatted.lines().collect();
    lines.sort();
    lines.join("\n")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projection_and_filter_matches_default() -> Result<()> {
    let ctx = sample_context()?;
    let sql = "SELECT id, v FROM t WHERE id > 4";
    let a = collect_default(&ctx, sql).await?;
    let b = collect_scheduler(&ctx, sql).await?;
    assert_eq!(normalise(&a), normalise(&b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repartition_hash_matches_default() -> Result<()> {
    let ctx = sample_context()?;
    // A GROUP BY forces a RepartitionExec in the plan. We wrap the
    // aggregate with an ExecutionPipeline, so the repartition is the
    // interesting push-based breaker exercised here.
    let sql = "SELECT grp, COUNT(*) AS c, SUM(v) AS s FROM t GROUP BY grp";
    let a = collect_default(&ctx, sql).await?;
    let b = collect_scheduler(&ctx, sql).await?;
    assert_eq!(normalise(&a), normalise(&b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sort_matches_default() -> Result<()> {
    let ctx = sample_context()?;
    let sql = "SELECT id, v FROM t ORDER BY v DESC, id ASC";
    let a = collect_default(&ctx, sql).await?;
    let b = collect_scheduler(&ctx, sql).await?;
    assert_eq!(normalise(&a), normalise(&b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn limit_with_sort_matches_default() -> Result<()> {
    let ctx = sample_context()?;
    let sql = "SELECT id, v FROM t ORDER BY v DESC LIMIT 3";
    let a = collect_default(&ctx, sql).await?;
    let b = collect_scheduler(&ctx, sql).await?;
    assert_eq!(normalise(&a), normalise(&b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_with_where_and_order_by_matches_default() -> Result<()> {
    let ctx = sample_context()?;
    let sql = "\
        SELECT grp, SUM(v) AS s FROM t \
        WHERE id <= 10 \
        GROUP BY grp \
        ORDER BY s DESC";
    let a = collect_default(&ctx, sql).await?;
    let b = collect_scheduler(&ctx, sql).await?;
    assert_eq!(normalise(&a), normalise(&b));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn planner_cuts_at_repartition_for_group_by() -> Result<()> {
    // Confirm that the planner actually produces multiple pipelines for
    // a query with a RepartitionExec — i.e. that Inbox rewiring is on.
    let ctx = sample_context()?;
    let sql = "SELECT grp, COUNT(*) FROM t GROUP BY grp";
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    let pipeline_plan = PipelinePlanner::new(plan, ctx.task_ctx()).build()?;
    assert!(
        pipeline_plan.pipelines.len() >= 2,
        "expected the group-by plan to be split into >=2 pipelines \
         (breaker cut at RepartitionExec), got {}",
        pipeline_plan.pipelines.len(),
    );
    Ok(())
}
