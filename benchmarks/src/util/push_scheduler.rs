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

//! Benchmark helpers for driving queries through
//! [`datafusion_push_scheduler::Scheduler`].

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_push_scheduler::Scheduler;
use futures::StreamExt;

/// Compile a SQL query via `ctx`, then execute the resulting physical plan
/// through [`Scheduler`] and collect all output batches. The worker count
/// is taken from the session's `target_partitions`, matching how the
/// default path fans out.
pub async fn collect_sql_via_push_scheduler(
    ctx: &SessionContext,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    let workers = ctx.state().config().target_partitions().max(1);
    let scheduler = Scheduler::new(workers)?;
    let mut stream = scheduler.schedule(plan, ctx.task_ctx())?.stream();

    let mut out = Vec::new();
    while let Some(batch) = stream.next().await {
        out.push(batch?);
    }
    Ok(out)
}
