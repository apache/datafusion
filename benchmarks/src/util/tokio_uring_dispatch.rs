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

//! [`PoolDispatchExec`]: wraps an [`ExecutionPlan`] so that each
//! `execute(partition, ...)` call runs on a [`TokioUringPool`] worker,
//! regardless of which worker the caller is on.
//!
//! # Why
//!
//! `RepartitionExec` spawns its `M` input-fetcher tasks with
//! `tokio::spawn` on the *current* runtime the first time any output
//! partition is polled. On a pool of thread-per-core `tokio-uring`
//! reactors that means all `M` fetchers pile onto whichever worker
//! won the race, and the reads and decodes they drive serialize on
//! that one ring.
//!
//! Wrapping the scan-heavy child (typically the `DataSourceExec` at
//! the bottom of the plan) with [`PoolDispatchExec`] fixes this: every
//! input partition is `pool.spawn`-ed across the pool, so `io_uring`
//! submissions and Parquet decode run on different workers in
//! parallel. The wrapper is transparent w.r.t. schema, partitioning,
//! ordering, and statistics — it only redirects where `execute` runs.

#![cfg(target_os = "linux")]

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use super::tokio_uring_pool::TokioUringPool;

/// Transparent wrapper that dispatches `execute` onto a
/// [`TokioUringPool`] worker.
pub struct PoolDispatchExec {
    inner: Arc<dyn ExecutionPlan>,
    pool: Arc<TokioUringPool>,
}

impl fmt::Debug for PoolDispatchExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolDispatchExec")
            .field("inner", &self.inner)
            .finish()
    }
}

impl PoolDispatchExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, pool: Arc<TokioUringPool>) -> Self {
        Self { inner, pool }
    }
}

impl DisplayAs for PoolDispatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "PoolDispatchExec")
    }
}

impl ExecutionPlan for PoolDispatchExec {
    fn name(&self) -> &str {
        "PoolDispatchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "PoolDispatchExec expects exactly one child, got {}",
                children.len()
            )));
        }
        let child = children.pop().expect("len checked above");
        Ok(Arc::new(PoolDispatchExec::new(
            child,
            Arc::clone(&self.pool),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = Arc::clone(&self.inner);
        let pool = Arc::clone(&self.pool);
        let schema = self.inner.schema();

        // Bridge the inner stream back to the caller via a bounded
        // channel. Batches produced on the pool worker are delivered
        // to whoever is polling the outer stream (typically a
        // RepartitionExec fetcher task on a different worker).
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 2);
        let tx = builder.tx();

        builder.spawn(async move {
            let job = pool.spawn(move || async move {
                let mut stream = match inner.execute(partition, context) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return Ok::<(), DataFusionError>(());
                    }
                };
                while let Some(batch) = stream.next().await {
                    if tx.send(batch).await.is_err() {
                        // Consumer dropped; abandon.
                        break;
                    }
                }
                Ok(())
            });
            job.await??;
            Ok(())
        });

        Ok(builder.build())
    }
}

/// Walk `plan` bottom-up and wrap every leaf node (no children) with a
/// [`PoolDispatchExec`]. Leaves are typically data sources (Parquet,
/// CSV, etc.) — wrapping them distributes all per-partition scans
/// across the pool, bypassing `RepartitionExec`'s current-runtime
/// lazy-spawn bottleneck.
pub fn wrap_leaves_with_pool_dispatch(
    plan: Arc<dyn ExecutionPlan>,
    pool: &Arc<TokioUringPool>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        if node.children().is_empty() {
            Ok(Transformed::yes(
                Arc::new(PoolDispatchExec::new(node, Arc::clone(pool)))
                    as Arc<dyn ExecutionPlan>,
            ))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}
