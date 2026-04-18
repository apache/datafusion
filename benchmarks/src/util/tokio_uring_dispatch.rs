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
use datafusion::physical_plan::repartition::RepartitionExec;
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

/// Walk `plan` bottom-up and wrap the **child** of every
/// [`RepartitionExec`] with a [`PoolDispatchExec`].
///
/// `RepartitionExec` lazily spawns one input-fetcher task per input
/// partition on the *current* runtime the first time any output
/// partition is polled. On a thread-per-core pool that means all `N`
/// fetchers land on whichever worker won the race — and every input
/// subtree (partial aggregation, filter, scan) runs serially on that
/// one core. Wrapping each fetcher's root with `PoolDispatchExec`
/// redirects the actual work to round-robin pool workers, so the `N`
/// input pipelines run in parallel.
///
/// This also covers leaf I/O: the scan sits *inside* each
/// dispatched subtree, so its decode runs on the same pool worker as
/// its partial aggregation (one cross-thread hop per batch into the
/// repartition channel, instead of two).
pub fn wrap_repartition_inputs_with_pool_dispatch(
    plan: Arc<dyn ExecutionPlan>,
    pool: &Arc<TokioUringPool>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        if node.downcast_ref::<RepartitionExec>().is_none() {
            return Ok(Transformed::no(node));
        }
        let children = node.children();
        if children.len() != 1 {
            return Ok(Transformed::no(node));
        }
        let child: Arc<dyn ExecutionPlan> = Arc::clone(children[0]);
        if child.downcast_ref::<PoolDispatchExec>().is_some() {
            return Ok(Transformed::no(node));
        }
        let wrapped: Arc<dyn ExecutionPlan> =
            Arc::new(PoolDispatchExec::new(child, Arc::clone(pool)));
        let new_node = node.with_new_children(vec![wrapped])?;
        Ok(Transformed::yes(new_node))
    })
    .map(|t| t.data)
}
