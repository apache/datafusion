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

//! [`WorkerDispatchExec`]: transparent wrapper that redirects
//! `execute(partition, ..)` onto a [`WorkerPool`] worker.
//!
//! # Why
//!
//! A [`RepartitionExec`](datafusion_physical_plan::repartition::RepartitionExec)
//! inside a pipeline spawns its input-fetcher tasks with `tokio::spawn`
//! the first time **any** output partition is polled. On a pool of
//! thread-per-core runtimes that means all input fetchers land on
//! whichever worker won the race, serializing scans and decodes on
//! that one runtime.
//!
//! Wrapping the scan-heavy leaves of a pipeline with
//! [`WorkerDispatchExec`] breaks the tie: each leaf partition runs on a
//! specific worker (typically `partition % worker_count`), distributing
//! scan + decode across the pool. Downstream polls see a normal
//! `SendableRecordBatchStream` whose batches were produced on a
//! different worker and ferried across via a bounded channel.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::runtime::WorkerPool;

/// How dispatch assigns partitions to workers.
#[derive(Debug, Clone, Copy)]
pub enum PartitionAssignment {
    /// Partition `p` runs on worker `p % worker_count`.
    Modulo,
    /// Round-robin, tracked by the pool's own counter.
    RoundRobin,
}

/// Bounded channel capacity (in batches) used to bridge the worker's
/// stream back to the caller.
const DISPATCH_CHANNEL_CAPACITY: usize = 2;

/// Transparent wrapper that dispatches `execute` onto a [`WorkerPool`]
/// worker. See the module docs for the motivation.
pub struct WorkerDispatchExec {
    inner: Arc<dyn ExecutionPlan>,
    pool: Arc<WorkerPool>,
    assignment: PartitionAssignment,
}

impl WorkerDispatchExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, pool: Arc<WorkerPool>) -> Self {
        Self {
            inner,
            pool,
            assignment: PartitionAssignment::Modulo,
        }
    }

    pub fn with_assignment(mut self, assignment: PartitionAssignment) -> Self {
        self.assignment = assignment;
        self
    }

    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl fmt::Debug for WorkerDispatchExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerDispatchExec")
            .field("inner", &self.inner)
            .field("assignment", &self.assignment)
            .finish()
    }
}

impl DisplayAs for WorkerDispatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "WorkerDispatchExec")
    }
}

impl ExecutionPlan for WorkerDispatchExec {
    fn name(&self) -> &str {
        "WorkerDispatchExec"
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
                "WorkerDispatchExec expects one child, got {}",
                children.len()
            )));
        }
        let child = children.pop().expect("len checked above");
        Ok(Arc::new(
            WorkerDispatchExec::new(child, Arc::clone(&self.pool))
                .with_assignment(self.assignment),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = Arc::clone(&self.inner);
        let pool = Arc::clone(&self.pool);
        let schema = self.inner.schema();
        let worker_id = match self.assignment {
            PartitionAssignment::Modulo => partition,
            PartitionAssignment::RoundRobin => pool.next_worker(),
        };

        let mut builder =
            RecordBatchReceiverStreamBuilder::new(schema, DISPATCH_CHANNEL_CAPACITY);
        let tx = builder.tx();

        builder.spawn(async move {
            let job = pool.spawn_on(worker_id, move || async move {
                let mut stream = match inner.execute(partition, context) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return Ok::<(), DataFusionError>(());
                    }
                };
                while let Some(batch) = stream.next().await {
                    if tx.send(batch).await.is_err() {
                        // Consumer dropped; nothing more to do.
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
/// [`WorkerDispatchExec`]. This is the simplest policy and matches the
/// tokio-uring demo's leaf-wrapping trick: it distributes per-partition
/// scans across the pool.
pub fn wrap_leaves(
    plan: Arc<dyn ExecutionPlan>,
    pool: &Arc<WorkerPool>,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion_common::tree_node::{Transformed, TreeNode};
    plan.transform_up(|node| {
        if node.children().is_empty() {
            Ok(Transformed::yes(
                Arc::new(WorkerDispatchExec::new(node, Arc::clone(pool)))
                    as Arc<dyn ExecutionPlan>,
            ))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .map(|t| t.data)
}
