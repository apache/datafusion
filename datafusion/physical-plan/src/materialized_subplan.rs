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

//! Physical plan nodes for materialized subplans.
//!
//! A *materialized subplan* is computed once, fully buffered, and replayed to
//! every reference instead of being recomputed per reference. The first user is
//! multiply-referenced CTEs, but the operators are CTE-agnostic (keyed by a
//! [`SubplanId`], not a name) so a future `CommonSubplanEliminate` rule can reuse
//! them for any duplicated subplan (epic
//! <https://github.com/apache/datafusion/issues/22676>).
//!
//! # Why fully materialize (and the trade-off)
//!
//! When one subplan feeds multiple references the plan becomes a *diamond*
//! (DAG, not a tree): the same output is consumed by several parents that may
//! pull at different rates. The canonical hazard (issue
//! <https://github.com/apache/datafusion/issues/8777>) is a self-join whose hash
//! build side is drained completely before the probe side is read at all — a
//! single lazily-pulled shared stream would then deadlock or require unbounded
//! buffering. [`MaterializedSubplanExec`] sidesteps this by **fully buffering the
//! subplan before any reader runs** (a *pipeline breaker*), the same approach
//! PostgreSQL takes with a `tuplestore`/`CteScan` and DuckDB with a
//! `ColumnDataCollection`/`PhysicalCTE`.
//!
//! The trade-off: there is no pipelining and the entire intermediate result is
//! held in memory. Memory is *accounted* against the [`MemoryPool`] (so the
//! operator fails gracefully under a configured limit), but it is not yet
//! *spilled* to disk — spilling and true streaming reuse are tracked as
//! follow-up work.
//!
//! [`MemoryPool`]: datafusion_execution::memory_pool::MemoryPool
//! [`SubplanId`]: datafusion_expr::logical_plan::SubplanId

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::coop::cooperative;
use crate::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, collect_partitioned,
};
use crate::joins::utils::{OnceAsync, OnceFut};
use crate::memory::MemoryStream;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::operator_statistics::StatisticsRegistry;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::logical_plan::SubplanId;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::TryStreamExt;

/// The fully materialized result of a subplan, partitioned exactly as the
/// subplan produced it, together with the [`MemoryReservation`] accounting for
/// its size. The reservation is released when the last reference (the producer
/// and all readers sharing the `Arc`) is dropped.
#[derive(Debug)]
pub struct MaterializedBatches {
    /// One inner `Vec` per subplan output partition.
    pub partitions: Vec<Vec<RecordBatch>>,
    /// Memory accounted for `partitions` against the pool. Held alive for the
    /// lifetime of the cached result; freed on drop.
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

/// A shared cache that stores the materialized subplan results.
/// The cache uses [`OnceAsync`] to ensure the subplan is only computed once,
/// while allowing multiple consumers to await the result concurrently.
#[derive(Debug)]
pub struct MaterializedSubplanCache {
    /// Id of the subplan (for debugging).
    #[expect(dead_code)]
    id: SubplanId,
    /// The shared one-time async computation of the subplan batches.
    once: OnceAsync<MaterializedBatches>,
}

impl MaterializedSubplanCache {
    /// Create a new empty cache for the given subplan id.
    pub fn new(id: SubplanId) -> Self {
        Self {
            id,
            once: OnceAsync::default(),
        }
    }

    /// Get or initialize the cached batches via [`OnceAsync::try_once`].
    /// The first caller triggers computation; subsequent callers share the result.
    pub(crate) fn try_once<F, Fut>(&self, f: F) -> Result<OnceFut<MaterializedBatches>>
    where
        F: FnOnce() -> Result<Fut>,
        Fut: Future<Output = Result<MaterializedBatches>> + Send + 'static,
    {
        self.once.try_once(f)
    }
}

/// Physical execution plan that materializes a subplan and then executes a
/// continuation plan. The subplan results are cached in a shared
/// [`MaterializedSubplanCache`] for use by [`MaterializedSubplanReaderExec`] nodes
/// with the same [`SubplanId`].
///
/// Children are `[plan (child 0), continuation (child 1)]`. The node's output is
/// the continuation's output; the materialized `plan` is consumed only to fill
/// the cache.
#[derive(Debug)]
pub struct MaterializedSubplanExec {
    /// Id binding this producer to its readers and keying the cache.
    id: SubplanId,
    /// Human-readable name (display/debug only).
    name: String,
    /// The plan that computes the subplan.
    plan: Arc<dyn ExecutionPlan>,
    /// The continuation plan that uses the materialized subplan.
    continuation: Arc<dyn ExecutionPlan>,
    /// Shared cache for the subplan results.
    cache: Arc<MaterializedSubplanCache>,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties (copied from the continuation).
    properties: Arc<PlanProperties>,
}

impl MaterializedSubplanExec {
    /// Create a new `MaterializedSubplanExec`.
    pub fn new(
        id: SubplanId,
        name: String,
        plan: Arc<dyn ExecutionPlan>,
        continuation: Arc<dyn ExecutionPlan>,
        cache: Arc<MaterializedSubplanCache>,
    ) -> Self {
        let properties = Arc::clone(continuation.properties());
        Self {
            id,
            name,
            plan,
            continuation,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }
}

impl DisplayAs for MaterializedSubplanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MaterializedSubplanExec: id={}, name={}",
                    self.id, self.name
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for MaterializedSubplanExec {
    fn name(&self) -> &'static str {
        "MaterializedSubplanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan, &self.continuation]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // The node's output is the continuation's output, so ordering flows from
        // child 1 (continuation) only. The materialized `plan` (child 0) is
        // replayed from a buffer and does not contribute to output ordering.
        vec![false, true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Neither child benefits from repartitioning *under this node*: the
        // continuation is a pass-through of its own output, and the subplan is
        // fully materialized before any read, so extra parallelism on it does
        // not change this node's behavior. (Whether the subplan body itself
        // benefits from partitioning is the producer subtree's own concern.)
        vec![false, false]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        // Output equals the continuation's output, unchanged by materialization.
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "MaterializedSubplanExec expected 2 children, got {}",
                children.len()
            );
        }
        let plan = Arc::clone(&children[0]);
        let partition_count = plan.output_partitioning().partition_count();
        let statistics = materialized_subplan_statistics(plan.as_ref())?;
        let continuation = replace_materialized_subplan_readers(
            Arc::clone(&children[1]),
            self.id,
            &self.cache,
            partition_count,
            &statistics,
        )?;
        Ok(Arc::new(Self::new(
            self.id,
            self.name.clone(),
            plan,
            continuation,
            Arc::clone(&self.cache),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_partitions = self.properties.output_partitioning().partition_count();
        if partition >= output_partitions {
            return internal_err!(
                "MaterializedSubplanExec got partition {partition}, expected less than {output_partitions}"
            );
        }

        let plan = Arc::clone(&self.plan);
        let continuation = Arc::clone(&self.continuation);
        let name = self.name.clone();
        let collect_ctx = Arc::clone(&context);
        let pool = Arc::clone(collect_ctx.memory_pool());
        let schema = Arc::clone(&self.continuation.schema());
        let baseline = BaselineMetrics::new(&self.metrics, partition);

        // Use OnceAsync to ensure the subplan is materialized exactly once,
        // even when multiple partitions call execute() concurrently.
        let mut once_fut = self.cache.try_once(move || {
            Ok(async move {
                // Time spent materializing the subplan (first caller only).
                let _timer = baseline.elapsed_compute().timer();
                let partitions = collect_partitioned(plan, collect_ctx).await?;

                // Account the buffered result against the memory pool so the
                // operator respects configured limits instead of OOMing the
                // process. (Spilling is future work; this is accounting only.)
                let total_bytes: usize = partitions
                    .iter()
                    .flatten()
                    .map(|b| b.get_array_memory_size())
                    .sum();
                let reservation =
                    MemoryConsumer::new(format!("MaterializedSubplan[{name}]"))
                        .register(&pool);
                reservation.try_grow(total_bytes)?;

                let num_partitions = partitions.len();
                let num_batches: usize = partitions.iter().map(Vec::len).sum();
                let num_rows: usize =
                    partitions.iter().flatten().map(|b| b.num_rows()).sum();
                log::debug!(
                    "Materializing subplan '{name}': {num_partitions} partitions, {num_batches} batches, {num_rows} rows, {total_bytes} bytes"
                );

                Ok(MaterializedBatches {
                    partitions,
                    reservation,
                })
            })
        })?;

        let exec_ctx = Arc::clone(&context);
        let fut = async move {
            // Wait for the subplan to be materialized.
            std::future::poll_fn(|cx| once_fut.get_shared(cx)).await?;
            // Now execute the continuation.
            continuation.execute(partition, exec_ctx)
        };

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        // The node's output is the continuation's output, so delegate rather
        // than discarding statistics with `new_unknown`.
        self.continuation.partition_statistics(partition)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let cache = Arc::new(MaterializedSubplanCache::new(self.id));
        let partition_count = self.plan.output_partitioning().partition_count();
        let statistics = materialized_subplan_statistics(self.plan.as_ref())?;
        let continuation = replace_materialized_subplan_readers(
            Arc::clone(&self.continuation),
            self.id,
            &cache,
            partition_count,
            &statistics,
        )?;
        Ok(Arc::new(Self::new(
            self.id,
            self.name.clone(),
            Arc::clone(&self.plan),
            continuation,
            cache,
        )))
    }
}

/// Physical execution plan that reads from a previously materialized subplan
/// cache. This is a leaf node that replays the cached batches via a
/// [`MemoryStream`].
///
/// Its output partitioning matches the materialized subplan 1:1: partition `i`
/// replays exactly the subplan's `i`th output partition. Downstream distribution
/// requirements (e.g. a hash-partitioned join, or a single-partition consumer)
/// are satisfied by the standard `EnforceDistribution` pass inserting a
/// repartition/coalesce above this node — this node never silently re-buckets
/// rows, so reported partitioning and replayed contents always agree.
#[derive(Debug)]
pub struct MaterializedSubplanReaderExec {
    /// Id of the producer whose cache this reader replays.
    id: SubplanId,
    /// Human-readable name (display/debug only).
    name: String,
    /// The schema of the subplan output.
    schema: SchemaRef,
    /// Shared cache to read from.
    cache: Arc<MaterializedSubplanCache>,
    /// Execution metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Statistics of the materialized subplan (the rows this reader replays).
    statistics: Arc<Statistics>,
    /// Cache holding plan properties.
    properties: Arc<PlanProperties>,
}

impl MaterializedSubplanReaderExec {
    /// Create a new `MaterializedSubplanReaderExec`.
    ///
    /// `partition_count` must equal the number of output partitions of the
    /// materialized subplan (i.e. `MaterializedBatches::partitions.len()`); the
    /// reader exposes that count 1:1.
    pub fn new(
        id: SubplanId,
        name: String,
        schema: SchemaRef,
        cache: Arc<MaterializedSubplanCache>,
        partition_count: usize,
        statistics: Arc<Statistics>,
    ) -> Self {
        let properties = Self::compute_properties(Arc::clone(&schema), partition_count);
        Self {
            id,
            name,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics,
            properties: Arc::new(properties),
        }
    }

    /// The id of the subplan this reader reads from.
    pub fn subplan_id(&self) -> SubplanId {
        self.id
    }

    fn compute_properties(schema: SchemaRef, partition_count: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for MaterializedSubplanReaderExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MaterializedSubplanReaderExec: id={}, name={}",
                    self.id, self.name
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for MaterializedSubplanReaderExec {
    fn name(&self) -> &'static str {
        "MaterializedSubplanReaderExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let output_partitions = self.properties.output_partitioning().partition_count();
        if partition >= output_partitions {
            return internal_err!(
                "MaterializedSubplanReaderExec got partition {partition}, expected less than {output_partitions}"
            );
        }

        let schema = Arc::clone(&self.schema);
        let id = self.id;
        let baseline = BaselineMetrics::new(&self.metrics, partition);

        // Get a OnceFut handle to the shared computation. The producer
        // (MaterializedSubplanExec) triggers the actual work; here we just await
        // the result, which is ready immediately if the producer already
        // finished. The error closure only fires if a reader wins the race to
        // initialize the cache, which a well-formed plan (producer above all of
        // its readers) prevents — it is a defensive guard, not an expected path.
        let mut once_fut =
            self.cache.try_once(move || -> Result<std::future::Ready<_>> {
                internal_err!(
                    "MaterializedSubplanReaderExec: cache for subplan id={id} was never initialized by the producer."
                )
            })?;

        let schema_for_stream = Arc::clone(&schema);
        let fut = async move {
            let cached = std::future::poll_fn(|cx| once_fut.get_shared(cx)).await?;

            // 1:1 partition contract: partition `i` replays the subplan's `i`th
            // output partition exactly. No re-bucketing or coalescing.
            let partition_batches = cached
                .partitions
                .get(partition)
                .cloned()
                .unwrap_or_default();

            let rows: usize = partition_batches.iter().map(|b| b.num_rows()).sum();
            baseline.record_output(rows);

            let stream = MemoryStream::try_new(partition_batches, schema, None)?;
            Ok::<_, datafusion_common::DataFusionError>(
                Box::pin(cooperative(stream)) as SendableRecordBatchStream
            )
        };

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&self.statistics))
    }
}

/// Estimate the statistics of a materialized subplan from the plan that produces it.
pub fn materialized_subplan_statistics(
    plan: &dyn ExecutionPlan,
) -> Result<Arc<Statistics>> {
    Ok(Arc::clone(
        StatisticsRegistry::default_with_builtin_providers()
            .compute(plan)?
            .base_arc(),
    ))
}

/// Replace readers for a materialized subplan with readers that use the provided
/// cache and expose the provided partition count and statistics. Readers are
/// matched to their producer by [`SubplanId`], never by name.
pub fn replace_materialized_subplan_readers(
    plan: Arc<dyn ExecutionPlan>,
    id: SubplanId,
    cache: &Arc<MaterializedSubplanCache>,
    partition_count: usize,
    statistics: &Arc<Statistics>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(reader) = plan.downcast_ref::<MaterializedSubplanReaderExec>() else {
            return Ok(Transformed::no(plan));
        };

        if reader.subplan_id() != id {
            return Ok(Transformed::no(plan));
        }

        let name = reader.name.clone();
        Ok(Transformed::yes(
            Arc::new(MaterializedSubplanReaderExec::new(
                id,
                name,
                plan.schema(),
                Arc::clone(cache),
                partition_count,
                Arc::clone(statistics),
            )) as Arc<dyn ExecutionPlan>,
        ))
    })
    .data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::assert_batches_eq;
    use datafusion_execution::memory_pool::UnboundedMemoryPool;
    use futures::TryStreamExt;

    fn test_id() -> SubplanId {
        SubplanId::next()
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        RecordBatch::try_new(Arc::clone(schema), vec![array]).unwrap()
    }

    fn test_statistics(schema: &SchemaRef) -> Arc<Statistics> {
        Arc::new(Statistics::new_unknown(schema))
    }

    /// Helper: pre-populate the cache with a ready value (as the producer would),
    /// accounting it against an unbounded pool.
    fn prepopulate_cache(
        cache: &MaterializedSubplanCache,
        partitions: Vec<Vec<RecordBatch>>,
    ) {
        let pool = Arc::new(UnboundedMemoryPool::default()) as _;
        let reservation = MemoryConsumer::new("test").register(&pool);
        let batches = MaterializedBatches {
            partitions,
            reservation,
        };
        cache
            .try_once(move || Ok(async move { Ok(batches) }))
            .expect("try_once should succeed on first call");
    }

    #[tokio::test]
    async fn test_cache_try_once_populates() {
        let cache = MaterializedSubplanCache::new(test_id());
        let schema = test_schema();
        let batch = test_batch(&schema);
        prepopulate_cache(&cache, vec![vec![batch.clone()]]);

        let mut once_fut = cache
            .try_once(|| -> Result<std::future::Ready<_>> {
                internal_err!("should not be called")
            })
            .unwrap();
        let cached = std::future::poll_fn(|cx| once_fut.get_shared(cx))
            .await
            .unwrap();
        assert_eq!(cached.partitions.len(), 1);
        assert_eq!(cached.partitions[0].len(), 1);
        assert_eq!(cached.partitions[0][0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_cache_try_once_returns_same_result() {
        let cache = MaterializedSubplanCache::new(test_id());
        let schema = test_schema();
        let batch = test_batch(&schema);
        prepopulate_cache(&cache, vec![vec![batch.clone()]]);

        // Second call returns the same result (closure is never invoked).
        let mut fut = cache
            .try_once(|| -> Result<std::future::Ready<_>> {
                internal_err!("closure must not run on the second call")
            })
            .unwrap();
        let result = std::future::poll_fn(|cx| fut.get_shared(cx)).await.unwrap();
        assert_eq!(result.partitions[0][0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_reader_exec_reads_from_cache() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let cache = Arc::new(MaterializedSubplanCache::new(test_id()));
        prepopulate_cache(&cache, vec![vec![batch.clone()]]);

        let reader = MaterializedSubplanReaderExec::new(
            test_id(),
            "test".into(),
            Arc::clone(&schema),
            cache,
            1,
            test_statistics(&schema),
        );

        let context = Arc::new(TaskContext::default());
        let stream = reader.execute(0, context).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let expected = [
            "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_reader_exec_serves_partitions_one_to_one() {
        let schema = test_schema();
        let p0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10]))],
        )
        .unwrap();
        let p1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![20, 21]))],
        )
        .unwrap();
        let cache = Arc::new(MaterializedSubplanCache::new(test_id()));
        prepopulate_cache(&cache, vec![vec![p0], vec![p1]]);

        let reader = MaterializedSubplanReaderExec::new(
            test_id(),
            "test".into(),
            Arc::clone(&schema),
            cache,
            2,
            test_statistics(&schema),
        );

        // Reports exactly the materialized partition count.
        assert_eq!(
            reader.properties().output_partitioning().partition_count(),
            2
        );

        let context = Arc::new(TaskContext::default());
        // Partition 1 replays exactly the subplan's 1st partition (no coalescing).
        let stream = reader.execute(1, context).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let expected = ["+----+", "| a  |", "+----+", "| 20 |", "| 21 |", "+----+"];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_reader_exec_fails_when_cache_empty() {
        // When no producer has initialized the cache and the reader is the first
        // (and only) caller, its defensive error closure fires.
        let schema = test_schema();
        let cache = Arc::new(MaterializedSubplanCache::new(test_id()));

        let reader = MaterializedSubplanReaderExec::new(
            test_id(),
            "test".into(),
            Arc::clone(&schema),
            cache,
            1,
            test_statistics(&schema),
        );

        let context = Arc::new(TaskContext::default());
        let result = reader.execute(0, context);
        assert!(result.is_err());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("never initialized by the producer")
        );
    }
}
