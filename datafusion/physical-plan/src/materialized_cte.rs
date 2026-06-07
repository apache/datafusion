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

//! Physical plan nodes for materialized CTEs.

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::coop::cooperative;
use crate::execution_plan::{Boundedness, EmissionType, collect_partitioned};
use crate::joins::utils::{OnceAsync, OnceFut};
use crate::memory::MemoryStream;
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
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
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::TryStreamExt;

/// A shared cache that stores the materialized CTE results.
/// The cache uses `OnceAsync` to ensure the CTE is only computed once,
/// while allowing multiple consumers to await the result concurrently.
#[derive(Debug)]
pub struct MaterializedCteCache {
    /// Name of the CTE (for debugging)
    #[expect(dead_code)]
    name: String,
    /// The shared one-time async computation of the CTE batches
    once: OnceAsync<Vec<Vec<RecordBatch>>>,
}

impl MaterializedCteCache {
    /// Create a new empty cache for the given CTE name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            once: OnceAsync::default(),
        }
    }

    /// Get or initialize the cached batches via `OnceAsync::try_once`.
    /// The first caller triggers computation; subsequent callers share the result.
    pub(crate) fn try_once<F, Fut>(&self, f: F) -> Result<OnceFut<Vec<Vec<RecordBatch>>>>
    where
        F: FnOnce() -> Result<Fut>,
        Fut: Future<Output = Result<Vec<Vec<RecordBatch>>>> + Send + 'static,
    {
        self.once.try_once(f)
    }
}

/// Physical execution plan that materializes a CTE and then executes
/// a continuation plan. The CTE results are cached in a shared
/// `MaterializedCteCache` for use by `MaterializedCteReaderExec` nodes.
#[derive(Debug)]
pub struct MaterializedCteExec {
    /// Name of the CTE
    name: String,
    /// The plan that computes the CTE
    cte_plan: Arc<dyn ExecutionPlan>,
    /// The continuation plan that uses the materialized CTE
    continuation: Arc<dyn ExecutionPlan>,
    /// Shared cache for the CTE results
    cache: Arc<MaterializedCteCache>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties
    properties: Arc<PlanProperties>,
}

impl MaterializedCteExec {
    /// Create a new MaterializedCteExec.
    pub fn new(
        name: String,
        cte_plan: Arc<dyn ExecutionPlan>,
        continuation: Arc<dyn ExecutionPlan>,
        cache: Arc<MaterializedCteCache>,
    ) -> Self {
        let properties = Arc::clone(continuation.properties());
        Self {
            name,
            cte_plan,
            continuation,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }
    }
}

impl DisplayAs for MaterializedCteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for MaterializedCteExec {
    fn name(&self) -> &'static str {
        "MaterializedCteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.cte_plan, &self.continuation]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "MaterializedCteExec expected 2 children, got {}",
                children.len()
            );
        }
        let cte_plan = Arc::clone(&children[0]);
        let partition_count = cte_plan.output_partitioning().partition_count();
        let statistics = materialized_cte_statistics(cte_plan.as_ref())?;
        let continuation = replace_materialized_cte_readers(
            Arc::clone(&children[1]),
            &self.name,
            &self.cache,
            partition_count,
            &statistics,
        )?;
        Ok(Arc::new(Self::new(
            self.name.clone(),
            cte_plan,
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
                "MaterializedCteExec got partition {partition}, expected less than {output_partitions}"
            );
        }

        let cte_plan = Arc::clone(&self.cte_plan);
        let continuation = Arc::clone(&self.continuation);
        let name = self.name.clone();
        let ctx = Arc::clone(&context);
        let schema = Arc::clone(&self.continuation.schema());

        // Use OnceAsync to ensure the CTE is materialized exactly once,
        // even when multiple partitions call execute() concurrently.
        let mut once_fut = self.cache.try_once(move || {
            Ok(async move {
                let partitions = collect_partitioned(cte_plan, ctx).await?;

                let num_partitions = partitions.len();
                let num_batches: usize = partitions.iter().map(Vec::len).sum();
                let num_rows: usize =
                    partitions.iter().flatten().map(|b| b.num_rows()).sum();
                log::info!(
                    "Materializing CTE '{name}': {num_partitions} partitions, {num_batches} batches, {num_rows} rows"
                );

                Ok(partitions)
            })
        })?;

        let ctx = Arc::clone(&context);
        let fut = async move {
            // Wait for the CTE to be materialized
            std::future::poll_fn(|cx| once_fut.get_shared(cx)).await?;
            // Now execute the continuation
            continuation.execute(partition, ctx)
        };

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(
            &self.continuation.schema(),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let cache = Arc::new(MaterializedCteCache::new(self.name.clone()));
        let partition_count = self.cte_plan.output_partitioning().partition_count();
        let statistics = materialized_cte_statistics(self.cte_plan.as_ref())?;
        let continuation = replace_materialized_cte_readers(
            Arc::clone(&self.continuation),
            &self.name,
            &cache,
            partition_count,
            &statistics,
        )?;
        Ok(Arc::new(Self::new(
            self.name.clone(),
            Arc::clone(&self.cte_plan),
            continuation,
            cache,
        )))
    }
}

/// Physical execution plan that reads from a previously materialized CTE cache.
/// This is a leaf node that retrieves the cached batches from the shared
/// `MaterializedCteCache`.
#[derive(Debug)]
pub struct MaterializedCteReaderExec {
    /// Name of the CTE
    name: String,
    /// The schema of the CTE output
    schema: SchemaRef,
    /// Shared cache to read from
    cache: Arc<MaterializedCteCache>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Statistics from the plan that produces the materialized CTE
    statistics: Arc<Statistics>,
    /// Cache holding plan properties
    properties: Arc<PlanProperties>,
}

impl MaterializedCteReaderExec {
    /// Create a new MaterializedCteReaderExec.
    pub fn new(
        name: String,
        schema: SchemaRef,
        cache: Arc<MaterializedCteCache>,
        partition_count: usize,
        statistics: Arc<Statistics>,
    ) -> Self {
        let partition_count = reader_partition_count(partition_count, &statistics);
        let properties = Self::compute_properties(Arc::clone(&schema), partition_count);
        Self {
            name,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics,
            properties: Arc::new(properties),
        }
    }

    /// The CTE this reader reads from.
    pub fn cte_name(&self) -> &str {
        &self.name
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

impl DisplayAs for MaterializedCteReaderExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteReaderExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for MaterializedCteReaderExec {
    fn name(&self) -> &'static str {
        "MaterializedCteReaderExec"
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
                "MaterializedCteReaderExec got partition {partition}, expected less than {output_partitions}"
            );
        }

        let schema = Arc::clone(&self.schema);
        let name = self.name.clone();

        // Get a OnceFut handle to the shared computation. The producer
        // (MaterializedCteExec) triggers the actual work; here we just
        // await the result which will be ready immediately if the producer
        // has already finished.
        let mut once_fut =
            self.cache.try_once(move || -> Result<std::future::Ready<_>> {
                internal_err!(
                    "MaterializedCteReaderExec: cache for CTE '{}' was never initialized by the producer.",
                    name
                )
            })?;

        let schema_for_stream = Arc::clone(&schema);
        let fut = async move {
            let batches = std::future::poll_fn(|cx| once_fut.get_shared(cx)).await?;

            let partition_batches = if output_partitions == 1 {
                batches.iter().flatten().cloned().collect()
            } else {
                batches.get(partition).cloned().unwrap_or_default()
            };

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

fn reader_partition_count(partition_count: usize, statistics: &Statistics) -> usize {
    match statistics.num_rows.get_value() {
        Some(rows) if *rows < partition_count => 1,
        _ => partition_count,
    }
}

/// Estimate the statistics exposed by materialized CTE readers.
pub fn materialized_cte_statistics(plan: &dyn ExecutionPlan) -> Result<Arc<Statistics>> {
    Ok(Arc::clone(
        StatisticsRegistry::default_with_builtin_providers()
            .compute(plan)?
            .base_arc(),
    ))
}

/// Replace readers for a materialized CTE with readers that use the provided
/// cache and expose the provided partition count and statistics.
pub fn replace_materialized_cte_readers(
    plan: Arc<dyn ExecutionPlan>,
    name: &str,
    cache: &Arc<MaterializedCteCache>,
    partition_count: usize,
    statistics: &Arc<Statistics>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(reader) = plan.downcast_ref::<MaterializedCteReaderExec>() else {
            return Ok(Transformed::no(plan));
        };

        if reader.cte_name() != name {
            return Ok(Transformed::no(plan));
        }

        Ok(Transformed::yes(Arc::new(MaterializedCteReaderExec::new(
            name.to_string(),
            plan.schema(),
            Arc::clone(cache),
            partition_count,
            Arc::clone(statistics),
        )) as Arc<dyn ExecutionPlan>))
    })
    .data()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::assert_batches_eq;
    use datafusion_common::stats::Precision;
    use futures::TryStreamExt;

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

    fn test_statistics_with_rows(schema: &SchemaRef, rows: usize) -> Arc<Statistics> {
        Arc::new(Statistics::new_unknown(schema).with_num_rows(Precision::Exact(rows)))
    }

    /// Helper: pre-populate the cache by triggering `try_once` with a ready value.
    fn prepopulate_cache(cache: &MaterializedCteCache, batches: Vec<Vec<RecordBatch>>) {
        cache
            .try_once(move || Ok(async move { Ok(batches) }))
            .expect("try_once should succeed on first call");
    }

    #[tokio::test]
    async fn test_cache_try_once_populates() {
        let cache = MaterializedCteCache::new("test".into());

        let schema = test_schema();
        let batch = test_batch(&schema);
        let data = vec![vec![batch.clone()]];
        let mut once_fut = cache.try_once(move || Ok(async move { Ok(data) })).unwrap();

        let cached = std::future::poll_fn(|cx| once_fut.get_shared(cx))
            .await
            .unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].len(), 1);
        assert_eq!(cached[0][0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_cache_try_once_returns_same_result() {
        let cache = MaterializedCteCache::new("test".into());
        let schema = test_schema();
        let batch = test_batch(&schema);

        let data = vec![vec![batch.clone()]];
        // First call populates
        let mut fut1 = cache.try_once(move || Ok(async move { Ok(data) })).unwrap();
        let result1 = std::future::poll_fn(|cx| fut1.get_shared(cx))
            .await
            .unwrap();

        // Second call returns the same result (closure is never invoked)
        let mut fut2 = cache.try_once(|| Ok(async move { Ok(vec![]) })).unwrap();
        let result2 = std::future::poll_fn(|cx| fut2.get_shared(cx))
            .await
            .unwrap();

        assert_eq!(result1.len(), result2.len());
        assert_eq!(result1[0][0].num_rows(), result2[0][0].num_rows());
    }

    #[tokio::test]
    async fn test_reader_exec_reads_from_cache() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let cache = Arc::new(MaterializedCteCache::new("test".into()));
        prepopulate_cache(&cache, vec![vec![batch.clone()]]);

        let reader = MaterializedCteReaderExec::new(
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
    async fn test_reader_exec_preserves_cache_partitions() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let cache = Arc::new(MaterializedCteCache::new("test".into()));
        prepopulate_cache(&cache, vec![vec![batch.clone()], vec![batch.clone()]]);

        let reader = MaterializedCteReaderExec::new(
            "test".into(),
            Arc::clone(&schema),
            cache,
            2,
            test_statistics(&schema),
        );

        assert_eq!(
            reader.properties().output_partitioning().partition_count(),
            2
        );

        let context = Arc::new(TaskContext::default());
        let stream = reader.execute(1, context).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let expected = [
            "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_reader_exec_coalesces_exact_scalar_cache() {
        let schema = test_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let cache = Arc::new(MaterializedCteCache::new("test".into()));
        prepopulate_cache(&cache, vec![vec![], vec![batch.clone()]]);

        let reader = MaterializedCteReaderExec::new(
            "test".into(),
            Arc::clone(&schema),
            cache,
            2,
            test_statistics_with_rows(&schema, 1),
        );

        assert_eq!(
            reader.properties().output_partitioning().partition_count(),
            1
        );

        let context = Arc::new(TaskContext::default());
        let stream = reader.execute(0, context).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let expected = ["+---+", "| a |", "+---+", "| 1 |", "+---+"];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_reader_exec_fails_when_cache_empty() {
        let schema = test_schema();
        let cache = Arc::new(MaterializedCteCache::new("test".into()));

        let reader = MaterializedCteReaderExec::new(
            "test".into(),
            Arc::clone(&schema),
            cache,
            1,
            test_statistics(&schema),
        );

        let context = Arc::new(TaskContext::default());
        let result = reader.execute(0, context);
        // With OnceAsync, the error is returned from try_once when the
        // producer closure returns an error. The reader's closure produces
        // an internal_err if no producer has initialized the cache first.
        // However, since try_once returns the FIRST caller's result, and
        // the reader IS the first caller here, the error closure fires.
        assert!(result.is_err());
    }
}
