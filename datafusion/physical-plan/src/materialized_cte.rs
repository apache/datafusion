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
use std::sync::Arc;

use crate::execution_plan::{Boundedness, EmissionType};
use crate::memory::MemoryStream;
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use futures::TryStreamExt;
use tokio::sync::OnceCell;

/// A shared cache that stores the materialized CTE results.
/// The cache uses a `OnceCell` to ensure the CTE is only computed once.
#[derive(Debug)]
pub struct MaterializedCteCache {
    /// Name of the CTE (for debugging)
    name: String,
    /// The cached batches, populated once by the producer
    batches: OnceCell<Vec<RecordBatch>>,
}

impl MaterializedCteCache {
    /// Create a new empty cache for the given CTE name.
    pub fn new(name: String) -> Self {
        Self {
            name,
            batches: OnceCell::new(),
        }
    }

    /// Store batches into the cache. Returns error if already populated.
    pub fn store(&self, batches: Vec<RecordBatch>) -> Result<()> {
        self.batches.set(batches).map_err(|_| {
            datafusion_common::DataFusionError::Internal(format!(
                "MaterializedCteCache '{}' was already populated",
                self.name
            ))
        })
    }

    /// Get the cached batches. Returns None if not yet populated.
    pub fn get(&self) -> Option<&Vec<RecordBatch>> {
        self.batches.get()
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
        let properties = Self::compute_properties(&continuation);
        Self {
            name,
            cte_plan,
            continuation,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(properties),
        }
    }

    fn compute_properties(continuation: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&continuation.schema())),
            Partitioning::UnknownPartitioning(
                continuation
                    .properties()
                    .output_partitioning()
                    .partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
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
        Ok(Arc::new(Self::new(
            self.name.clone(),
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            Arc::clone(&self.cache),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let cache = Arc::clone(&self.cache);
        let cte_plan = Arc::clone(&self.cte_plan);
        let continuation = Arc::clone(&self.continuation);
        let name = self.name.clone();
        let ctx = Arc::clone(&context);
        let schema = Arc::clone(&self.continuation.schema());

        let fut = async move {
            // Materialize the CTE if not already done
            if cache.get().is_none() {
                let stream = cte_plan.execute(0, Arc::clone(&ctx))?;
                let batches: Vec<RecordBatch> = stream.try_collect().await?;

                let num_batches = batches.len();
                let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                log::info!(
                    "Materializing CTE '{}': {} batches, {} rows",
                    name,
                    num_batches,
                    num_rows
                );

                cache.store(batches)?;
            }

            // Execute the continuation plan
            continuation.execute(partition, ctx)
        };

        // Use futures::stream::once to create a stream from the future,
        // then flatten it to get a stream of RecordBatches
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
    /// Cache holding plan properties
    properties: Arc<PlanProperties>,
}

impl MaterializedCteReaderExec {
    /// Create a new MaterializedCteReaderExec.
    pub fn new(
        name: String,
        schema: SchemaRef,
        cache: Arc<MaterializedCteCache>,
    ) -> Self {
        let properties = Self::compute_properties(Arc::clone(&schema));
        Self {
            name,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(properties),
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batches = self.cache.get().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(format!(
                "MaterializedCteReaderExec: cache for CTE '{}' is not yet populated. \
                 The producer must execute before the reader.",
                self.name
            ))
        })?;

        let stream =
            MemoryStream::try_new(batches.clone(), Arc::clone(&self.schema), None)?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::assert_batches_eq;
    use futures::TryStreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn test_batch(schema: &SchemaRef) -> RecordBatch {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        RecordBatch::try_new(Arc::clone(schema), vec![array]).unwrap()
    }

    #[test]
    fn test_cache_store_and_get() {
        let cache = MaterializedCteCache::new("test".into());
        assert!(cache.get().is_none());

        let schema = test_schema();
        let batch = test_batch(&schema);
        cache.store(vec![batch.clone()]).unwrap();

        let cached = cache.get().unwrap();
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].num_rows(), 3);
    }

    #[test]
    fn test_cache_double_store_fails() {
        let cache = MaterializedCteCache::new("test".into());
        let schema = test_schema();
        let batch = test_batch(&schema);

        cache.store(vec![batch.clone()]).unwrap();
        assert!(cache.store(vec![batch]).is_err());
    }

    #[tokio::test]
    async fn test_reader_exec_reads_from_cache() {
        let schema = test_schema();
        let batch = test_batch(&schema);
        let cache = Arc::new(MaterializedCteCache::new("test".into()));
        cache.store(vec![batch.clone()]).unwrap();

        let reader =
            MaterializedCteReaderExec::new("test".into(), Arc::clone(&schema), cache);

        let context = Arc::new(TaskContext::default());
        let stream = reader.execute(0, context).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let expected = vec![
            "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_reader_exec_fails_when_cache_empty() {
        let schema = test_schema();
        let cache = Arc::new(MaterializedCteCache::new("test".into()));

        let reader =
            MaterializedCteReaderExec::new("test".into(), Arc::clone(&schema), cache);

        let context = Arc::new(TaskContext::default());
        let result = reader.execute(0, context);
        assert!(result.is_err());
    }
}
