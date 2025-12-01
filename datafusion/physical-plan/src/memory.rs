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

//! Execution plan for reading in-memory batches of data

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::coop::cooperative;
use crate::execution_plan::{Boundedness, EmissionType, SchedulingType};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::{assert_eq_or_internal_err, assert_or_internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use futures::Stream;
use parking_lot::RwLock;

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Optional memory reservation bound to the data, freed on drop
    reservation: Option<MemoryReservation>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
    /// The remaining number of rows to return. If None, all rows are returned
    fetch: Option<usize>,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            reservation: None,
            schema,
            projection,
            index: 0,
            fetch: None,
        })
    }

    /// Set the memory reservation for the data
    pub fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    /// Set the number of rows to produce
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index >= self.data.len() {
            return Poll::Ready(None);
        }
        self.index += 1;
        let batch = &self.data[self.index - 1];
        // return just the columns requested
        let batch = match self.projection.as_ref() {
            Some(columns) => batch.project(columns)?,
            None => batch.clone(),
        };

        let Some(&fetch) = self.fetch.as_ref() else {
            return Poll::Ready(Some(Ok(batch)));
        };
        if fetch == 0 {
            return Poll::Ready(None);
        }

        let batch = if batch.num_rows() > fetch {
            batch.slice(0, fetch)
        } else {
            batch
        };
        self.fetch = Some(fetch - batch.num_rows());
        Poll::Ready(Some(Ok(batch)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub trait LazyBatchGenerator: Send + Sync + fmt::Debug + fmt::Display {
    /// Returns the generator as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    /// Generate the next batch, return `None` when no more batches are available
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Execution plan for lazy in-memory batches of data
///
/// This plan generates output batches lazily, it doesn't have to buffer all batches
/// in memory up front (compared to `MemorySourceConfig`), thus consuming constant memory.
pub struct LazyMemoryExec {
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Functions to generate batches for each partition
    batch_generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    /// Plan properties cache storing equivalence properties, partitioning, and execution mode
    cache: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl LazyMemoryExec {
    /// Create a new lazy memory execution plan
    pub fn try_new(
        schema: SchemaRef,
        generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    ) -> Result<Self> {
        let boundedness = generators
            .iter()
            .map(|g| g.read().boundedness())
            .reduce(|acc, b| match acc {
                Boundedness::Bounded => b,
                Boundedness::Unbounded {
                    requires_infinite_memory,
                } => {
                    let acc_infinite_memory = requires_infinite_memory;
                    match b {
                        Boundedness::Bounded => acc,
                        Boundedness::Unbounded {
                            requires_infinite_memory,
                        } => Boundedness::Unbounded {
                            requires_infinite_memory: requires_infinite_memory
                                || acc_infinite_memory,
                        },
                    }
                }
            })
            .unwrap_or(Boundedness::Bounded);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::RoundRobinBatch(generators.len()),
            EmissionType::Incremental,
            boundedness,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Ok(Self {
            schema,
            projection: None,
            batch_generators: generators,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        match projection.as_ref() {
            Some(columns) => {
                let projected = Arc::new(self.schema.project(columns).unwrap());
                self.cache = self.cache.with_eq_properties(EquivalenceProperties::new(
                    Arc::clone(&projected),
                ));
                self.schema = projected;
                self.projection = projection;
                self
            }
            _ => self,
        }
    }

    pub fn try_set_partitioning(&mut self, partitioning: Partitioning) -> Result<()> {
        let partition_count = partitioning.partition_count();
        let generator_count = self.batch_generators.len();
        assert_eq_or_internal_err!(
            partition_count,
            generator_count,
            "Partition count must match generator count: {} != {}",
            partition_count,
            generator_count
        );
        self.cache.partitioning = partitioning;
        Ok(())
    }

    pub fn add_ordering(&mut self, ordering: impl IntoIterator<Item = PhysicalSortExpr>) {
        self.cache
            .eq_properties
            .add_orderings(std::iter::once(ordering));
    }

    /// Get the batch generators
    pub fn generators(&self) -> &Vec<Arc<RwLock<dyn LazyBatchGenerator>>> {
        &self.batch_generators
    }
}

impl fmt::Debug for LazyMemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LazyMemoryExec")
            .field("schema", &self.schema)
            .field("batch_generators", &self.batch_generators)
            .finish()
    }
}

impl DisplayAs for LazyMemoryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LazyMemoryExec: partitions={}, batch_generators=[{}]",
                    self.batch_generators.len(),
                    self.batch_generators
                        .iter()
                        .map(|g| g.read().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                //TODO: remove batch_size, add one line per generator
                writeln!(
                    f,
                    "batch_generators={}",
                    self.batch_generators
                        .iter()
                        .map(|g| g.read().to_string())
                        .collect::<Vec<String>>()
                        .join(", ")
                )?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for LazyMemoryExec {
    fn name(&self) -> &'static str {
        "LazyMemoryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_or_internal_err!(
            children.is_empty(),
            "Children cannot be replaced in LazyMemoryExec"
        );
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_or_internal_err!(
            partition < self.batch_generators.len(),
            "Invalid partition {} for LazyMemoryExec with {} partitions",
            partition,
            self.batch_generators.len()
        );

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = LazyMemoryStream {
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            generator: Arc::clone(&self.batch_generators[partition]),
            baseline_metrics,
        };
        Ok(Box::pin(cooperative(stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Stream that generates record batches on demand
pub struct LazyMemoryStream {
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Generator to produce batches
    ///
    /// Note: Idiomatically, DataFusion uses plan-time parallelism - each stream
    /// should have a unique `LazyBatchGenerator`. Use RepartitionExec or
    /// construct multiple `LazyMemoryStream`s during planning to enable
    /// parallel execution.
    /// Sharing generators between streams should be used with caution.
    generator: Arc<RwLock<dyn LazyBatchGenerator>>,
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
}

impl Stream for LazyMemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let _timer_guard = self.baseline_metrics.elapsed_compute().timer();
        let batch = self.generator.write().generate_next_batch();

        let poll = match batch {
            Ok(Some(batch)) => {
                // return just the columns requested
                let batch = match self.projection.as_ref() {
                    Some(columns) => batch.project(columns)?,
                    None => batch,
                };
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        };

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for LazyMemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod lazy_memory_tests {
    use super::*;
    use crate::common::collect;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    #[derive(Debug, Clone)]
    struct TestGenerator {
        counter: i64,
        max_batches: i64,
        batch_size: usize,
        schema: SchemaRef,
    }

    impl fmt::Display for TestGenerator {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "TestGenerator: counter={}, max_batches={}, batch_size={}",
                self.counter, self.max_batches, self.batch_size
            )
        }
    }

    impl LazyBatchGenerator for TestGenerator {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.counter >= self.max_batches {
                return Ok(None);
            }

            let array = Int64Array::from_iter_values(
                (self.counter * self.batch_size as i64)
                    ..(self.counter * self.batch_size as i64 + self.batch_size as i64),
            );
            self.counter += 1;
            Ok(Some(RecordBatch::try_new(
                Arc::clone(&self.schema),
                vec![Arc::new(array)],
            )?))
        }
    }

    #[tokio::test]
    async fn test_lazy_memory_exec() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let generator = TestGenerator {
            counter: 0,
            max_batches: 3,
            batch_size: 2,
            schema: Arc::clone(&schema),
        };

        let exec =
            LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;

        // Test schema
        assert_eq!(exec.schema().fields().len(), 1);
        assert_eq!(exec.schema().field(0).name(), "a");

        // Test execution
        let stream = exec.execute(0, Arc::new(TaskContext::default()))?;
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 3);

        // Verify batch contents
        let batch0 = batches[0].as_ref().unwrap();
        let array0 = batch0
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array0.values(), &[0, 1]);

        let batch1 = batches[1].as_ref().unwrap();
        let array1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array1.values(), &[2, 3]);

        let batch2 = batches[2].as_ref().unwrap();
        let array2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array2.values(), &[4, 5]);

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_invalid_partition() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let generator = TestGenerator {
            counter: 0,
            max_batches: 1,
            batch_size: 1,
            schema: Arc::clone(&schema),
        };

        let exec =
            LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;

        // Test invalid partition
        let result = exec.execute(1, Arc::new(TaskContext::default()));

        // partition is 0-indexed, so there only should be partition 0
        assert!(matches!(
            result,
            Err(e) if e.to_string().contains("Invalid partition 1 for LazyMemoryExec with 1 partitions")
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_series_metrics_integration() -> Result<()> {
        // Test LazyMemoryExec metrics with different configurations
        let test_cases = vec![
            (10, 2, 10),    // 10 rows, batch size 2, expected 10 rows
            (100, 10, 100), // 100 rows, batch size 10, expected 100 rows
            (5, 1, 5),      // 5 rows, batch size 1, expected 5 rows
        ];

        for (total_rows, batch_size, expected_rows) in test_cases {
            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
            let generator = TestGenerator {
                counter: 0,
                max_batches: (total_rows + batch_size - 1) / batch_size, // ceiling division
                batch_size: batch_size as usize,
                schema: Arc::clone(&schema),
            };

            let exec =
                LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;
            let task_ctx = Arc::new(TaskContext::default());

            let stream = exec.execute(0, task_ctx)?;
            let batches = collect(stream).await?;

            // Verify metrics exist with actual expected numbers
            let metrics = exec.metrics().unwrap();

            // Count actual rows returned
            let actual_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(actual_rows, expected_rows);

            // Verify metrics match actual output
            assert_eq!(metrics.output_rows().unwrap(), expected_rows);
            assert!(metrics.elapsed_compute().unwrap() > 0);
        }

        Ok(())
    }
}
