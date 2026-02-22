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
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, assert_eq_or_internal_err, assert_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::EquivalenceProperties;

use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use futures::{Stream, StreamExt, stream};
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

#[deprecated(
    note = "Use LazyPartition with LazyMemoryExec::try_new_with_partitions instead"
)]
pub trait LazyBatchGenerator: Send + Sync + fmt::Debug + fmt::Display {
    /// Returns the generator as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    /// Generate the next batch, return `None` when no more batches are available
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Returns a new instance with the state reset.
    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>>;
}

/// A partition that lazily produces record batches via [`SendableRecordBatchStream`].
///
/// Each call to [`execute`](Self::execute) must return an independent stream
/// starting from the beginning of the partition's data.  Implementations must
/// be safe to call `execute` multiple times (replay semantics).
///
/// Used with [`LazyMemoryExec::try_new_with_partitions`] to create execution
/// plans that generate data on-demand without buffering all batches in memory.
pub trait LazyPartition: fmt::Debug + fmt::Display + Send + Sync {
    /// Returns the partition as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Schema produced by this partition.
    fn schema(&self) -> &SchemaRef;

    /// Returns the boundedness of this partition.
    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    /// Creates a fresh stream for this partition.
    fn execute(&self) -> Result<SendableRecordBatchStream>;
}

/// Compatibility adapter for legacy [`LazyBatchGenerator`].
#[derive(Debug)]
#[expect(deprecated)]
pub struct LazyBatchGeneratorPartition {
    schema: SchemaRef,
    generator: Arc<RwLock<dyn LazyBatchGenerator>>,
}

#[expect(deprecated)]
impl LazyBatchGeneratorPartition {
    pub fn new(
        schema: SchemaRef,
        generator: Arc<RwLock<dyn LazyBatchGenerator>>,
    ) -> Self {
        Self { schema, generator }
    }

    pub fn generator(&self) -> &Arc<RwLock<dyn LazyBatchGenerator>> {
        &self.generator
    }
}

impl fmt::Display for LazyBatchGeneratorPartition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.generator.read().fmt(f)
    }
}

#[expect(deprecated)]
impl LazyPartition for LazyBatchGeneratorPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn boundedness(&self) -> Boundedness {
        self.generator.read().boundedness()
    }

    fn execute(&self) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let generator = self.generator.read().reset_state();
        let stream = stream::try_unfold(generator, |generator| async move {
            let batch = generator.write().generate_next_batch()?;
            Ok(batch.map(|batch| (batch, generator)))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn aggregate_boundedness(boundedness: impl Iterator<Item = Boundedness>) -> Boundedness {
    boundedness
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
        .unwrap_or(Boundedness::Bounded)
}

#[expect(deprecated)]
fn collect_legacy_generators(
    partitions: &[Arc<dyn LazyPartition>],
) -> Vec<Arc<RwLock<dyn LazyBatchGenerator>>> {
    partitions
        .iter()
        .filter_map(|partition| {
            partition
                .as_any()
                .downcast_ref::<LazyBatchGeneratorPartition>()
                .map(|adapter| Arc::clone(adapter.generator()))
        })
        .collect()
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
    /// Partition implementations for each output partition
    partitions: Vec<Arc<dyn LazyPartition>>,
    /// Legacy generator compatibility cache for deprecated APIs.
    #[expect(deprecated)]
    legacy_generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    /// Plan properties cache storing equivalence properties, partitioning, and execution mode
    cache: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl LazyMemoryExec {
    /// Create a new lazy memory execution plan from partition implementations.
    pub fn try_new_with_partitions(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn LazyPartition>>,
    ) -> Result<Self> {
        for partition in &partitions {
            assert_eq_or_internal_err!(
                partition.schema().as_ref(),
                schema.as_ref(),
                "Partition schema must match LazyMemoryExec schema"
            );
        }

        let boundedness =
            aggregate_boundedness(partitions.iter().map(|p| p.boundedness()));
        let legacy_generators = collect_legacy_generators(&partitions);

        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::RoundRobinBatch(partitions.len()),
            EmissionType::Incremental,
            boundedness,
        )
        .with_scheduling_type(SchedulingType::Cooperative);

        Ok(Self {
            schema,
            projection: None,
            partitions,
            legacy_generators,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Create a new lazy memory execution plan
    #[deprecated(note = "Use LazyMemoryExec::try_new_with_partitions instead")]
    #[expect(deprecated)]
    pub fn try_new(
        schema: SchemaRef,
        generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    ) -> Result<Self> {
        let partitions = generators
            .into_iter()
            .map(|generator| {
                Arc::new(LazyBatchGeneratorPartition::new(
                    Arc::clone(&schema),
                    generator,
                )) as Arc<dyn LazyPartition>
            })
            .collect::<Vec<_>>();
        Self::try_new_with_partitions(schema, partitions)
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
        let generator_count = self.partitions.len();
        assert_eq_or_internal_err!(
            partition_count,
            generator_count,
            "Partitioning count must match number of partitions: {} != {}",
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

    /// Get the partitions.
    pub fn partitions(&self) -> &[Arc<dyn LazyPartition>] {
        &self.partitions
    }

    /// Get the batch generators
    #[deprecated(note = "Use LazyMemoryExec::partitions instead")]
    #[expect(deprecated)]
    pub fn generators(&self) -> &Vec<Arc<RwLock<dyn LazyBatchGenerator>>> {
        &self.legacy_generators
    }
}

impl fmt::Debug for LazyMemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LazyMemoryExec")
            .field("schema", &self.schema)
            .field("partitions", &self.partitions)
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
                    self.partitions.len(),
                    self.partitions
                        .iter()
                        .map(|partition| partition.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
            DisplayFormatType::TreeRender => {
                //TODO: remove batch_size, add one line per generator
                writeln!(
                    f,
                    "batch_generators={}",
                    self.partitions
                        .iter()
                        .map(|partition| partition.to_string())
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
            partition < self.partitions.len(),
            "Invalid partition {} for LazyMemoryExec with {} partitions",
            partition,
            self.partitions.len()
        );

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let stream = self.partitions[partition].execute()?;
        let stream = match self.projection.as_ref() {
            Some(columns) => {
                let columns = Arc::new(columns.clone());
                let schema = Arc::clone(&self.schema);
                let stream = stream.map(move |batch| {
                    batch.and_then(|batch| {
                        batch.project(columns.as_ref()).map_err(Into::into)
                    })
                });
                Box::pin(RecordBatchStreamAdapter::new(schema, stream))
                    as SendableRecordBatchStream
            }
            None => stream,
        };
        let stream = ObservedStream::new(stream, baseline_metrics, None);
        Ok(Box::pin(cooperative(stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    #[expect(deprecated)]
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = self
            .partitions
            .iter()
            .map(|partition| {
                partition
                    .as_any()
                    .downcast_ref::<LazyBatchGeneratorPartition>()
                    .map_or_else(
                        || Arc::clone(partition),
                        |adapter| {
                            Arc::new(LazyBatchGeneratorPartition::new(
                                Arc::clone(adapter.schema()),
                                adapter.generator().read().reset_state(),
                            )) as Arc<dyn LazyPartition>
                        },
                    )
            })
            .collect::<Vec<_>>();
        let legacy_generators = collect_legacy_generators(&partitions);
        Ok(Arc::new(LazyMemoryExec {
            schema: Arc::clone(&self.schema),
            partitions,
            legacy_generators,
            cache: self.cache.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            projection: self.projection.clone(),
        }))
    }
}

#[cfg(test)]
#[expect(deprecated)]
mod lazy_memory_tests {
    use super::*;
    use crate::common::collect;
    use crate::stream::RecordBatchStreamAdapter;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{StreamExt, stream};

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

        fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
            Arc::new(RwLock::new(TestGenerator {
                counter: 0,
                max_batches: self.max_batches,
                batch_size: self.batch_size,
                schema: Arc::clone(&self.schema),
            }))
        }
    }

    #[derive(Debug, Clone)]
    struct TestPartition {
        schema: SchemaRef,
        batches: Arc<Vec<RecordBatch>>,
        error_at_batch: Option<usize>,
    }

    impl fmt::Display for TestPartition {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "TestPartition: batches={}, error_at_batch={:?}",
                self.batches.len(),
                self.error_at_batch
            )
        }
    }

    impl LazyPartition for TestPartition {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self) -> Result<SendableRecordBatchStream> {
            let schema = Arc::clone(&self.schema);
            let batches = Arc::clone(&self.batches);
            let error_at_batch = self.error_at_batch;
            let stream = stream::try_unfold(0usize, move |index| {
                let batches = Arc::clone(&batches);
                async move {
                    if error_at_batch.is_some_and(|fail_at| fail_at == index) {
                        return Err(datafusion_common::internal_datafusion_err!(
                            "injected partition error at batch {index}"
                        ));
                    }

                    let Some(batch) = batches.get(index).cloned() else {
                        return Ok(None);
                    };
                    Ok(Some((batch, index + 1)))
                }
            });

            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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
    async fn test_lazy_memory_exec_native_partition_replay_without_reset() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let partition = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch]),
            error_at_batch: None,
        });

        let exec = LazyMemoryExec::try_new_with_partitions(schema, vec![partition])?;

        let batches_first =
            collect(exec.execute(0, Arc::new(TaskContext::default()))?).await?;
        let batches_second =
            collect(exec.execute(0, Arc::new(TaskContext::default()))?).await?;

        assert_eq!(batches_first, batches_second);
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_partition_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )?;
        let partition = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch]),
            error_at_batch: None,
        });

        let exec = LazyMemoryExec::try_new_with_partitions(schema, vec![partition])?
            .with_projection(Some(vec![1]));

        let batches = collect(exec.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "b");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 2]);
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_deprecated_try_new_replays_on_execute() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let generator = TestGenerator {
            counter: 0,
            max_batches: 2,
            batch_size: 2,
            schema: Arc::clone(&schema),
        };

        let exec =
            LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;
        let task_ctx = Arc::new(TaskContext::default());

        let first = collect(exec.execute(0, Arc::clone(&task_ctx))?).await?;
        let second = collect(exec.execute(0, task_ctx)?).await?;

        assert_eq!(first, second);
        Ok(())
    }

    #[test]
    fn test_lazy_memory_exec_generators_compatibility_accessor() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let legacy_exec = LazyMemoryExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(RwLock::new(TestGenerator {
                counter: 0,
                max_batches: 1,
                batch_size: 1,
                schema: Arc::clone(&schema),
            }))],
        )?;
        assert_eq!(legacy_exec.generators().len(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )?;
        let native_exec = LazyMemoryExec::try_new_with_partitions(
            schema,
            vec![Arc::new(TestPartition {
                schema: legacy_exec.schema(),
                batches: Arc::new(vec![batch]),
                error_at_batch: None,
            })],
        )?;
        assert!(native_exec.generators().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_error_then_end() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )?;
        let partition = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch]),
            error_at_batch: Some(1),
        });
        let exec = LazyMemoryExec::try_new_with_partitions(schema, vec![partition])?;

        let mut stream = exec.execute(0, Arc::new(TaskContext::default()))?;
        assert!(matches!(stream.next().await, Some(Ok(_))));
        assert!(matches!(stream.next().await, Some(Err(_))));
        assert!(stream.next().await.is_none());
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
    async fn test_lazy_memory_exec_metrics() -> Result<()> {
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

    #[tokio::test]
    async fn test_lazy_memory_exec_reset_state() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let generator = TestGenerator {
            counter: 0,
            max_batches: 3,
            batch_size: 2,
            schema: Arc::clone(&schema),
        };

        let exec = Arc::new(LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(generator))],
        )?);
        let stream = exec.execute(0, Arc::new(TaskContext::default()))?;
        let batches = collect(stream).await?;

        let exec_reset = exec.reset_state()?;
        let stream = exec_reset.execute(0, Arc::new(TaskContext::default()))?;
        let batches_reset = collect(stream).await?;

        // if the reset_state is not correct, the batches_reset will be empty
        assert_eq!(batches, batches_reset);

        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_multi_partition() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch_a = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )?;
        let batch_b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )?;
        let p0 = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch_a.clone()]),
            error_at_batch: None,
        });
        let p1 = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch_b.clone()]),
            error_at_batch: None,
        });

        let exec = LazyMemoryExec::try_new_with_partitions(
            schema,
            vec![p0 as Arc<dyn LazyPartition>, p1 as Arc<dyn LazyPartition>],
        )?;

        let ctx = Arc::new(TaskContext::default());
        let batches_0 = collect(exec.execute(0, Arc::clone(&ctx))?).await?;
        let batches_1 = collect(exec.execute(1, Arc::clone(&ctx))?).await?;

        assert_eq!(batches_0, vec![batch_a]);
        assert_eq!(batches_1, vec![batch_b]);
        Ok(())
    }

    #[tokio::test]
    async fn test_lazy_memory_exec_native_partition_reset_state() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let partition = Arc::new(TestPartition {
            schema: Arc::clone(&schema),
            batches: Arc::new(vec![batch]),
            error_at_batch: None,
        });

        let exec = Arc::new(LazyMemoryExec::try_new_with_partitions(
            schema,
            vec![partition],
        )?);

        let ctx = Arc::new(TaskContext::default());
        let batches_before = collect(exec.execute(0, Arc::clone(&ctx))?).await?;

        let exec_reset = exec.reset_state()?;
        let batches_after = collect(exec_reset.execute(0, ctx)?).await?;

        assert_eq!(batches_before, batches_after);
        Ok(())
    }
}
