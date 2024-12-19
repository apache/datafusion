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

use parking_lot::RwLock;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{
    common, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::execution_plan::{Boundedness, EmissionType};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, project_schema, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

use futures::Stream;

/// Execution plan for reading in-memory batches of data
#[derive(Clone)]
pub struct MemoryExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    // Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    cache: PlanProperties,
    /// if partition sizes should be displayed
    show_sizes: bool,
}

impl fmt::Debug for MemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("MemoryExec")
            .field("partitions", &"[...]")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("sort_information", &self.sort_information)
            .finish()
    }
}

impl DisplayAs for MemoryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_sizes: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();

                let output_ordering = self
                    .sort_information
                    .first()
                    .map(|output_ordering| {
                        format!(", output_ordering={}", output_ordering)
                    })
                    .unwrap_or_default();

                if self.show_sizes {
                    write!(
                        f,
                        "MemoryExec: partitions={}, partition_sizes={partition_sizes:?}{output_ordering}",
                        partition_sizes.len(),
                    )
                } else {
                    write!(f, "MemoryExec: partitions={}", partition_sizes.len(),)
                }
            }
        }
    }
}

impl ExecutionPlan for MemoryExec {
    fn name(&self) -> &'static str {
        "MemoryExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // MemoryExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            Arc::clone(&self.projected_schema),
            self.projection.clone(),
        )?))
    }

    /// We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    fn statistics(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        ))
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        let cache =
            Self::compute_properties(Arc::clone(&projected_schema), &[], partitions);
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: vec![],
            cache,
            show_sizes: true,
        })
    }

    /// Set `show_sizes` to determine whether to display partition sizes
    pub fn with_show_sizes(mut self, show_sizes: bool) -> Self {
        self.show_sizes = show_sizes;
        self
    }

    /// Ref to partitions
    pub fn partitions(&self) -> &[Vec<RecordBatch>] {
        &self.partitions
    }

    /// Ref to projection
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    /// Show sizes
    pub fn show_sizes(&self) -> bool {
        self.show_sizes
    }

    /// Ref to sort information
    pub fn sort_information(&self) -> &[LexOrdering] {
        &self.sort_information
    }

    /// A memory table can be ordered by multiple expressions simultaneously.
    /// [`EquivalenceProperties`] keeps track of expressions that describe the
    /// global ordering of the schema. These columns are not necessarily same; e.g.
    /// ```text
    /// ┌-------┐
    /// | a | b |
    /// |---|---|
    /// | 1 | 9 |
    /// | 2 | 8 |
    /// | 3 | 7 |
    /// | 5 | 5 |
    /// └---┴---┘
    /// ```
    /// where both `a ASC` and `b DESC` can describe the table ordering. With
    /// [`EquivalenceProperties`], we can keep track of these equivalences
    /// and treat `a ASC` and `b DESC` as the same ordering requirement.
    ///
    /// Note that if there is an internal projection, that projection will be
    /// also applied to the given `sort_information`.
    pub fn try_with_sort_information(
        mut self,
        mut sort_information: Vec<LexOrdering>,
    ) -> Result<Self> {
        // All sort expressions must refer to the original schema
        let fields = self.schema.fields();
        let ambiguous_column = sort_information
            .iter()
            .flat_map(|ordering| ordering.inner.clone())
            .flat_map(|expr| collect_columns(&expr.expr))
            .find(|col| {
                fields
                    .get(col.index())
                    .map(|field| field.name() != col.name())
                    .unwrap_or(true)
            });
        if let Some(col) = ambiguous_column {
            return internal_err!(
                "Column {:?} is not found in the original schema of the MemoryExec",
                col
            );
        }

        // If there is a projection on the source, we also need to project orderings
        if let Some(projection) = &self.projection {
            let base_eqp = EquivalenceProperties::new_with_orderings(
                self.original_schema(),
                &sort_information,
            );
            let proj_exprs = projection
                .iter()
                .map(|idx| {
                    let base_schema = self.original_schema();
                    let name = base_schema.field(*idx).name();
                    (Arc::new(Column::new(name, *idx)) as _, name.to_string())
                })
                .collect::<Vec<_>>();
            let projection_mapping =
                ProjectionMapping::try_new(&proj_exprs, &self.original_schema())?;
            sort_information = base_eqp
                .project(&projection_mapping, self.schema())
                .oeq_class
                .orderings;
        }

        self.sort_information = sort_information;
        // We need to update equivalence properties when updating sort information.
        let eq_properties = EquivalenceProperties::new_with_orderings(
            self.schema(),
            &self.sort_information,
        );
        self.cache = self.cache.with_eq_properties(eq_properties);

        Ok(self)
    }

    /// Arc clone of ref to original schema
    pub fn original_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        partitions: &[Vec<RecordBatch>],
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

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
        })
    }

    /// Set the memory reservation for the data
    pub(super) fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];

            // return just the columns requested
            let batch = match self.projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };

            Some(Ok(batch))
        } else {
            None
        })
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
    /// Generate the next batch, return `None` when no more batches are available
    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// Execution plan for lazy in-memory batches of data
///
/// This plan generates output batches lazily, it doesn't have to buffer all batches
/// in memory up front (compared to `MemoryExec`), thus consuming constant memory.
pub struct LazyMemoryExec {
    /// Schema representing the data
    schema: SchemaRef,
    /// Functions to generate batches for each partition
    batch_generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    /// Plan properties cache storing equivalence properties, partitioning, and execution mode
    cache: PlanProperties,
}

impl LazyMemoryExec {
    /// Create a new lazy memory execution plan
    pub fn try_new(
        schema: SchemaRef,
        generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    ) -> Result<Self> {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::RoundRobinBatch(generators.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Ok(Self {
            schema,
            batch_generators: generators,
            cache,
        })
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
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in LazyMemoryExec")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.batch_generators.len() {
            return internal_err!(
                "Invalid partition {} for LazyMemoryExec with {} partitions",
                partition,
                self.batch_generators.len()
            );
        }

        Ok(Box::pin(LazyMemoryStream {
            schema: Arc::clone(&self.schema),
            generator: Arc::clone(&self.batch_generators[partition]),
        }))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Stream that generates record batches on demand
pub struct LazyMemoryStream {
    schema: SchemaRef,
    /// Generator to produce batches
    ///
    /// Note: Idiomatically, DataFusion uses plan-time parallelism - each stream
    /// should have a unique `LazyBatchGenerator`. Use RepartitionExec or
    /// construct multiple `LazyMemoryStream`s during planning to enable
    /// parallel execution.
    /// Sharing generators between streams should be used with caution.
    generator: Arc<RwLock<dyn LazyBatchGenerator>>,
}

impl Stream for LazyMemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let batch = self.generator.write().generate_next_batch();

        match batch {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl RecordBatchStream for LazyMemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod memory_exec_tests {
    use std::sync::Arc;

    use crate::memory::MemoryExec;
    use crate::ExecutionPlan;

    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;

    #[test]
    fn test_memory_order_eq() -> datafusion_common::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let sort1 = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions::default(),
            },
        ]);
        let sort2 = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c", &schema)?,
            options: SortOptions::default(),
        }]);
        let mut expected_output_order = LexOrdering::default();
        expected_output_order.extend(sort1.clone());
        expected_output_order.extend(sort2.clone());

        let sort_information = vec![sort1.clone(), sort2.clone()];
        let mem_exec = MemoryExec::try_new(&[vec![]], schema, None)?
            .try_with_sort_information(sort_information)?;

        assert_eq!(
            mem_exec.properties().output_ordering().unwrap().to_vec(),
            expected_output_order.inner
        );
        let eq_properties = mem_exec.properties().equivalence_properties();
        assert!(eq_properties.oeq_class().contains(&sort1));
        assert!(eq_properties.oeq_class().contains(&sort2));
        Ok(())
    }
}

#[cfg(test)]
mod lazy_memory_tests {
    use super::*;
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
}
