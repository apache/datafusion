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

//! Utilities for testing datafusion-physical-plan

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::fmt;
use arrow::array::{ArrayRef, Int32Array, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::LexOrdering;
use futures::{Future, FutureExt};


use std::task::{Context, Poll};

use crate::projection::{
    all_alias_free_columns, new_projections_for_columns,
};
use crate::{
    common, ColumnarValue,
    PhysicalExpr, RecordBatchStream,
};

use datafusion_common::{
    plan_err, project_schema, Result,
};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::Column;

use futures::Stream;

use crate::projection::ProjectionExec;
// use crate::memory::MockMemorySourceConfig;
// use crate::source::MockDataSourceExec;
use crate::stream::RecordBatchStreamAdapter;
use crate::streaming::PartitionStream;
use crate::ExecutionPlan;

pub mod exec;

use std::any::Any;
use std::fmt::{Debug, Formatter};

use crate::execution_plan::{Boundedness, EmissionType};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, PlanProperties,
};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{internal_err, Constraints, ScalarValue, Statistics};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

/// Common behaviors in Data Sources for both from Files and MockMemory.
/// See `MockDataSourceExec` for physical plan implementation
pub trait MockDataSource: Send + Sync {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream>;
    fn as_any(&self) -> &dyn Any;
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result;
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn MockDataSource>>> {
        Ok(None)
    }

    fn output_partitioning(&self) -> Partitioning;
    fn eq_properties(&self) -> EquivalenceProperties;
    fn statistics(&self) -> Result<Statistics>;
    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn MockDataSource>>;
    fn fetch(&self) -> Option<usize>;
    fn metrics(&self) -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }
    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

impl Debug for dyn MockDataSource {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MockDataSource: ")
    }
}

/// Unified data source for file formats like JSON, CSV, AVRO, ARROW, PARQUET
#[derive(Clone, Debug)]
pub struct MockDataSourceExec {
    source: Arc<dyn MockDataSource>,
    cache: PlanProperties,
}

impl DisplayAs for MockDataSourceExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "MockDataSourceExec: ")?;
        self.source.fmt_as(t, f)
    }
}

impl ExecutionPlan for MockDataSourceExec {
    fn name(&self) -> &'static str {
        "MockDataSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let source = self.source.repartitioned(
            target_partitions,
            config.optimizer.repartition_file_min_size,
            self.properties().eq_properties.output_ordering(),
        )?;

        if let Some(source) = source {
            let output_partitioning = source.output_partitioning();
            let plan = self
                .clone()
                .with_source(source)
                // Changing source partitioning may invalidate output partitioning. Update it also
                .with_partitioning(output_partitioning);
            Ok(Some(Arc::new(plan)))
        } else {
            Ok(Some(Arc::new(self.clone())))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.source.open(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.source.metrics().clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.source.statistics()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut source = Arc::clone(&self.source);
        source = source.with_fetch(limit)?;
        let cache = self.cache.clone();

        Some(Arc::new(Self { source, cache }))
    }

    fn fetch(&self) -> Option<usize> {
        self.source.fetch()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.source.try_swapping_with_projection(projection)
    }
    
}

impl MockDataSourceExec {
    pub fn new(source: Arc<dyn MockDataSource>) -> Self {
        let cache = Self::compute_properties(Arc::clone(&source));
        Self { source, cache }
    }

    /// Return the source object
    pub fn source(&self) -> &Arc<dyn MockDataSource> {
        &self.source
    }

    pub fn with_source(mut self, source: Arc<dyn MockDataSource>) -> Self {
        self.cache = Self::compute_properties(Arc::clone(&source));
        self.source = source;
        self
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.cache = self.cache.with_constraints(constraints);
        self
    }

    /// Assign output partitioning
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.cache = self.cache.with_partitioning(partitioning);
        self
    }

    fn compute_properties(source: Arc<dyn MockDataSource>) -> PlanProperties {
        PlanProperties::new(
            source.eq_properties(),
            source.output_partitioning(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

/// Data source configuration for reading in-memory batches of data
#[derive(Clone)]
pub struct MockMemorySourceConfig {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    /// if partition sizes should be displayed
    show_sizes: bool,
    /// The maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    fetch: Option<usize>,
}

impl MockDataSource for MockMemorySourceConfig {
    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(
            MockMemoryStream::try_new(
                self.partitions[partition].clone(),
                Arc::clone(&self.projected_schema),
                self.projection.clone(),
            )?
            .with_fetch(self.fetch),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
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

                let eq_properties = self.eq_properties();
                let constraints = eq_properties.constraints();
                let constraints = if constraints.is_empty() {
                    String::new()
                } else {
                    format!(", {}", constraints)
                };

                let limit = self
                    .fetch
                    .map_or(String::new(), |limit| format!(", fetch={}", limit));
                if self.show_sizes {
                    write!(
                        f,
                        "partitions={}, partition_sizes={partition_sizes:?}{limit}{output_ordering}{constraints}",
                        partition_sizes.len(),
                    )
                } else {
                    write!(
                        f,
                        "partitions={}{limit}{output_ordering}{constraints}",
                        partition_sizes.len(),
                    )
                }
            }
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(
            Arc::clone(&self.projected_schema),
            self.sort_information.as_slice(),
        )
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        ))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn MockDataSource>> {
        let source = self.clone();
        Some(Arc::new(source.with_limit(limit)))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If there is any non-column or alias-carrier expression, Projection should not be removed.
        // This process can be moved into MockMemoryExec, but it would be an overlap of their responsibility.
        all_alias_free_columns(projection.expr())
            .then(|| {
                let all_projections = (0..self.schema.fields().len()).collect();
                let new_projections = new_projections_for_columns(
                    projection,
                    self.projection().as_ref().unwrap_or(&all_projections),
                );

                MockMemorySourceConfig::try_new_exec(
                    self.partitions(),
                    self.original_schema(),
                    Some(new_projections),
                )
                .map(|e| e as _)
            })
            .transpose()
    }
}

impl MockMemorySourceConfig {
    /// Create a new `MockMemorySourceConfig` for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        })
    }

    /// Create a new `DataSourceExec` plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new_exec(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Arc<MockDataSourceExec>> {
        let source = Self::try_new(partitions, schema, projection)?;
        Ok(Arc::new(MockDataSourceExec::new(Arc::new(source))))
    }

    /// Create a new execution plan from a list of constant values (`ValuesExec`)
    pub fn try_new_as_values(
        schema: SchemaRef,
        data: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Arc<MockDataSourceExec>> {
        if data.is_empty() {
            return plan_err!("Values list cannot be empty");
        }

        let n_row = data.len();
        let n_col = schema.fields().len();

        // We have this single row batch as a placeholder to satisfy evaluation argument
        // and generate a single output row
        let placeholder_schema = Arc::new(Schema::empty());
        let placeholder_batch = RecordBatch::try_new_with_options(
            Arc::clone(&placeholder_schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        // Evaluate each column
        let arrays = (0..n_col)
            .map(|j| {
                (0..n_row)
                    .map(|i| {
                        let expr = &data[i][j];
                        let result = expr.evaluate(&placeholder_batch)?;

                        match result {
                            ColumnarValue::Scalar(scalar) => Ok(scalar),
                            ColumnarValue::Array(array) if array.len() == 1 => {
                                ScalarValue::try_from_array(&array, 0)
                            }
                            ColumnarValue::Array(_) => {
                                plan_err!("Cannot have array values in a values list")
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(ScalarValue::iter_to_array)
            })
            .collect::<Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(n_row)),
        )?;

        let partitions = vec![batch];
        Self::try_new_from_batches(Arc::clone(&schema), partitions)
    }

    /// Create a new plan using the provided schema and batches.
    ///
    /// Errors if any of the batches don't match the provided schema, or if no
    /// batches are provided.
    pub fn try_new_from_batches(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Arc<MockDataSourceExec>> {
        if batches.is_empty() {
            return plan_err!("Values list cannot be empty");
        }

        for batch in &batches {
            let batch_schema = batch.schema();
            if batch_schema != schema {
                return plan_err!(
                    "Batch has invalid schema. Expected: {}, got: {}",
                    schema,
                    batch_schema
                );
            }
        }

        let partitions = vec![batches];
        let source = Self {
            partitions,
            schema: Arc::clone(&schema),
            projected_schema: Arc::clone(&schema),
            projection: None,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        };
        Ok(Arc::new(MockDataSourceExec::new(Arc::new(source))))
    }

    /// Set the limit of the files
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.fetch = limit;
        self
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
            .flat_map(|ordering| ordering.clone())
            .flat_map(|expr| collect_columns(&expr.expr))
            .find(|col| {
                fields
                    .get(col.index())
                    .map(|field| field.name() != col.name())
                    .unwrap_or(true)
            });
        if let Some(col) = ambiguous_column {
            return internal_err!(
                "Column {:?} is not found in the original schema of the MockMemorySourceConfig",
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
                .project(&projection_mapping, Arc::clone(&self.projected_schema))
                .into_oeq_class()
                .into_inner();
        }

        self.sort_information = sort_information;
        Ok(self)
    }

    /// Arc clone of ref to original schema
    pub fn original_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}


/// Iterator over batches
pub struct MockMemoryStream {
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

impl MockMemoryStream {
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
    pub(super) fn with_reservation(mut self, reservation: MemoryReservation) -> Self {
        self.reservation = Some(reservation);
        self
    }

    /// Set the number of rows to produce
    pub(super) fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }
}

impl Stream for MockMemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
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

impl RecordBatchStream for MockMemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
/// Asserts that given future is pending.
pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let poll = fut.poll_unpin(&mut cx);

    assert!(poll.is_pending());
}

/// Get the schema for the aggregate_test_* csv files
pub fn aggr_test_schema() -> SchemaRef {
    let mut f1 = Field::new("c1", DataType::Utf8, false);
    f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
    let schema = Schema::new(vec![
        f1,
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]);

    Arc::new(schema)
}

/// Returns record batch with 3 columns of i32 in memory
pub fn build_table_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap()
}

/// Returns record batch with 2 columns of i32 in memory
pub fn build_table_i32_two_cols(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
        ],
    )
    .unwrap()
}

/// Returns memory table scan wrapped around record batch with 3 columns of i32
pub fn build_table_scan_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let batch = build_table_i32(a, b, c);
    let schema = batch.schema();
    MockMemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

/// Return a RecordBatch with a single Int32 array with values (0..sz) in a field named "i"
pub fn make_partition(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from(values));
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Returns a `MockDataSourceExec` that scans `partitions` of 100 batches each
pub fn scan_partitioned(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(mem_exec(partitions))
}

/// Returns a `MockDataSourceExec` that scans `partitions` of 100 batches each
pub fn mem_exec(partitions: usize) -> MockDataSourceExec {
    let data: Vec<Vec<_>> = (0..partitions).map(|_| vec![make_partition(100)]).collect();

    let schema = data[0][0].schema();
    let projection = None;
    MockDataSourceExec::new(Arc::new(
        MockMemorySourceConfig::try_new(&data, schema, projection).unwrap(),
    ))
}

// Construct a stream partition for test purposes
#[derive(Debug)]
pub struct TestPartitionStream {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl TestPartitionStream {
    /// Create a new stream partition with the provided batches
    pub fn new_with_batches(batches: Vec<RecordBatch>) -> Self {
        let schema = batches[0].schema();
        Self { schema, batches }
    }
}
impl PartitionStream for TestPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = futures::stream::iter(self.batches.clone().into_iter().map(Ok));
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}
