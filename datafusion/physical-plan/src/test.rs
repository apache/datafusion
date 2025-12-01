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

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use crate::common;
use crate::execution_plan::{Boundedness, EmissionType};
use crate::memory::MemoryStream;
use crate::metrics::MetricsSet;
use crate::stream::RecordBatchStreamAdapter;
use crate::streaming::PartitionStream;
use crate::ExecutionPlan;
use crate::{DisplayAs, DisplayFormatType, PlanProperties};

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{
    assert_or_internal_err, config::ConfigOptions, project_schema, Result, Statistics,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::equivalence::{
    OrderingEquivalenceClass, ProjectionMapping,
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering, Partitioning};

use futures::{Future, FutureExt};

pub mod exec;

/// `TestMemoryExec` is a mock equivalent to [`MemorySourceConfig`] with [`ExecutionPlan`] implemented for testing.
/// i.e. It has some but not all the functionality of [`MemorySourceConfig`].
/// This implements an in-memory DataSource rather than explicitly implementing a trait.
/// It is implemented in this manner to keep relevant unit tests in place
/// while avoiding circular dependencies between `datafusion-physical-plan` and `datafusion-datasource`.
///
/// [`MemorySourceConfig`]: https://github.com/apache/datafusion/tree/main/datafusion/datasource/src/memory.rs
#[derive(Clone, Debug)]
pub struct TestMemoryExec {
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
    cache: PlanProperties,
}

impl DisplayAs for TestMemoryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "DataSourceExec: ")?;
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_sizes: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();

                let output_ordering = self
                    .sort_information
                    .first()
                    .map(|output_ordering| format!(", output_ordering={output_ordering}"))
                    .unwrap_or_default();

                let eq_properties = self.eq_properties();
                let constraints = eq_properties.constraints();
                let constraints = if constraints.is_empty() {
                    String::new()
                } else {
                    format!(", {constraints}")
                };

                let limit = self
                    .fetch
                    .map_or(String::new(), |limit| format!(", fetch={limit}"));
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
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for TestMemoryExec {
    fn name(&self) -> &'static str {
        "DataSourceExec"
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
        unimplemented!()
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.open(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        unimplemented!()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.statistics_inner()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if partition.is_some() {
            Ok(Statistics::new_unknown(&self.schema))
        } else {
            self.statistics_inner()
        }
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl TestMemoryExec {
    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(
            MemoryStream::try_new(
                self.partitions[partition].clone(),
                Arc::clone(&self.projected_schema),
                self.projection.clone(),
            )?
            .with_fetch(self.fetch),
        ))
    }

    fn compute_properties(&self) -> PlanProperties {
        PlanProperties::new(
            self.eq_properties(),
            self.output_partitioning(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(
            Arc::clone(&self.projected_schema),
            self.sort_information.clone(),
        )
    }

    fn statistics_inner(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        ))
    }

    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            cache: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(
                    Arc::clone(&projected_schema),
                    Vec::<LexOrdering>::new(),
                ),
                Partitioning::UnknownPartitioning(partitions.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            projected_schema,
            projection,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        })
    }

    /// Create a new `DataSourceExec` Equivalent plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new_exec(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Arc<TestMemoryExec>> {
        let mut source = Self::try_new(partitions, schema, projection)?;
        let cache = source.compute_properties();
        source.cache = cache;
        Ok(Arc::new(source))
    }

    // Equivalent of `DataSourceExec::new`
    pub fn update_cache(source: &Arc<TestMemoryExec>) -> TestMemoryExec {
        let cache = source.compute_properties();
        let mut source = (**source).clone();
        source.cache = cache;
        source
    }

    /// Set the limit of the files
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.fetch = limit;
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

    /// Ref to sort information
    pub fn sort_information(&self) -> &[LexOrdering] {
        &self.sort_information
    }

    /// refer to `try_with_sort_information` at MemorySourceConfig for more information.
    /// <https://github.com/apache/datafusion/tree/main/datafusion/datasource/src/memory.rs>
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
        assert_or_internal_err!(
            ambiguous_column.is_none(),
            "Column {:?} is not found in the original schema of the TestMemoryExec",
            ambiguous_column.as_ref().unwrap()
        );

        // If there is a projection on the source, we also need to project orderings
        if let Some(projection) = &self.projection {
            let base_schema = self.original_schema();
            let proj_exprs = projection.iter().map(|idx| {
                let name = base_schema.field(*idx).name();
                (Arc::new(Column::new(name, *idx)) as _, name.to_string())
            });
            let projection_mapping =
                ProjectionMapping::try_new(proj_exprs, &base_schema)?;
            let base_eqp = EquivalenceProperties::new_with_orderings(
                Arc::clone(&base_schema),
                sort_information,
            );
            let proj_eqp =
                base_eqp.project(&projection_mapping, Arc::clone(&self.projected_schema));
            let oeq_class: OrderingEquivalenceClass = proj_eqp.into();
            sort_information = oeq_class.into();
        }

        self.sort_information = sort_information;
        Ok(self)
    }

    /// Arc clone of ref to original schema
    pub fn original_schema(&self) -> SchemaRef {
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
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
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

pub fn make_partition_utf8(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end)
        .map(|i| format!("test_long_string_that_is_roughly_42_bytes_{i}"))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Utf8, true)]));
    let mut string_array = arrow::array::StringArray::from(values);
    string_array.shrink_to_fit();
    let arr = Arc::new(string_array);
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Returns a `DataSourceExec` that scans `partitions` of 100 batches each
pub fn scan_partitioned(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(mem_exec(partitions))
}

pub fn scan_partitioned_utf8(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(mem_exec_utf8(partitions))
}

/// Returns a `DataSourceExec` that scans `partitions` of 100 batches each
pub fn mem_exec(partitions: usize) -> TestMemoryExec {
    let data: Vec<Vec<_>> = (0..partitions).map(|_| vec![make_partition(100)]).collect();

    let schema = data[0][0].schema();
    let projection = None;

    TestMemoryExec::try_new(&data, schema, projection).unwrap()
}

pub fn mem_exec_utf8(partitions: usize) -> TestMemoryExec {
    let data: Vec<Vec<_>> = (0..partitions)
        .map(|_| vec![make_partition_utf8(100)])
        .collect();

    let schema = data[0][0].schema();
    let projection = None;

    TestMemoryExec::try_new(&data, schema, projection).unwrap()
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

#[cfg(test)]
macro_rules! assert_join_metrics {
    ($metrics:expr, $expected_rows:expr) => {
        assert_eq!($metrics.output_rows().unwrap(), $expected_rows);

        let elapsed_compute = $metrics
            .elapsed_compute()
            .expect("did not find elapsed_compute metric");
        let join_time = $metrics
            .sum_by_name("join_time")
            .expect("did not find join_time metric")
            .as_usize();
        let build_time = $metrics
            .sum_by_name("build_time")
            .expect("did not find build_time metric")
            .as_usize();
        // ensure join_time and build_time are considered in elapsed_compute
        assert!(
            join_time + build_time <= elapsed_compute,
            "join_time ({}) + build_time ({}) = {} was <= elapsed_compute = {}",
            join_time,
            build_time,
            join_time + build_time,
            elapsed_compute
        );
    };
}
#[cfg(test)]
pub(crate) use assert_join_metrics;
