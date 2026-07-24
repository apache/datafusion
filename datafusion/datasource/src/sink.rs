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

//! Execution plan for writing data to [`DataSink`]s

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{Distribution, EquivalenceProperties};
use datafusion_physical_expr_common::sort_expr::{LexRequirement, OrderingRequirements};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    InputDistributionRequirements, Partitioning, PlanProperties,
    SendableRecordBatchStream, execute_input_stream,
};

use async_trait::async_trait;
use datafusion_physical_plan::execution_plan::{EvaluationType, SchedulingType};
use futures::StreamExt;

/// Metadata about a single file produced by a [`DataSink`] write operation.
///
/// This struct is format-agnostic. The [`Self::format_metadata`] field carries
/// serialized format-specific metadata (e.g., a Parquet file footer serialized
/// via Thrift Compact Protocol).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWriteMetadata {
    /// Object-store path where the file was written.
    pub path: String,
    /// Number of rows written to this specific file.
    pub row_count: u64,
    /// Sum of compressed row group sizes in bytes.
    ///
    /// Note: this may differ slightly from the actual on-disk file size as it
    /// excludes the Parquet footer, page indexes, and other metadata overhead.
    pub byte_size: u64,
    /// Format-specific metadata serialized as bytes.
    ///
    /// For Parquet files this contains the Thrift-serialized `FileMetaData`
    /// (the same bytes found in the Parquet footer), enabling consumers to
    /// reconstruct column statistics without re-reading the file.
    ///
    /// For formats that do not produce file-level metadata this is `None`.
    pub format_metadata: Option<Bytes>,
}

/// `DataSink` implements writing streams of [`RecordBatch`]es to
/// user defined destinations.
///
/// The `Display` impl is used to format the sink for explain plan
/// output.
#[async_trait]
pub trait DataSink: Any + DisplayAs + Debug + Send + Sync {
    /// Return a snapshot of the [MetricsSet] for this
    /// [DataSink].
    ///
    /// See [ExecutionPlan::metrics()] for more details
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef;

    // TODO add desired input ordering
    // How does this sink want its input ordered?

    /// Writes the data to the sink, returns the number of values written
    ///
    /// This method will be called exactly once during each DML
    /// statement. Thus prior to return, the sink should do any commit
    /// or rollback required.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64>;

    /// Returns metadata for files written during the most recent
    /// [`Self::write_all`] call.
    ///
    /// This accessor is intended for consumers that need per-file
    /// statistics (e.g., column sizes, null counts, value bounds) after
    /// a write completes. It should be called after `write_all` returns
    /// successfully.
    ///
    /// The default implementation returns an empty vector. Implementations
    /// that collect file metadata during writes (e.g., Parquet sinks)
    /// should override this method.
    fn file_metadata(&self) -> Vec<FileWriteMetadata> {
        Vec::new()
    }
}

impl dyn DataSink {
    /// Returns true if the inner type is `T`.
    pub fn is<T: DataSink>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Returns a reference to the inner value as the type `T` if it is of that type.
    pub fn downcast_ref<T: DataSink>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }
}

/// Execution plan for writing record batches to a [`DataSink`]
///
/// Returns a single row with the number of values written
#[derive(Clone)]
pub struct DataSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Sink to which to write
    sink: Arc<dyn DataSink>,
    /// Schema describing the structure of the output data.
    count_schema: SchemaRef,
    /// Optional required sort order for output data.
    sort_order: Option<LexRequirement>,
    cache: Arc<PlanProperties>,
}

impl Debug for DataSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DataSinkExec schema: {}", self.count_schema)
    }
}

impl DataSinkExec {
    /// Create a plan to write to `sink`
    /// Note: DataSinkExec requires its input to have a single partition.
    /// If the input has multiple partitions, the physical optimizer will
    /// automatically insert a Merge-related operator to merge them.
    /// If you construct PhysicalPlan without going through the physical optimizer,
    /// you must ensure that the input has a single partition.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
        sort_order: Option<LexRequirement>,
    ) -> Self {
        let count_schema = make_count_schema();
        let cache = Self::create_schema(&input, count_schema);
        Self {
            input,
            sink,
            count_schema: make_count_schema(),
            sort_order,
            cache: Arc::new(cache),
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns insert sink
    pub fn sink(&self) -> &dyn DataSink {
        self.sink.as_ref()
    }

    /// Optional sort order for output data
    pub fn sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }

    /// Returns per-file metadata from the underlying sink, if available.
    ///
    /// This is a convenience accessor that delegates to
    /// [`DataSink::file_metadata`]. It should be called after the write
    /// operation has completed.
    pub fn file_metadata(&self) -> Vec<FileWriteMetadata> {
        self.sink.file_metadata()
    }

    fn create_schema(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .with_evaluation_type(EvaluationType::Eager)
    }
}

impl DisplayAs for DataSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DataSinkExec: sink=")?;
                self.sink.fmt_as(t, f)
            }
            DisplayFormatType::TreeRender => self.sink().fmt_as(t, f),
        }
    }
}

impl ExecutionPlan for DataSinkExec {
    fn name(&self) -> &'static str {
        "DataSinkExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time.
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        InputDistributionRequirements::new(vec![
            Distribution::SinglePartition;
            self.children().len()
        ])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // The required input ordering is set externally (e.g. by a `ListingTable`).
        // Otherwise, there is no specific requirement (i.e. `sort_order` is `None`).
        vec![self.sort_order.as_ref().cloned().map(Into::into)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Maintains ordering in the sense that the written file will reflect
        // the ordering of the input. For more context, see:
        //
        // https://github.com/apache/datafusion/pull/6354#discussion_r1195284178
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.sink),
            self.sort_order.clone(),
        )))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            partition,
            0,
            "DataSinkExec can only be called on partition 0!"
        );
        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(self.sink.schema()),
            0,
            Arc::clone(&context),
        )?;

        let count_schema = Arc::clone(&self.count_schema);
        let sink = Arc::clone(&self.sink);

        let stream = futures::stream::once(async move {
            sink.write_all(data, &context).await.map(make_count_batch)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }

    /// Returns the metrics of the underlying [DataSink]
    fn metrics(&self) -> Option<MetricsSet> {
        self.sink.metrics()
    }
}

/// Create a output record batch with a count
///
/// ```text
/// +-------+,
/// | count |,
/// +-------+,
/// | 6     |,
/// +-------+,
/// ```
fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).unwrap()
}

fn make_count_schema() -> SchemaRef {
    // Define a schema.
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[cfg(test)]
mod sink_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;

    /// A minimal DataSink that does NOT override `file_metadata`.
    /// Used to verify that the default implementation returns empty.
    #[derive(Debug)]
    struct MinimalSink {
        schema: SchemaRef,
    }

    impl MinimalSink {
        fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }
    }

    impl DisplayAs for MinimalSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MinimalSink")
        }
    }

    #[async_trait]
    impl DataSink for MinimalSink {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> Result<u64> {
            Ok(42)
        }
    }

    /// A DataSink that overrides `file_metadata`, simulating a
    /// format-specific sink (like ParquetSink) that provides metadata.
    #[derive(Debug)]
    struct MetadataProvidingSink {
        schema: SchemaRef,
    }

    impl MetadataProvidingSink {
        fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }
    }

    impl DisplayAs for MetadataProvidingSink {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MetadataProvidingSink")
        }
    }

    #[async_trait]
    impl DataSink for MetadataProvidingSink {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        async fn write_all(
            &self,
            _data: SendableRecordBatchStream,
            _context: &Arc<TaskContext>,
        ) -> Result<u64> {
            Ok(100)
        }

        fn file_metadata(&self) -> Vec<FileWriteMetadata> {
            vec![
                FileWriteMetadata {
                    path: "part-0.parquet".to_string(),
                    row_count: 60,
                    byte_size: 4096,
                    format_metadata: Some(Bytes::from_static(b"fake-metadata-0")),
                },
                FileWriteMetadata {
                    path: "part-1.parquet".to_string(),
                    row_count: 40,
                    byte_size: 3072,
                    format_metadata: Some(Bytes::from_static(b"fake-metadata-1")),
                },
            ]
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn test_context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()))
    }

    #[test]
    fn file_write_metadata_equality() {
        let a = FileWriteMetadata {
            path: "a.parquet".to_string(),
            row_count: 10,
            byte_size: 1024,
            format_metadata: None,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn file_write_metadata_without_format_metadata() {
        let meta = FileWriteMetadata {
            path: "data.csv".to_string(),
            row_count: 50,
            byte_size: 1024,
            format_metadata: None,
        };
        assert_eq!(meta.path, "data.csv");
        assert!(meta.format_metadata.is_none());
    }

    #[tokio::test]
    async fn default_file_metadata_returns_empty() {
        let schema = test_schema();
        let sink = MinimalSink::new(Arc::clone(&schema));
        let ctx = test_context();
        let data = empty_stream(schema);

        // write_all succeeds
        let count = sink.write_all(data, &ctx).await.unwrap();
        assert_eq!(count, 42);

        // default file_metadata returns empty
        let metadata = sink.file_metadata();
        assert!(metadata.is_empty());
    }

    #[test]
    fn overridden_file_metadata_returns_entries() {
        let schema = test_schema();
        let sink = MetadataProvidingSink::new(schema);

        let metadata = sink.file_metadata();
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].path, "part-0.parquet");
        assert_eq!(metadata[1].path, "part-1.parquet");
    }

    #[test]
    fn file_metadata_is_idempotent() {
        let schema = test_schema();
        let sink = MetadataProvidingSink::new(schema);

        let first = sink.file_metadata();
        let second = sink.file_metadata();
        assert_eq!(first, second);
    }

    #[test]
    fn data_sink_exec_file_metadata_delegates_to_sink() {
        let schema = test_schema();
        let sink: Arc<dyn DataSink> =
            Arc::new(MetadataProvidingSink::new(Arc::clone(&schema)));

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion_physical_plan::empty::EmptyExec::new(schema));

        let exec = DataSinkExec::new(input, sink, None);
        let metadata = exec.file_metadata();

        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].path, "part-0.parquet");
        assert_eq!(metadata[1].path, "part-1.parquet");
    }

    #[test]
    fn data_sink_exec_file_metadata_empty_for_minimal_sink() {
        let schema = test_schema();
        let sink: Arc<dyn DataSink> = Arc::new(MinimalSink::new(Arc::clone(&schema)));

        let input: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion_physical_plan::empty::EmptyExec::new(schema));

        let exec = DataSinkExec::new(input, sink, None);
        let metadata = exec.file_metadata();

        assert!(metadata.is_empty());
    }
}
