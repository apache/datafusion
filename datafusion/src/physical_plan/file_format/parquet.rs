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

//! Execution plan for reading Parquet files

use std::fmt;
use std::sync::{Arc, Mutex};
use std::{any::Any, convert::TryInto};

use crate::datasource::object_store::{ChunkReader, ObjectStore, SizedFile};
use crate::datasource::PartitionedFile;
use crate::{
    error::{DataFusionError, Result},
    execution::runtime_env::RuntimeEnv,
    logical_plan::{Column, Expr},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    physical_plan::{
        file_format::FileScanConfig,
        metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
    scalar::ScalarValue,
};

use arrow::{
    array::ArrayRef,
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use log::{debug, info};
use parquet::{
    arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder},
    file::{metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics},
};

use arrow::array::new_null_array;
use fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};

use super::PartitionColumnProjector;

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate for pruning row groups
    pruning_predicate: Option<PruningPredicate>,
}

/// Stores metrics about the parquet execution for a particular parquet file
#[derive(Debug, Clone)]
struct ParquetFileMetrics {
    /// Number of times the predicate could not be evaluated
    pub predicate_evaluation_errors: metrics::Count,
    /// Number of row groups pruned using
    pub row_groups_pruned: metrics::Count,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    /// Even if `limit` is set, ParquetExec rounds up the number of records to the next `batch_size`.
    pub fn new(base_config: FileScanConfig, predicate: Option<Expr>) -> Self {
        debug!("Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
        base_config.file_groups, base_config.projection, predicate, base_config.limit);

        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let pruning_predicate = predicate.and_then(|predicate_expr| {
            match PruningPredicate::try_new(
                &predicate_expr,
                base_config.file_schema.clone(),
            ) {
                Ok(pruning_predicate) => Some(pruning_predicate),
                Err(e) => {
                    debug!(
                        "Could not create pruning predicate for {:?}: {}",
                        predicate_expr, e
                    );
                    predicate_creation_errors.add(1);
                    None
                }
            }
        });

        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
            pruning_predicate,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let row_groups_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_pruned", partition);

        Self {
            predicate_evaluation_errors,
            row_groups_pruned,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(
        &self,
        partition_index: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let files = self.base_config.file_groups[partition_index].to_vec();

        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let partition_column_projector = PartitionColumnProjector::new(
            Arc::clone(&self.projected_schema),
            &self.base_config.table_partition_cols,
        );

        let config = Arc::new(PartitionConfig {
            projection,
            projected_schema: Arc::clone(&self.projected_schema),
            object_store: Arc::clone(&self.base_config.object_store),
            partition_idx: partition_index,
            batch_size: runtime.batch_size(),
            pruning_predicate: self.pruning_predicate.clone(),
            metrics: self.metrics.clone(),
            file_schema: self.base_config.file_schema.clone(),
        });

        Ok(Box::pin(ParquetExecStream {
            partition_column_projector,
            config,
            files,
            state: Mutex::new(StreamState::Init),
            file_idx: 0,
            remaining_rows: self.base_config.limit.unwrap_or(usize::MAX),
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "ParquetExec: limit={:?}, partitions={}",
                    self.base_config.limit,
                    super::FileGroupsDisplay(&self.base_config.file_groups)
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

struct PartitionConfig {
    projected_schema: SchemaRef,

    object_store: Arc<dyn ObjectStore>,

    partition_idx: usize,

    batch_size: usize,

    file_schema: SchemaRef,

    projection: Vec<usize>,

    pruning_predicate: Option<PruningPredicate>,

    metrics: ExecutionPlanMetricsSet,
}

struct ParquetExecStream {
    config: Arc<PartitionConfig>,

    remaining_rows: usize,

    file_idx: usize,

    partition_column_projector: PartitionColumnProjector,

    files: Vec<PartitionedFile>,

    // Mutex needed because of #1614
    state: Mutex<StreamState>,
}

enum StreamState {
    Init,
    Create(BoxFuture<'static, Result<ParquetRecordBatchStream<Box<dyn ChunkReader>>>>),
    Stream(ParquetRecordBatchStream<Box<dyn ChunkReader>>),
    Error,
}

impl RecordBatchStream for ParquetExecStream {
    fn schema(&self) -> SchemaRef {
        self.config.projected_schema.clone()
    }
}

impl Stream for ParquetExecStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let mut state = this.state.lock().expect("not poisoned");

        if this.remaining_rows == 0 {
            return Poll::Ready(None);
        }

        loop {
            match &mut *state {
                StreamState::Init => {
                    let file = match this.files.get(this.file_idx) {
                        Some(file) => file.file_meta.sized_file.clone(),
                        None => return Poll::Ready(None),
                    };

                    *state = StreamState::Create(
                        create_stream(this.config.clone(), file).boxed(),
                    );
                }
                StreamState::Create(f) => match futures::ready!(f.poll_unpin(cx)) {
                    Ok(s) => {
                        *state = StreamState::Stream(s);
                    }
                    Err(e) => {
                        *state = StreamState::Error;
                        return Poll::Ready(Some(Err(ArrowError::ExternalError(
                            Box::new(e),
                        ))));
                    }
                },
                StreamState::Stream(s) => match futures::ready!(s.poll_next_unpin(cx)) {
                    Some(Ok(batch)) => {
                        this.remaining_rows =
                            this.remaining_rows.saturating_sub(batch.num_rows());

                        let proj_batch = project_batch(
                            batch,
                            &mut this.partition_column_projector,
                            &this.files[this.file_idx].partition_values,
                            this.config.file_schema.as_ref(),
                            &this.config.projection,
                        );

                        return Poll::Ready(Some(proj_batch));
                    }
                    Some(Err(e)) => {
                        *state = StreamState::Error;
                        return Poll::Ready(Some(Err(ArrowError::ExternalError(
                            Box::new(e),
                        ))));
                    }
                    None => {
                        this.file_idx += 1;
                        *state = StreamState::Init
                    }
                },
                StreamState::Error => return Poll::Pending,
            }
        }
    }
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a [RowGroupMetaData],
    parquet_schema: &'a Schema,
}

/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $func:ident, $bytes_func:ident) => {{
        if !$column_statistics.has_min_max_set() {
            return None;
        }
        match $column_statistics {
            ParquetStatistics::Boolean(s) => Some(ScalarValue::Boolean(Some(*s.$func()))),
            ParquetStatistics::Int32(s) => Some(ScalarValue::Int32(Some(*s.$func()))),
            ParquetStatistics::Int64(s) => Some(ScalarValue::Int64(Some(*s.$func()))),
            // 96 bit ints not supported
            ParquetStatistics::Int96(_) => None,
            ParquetStatistics::Float(s) => Some(ScalarValue::Float32(Some(*s.$func()))),
            ParquetStatistics::Double(s) => Some(ScalarValue::Float64(Some(*s.$func()))),
            ParquetStatistics::ByteArray(s) => {
                let s = std::str::from_utf8(s.$bytes_func())
                    .map(|s| s.to_string())
                    .ok();
                Some(ScalarValue::Utf8(s))
            }
            // type not supported yet
            ParquetStatistics::FixedLenByteArray(_) => None,
        }
    }};
}

// Extract the min or max value calling `func` or `bytes_func` on the ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $func:ident, $bytes_func:ident) => {{
        let (column_index, field) = if let Some((v, f)) = $self.parquet_schema.column_with_name(&$column.name) {
            (v, f)
        } else {
            // Named column was not present
            return None
        };

        let data_type = field.data_type();
        // The result may be None, because DataFusion doesn't have support for ScalarValues of the column type
        let null_scalar: ScalarValue = data_type.try_into().ok()?;

        let scalar_values : Vec<ScalarValue> = $self.row_group_metadata
            .iter()
            .flat_map(|meta| {
                meta.column(column_index).statistics()
            })
            .map(|stats| {
                get_statistic!(stats, $func, $bytes_func)
            })
            .map(|maybe_scalar| {
                // column either did't have statistics at all or didn't have min/max values
                maybe_scalar.unwrap_or_else(|| null_scalar.clone())
            })
            .collect();

        // ignore errors converting to arrays (e.g. different types)
        ScalarValue::iter_to_array(scalar_values).ok()
    }}
}

// Extract the null count value on the ParquetStatistics
macro_rules! get_null_count_values {
    ($self:expr, $column:expr) => {{
        let column_index =
            if let Some((v, _)) = $self.parquet_schema.column_with_name(&$column.name) {
                v
            } else {
                // Named column was not present
                return None;
            };

        let scalar_values: Vec<ScalarValue> = $self
            .row_group_metadata
            .iter()
            .flat_map(|meta| meta.column(column_index).statistics())
            .map(|stats| {
                ScalarValue::UInt64(Some(stats.null_count().try_into().unwrap()))
            })
            .collect();

        // ignore errors converting to arrays (e.g. different types)
        ScalarValue::iter_to_array(scalar_values).ok()
    }};
}

impl<'a> PruningStatistics for RowGroupPruningStatistics<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, min, min_bytes)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max, max_bytes)
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadata.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        get_null_count_values!(self, column)
    }
}

// Map projections from the schema which merges all file schemas to projections on a particular
// file
fn map_projections(
    merged_schema: &Schema,
    file_schema: &Schema,
    projections: &[usize],
) -> Result<Vec<usize>> {
    let mut mapped: Vec<usize> = vec![];
    for idx in projections {
        let field = merged_schema.field(*idx);
        if let Ok(mapped_idx) = file_schema.index_of(field.name().as_str()) {
            if file_schema.field(mapped_idx).data_type() == field.data_type() {
                mapped.push(mapped_idx)
            } else {
                let msg = format!("Failed to map column projection for field {}. Incompatible data types {:?} and {:?}", field.name(), file_schema.field(mapped_idx).data_type(), field.data_type());
                info!("{}", msg);
                return Err(DataFusionError::Execution(msg));
            }
        }
    }
    Ok(mapped)
}

fn project_batch(
    batch: RecordBatch,
    column_projector: &mut PartitionColumnProjector,
    partition_values: &[ScalarValue],
    file_schema: &Schema,
    projection: &[usize],
) -> ArrowResult<RecordBatch> {
    let total_cols = file_schema.fields().len();
    let batch_schema = batch.schema();
    let projected_schema = file_schema.clone().project(projection)?;

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(total_cols);
    let batch_cols = batch.columns().to_vec();

    for field_idx in projection {
        let merged_field = &file_schema.fields()[*field_idx];
        if let Some((batch_idx, _name)) =
            batch_schema.column_with_name(merged_field.name().as_str())
        {
            cols.push(batch_cols[batch_idx].clone());
        } else {
            cols.push(new_null_array(merged_field.data_type(), batch.num_rows()))
        }
    }

    let merged_batch = RecordBatch::try_new(Arc::new(projected_schema), cols)?;
    column_projector.project(merged_batch, partition_values)
}

async fn create_stream(
    config: Arc<PartitionConfig>,
    file: SizedFile,
) -> Result<ParquetRecordBatchStream<Box<dyn ChunkReader>>> {
    let file_metrics =
        ParquetFileMetrics::new(config.partition_idx, &file.path, &config.metrics);

    let object_reader = config.object_store.file_reader(file)?;
    let reader = object_reader.chunk_reader().await?;

    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

    let pruning_stats = RowGroupPruningStatistics {
        row_group_metadata: builder.metadata().row_groups(),
        parquet_schema: builder.schema().as_ref(),
    };

    if let Some(predicate) = &config.pruning_predicate {
        match predicate.prune(&pruning_stats) {
            Ok(predicate_values) => {
                let num_pruned = predicate_values.iter().filter(|&v| !*v).count();
                file_metrics.row_groups_pruned.add(num_pruned);

                let row_groups = predicate_values
                    .into_iter()
                    .enumerate()
                    .filter_map(|(idx, v)| match v {
                        true => Some(idx),
                        false => None,
                    })
                    .collect();

                builder = builder.with_row_groups(row_groups)
            }
            Err(e) => {
                debug!("Error evaluating row group predicate values {}", e);
                file_metrics.predicate_evaluation_errors.add(1);
            }
        }
    };

    let projection = map_projections(
        config.file_schema.as_ref(),
        builder.schema().as_ref(),
        &config.projection,
    )?;

    builder
        .with_batch_size(config.batch_size)
        .with_projection(projection)
        .build()
        .map_err(DataFusionError::ParquetError)
}

#[cfg(test)]
mod tests {
    use crate::{
        assert_batches_sorted_eq,
        datasource::{
            file_format::{parquet::ParquetFormat, FileFormat},
            object_store::local::{
                local_object_reader_stream, local_unpartitioned_file, LocalFileSystem,
            },
        },
        physical_plan::collect,
    };

    use super::*;
    use crate::physical_plan::execute_stream;
    use arrow::array::Float32Array;
    use arrow::{
        array::{Int64Array, Int8Array, StringArray},
        datatypes::{DataType, Field},
    };
    use futures::StreamExt;
    use parquet::{
        arrow::ArrowWriter,
        basic::Type as PhysicalType,
        file::{
            metadata::RowGroupMetaData, properties::WriterProperties,
            statistics::Statistics as ParquetStatistics,
        },
        schema::types::SchemaDescPtr,
    };
    use tempfile::NamedTempFile;

    // Writes the `batches` to temporary files
    fn write_files(batches: Vec<RecordBatch>) -> Vec<NamedTempFile> {
        // When vec is dropped, temp files are deleted
        batches
            .into_iter()
            .map(|batch| {
                let output = tempfile::NamedTempFile::new().expect("creating temp file");

                let props = WriterProperties::builder().build();
                let file: std::fs::File = (*output.as_file())
                    .try_clone()
                    .expect("cloning file descriptor");
                let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
                    .expect("creating writer");

                writer.write(&batch).expect("Writing batch");
                writer.close().unwrap();
                output
            })
            .collect()
    }

    async fn make_exec(
        files: &[NamedTempFile],
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
    ) -> ParquetExec {
        let file_names: Vec<_> = files
            .iter()
            .map(|t| t.path().to_string_lossy().to_string())
            .collect();

        // Now, read the files back in
        let file_groups: Vec<_> = file_names
            .iter()
            .map(|name| local_unpartitioned_file(name.clone()))
            .collect();

        // Infer the schema (if not provided)
        let file_schema = match schema {
            Some(provided_schema) => provided_schema,
            None => ParquetFormat::default()
                .infer_schema(local_object_reader_stream(file_names))
                .await
                .expect("inferring schema"),
        };

        // prepare the scan
        ParquetExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_groups: vec![file_groups],
                file_schema,
                statistics: Statistics::default(),
                projection,
                limit: None,
                table_partition_cols: vec![],
            },
            None,
        )
    }

    /// writes each RecordBatch as an individual parquet file and then
    /// reads it back in to the named location.
    async fn round_trip_to_parquet(
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
    ) -> Vec<RecordBatch> {
        let files = write_files(batches);
        let parquet_exec = make_exec(&files, projection, schema).await;
        let runtime = Arc::new(RuntimeEnv::default());
        collect(Arc::new(parquet_exec), runtime)
            .await
            .expect("reading parquet data")
    }

    // Add a new column with the specified field name to the RecordBatch
    fn add_to_batch(
        batch: &RecordBatch,
        field_name: &str,
        array: ArrayRef,
    ) -> RecordBatch {
        let mut fields = batch.schema().fields().clone();
        fields.push(Field::new(field_name, array.data_type().clone(), true));
        let schema = Arc::new(Schema::new(fields));

        let mut columns = batch.columns().to_vec();
        columns.push(array);
        RecordBatch::try_new(schema, columns).expect("error; creating record batch")
    }

    fn create_batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        columns.into_iter().fold(
            RecordBatch::new_empty(Arc::new(Schema::new(vec![]))),
            |batch, (field_name, arr)| add_to_batch(&batch, field_name, arr.clone()),
        )
    }

    #[tokio::test]
    async fn evolved_schema() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));
        // batch1: c1(string)
        let batch1 = add_to_batch(
            &RecordBatch::new_empty(Arc::new(Schema::new(vec![]))),
            "c1",
            c1,
        );

        // batch2: c1(string) and c2(int64)
        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
        let batch2 = add_to_batch(&batch1, "c2", c2);

        // batch3: c1(string) and c3(int8)
        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));
        let batch3 = add_to_batch(&batch1, "c3", c3);

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1, batch2, batch3], None, None).await;
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "|     |    |    |",
            "|     |    | 20 |",
            "|     | 2  |    |",
            "| Foo |    |    |",
            "| Foo |    | 10 |",
            "| Foo | 1  |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_inconsistent_order() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1)]);

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1, batch2], None, None).await;
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_intersection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![("c1", c1), ("c3", c3.clone())]);

        // batch2: c3(int8), c2(int64), c1(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2)]);

        // read/write them files:
        let read = round_trip_to_parquet(vec![batch1, batch2], None, None).await;
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c3 | c2 |",
            "+-----+----+----+",
            "| Foo | 10 |    |",
            "|     | 20 |    |",
            "| bar |    |    |",
            "|     | 10 | 1  |",
            "|     | 20 | 2  |",
            "|     |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_projection() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(StringArray::from(vec![Some("baz"), Some("boo"), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c3), ("c2", c2), ("c1", c1), ("c4", c4)]);

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], Some(vec![0, 3]), None).await;
        let expected = vec![
            "+-----+-----+",
            "| c1  | c4  |",
            "+-----+-----+",
            "| Foo | baz |",
            "|     | boo |",
            "| bar |     |",
            "| Foo |     |",
            "|     |     |",
            "| bar |     |",
            "+-----+-----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
    }

    #[tokio::test]
    async fn evolved_schema_incompatible_types() {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let c3: ArrayRef = Arc::new(Int8Array::from(vec![Some(10), Some(20), None]));

        let c4: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(1.0_f32), Some(2.0_f32), None]));

        // batch1: c1(string), c2(int64), c3(int8)
        let batch1 = create_batch(vec![
            ("c1", c1.clone()),
            ("c2", c2.clone()),
            ("c3", c3.clone()),
        ]);

        // batch2: c3(int8), c2(int64), c1(string), c4(string)
        let batch2 = create_batch(vec![("c3", c4), ("c2", c2), ("c1", c1)]);

        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int8, true),
        ]);

        let files = write_files(vec![batch1, batch2]);
        let plan = make_exec(&files, None, Some(Arc::new(schema))).await;
        let runtime = Arc::new(RuntimeEnv::default());
        let mut stream = execute_stream(Arc::new(plan), runtime).await.unwrap();
        let batch = stream.next().await.unwrap().unwrap();

        // First batch should read successfully
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &[batch]);

        // Second batch should error
        let err = stream.next().await.unwrap().unwrap_err().to_string();
        assert!(err.contains("Failed to map column projection for field c3. Incompatible data types Float32 and Int8"), "{}", err);
    }

    #[tokio::test]
    async fn parquet_exec_with_projection() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_groups: vec![vec![local_unpartitioned_file(filename.clone())]],
                file_schema: ParquetFormat::default()
                    .infer_schema(local_object_reader_stream(vec![filename]))
                    .await?,
                statistics: Statistics::default(),
                projection: Some(vec![0, 1, 2]),
                limit: None,
                table_partition_cols: vec![],
            },
            None,
        );
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, runtime).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn parquet_exec_with_partition() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let mut partitioned_file = local_unpartitioned_file(filename.clone());
        partitioned_file.partition_values = vec![
            ScalarValue::Utf8(Some("2021".to_owned())),
            ScalarValue::Utf8(Some("10".to_owned())),
            ScalarValue::Utf8(Some("26".to_owned())),
        ];
        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_groups: vec![vec![partitioned_file]],
                file_schema: ParquetFormat::default()
                    .infer_schema(local_object_reader_stream(vec![filename]))
                    .await?,
                statistics: Statistics::default(),
                // file has 10 cols so index 12 should be month
                projection: Some(vec![0, 1, 2, 12]),
                limit: None,
                table_partition_cols: vec![
                    "year".to_owned(),
                    "month".to_owned(),
                    "day".to_owned(),
                ],
            },
            None,
        );
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0, runtime).await?;
        let batch = results.next().await.unwrap()?;
        let expected = vec![
            "+----+----------+-------------+-------+",
            "| id | bool_col | tinyint_col | month |",
            "+----+----------+-------------+-------+",
            "| 4  | true     | 0           | 10    |",
            "| 5  | false    | 1           | 10    |",
            "| 6  | true     | 0           | 10    |",
            "| 7  | false    | 1           | 10    |",
            "| 2  | true     | 0           | 10    |",
            "| 3  | false    | 1           | 10    |",
            "| 0  | true     | 0           | 10    |",
            "| 1  | false    | 1           | 10    |",
            "+----+----------+-------------+-------+",
        ];
        crate::assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[test]
    fn row_group_pruning_predicate_simple_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate = PruningPredicate::try_new(&expr, Arc::new(schema))?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(1), Some(10), None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_stats = RowGroupPruningStatistics {
            row_group_metadata: &row_group_metadata,
            parquet_schema: pruning_predicate.schema().as_ref(),
        };

        let row_group_filter = pruning_predicate.prune(&row_group_stats).unwrap();
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[test]
    fn row_group_pruning_predicate_missing_stats() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate = PruningPredicate::try_new(&expr, Arc::new(schema))?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(None, None, None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_stats = RowGroupPruningStatistics {
            row_group_metadata: &row_group_metadata,
            parquet_schema: pruning_predicate.schema().as_ref(),
        };

        let row_group_filter = pruning_predicate.prune(&row_group_stats).unwrap();
        // missing statistics for first row group mean that the result from the predicate expression
        // is null / undefined so the first row group can't be filtered out
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    #[test]
    fn row_group_pruning_predicate_partial_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with partially supported expression
        // int > 1 and int % 2 => c1_max > 1 and true
        let expr = col("c1").gt(lit(15)).and(col("c2").modulus(lit(2)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(&expr, schema.clone())?;

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::INT32),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_stats = RowGroupPruningStatistics {
            row_group_metadata: &row_group_metadata,
            parquet_schema: pruning_predicate.schema().as_ref(),
        };

        let row_group_filter = pruning_predicate.prune(&row_group_stats).unwrap();
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(row_group_filter, vec![false, true]);

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").modulus(lit(2)));
        let pruning_predicate = PruningPredicate::try_new(&expr, schema)?;
        let row_group_stats = RowGroupPruningStatistics {
            row_group_metadata: &row_group_metadata,
            parquet_schema: pruning_predicate.schema().as_ref(),
        };

        let row_group_filter = pruning_predicate.prune(&row_group_stats).unwrap();
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    fn gen_row_group_meta_data_for_pruning_predicate() -> Vec<RowGroupMetaData> {
        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::BOOLEAN),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 1, false),
            ],
        );
        vec![rgm1, rgm2]
    }

    #[test]
    fn row_group_pruning_predicate_null_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 and IsNull(bool) => c1_max > 1 and bool_null_count > 0
        let expr = col("c1").gt(lit(15)).and(col("c2").is_null());
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(&expr, schema)?;
        let row_group_metadata = gen_row_group_meta_data_for_pruning_predicate();

        let row_group_stats = RowGroupPruningStatistics {
            row_group_metadata: &row_group_metadata,
            parquet_schema: pruning_predicate.schema().as_ref(),
        };

        let row_group_filter = pruning_predicate.prune(&row_group_stats).unwrap();
        // First row group was filtered out because it contains no null value on "c2".
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[tokio::test]
    async fn row_group_pruning_predicate_eq_null_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with an unknown (Null) expr
        //
        // int > 1 and bool = NULL => c1_max > 1 and null
        let expr = col("tinyint_col")
            .gt(lit(15))
            .and(col("bool_col").eq(lit(ScalarValue::Boolean(None))));

        let runtime = Arc::new(RuntimeEnv::default());
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);

        let parquet_exec = ParquetExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_groups: vec![vec![local_unpartitioned_file(filename.clone())]],
                file_schema: ParquetFormat::default()
                    .infer_schema(local_object_reader_stream(vec![filename]))
                    .await?,
                statistics: Statistics::default(),
                projection: Some(vec![1, 2, 3]),
                limit: None,
                table_partition_cols: vec![],
            },
            Some(expr),
        );
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        // no row group is filtered out because the predicate expression can't be evaluated
        let mut results = parquet_exec.execute(0, runtime).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["bool_col", "tinyint_col", "smallint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let metrics = parquet_exec.metrics().unwrap();
        let metric = metrics
            .iter()
            .find(|metric| metric.value().name() == "predicate_evaluation_errors")
            .unwrap();
        assert_eq!(metric.value().as_usize(), 1);

        Ok(())
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescPtr,
        column_statistics: Vec<ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = vec![];
        for (i, s) in column_statistics.iter().enumerate() {
            let column = ColumnChunkMetaData::builder(schema_descr.column(i))
                .set_statistics(s.clone())
                .build()
                .unwrap();
            columns.push(column);
        }
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr(fields: Vec<(&str, PhysicalType)>) -> SchemaDescPtr {
        use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
        let mut schema_fields = fields
            .iter()
            .map(|(n, t)| {
                Arc::new(SchemaType::primitive_type_builder(n, *t).build().unwrap())
            })
            .collect::<Vec<_>>();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut schema_fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
