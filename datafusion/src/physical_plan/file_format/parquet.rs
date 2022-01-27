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

/// FIXME: https://github.com/apache/arrow-datafusion/issues/1058
use fmt::Debug;
use std::fmt;
use std::sync::Arc;
use std::{any::Any, convert::TryInto};

use crate::datasource::object_store::ObjectStore;
use crate::datasource::PartitionedFile;
use crate::field_util::SchemaExt;
use crate::record_batch::RecordBatch;
use crate::{
    error::{DataFusionError, Result},
    logical_plan::{Column, Expr},
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    physical_plan::{
        file_format::FileScanConfig,
        metrics::{self, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchReceiverStream,
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
        Statistics,
    },
    scalar::ScalarValue,
};
use arrow::{
    array::ArrayRef,
    datatypes::*,
    error::Result as ArrowResult,
    io::parquet::read::{self, RowGroupMetaData},
};
use log::{debug, info};

use parquet::statistics::{
    BinaryStatistics as ParquetBinaryStatistics,
    BooleanStatistics as ParquetBooleanStatistics,
    PrimitiveStatistics as ParquetPrimitiveStatistics,
};

use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};

use crate::execution::runtime_env::RuntimeEnv;
use async_trait::async_trait;

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

type Payload = ArrowResult<RecordBatch>;

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
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (Sender<Payload>, Receiver<Payload>) = channel(2);

        let partition = self.base_config.file_groups[partition_index].clone();
        let metrics = self.metrics.clone();
        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };
        let pruning_predicate = self.pruning_predicate.clone();
        let batch_size = runtime.batch_size();
        let limit = self.base_config.limit;
        let object_store = Arc::clone(&self.base_config.object_store);
        let partition_col_proj = PartitionColumnProjector::new(
            Arc::clone(&self.projected_schema),
            &self.base_config.table_partition_cols,
        );

        let file_schema_ref = self.base_config().file_schema.clone();
        let join_handle = task::spawn_blocking(move || {
            if let Err(e) = read_partition(
                object_store.as_ref(),
                file_schema_ref,
                partition_index,
                partition,
                metrics,
                &projection,
                &pruning_predicate,
                batch_size,
                response_tx,
                limit,
                partition_col_proj,
            ) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(RecordBatchReceiverStream::create(
            &self.projected_schema,
            response_rx,
            join_handle,
        ))
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

#[allow(dead_code)]
fn send_result(
    response_tx: &Sender<ArrowResult<RecordBatch>>,
    result: ArrowResult<RecordBatch>,
) -> Result<()> {
    // Note this function is running on its own blockng tokio thread so blocking here is ok.
    response_tx
        .blocking_send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

/// Wraps parquet statistics in a way
/// that implements [`PruningStatistics`]
struct RowGroupPruningStatistics<'a> {
    row_group_metadata: &'a [RowGroupMetaData],
    parquet_schema: &'a Schema,
}

/// Extract the min/max statistics from a `ParquetStatistics` object
macro_rules! get_statistic {
    ($column_statistics:expr, $attr:ident) => {{
        use arrow::io::parquet::read::PhysicalType;

        match $column_statistics.physical_type() {
            PhysicalType::Boolean => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetBooleanStatistics>()?;
                stats.$attr.map(|v| ScalarValue::Boolean(Some(v)))
            }
            PhysicalType::Int32 => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetPrimitiveStatistics<i32>>()?;
                stats.$attr.map(|v| ScalarValue::Int32(Some(v)))
            }
            PhysicalType::Int64 => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetPrimitiveStatistics<i64>>()?;
                stats.$attr.map(|v| ScalarValue::Int64(Some(v)))
            }
            // 96 bit ints not supported
            PhysicalType::Int96 => None,
            PhysicalType::Float => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetPrimitiveStatistics<f32>>()?;
                stats.$attr.map(|v| ScalarValue::Float32(Some(v)))
            }
            PhysicalType::Double => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetPrimitiveStatistics<f64>>()?;
                stats.$attr.map(|v| ScalarValue::Float64(Some(v)))
            }
            PhysicalType::ByteArray => {
                let stats = $column_statistics
                    .as_any()
                    .downcast_ref::<ParquetBinaryStatistics>()?;
                stats.$attr.as_ref().map(|v| {
                    ScalarValue::Utf8(std::str::from_utf8(v).map(|s| s.to_string()).ok())
                })
            }
            // type not supported yet
            PhysicalType::FixedLenByteArray(_) => None,
        }
    }};
}

// Extract the min or max value through the `attr` field from ParquetStatistics as appropriate
macro_rules! get_min_max_values {
    ($self:expr, $column:expr, $attr:ident) => {{
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
                get_statistic!(stats.as_ref().unwrap(), $attr)
            })
            .map(|maybe_scalar| {
                // column either did't have statistics at all or didn't have min/max values
                maybe_scalar.unwrap_or_else(|| null_scalar.clone())
            })
            .collect();

        // ignore errors converting to arrays (e.g. different types)
        ScalarValue::iter_to_array(scalar_values).ok().map(Arc::from)
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
        get_min_max_values!(self, column, min_value)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_min_max_values!(self, column, max_value)
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadata.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        get_null_count_values!(self, column)
    }
}

fn build_row_group_predicate(
    pruning_predicate: &PruningPredicate,
    metrics: ParquetFileMetrics,
    row_group_metadata: &[RowGroupMetaData],
) -> Box<dyn Fn(usize, &RowGroupMetaData) -> bool> {
    let parquet_schema = pruning_predicate.schema().as_ref();

    let pruning_stats = RowGroupPruningStatistics {
        row_group_metadata,
        parquet_schema,
    };
    let predicate_values = pruning_predicate.prune(&pruning_stats);

    match predicate_values {
        Ok(values) => {
            // NB: false means don't scan row group
            let num_pruned = values.iter().filter(|&v| !*v).count();
            metrics.row_groups_pruned.add(num_pruned);
            Box::new(move |i, _| values[i])
        }
        // stats filter array could not be built
        // return a closure which will not filter out any row groups
        Err(e) => {
            debug!("Error evaluating row group predicate values {}", e);
            metrics.predicate_evaluation_errors.add(1);
            Box::new(|_i, _r| true)
        }
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

#[allow(clippy::too_many_arguments)]
fn read_partition(
    object_store: &dyn ObjectStore,
    file_schema: SchemaRef,
    partition_index: usize,
    partition: Vec<PartitionedFile>,
    metrics: ExecutionPlanMetricsSet,
    projection: &[usize],
    pruning_predicate: &Option<PruningPredicate>,
    _batch_size: usize,
    response_tx: Sender<ArrowResult<RecordBatch>>,
    limit: Option<usize>,
    mut partition_column_projector: PartitionColumnProjector,
) -> Result<()> {
    for partitioned_file in partition {
        debug!("Reading file {}", &partitioned_file.file_meta.path());

        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            &*partitioned_file.file_meta.path(),
            &metrics,
        );
        let object_reader =
            object_store.file_reader(partitioned_file.file_meta.sized_file.clone())?;
        let reader = object_reader.sync_reader()?;
        let mut record_reader = read::RecordReader::try_new(
            reader,
            Some(projection.to_vec()),
            limit,
            None,
            None,
        )?;
        if let Some(pruning_predicate) = pruning_predicate {
            record_reader.set_groups_filter(Arc::new(build_row_group_predicate(
                pruning_predicate,
                file_metrics,
                &record_reader.metadata().row_groups,
            )));
        }

        let schema = record_reader.schema().clone();
        for chunk in record_reader {
            let batch = RecordBatch::new_with_chunk(&schema, chunk?);
            let proj_batch = partition_column_projector
                .project(batch, &partitioned_file.partition_values);
            response_tx
                .blocking_send(proj_batch)
                .map_err(|x| DataFusionError::Execution(format!("{}", x)))?;
        }
    }

    // finished reading files (dropping response_tx will close channel)
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::assert_batches_eq;
    use crate::datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        object_store::local::{
            local_object_reader_stream, local_unpartitioned_file, LocalFileSystem,
        },
        physical_plan::collect,
    };

    use super::*;
    use crate::field_util::FieldExt;
    use arrow::datatypes::{DataType, Field};
    use arrow::io::parquet::write::to_parquet_schema;
    use arrow::io::parquet::write::{ColumnDescriptor, SchemaDescriptor};
    use futures::StreamExt;
    use parquet::metadata::ColumnChunkMetaData;
    use parquet::statistics::Statistics as ParquetStatistics;
    use parquet_format_async_temp::RowGroup;

    /// writes each RecordBatch as an individual parquet file and then
    /// reads it back in to the named location.
    async fn round_trip_to_parquet(
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
        schema: Option<SchemaRef>,
    ) -> Vec<RecordBatch> {
        // When vec is dropped, temp files are deleted
        let files: Vec<_> = batches
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
            .collect();

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
        let parquet_exec = ParquetExec::new(
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
        );

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

        // read/write them files:
        let read =
            round_trip_to_parquet(vec![batch1, batch2], None, Some(Arc::new(schema)))
                .await;

        // expect only the first batch to be read
        let expected = vec![
            "+-----+----+----+",
            "| c1  | c2 | c3 |",
            "+-----+----+----+",
            "| Foo | 1  | 10 |",
            "|     | 2  | 20 |",
            "| bar |    |    |",
            "+-----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &read);
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
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
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
        assert_batches_eq!(expected, &[batch]);

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    fn parquet_file_metrics() -> ParquetFileMetrics {
        let metrics = Arc::new(ExecutionPlanMetricsSet::new());
        ParquetFileMetrics::new(0, "file.parquet", &metrics)
    }

    fn parquet_primitive_column_stats<T: parquet::types::NativeType>(
        column_descr: ColumnDescriptor,
        min: Option<T>,
        max: Option<T>,
        distinct: Option<i64>,
        nulls: i64,
    ) -> ParquetPrimitiveStatistics<T> {
        ParquetPrimitiveStatistics::<T> {
            descriptor: column_descr,
            min_value: min,
            max_value: max,
            null_count: Some(nulls),
            distinct_count: distinct,
        }
    }

    #[test]
    fn row_group_pruning_predicate_simple_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate =
            PruningPredicate::try_new(&expr, Arc::new(schema.clone()))?;

        let schema_descr = to_parquet_schema(&schema)?;
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![&parquet_primitive_column_stats::<i32>(
                schema_descr.column(0).clone(),
                Some(1),
                Some(10),
                None,
                0,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![&parquet_primitive_column_stats::<i32>(
                schema_descr.column(0).clone(),
                Some(11),
                Some(20),
                None,
                0,
            )],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(i, g))
            .collect::<Vec<_>>();
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[test]
    fn row_group_pruning_predicate_missing_stats() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let pruning_predicate =
            PruningPredicate::try_new(&expr, Arc::new(schema.clone()))?;

        let schema_descr = to_parquet_schema(&schema)?;
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![&parquet_primitive_column_stats::<i32>(
                schema_descr.column(0).clone(),
                None,
                None,
                None,
                0,
            )],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![&parquet_primitive_column_stats::<i32>(
                schema_descr.column(0).clone(),
                Some(11),
                Some(20),
                None,
                0,
            )],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(i, g))
            .collect::<Vec<_>>();
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

        let schema_descr = to_parquet_schema(&schema)?;
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                &parquet_primitive_column_stats::<i32>(
                    schema_descr.column(0).clone(),
                    Some(1),
                    Some(10),
                    None,
                    0,
                ),
                &parquet_primitive_column_stats::<i32>(
                    schema_descr.column(0).clone(),
                    Some(1),
                    Some(10),
                    None,
                    0,
                ),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                &parquet_primitive_column_stats::<i32>(
                    schema_descr.column(0).clone(),
                    Some(11),
                    Some(20),
                    None,
                    0,
                ),
                &parquet_primitive_column_stats::<i32>(
                    schema_descr.column(0).clone(),
                    Some(11),
                    Some(20),
                    None,
                    0,
                ),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(i, g))
            .collect::<Vec<_>>();
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(row_group_filter, vec![false, true]);

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").modulus(lit(2)));
        let pruning_predicate = PruningPredicate::try_new(&expr, schema)?;
        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(i, g))
            .collect::<Vec<_>>();
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

        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // First row group was filtered out because it contains no null value on "c2".
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[test]
    fn row_group_pruning_predicate_eq_null_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with an unknown (Null) expr
        //
        // int > 15 and bool = NULL => c1_max > 15 and null
        let expr = col("c1")
            .gt(lit(15))
            .and(col("c2").eq(lit(ScalarValue::Boolean(None))));
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]));
        let pruning_predicate = PruningPredicate::try_new(&expr, schema.clone())?;
        let row_group_metadata = gen_row_group_meta_data_for_pruning_predicate();

        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate = build_row_group_predicate(
            &pruning_predicate,
            parquet_file_metrics(),
            &row_group_metadata,
        );
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(i, g))
            .collect::<Vec<_>>();
        // no row group is filtered out because the predicate expression can't be evaluated
        // when a null array is generated for a statistics column,
        // because the null values propagate to the end result, making the predicate result undefined
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescriptor,
        column_statistics: Vec<&dyn ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::schema::types::{physical_type_to_type, ParquetType};
        use parquet_format_async_temp::{ColumnChunk, ColumnMetaData};

        let mut chunks = vec![];
        let mut columns = vec![];
        for (i, s) in column_statistics.into_iter().enumerate() {
            let column_descr = schema_descr.column(i);
            let type_ = match column_descr.type_() {
                ParquetType::PrimitiveType { physical_type, .. } => {
                    physical_type_to_type(physical_type).0
                }
                _ => {
                    panic!("Trying to write a row group of a non-physical type")
                }
            };
            let column_chunk = ColumnChunk {
                file_path: None,
                file_offset: 0,
                meta_data: Some(ColumnMetaData::new(
                    type_,
                    Vec::new(),
                    column_descr.path_in_schema().to_vec(),
                    parquet::compression::Compression::Uncompressed.into(),
                    0,
                    0,
                    0,
                    None,
                    0,
                    None,
                    None,
                    Some(parquet::statistics::serialize_statistics(s)),
                    None,
                    None,
                )),
                offset_index_offset: None,
                offset_index_length: None,
                column_index_offset: None,
                column_index_length: None,
                crypto_metadata: None,
                encrypted_column_metadata: None,
            };
            let column = ColumnChunkMetaData::try_from_thrift(
                column_descr.clone(),
                column_chunk.clone(),
            )
            .unwrap();
            columns.push(column);
            chunks.push(column_chunk);
        }
        let rg = RowGroup::new(chunks, 0, 0, None, None, None, None);
        RowGroupMetaData::try_from_thrift(schema_descr, rg).unwrap()
    }
}
