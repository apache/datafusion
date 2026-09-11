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

//! [`ParquetSink`] — DataFusion `DataSink` implementation that writes one
//! or more Parquet files to an [`ObjectStore`], optionally with parallel
//! per-column and per-row-group serialization.

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, HashMap, Result, internal_datafusion_err};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::DataSink;
#[cfg(feature = "proto")]
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::{
    ObjectWriterBuilder, SharedBuffer, get_writer_schema,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
#[cfg(feature = "proto")]
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::{
    ElapsedComputeFutureExt, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
    MetricsSet, Time,
};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType};
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn, ArrowRowGroupWriterFactory,
    ArrowWriterOptions, compute_leaves,
};
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
#[cfg(feature = "parquet_encryption")]
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::metadata::{ParquetMetaData, SortingColumn};
use parquet::file::properties::{
    DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterProperties, WriterPropertiesBuilder,
};
use parquet::file::writer::SerializedFileWriter;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::watch;

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// When writing parquet files in parallel, if the buffered Parquet data exceeds
/// this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

/// Implements [`DataSink`] for writing to a parquet file.
pub struct ParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Underlying parquet options
    parquet_options: TableParquetOptions,
    /// File metadata from successfully produced parquet files. The Mutex is only used
    /// to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, ParquetMetaData>>>,
    /// Optional sorting columns to write to Parquet metadata
    sorting_columns: Option<Vec<SortingColumn>>,
    /// Metrics for tracking write operations
    metrics: ExecutionPlanMetricsSet,
}

impl Debug for ParquetSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetSink").finish()
    }
}

impl DisplayAs for ParquetSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ParquetSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ParquetSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, parquet_options: TableParquetOptions) -> Self {
        Self {
            config,
            parquet_options,
            written: Default::default(),
            sorting_columns: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Set sorting columns for the Parquet file metadata.
    pub fn with_sorting_columns(
        mut self,
        sorting_columns: Option<Vec<SortingColumn>>,
    ) -> Self {
        self.sorting_columns = sorting_columns;
        self
    }

    /// Retrieve the file metadata for the written files, keyed to the path
    /// which may be partitioned (in the case of hive style partitioning).
    pub fn written(&self) -> HashMap<Path, ParquetMetaData> {
        self.written.lock().clone()
    }

    /// Create writer properties based upon configuration settings,
    /// including partitioning and the inclusion of arrow schema metadata.
    async fn create_writer_props(
        &self,
        runtime: &Arc<RuntimeEnv>,
        path: &Path,
    ) -> Result<WriterProperties> {
        let schema = self.config.output_schema();

        // TODO: avoid this clone in follow up PR, where the writer properties & schema
        // are calculated once on `ParquetSink::new`
        let mut parquet_opts = self.parquet_options.clone();
        if !self.parquet_options.global.skip_arrow_metadata {
            parquet_opts.arrow_schema(schema);
        }

        let mut builder = WriterPropertiesBuilder::try_from(&parquet_opts)?;

        // Set sorting columns if configured
        if let Some(ref sorting_columns) = self.sorting_columns {
            builder = builder.set_sorting_columns(Some(sorting_columns.clone()));
        }

        builder = set_writer_encryption_properties(
            builder,
            runtime,
            parquet_opts,
            schema,
            path,
        )
        .await?;
        Ok(builder.build())
    }

    /// Creates an AsyncArrowWriter which serializes a parquet file to an ObjectStore
    /// AsyncArrowWriters are used when individual parquet file serialization is not parallelized
    fn create_async_arrow_writer(
        &self,
        location: &Path,
        object_store: Arc<dyn ObjectStore>,
        context: &Arc<TaskContext>,
        parquet_props: WriterProperties,
    ) -> Result<AsyncArrowWriter<BufWriter>> {
        let buf_writer = BufWriter::with_capacity(
            object_store,
            location.clone(),
            context
                .session_config()
                .options()
                .execution
                .objectstore_writer_buffer_size,
        );
        let options = ArrowWriterOptions::new()
            .with_properties(parquet_props)
            .with_skip_arrow_metadata(self.parquet_options.global.skip_arrow_metadata);

        let writer = AsyncArrowWriter::try_new_with_options(
            buf_writer,
            get_writer_schema(&self.config),
            options,
        )?;
        Ok(writer)
    }

    /// Parquet options
    pub fn parquet_options(&self) -> &TableParquetOptions {
        &self.parquet_options
    }
}

#[cfg(feature = "parquet_encryption")]
async fn set_writer_encryption_properties(
    builder: WriterPropertiesBuilder,
    runtime: &Arc<RuntimeEnv>,
    parquet_opts: TableParquetOptions,
    schema: &Arc<Schema>,
    path: &Path,
) -> Result<WriterPropertiesBuilder> {
    if let Some(file_encryption_properties) = parquet_opts.crypto.file_encryption {
        // Encryption properties have been specified directly
        return Ok(builder.with_file_encryption_properties(Arc::new(
            FileEncryptionProperties::try_from(file_encryption_properties)?,
        )));
    } else if let Some(encryption_factory_id) = &parquet_opts.crypto.factory_id.as_ref() {
        // Encryption properties will be generated by an encryption factory
        let encryption_factory =
            runtime.parquet_encryption_factory(encryption_factory_id)?;
        let file_encryption_properties = encryption_factory
            .get_file_encryption_properties(
                &parquet_opts.crypto.factory_options,
                schema,
                path,
            )
            .await?;
        if let Some(file_encryption_properties) = file_encryption_properties {
            return Ok(
                builder.with_file_encryption_properties(file_encryption_properties)
            );
        }
    }
    Ok(builder)
}

#[cfg(not(feature = "parquet_encryption"))]
#[expect(clippy::unused_async)]
async fn set_writer_encryption_properties(
    builder: WriterPropertiesBuilder,
    _runtime: &Arc<RuntimeEnv>,
    _parquet_opts: TableParquetOptions,
    _schema: &Arc<Schema>,
    _path: &Path,
) -> Result<WriterPropertiesBuilder> {
    Ok(builder)
}

#[async_trait]
impl FileSink for ParquetSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let rows_written_counter = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("rows_written");
        // Note: bytes_written is the sum of compressed row group sizes, which
        // may differ slightly from the actual on-disk file size (excludes footer,
        // page indexes, and other Parquet metadata overhead).
        let bytes_written_counter =
            MetricBuilder::new(&self.metrics).global_bytes_counter("bytes_written");
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(0);

        let parquet_opts = &self.parquet_options;

        let mut file_write_tasks: JoinSet<
            std::result::Result<(Path, ParquetMetaData), DataFusionError>,
        > = JoinSet::new();

        let runtime = context.runtime_env();
        let parallel_options = ParallelParquetWriterOptions {
            max_parallel_row_groups: parquet_opts
                .global
                .maximum_parallel_row_group_writers,
            max_buffered_record_batches_per_stream: parquet_opts
                .global
                .maximum_buffered_record_batches_per_stream,
        };

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let parquet_props = self.create_writer_props(&runtime, &path).await?;
            // CDC requires the sequential writer: the chunker state lives in ArrowWriter
            // and persists across row groups. The parallel path bypasses ArrowWriter entirely.
            if !parquet_opts.global.allow_single_file_parallelism
                || parquet_opts.global.content_defined_chunking.enabled
            {
                let mut writer = self.create_async_arrow_writer(
                    &path,
                    Arc::clone(&object_store),
                    context,
                    parquet_props.clone(),
                )?;
                let reservation = MemoryConsumer::new(format!("ParquetSink[{path}]"))
                    .register(context.memory_pool());
                file_write_tasks.spawn(
                    async move {
                        while let Some(batch) = rx.recv().await {
                            writer.write(&batch).await?;
                            reservation.try_resize(writer.memory_size())?;
                        }
                        let parquet_meta_data = writer
                            .close()
                            .await
                            .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
                        Ok((path, parquet_meta_data))
                    }
                    .with_elapsed_compute(elapsed_compute.clone()),
                );
            } else {
                let writer = ObjectWriterBuilder::new(
                    // Parquet files as a whole are never compressed, since they
                    // manage compressed blocks themselves.
                    FileCompressionType::UNCOMPRESSED,
                    &path,
                    Arc::clone(&object_store),
                )
                .with_buffer_size(Some(
                    context
                        .session_config()
                        .options()
                        .execution
                        .objectstore_writer_buffer_size,
                ))
                .build()?;
                let ctx = ParquetFileWriteContext {
                    schema: get_writer_schema(&self.config),
                    props: Arc::new(parquet_props),
                    skip_arrow_metadata: self.parquet_options.global.skip_arrow_metadata,
                    parallel_options: Arc::new(parallel_options.clone()),
                    pool: Arc::clone(context.memory_pool()),
                };
                let encoding_time = elapsed_compute.clone();
                file_write_tasks.spawn(async move {
                    let parquet_meta_data = output_single_parquet_file_parallelized(
                        writer,
                        rx,
                        ctx,
                        encoding_time,
                    )
                    .await?;
                    Ok((path, parquet_meta_data))
                });
            }
        }

        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    let (path, parquet_meta_data) = r?;
                    let file_rows = parquet_meta_data.file_metadata().num_rows() as usize;
                    let file_bytes: usize = parquet_meta_data
                        .row_groups()
                        .iter()
                        .map(|rg| rg.compressed_size() as usize)
                        .sum();
                    rows_written_counter.add(file_rows);
                    bytes_written_counter.add(file_bytes);
                    let mut written_files = self.written.lock();
                    written_files
                        .try_insert(path.clone(), parquet_meta_data)
                        .map_err(|e| internal_datafusion_err!("duplicate entry detected for partitioned file {path}: {e}"))?;
                    drop(written_files);
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        demux_task
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

        Ok(rows_written_counter.value() as u64)
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        exec: &DataSinkExec,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        use protobuf::physical_plan_node::PhysicalPlanType;

        let input = ctx.encode_child(exec.input())?;
        let sort_order = exec.encode_sort_order(ctx)?;
        let sink = protobuf::ParquetSink::try_from(self)?;
        let node = protobuf::ParquetSinkExecNode {
            input: Some(Box::new(input)),
            sink: Some(sink),
            sink_schema: Some(exec.schema().as_ref().try_into()?),
            sort_order,
        };
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::ParquetSink(Box::new(node))),
        }))
    }
}

#[cfg(feature = "proto")]
impl TryFrom<&ParquetSink> for datafusion_proto_models::protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}

#[cfg(feature = "proto")]
impl TryFrom<&datafusion_proto_models::protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &datafusion_proto_models::protobuf::ParquetSink) -> Result<Self> {
        let config =
            FileSinkConfig::try_from(value.config.as_ref().ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "ParquetSink is missing required field 'config'"
                )
            })?)?;
        let parquet_options = value
            .parquet_options
            .as_ref()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "ParquetSink is missing required field 'parquet_options'"
                )
            })?
            .try_into()?;

        Ok(Self::new(config, parquet_options))
    }
}

#[cfg(feature = "proto")]
impl ParquetSink {
    /// Reconstructs a [`DataSinkExec`] containing a `ParquetSink` from protobuf.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;

        let sink_node = datafusion_physical_plan::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::ParquetSink,
            "ParquetSink",
        );
        let input = ctx.decode_required_child(
            sink_node.input.as_deref(),
            "ParquetSinkExecNode",
            "input",
        )?;
        let proto_sink = sink_node.sink.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ParquetSinkExecNode is missing required field 'sink'"
            )
        })?;
        let data_sink = ParquetSink::try_from(proto_sink)?;
        let sort_order = DataSinkExec::decode_sort_order(
            sink_node.sort_order.as_ref(),
            ctx,
            input.schema().as_ref(),
        )?;

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(data_sink),
            sort_order,
        )))
    }
}

/// A leaf array and its number of root records. Nested leaf arrays can have a
/// different length, so the dispatcher supplies the record count explicitly.
struct ColumnInput {
    column: ArrowLeafColumn,
    rows: usize,
}

/// A coherent snapshot published only after encoding and reserving memory.
#[derive(Clone, Copy, Debug, Default)]
struct ColumnProgress {
    rows: usize,
    bytes: usize,
}

/// Encodes one leaf column. Dropping the input channel finishes the row group.
async fn column_serializer_task(
    mut rx: Receiver<ColumnInput>,
    mut writer: ArrowColumnWriter,
    reservation: MemoryReservation,
    encoding_time: Time,
    progress: Option<watch::Sender<ColumnProgress>>,
) -> Result<(ArrowColumnWriter, MemoryReservation)> {
    let mut rows = 0;
    while let Some(input) = rx.recv().await {
        let _timer = encoding_time.timer();
        writer.write(&input.column)?;
        reservation.try_resize(writer.memory_size())?;
        if let Some(progress) = &progress {
            rows += input.rows;
            // Observation ends when the dispatcher hands this group to the
            // finalizer. The worker result still carries any encoding error.
            let _ = progress.send(ColumnProgress {
                rows,
                bytes: writer.get_estimated_total_bytes(),
            });
        }
    }
    Ok((writer, reservation))
}

type ColumnWriterTask = SpawnedTask<Result<(ArrowColumnWriter, MemoryReservation)>>;
type ColSender = Sender<ColumnInput>;

/// Settings related to writing parquet files in parallel
#[derive(Clone)]
struct ParallelParquetWriterOptions {
    max_parallel_row_groups: usize,
    max_buffered_record_batches_per_stream: usize,
}

/// Write configuration inputs shared across all parallel tasks that encode a
/// single Parquet file. These values are invariant for the duration of one file
/// write and do not change per row-group or per column.
///
/// Separating these from per-call parameters (`object_store_writer`, `data`,
/// `encoding_time`) keeps the deep parallel call chain below the argument-count
/// limit without mixing configuration with runtime state.
#[derive(Clone)]
struct ParquetFileWriteContext {
    schema: Arc<Schema>,
    props: Arc<WriterProperties>,
    skip_arrow_metadata: bool,
    parallel_options: Arc<ParallelParquetWriterOptions>,
    pool: Arc<dyn MemoryPool>,
}

/// This is the return type of calling [ArrowColumnWriter].close() on each column
/// i.e. the Vec of encoded columns which can be appended to a row group
type RBStreamSerializeResult = Result<(Vec<ArrowColumnChunk>, MemoryReservation, usize)>;

/// Sends a batch to every leaf writer, returning the index of a closed input
/// channel so its owner can join the worker and propagate the original error.
async fn send_arrays_to_col_writers(
    col_array_channels: &[ColSender],
    rb: &RecordBatch,
    schema: &Schema,
) -> Result<Option<usize>> {
    let mut next_channel = 0;
    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        for column in compute_leaves(field, array)? {
            let input = ColumnInput {
                column,
                rows: rb.num_rows(),
            };
            if col_array_channels[next_channel].send(input).await.is_err() {
                return Ok(Some(next_channel));
            }
            next_channel += 1;
        }
    }
    Ok(None)
}

/// Spawns a tokio task which joins the parallel column writer tasks,
/// and finalizes the row group
fn spawn_rg_join_and_finalize_task(
    column_writer_tasks: Vec<ColumnWriterTask>,
    rg_rows: usize,
    pool: &Arc<dyn MemoryPool>,
    encoding_time: Time,
) -> SpawnedTask<RBStreamSerializeResult> {
    let rg_reservation =
        MemoryConsumer::new("ParquetSink(SerializedRowGroupWriter)").register(pool);

    SpawnedTask::spawn(async move {
        let num_cols = column_writer_tasks.len();
        let mut finalized_rg = Vec::with_capacity(num_cols);
        for task in column_writer_tasks.into_iter() {
            let (writer, _col_reservation) = task
                .join_unwind()
                .await
                .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
            let encoded_size = writer.get_estimated_total_bytes();
            rg_reservation.grow(encoded_size);
            let _timer = encoding_time.timer();
            finalized_rg.push(writer.close()?);
        }

        Ok((finalized_rg, rg_reservation, rg_rows))
    })
}

/// Owns all tasks and feedback for a single row group. Worker results must
/// either be joined here on failure or transferred to the finalizer.
struct InProgressRowGroup {
    column_writer_handles: Vec<ColumnWriterTask>,
    col_array_channels: Vec<ColSender>,
    progress: Vec<watch::Receiver<ColumnProgress>>,
    rows: usize,
}

impl InProgressRowGroup {
    fn new(
        factory: &ArrowRowGroupWriterFactory,
        index: usize,
        ctx: &ParquetFileWriteContext,
        encoding_time: &Time,
    ) -> Result<Self> {
        let writers = factory.create_column_writers(index)?;
        let track_bytes = ctx.props.max_row_group_bytes().is_some();
        let mut group = Self {
            column_writer_handles: Vec::with_capacity(writers.len()),
            col_array_channels: Vec::with_capacity(writers.len()),
            progress: Vec::with_capacity(if track_bytes { writers.len() } else { 0 }),
            rows: 0,
        };
        for writer in writers {
            let (tx, rx) = mpsc::channel(
                ctx.parallel_options.max_buffered_record_batches_per_stream,
            );
            let progress = if track_bytes {
                let (tx, rx) = watch::channel(ColumnProgress::default());
                group.progress.push(rx);
                Some(tx)
            } else {
                None
            };
            let reservation =
                MemoryConsumer::new("ParquetSink(ArrowColumnWriter)").register(&ctx.pool);
            group
                .column_writer_handles
                .push(SpawnedTask::spawn(column_serializer_task(
                    rx,
                    writer,
                    reservation,
                    encoding_time.clone(),
                    progress,
                )));
            group.col_array_channels.push(tx);
        }
        Ok(group)
    }

    async fn write(&mut self, batch: &RecordBatch, schema: &Schema) -> Result<()> {
        if let Some(index) =
            send_arrays_to_col_writers(&self.col_array_channels, batch, schema).await?
        {
            return self.column_error(index).await;
        }
        self.rows += batch.num_rows();
        Ok(())
    }

    /// A closed worker channel is not evidence of successful shutdown. Join its
    /// task before dropping the remaining workers so the original error survives.
    async fn column_error<T>(&mut self, index: usize) -> Result<T> {
        self.col_array_channels.clear();
        let _ = self
            .column_writer_handles
            .swap_remove(index)
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
        Err(internal_datafusion_err!(
            "Parquet column writer exited before completing its input"
        ))
    }

    async fn synchronize(&mut self) -> Result<()> {
        for index in 0..self.progress.len() {
            // Drop the watch borrow before any other await.
            let closed = self.progress[index]
                .wait_for(|p| p.rows >= self.rows)
                .await
                .is_err();
            if closed {
                return self.column_error(index).await;
            }
        }
        Ok(())
    }

    /// All columns have acknowledged the current prefix in `after_write`.
    fn estimated_bytes(&self) -> usize {
        self.progress
            .iter()
            .fold(0usize, |total, rx| total.saturating_add(rx.borrow().bytes))
    }

    async fn after_write(&mut self) -> Result<()> {
        self.synchronize().await
    }

    fn finish(
        self,
        pool: &Arc<dyn MemoryPool>,
        encoding_time: &Time,
    ) -> SpawnedTask<RBStreamSerializeResult> {
        let Self {
            column_writer_handles,
            col_array_channels,
            rows,
            ..
        } = self;
        drop(col_array_channels);
        spawn_rg_join_and_finalize_task(
            column_writer_handles,
            rows,
            pool,
            encoding_time.clone(),
        )
    }
}

async fn finish_and_restart_row_group(
    group: &mut InProgressRowGroup,
    index: &mut usize,
    factory: &ArrowRowGroupWriterFactory,
    ctx: &ParquetFileWriteContext,
    encoding_time: &Time,
    serialize_tx: &Sender<SpawnedTask<RBStreamSerializeResult>>,
) -> Result<bool> {
    group.col_array_channels.clear();
    let task = spawn_rg_join_and_finalize_task(
        std::mem::take(&mut group.column_writer_handles),
        group.rows,
        &ctx.pool,
        encoding_time.clone(),
    );
    // The consumer owns the error when its output channel has closed.
    if serialize_tx.send(task).await.is_err() {
        return Ok(false);
    }
    *index += 1;
    *group = InProgressRowGroup::new(factory, *index, ctx, encoding_time)?;
    Ok(true)
}

/// Selects common root-record boundaries for the parallel leaf writers.
/// Byte decisions use the same acknowledged estimates as ArrowWriter.
fn spawn_parquet_parallel_serialization_task(
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<SpawnedTask<RBStreamSerializeResult>>,
    ctx: ParquetFileWriteContext,
    encoding_time: Time,
) -> SpawnedTask<Result<(), DataFusionError>> {
    SpawnedTask::spawn(async move {
        let max_rows = ctx
            .props
            .max_row_group_row_count()
            .unwrap_or(DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        let max_bytes = ctx.props.max_row_group_bytes();
        let mut index = 0;
        let mut group = InProgressRowGroup::new(
            &row_group_writer_factory,
            index,
            &ctx,
            &encoding_time,
        )?;
        while let Some(mut batch) = data.recv().await {
            if batch.num_rows() == 0 {
                continue;
            }
            // Preserve ArrowWriter's row-first, then byte-based recursive
            // slicing order without recursive async calls. No allocation or
            // batch slice is needed when the whole batch fits.
            let mut pending = Vec::new();
            loop {
                let row_budget = max_rows - group.rows;
                if batch.num_rows() > row_budget {
                    pending.push(batch.slice(row_budget, batch.num_rows() - row_budget));
                    batch = batch.slice(0, row_budget);
                }

                if let Some(limit) = max_bytes.filter(|_| group.rows > 0) {
                    let bytes = group.estimated_bytes();
                    let avg = bytes / group.rows;
                    // A zero integer average disables prediction, just as in
                    // ArrowWriter. The accumulated byte check still applies.
                    let byte_budget = if bytes >= limit {
                        0
                    } else {
                        (limit - bytes).checked_div(avg).unwrap_or(usize::MAX)
                    };
                    if byte_budget == 0 {
                        if !finish_and_restart_row_group(
                            &mut group,
                            &mut index,
                            &row_group_writer_factory,
                            &ctx,
                            &encoding_time,
                            &serialize_tx,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                        continue;
                    }
                    if batch.num_rows() > byte_budget {
                        pending.push(
                            batch.slice(byte_budget, batch.num_rows() - byte_budget),
                        );
                        batch = batch.slice(0, byte_budget);
                    }
                }

                group.write(&batch, &ctx.schema).await?;
                let full = if group.rows == max_rows {
                    // This boundary is already fixed. Finalization and the
                    // next group's encoding can overlap without a feedback wait.
                    true
                } else if let Some(limit) = max_bytes {
                    group.after_write().await?;
                    group.estimated_bytes() >= limit
                } else {
                    false
                };
                if full
                    && !finish_and_restart_row_group(
                        &mut group,
                        &mut index,
                        &row_group_writer_factory,
                        &ctx,
                        &encoding_time,
                        &serialize_tx,
                    )
                    .await?
                {
                    return Ok(());
                }
                match pending.pop() {
                    Some(next) => batch = next,
                    None => break,
                }
            }
        }
        if group.rows > 0 {
            let task = group.finish(&ctx.pool, &encoding_time);
            if serialize_tx.send(task).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    })
}

/// Consume RowGroups serialized by other parallel tasks and concatenate them in
/// to the final parquet file, while flushing finalized bytes to an [ObjectStore]
async fn concatenate_parallel_row_groups(
    mut parquet_writer: SerializedFileWriter<SharedBuffer>,
    merged_buff: SharedBuffer,
    mut serialize_rx: Receiver<SpawnedTask<RBStreamSerializeResult>>,
    mut object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    pool: Arc<dyn MemoryPool>,
) -> Result<ParquetMetaData> {
    let file_reservation =
        MemoryConsumer::new("ParquetSink(SerializedFileWriter)").register(&pool);

    while let Some(task) = serialize_rx.recv().await {
        let result = task.join_unwind().await;
        let (serialized_columns, rg_reservation, _cnt) =
            result.map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

        let mut rg_out = parquet_writer.next_row_group()?;
        for chunk in serialized_columns {
            chunk.append_to_row_group(&mut rg_out)?;
            rg_reservation.free();

            let mut buff_to_flush = merged_buff.buffer.try_lock().unwrap();
            file_reservation.try_resize(buff_to_flush.len())?;

            if buff_to_flush.len() > BUFFER_FLUSH_BYTES {
                object_store_writer
                    .write_all(buff_to_flush.as_slice())
                    .await?;
                buff_to_flush.clear();
                file_reservation.try_resize(buff_to_flush.len())?; // will set to zero
            }
        }
        rg_out.close()?;
    }

    let parquet_meta_data = parquet_writer.close()?;
    let final_buff = merged_buff.buffer.try_lock().unwrap();

    object_store_writer.write_all(final_buff.as_slice()).await?;
    object_store_writer.shutdown().await?;
    file_reservation.free();

    Ok(parquet_meta_data)
}

/// Parallelizes the serialization of a single parquet file, by first serializing N
/// independent RecordBatch streams in parallel to RowGroups in memory. Another
/// task then stitches these independent RowGroups together and streams this large
/// single parquet file to an ObjectStore in multiple parts.
async fn output_single_parquet_file_parallelized(
    object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    data: Receiver<RecordBatch>,
    ctx: ParquetFileWriteContext,
    encoding_time: Time,
) -> Result<ParquetMetaData> {
    let max_rowgroups = ctx.parallel_options.max_parallel_row_groups;
    // Buffer size of this channel limits maximum number of RowGroups being worked on in parallel
    let (serialize_tx, serialize_rx) =
        mpsc::channel::<SpawnedTask<RBStreamSerializeResult>>(max_rowgroups);

    let merged_buff = SharedBuffer::new(INITIAL_BUFFER_BYTES);
    let options = ArrowWriterOptions::new()
        .with_properties((*ctx.props).clone())
        .with_skip_arrow_metadata(ctx.skip_arrow_metadata);
    let writer = ArrowWriter::try_new_with_options(
        merged_buff.clone(),
        Arc::clone(&ctx.schema),
        options,
    )?;
    let (writer, row_group_writer_factory) = writer.into_serialized_writer()?;

    let pool = Arc::clone(&ctx.pool);
    let launch_serialization_task = spawn_parquet_parallel_serialization_task(
        row_group_writer_factory,
        data,
        serialize_tx,
        ctx,
        encoding_time,
    );
    let parquet_meta_data = concatenate_parallel_row_groups(
        writer,
        merged_buff,
        serialize_rx,
        object_store_writer,
        pool,
    )
    .await?;

    launch_serialization_task
        .join_unwind()
        .await
        .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
    Ok(parquet_meta_data)
}

#[cfg(test)]
mod tests;
