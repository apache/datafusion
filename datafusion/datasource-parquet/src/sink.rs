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
use datafusion_datasource::sink::{DataSink, FileWriteMetadata};
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::{
    ObjectWriterBuilder, SharedBuffer, get_writer_schema,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
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
                write!(f, "ParquetSink(file_groups=",)?;
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
    async fn create_async_arrow_writer(
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
        let bytes_written_counter = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Bytes)
            .global_counter("bytes_written");
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
                let mut writer = self
                    .create_async_arrow_writer(
                        &path,
                        Arc::clone(&object_store),
                        context,
                        parquet_props.clone(),
                    )
                    .await?;
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

    fn file_metadata(&self) -> Vec<FileWriteMetadata> {
        let written = self.written.lock();
        written
            .iter()
            .map(|(path, parquet_meta)| {
                let row_count = parquet_meta.file_metadata().num_rows() as u64;
                let byte_size: u64 = parquet_meta
                    .row_groups()
                    .iter()
                    .map(|rg| rg.compressed_size() as u64)
                    .sum();

                FileWriteMetadata {
                    path: path.to_string(),
                    row_count,
                    byte_size,
                    // Full Parquet metadata is accessible via ParquetSink::written()
                    // for Rust consumers. Thrift serialization for FFI consumers can
                    // be added when a concrete use case (e.g. Python via PyO3) needs it.
                    format_metadata: None,
                }
            })
            .collect()
    }
}

/// Consumes a stream of [ArrowLeafColumn] via a channel and serializes them using an [ArrowColumnWriter]
/// Once the channel is exhausted, returns the ArrowColumnWriter.
async fn column_serializer_task(
    mut rx: Receiver<ArrowLeafColumn>,
    mut writer: ArrowColumnWriter,
    reservation: MemoryReservation,
    encoding_time: Time,
) -> Result<(ArrowColumnWriter, MemoryReservation)> {
    while let Some(col) = rx.recv().await {
        let _timer = encoding_time.timer();
        writer.write(&col)?;
        reservation.try_resize(writer.memory_size())?;
    }
    Ok((writer, reservation))
}

type ColumnWriterTask = SpawnedTask<Result<(ArrowColumnWriter, MemoryReservation)>>;
type ColSender = Sender<ArrowLeafColumn>;

/// Spawns a parallel serialization task for each column
/// Returns join handles for each columns serialization task along with a send channel
/// to send arrow arrays to each serialization task.
fn spawn_column_parallel_row_group_writer(
    col_writers: Vec<ArrowColumnWriter>,
    max_buffer_size: usize,
    pool: &Arc<dyn MemoryPool>,
    encoding_time: &Time,
) -> Result<(Vec<ColumnWriterTask>, Vec<ColSender>)> {
    let num_columns = col_writers.len();

    let mut col_writer_tasks = Vec::with_capacity(num_columns);
    let mut col_array_channels = Vec::with_capacity(num_columns);
    for writer in col_writers.into_iter() {
        // Buffer size of this channel limits the number of arrays queued up for column level serialization
        let (send_array, receive_array) =
            mpsc::channel::<ArrowLeafColumn>(max_buffer_size);
        col_array_channels.push(send_array);

        let reservation =
            MemoryConsumer::new("ParquetSink(ArrowColumnWriter)").register(pool);
        let task = SpawnedTask::spawn(column_serializer_task(
            receive_array,
            writer,
            reservation,
            encoding_time.clone(),
        ));
        col_writer_tasks.push(task);
    }

    Ok((col_writer_tasks, col_array_channels))
}

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

/// Sends the ArrowArrays in passed [RecordBatch] through the channels to their respective
/// parallel column serializers.
async fn send_arrays_to_col_writers(
    col_array_channels: &[ColSender],
    rb: &RecordBatch,
    schema: Arc<Schema>,
) -> Result<()> {
    // Each leaf column has its own channel, increment next_channel for each leaf column sent.
    let mut next_channel = 0;
    for (array, field) in rb.columns().iter().zip(schema.fields()) {
        for c in compute_leaves(field, array)? {
            // Do not surface error from closed channel (means something
            // else hit an error, and the plan is shutting down).
            if col_array_channels[next_channel].send(c).await.is_err() {
                return Ok(());
            }

            next_channel += 1;
        }
    }

    Ok(())
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

/// This task coordinates the serialization of a parquet file in parallel.
/// As the query produces RecordBatches, these are written to a RowGroup
/// via parallel [ArrowColumnWriter] tasks. Once the desired max rows per
/// row group is reached, the parallel tasks are joined on another separate task
/// and sent to a concatenation task. This task immediately continues to work
/// on the next row group in parallel. So, parquet serialization is parallelized
/// across both columns and row_groups, with a theoretical max number of parallel tasks
/// given by n_columns * num_row_groups.
fn spawn_parquet_parallel_serialization_task(
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<SpawnedTask<RBStreamSerializeResult>>,
    ctx: ParquetFileWriteContext,
    encoding_time: Time,
) -> SpawnedTask<Result<(), DataFusionError>> {
    SpawnedTask::spawn(async move {
        let max_buffer_rb = ctx.parallel_options.max_buffered_record_batches_per_stream;
        let max_row_group_rows = ctx
            .props
            .max_row_group_row_count()
            .unwrap_or(DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        let mut row_group_index = 0;
        let col_writers =
            row_group_writer_factory.create_column_writers(row_group_index)?;
        let (mut column_writer_handles, mut col_array_channels) =
            spawn_column_parallel_row_group_writer(
                col_writers,
                max_buffer_rb,
                &ctx.pool,
                &encoding_time,
            )?;
        let mut current_rg_rows = 0;

        while let Some(mut rb) = data.recv().await {
            // This loop allows the "else" block to repeatedly split the RecordBatch to handle the case
            // when max_row_group_rows < execution.batch_size as an alternative to a recursive async
            // function.
            loop {
                if current_rg_rows + rb.num_rows() < max_row_group_rows {
                    send_arrays_to_col_writers(
                        &col_array_channels,
                        &rb,
                        Arc::clone(&ctx.schema),
                    )
                    .await?;
                    current_rg_rows += rb.num_rows();
                    break;
                } else {
                    let rows_left = max_row_group_rows - current_rg_rows;
                    let a = rb.slice(0, rows_left);
                    send_arrays_to_col_writers(
                        &col_array_channels,
                        &a,
                        Arc::clone(&ctx.schema),
                    )
                    .await?;

                    // Signal the parallel column writers that the RowGroup is done, join and finalize RowGroup
                    // on a separate task, so that we can immediately start on the next RG before waiting
                    // for the current one to finish.
                    drop(col_array_channels);
                    let finalize_rg_task = spawn_rg_join_and_finalize_task(
                        column_writer_handles,
                        max_row_group_rows,
                        &ctx.pool,
                        encoding_time.clone(),
                    );

                    // Do not surface error from closed channel (means something
                    // else hit an error, and the plan is shutting down).
                    if serialize_tx.send(finalize_rg_task).await.is_err() {
                        return Ok(());
                    }

                    current_rg_rows = 0;
                    rb = rb.slice(rows_left, rb.num_rows() - rows_left);

                    row_group_index += 1;
                    let col_writers = row_group_writer_factory
                        .create_column_writers(row_group_index)?;
                    (column_writer_handles, col_array_channels) =
                        spawn_column_parallel_row_group_writer(
                            col_writers,
                            max_buffer_rb,
                            &ctx.pool,
                            &encoding_time,
                        )?;
                }
            }
        }

        drop(col_array_channels);
        // Handle leftover rows as final rowgroup, which may be smaller than max_row_group_rows
        if current_rg_rows > 0 {
            let finalize_rg_task = spawn_rg_join_and_finalize_task(
                column_writer_handles,
                current_rg_rows,
                &ctx.pool,
                encoding_time.clone(),
            );

            // Do not surface error from closed channel (means something
            // else hit an error, and the plan is shutting down).
            if serialize_tx.send(finalize_rg_task).await.is_err() {
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
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::TableParquetOptions;
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_sink_config::{FileOutputMode, FileSinkConfig};
    use datafusion_datasource::sink::{DataSink, DataSinkExec};
    use datafusion_datasource::url::ListingTableUrl;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_expr::dml::InsertOp;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use object_store::local::LocalFileSystem;

    fn build_test_ctx(store_url: &ObjectStoreUrl) -> Arc<TaskContext> {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let local = Arc::new(
            LocalFileSystem::new_with_prefix(&tmp_dir)
                .expect("should create object store"),
        );

        let session = SessionConfig::default();
        let runtime = RuntimeEnv::default();
        runtime
            .object_store_registry
            .register_store(store_url.as_ref(), local);

        Arc::new(
            TaskContext::default()
                .with_session_config(session)
                .with_runtime(Arc::new(runtime)),
        )
    }

    fn create_test_sink() -> (Arc<ParquetSink>, SchemaRef, ObjectStoreUrl) {
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        let file_sink_config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: object_store_url.clone(),
            file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
            table_paths: vec![ListingTableUrl::parse("file:///tmp/test/").unwrap()],
            output_schema: Arc::clone(&schema),
            table_partition_cols: vec![],
            insert_op: InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: "parquet".into(),
            file_output_mode: FileOutputMode::Automatic,
        };

        let parquet_sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions::default(),
        ));

        (parquet_sink, schema, object_store_url)
    }

    fn make_test_batch(schema: &SchemaRef) -> RecordBatch {
        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        RecordBatch::try_new(Arc::clone(schema), vec![col_a, col_b]).unwrap()
    }

    #[test]
    fn file_metadata_empty_before_write() {
        let (sink, _schema, _url) = create_test_sink();
        assert!(
            sink.file_metadata().is_empty(),
            "file_metadata should be empty before any write"
        );
    }

    #[tokio::test]
    async fn file_metadata_populated_after_write() {
        let (sink, schema, object_store_url) = create_test_sink();
        let ctx = build_test_ctx(&object_store_url);
        let batch = make_test_batch(&schema);

        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let count = DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();
        assert_eq!(count, 3, "should have written 3 rows");

        let metadata = sink.file_metadata();
        assert_eq!(metadata.len(), 1, "should have one file metadata entry");
        assert_eq!(metadata[0].row_count, 3);
        assert!(metadata[0].byte_size > 0, "byte_size should be non-zero");
        assert!(!metadata[0].path.is_empty(), "path should be non-empty");
        assert!(metadata[0].format_metadata.is_none());
    }

    #[tokio::test]
    async fn file_metadata_count_equals_write_all_count() {
        let (sink, schema, object_store_url) = create_test_sink();
        let ctx = build_test_ctx(&object_store_url);
        let batch = make_test_batch(&schema);

        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let count = DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();

        let sum_rows: u64 = sink.file_metadata().iter().map(|f| f.row_count).sum();
        assert_eq!(
            count, sum_rows,
            "write_all count must equal sum of per-file row_counts"
        );
    }

    #[tokio::test]
    async fn file_metadata_consistent_with_written() {
        let (sink, schema, object_store_url) = create_test_sink();
        let ctx = build_test_ctx(&object_store_url);
        let batch = make_test_batch(&schema);

        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();

        let file_meta = sink.file_metadata();
        let written = sink.written();

        assert_eq!(
            file_meta.len(),
            written.len(),
            "file_metadata and written() should have same number of entries"
        );

        for fm in &file_meta {
            let path = Path::from(fm.path.as_str());
            let parquet_meta = written
                .get(&path)
                .expect("file_metadata path should exist in written()");
            assert_eq!(fm.row_count, parquet_meta.file_metadata().num_rows() as u64);

            let expected_bytes: u64 = parquet_meta
                .row_groups()
                .iter()
                .map(|rg| rg.compressed_size() as u64)
                .sum();
            assert_eq!(fm.byte_size, expected_bytes);
        }
    }

    #[tokio::test]
    async fn file_metadata_is_idempotent() {
        let (sink, schema, object_store_url) = create_test_sink();
        let ctx = build_test_ctx(&object_store_url);
        let batch = make_test_batch(&schema);

        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();

        let first = sink.file_metadata();
        let second = sink.file_metadata();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn partitioned_write_produces_multiple_metadata_entries() {
        let field_a = Field::new("a", DataType::Utf8, false);
        let field_b = Field::new("b", DataType::Utf8, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let object_store_url = ObjectStoreUrl::local_filesystem();

        let file_sink_config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: object_store_url.clone(),
            file_group: FileGroup::new(vec![PartitionedFile::new("/tmp".to_string(), 1)]),
            table_paths: vec![
                ListingTableUrl::parse("file:///tmp/partitioned/").unwrap(),
            ],
            output_schema: Arc::clone(&schema),
            table_partition_cols: vec![("a".to_string(), DataType::Utf8)],
            insert_op: InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: "parquet".into(),
            file_output_mode: FileOutputMode::Automatic,
        };

        let sink = Arc::new(ParquetSink::new(
            file_sink_config,
            TableParquetOptions::default(),
        ));

        let col_a: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "x"]));
        let col_b: ArrayRef = Arc::new(StringArray::from(vec!["one", "two", "three"]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![col_a, col_b]).unwrap();

        let ctx = build_test_ctx(&object_store_url);
        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        let count = DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();

        let metadata = sink.file_metadata();
        assert_eq!(
            metadata.len(),
            2,
            "partitioned write with 2 partition values should produce 2 files"
        );

        assert_eq!(count, 3);
        let sum_rows: u64 = metadata.iter().map(|f| f.row_count).sum();
        assert_eq!(sum_rows, 3);
    }

    /// Verifies that `DataSinkExec::file_metadata()` returns the correct
    /// metadata when the underlying `ParquetSink` has written files.
    /// This tests the `Arc<dyn DataSink>` dispatch path that real consumers use.
    #[tokio::test]
    async fn data_sink_exec_e2e_file_metadata_after_execute() {
        let (sink, schema, object_store_url) = create_test_sink();
        let ctx = build_test_ctx(&object_store_url);
        let batch = make_test_batch(&schema);

        let data: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch)]),
        ));

        // Write through the DataSink trait (same call DataSinkExec::execute makes)
        let count = DataSink::write_all(sink.as_ref(), data, &ctx)
            .await
            .unwrap();
        assert_eq!(count, 3);

        // Wrap in DataSinkExec and verify file_metadata is accessible
        // through the exec's convenience accessor (the Arc<dyn DataSink> path)
        let input: Arc<dyn datafusion_physical_plan::ExecutionPlan> = Arc::new(
            datafusion_physical_plan::empty::EmptyExec::new(Arc::clone(&schema)),
        );
        let exec = DataSinkExec::new(input, sink, None);

        let metadata = exec.file_metadata();
        assert_eq!(metadata.len(), 1, "should have one file after write");
        assert_eq!(metadata[0].row_count, 3);
        assert!(metadata[0].byte_size > 0);
        assert!(!metadata[0].path.is_empty());
    }
}
