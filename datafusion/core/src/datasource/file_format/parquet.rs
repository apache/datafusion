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

//! Parquet format abstractions

use parquet::column::writer::ColumnCloseResult;
use parquet::file::writer::SerializedFileWriter;
use rand::distributions::DistString;
use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle, JoinSet};

use crate::datasource::file_format::file_compression_type::FileCompressionType;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::{Fields, Schema};
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use datafusion_common::{exec_err, not_impl_err, plan_err, DataFusionError, FileType};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::{StreamExt, TryStreamExt};
use hashbrown::HashMap;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::{parquet_to_arrow_schema, AsyncArrowWriter};
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics as ParquetStatistics;
use rand::distributions::Alphanumeric;

use super::write::{create_writer, AbortableWrite, FileWriterMode};
use super::FileFormat;
use super::FileScanConfig;
use crate::arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
};
use crate::arrow::datatypes::DataType;
use crate::config::ConfigOptions;

use crate::datasource::physical_plan::{
    FileGroupDisplay, FileMeta, FileSinkConfig, ParquetExec, SchemaAdapter,
};

use crate::datasource::{create_max_min_accs, get_col_stats};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::insert::{DataSink, FileSinkExec};
use crate::physical_plan::{
    Accumulator, DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
    Statistics,
};

/// The number of files to read in parallel when inferring schema
const SCHEMA_INFERENCE_CONCURRENCY: usize = 32;

/// The Apache Parquet `FileFormat` implementation
///
/// Note it is recommended these are instead configured on the [`ConfigOptions`]
/// associated with the [`SessionState`] instead of overridden on a format-basis
///
/// TODO: Deprecate and remove overrides
/// <https://github.com/apache/arrow-datafusion/issues/4349>
#[derive(Debug, Default)]
pub struct ParquetFormat {
    /// Override the global setting for `enable_pruning`
    enable_pruning: Option<bool>,
    /// Override the global setting for `metadata_size_hint`
    metadata_size_hint: Option<usize>,
    /// Override the global setting for `skip_metadata`
    skip_metadata: Option<bool>,
}

impl ParquetFormat {
    /// Construct a new Format with no local overrides
    pub fn new() -> Self {
        Self::default()
    }

    /// Activate statistics based row group level pruning
    /// - If `None`, defaults to value on `config_options`
    pub fn with_enable_pruning(mut self, enable: Option<bool>) -> Self {
        self.enable_pruning = enable;
        self
    }

    /// Return `true` if pruning is enabled
    pub fn enable_pruning(&self, config_options: &ConfigOptions) -> bool {
        self.enable_pruning
            .unwrap_or(config_options.execution.parquet.pruning)
    }

    /// Provide a hint to the size of the file metadata. If a hint is provided
    /// the reader will try and fetch the last `size_hint` bytes of the parquet file optimistically.
    /// Without a hint, two read are required. One read to fetch the 8-byte parquet footer and then
    /// another read to fetch the metadata length encoded in the footer.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_metadata_size_hint(mut self, size_hint: Option<usize>) -> Self {
        self.metadata_size_hint = size_hint;
        self
    }

    /// Return the metadata size hint if set
    pub fn metadata_size_hint(&self, config_options: &ConfigOptions) -> Option<usize> {
        let hint = config_options.execution.parquet.metadata_size_hint;
        self.metadata_size_hint.or(hint)
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_skip_metadata(mut self, skip_metadata: Option<bool>) -> Self {
        self.skip_metadata = skip_metadata;
        self
    }

    /// Returns `true` if schema metadata will be cleared prior to
    /// schema merging.
    pub fn skip_metadata(&self, config_options: &ConfigOptions) -> bool {
        self.skip_metadata
            .unwrap_or(config_options.execution.parquet.skip_metadata)
    }
}

/// Clears all metadata (Schema level and field level) on an iterator
/// of Schemas
fn clear_metadata(
    schemas: impl IntoIterator<Item = Schema>,
) -> impl Iterator<Item = Schema> {
    schemas.into_iter().map(|schema| {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                field.as_ref().clone().with_metadata(Default::default()) // clear meta
            })
            .collect::<Fields>();
        Schema::new(fields)
    })
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| fetch_schema(store.as_ref(), object, self.metadata_size_hint))
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(SCHEMA_INFERENCE_CONCURRENCY)
            .try_collect()
            .await?;

        let schema = if self.skip_metadata(state.config_options()) {
            Schema::try_merge(clear_metadata(schemas))
        } else {
            Schema::try_merge(schemas)
        }?;

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let stats = fetch_statistics(
            store.as_ref(),
            table_schema,
            object,
            self.metadata_size_hint,
        )
        .await?;
        Ok(stats)
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = self
            .enable_pruning(state.config_options())
            .then(|| filters.cloned())
            .flatten();

        Ok(Arc::new(ParquetExec::new(
            conf,
            predicate,
            self.metadata_size_hint(state.config_options()),
        )))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return not_impl_err!("Overwrites are not implemented yet for Parquet");
        }

        let sink_schema = conf.output_schema().clone();
        let sink = Arc::new(ParquetSink::new(conf));

        Ok(Arc::new(FileSinkExec::new(input, sink, sink_schema)) as _)
    }

    fn file_type(&self) -> FileType {
        FileType::PARQUET
    }
}

fn summarize_min_max(
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
    fields: &Fields,
    i: usize,
    stat: &ParquetStatistics,
) {
    match stat {
        ParquetStatistics::Boolean(s) => {
            if let DataType::Boolean = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(BooleanArray::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(BooleanArray::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Int32(s) => {
            if let DataType::Int32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Int32Array::from_value(
                            *s.max(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Int32Array::from_value(
                            *s.min(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Int64(s) => {
            if let DataType::Int64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Int64Array::from_value(
                            *s.max(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Int64Array::from_value(
                            *s.min(),
                            1,
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Float(s) => {
            if let DataType::Float32 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Float32Array::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Float32Array::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        ParquetStatistics::Double(s) => {
            if let DataType::Float64 = fields[i].data_type() {
                if s.has_min_max_set() {
                    if let Some(max_value) = &mut max_values[i] {
                        match max_value.update_batch(&[Arc::new(Float64Array::from(
                            vec![Some(*s.max())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                    if let Some(min_value) = &mut min_values[i] {
                        match min_value.update_batch(&[Arc::new(Float64Array::from(
                            vec![Some(*s.min())],
                        ))]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                    return;
                }
            }
            max_values[i] = None;
            min_values[i] = None;
        }
        _ => {
            max_values[i] = None;
            min_values[i] = None;
        }
    }
}

/// Fetches parquet metadata from ObjectStore for given object
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
///
/// [`ParquetFileReaderFactory`]: crate::datasource::physical_plan::ParquetFileReaderFactory
pub async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    size_hint: Option<usize>,
) -> Result<ParquetMetaData> {
    if meta.size < 8 {
        return exec_err!("file size of {} is less than footer", meta.size);
    }

    // If a size hint is provided, read more than the minimum size
    // to try and avoid a second fetch.
    let footer_start = if let Some(size_hint) = size_hint {
        meta.size.saturating_sub(size_hint)
    } else {
        meta.size - 8
    };

    let suffix = store
        .get_range(&meta.location, footer_start..meta.size)
        .await?;

    let suffix_len = suffix.len();

    let mut footer = [0; 8];
    footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);

    let length = decode_footer(&footer)?;

    if meta.size < length + 8 {
        return exec_err!(
            "file size of {} is less than footer + metadata {}",
            meta.size,
            length + 8
        );
    }

    // Did not fetch the entire file metadata in the initial read, need to make a second request
    if length > suffix_len - 8 {
        let metadata_start = meta.size - length - 8;
        let remaining_metadata = store
            .get_range(&meta.location, metadata_start..footer_start)
            .await?;

        let mut metadata = BytesMut::with_capacity(length);

        metadata.put(remaining_metadata.as_ref());
        metadata.put(&suffix[..suffix_len - 8]);

        Ok(decode_metadata(metadata.as_ref())?)
    } else {
        let metadata_start = meta.size - length - 8;

        Ok(decode_metadata(
            &suffix[metadata_start - footer_start..suffix_len - 8],
        )?)
    }
}

/// Read and parse the schema of the Parquet file at location `path`
async fn fetch_schema(
    store: &dyn ObjectStore,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Schema> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    let file_metadata = metadata.file_metadata();
    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    Ok(schema)
}

/// Read and parse the statistics of the Parquet file at location `path`
async fn fetch_statistics(
    store: &dyn ObjectStore,
    table_schema: SchemaRef,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Statistics> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    let file_metadata = metadata.file_metadata();

    let file_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    let num_fields = table_schema.fields().len();
    let fields = table_schema.fields();

    let mut num_rows = 0;
    let mut total_byte_size = 0;
    let mut null_counts = vec![0; num_fields];
    let mut has_statistics = false;

    let schema_adapter = SchemaAdapter::new(table_schema.clone());

    let (mut max_values, mut min_values) = create_max_min_accs(&table_schema);

    for row_group_meta in metadata.row_groups() {
        num_rows += row_group_meta.num_rows();
        total_byte_size += row_group_meta.total_byte_size();

        let mut column_stats: HashMap<usize, (u64, &ParquetStatistics)> = HashMap::new();

        for (i, column) in row_group_meta.columns().iter().enumerate() {
            if let Some(stat) = column.statistics() {
                has_statistics = true;
                column_stats.insert(i, (stat.null_count(), stat));
            }
        }

        if has_statistics {
            for (table_idx, null_cnt) in null_counts.iter_mut().enumerate() {
                if let Some(file_idx) =
                    schema_adapter.map_column_index(table_idx, &file_schema)
                {
                    if let Some((null_count, stats)) = column_stats.get(&file_idx) {
                        *null_cnt += *null_count as usize;
                        summarize_min_max(
                            &mut max_values,
                            &mut min_values,
                            fields,
                            table_idx,
                            stats,
                        )
                    } else {
                        // If none statistics of current column exists, set the Max/Min Accumulator to None.
                        max_values[table_idx] = None;
                        min_values[table_idx] = None;
                    }
                } else {
                    *null_cnt += num_rows as usize;
                }
            }
        }
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &table_schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows as usize),
        total_byte_size: Some(total_byte_size as usize),
        column_statistics: column_stats,
        is_exact: true,
    };

    Ok(statistics)
}

/// Implements [`DataSink`] for writing to a parquet file.
struct ParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
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
                write!(
                    f,
                    "ParquetSink(writer_mode={:?}, file_groups=",
                    self.config.writer_mode
                )?;
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl ParquetSink {
    fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }

    /// Creates an AsyncArrowWriter which serializes a parquet file to an ObjectStore
    /// AsyncArrowWriters are used when individual parquet file serialization is not parallelized
    async fn create_async_arrow_writer(
        &self,
        file_meta: FileMeta,
        object_store: Arc<dyn ObjectStore>,
        parquet_props: WriterProperties,
    ) -> Result<
        AsyncArrowWriter<Box<dyn tokio::io::AsyncWrite + std::marker::Send + Unpin>>,
    > {
        let object = &file_meta.object_meta;
        match self.config.writer_mode {
            FileWriterMode::Append => {
                plan_err!(
                    "Appending to Parquet files is not supported by the file format!"
                )
            }
            FileWriterMode::Put => {
                not_impl_err!("FileWriterMode::Put is not implemented for ParquetSink")
            }
            FileWriterMode::PutMultipart => {
                let (_, multipart_writer) = object_store
                    .put_multipart(&object.location)
                    .await
                    .map_err(DataFusionError::ObjectStore)?;
                let writer = AsyncArrowWriter::try_new(
                    multipart_writer,
                    self.config.output_schema.clone(),
                    10485760,
                    Some(parquet_props),
                )?;
                Ok(writer)
            }
        }
    }

    /// Creates an AsyncArrowWriter for each partition to be written out
    /// AsyncArrowWriters are used when individual parquet file serialization is not parallelized
    async fn create_all_async_arrow_writers(
        &self,
        num_partitions: usize,
        parquet_props: &WriterProperties,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<
        Vec<AsyncArrowWriter<Box<dyn tokio::io::AsyncWrite + std::marker::Send + Unpin>>>,
    > {
        // Construct writer for each file group
        let mut writers = vec![];
        match self.config.writer_mode {
            FileWriterMode::Append => {
                return plan_err!(
                    "Parquet format does not support appending to existing file!"
                )
            }
            FileWriterMode::Put => {
                return not_impl_err!("Put Mode is not implemented for ParquetSink yet")
            }
            FileWriterMode::PutMultipart => {
                // Currently assuming only 1 partition path (i.e. not hive-style partitioning on a column)
                let base_path = &self.config.table_paths[0];
                match self.config.single_file_output {
                    false => {
                        // Uniquely identify this batch of files with a random string, to prevent collisions overwriting files
                        let write_id =
                            Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
                        for part_idx in 0..num_partitions {
                            let file_path = base_path
                                .prefix()
                                .child(format!("{}_{}.parquet", write_id, part_idx));
                            let object_meta = ObjectMeta {
                                location: file_path,
                                last_modified: chrono::offset::Utc::now(),
                                size: 0,
                                e_tag: None,
                            };
                            let writer = self
                                .create_async_arrow_writer(
                                    object_meta.into(),
                                    object_store.clone(),
                                    parquet_props.clone(),
                                )
                                .await?;
                            writers.push(writer);
                        }
                    }
                    true => {
                        let file_path = base_path.prefix();
                        let object_meta = ObjectMeta {
                            location: file_path.clone(),
                            last_modified: chrono::offset::Utc::now(),
                            size: 0,
                            e_tag: None,
                        };
                        let writer = self
                            .create_async_arrow_writer(
                                object_meta.into(),
                                object_store.clone(),
                                parquet_props.clone(),
                            )
                            .await?;
                        writers.push(writer);
                    }
                }
            }
        }

        Ok(writers)
    }

    /// Creates an object store writer for each output partition
    /// This is used when parallelizing individual parquet file writes.
    async fn create_object_store_writers(
        &self,
        num_partitions: usize,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Vec<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>>> {
        let mut writers = Vec::new();

        for _ in 0..num_partitions {
            let file_path = self.config.table_paths[0].prefix();
            let object_meta = ObjectMeta {
                location: file_path.clone(),
                last_modified: chrono::offset::Utc::now(),
                size: 0,
                e_tag: None,
            };
            writers.push(
                create_writer(
                    FileWriterMode::PutMultipart,
                    FileCompressionType::UNCOMPRESSED,
                    object_meta.into(),
                    object_store.clone(),
                )
                .await?,
            );
        }

        Ok(writers)
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    async fn write_all(
        &self,
        mut data: Vec<SendableRecordBatchStream>,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = data.len();
        let parquet_props = self
            .config
            .file_type_writer_options
            .try_into_parquet()?
            .writer_options();

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        let mut row_count = 0;

        let allow_single_file_parallelism = context
            .session_config()
            .options()
            .execution
            .parquet
            .allow_single_file_parallelism;

        match self.config.single_file_output {
            false => {
                let writers = self
                    .create_all_async_arrow_writers(
                        num_partitions,
                        parquet_props,
                        object_store.clone(),
                    )
                    .await?;
                // TODO parallelize individual parquet serialization when already outputting multiple parquet files
                // e.g. if outputting 2 parquet files on a system with 32 threads, spawn 16 tasks for each individual
                // file to be serialized.
                row_count = output_multiple_parquet_files(writers, data).await?;
            }
            true => {
                if !allow_single_file_parallelism || data.len() <= 1 {
                    let mut writer = self
                        .create_all_async_arrow_writers(
                            num_partitions,
                            parquet_props,
                            object_store.clone(),
                        )
                        .await?
                        .remove(0);
                    for data_stream in data.iter_mut() {
                        while let Some(batch) = data_stream.next().await.transpose()? {
                            row_count += batch.num_rows();
                            writer.write(&batch).await?;
                        }
                    }

                    writer.close().await?;
                } else {
                    let object_store_writer = self
                        .create_object_store_writers(1, object_store)
                        .await?
                        .remove(0);
                    row_count = output_single_parquet_file_parallelized(
                        object_store_writer,
                        data,
                        self.config.output_schema.clone(),
                        parquet_props,
                    )
                    .await?;
                }
            }
        }

        Ok(row_count as u64)
    }
}

/// This is the return type when joining subtasks which are serializing parquet files
/// into memory buffers. The first part of the tuple is the parquet bytes and the
/// second is how many rows were written into the file.
type ParquetFileSerializedResult = Result<(Vec<u8>, usize), DataFusionError>;

/// Parallelizes the serialization of a single parquet file, by first serializing N
/// independent RecordBatch streams in parallel to parquet files in memory. Another
/// task then stitches these independent files back together and streams this large
/// single parquet file to an ObjectStore in multiple parts.
async fn output_single_parquet_file_parallelized(
    mut object_store_writer: AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>,
    mut data: Vec<SendableRecordBatchStream>,
    output_schema: Arc<Schema>,
    parquet_props: &WriterProperties,
) -> Result<usize> {
    let mut row_count = 0;
    let parallelism = data.len();
    let mut join_handles: Vec<JoinHandle<ParquetFileSerializedResult>> =
        Vec::with_capacity(parallelism);
    for _ in 0..parallelism {
        let buffer: Vec<u8> = Vec::new();
        let mut writer = parquet::arrow::arrow_writer::ArrowWriter::try_new(
            buffer,
            output_schema.clone(),
            Some(parquet_props.clone()),
        )?;
        let mut data_stream = data.remove(0);
        join_handles.push(tokio::spawn(async move {
            let mut inner_row_count = 0;
            while let Some(batch) = data_stream.next().await.transpose()? {
                inner_row_count += batch.num_rows();
                writer.write(&batch)?;
            }
            let out = writer.into_inner()?;
            Ok((out, inner_row_count))
        }))
    }

    let mut writer = None;
    let endpoints: (UnboundedSender<Vec<u8>>, UnboundedReceiver<Vec<u8>>) =
        tokio::sync::mpsc::unbounded_channel();
    let (tx, mut rx) = endpoints;
    let writer_join_handle: JoinHandle<
        Result<
            AbortableWrite<Box<dyn tokio::io::AsyncWrite + std::marker::Send + Unpin>>,
            DataFusionError,
        >,
    > = tokio::task::spawn(async move {
        while let Some(data) = rx.recv().await {
            object_store_writer.write_all(data.as_slice()).await?;
        }
        Ok(object_store_writer)
    });
    let merged_buff = SharedBuffer::new(1048576);
    for handle in join_handles {
        let join_result = handle.await;
        match join_result {
            Ok(result) => {
                let (out, num_rows) = result?;
                let reader = bytes::Bytes::from(out);
                row_count += num_rows;
                //let reader = File::open(buffer)?;
                let metadata = parquet::file::footer::parse_metadata(&reader)?;
                let schema = metadata.file_metadata().schema();
                writer = match writer {
                    Some(writer) => Some(writer),
                    None => Some(SerializedFileWriter::new(
                        merged_buff.clone(),
                        Arc::new(schema.clone()),
                        Arc::new(parquet_props.clone()),
                    )?),
                };

                match &mut writer{
                    Some(w) => {
                        // Note: cannot use .await within this loop as RowGroupMetaData is not Send
                        // Instead, use a non-blocking channel to send bytes to separate worker
                        // which will write to ObjectStore.
                        for rg in metadata.row_groups() {
                            let mut rg_out = w.next_row_group()?;
                            for column in rg.columns() {
                                let result = ColumnCloseResult {
                                    bytes_written: column.compressed_size() as _,
                                    rows_written: rg.num_rows() as _,
                                    metadata: column.clone(),
                                    bloom_filter: None,
                                    column_index: None,
                                    offset_index: None,
                                };
                                rg_out.append_column(&reader, result)?;
                                let mut buff_to_flush = merged_buff.buffer.try_lock().unwrap();
                                if buff_to_flush.len() > 1024000{
                                    let bytes: Vec<u8> = buff_to_flush.drain(..).collect();
                                    tx.send(bytes).map_err(|_| DataFusionError::Execution("Failed to send bytes to ObjectStore writer".into()))?;

                                }
                            }
                            rg_out.close()?;
                            let mut buff_to_flush = merged_buff.buffer.try_lock().unwrap();
                            if buff_to_flush.len() > 1024000{
                                let bytes: Vec<u8> = buff_to_flush.drain(..).collect();
                                tx.send(bytes).map_err(|_| DataFusionError::Execution("Failed to send bytes to ObjectStore writer".into()))?;
                            }
                        }
                    },
                    None => unreachable!("Parquet writer should always be initialized in first iteration of loop!")
                }
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
    let inner_writer = writer.unwrap().into_inner()?;
    let final_buff = inner_writer.buffer.try_lock().unwrap();

    // Explicitly drop tx to signal to rx we are done sending data
    drop(tx);

    let mut object_store_writer = match writer_join_handle.await {
        Ok(r) => r?,
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic())
            } else {
                unreachable!()
            }
        }
    };
    object_store_writer.write_all(final_buff.as_slice()).await?;
    object_store_writer.shutdown().await?;
    println!("done!");

    Ok(row_count)
}

/// Serializes multiple parquet files independently in parallel from different RecordBatch streams.
/// AsyncArrowWriter is used to coordinate serialization and MultiPart puts to ObjectStore
/// Only a single CPU thread is used to serialize each individual parquet file, so write speed and overall
/// CPU utilization is dependent on the number of output files.
async fn output_multiple_parquet_files(
    writers: Vec<
        AsyncArrowWriter<Box<dyn tokio::io::AsyncWrite + std::marker::Send + Unpin>>,
    >,
    data: Vec<SendableRecordBatchStream>,
) -> Result<usize> {
    let mut row_count = 0;
    let mut join_set: JoinSet<Result<usize, DataFusionError>> = JoinSet::new();
    for (mut data_stream, mut writer) in data.into_iter().zip(writers.into_iter()) {
        join_set.spawn(async move {
            let mut cnt = 0;
            while let Some(batch) = data_stream.next().await.transpose()? {
                cnt += batch.num_rows();
                writer.write(&batch).await?;
            }
            writer.close().await?;
            Ok(cnt)
        });
    }
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => {
                row_count += res?;
            } // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(row_count)
}

/// A buffer with interior mutability shared by the SerializedFileWriter and
/// ObjectStore writer
#[derive(Clone)]
struct SharedBuffer {
    /// The inner buffer for reading and writing
    ///
    /// The lock is used to obtain internal mutability, so no worry about the
    /// lock contention.
    buffer: Arc<futures::lock::Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(futures::lock::Mutex::new(Vec::with_capacity(capacity))),
        }
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::write(&mut *buffer, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut buffer = self.buffer.try_lock().unwrap();
        Write::flush(&mut *buffer)
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::test::object_store::local_unpartitioned_file;
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::NamedTempFile;

    /// How many rows per page should be written
    const ROWS_PER_PAGE: usize = 2;

    /// Writes `batches` to a temporary parquet file
    ///
    /// If multi_page is set to `true`, the parquet file(s) are written
    /// with 2 rows per data page (used to test page filtering and
    /// boundaries).
    pub async fn store_parquet(
        batches: Vec<RecordBatch>,
        multi_page: bool,
    ) -> Result<(Vec<ObjectMeta>, Vec<NamedTempFile>)> {
        // Each batch writes to their own file
        let files: Vec<_> = batches
            .into_iter()
            .map(|batch| {
                let mut output = NamedTempFile::new().expect("creating temp file");

                let builder = WriterProperties::builder();
                let props = if multi_page {
                    builder.set_data_page_row_count_limit(ROWS_PER_PAGE)
                } else {
                    builder
                }
                .build();

                let mut writer =
                    ArrowWriter::try_new(&mut output, batch.schema(), Some(props))
                        .expect("creating writer");

                if multi_page {
                    // write in smaller batches as the parquet writer
                    // only checks datapage size limits on the boundaries of each batch
                    write_in_chunks(&mut writer, &batch, ROWS_PER_PAGE);
                } else {
                    writer.write(&batch).expect("Writing batch");
                };
                writer.close().unwrap();
                output
            })
            .collect();

        let meta: Vec<_> = files.iter().map(local_unpartitioned_file).collect();
        Ok((meta, files))
    }

    //// write batches chunk_size rows at a time
    fn write_in_chunks<W: std::io::Write + Send>(
        writer: &mut ArrowWriter<W>,
        batch: &RecordBatch,
        chunk_size: usize,
    ) {
        let mut i = 0;
        while i < batch.num_rows() {
            let num = chunk_size.min(batch.num_rows() - i);
            writer.write(&batch.slice(i, num)).unwrap();
            i += num;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use crate::physical_plan::collect;
    use std::fmt::{Display, Formatter};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    use crate::datasource::file_format::parquet::test_util::store_parquet;
    use crate::physical_plan::metrics::MetricValue;
    use crate::prelude::{SessionConfig, SessionContext};
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use bytes::Bytes;
    use datafusion_common::cast::{
        as_binary_array, as_boolean_array, as_float32_array, as_float64_array,
        as_int32_array, as_timestamp_nanosecond_array,
    };
    use datafusion_common::ScalarValue;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use log::error;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{GetOptions, GetResult, ListResult, MultipartId};
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex};
    use parquet::file::page_index::index::Index;
    use tokio::fs::File;
    use tokio::io::AsyncWrite;

    #[tokio::test]
    async fn read_merged_batches() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        let store = Arc::new(LocalFileSystem::new()) as _;
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default();
        let schema = format.infer_schema(&ctx, &store, &meta).await.unwrap();

        let stats =
            fetch_statistics(store.as_ref(), schema.clone(), &meta[0], None).await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

        let stats = fetch_statistics(store.as_ref(), schema, &meta[1], None).await?;
        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(3));
        assert_eq!(c2_stats.null_count, Some(1));
        assert_eq!(c2_stats.max_value, Some(ScalarValue::Int64(Some(2))));
        assert_eq!(c2_stats.min_value, Some(ScalarValue::Int64(Some(1))));

        Ok(())
    }

    #[derive(Debug)]
    struct RequestCountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        request_count: AtomicUsize,
    }

    impl Display for RequestCountingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "RequestCounting({})", self.inner)
        }
    }

    impl RequestCountingObjectStore {
        pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                request_count: Default::default(),
            }
        }

        pub fn request_count(&self) -> usize {
            self.request_count.load(Ordering::SeqCst)
        }

        pub fn upcast(self: &Arc<Self>) -> Arc<dyn ObjectStore> {
            self.clone()
        }
    }

    #[async_trait]
    impl ObjectStore for RequestCountingObjectStore {
        async fn put(&self, _location: &Path, _bytes: Bytes) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)>
        {
            Err(object_store::Error::NotImplemented)
        }

        async fn abort_multipart(
            &self,
            _location: &Path,
            _multipart_id: &MultipartId,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Err(object_store::Error::NotImplemented)
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>>
        {
            Err(object_store::Error::NotImplemented)
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }

        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            Err(object_store::Error::NotImplemented)
        }
    }

    #[tokio::test]
    async fn fetch_metadata_with_size_hint() -> Result<()> {
        let c1: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Foo"), None, Some("bar")]));

        let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch1 = RecordBatch::try_from_iter(vec![("c1", c1.clone())]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("c2", c2)]).unwrap();

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));
        let (meta, _files) = store_parquet(vec![batch1, batch2], false).await?;

        // Use a size hint larger than the parquet footer but smaller than the actual metadata, requiring a second fetch
        // for the remaining metadata
        fetch_parquet_metadata(store.as_ref() as &dyn ObjectStore, &meta[0], Some(9))
            .await
            .expect("error reading metadata with hint");

        assert_eq!(store.request_count(), 2);

        let session = SessionContext::new();
        let ctx = session.state();
        let format = ParquetFormat::default().with_metadata_size_hint(Some(9));
        let schema = format
            .infer_schema(&ctx, &store.upcast(), &meta)
            .await
            .unwrap();

        let stats =
            fetch_statistics(store.upcast().as_ref(), schema.clone(), &meta[0], Some(9))
                .await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use the file size as the hint so we can get the full metadata from the first fetch
        let size_hint = meta[0].size;

        fetch_parquet_metadata(store.upcast().as_ref(), &meta[0], Some(size_hint))
            .await
            .expect("error reading metadata with hint");

        // ensure the requests were coalesced into a single request
        assert_eq!(store.request_count(), 1);

        let format = ParquetFormat::default().with_metadata_size_hint(Some(size_hint));
        let schema = format
            .infer_schema(&ctx, &store.upcast(), &meta)
            .await
            .unwrap();
        let stats = fetch_statistics(
            store.upcast().as_ref(),
            schema.clone(),
            &meta[0],
            Some(size_hint),
        )
        .await?;

        assert_eq!(stats.num_rows, Some(3));
        let c1_stats = &stats.column_statistics.as_ref().expect("missing c1 stats")[0];
        let c2_stats = &stats.column_statistics.as_ref().expect("missing c2 stats")[1];
        assert_eq!(c1_stats.null_count, Some(1));
        assert_eq!(c2_stats.null_count, Some(3));

        let store = Arc::new(RequestCountingObjectStore::new(Arc::new(
            LocalFileSystem::new(),
        )));

        // Use the a size hint larger than the file size to make sure we don't panic
        let size_hint = meta[0].size + 100;

        fetch_parquet_metadata(store.upcast().as_ref(), &meta[0], Some(size_hint))
            .await
            .expect("error reading metadata with hint");

        assert_eq!(store.request_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn capture_bytes_scanned_metric() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session = SessionContext::with_config(config);
        let ctx = session.state();

        // Read the full file
        let projection = None;
        let exec = get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        // Read only one column. This should scan less data.
        let projection = Some(vec![0]);
        let exec_projected =
            get_exec(&ctx, "alltypes_plain.parquet", projection, None).await?;

        let task_ctx = ctx.task_ctx();

        let _ = collect(exec.clone(), task_ctx.clone()).await?;
        let _ = collect(exec_projected.clone(), task_ctx).await?;

        assert_bytes_scanned(exec, 671);
        assert_bytes_scanned(exec_projected, 73);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec =
            get_exec(&state, "alltypes_plain.parquet", projection, Some(1)).await?;

        // note: even if the limit is set, the executor rounds up to the batch size
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));
        assert!(exec.statistics().is_exact);
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Nanosecond, None)",
            y
        );

        let batches = collect(exec, task_ctx).await?;

        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![1]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_boolean_array(batches[0].column(0))?;
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_int32_array(batches[0].column(0))?;
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![10]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_timestamp_nanosecond_array(batches[0].column(0))?;
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{values:?}"));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![6]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float32_array(batches[0].column(0))?;
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![7]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_float64_array(batches[0].column(0))?;
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![9]);
        let exec = get_exec(&state, "alltypes_plain.parquet", projection, None).await?;

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = as_binary_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{values:?}")
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_decimal_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();

        // parquet use the int32 as the physical type to store decimal
        let exec = get_exec(&state, "int32_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        // parquet use the int64 as the physical type to store decimal
        let exec = get_exec(&state, "int64_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(10, 2), column.data_type());

        // parquet use the fixed length binary as the physical type to store decimal
        let exec = get_exec(&state, "fixed_length_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(25, 2), column.data_type());

        let exec =
            get_exec(&state, "fixed_length_decimal_legacy.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(13, 2), column.data_type());

        // parquet use the byte array as the physical type to store decimal
        let exec = get_exec(&state, "byte_array_decimal.parquet", None, None).await?;
        let batches = collect(exec, task_ctx.clone()).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        let column = batches[0].column(0);
        assert_eq!(&DataType::Decimal128(4, 2), column.data_type());

        Ok(())
    }
    #[tokio::test]
    async fn test_read_parquet_page_index() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let path = format!("{testdata}/alltypes_tiny_pages.parquet");
        let file = File::open(path).await.unwrap();
        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(file, options.clone())
                .await
                .unwrap()
                .metadata()
                .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        let path = format!("{testdata}/alltypes_tiny_pages_plain.parquet");
        let file = File::open(path).await.unwrap();

        let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
            .await
            .unwrap()
            .metadata()
            .clone();
        check_page_index_validation(builder.column_index(), builder.offset_index());

        Ok(())
    }

    fn check_page_index_validation(
        page_index: Option<&ParquetColumnIndex>,
        offset_index: Option<&ParquetOffsetIndex>,
    ) {
        assert!(page_index.is_some());
        assert!(offset_index.is_some());

        let page_index = page_index.unwrap();
        let offset_index = offset_index.unwrap();

        // there is only one row group in one file.
        assert_eq!(page_index.len(), 1);
        assert_eq!(offset_index.len(), 1);
        let page_index = page_index.get(0).unwrap();
        let offset_index = offset_index.get(0).unwrap();

        // 13 col in one row group
        assert_eq!(page_index.len(), 13);
        assert_eq!(offset_index.len(), 13);

        // test result in int_col
        let int_col_index = page_index.get(4).unwrap();
        let int_col_offset = offset_index.get(4).unwrap();

        // 325 pages in int_col
        assert_eq!(int_col_offset.len(), 325);
        match int_col_index {
            Index::INT32(index) => {
                assert_eq!(index.indexes.len(), 325);
                for min_max in index.clone().indexes {
                    assert!(min_max.min.is_some());
                    assert!(min_max.max.is_some());
                    assert!(min_max.null_count.is_some());
                }
            }
            _ => {
                error!("fail to read page index.")
            }
        }
    }

    fn assert_bytes_scanned(exec: Arc<dyn ExecutionPlan>, expected: usize) {
        let actual = exec
            .metrics()
            .expect("Metrics not recorded")
            .sum(|metric| matches!(metric.value(), MetricValue::Count { name, .. } if name == "bytes_scanned"))
            .map(|t| t.as_usize())
            .expect("bytes_scanned metric not recorded");

        assert_eq!(actual, expected);
    }

    async fn get_exec(
        state: &SessionState,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let format = ParquetFormat::default();
        scan_format(state, &format, &testdata, file_name, projection, limit).await
    }
}
