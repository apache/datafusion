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

//! [`ParquetFormat`]: Parquet [`FileFormat`] abstractions

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::sum;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::stats::Precision;
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, ColumnStatistics,
    DataFusionError, GetExt, Result, DEFAULT_PARQUET_EXTENSION,
};
use datafusion_common::{HashMap, Statistics};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{
    FileFormat, FileFormatFactory, FilePushdownSupport,
};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::{create_writer, get_writer_schema, SharedBuffer};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::Expr;
use datafusion_functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::Accumulator;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use crate::can_expr_be_pushed_down_with_schemas;
use crate::source::ParquetSource;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_datasource::source::DataSourceExec;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use log::debug;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_writer::{
    compute_leaves, get_column_writers, ArrowColumnChunk, ArrowColumnWriter,
    ArrowLeafColumn, ArrowWriterOptions,
};
use parquet::arrow::async_reader::MetadataFetch;
use parquet::arrow::{parquet_to_arrow_schema, ArrowSchemaConverter, AsyncArrowWriter};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::SerializedFileWriter;
use parquet::format::FileMetaData;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// When writing parquet files in parallel, if the buffered Parquet data exceeds
/// this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

#[derive(Default)]
/// Factory struct used to create [ParquetFormat]
pub struct ParquetFormatFactory {
    /// inner options for parquet
    pub options: Option<TableParquetOptions>,
}

impl ParquetFormatFactory {
    /// Creates an instance of [ParquetFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [ParquetFormatFactory] with customized default options
    pub fn new_with_options(options: TableParquetOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for ParquetFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let parquet_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::PARQUET);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.parquet
            }
            Some(parquet_options) => {
                let mut parquet_options = parquet_options.clone();
                for (k, v) in format_options {
                    parquet_options.set(k, v)?;
                }
                parquet_options
            }
        };

        Ok(Arc::new(
            ParquetFormat::default().with_options(parquet_options),
        ))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for ParquetFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_PARQUET_EXTENSION[1..].to_string()
    }
}

impl Debug for ParquetFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetFormatFactory")
            .field("ParquetFormatFactory", &self.options)
            .finish()
    }
}
/// The Apache Parquet `FileFormat` implementation
#[derive(Debug, Default)]
pub struct ParquetFormat {
    options: TableParquetOptions,
}

impl ParquetFormat {
    /// Construct a new Format with no local overrides
    pub fn new() -> Self {
        Self::default()
    }

    /// Activate statistics based row group level pruning
    /// - If `None`, defaults to value on `config_options`
    pub fn with_enable_pruning(mut self, enable: bool) -> Self {
        self.options.global.pruning = enable;
        self
    }

    /// Return `true` if pruning is enabled
    pub fn enable_pruning(&self) -> bool {
        self.options.global.pruning
    }

    /// Provide a hint to the size of the file metadata. If a hint is provided
    /// the reader will try and fetch the last `size_hint` bytes of the parquet file optimistically.
    /// Without a hint, two read are required. One read to fetch the 8-byte parquet footer and then
    /// another read to fetch the metadata length encoded in the footer.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_metadata_size_hint(mut self, size_hint: Option<usize>) -> Self {
        self.options.global.metadata_size_hint = size_hint;
        self
    }

    /// Return the metadata size hint if set
    pub fn metadata_size_hint(&self) -> Option<usize> {
        self.options.global.metadata_size_hint
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.
    ///
    /// - If `None`, defaults to value on `config_options`
    pub fn with_skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.options.global.skip_metadata = skip_metadata;
        self
    }

    /// Returns `true` if schema metadata will be cleared prior to
    /// schema merging.
    pub fn skip_metadata(&self) -> bool {
        self.options.global.skip_metadata
    }

    /// Set Parquet options for the ParquetFormat
    pub fn with_options(mut self, options: TableParquetOptions) -> Self {
        self.options = options;
        self
    }

    /// Parquet options
    pub fn options(&self) -> &TableParquetOptions {
        &self.options
    }

    /// Return `true` if should use view types.
    ///
    /// If this returns true, DataFusion will instruct the parquet reader
    /// to read string / binary columns using view `StringView` or `BinaryView`
    /// if the table schema specifies those types, regardless of any embedded metadata
    /// that may specify an alternate Arrow type. The parquet reader is optimized
    /// for reading `StringView` and `BinaryView` and such queries are significantly faster.
    ///
    /// If this returns false, the parquet reader will read the columns according to the
    /// defaults or any embedded Arrow type information. This may result in reading
    /// `StringArrays` and then casting to `StringViewArray` which is less efficient.
    pub fn force_view_types(&self) -> bool {
        self.options.global.schema_force_view_types
    }

    /// If true, will use view types. See [`Self::force_view_types`] for details
    pub fn with_force_view_types(mut self, use_views: bool) -> Self {
        self.options.global.schema_force_view_types = use_views;
        self
    }

    /// Return `true` if binary types will be read as strings.
    ///
    /// If this returns true, DataFusion will instruct the parquet reader
    /// to read binary columns such as `Binary` or `BinaryView` as the
    /// corresponding string type such as `Utf8` or `LargeUtf8`.
    /// The parquet reader has special optimizations for `Utf8` and `LargeUtf8`
    /// validation, and such queries are significantly faster than reading
    /// binary columns and then casting to string columns.
    pub fn binary_as_string(&self) -> bool {
        self.options.global.binary_as_string
    }

    /// If true, will read binary types as strings. See [`Self::binary_as_string`] for details
    pub fn with_binary_as_string(mut self, binary_as_string: bool) -> Self {
        self.options.global.binary_as_string = binary_as_string;
        self
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

async fn fetch_schema_with_location(
    store: &dyn ObjectStore,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<(Path, Schema)> {
    let loc_path = file.location.clone();
    let schema = fetch_schema(store, file, metadata_size_hint).await?;
    Ok((loc_path, schema))
}

#[async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        ParquetFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => internal_err!("Parquet FileFormat does not support compression."),
        }
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| {
                fetch_schema_with_location(
                    store.as_ref(),
                    object,
                    self.metadata_size_hint(),
                )
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        // Schema inference adds fields based the order they are seen
        // which depends on the order the files are processed. For some
        // object stores (like local file systems) the order returned from list
        // is not deterministic. Thus, to ensure deterministic schema inference
        // sort the files first.
        // https://github.com/apache/datafusion/pull/6629
        schemas.sort_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas
            .into_iter()
            .map(|(_, schema)| schema)
            .collect::<Vec<_>>();

        let schema = if self.skip_metadata() {
            Schema::try_merge(clear_metadata(schemas))
        } else {
            Schema::try_merge(schemas)
        }?;

        let schema = if self.binary_as_string() {
            transform_binary_to_string(&schema)
        } else {
            schema
        };

        let schema = if self.force_view_types() {
            transform_schema_to_view(&schema)
        } else {
            schema
        };

        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let stats = fetch_statistics(
            store.as_ref(),
            table_schema,
            object,
            self.metadata_size_hint(),
        )
        .await?;
        Ok(stats)
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut predicate = None;
        let mut metadata_size_hint = None;

        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        if self.enable_pruning() {
            if let Some(pred) = filters.cloned() {
                predicate = Some(pred);
            }
        }
        if let Some(metadata) = self.metadata_size_hint() {
            metadata_size_hint = Some(metadata);
        }

        let mut source = ParquetSource::new(self.options.clone());

        if let Some(predicate) = predicate {
            source = source.with_predicate(Arc::clone(&conf.file_schema), predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            source = source.with_metadata_size_hint(metadata_size_hint)
        }

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Parquet");
        }

        let sink = Arc::new(ParquetSink::new(conf, self.options.clone()));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn supports_filters_pushdown(
        &self,
        file_schema: &Schema,
        table_schema: &Schema,
        filters: &[&Expr],
    ) -> Result<FilePushdownSupport> {
        if !self.options().global.pushdown_filters {
            return Ok(FilePushdownSupport::NoSupport);
        }

        let all_supported = filters.iter().all(|filter| {
            can_expr_be_pushed_down_with_schemas(filter, file_schema, table_schema)
        });

        Ok(if all_supported {
            FilePushdownSupport::Supported
        } else {
            FilePushdownSupport::NotSupportedForFilter
        })
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(ParquetSource::default())
    }
}

/// Apply necessary schema type coercions to make file schema match table schema.
///
/// This function performs two main types of transformations in a single pass:
/// 1. Binary types to string types conversion - Converts binary data types to their
///    corresponding string types when the table schema expects string data
/// 2. Regular to view types conversion - Converts standard string/binary types to
///    view types when the table schema uses view types
///
/// # Arguments
/// * `table_schema` - The table schema containing the desired types
/// * `file_schema` - The file schema to be transformed
///
/// # Returns
/// * `Some(Schema)` - If any transformations were applied, returns the transformed schema
/// * `None` - If no transformations were needed
pub fn apply_file_schema_type_coercions(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    let mut needs_view_transform = false;
    let mut needs_string_transform = false;

    // Create a mapping of table field names to their data types for fast lookup
    // and simultaneously check if we need any transformations
    let table_fields: HashMap<_, _> = table_schema
        .fields()
        .iter()
        .map(|f| {
            let dt = f.data_type();
            // Check if we need view type transformation
            if matches!(dt, &DataType::Utf8View | &DataType::BinaryView) {
                needs_view_transform = true;
            }
            // Check if we need string type transformation
            if matches!(
                dt,
                &DataType::Utf8 | &DataType::LargeUtf8 | &DataType::Utf8View
            ) {
                needs_string_transform = true;
            }

            (f.name(), dt)
        })
        .collect();

    // Early return if no transformation needed
    if !needs_view_transform && !needs_string_transform {
        return None;
    }

    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields()
        .iter()
        .map(|field| {
            let field_name = field.name();
            let field_type = field.data_type();

            // Look up the corresponding field type in the table schema
            if let Some(table_type) = table_fields.get(field_name) {
                match (table_type, field_type) {
                    // table schema uses string type, coerce the file schema to use string type
                    (
                        &DataType::Utf8,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::Utf8);
                    }
                    // table schema uses large string type, coerce the file schema to use large string type
                    (
                        &DataType::LargeUtf8,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::LargeUtf8);
                    }
                    // table schema uses string view type, coerce the file schema to use view type
                    (
                        &DataType::Utf8View,
                        DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                    ) => {
                        return field_with_new_type(field, DataType::Utf8View);
                    }
                    // Handle view type conversions
                    (&DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8) => {
                        return field_with_new_type(field, DataType::Utf8View);
                    }
                    (&DataType::BinaryView, DataType::Binary | DataType::LargeBinary) => {
                        return field_with_new_type(field, DataType::BinaryView);
                    }
                    _ => {}
                }
            }

            // If no transformation is needed, keep the original field
            Arc::clone(field)
        })
        .collect();

    Some(Schema::new_with_metadata(
        transformed_fields,
        file_schema.metadata.clone(),
    ))
}

/// Coerces the file schema if the table schema uses a view type.
#[deprecated(
    since = "47.0.0",
    note = "Use `apply_file_schema_type_coercions` instead"
)]
pub fn coerce_file_schema_to_view_type(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    let mut transform = false;
    let table_fields: HashMap<_, _> = table_schema
        .fields
        .iter()
        .map(|f| {
            let dt = f.data_type();
            if dt.equals_datatype(&DataType::Utf8View)
                || dt.equals_datatype(&DataType::BinaryView)
            {
                transform = true;
            }
            (f.name(), dt)
        })
        .collect();

    if !transform {
        return None;
    }

    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields
        .iter()
        .map(
            |field| match (table_fields.get(field.name()), field.data_type()) {
                (Some(DataType::Utf8View), DataType::Utf8 | DataType::LargeUtf8) => {
                    field_with_new_type(field, DataType::Utf8View)
                }
                (
                    Some(DataType::BinaryView),
                    DataType::Binary | DataType::LargeBinary,
                ) => field_with_new_type(field, DataType::BinaryView),
                _ => Arc::clone(field),
            },
        )
        .collect();

    Some(Schema::new_with_metadata(
        transformed_fields,
        file_schema.metadata.clone(),
    ))
}

/// If the table schema uses a string type, coerce the file schema to use a string type.
///
/// See [ParquetFormat::binary_as_string] for details
#[deprecated(
    since = "47.0.0",
    note = "Use `apply_file_schema_type_coercions` instead"
)]
pub fn coerce_file_schema_to_string_type(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    let mut transform = false;
    let table_fields: HashMap<_, _> = table_schema
        .fields
        .iter()
        .map(|f| (f.name(), f.data_type()))
        .collect();
    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields
        .iter()
        .map(
            |field| match (table_fields.get(field.name()), field.data_type()) {
                // table schema uses string type, coerce the file schema to use string type
                (
                    Some(DataType::Utf8),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::Utf8)
                }
                // table schema uses large string type, coerce the file schema to use large string type
                (
                    Some(DataType::LargeUtf8),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::LargeUtf8)
                }
                // table schema uses string view type, coerce the file schema to use view type
                (
                    Some(DataType::Utf8View),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::Utf8View)
                }
                _ => Arc::clone(field),
            },
        )
        .collect();

    if !transform {
        None
    } else {
        Some(Schema::new_with_metadata(
            transformed_fields,
            file_schema.metadata.clone(),
        ))
    }
}

/// Create a new field with the specified data type, copying the other
/// properties from the input field
fn field_with_new_type(field: &FieldRef, new_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}

/// Transform a schema to use view types for Utf8 and Binary
///
/// See [ParquetFormat::force_view_types] for details
pub fn transform_schema_to_view(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                field_with_new_type(field, DataType::Utf8View)
            }
            DataType::Binary | DataType::LargeBinary => {
                field_with_new_type(field, DataType::BinaryView)
            }
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}

/// Transform a schema so that any binary types are strings
pub fn transform_binary_to_string(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Binary => field_with_new_type(field, DataType::Utf8),
            DataType::LargeBinary => field_with_new_type(field, DataType::LargeUtf8),
            DataType::BinaryView => field_with_new_type(field, DataType::Utf8View),
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}

/// [`MetadataFetch`] adapter for reading bytes from an [`ObjectStore`]
struct ObjectStoreFetch<'a> {
    store: &'a dyn ObjectStore,
    meta: &'a ObjectMeta,
}

impl<'a> ObjectStoreFetch<'a> {
    fn new(store: &'a dyn ObjectStore, meta: &'a ObjectMeta) -> Self {
        Self { store, meta }
    }
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        async {
            self.store
                .get_range(&self.meta.location, range)
                .await
                .map_err(ParquetError::from)
        }
        .boxed()
    }
}

/// Fetches parquet metadata from ObjectStore for given object
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
///
/// [`ParquetFileReaderFactory`]: crate::ParquetFileReaderFactory
pub async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    meta: &ObjectMeta,
    size_hint: Option<usize>,
) -> Result<ParquetMetaData> {
    let file_size = meta.size;
    let fetch = ObjectStoreFetch::new(store, meta);

    ParquetMetaDataReader::new()
        .with_prefetch_hint(size_hint)
        .load_and_finish(fetch, file_size)
        .await
        .map_err(DataFusionError::from)
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
///
/// See [`statistics_from_parquet_meta_calc`] for more details
pub async fn fetch_statistics(
    store: &dyn ObjectStore,
    table_schema: SchemaRef,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Statistics> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    statistics_from_parquet_meta_calc(&metadata, table_schema)
}

/// Convert statistics in [`ParquetMetaData`] into [`Statistics`] using [`StatisticsConverter`]
///
/// The statistics are calculated for each column in the table schema
/// using the row group statistics in the parquet metadata.
///
/// # Key behaviors:
///
/// 1. Extracts row counts and byte sizes from all row groups
/// 2. Applies schema type coercions to align file schema with table schema
/// 3. Collects and aggregates statistics across row groups when available
///
/// # When there are no statistics:
///
/// If the Parquet file doesn't contain any statistics (has_statistics is false), the function returns a Statistics object with:
/// - Exact row count
/// - Exact byte size
/// - All column statistics marked as unknown via Statistics::unknown_column(&table_schema)
/// # When only some columns have statistics:
///
/// For columns with statistics:
/// - Min/max values are properly extracted and represented as Precision::Exact
/// - Null counts are calculated by summing across row groups
///
/// For columns without statistics,
/// - For min/max, there are two situations:
///     1. The column isn't in arrow schema, then min/max values are set to Precision::Absent
///     2. The column is in arrow schema, but not in parquet schema due to schema revolution, min/max values are set to Precision::Exact(null)
/// - Null counts are set to Precision::Exact(num_rows) (conservatively assuming all values could be null)
pub fn statistics_from_parquet_meta_calc(
    metadata: &ParquetMetaData,
    table_schema: SchemaRef,
) -> Result<Statistics> {
    let row_groups_metadata = metadata.row_groups();

    let mut statistics = Statistics::new_unknown(&table_schema);
    let mut has_statistics = false;
    let mut num_rows = 0_usize;
    let mut total_byte_size = 0_usize;
    for row_group_meta in row_groups_metadata {
        num_rows += row_group_meta.num_rows() as usize;
        total_byte_size += row_group_meta.total_byte_size() as usize;

        if !has_statistics {
            has_statistics = row_group_meta
                .columns()
                .iter()
                .any(|column| column.statistics().is_some());
        }
    }
    statistics.num_rows = Precision::Exact(num_rows);
    statistics.total_byte_size = Precision::Exact(total_byte_size);

    let file_metadata = metadata.file_metadata();
    let mut file_schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;

    if let Some(merged) = apply_file_schema_type_coercions(&table_schema, &file_schema) {
        file_schema = merged;
    }

    statistics.column_statistics = if has_statistics {
        let (mut max_accs, mut min_accs) = create_max_min_accs(&table_schema);
        let mut null_counts_array =
            vec![Precision::Exact(0); table_schema.fields().len()];

        table_schema
            .fields()
            .iter()
            .enumerate()
            .for_each(|(idx, field)| {
                match StatisticsConverter::try_new(
                    field.name(),
                    &file_schema,
                    file_metadata.schema_descr(),
                ) {
                    Ok(stats_converter) => {
                        summarize_min_max_null_counts(
                            &mut min_accs,
                            &mut max_accs,
                            &mut null_counts_array,
                            idx,
                            num_rows,
                            &stats_converter,
                            row_groups_metadata,
                        )
                        .ok();
                    }
                    Err(e) => {
                        debug!("Failed to create statistics converter: {}", e);
                        null_counts_array[idx] = Precision::Exact(num_rows);
                    }
                }
            });

        get_col_stats(
            &table_schema,
            null_counts_array,
            &mut max_accs,
            &mut min_accs,
        )
    } else {
        Statistics::unknown_column(&table_schema)
    };

    Ok(statistics)
}

fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<Precision<usize>>,
    max_values: &mut [Option<MaxAccumulator>],
    min_values: &mut [Option<MinAccumulator>],
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match max_values.get_mut(i).unwrap() {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match min_values.get_mut(i).unwrap() {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: null_counts[i],
                max_value: max_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                min_value: min_value.map(Precision::Exact).unwrap_or(Precision::Absent),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
            }
        })
        .collect()
}

fn summarize_min_max_null_counts(
    min_accs: &mut [Option<MinAccumulator>],
    max_accs: &mut [Option<MaxAccumulator>],
    null_counts_array: &mut [Precision<usize>],
    arrow_schema_index: usize,
    num_rows: usize,
    stats_converter: &StatisticsConverter,
    row_groups_metadata: &[RowGroupMetaData],
) -> Result<()> {
    let max_values = stats_converter.row_group_maxes(row_groups_metadata)?;
    let min_values = stats_converter.row_group_mins(row_groups_metadata)?;
    let null_counts = stats_converter.row_group_null_counts(row_groups_metadata)?;

    if let Some(max_acc) = &mut max_accs[arrow_schema_index] {
        max_acc.update_batch(&[max_values])?;
    }

    if let Some(min_acc) = &mut min_accs[arrow_schema_index] {
        min_acc.update_batch(&[min_values])?;
    }

    null_counts_array[arrow_schema_index] = Precision::Exact(match sum(&null_counts) {
        Some(null_count) => null_count as usize,
        None => num_rows,
    });

    Ok(())
}

/// Implements [`DataSink`] for writing to a parquet file.
pub struct ParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Underlying parquet options
    parquet_options: TableParquetOptions,
    /// File metadata from successfully produced parquet files. The Mutex is only used
    /// to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, FileMetaData>>>,
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
        }
    }

    /// Retrieve the file metadata for the written files, keyed to the path
    /// which may be partitioned (in the case of hive style partitioning).
    pub fn written(&self) -> HashMap<Path, FileMetaData> {
        self.written.lock().clone()
    }

    /// Create writer properties based upon configuration settings,
    /// including partitioning and the inclusion of arrow schema metadata.
    fn create_writer_props(&self) -> Result<WriterProperties> {
        let schema = if self.parquet_options.global.allow_single_file_parallelism {
            // If parallelizing writes, we may be also be doing hive style partitioning
            // into multiple files which impacts the schema per file.
            // Refer to `get_writer_schema()`
            &get_writer_schema(&self.config)
        } else {
            self.config.output_schema()
        };

        // TODO: avoid this clone in follow up PR, where the writer properties & schema
        // are calculated once on `ParquetSink::new`
        let mut parquet_opts = self.parquet_options.clone();
        if !self.parquet_options.global.skip_arrow_metadata {
            parquet_opts.arrow_schema(schema);
        }

        Ok(WriterPropertiesBuilder::try_from(&parquet_opts)?.build())
    }

    /// Creates an AsyncArrowWriter which serializes a parquet file to an ObjectStore
    /// AsyncArrowWriters are used when individual parquet file serialization is not parallelized
    async fn create_async_arrow_writer(
        &self,
        location: &Path,
        object_store: Arc<dyn ObjectStore>,
        parquet_props: WriterProperties,
    ) -> Result<AsyncArrowWriter<BufWriter>> {
        let buf_writer = BufWriter::new(object_store, location.clone());
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
        let parquet_opts = &self.parquet_options;
        let allow_single_file_parallelism =
            parquet_opts.global.allow_single_file_parallelism;

        let mut file_write_tasks: JoinSet<
            std::result::Result<(Path, FileMetaData), DataFusionError>,
        > = JoinSet::new();

        let parquet_props = self.create_writer_props()?;
        let parallel_options = ParallelParquetWriterOptions {
            max_parallel_row_groups: parquet_opts
                .global
                .maximum_parallel_row_group_writers,
            max_buffered_record_batches_per_stream: parquet_opts
                .global
                .maximum_buffered_record_batches_per_stream,
        };

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            if !allow_single_file_parallelism {
                let mut writer = self
                    .create_async_arrow_writer(
                        &path,
                        Arc::clone(&object_store),
                        parquet_props.clone(),
                    )
                    .await?;
                let mut reservation =
                    MemoryConsumer::new(format!("ParquetSink[{}]", path))
                        .register(context.memory_pool());
                file_write_tasks.spawn(async move {
                    while let Some(batch) = rx.recv().await {
                        writer.write(&batch).await?;
                        reservation.try_resize(writer.memory_size())?;
                    }
                    let file_metadata = writer
                        .close()
                        .await
                        .map_err(DataFusionError::ParquetError)?;
                    Ok((path, file_metadata))
                });
            } else {
                let writer = create_writer(
                    // Parquet files as a whole are never compressed, since they
                    // manage compressed blocks themselves.
                    FileCompressionType::UNCOMPRESSED,
                    &path,
                    Arc::clone(&object_store),
                )
                .await?;
                let schema = get_writer_schema(&self.config);
                let props = parquet_props.clone();
                let parallel_options_clone = parallel_options.clone();
                let pool = Arc::clone(context.memory_pool());
                file_write_tasks.spawn(async move {
                    let file_metadata = output_single_parquet_file_parallelized(
                        writer,
                        rx,
                        schema,
                        &props,
                        parallel_options_clone,
                        pool,
                    )
                    .await?;
                    Ok((path, file_metadata))
                });
            }
        }

        let mut row_count = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    let (path, file_metadata) = r?;
                    row_count += file_metadata.num_rows;
                    let mut written_files = self.written.lock();
                    written_files
                        .try_insert(path.clone(), file_metadata)
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
            .map_err(DataFusionError::ExecutionJoin)??;

        Ok(row_count as u64)
    }
}

#[async_trait]
impl DataSink for ParquetSink {
    fn as_any(&self) -> &dyn Any {
        self
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
}

/// Consumes a stream of [ArrowLeafColumn] via a channel and serializes them using an [ArrowColumnWriter]
/// Once the channel is exhausted, returns the ArrowColumnWriter.
async fn column_serializer_task(
    mut rx: Receiver<ArrowLeafColumn>,
    mut writer: ArrowColumnWriter,
    mut reservation: MemoryReservation,
) -> Result<(ArrowColumnWriter, MemoryReservation)> {
    while let Some(col) = rx.recv().await {
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
    schema: Arc<Schema>,
    parquet_props: Arc<WriterProperties>,
    max_buffer_size: usize,
    pool: &Arc<dyn MemoryPool>,
) -> Result<(Vec<ColumnWriterTask>, Vec<ColSender>)> {
    let schema_desc = ArrowSchemaConverter::new().convert(&schema)?;
    let col_writers = get_column_writers(&schema_desc, &parquet_props, &schema)?;
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
) -> SpawnedTask<RBStreamSerializeResult> {
    let mut rg_reservation =
        MemoryConsumer::new("ParquetSink(SerializedRowGroupWriter)").register(pool);

    SpawnedTask::spawn(async move {
        let num_cols = column_writer_tasks.len();
        let mut finalized_rg = Vec::with_capacity(num_cols);
        for task in column_writer_tasks.into_iter() {
            let (writer, _col_reservation) = task
                .join_unwind()
                .await
                .map_err(DataFusionError::ExecutionJoin)??;
            let encoded_size = writer.get_estimated_total_bytes();
            rg_reservation.grow(encoded_size);
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
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<SpawnedTask<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
    writer_props: Arc<WriterProperties>,
    parallel_options: ParallelParquetWriterOptions,
    pool: Arc<dyn MemoryPool>,
) -> SpawnedTask<Result<(), DataFusionError>> {
    SpawnedTask::spawn(async move {
        let max_buffer_rb = parallel_options.max_buffered_record_batches_per_stream;
        let max_row_group_rows = writer_props.max_row_group_size();
        let (mut column_writer_handles, mut col_array_channels) =
            spawn_column_parallel_row_group_writer(
                Arc::clone(&schema),
                Arc::clone(&writer_props),
                max_buffer_rb,
                &pool,
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
                        Arc::clone(&schema),
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
                        Arc::clone(&schema),
                    )
                    .await?;

                    // Signal the parallel column writers that the RowGroup is done, join and finalize RowGroup
                    // on a separate task, so that we can immediately start on the next RG before waiting
                    // for the current one to finish.
                    drop(col_array_channels);
                    let finalize_rg_task = spawn_rg_join_and_finalize_task(
                        column_writer_handles,
                        max_row_group_rows,
                        &pool,
                    );

                    // Do not surface error from closed channel (means something
                    // else hit an error, and the plan is shutting down).
                    if serialize_tx.send(finalize_rg_task).await.is_err() {
                        return Ok(());
                    }

                    current_rg_rows = 0;
                    rb = rb.slice(rows_left, rb.num_rows() - rows_left);

                    (column_writer_handles, col_array_channels) =
                        spawn_column_parallel_row_group_writer(
                            Arc::clone(&schema),
                            Arc::clone(&writer_props),
                            max_buffer_rb,
                            &pool,
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
                &pool,
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
    mut serialize_rx: Receiver<SpawnedTask<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
    writer_props: Arc<WriterProperties>,
    mut object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    pool: Arc<dyn MemoryPool>,
) -> Result<FileMetaData> {
    let merged_buff = SharedBuffer::new(INITIAL_BUFFER_BYTES);

    let mut file_reservation =
        MemoryConsumer::new("ParquetSink(SerializedFileWriter)").register(&pool);

    let schema_desc = ArrowSchemaConverter::new().convert(schema.as_ref())?;
    let mut parquet_writer = SerializedFileWriter::new(
        merged_buff.clone(),
        schema_desc.root_schema_ptr(),
        writer_props,
    )?;

    while let Some(task) = serialize_rx.recv().await {
        let result = task.join_unwind().await;
        let mut rg_out = parquet_writer.next_row_group()?;
        let (serialized_columns, mut rg_reservation, _cnt) =
            result.map_err(DataFusionError::ExecutionJoin)??;
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

    let file_metadata = parquet_writer.close()?;
    let final_buff = merged_buff.buffer.try_lock().unwrap();

    object_store_writer.write_all(final_buff.as_slice()).await?;
    object_store_writer.shutdown().await?;
    file_reservation.free();

    Ok(file_metadata)
}

/// Parallelizes the serialization of a single parquet file, by first serializing N
/// independent RecordBatch streams in parallel to RowGroups in memory. Another
/// task then stitches these independent RowGroups together and streams this large
/// single parquet file to an ObjectStore in multiple parts.
async fn output_single_parquet_file_parallelized(
    object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    data: Receiver<RecordBatch>,
    output_schema: Arc<Schema>,
    parquet_props: &WriterProperties,
    parallel_options: ParallelParquetWriterOptions,
    pool: Arc<dyn MemoryPool>,
) -> Result<FileMetaData> {
    let max_rowgroups = parallel_options.max_parallel_row_groups;
    // Buffer size of this channel limits maximum number of RowGroups being worked on in parallel
    let (serialize_tx, serialize_rx) =
        mpsc::channel::<SpawnedTask<RBStreamSerializeResult>>(max_rowgroups);

    let arc_props = Arc::new(parquet_props.clone());
    let launch_serialization_task = spawn_parquet_parallel_serialization_task(
        data,
        serialize_tx,
        Arc::clone(&output_schema),
        Arc::clone(&arc_props),
        parallel_options,
        Arc::clone(&pool),
    );
    let file_metadata = concatenate_parallel_row_groups(
        serialize_rx,
        Arc::clone(&output_schema),
        Arc::clone(&arc_props),
        object_store_writer,
        pool,
    )
    .await?;

    launch_serialization_task
        .join_unwind()
        .await
        .map_err(DataFusionError::ExecutionJoin)??;
    Ok(file_metadata)
}

/// Min/max aggregation can take Dictionary encode input but always produces unpacked
/// (aka non Dictionary) output. We need to adjust the output data type to reflect this.
/// The reason min/max aggregate produces unpacked output because there is only one
/// min/max value per group; there is no needs to keep them Dictionary encode
fn min_max_aggregate_data_type(input_type: &DataType) -> &DataType {
    if let DataType::Dictionary(_, value_type) = input_type {
        value_type.as_ref()
    } else {
        input_type
    }
}

fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MaxAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| {
            MinAccumulator::try_new(min_max_aggregate_data_type(field.data_type())).ok()
        })
        .collect();
    (max_values, min_values)
}
