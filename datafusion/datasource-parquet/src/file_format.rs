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
use std::cell::RefCell;
use std::fmt::Debug;
use std::ops::Range;
use std::rc::Rc;
use std::sync::Arc;
use std::{fmt, vec};

use arrow::array::RecordBatch;
use arrow::datatypes::{Fields, Schema, SchemaRef, TimeUnit};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::write::{
    get_writer_schema, ObjectWriterBuilder, SharedBuffer,
};
use datafusion_datasource::TableSchema;

use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::write::demux::DemuxedStreamReceiver;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_err, DataFusionError, GetExt,
    HashSet, Result, DEFAULT_PARQUET_EXTENSION,
};
use datafusion_common::{HashMap, Statistics};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use crate::metadata::DFParquetMetadata;
use crate::reader::CachedParquetFileReaderFactory;
use crate::source::{parse_coerce_int96_string, ParquetSource};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use datafusion_execution::runtime_env::RuntimeEnv;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_writer::{
    compute_leaves, ArrowColumnChunk, ArrowColumnWriter, ArrowLeafColumn,
    ArrowRowGroupWriterFactory, ArrowWriterOptions,
};
use parquet::arrow::async_reader::MetadataFetch;
use parquet::arrow::{ArrowWriter, AsyncArrowWriter};
use parquet::basic::Type;
#[cfg(feature = "parquet_encryption")]
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::SchemaDescriptor;
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

    pub fn coerce_int96(&self) -> Option<String> {
        self.options.global.coerce_int96.clone()
    }

    pub fn with_coerce_int96(mut self, time_unit: Option<String>) -> Self {
        self.options.global.coerce_int96 = time_unit;
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

#[cfg(feature = "parquet_encryption")]
async fn get_file_decryption_properties(
    state: &dyn Session,
    options: &TableParquetOptions,
    file_path: &Path,
) -> Result<Option<Arc<FileDecryptionProperties>>> {
    Ok(match &options.crypto.file_decryption {
        Some(cfd) => Some(Arc::new(FileDecryptionProperties::from(cfd.clone()))),
        None => match &options.crypto.factory_id {
            Some(factory_id) => {
                let factory =
                    state.runtime_env().parquet_encryption_factory(factory_id)?;
                factory
                    .get_file_decryption_properties(
                        &options.crypto.factory_options,
                        file_path,
                    )
                    .await?
            }
            None => None,
        },
    })
}

#[cfg(not(feature = "parquet_encryption"))]
async fn get_file_decryption_properties(
    _state: &dyn Session,
    _options: &TableParquetOptions,
    _file_path: &Path,
) -> Result<Option<Arc<FileDecryptionProperties>>> {
    Ok(None)
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

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let coerce_int96 = match self.coerce_int96() {
            Some(time_unit) => Some(parse_coerce_int96_string(time_unit.as_str())?),
            None => None,
        };

        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();

        let mut schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| async {
                let file_decryption_properties = get_file_decryption_properties(
                    state,
                    &self.options,
                    &object.location,
                )
                .await?;
                let result = DFParquetMetadata::new(store.as_ref(), object)
                    .with_metadata_size_hint(self.metadata_size_hint())
                    .with_decryption_properties(file_decryption_properties)
                    .with_file_metadata_cache(Some(Arc::clone(&file_metadata_cache)))
                    .with_coerce_int96(coerce_int96)
                    .fetch_schema_with_location()
                    .await?;
                Ok::<_, DataFusionError>(result)
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            // fetch schemas concurrently, if requested
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
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let file_decryption_properties =
            get_file_decryption_properties(state, &self.options, &object.location)
                .await?;
        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();
        DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(self.metadata_size_hint())
            .with_decryption_properties(file_decryption_properties)
            .with_file_metadata_cache(Some(file_metadata_cache))
            .fetch_statistics(&table_schema)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut metadata_size_hint = None;

        if let Some(metadata) = self.metadata_size_hint() {
            metadata_size_hint = Some(metadata);
        }

        let mut source = conf
            .file_source()
            .as_any()
            .downcast_ref::<ParquetSource>()
            .cloned()
            .ok_or_else(|| internal_datafusion_err!("Expected ParquetSource"))?;
        source = source.with_table_parquet_options(self.options.clone());

        // Use the CachedParquetFileReaderFactory
        let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();
        let store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;
        let cached_parquet_read_factory =
            Arc::new(CachedParquetFileReaderFactory::new(store, metadata_cache));
        source = source.with_parquet_file_reader_factory(cached_parquet_read_factory);

        if let Some(metadata_size_hint) = metadata_size_hint {
            source = source.with_metadata_size_hint(metadata_size_hint)
        }

        source = self.set_source_encryption_factory(source, state)?;

        // Apply schema adapter factory before building the new config
        let file_source = source.apply_schema_adapter(&conf)?;

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(file_source)
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

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            ParquetSource::new(table_schema)
                .with_table_parquet_options(self.options.clone()),
        )
    }
}

#[cfg(feature = "parquet_encryption")]
impl ParquetFormat {
    fn set_source_encryption_factory(
        &self,
        source: ParquetSource,
        state: &dyn Session,
    ) -> Result<ParquetSource> {
        if let Some(encryption_factory_id) = &self.options.crypto.factory_id {
            Ok(source.with_encryption_factory(
                state
                    .runtime_env()
                    .parquet_encryption_factory(encryption_factory_id)?,
            ))
        } else {
            Ok(source)
        }
    }
}

#[cfg(not(feature = "parquet_encryption"))]
impl ParquetFormat {
    fn set_source_encryption_factory(
        &self,
        source: ParquetSource,
        _state: &dyn Session,
    ) -> Result<ParquetSource> {
        if let Some(encryption_factory_id) = &self.options.crypto.factory_id {
            Err(DataFusionError::Configuration(
                format!("Parquet encryption factory id is set to '{encryption_factory_id}' but the parquet_encryption feature is disabled")))
        } else {
            Ok(source)
        }
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

/// Coerces the file schema's Timestamps to the provided TimeUnit if Parquet schema contains INT96.
pub fn coerce_int96_to_resolution(
    parquet_schema: &SchemaDescriptor,
    file_schema: &Schema,
    time_unit: &TimeUnit,
) -> Option<Schema> {
    // Traverse the parquet_schema columns looking for int96 physical types. If encountered, insert
    // the field's full path into a set.
    let int96_fields: HashSet<_> = parquet_schema
        .columns()
        .iter()
        .filter(|f| f.physical_type() == Type::INT96)
        .map(|f| f.path().string())
        .collect();

    if int96_fields.is_empty() {
        // The schema doesn't contain any int96 fields, so skip the remaining logic.
        return None;
    }

    // Do a DFS into the schema using a stack, looking for timestamp(nanos) fields that originated
    // as int96 to coerce to the provided time_unit.

    type NestedFields = Rc<RefCell<Vec<FieldRef>>>;
    type StackContext<'a> = (
        Vec<&'a str>, // The Parquet column path (e.g., "c0.list.element.c1") for the current field.
        &'a FieldRef, // The current field to be processed.
        NestedFields, // The parent's fields that this field will be (possibly) type-coerced and
        // inserted into. All fields have a parent, so this is not an Option type.
        Option<NestedFields>, // Nested types need to create their own vector of fields for their
                              // children. For primitive types this will remain None. For nested
                              // types it is None the first time they are processed. Then, we
                              // instantiate a vector for its children, push the field back onto the
                              // stack to be processed again, and DFS into its children. The next
                              // time we process the field, we know we have DFS'd into the children
                              // because this field is Some.
    );

    // This is our top-level fields from which we will construct our schema. We pass this into our
    // initial stack context as the parent fields, and the DFS populates it.
    let fields = Rc::new(RefCell::new(Vec::with_capacity(file_schema.fields.len())));

    // TODO: It might be possible to only DFS into nested fields that we know contain an int96 if we
    // use some sort of LPM data structure to check if we're currently DFS'ing nested types that are
    // in a column path that contains an int96. That can be a future optimization for large schemas.
    let transformed_schema = {
        // Populate the stack with our top-level fields.
        let mut stack: Vec<StackContext> = file_schema
            .fields()
            .iter()
            .rev()
            .map(|f| (vec![f.name().as_str()], f, Rc::clone(&fields), None))
            .collect();

        // Pop fields to DFS into until we have exhausted the stack.
        while let Some((parquet_path, current_field, parent_fields, child_fields)) =
            stack.pop()
        {
            match (current_field.data_type(), child_fields) {
                (DataType::Struct(unprocessed_children), None) => {
                    // This is the first time popping off this struct. We don't yet know the
                    // correct types of its children (i.e., if they need coercing) so we create
                    // a vector for child_fields, push the struct node back onto the stack to be
                    // processed again (see below) after processing all its children.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(
                        unprocessed_children.len(),
                    )));
                    // Note that here we push the struct back onto the stack with its
                    // parent_fields in the same position, now with Some(child_fields).
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    // Push all the children in reverse to maintain original schema order due to
                    // stack processing.
                    for child in unprocessed_children.into_iter().rev() {
                        let mut child_path = parquet_path.clone();
                        // Build up a normalized path that we'll use as a key into the original
                        // int96_fields set above to test if this originated as int96.
                        child_path.push(".");
                        child_path.push(child.name());
                        // Note that here we push the field onto the stack using the struct's
                        // new child_fields vector as the field's parent_fields.
                        stack.push((child_path, child, Rc::clone(&child_fields), None));
                    }
                }
                (DataType::Struct(unprocessed_children), Some(processed_children)) => {
                    // This is the second time popping off this struct. The child_fields vector
                    // now contains each field that has been DFS'd into, and we can construct
                    // the resulting struct with correct child types.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), unprocessed_children.len());
                    let processed_struct = Field::new_struct(
                        current_field.name(),
                        processed_children.as_slice(),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_struct));
                }
                (DataType::List(unprocessed_child), None) => {
                    // This is the first time popping off this list. See struct docs above.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(1)));
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    let mut child_path = parquet_path.clone();
                    // Spark uses a definition for arrays/lists that results in a group
                    // named "list" that is not maintained when parsing to Arrow. We just push
                    // this name into the path.
                    child_path.push(".list.");
                    child_path.push(unprocessed_child.name());
                    stack.push((
                        child_path.clone(),
                        unprocessed_child,
                        Rc::clone(&child_fields),
                        None,
                    ));
                }
                (DataType::List(_), Some(processed_children)) => {
                    // This is the second time popping off this list. See struct docs above.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), 1);
                    let processed_list = Field::new_list(
                        current_field.name(),
                        Arc::clone(&processed_children[0]),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_list));
                }
                (DataType::Map(unprocessed_child, _), None) => {
                    // This is the first time popping off this map. See struct docs above.
                    let child_fields = Rc::new(RefCell::new(Vec::with_capacity(1)));
                    stack.push((
                        parquet_path.clone(),
                        current_field,
                        parent_fields,
                        Some(Rc::clone(&child_fields)),
                    ));
                    let mut child_path = parquet_path.clone();
                    child_path.push(".");
                    child_path.push(unprocessed_child.name());
                    stack.push((
                        child_path.clone(),
                        unprocessed_child,
                        Rc::clone(&child_fields),
                        None,
                    ));
                }
                (DataType::Map(_, sorted), Some(processed_children)) => {
                    // This is the second time popping off this map. See struct docs above.
                    let processed_children = processed_children.borrow();
                    assert_eq!(processed_children.len(), 1);
                    let processed_map = Field::new(
                        current_field.name(),
                        DataType::Map(Arc::clone(&processed_children[0]), *sorted),
                        current_field.is_nullable(),
                    );
                    parent_fields.borrow_mut().push(Arc::new(processed_map));
                }
                (DataType::Timestamp(TimeUnit::Nanosecond, None), None)
                    if int96_fields.contains(parquet_path.concat().as_str()) =>
                // We found a timestamp(nanos) and it originated as int96. Coerce it to the correct
                // time_unit.
                {
                    parent_fields.borrow_mut().push(field_with_new_type(
                        current_field,
                        DataType::Timestamp(*time_unit, None),
                    ));
                }
                // Other types can be cloned as they are.
                _ => parent_fields.borrow_mut().push(Arc::clone(current_field)),
            }
        }
        assert_eq!(fields.borrow().len(), file_schema.fields.len());
        Schema::new_with_metadata(
            fields.borrow_mut().clone(),
            file_schema.metadata.clone(),
        )
    };

    Some(transformed_schema)
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
pub struct ObjectStoreFetch<'a> {
    store: &'a dyn ObjectStore,
    meta: &'a ObjectMeta,
}

impl<'a> ObjectStoreFetch<'a> {
    pub fn new(store: &'a dyn ObjectStore, meta: &'a ObjectMeta) -> Self {
        Self { store, meta }
    }
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
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
#[deprecated(
    since = "50.0.0",
    note = "Use `DFParquetMetadata::fetch_metadata` instead"
)]
pub async fn fetch_parquet_metadata(
    store: &dyn ObjectStore,
    object_meta: &ObjectMeta,
    size_hint: Option<usize>,
    decryption_properties: Option<&FileDecryptionProperties>,
    file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
) -> Result<Arc<ParquetMetaData>> {
    let decryption_properties = decryption_properties.cloned().map(Arc::new);
    DFParquetMetadata::new(store, object_meta)
        .with_metadata_size_hint(size_hint)
        .with_decryption_properties(decryption_properties)
        .with_file_metadata_cache(file_metadata_cache)
        .fetch_metadata()
        .await
}

/// Read and parse the statistics of the Parquet file at location `path`
///
/// See [`statistics_from_parquet_meta_calc`] for more details
#[deprecated(
    since = "50.0.0",
    note = "Use `DFParquetMetadata::fetch_statistics` instead"
)]
pub async fn fetch_statistics(
    store: &dyn ObjectStore,
    table_schema: SchemaRef,
    file: &ObjectMeta,
    metadata_size_hint: Option<usize>,
    decryption_properties: Option<&FileDecryptionProperties>,
    file_metadata_cache: Option<Arc<dyn FileMetadataCache>>,
) -> Result<Statistics> {
    let decryption_properties = decryption_properties.cloned().map(Arc::new);
    DFParquetMetadata::new(store, file)
        .with_metadata_size_hint(metadata_size_hint)
        .with_decryption_properties(decryption_properties)
        .with_file_metadata_cache(file_metadata_cache)
        .fetch_statistics(&table_schema)
        .await
}

#[deprecated(
    since = "50.0.0",
    note = "Use `DFParquetMetadata::statistics_from_parquet_metadata` instead"
)]
#[expect(clippy::needless_pass_by_value)]
pub fn statistics_from_parquet_meta_calc(
    metadata: &ParquetMetaData,
    table_schema: SchemaRef,
) -> Result<Statistics> {
    DFParquetMetadata::statistics_from_parquet_metadata(metadata, &table_schema)
}

/// Implements [`DataSink`] for writing to a parquet file.
pub struct ParquetSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Underlying parquet options
    parquet_options: TableParquetOptions,
    /// File metadata from successfully produced parquet files. The Mutex is only used
    /// to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, ParquetMetaData>>>,
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
            FileEncryptionProperties::from(file_encryption_properties),
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
            if !parquet_opts.global.allow_single_file_parallelism {
                let mut writer = self
                    .create_async_arrow_writer(
                        &path,
                        Arc::clone(&object_store),
                        context,
                        parquet_props.clone(),
                    )
                    .await?;
                let mut reservation = MemoryConsumer::new(format!("ParquetSink[{path}]"))
                    .register(context.memory_pool());
                file_write_tasks.spawn(async move {
                    while let Some(batch) = rx.recv().await {
                        writer.write(&batch).await?;
                        reservation.try_resize(writer.memory_size())?;
                    }
                    let parquet_meta_data = writer
                        .close()
                        .await
                        .map_err(|e| DataFusionError::ParquetError(Box::new(e)))?;
                    Ok((path, parquet_meta_data))
                });
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
                let schema = get_writer_schema(&self.config);
                let props = parquet_props.clone();
                let skip_arrow_metadata = self.parquet_options.global.skip_arrow_metadata;
                let parallel_options_clone = parallel_options.clone();
                let pool = Arc::clone(context.memory_pool());
                file_write_tasks.spawn(async move {
                    let parquet_meta_data = output_single_parquet_file_parallelized(
                        writer,
                        rx,
                        schema,
                        &props,
                        skip_arrow_metadata,
                        parallel_options_clone,
                        pool,
                    )
                    .await?;
                    Ok((path, parquet_meta_data))
                });
            }
        }

        let mut row_count = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    let (path, parquet_meta_data) = r?;
                    row_count += parquet_meta_data.file_metadata().num_rows();
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
    col_writers: Vec<ArrowColumnWriter>,
    max_buffer_size: usize,
    pool: &Arc<dyn MemoryPool>,
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
                .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
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
    row_group_writer_factory: ArrowRowGroupWriterFactory,
    mut data: Receiver<RecordBatch>,
    serialize_tx: Sender<SpawnedTask<RBStreamSerializeResult>>,
    schema: Arc<Schema>,
    writer_props: Arc<WriterProperties>,
    parallel_options: Arc<ParallelParquetWriterOptions>,
    pool: Arc<dyn MemoryPool>,
) -> SpawnedTask<Result<(), DataFusionError>> {
    SpawnedTask::spawn(async move {
        let max_buffer_rb = parallel_options.max_buffered_record_batches_per_stream;
        let max_row_group_rows = writer_props.max_row_group_size();
        let mut row_group_index = 0;
        let col_writers =
            row_group_writer_factory.create_column_writers(row_group_index)?;
        let (mut column_writer_handles, mut col_array_channels) =
            spawn_column_parallel_row_group_writer(col_writers, max_buffer_rb, &pool)?;
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

                    row_group_index += 1;
                    let col_writers = row_group_writer_factory
                        .create_column_writers(row_group_index)?;
                    (column_writer_handles, col_array_channels) =
                        spawn_column_parallel_row_group_writer(
                            col_writers,
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
    mut parquet_writer: SerializedFileWriter<SharedBuffer>,
    merged_buff: SharedBuffer,
    mut serialize_rx: Receiver<SpawnedTask<RBStreamSerializeResult>>,
    mut object_store_writer: Box<dyn AsyncWrite + Send + Unpin>,
    pool: Arc<dyn MemoryPool>,
) -> Result<ParquetMetaData> {
    let mut file_reservation =
        MemoryConsumer::new("ParquetSink(SerializedFileWriter)").register(&pool);

    while let Some(task) = serialize_rx.recv().await {
        let result = task.join_unwind().await;
        let (serialized_columns, mut rg_reservation, _cnt) =
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
    output_schema: Arc<Schema>,
    parquet_props: &WriterProperties,
    skip_arrow_metadata: bool,
    parallel_options: ParallelParquetWriterOptions,
    pool: Arc<dyn MemoryPool>,
) -> Result<ParquetMetaData> {
    let max_rowgroups = parallel_options.max_parallel_row_groups;
    // Buffer size of this channel limits maximum number of RowGroups being worked on in parallel
    let (serialize_tx, serialize_rx) =
        mpsc::channel::<SpawnedTask<RBStreamSerializeResult>>(max_rowgroups);

    let arc_props = Arc::new(parquet_props.clone());
    let merged_buff = SharedBuffer::new(INITIAL_BUFFER_BYTES);
    let options = ArrowWriterOptions::new()
        .with_properties(parquet_props.clone())
        .with_skip_arrow_metadata(skip_arrow_metadata);
    let writer = ArrowWriter::try_new_with_options(
        merged_buff.clone(),
        Arc::clone(&output_schema),
        options,
    )?;
    let (writer, row_group_writer_factory) = writer.into_serialized_writer()?;

    let launch_serialization_task = spawn_parquet_parallel_serialization_task(
        row_group_writer_factory,
        data,
        serialize_tx,
        Arc::clone(&output_schema),
        Arc::clone(&arc_props),
        parallel_options.into(),
        Arc::clone(&pool),
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
    use parquet::arrow::parquet_to_arrow_schema;
    use std::sync::Arc;

    use super::*;

    use arrow::datatypes::DataType;
    use parquet::schema::parser::parse_message_type;

    #[test]
    fn coerce_int96_to_resolution_with_mixed_timestamps() {
        // Unclear if Spark (or other writer) could generate a file with mixed timestamps like this,
        // but we want to test the scenario just in case since it's at least a valid schema as far
        // as the Parquet spec is concerned.
        let spark_schema = "
        message spark_schema {
          optional int96 c0;
          optional int64 c1 (TIMESTAMP(NANOS,true));
          optional int64 c2 (TIMESTAMP(NANOS,false));
          optional int64 c3 (TIMESTAMP(MILLIS,true));
          optional int64 c4 (TIMESTAMP(MILLIS,false));
          optional int64 c5 (TIMESTAMP(MICROS,true));
          optional int64 c6 (TIMESTAMP(MICROS,false));
        }
        ";

        let schema = parse_message_type(spark_schema).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let arrow_schema = parquet_to_arrow_schema(&descr, None).unwrap();

        let result =
            coerce_int96_to_resolution(&descr, &arrow_schema, &TimeUnit::Microsecond)
                .unwrap();

        // Only the first field (c0) should be converted to a microsecond timestamp because it's the
        // only timestamp that originated from an INT96.
        let expected_schema = Schema::new(vec![
            Field::new("c0", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new(
                "c1",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("c2", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
            Field::new(
                "c3",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("c4", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new(
                "c5",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("c6", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]);

        assert_eq!(result, expected_schema);
    }

    #[test]
    fn coerce_int96_to_resolution_with_nested_types() {
        // This schema is derived from Comet's CometFuzzTestSuite ParquetGenerator only using int96
        // primitive types with generateStruct, generateArray, and generateMap set to true, with one
        // additional field added to c4's struct to make sure all fields in a struct get modified.
        // https://github.com/apache/datafusion-comet/blob/main/spark/src/main/scala/org/apache/comet/testing/ParquetGenerator.scala
        let spark_schema = "
        message spark_schema {
          optional int96 c0;
          optional group c1 {
            optional int96 c0;
          }
          optional group c2 {
            optional group c0 (LIST) {
              repeated group list {
                optional int96 element;
              }
            }
          }
          optional group c3 (LIST) {
            repeated group list {
              optional int96 element;
            }
          }
          optional group c4 (LIST) {
            repeated group list {
              optional group element {
                optional int96 c0;
                optional int96 c1;
              }
            }
          }
          optional group c5 (MAP) {
            repeated group key_value {
              required int96 key;
              optional int96 value;
            }
          }
          optional group c6 (LIST) {
            repeated group list {
              optional group element (MAP) {
                repeated group key_value {
                  required int96 key;
                  optional int96 value;
                }
              }
            }
          }
        }
        ";

        let schema = parse_message_type(spark_schema).expect("should parse schema");
        let descr = SchemaDescriptor::new(Arc::new(schema));

        let arrow_schema = parquet_to_arrow_schema(&descr, None).unwrap();

        let result =
            coerce_int96_to_resolution(&descr, &arrow_schema, &TimeUnit::Microsecond)
                .unwrap();

        let expected_schema = Schema::new(vec![
            Field::new("c0", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new_struct(
                "c1",
                vec![Field::new(
                    "c0",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                )],
                true,
            ),
            Field::new_struct(
                "c2",
                vec![Field::new_list(
                    "c0",
                    Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    true,
                )],
                true,
            ),
            Field::new_list(
                "c3",
                Field::new(
                    "element",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                true,
            ),
            Field::new_list(
                "c4",
                Field::new_struct(
                    "element",
                    vec![
                        Field::new(
                            "c0",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            true,
                        ),
                        Field::new(
                            "c1",
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            true,
                        ),
                    ],
                    true,
                ),
                true,
            ),
            Field::new_map(
                "c5",
                "key_value",
                Field::new(
                    "key",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "value",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                false,
                true,
            ),
            Field::new_list(
                "c6",
                Field::new_map(
                    "element",
                    "key_value",
                    Field::new(
                        "key",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        false,
                    ),
                    Field::new(
                        "value",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    false,
                    true,
                ),
                true,
            ),
        ]);

        assert_eq!(result, expected_schema);
    }
}
