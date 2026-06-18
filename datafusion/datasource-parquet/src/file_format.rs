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

use std::fmt;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

// Re-export so the historical `file_format::*` paths still resolve.
#[expect(deprecated)]
pub use crate::schema_coercion::{
    Int96Coercer, apply_file_schema_type_coercions, coerce_file_schema_to_string_type,
    coerce_file_schema_to_view_type, coerce_int96_to_resolution,
    transform_binary_to_string, transform_schema_to_view,
};
pub use crate::sink::ParquetSink;

use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_sink_config::FileSinkConfig;

use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};

use datafusion_common::Statistics;
use datafusion_common::config::{ConfigField, ConfigFileType, TableParquetOptions};
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    DEFAULT_PARQUET_EXTENSION, DataFusionError, GetExt, Result, internal_datafusion_err,
    internal_err, not_impl_err,
};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_session::Session;

use crate::metadata::{DFParquetMetadata, lex_ordering_to_sorting_columns};
use crate::reader::CachedParquetFileReaderFactory;
use crate::source::{
    ParquetSource, parse_coerce_int96_string, parse_coerce_int96_tz_string,
};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::cache::cache_manager::FileMetadataCache;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;

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
        Some(cfd) => Some(Arc::new(FileDecryptionProperties::try_from(cfd.clone())?)),
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
        let coerce_int96_tz = self
            .options
            .global
            .coerce_int96_tz
            .as_ref()
            .map(|tz| parse_coerce_int96_tz_string(tz))
            .transpose()?;

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
                    .with_coerce_int96_tz(coerce_int96_tz.clone())
                    .fetch_schema_with_location()
                    .await?;
                Ok::<_, DataFusionError>(result)
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            // fetch schemas concurrently, if requested
            .buffer_unordered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        // Schema inference adds fields based the order they are seen
        // which depends on the order the files are processed. For some
        // object stores (like local file systems) the order returned from list
        // is not deterministic. Thus, to ensure deterministic schema inference
        // sort the files first.
        // https://github.com/apache/datafusion/pull/6629
        schemas
            .sort_unstable_by(|(location1, _), (location2, _)| location1.cmp(location2));

        let schemas = schemas.into_iter().map(|(_, schema)| schema);

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

    async fn infer_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Option<LexOrdering>> {
        let file_decryption_properties =
            get_file_decryption_properties(state, &self.options, &object.location)
                .await?;
        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(self.metadata_size_hint())
            .with_decryption_properties(file_decryption_properties)
            .with_file_metadata_cache(Some(file_metadata_cache))
            .fetch_metadata()
            .await?;
        crate::metadata::ordering_from_parquet_metadata(&metadata, &table_schema)
    }

    async fn infer_stats_and_ordering(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<datafusion_datasource::file_format::FileMeta> {
        let file_decryption_properties =
            get_file_decryption_properties(state, &self.options, &object.location)
                .await?;
        let file_metadata_cache =
            state.runtime_env().cache_manager.get_file_metadata_cache();
        let metadata = DFParquetMetadata::new(store, object)
            .with_metadata_size_hint(self.metadata_size_hint())
            .with_decryption_properties(file_decryption_properties)
            .with_file_metadata_cache(Some(file_metadata_cache))
            .fetch_metadata()
            .await?;
        let statistics = DFParquetMetadata::statistics_from_parquet_metadata(
            &metadata,
            &table_schema,
        )?;
        let ordering =
            crate::metadata::ordering_from_parquet_metadata(&metadata, &table_schema)?;
        Ok(
            datafusion_datasource::file_format::FileMeta::new(statistics)
                .with_ordering(ordering),
        )
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

        // Convert ordering requirements to Parquet SortingColumns for file metadata
        let sorting_columns = if let Some(ref requirements) = order_requirements {
            let ordering: LexOrdering = requirements.clone().into();
            // In cases like `COPY (... ORDER BY ...) TO ...` the ORDER BY clause
            // may not be compatible with Parquet sorting columns (e.g. ordering on `random()`).
            // So if we cannot create a Parquet sorting column from the ordering requirement,
            // we skip setting sorting columns on the Parquet sink.
            lex_ordering_to_sorting_columns(&ordering).ok()
        } else {
            None
        };

        let sink = Arc::new(
            ParquetSink::new(conf, self.options.clone())
                .with_sorting_columns(sorting_columns),
        );

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
            Err(DataFusionError::Configuration(format!(
                "Parquet encryption factory id is set to '{encryption_factory_id}' but the parquet_encryption feature is disabled"
            )))
        } else {
            Ok(source)
        }
    }
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
    file_metadata_cache: Option<Arc<FileMetadataCache>>,
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
    file_metadata_cache: Option<Arc<FileMetadataCache>>,
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
