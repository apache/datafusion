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

//! ParquetSource implementation for reading parquet files
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::DefaultParquetFileReaderFactory;
use crate::ParquetFileReaderFactory;
use crate::opener::ParquetMorselizer;
use crate::opener::build_pruning_predicates;
use crate::opener::build_virtual_columns_state;
use crate::row_filter::can_expr_be_pushed_down_with_schemas;
use arrow_schema::Fields;
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field};
use datafusion_common::config::ConfigOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
use datafusion_datasource::as_file_source;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::morsel::Morselizer;

use arrow::array::timezone::Tz;
use arrow::datatypes::TimeUnit;
use datafusion_common::DataFusionError;
use datafusion_common::config::TableParquetOptions;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_functions::core::file_row_index::FileRowIndexFunc;
use datafusion_physical_expr::expressions::{Column, DynamicFilterTracking};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::{EquivalenceProperties, conjunction};
use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
use datafusion_physical_expr_adapter::rewrite::{
    expr_references_scalar_udf, rewrite_file_row_index_projection,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::SortOrderPushdownResult;
use datafusion_physical_plan::filter_pushdown::PushedDown;
use datafusion_physical_plan::filter_pushdown::{
    FilterPushdownPropagation, PushedDownPredicate,
};
use datafusion_physical_plan::metrics::Count;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use log::warn;

#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use itertools::Itertools;
use object_store::ObjectStore;
use parquet::arrow::RowNumber;
#[cfg(feature = "parquet_encryption")]
use parquet::encryption::decrypt::FileDecryptionProperties;

/// Execution plan for reading one or more Parquet files.
///
/// ```text
///             ▲
///             │
///             │  Produce a stream of
///             │  RecordBatches
///             │
/// ┌───────────────────────┐
/// │                       │
/// │     DataSourceExec    │
/// │                       │
/// └───────────────────────┘
///             ▲
///             │  Asynchronously read from one
///             │  or more parquet files via
///             │  ObjectStore interface
///             │
///             │
///   .───────────────────.
///  │                     )
///  │`───────────────────'│
///  │    ObjectStore      │
///  │.───────────────────.│
///  │                     )
///   `───────────────────'
/// ```
///
/// # Example: Create a `DataSourceExec`
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource_parquet::source::ParquetSource;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_physical_expr::expressions::lit;
/// # use datafusion_datasource::source::DataSourceExec;
/// # use datafusion_common::config::TableParquetOptions;
///
/// # let file_schema = Arc::new(Schema::empty());
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// # let predicate = lit(true);
/// let source = Arc::new(
///     ParquetSource::new(Arc::clone(&file_schema))
///         .with_predicate(predicate)
/// );
/// // Create a DataSourceExec for reading `file1.parquet` with a file size of 100MB
/// let config = FileScanConfigBuilder::new(object_store_url, source)
///    .with_file(PartitionedFile::new("file1.parquet", 100*1024*1024)).build();
/// let exec = DataSourceExec::from_data_source(config);
/// ```
///
/// # Features
///
/// Supports the following optimizations:
///
/// * Concurrent reads: reads from one or more files in parallel as multiple
///   partitions, including concurrently reading multiple row groups from a single
///   file.
///
/// * Predicate push down: skips row groups, pages, rows based on metadata
///   and late materialization. See "Predicate Pushdown" below.
///
/// * Projection pushdown: reads and decodes only the columns required.
///
/// * Limit pushdown: stop execution early after some number of rows are read.
///
/// * Custom readers: customize reading  parquet files, e.g. to cache metadata,
///   coalesce I/O operations, etc. See [`ParquetFileReaderFactory`] for more
///   details.
///
/// * Schema evolution: read parquet files with different schemas into a unified
///   table schema. See [`DefaultPhysicalExprAdapterFactory`] for more details.
///
/// * metadata_size_hint: controls the number of bytes read from the end of the
///   file in the initial I/O when the default [`ParquetFileReaderFactory`]. If a
///   custom reader is used, it supplies the metadata directly and this parameter
///   is ignored. [`ParquetSource::with_metadata_size_hint`] for more details.
///
/// * User provided  `ParquetAccessPlan`s to skip row groups and/or pages
///   based on external information. See "Implementing External Indexes" below
///
/// # Predicate Pushdown
///
/// `DataSourceExec` uses the provided [`PhysicalExpr`] predicate as a filter to
/// skip reading unnecessary data and improve query performance using several techniques:
///
/// * Row group pruning: skips entire row groups based on min/max statistics
///   found in [`ParquetMetaData`] and any Bloom filters that are present.
///
/// * Page pruning: skips individual pages within a ColumnChunk using the
///   [Parquet PageIndex], if present.
///
/// * Row filtering: skips rows within a page using a form of late
///   materialization. When possible, predicates are applied by the parquet
///   decoder *during* decode (see [`ArrowPredicate`] and [`RowFilter`] for more
///   details). This is only enabled if `ParquetScanOptions::pushdown_filters` is set to true.
///
/// Note: If the predicate can not be used to accelerate the scan, it is ignored
/// (no error is raised on predicate evaluation errors).
///
/// [`ArrowPredicate`]: parquet::arrow::arrow_reader::ArrowPredicate
/// [`RowFilter`]: parquet::arrow::arrow_reader::RowFilter
/// [Parquet PageIndex]: https://github.com/apache/parquet-format/blob/master/PageIndex.md
///
/// # Example: rewriting `DataSourceExec`
///
/// You can modify a `DataSourceExec` using [`ParquetSource`], for example
/// to change files or add a predicate.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource::source::DataSourceExec;
///
/// # fn parquet_exec() -> DataSourceExec { unimplemented!() }
/// // Split a single DataSourceExec into multiple DataSourceExecs, one for each file
/// let exec = parquet_exec();
/// let data_source = exec.data_source();
/// let base_config = data_source.downcast_ref::<FileScanConfig>().unwrap();
/// let existing_file_groups = &base_config.file_groups;
/// let new_execs = existing_file_groups
///   .iter()
///   .map(|file_group| {
///     // create a new exec by copying the existing exec's source config
///     let new_config = FileScanConfigBuilder::from(base_config.clone())
///        .with_file_groups(vec![file_group.clone()])
///       .build();
///
///     (DataSourceExec::from_data_source(new_config))
///   })
///   .collect::<Vec<_>>();
/// ```
///
/// # Implementing External Indexes
///
/// It is possible to restrict the row groups and selections within those row
/// groups that the DataSourceExec will consider by providing an initial
/// `ParquetAccessPlan` as `extensions` on `PartitionedFile`. This can be
/// used to implement external indexes on top of parquet files and select only
/// portions of the files.
///
/// If the external index naturally produces a file-level
/// [`RowSelection`](parquet::arrow::arrow_reader::RowSelection), wrap it in
/// [`ParquetRowSelection`](crate::ParquetRowSelection) and provide it as an
/// extension. DataFusion will use the parquet metadata to split the selection
/// into row-group-level access.
///
/// The `DataSourceExec` will try and reduce any provided `ParquetAccessPlan`
/// further based on the contents of `ParquetMetadata` and other settings.
///
/// ## Example of providing a ParquetAccessPlan
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Schema, SchemaRef};
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource_parquet::ParquetAccessPlan;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource_parquet::source::ParquetSource;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_datasource::source::DataSourceExec;
///
/// # fn schema() -> SchemaRef {
/// #   Arc::new(Schema::empty())
/// # }
/// // create an access plan to scan row group 0, 1 and 3 and skip row groups 2 and 4
/// let mut access_plan = ParquetAccessPlan::new_all(5);
/// access_plan.skip(2);
/// access_plan.skip(4);
/// // provide the plan as extension to the FileScanConfig
/// let partitioned_file = PartitionedFile::new("my_file.parquet", 1234)
///   .with_extension(access_plan);
/// // create a FileScanConfig to scan this file
/// let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(ParquetSource::new(schema())))
///     .with_file(partitioned_file).build();
/// // this parquet DataSourceExec will not even try to read row groups 2 and 4. Additional
/// // pruning based on predicates may also happen
/// let exec = DataSourceExec::from_data_source(config);
/// ```
///
/// For a complete example, see the [`advanced_parquet_index` example]).
///
/// [`parquet_index_advanced` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/data_io/parquet_advanced_index.rs
///
/// # Execution Overview
///
/// * Step 1: `DataSourceExec::execute` is called, returning a `FileStream`
///   configured to morselize parquet files with a `ParquetMorselizer`.
///
/// * Step 2: When the stream is polled, the `ParquetMorselizer` is called to
///   plan the file.
///
/// * Step 3: The `ParquetMorselizer` gets the [`ParquetMetaData`] (file metadata)
///   via [`ParquetFileReaderFactory`], creating a `ParquetAccessPlan` by
///   applying predicates to metadata. The plan and projections are used to
///   determine what pages must be read.
///
/// * Step 4: The stream begins reading data, fetching the required parquet
///   pages incrementally decoding them, and applying any row filters (see
///   [`Self::with_pushdown_filters`]).
///
/// * Step 5: As each [`RecordBatch`] is read, it may be adapted by a
///   [`DefaultPhysicalExprAdapterFactory`] to match the table schema. By default missing columns are
///   filled with nulls, but this can be customized via [`PhysicalExprAdapterFactory`].
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
/// [`ParquetMetadata`]: parquet::file::metadata::ParquetMetaData
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[derive(Clone, Debug)]
pub struct ParquetSource {
    /// Options for reading Parquet files
    pub(crate) table_parquet_options: TableParquetOptions,
    /// Optional metrics
    pub(crate) metrics: ExecutionPlanMetricsSet,
    /// The schema of the file.
    /// In particular, this is the schema of the table without partition columns,
    /// *not* the physical schema of the file.
    pub(crate) table_schema: TableSchema,
    /// Optional predicate for row filtering during parquet scan
    pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional user defined parquet file reader factory
    pub(crate) parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Batch size configuration
    pub(crate) batch_size: Option<usize>,
    /// Optional hint for the size of the parquet metadata
    pub(crate) metadata_size_hint: Option<usize>,
    /// Projection to apply to the output.
    pub(crate) projection: ProjectionExprs,
    #[cfg(feature = "parquet_encryption")]
    pub(crate) encryption_factory: Option<Arc<dyn EncryptionFactory>>,
    /// If true, the opener flips row-group iteration order. Within-
    /// row-group order is on-disk order, so the scan is `Inexact` and
    /// a `SortExec` is kept in the plan.
    reverse_row_groups: bool,
    /// Sort order driving `PreparedAccessPlan::reorder_by_statistics`
    /// in the opener.
    sort_order_for_reorder: Option<LexOrdering>,
}

impl ParquetSource {
    /// Create a new ParquetSource to read the data specified in the file scan
    /// configuration with the provided schema.
    ///
    /// Uses default `TableParquetOptions`.
    /// To set custom options, use [ParquetSource::with_table_parquet_options`].
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        // Projection over the full table schema (file columns + partition columns)
        let full_schema = table_schema.table_schema();
        let indices: Vec<usize> = (0..full_schema.fields().len()).collect();
        Self {
            projection: ProjectionExprs::from_indices(&indices, full_schema),
            table_schema,
            table_parquet_options: TableParquetOptions::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            predicate: None,
            parquet_file_reader_factory: None,
            batch_size: None,
            metadata_size_hint: None,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: None,
            reverse_row_groups: false,
            sort_order_for_reorder: None,
        }
    }

    /// Set the `TableParquetOptions` for this ParquetSource.
    pub fn with_table_parquet_options(
        mut self,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        self.table_parquet_options = table_parquet_options;
        self
    }

    /// Set the metadata size hint
    ///
    /// This value determines how many bytes at the end of the file the default
    /// [`ParquetFileReaderFactory`] will request in the initial IO. If this is
    /// too small, the ParquetSource will need to make additional IO requests to
    /// read the footer.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Set predicate information.
    ///
    /// Predicates referencing virtual columns must go through
    /// [`Self::try_pushdown_filters`]. Passing them here with pushdown
    /// enabled trips a debug assert in the opener.
    #[expect(clippy::needless_pass_by_value)]
    pub fn with_predicate(&self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        let mut conf = self.clone();
        conf.predicate = Some(Arc::clone(&predicate));
        conf
    }

    /// Set the encryption factory to use to generate file decryption properties
    #[cfg(feature = "parquet_encryption")]
    pub fn with_encryption_factory(
        mut self,
        encryption_factory: Arc<dyn EncryptionFactory>,
    ) -> Self {
        self.encryption_factory = Some(encryption_factory);
        self
    }

    /// Options passed to the parquet reader for this scan
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.table_parquet_options
    }

    /// Optional predicate.
    #[deprecated(since = "50.2.0", note = "use `filter` instead")]
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }

    /// return the optional file reader factory
    pub fn parquet_file_reader_factory(
        &self,
    ) -> Option<&Arc<dyn ParquetFileReaderFactory>> {
        self.parquet_file_reader_factory.as_ref()
    }

    /// Optional user defined parquet file reader factory.
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// If true, the predicate will be used during the parquet scan.
    /// Defaults to false.
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        self.table_parquet_options.global.pushdown_filters = pushdown_filters;
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    pub(crate) fn pushdown_filters(&self) -> bool {
        self.table_parquet_options.global.pushdown_filters
    }

    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        self.table_parquet_options.global.reorder_filters = reorder_filters;
        self
    }

    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self) -> bool {
        self.table_parquet_options.global.reorder_filters
    }

    /// Return the value of [`datafusion_common::config::ParquetOptions::force_filter_selections`]
    fn force_filter_selections(&self) -> bool {
        self.table_parquet_options.global.force_filter_selections
    }

    /// If enabled, the reader will read the page index
    /// This is used to optimize filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    pub fn with_enable_page_index(mut self, enable_page_index: bool) -> Self {
        self.table_parquet_options.global.enable_page_index = enable_page_index;
        self
    }

    /// Return the value described in [`Self::with_enable_page_index`]
    fn enable_page_index(&self) -> bool {
        self.table_parquet_options.global.enable_page_index
    }

    /// If enabled, the reader will read by the bloom filter
    pub fn with_bloom_filter_on_read(mut self, bloom_filter_on_read: bool) -> Self {
        self.table_parquet_options.global.bloom_filter_on_read = bloom_filter_on_read;
        self
    }

    /// If enabled, the writer will write by the bloom filter
    pub fn with_bloom_filter_on_write(
        mut self,
        enable_bloom_filter_on_write: bool,
    ) -> Self {
        self.table_parquet_options.global.bloom_filter_on_write =
            enable_bloom_filter_on_write;
        self
    }

    /// Return the value described in [`Self::with_bloom_filter_on_read`]
    fn bloom_filter_on_read(&self) -> bool {
        self.table_parquet_options.global.bloom_filter_on_read
    }

    /// Return the maximum predicate cache size, in bytes, used when
    /// `pushdown_filters`
    pub fn max_predicate_cache_size(&self) -> Option<usize> {
        self.table_parquet_options.global.max_predicate_cache_size
    }

    #[cfg(feature = "parquet_encryption")]
    fn get_encryption_factory_with_config(
        &self,
    ) -> Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)> {
        match &self.encryption_factory {
            None => None,
            Some(factory) => Some((
                Arc::clone(factory),
                self.table_parquet_options.crypto.factory_options.clone(),
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_reverse_row_groups(mut self, reverse_row_groups: bool) -> Self {
        self.reverse_row_groups = reverse_row_groups;
        self
    }
    #[cfg(test)]
    pub(crate) fn reverse_row_groups(&self) -> bool {
        self.reverse_row_groups
    }
}

/// Parses datafusion.common.config.ParquetOptions.coerce_int96 String to a arrow_schema.datatype.TimeUnit
pub(crate) fn parse_coerce_int96_string(
    str_setting: &str,
) -> datafusion_common::Result<TimeUnit> {
    let str_setting_lower: &str = &str_setting.to_lowercase();

    match str_setting_lower {
        "ns" => Ok(TimeUnit::Nanosecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ms" => Ok(TimeUnit::Millisecond),
        "s" => Ok(TimeUnit::Second),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet coerce_int96: \
        {str_setting}. Valid values are: ns, us, ms, and s."
        ))),
    }
}

/// Validates that `tz` is a parseable IANA timezone and returns it as an
/// `Arc<str>` for use in `Timestamp(_, Some(tz))` types.
pub(crate) fn parse_coerce_int96_tz_string(
    tz: &str,
) -> datafusion_common::Result<Arc<str>> {
    tz.parse::<Tz>().map_err(|e| {
        DataFusionError::Configuration(format!(
            "Invalid parquet coerce_int96_tz {tz:?}: {e}"
        ))
    })?;
    Ok(Arc::<str>::from(tz))
}

/// Allows easy conversion from ParquetSource to Arc&lt;dyn FileSource&gt;
impl From<ParquetSource> for Arc<dyn FileSource> {
    fn from(source: ParquetSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for ParquetSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion_common::Result<Arc<dyn FileOpener>> {
        datafusion_common::internal_err!(
            "ParquetSource::create_file_opener called but it supports the Morsel API, please use that instead"
        )
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion_common::Result<Box<dyn Morselizer>> {
        let expr_adapter_factory = base_config
            .expr_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory) as _);

        let parquet_file_reader_factory =
            self.parquet_file_reader_factory.clone().unwrap_or_else(|| {
                Arc::new(DefaultParquetFileReaderFactory::new(object_store)) as _
            });

        #[cfg(feature = "parquet_encryption")]
        let file_decryption_properties = self
            .table_parquet_options()
            .crypto
            .file_decryption
            .clone()
            .map(FileDecryptionProperties::try_from)
            .transpose()?
            .map(Arc::new);

        let coerce_int96 = self
            .table_parquet_options
            .global
            .coerce_int96
            .as_ref()
            .map(|time_unit| parse_coerce_int96_string(time_unit.as_str()).unwrap());
        let coerce_int96_tz = self
            .table_parquet_options
            .global
            .coerce_int96_tz
            .as_ref()
            .map(|tz| parse_coerce_int96_tz_string(tz))
            .transpose()?;
        if coerce_int96_tz.is_some() && coerce_int96.is_none() {
            warn!(
                "coerce_int96_tz is set but coerce_int96 is not; the timezone will be ignored"
            );
        }

        // Validate virtual columns (extension-type allowlist) and, when
        // pushdown is enabled, reject predicates that reference them. Both
        // checks depend only on morselizer-level state, so we pay their cost
        // once per scan partition rather than per file.
        //
        // Gating predicate validation on `pushdown_filters` is deliberate:
        // when pushdown is off the predicate stays above the scan as a
        // `FilterExec` and resolves virtual columns there; the row-filter
        // ban only applies to the pushdown path.
        let virtual_state = build_virtual_columns_state(
            self.table_schema.virtual_columns(),
            self.table_schema.file_schema(),
            self.predicate.as_ref(),
            self.pushdown_filters(),
        )?;

        Ok(Box::new(ParquetMorselizer {
            partition_index: partition,
            projection: self.projection.clone(),
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating ParquetMorselizer"),
            limit: base_config.limit,
            preserve_order: base_config.preserve_order,
            predicate: self.predicate.clone(),
            table_schema: self.table_schema.clone(),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics().clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(),
            reorder_filters: self.reorder_filters(),
            force_filter_selections: self.force_filter_selections(),
            enable_page_index: self.enable_page_index(),
            enable_bloom_filter: self.bloom_filter_on_read(),
            enable_row_group_stats_pruning: self.table_parquet_options.global.pruning,
            coerce_int96,
            coerce_int96_tz,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties,
            expr_adapter_factory,
            #[cfg(feature = "parquet_encryption")]
            encryption_factory: self.get_encryption_factory_with_config(),
            max_predicate_cache_size: self.max_predicate_cache_size(),
            reverse_row_groups: self.reverse_row_groups,
            sort_order_for_reorder: self.sort_order_for_reorder.clone(),
            virtual_state,
        }))
    }

    fn reorder_files(
        &self,
        files: Vec<datafusion_datasource::PartitionedFile>,
    ) -> Vec<datafusion_datasource::PartitionedFile> {
        crate::sort::reorder_files_by_min_statistics(
            files,
            self.sort_order_for_reorder.as_ref(),
            self.reverse_row_groups,
            self.table_schema.table_schema(),
        )
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate.clone()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion_common::Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();

        // If there's no reference to `FileRowIndexFunc` in the projection, we can just merge
        // both projections as-is, there's no need to modify the projection first.
        if !projection.iter().any(|projection_expr| {
            expr_references_scalar_udf::<FileRowIndexFunc>(&projection_expr.expr)
        }) {
            source.projection = self.projection.try_merge(projection)?;
            return Ok(Some(Arc::new(source)));
        }

        // If we can find a reference to `FileRowIndexFunc`, we add it as a virtual column
        // or re-use an existing one in the table's schema.
        let (table_schema, row_index_col) =
            table_schema_with_row_index_col(self.table_schema());

        source.table_schema = table_schema;
        source.projection = rewrite_file_row_index_projection(
            &self.projection,
            projection,
            &row_index_col,
        )?;

        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "parquet"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let predicate_string = self
                    .filter()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                write!(f, "{predicate_string}")?;

                // Inexact sort-pushdown markers: surface both flags so
                // readers can see the optimization fired.
                if let Some(sort_order) = &self.sort_order_for_reorder {
                    write!(f, ", sort_order_for_reorder=[{sort_order}]")?;
                }
                if self.reverse_row_groups {
                    write!(f, ", reverse_row_groups=true")?;
                }

                // Plan-time marker for dynamic RG-level pruning: if the
                // predicate is dynamic (e.g. a TopK threshold expression),
                // the parquet opener will pause the single decoder at row
                // group boundaries and consult `RowGroupPruner` to drop
                // RGs the current threshold proves unwinnable, rebuilding
                // the decoder via `into_builder().with_row_groups(...)` to
                // skip them. The actual pruning count appears as
                // `row_groups_pruned_dynamic_filter` in EXPLAIN ANALYZE.
                // We use `contains_dynamic_filter()` (matches both `Watching`
                // and `AllComplete`) rather than the stricter `Watching(_)`
                // check the opener uses to construct the pruner. Reason: the
                // opener gate is evaluated at file-open time, when a TopK
                // threshold has not yet been pushed — at that moment a still-
                // useful pruner needs `Watching`. `fmt_extra`, on the other
                // hand, is called *also* by `EXPLAIN ANALYZE` after execution
                // completes, at which point TopK has marked its dynamic
                // filter complete and `classify` returns `AllComplete`. The
                // marker is plan-time metadata ("this scan was eligible for
                // runtime RG pruning"), so it should still show in that
                // post-run rendering.
                if let Some(predicate) = self.filter()
                    && DynamicFilterTracking::classify(&predicate)
                        .contains_dynamic_filter()
                {
                    write!(f, ", dynamic_rg_pruning=eligible")?;
                }

                // Try to build the pruning predicates.
                // These are only generated here because it's useful to have *some*
                // idea of what pushdown is happening when viewing plans.
                // However, it is important to note that these predicates are *not*
                // necessarily the predicates that are actually evaluated:
                // the actual predicates are built in reference to the physical schema of
                // each file, which we do not have at this point and hence cannot use.
                // Instead, we use the logical schema of the file (the table schema without partition columns).
                if let Some(predicate) = &self.predicate {
                    let predicate_creation_errors = Count::new();
                    if let Some(pruning_predicate) = build_pruning_predicates(
                        Some(predicate),
                        self.table_schema.table_schema(),
                        &predicate_creation_errors,
                    ) {
                        let mut guarantees = pruning_predicate
                            .literal_guarantees()
                            .iter()
                            .map(|item| format!("{item}"))
                            .collect_vec();
                        guarantees.sort();
                        write!(
                            f,
                            ", pruning_predicate={}, required_guarantees=[{}]",
                            pruning_predicate.predicate_expr(),
                            guarantees.join(", ")
                        )?;
                    }
                };
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                if let Some(predicate) = self.filter() {
                    writeln!(f, "predicate={}", fmt_sql(predicate.as_ref()))?;
                }
                Ok(())
            }
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> datafusion_common::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        // Use the schema excluding virtual columns: virtual columns (e.g.
        // Parquet `row_number`) are produced by the reader itself and cannot
        // be referenced inside a RowFilter, so predicates that reference them
        // must not be marked as pushed down — otherwise the scan would
        // silently drop them and produce wrong results.
        let pushable_schema = self.table_schema.schema_without_virtual_columns();
        // Determine if based on configs we should push filters down.
        // If either the table / scan itself or the config has pushdown enabled,
        // we will push down the filters.
        // If both are disabled, we will not push down the filters.
        // By default they are both disabled.
        // Regardless of pushdown, we will update the predicate to include the filters
        // because even if scan pushdown is disabled we can still use the filters for stats pruning.
        let config_pushdown_enabled = config.execution.parquet.pushdown_filters;
        let table_pushdown_enabled = self.pushdown_filters();
        let pushdown_filters = table_pushdown_enabled || config_pushdown_enabled;

        let mut source = self.clone();
        let filters: Vec<PushedDownPredicate> = filters
            .into_iter()
            .map(|filter| {
                if can_expr_be_pushed_down_with_schemas(&filter, pushable_schema) {
                    PushedDownPredicate::supported(filter)
                } else {
                    PushedDownPredicate::unsupported(filter)
                }
            })
            .collect();
        if filters
            .iter()
            .all(|f| matches!(f.discriminant, PushedDown::No))
        {
            // No filters can be pushed down, so we can just return the remaining filters
            // and avoid replacing the source in the physical plan.
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            ));
        }
        let allowed_filters = filters
            .iter()
            .filter_map(|f| match f.discriminant {
                PushedDown::Yes => Some(Arc::clone(&f.predicate)),
                PushedDown::No => None,
            })
            .collect_vec();
        let predicate = match source.predicate {
            Some(predicate) => {
                conjunction(std::iter::once(predicate).chain(allowed_filters))
            }
            None => conjunction(allowed_filters),
        };
        source.predicate = Some(predicate);
        source = source.with_pushdown_filters(pushdown_filters);
        let source = Arc::new(source);
        // If pushdown_filters is false we tell our parents that they still have to handle the filters,
        // even if we updated the predicate to include the filters (they will only be used for stats pruning).
        if !pushdown_filters {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                vec![PushedDown::No; filters.len()],
            )
            .with_updated_node(source));
        }
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            filters.iter().map(|f| f.discriminant).collect(),
        )
        .with_updated_node(source))
    }

    /// Try to optimize the scan to produce data in the requested sort order.
    ///
    /// Inputs:
    /// 1. The query's required ordering (`order` parameter)
    /// 2. The source's equivalence properties (`eq_properties`)
    ///
    /// # Returns
    /// - `Exact`: the source's natural ordering already satisfies the
    ///   request. The surrounding `SortExec` can be eliminated provided
    ///   files within each group are non-overlapping (verified by
    ///   `FileScanConfig`).
    /// - `Inexact`: the source can approximate the request via two
    ///   composable runtime steps — stats-based row-group reorder
    ///   (skipped when the leading sort key isn't a plain `Column`
    ///   in the file schema) and row-group iteration reverse. A
    ///   `SortExec` is still required for full correctness, but limit
    ///   pushdown and `TopK` benefit immediately.
    /// - `Unsupported`: no approximation is available.
    ///
    /// # How the Inexact result is communicated
    ///
    /// The result is carried through two fields on `ParquetSource`:
    ///
    /// - `sort_order_for_reorder`: set to the request's `LexOrdering`
    ///   whenever the pushdown fires, regardless of whether the
    ///   leading expression is a plain `Column`. The opener invokes
    ///   `PreparedAccessPlan::reorder_by_statistics`, which skips
    ///   when the expression can't be looked up in parquet metadata.
    ///   Exposing the field unconditionally keeps `EXPLAIN` honest
    ///   about what the source was asked to approximate.
    /// - `reverse_row_groups`: drives the opener's iteration flip.
    ///   When stats reorder applies (column-in-schema), this is just
    ///   the request's direction — the reorder produces ASC-by-min,
    ///   so reverse iff the query asks for DESC. When stats reorder
    ///   doesn't apply but the reversed source ordering satisfies
    ///   the request (function-wrapped case), this is always `true`
    ///   because we're flipping the file's natural order.
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> datafusion_common::Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        if order.is_empty() {
            return Ok(SortOrderPushdownResult::Unsupported);
        }

        // Check if the natural (non-reversed) ordering already satisfies the request.
        // Parquet metadata guarantees within-file ordering, so if the ordering matches
        // we can return Exact. FileScanConfig will verify that files within each group
        // are non-overlapping before declaring the entire scan as Exact.
        if eq_properties.ordering_satisfy(order.iter().cloned())? {
            return Ok(SortOrderPushdownResult::Exact {
                inner: Arc::new(self.clone()) as Arc<dyn FileSource>,
            });
        }

        // If the source's declared ordering is a non-empty *proper* prefix
        // of the request (e.g. source `[a DESC, b ASC]`, request
        // `[a DESC, b ASC, c DESC]`), decline pushdown so the outer
        // `SortExec`'s `sort_prefix` optimisation — prefix-aware early
        // termination in `TopK` — can still fire. Firing the Inexact
        // pipeline below would invalidate the source's `output_ordering`
        // (the runtime row-group reorder is approximate, so we can't
        // honour the declared ordering anymore), which is exactly what
        // `EnforceSorting` needs to derive `sort_prefix`. On data that
        // is already in prefix order the stats-based reorder is mostly
        // a no-op anyway, so the trade-off is plainly bad.
        for prefix_len in 1..order.len() {
            let prefix = order[..prefix_len].to_vec();
            if eq_properties.ordering_satisfy(prefix.iter().cloned())? {
                return Ok(SortOrderPushdownResult::Unsupported);
            }
        }

        // Inexact pushdown. Two independent signals; either is enough
        // to produce an approximate ordering, and they compose when
        // both apply:
        //
        // 1. `column_in_file_schema`: the request's leading sort key is
        //    a plain `Column` present in the file schema. The opener
        //    can sort row groups by that column's `min` via parquet
        //    statistics. Drives `sort_order_for_reorder`'s actual use.
        //
        // 2. `reversed_satisfies`: the source's declared ordering,
        //    when reversed, satisfies the request. This is strictly
        //    more powerful than the column-in-schema check because it
        //    runs the request through `EquivalenceProperties`'s full
        //    reasoning machinery:
        //
        //    - Function monotonicity: e.g. file declares
        //      `[extract_year_month(ws) DESC, ws DESC]`, request is
        //      `[ws ASC]`; the reversed ordering still satisfies the
        //      request via `extract_year_month`'s monotonicity even
        //      though parquet has no stats keyed by the function
        //      expression itself.
        //    - Constant columns from filters: equivalence classes can
        //      mark columns as constant under a predicate, allowing
        //      requested orderings on those columns to be trivially
        //      satisfied.
        //    - Other equivalence relationships (e.g. `a = b` transfers
        //      ordering between `a` and `b`).
        //
        //    `reorder_by_statistics` can't substitute for any of the
        //    above because it can only look up min/max for a plain
        //    physical column.
        //
        // `sort_order_for_reorder` is set in both cases so EXPLAIN
        // shows what the source was asked to approximate; the opener
        // skips stats-based reorder when the leading expression isn't
        // a plain `Column`.
        //
        // The reversal flips each `PhysicalSortExpr` (both descending
        // and nulls_first) and rebuilds an `EquivalenceProperties` so
        // the request can be tested against the reversed orderings
        // via the same `ordering_satisfy` API.
        let reversed_eq_properties = {
            let mut new = eq_properties.clone();
            new.clear_orderings();
            let reversed_orderings = eq_properties
                .oeq_class()
                .iter()
                .map(|ordering| {
                    ordering
                        .iter()
                        .map(|expr| expr.reverse())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            new.add_orderings(reversed_orderings);
            new
        };
        let reversed_satisfies =
            reversed_eq_properties.ordering_satisfy(order.iter().cloned())?;
        let sort_order = LexOrdering::new(order.iter().cloned());
        let column_in_file_schema = sort_order.as_ref().is_some_and(|s| {
            s.first().expr.downcast_ref::<Column>().is_some_and(|col| {
                self.table_schema
                    .file_schema()
                    .field_with_name(col.name())
                    .is_ok()
            })
        });

        if !column_in_file_schema && !reversed_satisfies {
            return Ok(SortOrderPushdownResult::Unsupported);
        }

        // `reverse_row_groups` has different starting points in the
        // two cases:
        // - With stats reorder (column-in-schema): the reorder produces
        //   ASC-by-min, so reverse iff the request is DESC.
        // - Without stats reorder (reversed-eq fallback): we flip the
        //   file's natural order, so always reverse.
        let is_descending = sort_order
            .as_ref()
            .is_some_and(|s| s.first().options.descending);
        let mut new_source = self.clone();
        new_source.sort_order_for_reorder = sort_order;
        new_source.reverse_row_groups = if column_in_file_schema {
            is_descending
        } else {
            true
        };
        Ok(SortOrderPushdownResult::Inexact {
            inner: Arc::new(new_source) as Arc<dyn FileSource>,
        })
    }
}

/// Returns the a [`TableSchema`] containing a [`RowNumber`] virtual column and a [`Column`] expression referencing its row index column.
/// The expression is then merged into a projection.
///
/// - If the schema already has a virtual column with the [`RowNumber`] type, it returns the schema unchanged.
/// - If the schema doesn't have the appropriate virtual column, it returns a modified schema with the virtual column appended to it.
fn table_schema_with_row_index_col(table_schema: &TableSchema) -> (TableSchema, Column) {
    // If we can find a virtual column with the `RowNumber` type, we just return the schema
    // and create the appropriate `column` we're going to use
    if let Some((idx, field)) =
        table_schema
            .virtual_columns()
            .iter()
            .enumerate()
            .find(|(_, field)| {
                field
                    .extension_type_name()
                    .is_some_and(|name| name == RowNumber::NAME)
            })
    {
        let virtual_offset = table_schema.file_schema().fields().len()
            + table_schema.table_partition_cols().len();

        return (
            table_schema.clone(),
            Column::new(field.name(), virtual_offset + idx),
        );
    }

    // The hidden field is shared across all files in this scan, but it must
    // have a unique table-schema name because later rewrites resolve it by
    // column name and index.
    let base_row_index_name = "__datafusion_file_row_index";
    let mut row_index_name = base_row_index_name.to_string();
    let mut suffix = 0;
    while table_schema
        .table_schema()
        .field_with_name(&row_index_name)
        .is_ok()
    {
        suffix += 1;
        row_index_name = format!("{base_row_index_name}_{suffix}");
    }

    let row_index_table_idx = table_schema.table_schema().fields().len();
    let row_index_field = Arc::new(
        Field::new(&row_index_name, DataType::Int64, true).with_extension_type(RowNumber),
    );
    (
        TableSchema::builder(Arc::clone(table_schema.file_schema()))
            .with_table_partition_cols(table_schema.table_partition_cols().clone())
            .with_virtual_columns(
                table_schema
                    .virtual_columns()
                    .iter()
                    .cloned()
                    .chain([row_index_field])
                    .collect::<Fields>(),
            )
            .build(),
        Column::new(&row_index_name, row_index_table_idx),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use datafusion_physical_expr::expressions::lit;

    #[test]
    #[expect(deprecated)]
    fn test_parquet_source_predicate_same_as_filter() {
        let predicate = lit(true);

        let parquet_source =
            ParquetSource::new(Arc::new(Schema::empty())).with_predicate(predicate);
        // same value. but filter() call Arc::clone internally
        assert_eq!(parquet_source.predicate(), parquet_source.filter().as_ref());
    }

    #[test]
    fn test_reverse_scan_default_value() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());
        let source = ParquetSource::new(schema);

        assert!(!source.reverse_row_groups());
    }

    #[test]
    fn test_reverse_scan_with_setter() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());

        let source = ParquetSource::new(schema.clone()).with_reverse_row_groups(true);
        assert!(source.reverse_row_groups());

        let source = source.with_reverse_row_groups(false);
        assert!(!source.reverse_row_groups());
    }

    #[test]
    fn test_reverse_scan_clone_preserves_value() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());

        let source = ParquetSource::new(schema).with_reverse_row_groups(true);
        let cloned = source.clone();

        assert!(cloned.reverse_row_groups());
        assert_eq!(source.reverse_row_groups(), cloned.reverse_row_groups());
    }

    #[test]
    fn test_reverse_scan_with_other_options() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());
        let options = TableParquetOptions::default();

        let source = ParquetSource::new(schema)
            .with_table_parquet_options(options)
            .with_metadata_size_hint(8192)
            .with_reverse_row_groups(true);

        assert!(source.reverse_row_groups());
        assert_eq!(source.metadata_size_hint, Some(8192));
    }

    #[test]
    fn test_reverse_scan_builder_pattern() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());

        let source = ParquetSource::new(schema)
            .with_reverse_row_groups(true)
            .with_reverse_row_groups(false)
            .with_reverse_row_groups(true);

        assert!(source.reverse_row_groups());
    }

    #[test]
    fn test_reverse_scan_independent_of_predicate() {
        use arrow::datatypes::Schema;
        use datafusion_physical_expr::expressions::lit;

        let schema = Arc::new(Schema::empty());
        let predicate = lit(true);

        let source = ParquetSource::new(schema)
            .with_predicate(predicate)
            .with_reverse_row_groups(true);

        assert!(source.reverse_row_groups());
        assert!(source.filter().is_some());
    }

    /// Render a `ParquetSource`'s `fmt_extra` output as a `String` for
    /// inspection in tests.
    fn render_fmt_extra(source: &ParquetSource, t: DisplayFormatType) -> String {
        use std::fmt::Display;

        struct Wrap<'a> {
            source: &'a ParquetSource,
            t: DisplayFormatType,
        }
        impl Display for Wrap<'_> {
            fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
                self.source.fmt_extra(self.t, f)
            }
        }
        Wrap { source, t }.to_string()
    }

    /// EXPLAIN must surface a `dynamic_rg_pruning=eligible` marker when the
    /// predicate carries a `DynamicFilterPhysicalExpr`. This is the
    /// plan-time signal that the runtime row-group pruner will fire at
    /// every RG boundary.
    #[test]
    fn fmt_extra_marks_dynamic_predicate_as_pruning_eligible() {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("v", 0))],
            lit(true),
        )) as Arc<dyn PhysicalExpr>;

        let source =
            ParquetSource::new(Arc::clone(&schema)).with_predicate(Arc::clone(&dynamic));

        let rendered = render_fmt_extra(&source, DisplayFormatType::Default);
        assert!(
            rendered.contains("dynamic_rg_pruning=eligible"),
            "expected marker in Default fmt_extra, got: {rendered}"
        );

        let rendered_verbose = render_fmt_extra(&source, DisplayFormatType::Verbose);
        assert!(
            rendered_verbose.contains("dynamic_rg_pruning=eligible"),
            "expected marker in Verbose fmt_extra, got: {rendered_verbose}"
        );
    }

    /// EXPLAIN must NOT show the dynamic-RG-pruning marker when the
    /// predicate is purely static — the optimization will not fire, so
    /// surfacing it would mislead the reader.
    #[test]
    fn fmt_extra_omits_marker_for_static_predicate() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());
        let predicate = lit(true);
        let source = ParquetSource::new(schema).with_predicate(predicate);

        let rendered = render_fmt_extra(&source, DisplayFormatType::Default);
        assert!(
            !rendered.contains("dynamic_rg_pruning"),
            "did not expect marker for static predicate, got: {rendered}"
        );
    }

    /// EXPLAIN must NOT show the marker when there is no predicate at all
    /// (e.g. unfiltered table scan).
    #[test]
    fn fmt_extra_omits_marker_when_no_predicate() {
        use arrow::datatypes::Schema;

        let schema = Arc::new(Schema::empty());
        let source = ParquetSource::new(schema);

        let rendered = render_fmt_extra(&source, DisplayFormatType::Default);
        assert!(
            !rendered.contains("dynamic_rg_pruning"),
            "did not expect marker for predicate-less scan, got: {rendered}"
        );
    }

    /// Helpers for the `try_pushdown_sort` regression tests below.
    mod pushdown_sort_helpers {
        use super::*;
        use arrow::compute::SortOptions;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

        pub(super) fn schema_with_a_int() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]))
        }

        pub(super) fn sort_expr_on(
            schema: &Arc<Schema>,
            name: &str,
            descending: bool,
        ) -> PhysicalSortExpr {
            let idx = schema.index_of(name).unwrap();
            PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)),
                options: SortOptions {
                    descending,
                    nulls_first: true,
                },
            }
        }
    }

    /// When neither natural nor reversed ordering matches the request,
    /// but the sort column is a plain `Column` present in the file
    /// schema, `try_pushdown_sort` returns `Inexact` with
    /// `sort_order_for_reorder` set so the opener can reorder row
    /// groups by min/max statistics at runtime.
    #[test]
    fn try_pushdown_sort_returns_inexact_when_column_in_schema_asc() {
        use datafusion_physical_expr::EquivalenceProperties;
        use pushdown_sort_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(Arc::clone(&schema));
        let order = vec![sort_expr_on(&schema, "a", false)];
        // No declared natural ordering on the source.
        let eq = EquivalenceProperties::new(Arc::clone(&schema));

        let result = source.try_pushdown_sort(&order, &eq).unwrap();

        let SortOrderPushdownResult::Inexact { inner } = result else {
            panic!("expected Inexact, got a different variant");
        };
        // Downcast back to `ParquetSource` to inspect the fields the
        // opener reads to drive `reorder_by_statistics` / `reverse`.
        let inner_parquet = inner
            .downcast_ref::<ParquetSource>()
            .expect("inner is ParquetSource");
        let sort_order = inner_parquet
            .sort_order_for_reorder
            .as_ref()
            .expect("sort_order_for_reorder must be set so the opener can reorder");
        assert_eq!(sort_order.first().expr.to_string(), "a@0");
        // ASC request must not flip RG iteration order.
        assert!(
            !inner_parquet.reverse_row_groups(),
            "ASC request must not set reverse_row_groups",
        );
    }

    /// Same as above but for DESC. `reverse_row_groups` must also be
    /// `true` so the opener flips iteration order.
    #[test]
    fn try_pushdown_sort_returns_inexact_when_column_in_schema_desc() {
        use datafusion_physical_expr::EquivalenceProperties;
        use pushdown_sort_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(Arc::clone(&schema));
        let order = vec![sort_expr_on(&schema, "a", true)];
        let eq = EquivalenceProperties::new(Arc::clone(&schema));

        let result = source.try_pushdown_sort(&order, &eq).unwrap();

        let SortOrderPushdownResult::Inexact { inner } = result else {
            panic!("expected Inexact, got a different variant");
        };
        let inner_parquet = inner
            .downcast_ref::<ParquetSource>()
            .expect("inner is ParquetSource");
        assert!(inner_parquet.sort_order_for_reorder.is_some());
        assert!(
            inner_parquet.reverse_row_groups(),
            "DESC request must set reverse_row_groups",
        );
    }

    /// A non-`Column` leading sort expression (e.g. `a + 1`,
    /// `date_trunc('hour', ts)`) with no declared source ordering
    /// yields `Unsupported` — parquet stats need a column name to
    /// look up min/max, and there's no source ordering to reverse.
    #[test]
    fn try_pushdown_sort_returns_unsupported_for_non_column_sort_expr() {
        use arrow::compute::SortOptions;
        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_expr::expressions::{BinaryExpr, Column, lit};
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
        use pushdown_sort_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(Arc::clone(&schema));

        // `a + 1` — not a plain Column.
        let order = vec![PhysicalSortExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                datafusion_expr::Operator::Plus,
                lit(1i32),
            )),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let eq = EquivalenceProperties::new(Arc::clone(&schema));

        let result = source.try_pushdown_sort(&order, &eq).unwrap();
        assert!(
            matches!(result, SortOrderPushdownResult::Unsupported),
            "non-Column sort expression must yield Unsupported",
        );
    }

    /// A sort column missing from the file schema with no declared
    /// source ordering yields `Unsupported` — there are no parquet
    /// stats for that column and no source ordering to reverse.
    #[test]
    fn try_pushdown_sort_returns_unsupported_when_column_not_in_file_schema() {
        use arrow::compute::SortOptions;
        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
        use pushdown_sort_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(Arc::clone(&schema));

        // Reference a column ("b") that does not exist in the file
        // schema (which only has "a"). The Column expression itself is
        // well-formed; only its name is unknown to the file.
        let order = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("b", 0)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let eq = EquivalenceProperties::new(Arc::clone(&schema));

        let result = source.try_pushdown_sort(&order, &eq).unwrap();
        assert!(
            matches!(result, SortOrderPushdownResult::Unsupported),
            "column not in file schema must yield Unsupported",
        );
    }

    /// Regression: when the source declares `[a DESC]` and the request is
    /// `[a ASC]`, both `column_in_file_schema` and `reversed_satisfies`
    /// are true. `reverse_row_groups` must follow the *request's*
    /// direction (false for ASC) — not the source's, and not the OR of
    /// both signals. The opener's stats-based reorder produces
    /// ASC-by-min, so an ASC request needs no further flip; flipping
    /// would incorrectly emit DESC.
    #[test]
    fn try_pushdown_sort_source_desc_request_asc_does_not_reverse() {
        use datafusion_physical_expr::EquivalenceProperties;
        use pushdown_sort_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(Arc::clone(&schema));
        // Source declares `[a DESC]`.
        let mut eq = EquivalenceProperties::new(Arc::clone(&schema));
        eq.add_ordering(vec![sort_expr_on(&schema, "a", true)]);
        // Request `[a ASC]`.
        let order = vec![sort_expr_on(&schema, "a", false)];

        let result = source.try_pushdown_sort(&order, &eq).unwrap();

        let SortOrderPushdownResult::Inexact { inner } = result else {
            panic!("expected Inexact, got a different variant");
        };
        let inner_parquet = inner
            .downcast_ref::<ParquetSource>()
            .expect("inner is ParquetSource");
        assert!(
            inner_parquet.sort_order_for_reorder.is_some(),
            "sort_order_for_reorder must be set",
        );
        assert!(
            !inner_parquet.reverse_row_groups(),
            "ASC request on source-DESC must not set reverse_row_groups; \
             a stale `reversed_satisfies || is_descending` formula would \
             incorrectly flip iteration to DESC after the stats reorder",
        );
    }

    /// A sort column that is *not* in the file schema (here: a partition
    /// column `b`) but the source's *reversed* declared ordering does
    /// satisfy the request. Pushdown fires via the reversed-equivalence
    /// path; `sort_order_for_reorder` is still set (so EXPLAIN reflects
    /// what the source was asked to approximate, even though the opener
    /// will skip its stats reorder because `b` has no per-RG min/max in
    /// the parquet file), and `reverse_row_groups` is `true` because we
    /// flip the file's natural order rather than re-sort by stats.
    #[test]
    fn try_pushdown_sort_returns_inexact_via_reversed_eq_when_column_not_in_file_schema()
    {
        use arrow::compute::SortOptions;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_datasource::TableSchema;
        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

        // File schema is just `[a]`; `b` lives as a partition column on
        // top, so it appears in the table schema but not the file schema
        // — the same shape `column_in_file_schema` discards.
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let partition_b = Arc::new(Field::new("b", DataType::Int32, true));
        let table_schema = TableSchema::builder(file_schema)
            .with_table_partition_cols(vec![partition_b])
            .build();
        let source = ParquetSource::new(table_schema);

        // EquivalenceProperties is built on the *full* table schema so
        // it can carry an ordering on `b`.
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        // Construct the request first, then derive the declared
        // ordering as its reverse, so `ordering_satisfy` on the
        // reversed-eq holds exactly. `PhysicalSortExpr::reverse` flips
        // both `descending` and `nulls_first`, so spelling the
        // declared ordering directly is error-prone.
        let request_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new("b", 1)),
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        };
        let declared = request_expr.reverse();
        let mut eq = EquivalenceProperties::new(Arc::clone(&full_schema));
        eq.add_ordering(vec![declared]);
        let order = vec![request_expr];

        let result = source.try_pushdown_sort(&order, &eq).unwrap();

        let SortOrderPushdownResult::Inexact { inner } = result else {
            panic!("expected Inexact, got a different variant");
        };
        let inner_parquet = inner
            .downcast_ref::<ParquetSource>()
            .expect("inner is ParquetSource");
        assert!(
            inner_parquet.sort_order_for_reorder.is_some(),
            "sort_order_for_reorder must be set so EXPLAIN reflects the request",
        );
        assert!(
            inner_parquet.reverse_row_groups(),
            "request reached via reversed_satisfies (column-not-in-file-schema) \
             must set reverse_row_groups to flip the file's natural order",
        );
    }

    /// Regression: when the source's declared ordering is a non-empty
    /// *proper* prefix of the request, `try_pushdown_sort` must return
    /// `Unsupported` so that the outer `SortExec` can keep its
    /// `sort_prefix` annotation and `TopK` can early-terminate within
    /// each prefix block. Letting the Phase 3 Inexact pipeline fire
    /// would drop the source's `output_ordering`, destroying the
    /// information `EnforceSorting` needs to compute `sort_prefix`.
    #[test]
    fn try_pushdown_sort_preserves_sort_prefix_when_source_declares_prefix_ordering() {
        use arrow::compute::SortOptions;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));
        let source = ParquetSource::new(Arc::clone(&schema));

        // Source declares `[a DESC, b ASC NULLS LAST]` — the same prefix
        // the SortExec input will see.
        let mut eq = EquivalenceProperties::new(Arc::clone(&schema));
        eq.add_ordering(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ]);

        // Request `[a DESC, b ASC NULLS LAST, c DESC]` — three columns;
        // source's two-column declaration is a strict prefix.
        let order = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
        ];

        let result = source.try_pushdown_sort(&order, &eq).unwrap();
        assert!(
            matches!(result, SortOrderPushdownResult::Unsupported),
            "source ordering [a DESC, b ASC NULLS LAST] is a proper prefix \
             of the request — `try_pushdown_sort` must return Unsupported so \
             the SortExec sort_prefix optimisation can fire",
        );
    }

    /// Helpers for the `reorder_files` regression tests below.
    mod reorder_files_helpers {
        use super::*;
        use datafusion_common::stats::Precision;
        use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
        use datafusion_datasource::PartitionedFile;

        pub(super) fn file_with_min(name: &str, min: Option<i32>) -> PartitionedFile {
            let mut pf = PartitionedFile::new(name.to_string(), 0);
            let min_value = min
                .map(|v| Precision::Exact(ScalarValue::Int32(Some(v))))
                .unwrap_or(Precision::Absent);
            pf.statistics = Some(Arc::new(Statistics {
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                }],
            }));
            pf
        }

        pub(super) fn names(files: &[PartitionedFile]) -> Vec<&str> {
            files
                .iter()
                .map(|f| f.object_meta.location.as_ref())
                .collect()
        }
    }

    /// ASC TopK: `reorder_files` keys off file `min` and sorts ASC,
    /// so the file with the smallest `min` is read first. This
    /// matches `PreparedAccessPlan::reorder_by_statistics` at the
    /// row-group level (also `min ASC`).
    #[test]
    fn reorder_files_sorts_asc_by_min_for_asc_request() {
        use pushdown_sort_helpers::*;
        use reorder_files_helpers::*;

        let schema = schema_with_a_int();
        let mut source = ParquetSource::new(Arc::clone(&schema));
        source.sort_order_for_reorder =
            Some(LexOrdering::new(vec![sort_expr_on(&schema, "a", false)]).unwrap());
        // ASC request → `reverse_row_groups` left at its default `false`.

        let reordered = source.reorder_files(vec![
            file_with_min("middle", Some(50)),
            file_with_min("small", Some(10)),
            file_with_min("large", Some(100)),
        ]);

        assert_eq!(names(&reordered), vec!["small", "middle", "large"]);
    }

    /// DESC TopK: same `min` key, but sorted DESC — file with the
    /// largest `min` first. Mirrors the row-group strategy of
    /// "ASC-by-min then `reverse`".
    #[test]
    fn reorder_files_sorts_desc_by_min_for_desc_request() {
        use pushdown_sort_helpers::*;
        use reorder_files_helpers::*;

        let schema = schema_with_a_int();
        let mut source =
            ParquetSource::new(Arc::clone(&schema)).with_reverse_row_groups(true);
        source.sort_order_for_reorder =
            Some(LexOrdering::new(vec![sort_expr_on(&schema, "a", true)]).unwrap());

        let reordered = source.reorder_files(vec![
            file_with_min("middle", Some(50)),
            file_with_min("small", Some(10)),
            file_with_min("large", Some(100)),
        ]);

        assert_eq!(names(&reordered), vec!["large", "middle", "small"]);
    }

    /// Files without statistics sort to the *end* so present-stats
    /// files run first regardless of direction. Verified for both
    /// ASC and DESC.
    #[test]
    fn reorder_files_pushes_missing_stats_to_the_end() {
        use pushdown_sort_helpers::*;
        use reorder_files_helpers::*;

        let schema = schema_with_a_int();
        let mut source = ParquetSource::new(Arc::clone(&schema));
        source.sort_order_for_reorder =
            Some(LexOrdering::new(vec![sort_expr_on(&schema, "a", false)]).unwrap());

        let reordered = source.reorder_files(vec![
            file_with_min("no_stats", None),
            file_with_min("has_min", Some(10)),
        ]);
        assert_eq!(names(&reordered), vec!["has_min", "no_stats"]);

        // Same for DESC.
        let mut source =
            ParquetSource::new(Arc::clone(&schema)).with_reverse_row_groups(true);
        source.sort_order_for_reorder =
            Some(LexOrdering::new(vec![sort_expr_on(&schema, "a", true)]).unwrap());
        let reordered = source.reorder_files(vec![
            file_with_min("no_stats", None),
            file_with_min("has_min", Some(10)),
        ]);
        assert_eq!(names(&reordered), vec!["has_min", "no_stats"]);
    }

    /// When no sort pushdown has fired (`sort_order_for_reorder` is
    /// `None`), `reorder_files` is a no-op and preserves input order.
    #[test]
    fn reorder_files_is_a_no_op_without_pushdown() {
        use pushdown_sort_helpers::*;
        use reorder_files_helpers::*;

        let schema = schema_with_a_int();
        let source = ParquetSource::new(schema);
        // No `sort_order_for_reorder` set on the source.

        let input = vec![
            file_with_min("c", Some(30)),
            file_with_min("a", Some(10)),
            file_with_min("b", Some(20)),
        ];
        let reordered = source.reorder_files(input.clone());
        assert_eq!(names(&reordered), names(&input));
    }

    /// `sort_order_for_reorder` is surfaced in both `EXPLAIN` (Default)
    /// and `EXPLAIN VERBOSE` / `EXPLAIN ANALYZE` (Verbose) so readers
    /// and snapshot tests can see the inexact sort-pushdown fired.
    #[test]
    fn sort_order_for_reorder_shown_in_explain() {
        use pushdown_sort_helpers::*;

        // `std::fmt::Formatter` can't be constructed outside core fmt
        // machinery, so we drive `fmt_extra` through a Display adapter
        // and read the rendered string back with `format!`.
        struct DisplayHelper<'a> {
            source: &'a ParquetSource,
            mode: DisplayFormatType,
        }
        impl std::fmt::Display for DisplayHelper<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                self.source.fmt_extra(self.mode, f)
            }
        }

        let schema = schema_with_a_int();
        let mut source = ParquetSource::new(Arc::clone(&schema));
        let order = LexOrdering::new(vec![sort_expr_on(&schema, "a", false)]).unwrap();
        source.sort_order_for_reorder = Some(order);

        for mode in [DisplayFormatType::Default, DisplayFormatType::Verbose] {
            let out = format!(
                "{}",
                DisplayHelper {
                    source: &source,
                    mode,
                },
            );
            assert!(
                out.contains("sort_order_for_reorder=[a@0 ASC]"),
                "{mode:?} display must surface sort_order_for_reorder, got: {out}",
            );
        }
    }

    #[test]
    fn test_try_pushdown_filters_rejects_virtual_column_refs() {
        // Virtual columns are produced by the reader and cannot be referenced
        // inside a RowFilter. `try_pushdown_filters` must report such filters
        // as `PushedDown::No` so the FilterExec above the scan stays in
        // place — otherwise the scan would silently drop the predicate and
        // produce wrong results.
        use arrow::datatypes::{DataType, Field, FieldRef, Schema};
        use datafusion_common::config::ConfigOptions;
        use datafusion_datasource::TableSchema;
        use datafusion_expr::{col, lit as logical_lit};
        use datafusion_functions::core::expr_fn::file_row_index;
        use datafusion_physical_expr::planner::logical2physical;
        use datafusion_physical_expr_adapter::rewrite::rewrite_file_row_index_expr;
        use datafusion_physical_plan::filter_pushdown::PushedDown;
        use parquet::arrow::RowNumber;

        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let row_number_field: FieldRef = Arc::new(
            Field::new("row_number", DataType::Int64, false)
                .with_extension_type(RowNumber),
        );
        let table_schema = TableSchema::builder(file_schema)
            .with_virtual_columns(vec![row_number_field])
            .build();

        let source = ParquetSource::new(table_schema).with_pushdown_filters(true);

        let full_schema = source.table_schema.table_schema();

        let pushable = logical2physical(&col("value").eq(logical_lit(1i64)), full_schema);
        let virtual_only =
            logical2physical(&col("row_number").eq(logical_lit(2i64)), full_schema);
        let mixed = logical2physical(
            &col("row_number")
                .eq(logical_lit(2i64))
                .or(col("value").eq(logical_lit(4i64))),
            full_schema,
        );
        let (_, row_index_col) = table_schema_with_row_index_col(source.table_schema());
        let row_index = rewrite_file_row_index_expr(
            logical2physical(&file_row_index().gt(logical_lit(2i64)), full_schema),
            row_index_col.name(),
            row_index_col.index(),
        )
        .expect("file_row_index should rewrite to the row_number virtual column");

        let config = ConfigOptions::default();
        let prop = source
            .try_pushdown_filters(vec![pushable, virtual_only, mixed, row_index], &config)
            .expect("try_pushdown_filters must not error");

        assert_eq!(prop.filters.len(), 4);
        assert!(
            matches!(prop.filters[0], PushedDown::Yes),
            "file-column filter should be pushable"
        );
        assert!(
            matches!(prop.filters[1], PushedDown::No),
            "filter referencing only a virtual column must not be pushed down"
        );
        assert!(
            matches!(prop.filters[2], PushedDown::No),
            "filter mixing a virtual column with a file column must not be \
             pushed down (row filter would silently drop it)"
        );
        assert!(
            matches!(prop.filters[3], PushedDown::No),
            "file_row_index() rewrites to a virtual column and must not be \
             pushed down"
        );
    }
}
