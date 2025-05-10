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
use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::opener::build_pruning_predicates;
use crate::opener::ParquetOpener;
use crate::row_filter::can_expr_be_pushed_down_with_schemas;
use crate::DefaultParquetFileReaderFactory;
use crate::ParquetFileReaderFactory;
use datafusion_common::config::ConfigOptions;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapterFactory,
};

use arrow::datatypes::{SchemaRef, TimeUnit};
use datafusion_common::config::TableParquetOptions;
use datafusion_common::{DataFusionError, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion_physical_plan::filter_pushdown::PredicateSupport;
use datafusion_physical_plan::filter_pushdown::PredicateSupports;
use datafusion_physical_plan::metrics::Count;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::DisplayFormatType;

use itertools::Itertools;
use object_store::ObjectStore;

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
///
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
///     ParquetSource::default()
///     .with_predicate(predicate)
/// );
/// // Create a DataSourceExec for reading `file1.parquet` with a file size of 100MB
/// let config = FileScanConfigBuilder::new(object_store_url, file_schema, source)
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
///   table schema. See [`SchemaAdapterFactory`] for more details.
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
/// # use datafusion_datasource::file_scan_config::FileScanConfig;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource::source::DataSourceExec;
///
/// # fn parquet_exec() -> DataSourceExec { unimplemented!() }
/// // Split a single DataSourceExec into multiple DataSourceExecs, one for each file
/// let exec = parquet_exec();
/// let data_source = exec.data_source();
/// let base_config = data_source.as_any().downcast_ref::<FileScanConfig>().unwrap();
/// let existing_file_groups = &base_config.file_groups;
/// let new_execs = existing_file_groups
///   .iter()
///   .map(|file_group| {
///     // create a new exec by copying the existing exec's source config
///     let new_config = base_config
///         .clone()
///        .with_file_groups(vec![file_group.clone()]);
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
///   .with_extensions(Arc::new(access_plan));
/// // create a FileScanConfig to scan this file
/// let config = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), schema(), Arc::new(ParquetSource::default()))
///     .with_file(partitioned_file).build();
/// // this parquet DataSourceExec will not even try to read row groups 2 and 4. Additional
/// // pruning based on predicates may also happen
/// let exec = DataSourceExec::from_data_source(config);
/// ```
///
/// For a complete example, see the [`advanced_parquet_index` example]).
///
/// [`parquet_index_advanced` example]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/advanced_parquet_index.rs
///
/// # Execution Overview
///
/// * Step 1: `DataSourceExec::execute` is called, returning a `FileStream`
///   configured to open parquet files with a `ParquetOpener`.
///
/// * Step 2: When the stream is polled, the `ParquetOpener` is called to open
///   the file.
///
/// * Step 3: The `ParquetOpener` gets the [`ParquetMetaData`] (file metadata)
///   via [`ParquetFileReaderFactory`], creating a `ParquetAccessPlan` by
///   applying predicates to metadata. The plan and projections are used to
///   determine what pages must be read.
///
/// * Step 4: The stream begins reading data, fetching the required parquet
///   pages incrementally decoding them, and applying any row filters (see
///   [`Self::with_pushdown_filters`]).
///
/// * Step 5: As each [`RecordBatch`] is read, it may be adapted by a
///   [`SchemaAdapter`] to match the table schema. By default missing columns are
///   filled with nulls, but this can be customized via [`SchemaAdapterFactory`].
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
/// [`SchemaAdapter`]: datafusion_datasource::schema_adapter::SchemaAdapter
/// [`ParquetMetadata`]: parquet::file::metadata::ParquetMetaData
#[derive(Clone, Default, Debug)]
pub struct ParquetSource {
    /// Options for reading Parquet files
    pub(crate) table_parquet_options: TableParquetOptions,
    /// Optional metrics
    pub(crate) metrics: ExecutionPlanMetricsSet,
    /// The schema of the file.
    /// In particular, this is the schema of the table without partition columns,
    /// *not* the physical schema of the file.
    pub(crate) file_schema: Option<SchemaRef>,
    /// Optional predicate for row filtering during parquet scan
    pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional user defined parquet file reader factory
    pub(crate) parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Optional user defined schema adapter
    pub(crate) schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// Batch size configuration
    pub(crate) batch_size: Option<usize>,
    /// Optional hint for the size of the parquet metadata
    pub(crate) metadata_size_hint: Option<usize>,
    pub(crate) projected_statistics: Option<Statistics>,
}

impl ParquetSource {
    /// Create a new ParquetSource to read the data specified in the file scan
    /// configuration with the provided `TableParquetOptions`.
    /// if default values are going to be used, use `ParguetConfig::default()` instead
    pub fn new(table_parquet_options: TableParquetOptions) -> Self {
        Self {
            table_parquet_options,
            ..Self::default()
        }
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

    fn with_metrics(mut self, metrics: ExecutionPlanMetricsSet) -> Self {
        self.metrics = metrics;
        self
    }

    /// Set predicate information
    pub fn with_predicate(&self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        let mut conf = self.clone();
        let metrics = ExecutionPlanMetricsSet::new();
        conf = conf.with_metrics(metrics);
        conf.predicate = Some(Arc::clone(&predicate));
        conf
    }

    /// Options passed to the parquet reader for this scan
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.table_parquet_options
    }

    /// Optional predicate.
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
    ///
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
    }

    /// return the optional schema adapter factory
    pub fn schema_adapter_factory(&self) -> Option<&Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.as_ref()
    }

    /// Set optional schema adapter factory.
    ///
    /// [`SchemaAdapterFactory`] allows user to specify how fields from the
    /// parquet file get mapped to that of the table schema.  The default schema
    /// adapter uses arrow's cast library to map the parquet fields to the table
    /// schema.
    pub fn with_schema_adapter_factory(
        mut self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }

    /// If true, the predicate will be used during the parquet scan.
    /// Defaults to false
    ///
    /// [`Expr`]: datafusion_expr::Expr
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

impl FileSource for ParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        let projection = base_config
            .file_column_projection_indices()
            .unwrap_or_else(|| (0..base_config.file_schema.fields().len()).collect());
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let parquet_file_reader_factory =
            self.parquet_file_reader_factory.clone().unwrap_or_else(|| {
                Arc::new(DefaultParquetFileReaderFactory::new(object_store)) as _
            });

        let coerce_int96 = self
            .table_parquet_options
            .global
            .coerce_int96
            .as_ref()
            .map(|time_unit| parse_coerce_int96_string(time_unit.as_str()).unwrap());

        Arc::new(ParquetOpener {
            partition_index: partition,
            projection: Arc::from(projection),
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating ParquetOpener"),
            limit: base_config.limit,
            predicate: self.predicate.clone(),
            table_schema: Arc::clone(&base_config.file_schema),
            metadata_size_hint: self.metadata_size_hint,
            metrics: self.metrics().clone(),
            parquet_file_reader_factory,
            pushdown_filters: self.pushdown_filters(),
            reorder_filters: self.reorder_filters(),
            enable_page_index: self.enable_page_index(),
            enable_bloom_filter: self.bloom_filter_on_read(),
            enable_row_group_stats_pruning: self.table_parquet_options.global.pruning,
            schema_adapter_factory,
            coerce_int96,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        let statistics = &self.projected_statistics;
        let statistics = statistics
            .clone()
            .expect("projected_statistics must be set");
        // When filters are pushed down, we have no way of knowing the exact statistics.
        // Note that pruning predicate is also a kind of filter pushdown.
        // (bloom filters use `pruning_predicate` too).
        // Because filter pushdown may happen dynamically as long as there is a predicate
        // if we have *any* predicate applied, we can't guarantee the statistics are exact.
        if self.predicate().is_some() {
            Ok(statistics.to_inexact())
        } else {
            Ok(statistics)
        }
    }

    fn file_type(&self) -> &str {
        "parquet"
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let predicate_string = self
                    .predicate()
                    .map(|p| format!(", predicate={p}"))
                    .unwrap_or_default();

                write!(f, "{}", predicate_string)?;

                // Try to build a the pruning predicates.
                // These are only generated here because it's useful to have *some*
                // idea of what pushdown is happening when viewing plans.
                // However it is important to note that these predicates are *not*
                // necessarily the predicates that are actually evaluated:
                // the actual predicates are built in reference to the physical schema of
                // each file, which we do not have at this point and hence cannot use.
                // Instead we use the logical schema of the file (the table schema without partition columns).
                if let (Some(file_schema), Some(predicate)) =
                    (&self.file_schema, &self.predicate)
                {
                    let predicate_creation_errors = Count::new();
                    if let (Some(pruning_predicate), _) = build_pruning_predicates(
                        Some(predicate),
                        file_schema,
                        &predicate_creation_errors,
                    ) {
                        let mut guarantees = pruning_predicate
                            .literal_guarantees()
                            .iter()
                            .map(|item| format!("{}", item))
                            .collect_vec();
                        guarantees.sort();
                        writeln!(
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
                if let Some(predicate) = self.predicate() {
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
        let Some(file_schema) = self.file_schema.clone() else {
            return Ok(FilterPushdownPropagation::unsupported(filters));
        };
        // Can we push down the filters themselves into the scan or only use stats pruning?
        let config_pushdown_enabled = config.execution.parquet.pushdown_filters;
        let table_pushdown_enabled = self.pushdown_filters();
        let pushdown_filters = table_pushdown_enabled || config_pushdown_enabled;

        let mut source = self.clone();
        let mut allowed_filters = vec![];
        let mut remaining_filters = vec![];
        for filter in &filters {
            if can_expr_be_pushed_down_with_schemas(filter, &file_schema) {
                // This filter can be pushed down
                allowed_filters.push(Arc::clone(filter));
            } else {
                // This filter cannot be pushed down
                remaining_filters.push(Arc::clone(filter));
            }
        }
        if allowed_filters.is_empty() {
            // No filters can be pushed down, so we can just return the remaining filters
            // and avoid replacing the source in the physical plan.
            return Ok(FilterPushdownPropagation::unsupported(filters));
        }
        let predicate = match source.predicate {
            Some(predicate) => conjunction(
                std::iter::once(predicate).chain(allowed_filters.iter().cloned()),
            ),
            None => conjunction(allowed_filters.iter().cloned()),
        };
        source.predicate = Some(predicate);
        let source = Arc::new(source);
        let filters = PredicateSupports::new(
            allowed_filters
                .into_iter()
                .map(|f| {
                    if pushdown_filters {
                        PredicateSupport::Supported(f)
                    } else {
                        PredicateSupport::Unsupported(f)
                    }
                })
                .chain(
                    remaining_filters
                        .into_iter()
                        .map(PredicateSupport::Unsupported),
                )
                .collect(),
        );
        Ok(FilterPushdownPropagation::with_filters(filters).with_updated_node(source))
    }
}
