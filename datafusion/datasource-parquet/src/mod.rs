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

// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

//! [`ParquetExec`] FileSource for reading Parquet files

pub mod access_plan;
pub mod file_format;
mod metrics;
mod opener;
mod page_filter;
mod reader;
mod row_filter;
mod row_group_filter;
pub mod source;
mod writer;

use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

pub use access_plan::{ParquetAccessPlan, RowGroupAccess};
use arrow::datatypes::SchemaRef;
use datafusion_common::config::{ConfigOptions, TableParquetOptions};
use datafusion_common::Result;
use datafusion_common::{Constraints, Statistics};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr,
};
use datafusion_physical_optimizer::pruning::PruningPredicate;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
pub use file_format::*;
pub use metrics::ParquetFileMetrics;
pub use page_filter::PagePruningAccessPlanFilter;
pub use reader::{DefaultParquetFileReaderFactory, ParquetFileReaderFactory};
pub use row_filter::build_row_filter;
pub use row_filter::can_expr_be_pushed_down_with_schemas;
pub use row_group_filter::RowGroupAccessPlanFilter;
use source::ParquetSource;
pub use writer::plan_to_parquet;

use datafusion_datasource::file_groups::FileGroup;
use log::debug;

#[derive(Debug, Clone)]
#[deprecated(since = "46.0.0", note = "use DataSourceExec instead")]
/// Deprecated Execution plan replaced with DataSourceExec
pub struct ParquetExec {
    inner: DataSourceExec,
    base_config: FileScanConfig,
    table_parquet_options: TableParquetOptions,
    /// Optional predicate for row filtering during parquet scan
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Optional predicate for pruning row groups (derived from `predicate`)
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Optional user defined parquet file reader factory
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    /// Optional user defined schema adapter
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

#[allow(unused, deprecated)]
impl From<ParquetExec> for ParquetExecBuilder {
    fn from(exec: ParquetExec) -> Self {
        exec.into_builder()
    }
}

/// [`ParquetExecBuilder`], deprecated builder for [`ParquetExec`].
///
/// ParquetExec is replaced with `DataSourceExec` and it includes `ParquetSource`
///
/// See example on [`ParquetSource`].
#[deprecated(
    since = "46.0.0",
    note = "use DataSourceExec with ParquetSource instead"
)]
#[allow(unused, deprecated)]
pub struct ParquetExecBuilder {
    file_scan_config: FileScanConfig,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    metadata_size_hint: Option<usize>,
    table_parquet_options: TableParquetOptions,
    parquet_file_reader_factory: Option<Arc<dyn ParquetFileReaderFactory>>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

#[allow(unused, deprecated)]
impl ParquetExecBuilder {
    /// Create a new builder to read the provided file scan configuration
    pub fn new(file_scan_config: FileScanConfig) -> Self {
        Self::new_with_options(file_scan_config, TableParquetOptions::default())
    }

    /// Create a new builder to read the data specified in the file scan
    /// configuration with the provided `TableParquetOptions`.
    pub fn new_with_options(
        file_scan_config: FileScanConfig,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        Self {
            file_scan_config,
            predicate: None,
            metadata_size_hint: None,
            table_parquet_options,
            parquet_file_reader_factory: None,
            schema_adapter_factory: None,
        }
    }

    /// Update the list of files groups to read
    pub fn with_file_groups(mut self, file_groups: Vec<FileGroup>) -> Self {
        self.file_scan_config.file_groups = file_groups;
        self
    }

    /// Set the filter predicate when reading.
    ///
    /// See the "Predicate Pushdown" section of the [`ParquetExec`] documentation
    /// for more details.
    pub fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set the metadata size hint
    ///
    /// This value determines how many bytes at the end of the file the default
    /// [`ParquetFileReaderFactory`] will request in the initial IO. If this is
    /// too small, the ParquetExec will need to make additional IO requests to
    /// read the footer.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Set the options for controlling how the ParquetExec reads parquet files.
    ///
    /// See also [`Self::new_with_options`]
    pub fn with_table_parquet_options(
        mut self,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        self.table_parquet_options = table_parquet_options;
        self
    }

    /// Set optional user defined parquet file reader factory.
    ///
    /// You can use [`ParquetFileReaderFactory`] to more precisely control how
    /// data is read from parquet files (e.g. skip re-reading metadata, coalesce
    /// I/O operations, etc).
    ///
    /// The default reader factory reads directly from an [`ObjectStore`]
    /// instance using individual I/O operations for the footer and each page.
    ///
    /// If a custom `ParquetFileReaderFactory` is provided, then data access
    /// operations will be routed to this factory instead of [`ObjectStore`].
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    pub fn with_parquet_file_reader_factory(
        mut self,
        parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    ) -> Self {
        self.parquet_file_reader_factory = Some(parquet_file_reader_factory);
        self
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

    /// Convenience: build an `Arc`d `ParquetExec` from this builder
    pub fn build_arc(self) -> Arc<ParquetExec> {
        Arc::new(self.build())
    }

    /// Build a [`ParquetExec`]
    #[must_use]
    pub fn build(self) -> ParquetExec {
        let Self {
            file_scan_config,
            predicate,
            metadata_size_hint,
            table_parquet_options,
            parquet_file_reader_factory,
            schema_adapter_factory,
        } = self;
        let mut parquet = ParquetSource::new(table_parquet_options);
        if let Some(predicate) = predicate.clone() {
            parquet = parquet
                .with_predicate(Arc::clone(&file_scan_config.file_schema), predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            parquet = parquet.with_metadata_size_hint(metadata_size_hint)
        }
        if let Some(parquet_reader_factory) = parquet_file_reader_factory {
            parquet = parquet.with_parquet_file_reader_factory(parquet_reader_factory)
        }
        if let Some(schema_factory) = schema_adapter_factory {
            parquet = parquet.with_schema_adapter_factory(schema_factory);
        }

        let base_config = file_scan_config.with_source(Arc::new(parquet.clone()));
        debug!("Creating ParquetExec, files: {:?}, projection {:?}, predicate: {:?}, limit: {:?}",
        base_config.file_groups, base_config.projection, predicate, base_config.limit);

        ParquetExec {
            inner: DataSourceExec::new(Arc::new(base_config.clone())),
            base_config,
            predicate,
            pruning_predicate: parquet.pruning_predicate,
            schema_adapter_factory: parquet.schema_adapter_factory,
            parquet_file_reader_factory: parquet.parquet_file_reader_factory,
            table_parquet_options: parquet.table_parquet_options,
        }
    }
}

#[allow(unused, deprecated)]
impl ParquetExec {
    /// Create a new Parquet reader execution plan provided file list and schema.
    pub fn new(
        base_config: FileScanConfig,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metadata_size_hint: Option<usize>,
        table_parquet_options: TableParquetOptions,
    ) -> Self {
        let mut builder =
            ParquetExecBuilder::new_with_options(base_config, table_parquet_options);
        if let Some(predicate) = predicate {
            builder = builder.with_predicate(predicate);
        }
        if let Some(metadata_size_hint) = metadata_size_hint {
            builder = builder.with_metadata_size_hint(metadata_size_hint);
        }
        builder.build()
    }
    /// Return a [`ParquetExecBuilder`].
    ///
    /// See example on [`ParquetExec`] and [`ParquetExecBuilder`] for specifying
    /// parquet table options.
    pub fn builder(file_scan_config: FileScanConfig) -> ParquetExecBuilder {
        ParquetExecBuilder::new(file_scan_config)
    }

    /// Convert this `ParquetExec` into a builder for modification
    pub fn into_builder(self) -> ParquetExecBuilder {
        // list out fields so it is clear what is being dropped
        // (note the fields which are dropped are re-created as part of calling
        // `build` on the builder)
        let file_scan_config = self.file_scan_config();
        let parquet = self.parquet_source();

        ParquetExecBuilder {
            file_scan_config,
            predicate: parquet.predicate,
            metadata_size_hint: parquet.metadata_size_hint,
            table_parquet_options: parquet.table_parquet_options,
            parquet_file_reader_factory: parquet.parquet_file_reader_factory,
            schema_adapter_factory: parquet.schema_adapter_factory,
        }
    }
    fn file_scan_config(&self) -> FileScanConfig {
        self.inner
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .unwrap()
            .clone()
    }

    fn parquet_source(&self) -> ParquetSource {
        self.file_scan_config()
            .file_source()
            .as_any()
            .downcast_ref::<ParquetSource>()
            .unwrap()
            .clone()
    }

    /// [`FileScanConfig`] that controls this scan (such as which files to read)
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
    /// Options passed to the parquet reader for this scan
    pub fn table_parquet_options(&self) -> &TableParquetOptions {
        &self.table_parquet_options
    }
    /// Optional predicate.
    pub fn predicate(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.predicate.as_ref()
    }
    /// Optional reference to this parquet scan's pruning predicate
    pub fn pruning_predicate(&self) -> Option<&Arc<PruningPredicate>> {
        self.pruning_predicate.as_ref()
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
        let mut parquet = self.parquet_source();
        parquet.parquet_file_reader_factory =
            Some(Arc::clone(&parquet_file_reader_factory));
        let file_source = self.file_scan_config();
        self.inner = self
            .inner
            .with_data_source(Arc::new(file_source.with_source(Arc::new(parquet))));
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
        let mut parquet = self.parquet_source();
        parquet.schema_adapter_factory = Some(Arc::clone(&schema_adapter_factory));
        let file_source = self.file_scan_config();
        self.inner = self
            .inner
            .with_data_source(Arc::new(file_source.with_source(Arc::new(parquet))));
        self.schema_adapter_factory = Some(schema_adapter_factory);
        self
    }
    /// If true, the predicate will be used during the parquet scan.
    /// Defaults to false
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_pushdown_filters(mut self, pushdown_filters: bool) -> Self {
        let mut parquet = self.parquet_source();
        parquet.table_parquet_options.global.pushdown_filters = pushdown_filters;
        let file_source = self.file_scan_config();
        self.inner = self
            .inner
            .with_data_source(Arc::new(file_source.with_source(Arc::new(parquet))));
        self.table_parquet_options.global.pushdown_filters = pushdown_filters;
        self
    }

    /// Return the value described in [`Self::with_pushdown_filters`]
    fn pushdown_filters(&self) -> bool {
        self.parquet_source()
            .table_parquet_options
            .global
            .pushdown_filters
    }
    /// If true, the `RowFilter` made by `pushdown_filters` may try to
    /// minimize the cost of filter evaluation by reordering the
    /// predicate [`Expr`]s. If false, the predicates are applied in
    /// the same order as specified in the query. Defaults to false.
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_reorder_filters(mut self, reorder_filters: bool) -> Self {
        let mut parquet = self.parquet_source();
        parquet.table_parquet_options.global.reorder_filters = reorder_filters;
        let file_source = self.file_scan_config();
        self.inner = self
            .inner
            .with_data_source(Arc::new(file_source.with_source(Arc::new(parquet))));
        self.table_parquet_options.global.reorder_filters = reorder_filters;
        self
    }
    /// Return the value described in [`Self::with_reorder_filters`]
    fn reorder_filters(&self) -> bool {
        self.parquet_source()
            .table_parquet_options
            .global
            .reorder_filters
    }
    /// If enabled, the reader will read the page index
    /// This is used to optimize filter pushdown
    /// via `RowSelector` and `RowFilter` by
    /// eliminating unnecessary IO and decoding
    fn bloom_filter_on_read(&self) -> bool {
        self.parquet_source()
            .table_parquet_options
            .global
            .bloom_filter_on_read
    }
    /// Return the value described in [`ParquetSource::with_enable_page_index`]
    fn enable_page_index(&self) -> bool {
        self.parquet_source()
            .table_parquet_options
            .global
            .enable_page_index
    }

    fn output_partitioning_helper(file_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        file_config: &FileScanConfig,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings)
                .with_constraints(constraints),
            Self::output_partitioning_helper(file_config), // Output Partitioning
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    /// Updates the file groups to read and recalculates the output partitioning
    ///
    /// Note this function does not update statistics or other properties
    /// that depend on the file groups.
    fn with_file_groups_and_update_partitioning(
        mut self,
        file_groups: Vec<FileGroup>,
    ) -> Self {
        let mut config = self.file_scan_config();
        config.file_groups = file_groups;
        self.inner = self.inner.with_data_source(Arc::new(config));
        self
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for ParquetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for ParquetExec {
    fn name(&self) -> &'static str {
        "ParquetExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on `FileGroupPartitioner` for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.repartitioned(target_partitions, config)
    }

    fn execute(
        &self,
        partition_index: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition_index, ctx)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }
    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        self.inner.with_fetch(limit)
    }
}

fn should_enable_page_index(
    enable_page_index: bool,
    page_pruning_predicate: &Option<Arc<PagePruningAccessPlanFilter>>,
) -> bool {
    enable_page_index
        && page_pruning_predicate.is_some()
        && page_pruning_predicate
            .as_ref()
            .map(|p| p.filter_number() > 0)
            .unwrap_or(false)
}
