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

use crate::config::SchemaSource;
use crate::helpers::{expr_applicable_for_cols, pruned_partition_list};
use crate::{ListingOptions, ListingTableConfig};
use arrow::datatypes::{Field, Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion_common::stats::Precision;
use datafusion_common::{
    Constraints, SchemaExt, Statistics, internal_datafusion_err, plan_err, project_schema,
};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
#[expect(deprecated)]
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::{
    ListingTableUrl, PartitionedFile, TableSchema, compute_all_files_statistics,
};
use datafusion_execution::cache::TableScopedPath;
use datafusion_execution::cache::cache_manager::FileStatisticsCache;
use datafusion_execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_lex_ordering;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use object_store::ObjectStore;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinSet;

/// Result of a file listing operation from [`ListingTable::list_files_for_scan`].
#[derive(Debug)]
pub struct ListFilesResult {
    /// File groups organized by the partitioning strategy.
    pub file_groups: Vec<FileGroup>,
    /// Aggregated statistics for all files.
    pub statistics: Statistics,
    /// Whether files are grouped by partition values (enables Hash partitioning).
    pub grouped_by_partition: bool,
}

/// Built in [`TableProvider`] that reads data from one or more files as a single table.
///
/// The files are read using an  [`ObjectStore`] instance, for example from
/// local files or objects from AWS S3.
///
/// # Features:
/// * Reading multiple files as a single table
/// * Hive style partitioning (e.g., directories named `date=2024-06-01`)
/// * Merges schemas from files with compatible but not identical schemas (see [`ListingTableConfig::file_schema`])
/// * `limit`, `filter` and `projection` pushdown for formats that support it (e.g.,
///   Parquet)
/// * Statistics collection and pruning based on file metadata
/// * Pre-existing sort order (see [`ListingOptions::file_sort_order`])
/// * Metadata caching to speed up repeated queries (see [`FileMetadataCache`])
/// * Statistics caching (see [`FileStatisticsCache`])
///
/// [`FileMetadataCache`]: datafusion_execution::cache::cache_manager::FileMetadataCache
///
/// # Reading Directories and Hive Style Partitioning
///
/// For example, given the `table1` directory (or object store prefix)
///
/// ```text
/// table1
///  ├── file1.parquet
///  └── file2.parquet
/// ```
///
/// A `ListingTable` would read the files `file1.parquet` and `file2.parquet` as
/// a single table, merging the schemas if the files have compatible but not
/// identical schemas.
///
/// Given the `table2` directory (or object store prefix)
///
/// ```text
/// table2
///  ├── date=2024-06-01
///  │    ├── file3.parquet
///  │    └── file4.parquet
///  └── date=2024-06-02
///       └── file5.parquet
/// ```
///
/// A `ListingTable` would read the files `file3.parquet`, `file4.parquet`, and
/// `file5.parquet` as a single table, again merging schemas if necessary.
///
/// Given the hive style partitioning structure (e.g,. directories named
/// `date=2024-06-01` and `date=2026-06-02`), `ListingTable` also adds a `date`
/// column when reading the table:
/// * The files in `table2/date=2024-06-01` will have the value `2024-06-01`
/// * The files in `table2/date=2024-06-02` will have the value `2024-06-02`.
///
/// If the query has a predicate like `WHERE date = '2024-06-01'`
/// only the corresponding directory will be read.
///
/// # See Also
///
/// 1. [`ListingTableConfig`]: Configuration options
/// 1. [`DataSourceExec`]: `ExecutionPlan` used by `ListingTable`
///
/// [`DataSourceExec`]: datafusion_datasource::source::DataSourceExec
///
/// # Caching Metadata
///
/// Some formats, such as Parquet, use the `FileMetadataCache` to cache file
/// metadata that is needed to execute but expensive to read, such as row
/// groups and statistics. The cache is scoped to the `SessionContext` and can
/// be configured via the [runtime config options].
///
/// [runtime config options]: https://datafusion.apache.org/user-guide/configs.html#runtime-configuration-settings
///
/// # Example: Read a directory of parquet files using a [`ListingTable`]
///
/// ```no_run
/// # use datafusion_common::Result;
/// # use std::sync::Arc;
/// # use datafusion_catalog::TableProvider;
/// # use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
/// # use datafusion_datasource::ListingTableUrl;
/// # use datafusion_datasource_parquet::file_format::ParquetFormat;/// #
/// # use datafusion_catalog::Session;
/// async fn get_listing_table(session: &dyn Session) -> Result<Arc<dyn TableProvider>> {
/// let table_path = "/path/to/parquet";
///
/// // Parse the path
/// let table_path = ListingTableUrl::parse(table_path)?;
///
/// // Create default parquet options
/// let file_format = ParquetFormat::new();
/// let listing_options = ListingOptions::new(Arc::new(file_format))
///   .with_file_extension(".parquet");
///
/// // Resolve the schema
/// let resolved_schema = listing_options
///    .infer_schema(session, &table_path)
///    .await?;
///
/// let config = ListingTableConfig::new(table_path)
///   .with_listing_options(listing_options)
///   .with_schema(resolved_schema);
///
/// // Create a new TableProvider
/// let provider = Arc::new(ListingTable::try_new(config)?);
///
/// # Ok(provider)
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    /// `file_schema` contains only the columns physically stored in the data files themselves.
    ///     - Represents the actual fields found in files like Parquet, CSV, etc.
    ///     - Used when reading the raw data from files
    file_schema: SchemaRef,
    /// `table_schema` combines `file_schema` + partition columns
    ///     - Partition columns are derived from directory paths (not stored in files)
    ///     - These are columns like "year=2022/month=01" in paths like `/data/year=2022/month=01/file.parquet`
    table_schema: SchemaRef,
    /// Indicates how the schema was derived (inferred or explicitly specified)
    schema_source: SchemaSource,
    /// Options used to configure the listing table such as the file format
    /// and partitioning information
    options: ListingOptions,
    /// The SQL definition for this table, if any
    definition: Option<String>,
    /// Cache for collected file statistics
    collected_statistics: Arc<dyn FileStatisticsCache>,
    /// Constraints applied to this table
    constraints: Constraints,
    /// Column default expressions for columns that are not physically present in the data files
    column_defaults: HashMap<String, Expr>,
    /// Optional [`PhysicalExprAdapterFactory`] for creating physical expression adapters
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl ListingTable {
    /// Create new [`ListingTable`]
    ///
    /// See documentation and example on [`ListingTable`] and [`ListingTableConfig`]
    pub fn try_new(config: ListingTableConfig) -> datafusion_common::Result<Self> {
        // Extract schema_source before moving other parts of the config
        let schema_source = config.schema_source();

        let file_schema = config
            .file_schema
            .ok_or_else(|| internal_datafusion_err!("No schema provided."))?;

        let options = config
            .options
            .ok_or_else(|| internal_datafusion_err!("No ListingOptions provided"))?;

        // Add the partition columns to the file schema
        let mut builder = SchemaBuilder::from(file_schema.as_ref().to_owned());
        for (part_col_name, part_col_type) in &options.table_partition_cols {
            builder.push(Field::new(part_col_name, part_col_type.clone(), false));
        }

        let table_schema = Arc::new(
            builder
                .finish()
                .with_metadata(file_schema.metadata().clone()),
        );

        let table = Self {
            table_paths: config.table_paths,
            file_schema,
            table_schema,
            schema_source,
            options,
            definition: None,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            expr_adapter_factory: config.expr_adapter_factory,
        };

        Ok(table)
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Assign column defaults
    pub fn with_column_defaults(
        mut self,
        column_defaults: HashMap<String, Expr>,
    ) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Set the [`FileStatisticsCache`] used to cache parquet file statistics.
    ///
    /// Setting a statistics cache on the `SessionContext` can avoid refetching statistics
    /// multiple times in the same session.
    ///
    /// If `None`, creates a new [`DefaultFileStatisticsCache`] scoped to this query.
    pub fn with_cache(mut self, cache: Option<Arc<dyn FileStatisticsCache>>) -> Self {
        self.collected_statistics =
            cache.unwrap_or_else(|| Arc::new(DefaultFileStatisticsCache::default()));
        self
    }

    /// Specify the SQL definition for this table, if any
    pub fn with_definition(mut self, definition: Option<String>) -> Self {
        self.definition = definition;
        self
    }

    /// Get paths ref
    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        &self.table_paths
    }

    /// Get options ref
    pub fn options(&self) -> &ListingOptions {
        &self.options
    }

    /// Get the schema source
    pub fn schema_source(&self) -> SchemaSource {
        self.schema_source
    }

    /// Deprecated: Set the [`SchemaAdapterFactory`] for this [`ListingTable`]
    ///
    /// `SchemaAdapterFactory` has been removed. Use [`ListingTableConfig::with_expr_adapter_factory`]
    /// and `PhysicalExprAdapterFactory` instead. See `upgrading.md` for more details.
    ///
    /// This method is a no-op and returns `self` unchanged.
    #[deprecated(
        since = "52.0.0",
        note = "SchemaAdapterFactory has been removed. Use ListingTableConfig::with_expr_adapter_factory and PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    #[expect(deprecated)]
    pub fn with_schema_adapter_factory(
        self,
        _schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        // No-op - just return self unchanged
        self
    }

    /// Deprecated: Returns the [`SchemaAdapterFactory`] used by this [`ListingTable`].
    ///
    /// `SchemaAdapterFactory` has been removed. Use `PhysicalExprAdapterFactory` instead.
    /// See `upgrading.md` for more details.
    ///
    /// Always returns `None`.
    #[deprecated(
        since = "52.0.0",
        note = "SchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    #[expect(deprecated)]
    pub fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        None
    }

    /// Creates a file source for this table
    fn create_file_source(&self) -> Arc<dyn FileSource> {
        let table_schema = TableSchema::new(
            Arc::clone(&self.file_schema),
            self.options
                .table_partition_cols
                .iter()
                .map(|(col, field)| Arc::new(Field::new(col, field.clone(), false)))
                .collect(),
        );

        self.options.format.file_source(table_schema)
    }

    /// Creates output ordering from user-specified file_sort_order or derives
    /// from file orderings when user doesn't specify.
    ///
    /// If user specified `file_sort_order`, that takes precedence.
    /// Otherwise, attempts to derive common ordering from file orderings in
    /// the provided file groups.
    pub fn try_create_output_ordering(
        &self,
        execution_props: &ExecutionProps,
        file_groups: &[FileGroup],
    ) -> datafusion_common::Result<Vec<LexOrdering>> {
        // If user specified sort order, use that
        if !self.options.file_sort_order.is_empty() {
            return create_lex_ordering(
                &self.table_schema,
                &self.options.file_sort_order,
                execution_props,
            );
        }
        if let Some(ordering) = derive_common_ordering_from_files(file_groups) {
            return Ok(vec![ordering]);
        }
        Ok(vec![])
    }
}

/// Derives a common ordering from file orderings across all file groups.
///
/// Returns the common ordering if all files have compatible orderings,
/// otherwise returns None.
///
/// The function finds the longest common prefix among all file orderings.
/// For example, if files have orderings `[a, b, c]` and `[a, b]`, the common
/// ordering is `[a, b]`.
fn derive_common_ordering_from_files(file_groups: &[FileGroup]) -> Option<LexOrdering> {
    enum CurrentOrderingState {
        /// Initial state before processing any files
        FirstFile,
        /// Some common ordering found so far
        SomeOrdering(LexOrdering),
        /// No files have ordering
        NoOrdering,
    }
    let mut state = CurrentOrderingState::FirstFile;

    // Collect file orderings and track counts
    for group in file_groups {
        for file in group.iter() {
            state = match (&state, &file.ordering) {
                // If this is the first file with ordering, set it as current
                (CurrentOrderingState::FirstFile, Some(ordering)) => {
                    CurrentOrderingState::SomeOrdering(ordering.clone())
                }
                (CurrentOrderingState::FirstFile, None) => {
                    CurrentOrderingState::NoOrdering
                }
                // If we have an existing ordering, find common prefix with new ordering
                (CurrentOrderingState::SomeOrdering(current), Some(ordering)) => {
                    // Find common prefix between current and new ordering
                    let prefix_len = current
                        .as_ref()
                        .iter()
                        .zip(ordering.as_ref().iter())
                        .take_while(|(a, b)| a == b)
                        .count();
                    if prefix_len == 0 {
                        log::trace!(
                            "Cannot derive common ordering: no common prefix between orderings {current:?} and {ordering:?}"
                        );
                        return None;
                    } else {
                        let ordering =
                            LexOrdering::new(current.as_ref()[..prefix_len].to_vec())
                                .expect("prefix_len > 0, so ordering must be valid");
                        CurrentOrderingState::SomeOrdering(ordering)
                    }
                }
                // If one file has ordering and another doesn't, no common ordering
                // Return None and log a trace message explaining why
                (CurrentOrderingState::SomeOrdering(ordering), None)
                | (CurrentOrderingState::NoOrdering, Some(ordering)) => {
                    log::trace!(
                        "Cannot derive common ordering: some files have ordering {ordering:?}, others don't"
                    );
                    return None;
                }
                // Both have no ordering, remain in NoOrdering state
                (CurrentOrderingState::NoOrdering, None) => {
                    CurrentOrderingState::NoOrdering
                }
            };
        }
    }

    match state {
        CurrentOrderingState::SomeOrdering(ordering) => Some(ordering),
        _ => None,
    }
}

// Expressions can be used for partition pruning if they can be evaluated using
// only the partition columns and there are partition columns.
fn can_be_evaluated_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty()
        && expr_applicable_for_cols(partition_column_names, expr)
}

#[async_trait]
impl TableProvider for ListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let options = ScanArgs::default()
            .with_projection(projection.map(|p| p.as_slice()))
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(self.scan_with_args(state, options).await?.into_inner())
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> datafusion_common::Result<ScanResult> {
        let projection = args.projection().map(|p| p.to_vec());
        let filters = args.filters().map(|f| f.to_vec()).unwrap_or_default();
        let limit = args.limit();

        // extract types of partition columns
        let table_partition_cols = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| Ok(Arc::new(self.table_schema.field_with_name(&col.0)?.clone())))
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let table_partition_col_names = table_partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        // If the filters can be resolved using only partition cols, there is no need to
        // pushdown it to TableScan, otherwise, `unhandled` pruning predicates will be generated
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluated_for_partition_pruning(&table_partition_col_names, filter)
            });

        // We should not limit the number of partitioned files to scan if there are filters and limit
        // at the same time. This is because the limit should be applied after the filters are applied.
        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let ListFilesResult {
            file_groups: mut partitioned_file_lists,
            statistics,
            grouped_by_partition: partitioned_by_file_group,
        } = self
            .list_files_for_scan(state, &partition_filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection.as_ref())?;
            return Ok(ScanResult::new(Arc::new(EmptyExec::new(projected_schema))));
        }

        let output_ordering = self.try_create_output_ordering(
            state.execution_props(),
            &partitioned_file_lists,
        )?;
        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics_with_target_partitions(
                        &self.table_schema,
                        &partitioned_file_lists,
                        output_ordering,
                        self.options.target_partitions,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => log::debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= self.options.target_partitions {
                    partitioned_file_lists = new_groups;
                } else {
                    log::debug!(
                        "attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered"
                    )
                }
            }
            None => {} // no ordering required
        };

        let Some(object_store_url) =
            self.table_paths.first().map(ListingTableUrl::object_store)
        else {
            return Ok(ScanResult::new(Arc::new(EmptyExec::new(Arc::new(
                Schema::empty(),
            )))));
        };

        let file_source = self.create_file_source();

        // create the execution plan
        let plan = self
            .options
            .format
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(object_store_url, file_source)
                    .with_file_groups(partitioned_file_lists)
                    .with_constraints(self.constraints.clone())
                    .with_statistics(statistics)
                    .with_projection_indices(projection)?
                    .with_limit(limit)
                    .with_output_ordering(output_ordering)
                    .with_expr_adapter(self.expr_adapter_factory.clone())
                    .with_partitioned_by_file_group(partitioned_by_file_group)
                    .build(),
            )
            .await?;

        Ok(ScanResult::new(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        let partition_column_names = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| col.0.as_str())
            .collect::<Vec<_>>();
        filters
            .iter()
            .map(|filter| {
                if can_be_evaluated_for_partition_pruning(&partition_column_names, filter)
                {
                    // if filter can be handled by partition pruning, it is exact
                    return Ok(TableProviderFilterPushDown::Exact);
                }

                Ok(TableProviderFilterPushDown::Inexact)
            })
            .collect()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // Check that the schema of the plan matches the schema of this table.
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        let table_path = &self.table_paths()[0];
        if !table_path.is_collection() {
            return plan_err!(
                "Inserting into a ListingTable backed by a single file is not supported, URL is possibly missing a trailing `/`. \
                To append to an existing file use StreamTable, e.g. by using CREATE UNBOUNDED EXTERNAL TABLE"
            );
        }

        // Get the object store for the table path.
        let store = state.runtime_env().object_store(table_path)?;

        let file_list_stream = pruned_partition_list(
            state.config_options(),
            state.runtime_env(),
            store.as_ref(),
            table_path,
            &[],
            &self.options.file_extension,
            &self.options.table_partition_cols,
        )
        .await?;

        let file_group = file_list_stream.try_collect::<Vec<_>>().await?.into();
        let keep_partition_by_columns =
            state.config_options().execution.keep_partition_by_columns;

        // Invalidate cache entries for this table if they exist
        if let Some(lfc) = state.runtime_env().cache_manager.get_list_files_cache() {
            let key = TableScopedPath {
                table: table_path.get_table_ref().clone(),
                path: table_path.prefix().clone(),
            };
            let _ = lfc.remove(&key);
        }

        // Sink related option, apart from format
        let config = FileSinkConfig {
            original_url: String::default(),
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_group,
            output_schema: self.schema(),
            table_partition_cols: self.options.table_partition_cols.clone(),
            insert_op,
            keep_partition_by_columns,
            file_extension: self.options().format.get_ext(),
        };

        // For writes, we only use user-specified ordering (no file groups to derive from)
        let orderings = self.try_create_output_ordering(state.execution_props(), &[])?;
        // It is sufficient to pass only one of the equivalent orderings:
        let order_requirements = orderings.into_iter().next().map(Into::into);

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

impl ListingTable {
    /// Get the list of files for a scan as well as the file level statistics.
    /// The list is grouped to let the execution plan know how the files should
    /// be distributed to different threads / executors.
    pub async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<ListFilesResult> {
        let store = if let Some(url) = self.table_paths.first() {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok(ListFilesResult {
                file_groups: vec![],
                statistics: Statistics::new_unknown(&self.file_schema),
                grouped_by_partition: false,
            });
        };
        // list files (with partitions)
        // For non-WASM targets, use parallel execution with JoinSet.
        // Note: This implementation collects files into memory per table_path rather than
        // streaming lazily. This is a trade-off required because JoinSet tasks need 'static
        // lifetime, which prevents returning borrowed streams directly. For most use cases,
        // the parallelization benefit outweighs the temporary memory overhead. The WASM
        // fallback below preserves streaming behavior for memory-constrained environments.
        #[cfg(not(target_arch = "wasm32"))]
        let file_list = {
            let mut join_set = JoinSet::new();
            let config = ctx.config_options().clone();
            let runtime_env = Arc::clone(ctx.runtime_env());
            let file_extension = self.options.file_extension.clone();
            let partition_cols = self.options.table_partition_cols.clone();
            let filters = filters.to_vec();

            for table_path in &self.table_paths {
                let store = Arc::clone(&store);
                let table_path = table_path.clone();
                let config = config.clone();
                let runtime_env = Arc::clone(&runtime_env);
                let file_extension = file_extension.clone();
                let partition_cols = partition_cols.clone();
                let filters = filters.clone();

                join_set.spawn(async move {
                    let stream = pruned_partition_list(
                        &config,
                        &runtime_env,
                        store.as_ref(),
                        &table_path,
                        &filters,
                        &file_extension,
                        &partition_cols,
                    )
                    .await?;
                    stream.try_collect::<Vec<_>>().await
                });
            }

            let mut file_list: Vec<
                stream::BoxStream<'static, datafusion_common::Result<PartitionedFile>>,
            > = Vec::with_capacity(self.table_paths.len());
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(files)) => {
                        file_list.push(stream::iter(files.into_iter().map(Ok)).boxed())
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        return Err(datafusion_common::DataFusionError::External(
                            Box::new(e),
                        ));
                    }
                }
            }
            let meta_fetch_concurrency =
                ctx.config_options().execution.meta_fetch_concurrency;
            stream::iter(file_list).flatten_unordered(meta_fetch_concurrency)
        };

        // For WASM targets, use sequential execution with try_join_all
        #[cfg(target_arch = "wasm32")]
        let file_list = {
            let config = ctx.config_options();
            let runtime_env = ctx.runtime_env();
            let file_list = futures::future::try_join_all(self.table_paths.iter().map(
                |table_path| {
                    pruned_partition_list(
                        config,
                        runtime_env,
                        store.as_ref(),
                        table_path,
                        filters,
                        &self.options.file_extension,
                        &self.options.table_partition_cols,
                    )
                },
            ))
            .await?;
            let meta_fetch_concurrency =
                ctx.config_options().execution.meta_fetch_concurrency;
            stream::iter(file_list).flatten_unordered(meta_fetch_concurrency)
        };
        // collect the statistics and ordering if required by the config
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                let (statistics, ordering) = if self.options.collect_stat {
                    self.do_collect_statistics_and_ordering(ctx, &store, &part_file)
                        .await?
                } else {
                    (Arc::new(Statistics::new_unknown(&self.file_schema)), None)
                };
                Ok(part_file
                    .with_statistics(statistics)
                    .with_ordering(ordering))
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency);

        let (file_group, inexact_stats) =
            get_files_with_limit(files, limit, self.options.collect_stat).await?;

        // Threshold: 0 = disabled, N > 0 = enabled when distinct_keys >= N
        //
        // When enabled, files are grouped by their Hive partition column values, allowing
        // FileScanConfig to declare Hash partitioning. This enables the optimizer to skip
        // hash repartitioning for aggregates and joins on partition columns.
        let threshold = ctx.config_options().optimizer.preserve_file_partitions;

        let (file_groups, grouped_by_partition) = if threshold > 0
            && !self.options.table_partition_cols.is_empty()
        {
            let grouped =
                file_group.group_by_partition_values(self.options.target_partitions);
            if grouped.len() >= threshold {
                (grouped, true)
            } else {
                let all_files: Vec<_> =
                    grouped.into_iter().flat_map(|g| g.into_inner()).collect();
                (
                    FileGroup::new(all_files).split_files(self.options.target_partitions),
                    false,
                )
            }
        } else {
            (
                file_group.split_files(self.options.target_partitions),
                false,
            )
        };

        let (file_groups, stats) = compute_all_files_statistics(
            file_groups,
            self.schema(),
            self.options.collect_stat,
            inexact_stats,
        )?;

        // Note: Statistics already include both file columns and partition columns.
        // PartitionedFile::with_statistics automatically appends exact partition column
        // statistics (min=max=partition_value, null_count=0, distinct_count=1) computed
        // from partition_values.
        Ok(ListFilesResult {
            file_groups,
            statistics: stats,
            grouped_by_partition,
        })
    }

    /// Collects statistics and ordering for a given partitioned file.
    ///
    /// This method checks if statistics are cached. If cached, it returns the
    /// cached statistics and infers ordering separately. If not cached, it infers
    /// both statistics and ordering in a single metadata read for efficiency.
    async fn do_collect_statistics_and_ordering(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        part_file: &PartitionedFile,
    ) -> datafusion_common::Result<(Arc<Statistics>, Option<LexOrdering>)> {
        use datafusion_execution::cache::cache_manager::CachedFileMetadata;

        let path = &part_file.object_meta.location;
        let meta = &part_file.object_meta;

        // Check cache first - if we have valid cached statistics and ordering
        if let Some(cached) = self.collected_statistics.get(path)
            && cached.is_valid_for(meta)
        {
            // Return cached statistics and ordering
            return Ok((Arc::clone(&cached.statistics), cached.ordering.clone()));
        }

        // Cache miss or invalid: fetch both statistics and ordering in a single metadata read
        let file_meta = self
            .options
            .format
            .infer_stats_and_ordering(ctx, store, Arc::clone(&self.file_schema), meta)
            .await?;

        let statistics = Arc::new(file_meta.statistics);

        // Store in cache
        self.collected_statistics.put(
            path,
            CachedFileMetadata::new(
                meta.clone(),
                Arc::clone(&statistics),
                file_meta.ordering.clone(),
            ),
        );

        Ok((statistics, file_meta.ordering))
    }
}

/// Processes a stream of partitioned files and returns a `FileGroup` containing the files.
///
/// This function collects files from the provided stream until either:
/// 1. The stream is exhausted
/// 2. The accumulated number of rows exceeds the provided `limit` (if specified)
///
/// # Arguments
/// * `files` - A stream of `Result<PartitionedFile>` items to process
/// * `limit` - An optional row count limit. If provided, the function will stop collecting files
///   once the accumulated number of rows exceeds this limit
/// * `collect_stats` - Whether to collect and accumulate statistics from the files
///
/// # Returns
/// A `Result` containing a `FileGroup` with the collected files
/// and a boolean indicating whether the statistics are inexact.
///
/// # Note
/// The function will continue processing files if statistics are not available or if the
/// limit is not provided. If `collect_stats` is false, statistics won't be accumulated
/// but files will still be collected.
async fn get_files_with_limit(
    files: impl Stream<Item = datafusion_common::Result<PartitionedFile>>,
    limit: Option<usize>,
    collect_stats: bool,
) -> datafusion_common::Result<(FileGroup, bool)> {
    let mut file_group = FileGroup::default();
    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(files.fuse());
    enum ProcessingState {
        ReadingFiles,
        ReachedLimit,
    }

    let mut state = ProcessingState::ReadingFiles;
    let mut num_rows = Precision::Absent;

    while let Some(file_result) = all_files.next().await {
        // Early exit if we've already reached our limit
        if matches!(state, ProcessingState::ReachedLimit) {
            break;
        }

        let file = file_result?;

        // Update file statistics regardless of state
        if collect_stats && let Some(file_stats) = &file.statistics {
            num_rows = if file_group.is_empty() {
                // For the first file, just take its row count
                file_stats.num_rows
            } else {
                // For subsequent files, accumulate the counts
                num_rows.add(&file_stats.num_rows)
            };
        }

        // Always add the file to our group
        file_group.push(file);

        // Check if we've hit the limit (if one was specified)
        if let Some(limit) = limit
            && let Precision::Exact(row_count) = num_rows
            && row_count > limit
        {
            state = ProcessingState::ReachedLimit;
        }
    }
    // If we still have files in the stream, it means that the limit kicked
    // in, and the statistic could have been different had we processed the
    // files in a different order.
    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::SortOptions;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
    use std::sync::Arc;

    /// Helper to create a PhysicalSortExpr
    fn sort_expr(
        name: &str,
        idx: usize,
        descending: bool,
        nulls_first: bool,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr::new(
            Arc::new(Column::new(name, idx)),
            SortOptions {
                descending,
                nulls_first,
            },
        )
    }

    /// Helper to create a LexOrdering (unwraps the Option)
    fn lex_ordering(exprs: Vec<PhysicalSortExpr>) -> LexOrdering {
        LexOrdering::new(exprs).expect("expected non-empty ordering")
    }

    /// Helper to create a PartitionedFile with optional ordering
    fn create_file(name: &str, ordering: Option<LexOrdering>) -> PartitionedFile {
        PartitionedFile::new(name.to_string(), 1024).with_ordering(ordering)
    }

    #[test]
    fn test_derive_common_ordering_all_files_same_ordering() {
        // All files have the same ordering -> returns that ordering
        let ordering = lex_ordering(vec![
            sort_expr("a", 0, false, true),
            sort_expr("b", 1, true, false),
        ]);

        let file_groups = vec![
            FileGroup::new(vec![
                create_file("f1.parquet", Some(ordering.clone())),
                create_file("f2.parquet", Some(ordering.clone())),
            ]),
            FileGroup::new(vec![create_file("f3.parquet", Some(ordering.clone()))]),
        ];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, Some(ordering));
    }

    #[test]
    fn test_derive_common_ordering_common_prefix() {
        // Files have different orderings but share a common prefix
        let ordering_abc = lex_ordering(vec![
            sort_expr("a", 0, false, true),
            sort_expr("b", 1, false, true),
            sort_expr("c", 2, false, true),
        ]);
        let ordering_ab = lex_ordering(vec![
            sort_expr("a", 0, false, true),
            sort_expr("b", 1, false, true),
        ]);

        let file_groups = vec![FileGroup::new(vec![
            create_file("f1.parquet", Some(ordering_abc)),
            create_file("f2.parquet", Some(ordering_ab.clone())),
        ])];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, Some(ordering_ab));
    }

    #[test]
    fn test_derive_common_ordering_no_common_prefix() {
        // Files have completely different orderings -> returns None
        let ordering_a = lex_ordering(vec![sort_expr("a", 0, false, true)]);
        let ordering_b = lex_ordering(vec![sort_expr("b", 1, false, true)]);

        let file_groups = vec![FileGroup::new(vec![
            create_file("f1.parquet", Some(ordering_a)),
            create_file("f2.parquet", Some(ordering_b)),
        ])];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, None);
    }

    #[test]
    fn test_derive_common_ordering_mixed_with_none() {
        // Some files have ordering, some don't -> returns None
        let ordering = lex_ordering(vec![sort_expr("a", 0, false, true)]);

        let file_groups = vec![FileGroup::new(vec![
            create_file("f1.parquet", Some(ordering)),
            create_file("f2.parquet", None),
        ])];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, None);
    }

    #[test]
    fn test_derive_common_ordering_all_none() {
        // No files have ordering -> returns None
        let file_groups = vec![FileGroup::new(vec![
            create_file("f1.parquet", None),
            create_file("f2.parquet", None),
        ])];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, None);
    }

    #[test]
    fn test_derive_common_ordering_empty_groups() {
        // Empty file groups -> returns None
        let file_groups: Vec<FileGroup> = vec![];
        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, None);
    }

    #[test]
    fn test_derive_common_ordering_single_file() {
        // Single file with ordering -> returns that ordering
        let ordering = lex_ordering(vec![
            sort_expr("a", 0, false, true),
            sort_expr("b", 1, true, false),
        ]);

        let file_groups = vec![FileGroup::new(vec![create_file(
            "f1.parquet",
            Some(ordering.clone()),
        )])];

        let result = derive_common_ordering_from_files(&file_groups);
        assert_eq!(result, Some(ordering));
    }
}

#[cfg(test)]
mod benchmark_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use datafusion_catalog::Session;
    use datafusion_datasource::TableSchema;
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_compression_type::FileCompressionType;
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, path::Path,
    };
    use std::time::Duration;
    use tokio::time::Instant;

    #[derive(Debug)]
    struct MockDelayedStore {
        delay: Duration,
    }

    #[async_trait]
    impl ObjectStore for MockDelayedStore {
        async fn put(
            &self,
            _location: &Path,
            _payload: PutPayload,
        ) -> object_store::Result<object_store::PutResult> {
            Ok(object_store::PutResult {
                e_tag: None,
                version: None,
            })
        }
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            Ok(object_store::PutResult {
                e_tag: None,
                version: None,
            })
        }
        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }
        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }
        async fn get(&self, _location: &Path) -> object_store::Result<GetResult> {
            Err(object_store::Error::NotFound {
                path: "none".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "none",
                )),
            })
        }
        async fn get_opts(
            &self,
            _location: &Path,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            Err(object_store::Error::NotFound {
                path: "none".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "none",
                )),
            })
        }
        async fn get_range(
            &self,
            _location: &Path,
            _range: std::ops::Range<u64>,
        ) -> object_store::Result<bytes::Bytes> {
            Err(object_store::Error::NotFound {
                path: "none".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "none",
                )),
            })
        }
        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            Err(object_store::Error::NotFound {
                path: "none".to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "none",
                )),
            })
        }
        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            Ok(())
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            let delay = self.delay;
            let prefix = prefix.cloned();
            let fut = async move {
                tokio::time::sleep(delay).await;
                Ok(ObjectMeta {
                    location: prefix
                        .unwrap_or_else(|| Path::from("/"))
                        .child("file.parquet"),
                    last_modified: chrono::Utc::now(),
                    size: 1024,
                    e_tag: None,
                    version: None,
                })
            };
            stream::once(fut).boxed()
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            tokio::time::sleep(self.delay).await;
            let prefix_path = prefix.cloned().unwrap_or_else(|| Path::from("/"));
            Ok(ListResult {
                objects: vec![ObjectMeta {
                    location: prefix_path.child("file.parquet"),
                    last_modified: chrono::Utc::now(),
                    size: 1024,
                    e_tag: None,
                    version: None,
                }],
                common_prefixes: vec![],
            })
        }
        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            Ok(())
        }
        async fn rename(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            Ok(())
        }
        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            Ok(())
        }
    }

    impl std::fmt::Display for MockDelayedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockDelayedStore")
        }
    }

    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
    use futures::{StreamExt, stream};
    use std::any::Any;
    use std::collections::HashMap;

    struct MockSession {
        runtime: Arc<RuntimeEnv>,
        session_config: datafusion_execution::config::SessionConfig,
    }

    #[async_trait]
    impl Session for MockSession {
        fn session_id(&self) -> &str {
            "session"
        }
        fn config(&self) -> &datafusion_execution::config::SessionConfig {
            &self.session_config
        }
        async fn create_physical_plan(
            &self,
            _plan: &LogicalPlan,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            todo!()
        }
        fn create_physical_expr(
            &self,
            _expr: Expr,
            _schema: &DFSchema,
        ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
            todo!()
        }
        fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
            todo!()
        }
        fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
            todo!()
        }
        fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
            todo!()
        }
        fn runtime_env(&self) -> &Arc<RuntimeEnv> {
            &self.runtime
        }
        fn execution_props(&self) -> &ExecutionProps {
            todo!()
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn table_options(&self) -> &datafusion_common::config::TableOptions {
            todo!()
        }
        fn table_options_mut(&mut self) -> &mut datafusion_common::config::TableOptions {
            todo!()
        }
        fn task_ctx(&self) -> Arc<datafusion_execution::TaskContext> {
            todo!()
        }
    }

    #[derive(Debug)]
    struct MockFormat;
    #[async_trait]
    impl datafusion_datasource::file_format::FileFormat for MockFormat {
        fn as_any(&self) -> &dyn Any {
            self
        }
        async fn infer_schema(
            &self,
            _state: &dyn Session,
            _store: &Arc<dyn ObjectStore>,
            _objects: &[ObjectMeta],
        ) -> datafusion_common::Result<SchemaRef> {
            Ok(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int32,
                false,
            )])))
        }
        async fn infer_stats(
            &self,
            _state: &dyn Session,
            _store: &Arc<dyn ObjectStore>,
            _table_schema: SchemaRef,
            _object: &ObjectMeta,
        ) -> datafusion_common::Result<Statistics> {
            Ok(Statistics::new_unknown(&_table_schema))
        }
        async fn create_physical_plan(
            &self,
            _state: &dyn Session,
            _conf: FileScanConfig,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            todo!()
        }
        fn file_source(&self, _schema: TableSchema) -> Arc<dyn FileSource> {
            todo!()
        }
        fn get_ext(&self) -> String {
            "parquet".to_string()
        }
        fn get_ext_with_compression(
            &self,
            _c: &FileCompressionType,
        ) -> datafusion_common::Result<String> {
            Ok("parquet".to_string())
        }
        fn compression_type(&self) -> Option<FileCompressionType> {
            None
        }
    }

    #[tokio::test]
    async fn benchmark_parallel_listing() -> datafusion_common::Result<()> {
        let delay = Duration::from_millis(100);
        let num_paths = 10;
        let store = Arc::new(MockDelayedStore { delay });
        let runtime = Arc::new(RuntimeEnv::default());
        let url = url::Url::parse("test://").unwrap();
        runtime.register_object_store(&url, store.clone());

        let session = MockSession {
            runtime,
            session_config: datafusion_execution::config::SessionConfig::default(),
        };

        let paths: Vec<ListingTableUrl> = (0..num_paths)
            .map(|i| ListingTableUrl::parse(format!("test:///table/{}", i)).unwrap())
            .collect();

        let options = ListingOptions::new(Arc::new(MockFormat));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let config = ListingTableConfig::new_with_multi_paths(paths.clone())
            .with_listing_options(options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;

        let start = Instant::now();
        let _ = table.list_files_for_scan(&session, &[], None).await?;
        let duration = start.elapsed();

        println!(
            "Listing {} paths with {}ms delay took: {:?}",
            num_paths,
            delay.as_millis(),
            duration
        );

        // If it was sequential, it would take >= num_paths * delay (1000ms).
        // With JoinSet/concurrent, it should be much closer to 100ms.
        assert!(
            duration < Duration::from_millis(500),
            "Listing took too long: {:?}",
            duration
        );

        Ok(())
    }
}
