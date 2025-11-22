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
    internal_datafusion_err, plan_err, project_schema, Constraints, DataFusionError,
    SchemaExt, Statistics,
};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory,
};
use datafusion_datasource::{
    compute_all_files_statistics, ListingTableUrl, PartitionedFile, TableSchema,
};
use datafusion_execution::cache::cache_manager::FileStatisticsCache;
use datafusion_execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::create_lex_ordering;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::ExecutionPlan;
use futures::{future, stream, Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

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
    collected_statistics: FileStatisticsCache,
    /// Constraints applied to this table
    constraints: Constraints,
    /// Column default expressions for columns that are not physically present in the data files
    column_defaults: HashMap<String, Expr>,
    /// Optional [`SchemaAdapterFactory`] for creating schema adapters
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
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
            schema_adapter_factory: config.schema_adapter_factory,
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
    pub fn with_cache(mut self, cache: Option<FileStatisticsCache>) -> Self {
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

    /// Set the [`SchemaAdapterFactory`] for this [`ListingTable`]
    ///
    /// The schema adapter factory is used to create schema adapters that can
    /// handle schema evolution and type conversions when reading files with
    /// different schemas than the table schema.
    ///
    /// # Example: Adding Schema Evolution Support
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion_catalog_listing::{ListingTable, ListingTableConfig, ListingOptions};
    /// # use datafusion_datasource::ListingTableUrl;
    /// # use datafusion_datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapter};
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    /// # use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
    /// # let table_path = ListingTableUrl::parse("file:///path/to/data").unwrap();
    /// # let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// # let config = ListingTableConfig::new(table_path).with_listing_options(options).with_schema(schema);
    /// # let table = ListingTable::try_new(config).unwrap();
    /// let table_with_evolution = table
    ///     .with_schema_adapter_factory(Arc::new(DefaultSchemaAdapterFactory));
    /// ```
    /// See [`ListingTableConfig::with_schema_adapter_factory`] for an example of custom SchemaAdapterFactory.
    pub fn with_schema_adapter_factory(
        self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Self {
        Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self
        }
    }

    /// Get the [`SchemaAdapterFactory`] for this table
    pub fn schema_adapter_factory(&self) -> Option<&Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.as_ref()
    }

    /// Creates a schema adapter for mapping between file and table schemas
    ///
    /// Uses the configured schema adapter factory if available, otherwise falls back
    /// to the default implementation.
    fn create_schema_adapter(&self) -> Box<dyn SchemaAdapter> {
        let table_schema = self.schema();
        match &self.schema_adapter_factory {
            Some(factory) => {
                factory.create_with_projected_schema(Arc::clone(&table_schema))
            }
            None => DefaultSchemaAdapterFactory::from_schema(Arc::clone(&table_schema)),
        }
    }

    /// Creates a file source and applies schema adapter factory if available
    fn create_file_source_with_schema_adapter(
        &self,
    ) -> datafusion_common::Result<Arc<dyn FileSource>> {
        let table_schema = TableSchema::new(
            Arc::clone(&self.file_schema),
            self.options
                .table_partition_cols
                .iter()
                .map(|(col, field)| Arc::new(Field::new(col, field.clone(), false)))
                .collect(),
        );

        let mut source = self.options.format.file_source(table_schema);
        // Apply schema adapter to source if available
        //
        // The source will use this SchemaAdapter to adapt data batches as they flow up the plan.
        // Note: ListingTable also creates a SchemaAdapter in `scan()` but that is only used to adapt collected statistics.
        if let Some(factory) = &self.schema_adapter_factory {
            source = source.with_schema_adapter_factory(Arc::clone(factory))?;
        }
        Ok(source)
    }

    /// If file_sort_order is specified, creates the appropriate physical expressions
    pub fn try_create_output_ordering(
        &self,
        execution_props: &ExecutionProps,
    ) -> datafusion_common::Result<Vec<LexOrdering>> {
        create_lex_ordering(
            &self.table_schema,
            &self.options.file_sort_order,
            execution_props,
        )
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

        let (mut partitioned_file_lists, statistics) = self
            .list_files_for_scan(state, &partition_filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection.as_ref())?;
            return Ok(ScanResult::new(Arc::new(EmptyExec::new(projected_schema))));
        }

        let output_ordering = self.try_create_output_ordering(state.execution_props())?;
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
                    log::debug!("attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered")
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

        let file_source = self.create_file_source_with_schema_adapter()?;

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
            state,
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

        let orderings = self.try_create_output_ordering(state.execution_props())?;
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
    ) -> datafusion_common::Result<(Vec<FileGroup>, Statistics)> {
        let store = if let Some(url) = self.table_paths.first() {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok((vec![], Statistics::new_unknown(&self.file_schema)));
        };
        // list files (with partitions)
        let file_list = future::try_join_all(self.table_paths.iter().map(|table_path| {
            pruned_partition_list(
                ctx,
                store.as_ref(),
                table_path,
                filters,
                &self.options.file_extension,
                &self.options.table_partition_cols,
            )
        }))
        .await?;
        let meta_fetch_concurrency =
            ctx.config_options().execution.meta_fetch_concurrency;
        let file_list = stream::iter(file_list).flatten_unordered(meta_fetch_concurrency);
        // collect the statistics if required by the config
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                let statistics = if self.options.collect_stat {
                    self.do_collect_statistics(ctx, &store, &part_file).await?
                } else {
                    Arc::new(Statistics::new_unknown(&self.file_schema))
                };
                Ok(part_file.with_statistics(statistics))
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency);

        let (file_group, inexact_stats) =
            get_files_with_limit(files, limit, self.options.collect_stat).await?;

        let file_groups = file_group.split_files(self.options.target_partitions);
        let (mut file_groups, mut stats) = compute_all_files_statistics(
            file_groups,
            self.schema(),
            self.options.collect_stat,
            inexact_stats,
        )?;

        let schema_adapter = self.create_schema_adapter();
        let (schema_mapper, _) = schema_adapter.map_schema(self.file_schema.as_ref())?;

        stats.column_statistics =
            schema_mapper.map_column_statistics(&stats.column_statistics)?;
        file_groups.iter_mut().try_for_each(|file_group| {
            if let Some(stat) = file_group.statistics_mut() {
                stat.column_statistics =
                    schema_mapper.map_column_statistics(&stat.column_statistics)?;
            }
            Ok::<_, DataFusionError>(())
        })?;
        Ok((file_groups, stats))
    }

    /// Collects statistics for a given partitioned file.
    ///
    /// This method first checks if the statistics for the given file are already cached.
    /// If they are, it returns the cached statistics.
    /// If they are not, it infers the statistics from the file and stores them in the cache.
    async fn do_collect_statistics(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        part_file: &PartitionedFile,
    ) -> datafusion_common::Result<Arc<Statistics>> {
        match self
            .collected_statistics
            .get_with_extra(&part_file.object_meta.location, &part_file.object_meta)
        {
            Some(statistics) => Ok(statistics),
            None => {
                let statistics = self
                    .options
                    .format
                    .infer_stats(
                        ctx,
                        store,
                        Arc::clone(&self.file_schema),
                        &part_file.object_meta,
                    )
                    .await?;
                let statistics = Arc::new(statistics);
                self.collected_statistics.put_with_extra(
                    &part_file.object_meta.location,
                    Arc::clone(&statistics),
                    &part_file.object_meta,
                );
                Ok(statistics)
            }
        }
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
        if collect_stats {
            if let Some(file_stats) = &file.statistics {
                num_rows = if file_group.is_empty() {
                    // For the first file, just take its row count
                    file_stats.num_rows
                } else {
                    // For subsequent files, accumulate the counts
                    num_rows.add(&file_stats.num_rows)
                };
            }
        }

        // Always add the file to our group
        file_group.push(file);

        // Check if we've hit the limit (if one was specified)
        if let Some(limit) = limit {
            if let Precision::Exact(row_count) = num_rows {
                if row_count > limit {
                    state = ProcessingState::ReachedLimit;
                }
            }
        }
    }
    // If we still have files in the stream, it means that the limit kicked
    // in, and the statistic could have been different had we processed the
    // files in a different order.
    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}
