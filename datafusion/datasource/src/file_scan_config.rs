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

//! [`FileScanConfig`] to configure scanning of possibly partitioned
//! file sources.

use crate::file_groups::FileGroup;
#[allow(unused_imports)]
use crate::schema_adapter::SchemaAdapterFactory;
use crate::{
    display::FileGroupsDisplay, file::FileSource,
    file_compression_type::FileCompressionType, file_stream::FileStream,
    source::DataSource, statistics::MinMaxStatistics, PartitionedFile,
};
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    internal_datafusion_err, internal_err, ColumnStatistics, Constraints, Result,
    ScalarValue, Statistics,
};
use datafusion_execution::{
    object_store::ObjectStoreUrl, SendableRecordBatchStream, TaskContext,
};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{split_conjunction, EquivalenceProperties, Partitioning};
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::{
    display::{display_orderings, ProjectSchemaDisplay},
    filter_pushdown::FilterPushdownPropagation,
    metrics::ExecutionPlanMetricsSet,
    DisplayAs, DisplayFormatType,
};
use std::{any::Any, fmt::Debug, fmt::Formatter, fmt::Result as FmtResult, sync::Arc};

use datafusion_physical_expr::equivalence::project_orderings;
use datafusion_physical_plan::coop::cooperative;
use datafusion_physical_plan::execution_plan::SchedulingType;
use log::{debug, warn};

/// The base configurations for a [`DataSourceExec`], the a physical plan for
/// any given file format.
///
/// Use [`DataSourceExec::from_data_source`] to create a [`DataSourceExec`] from a ``FileScanConfig`.
///
/// # Example
/// ```
/// # use std::any::Any;
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Field, Fields, DataType, Schema, SchemaRef};
/// # use object_store::ObjectStore;
/// # use datafusion_common::Result;
/// # use datafusion_datasource::file::FileSource;
/// # use datafusion_datasource::file_groups::FileGroup;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource::file_stream::FileOpener;
/// # use datafusion_datasource::source::DataSourceExec;
/// # use datafusion_datasource::table_schema::TableSchema;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_physical_expr::projection::ProjectionExprs;
/// # use datafusion_physical_plan::ExecutionPlan;
/// # use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
/// # use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
/// # let file_schema = Arc::new(Schema::new(vec![
/// #  Field::new("c1", DataType::Int32, false),
/// #  Field::new("c2", DataType::Int32, false),
/// #  Field::new("c3", DataType::Int32, false),
/// #  Field::new("c4", DataType::Int32, false),
/// # ]));
/// # // Note: crate mock ParquetSource, as ParquetSource is not in the datasource crate
/// #[derive(Clone)]
/// # struct ParquetSource {
/// #    table_schema: TableSchema,
/// #    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>
/// # };
/// # impl FileSource for ParquetSource {
/// #  fn create_file_opener(&self, _: Arc<dyn ObjectStore>, _: &FileScanConfig, _: usize) -> Result<Arc<dyn FileOpener>> { unimplemented!() }
/// #  fn as_any(&self) -> &dyn Any { self  }
/// #  fn table_schema(&self) -> &TableSchema { &self.table_schema }
/// #  fn with_batch_size(&self, _: usize) -> Arc<dyn FileSource> { unimplemented!() }
/// #  fn metrics(&self) -> &ExecutionPlanMetricsSet { unimplemented!() }
/// #  fn file_type(&self) -> &str { "parquet" }
/// #  fn with_schema_adapter_factory(&self, factory: Arc<dyn SchemaAdapterFactory>) -> Result<Arc<dyn FileSource>> { Ok(Arc::new(Self {table_schema: self.table_schema.clone(), schema_adapter_factory: Some(factory)} )) }
/// #  fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> { self.schema_adapter_factory.clone() }
/// #  // Note that this implementation drops the projection on the floor, it is not complete!
/// #  fn try_pushdown_projection(&self, projection: &ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>> { Ok(Some(Arc::new(self.clone()) as Arc<dyn FileSource>)) }
/// #  }
/// # impl ParquetSource {
/// #  fn new(table_schema: impl Into<TableSchema>) -> Self { Self {table_schema: table_schema.into(), schema_adapter_factory: None} }
/// # }
/// // create FileScan config for reading parquet files from file://
/// let object_store_url = ObjectStoreUrl::local_filesystem();
/// let file_source = Arc::new(ParquetSource::new(file_schema.clone()));
/// let config = FileScanConfigBuilder::new(object_store_url, file_source)
///   .with_limit(Some(1000))            // read only the first 1000 records
///   .with_projection_indices(Some(vec![2, 3])) // project columns 2 and 3
///   .expect("Failed to push down projection")
///    // Read /tmp/file1.parquet with known size of 1234 bytes in a single group
///   .with_file(PartitionedFile::new("file1.parquet", 1234))
///   // Read /tmp/file2.parquet 56 bytes and /tmp/file3.parquet 78 bytes
///   // in a  single row group
///   .with_file_group(FileGroup::new(vec![
///    PartitionedFile::new("file2.parquet", 56),
///    PartitionedFile::new("file3.parquet", 78),
///   ])).build();
/// // create an execution plan from the config
/// let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
/// ```
///
/// [`DataSourceExec`]: crate::source::DataSourceExec
/// [`DataSourceExec::from_data_source`]: crate::source::DataSourceExec::from_data_source
#[derive(Clone)]
pub struct FileScanConfig {
    /// Object store URL, used to get an [`ObjectStore`] instance from
    /// [`RuntimeEnv::object_store`]
    ///
    /// This `ObjectStoreUrl` should be the prefix of the absolute url for files
    /// as `file://` or `s3://my_bucket`. It should not include the path to the
    /// file itself. The relevant URL prefix must be registered via
    /// [`RuntimeEnv::register_object_store`]
    ///
    /// [`ObjectStore`]: object_store::ObjectStore
    /// [`RuntimeEnv::register_object_store`]: datafusion_execution::runtime_env::RuntimeEnv::register_object_store
    /// [`RuntimeEnv::object_store`]: datafusion_execution::runtime_env::RuntimeEnv::object_store
    pub object_store_url: ObjectStoreUrl,
    /// List of files to be processed, grouped into partitions
    ///
    /// Each file must have a schema of `file_schema` or a subset. If
    /// a particular file has a subset, the missing columns are
    /// padded with NULLs.
    ///
    /// DataFusion may attempt to read each partition of files
    /// concurrently, however files *within* a partition will be read
    /// sequentially, one after the next.
    pub file_groups: Vec<FileGroup>,
    /// Table constraints
    pub constraints: Constraints,
    /// The maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    pub limit: Option<usize>,
    /// All equivalent lexicographical orderings that describe the schema.
    pub output_ordering: Vec<LexOrdering>,
    /// File compression type
    pub file_compression_type: FileCompressionType,
    /// Are new lines in values supported for CSVOptions
    pub new_lines_in_values: bool,
    /// File source such as `ParquetSource`, `CsvSource`, `JsonSource`, etc.
    pub file_source: Arc<dyn FileSource>,
    /// Batch size while creating new batches
    /// Defaults to [`datafusion_common::config::ExecutionOptions`] batch_size.
    pub batch_size: Option<usize>,
    /// Expression adapter used to adapt filters and projections that are pushed down into the scan
    /// from the logical schema to the physical schema of the file.
    pub expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    /// Unprojected statistics for the table (file schema + partition columns).
    /// These are projected on-demand via `projected_stats()`.
    ///
    /// Note that this field is pub(crate) because accessing it directly from outside
    /// would be incorrect if there are filters being applied, thus this should be accessed
    /// via [`FileScanConfig::statistics`].
    pub(crate) statistics: Statistics,
}

/// A builder for [`FileScanConfig`]'s.
///
/// Example:
///
/// ```rust
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_datasource::file_scan_config::{FileScanConfigBuilder, FileScanConfig};
/// # use datafusion_datasource::file_compression_type::FileCompressionType;
/// # use datafusion_datasource::file_groups::FileGroup;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource::table_schema::TableSchema;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_common::Statistics;
/// # use datafusion_datasource::file::FileSource;
///
/// # fn main() {
/// # fn with_source(file_source: Arc<dyn FileSource>) {
///     // Create a schema for our Parquet files
///     let file_schema = Arc::new(Schema::new(vec![
///         Field::new("id", DataType::Int32, false),
///         Field::new("value", DataType::Utf8, false),
///     ]));
///
///     // Create partition columns
///     let partition_cols = vec![
///         Arc::new(Field::new("date", DataType::Utf8, false)),
///     ];
///
///     // Create table schema with file schema and partition columns
///     let table_schema = TableSchema::new(file_schema, partition_cols);
///
///     // Create a builder for scanning Parquet files from a local filesystem
///     let config = FileScanConfigBuilder::new(
///         ObjectStoreUrl::local_filesystem(),
///         file_source,
///     )
///     // Set a limit of 1000 rows
///     .with_limit(Some(1000))
///     // Project only the first column
///     .with_projection_indices(Some(vec![0]))
///     .expect("Failed to push down projection")
///     // Add a file group with two files
///     .with_file_group(FileGroup::new(vec![
///         PartitionedFile::new("data/date=2024-01-01/file1.parquet", 1024),
///         PartitionedFile::new("data/date=2024-01-01/file2.parquet", 2048),
///     ]))
///     // Set compression type
///     .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
///     // Build the final config
///     .build();
/// # }
/// # }
/// ```
#[derive(Clone)]
pub struct FileScanConfigBuilder {
    object_store_url: ObjectStoreUrl,
    file_source: Arc<dyn FileSource>,
    limit: Option<usize>,
    constraints: Option<Constraints>,
    file_groups: Vec<FileGroup>,
    statistics: Option<Statistics>,
    output_ordering: Vec<LexOrdering>,
    file_compression_type: Option<FileCompressionType>,
    new_lines_in_values: Option<bool>,
    batch_size: Option<usize>,
    expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>,
}

impl FileScanConfigBuilder {
    /// Create a new [`FileScanConfigBuilder`] with default settings for scanning files.
    ///
    /// # Parameters:
    /// * `object_store_url`: See [`FileScanConfig::object_store_url`]
    /// * `file_source`: See [`FileScanConfig::file_source`]. The file source must have
    ///   a schema set via its constructor.
    pub fn new(
        object_store_url: ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
    ) -> Self {
        Self {
            object_store_url,
            file_source,
            file_groups: vec![],
            statistics: None,
            output_ordering: vec![],
            file_compression_type: None,
            new_lines_in_values: None,
            limit: None,
            constraints: None,
            batch_size: None,
            expr_adapter_factory: None,
        }
    }

    /// Set the maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Set the file source for scanning files.
    ///
    /// This method allows you to change the file source implementation (e.g. ParquetSource, CsvSource, etc.)
    /// after the builder has been created.
    pub fn with_source(mut self, file_source: Arc<dyn FileSource>) -> Self {
        self.file_source = file_source;
        self
    }

    pub fn table_schema(&self) -> &SchemaRef {
        self.file_source.table_schema().table_schema()
    }

    /// Set the columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    ///
    /// # Deprecated
    /// Use [`Self::with_projection_indices`] instead. This method will be removed in a future release.
    #[deprecated(since = "51.0.0", note = "Use with_projection_indices instead")]
    pub fn with_projection(self, indices: Option<Vec<usize>>) -> Self {
        match self.clone().with_projection_indices(indices) {
            Ok(builder) => builder,
            Err(e) => {
                warn!("Failed to push down projection in FileScanConfigBuilder::with_projection: {e}");
                self
            }
        }
    }

    /// Set the columns on which to project the data using column indices.
    ///
    /// Indexes that are higher than the number of columns of `file_schema` refer to `table_partition_cols`.
    pub fn with_projection_indices(
        mut self,
        indices: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projection_exprs = indices.map(|indices| {
            ProjectionExprs::from_indices(
                &indices,
                self.file_source.table_schema().table_schema(),
            )
        });
        let Some(projection_exprs) = projection_exprs else {
            return Ok(self);
        };
        let new_source = self
            .file_source
            .try_pushdown_projection(&projection_exprs)
            .map_err(|e| {
                internal_datafusion_err!(
                    "Failed to push down projection in FileScanConfigBuilder::build: {e}"
                )
            })?;
        if let Some(new_source) = new_source {
            self.file_source = new_source;
        } else {
            internal_err!(
                "FileSource {} does not support projection pushdown",
                self.file_source.file_type()
            )?;
        }
        Ok(self)
    }

    /// Set the table constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    /// Set the estimated overall statistics of the files, taking `filters` into account.
    /// Defaults to [`Statistics::new_unknown`].
    pub fn with_statistics(mut self, statistics: Statistics) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Set the list of files to be processed, grouped into partitions.
    ///
    /// Each file must have a schema of `file_schema` or a subset. If
    /// a particular file has a subset, the missing columns are
    /// padded with NULLs.
    ///
    /// DataFusion may attempt to read each partition of files
    /// concurrently, however files *within* a partition will be read
    /// sequentially, one after the next.
    pub fn with_file_groups(mut self, file_groups: Vec<FileGroup>) -> Self {
        self.file_groups = file_groups;
        self
    }

    /// Add a new file group
    ///
    /// See [`Self::with_file_groups`] for more information
    pub fn with_file_group(mut self, file_group: FileGroup) -> Self {
        self.file_groups.push(file_group);
        self
    }

    /// Add a file as a single group
    ///
    /// See [`Self::with_file_groups`] for more information.
    pub fn with_file(self, partitioned_file: PartitionedFile) -> Self {
        self.with_file_group(FileGroup::new(vec![partitioned_file]))
    }

    /// Set the output ordering of the files
    pub fn with_output_ordering(mut self, output_ordering: Vec<LexOrdering>) -> Self {
        self.output_ordering = output_ordering;
        self
    }

    /// Set the file compression type
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = Some(file_compression_type);
        self
    }

    /// Set whether new lines in values are supported for CSVOptions
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    pub fn with_newlines_in_values(mut self, new_lines_in_values: bool) -> Self {
        self.new_lines_in_values = Some(new_lines_in_values);
        self
    }

    /// Set the batch_size property
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Register an expression adapter used to adapt filters and projections that are pushed down into the scan
    /// from the logical schema to the physical schema of the file.
    /// This can include things like:
    /// - Column ordering changes
    /// - Handling of missing columns
    /// - Rewriting expression to use pre-computed values or file format specific optimizations
    pub fn with_expr_adapter(
        mut self,
        expr_adapter: Option<Arc<dyn PhysicalExprAdapterFactory>>,
    ) -> Self {
        self.expr_adapter_factory = expr_adapter;
        self
    }

    /// Build the final [`FileScanConfig`] with all the configured settings.
    ///
    /// This method takes ownership of the builder and returns the constructed `FileScanConfig`.
    /// Any unset optional fields will use their default values.
    ///
    /// # Errors
    /// Returns an error if projection pushdown fails or if schema operations fail.
    pub fn build(self) -> FileScanConfig {
        let Self {
            object_store_url,
            file_source,
            limit,
            constraints,
            file_groups,
            statistics,
            output_ordering,
            file_compression_type,
            new_lines_in_values,
            batch_size,
            expr_adapter_factory: expr_adapter,
        } = self;

        let constraints = constraints.unwrap_or_default();
        let statistics = statistics.unwrap_or_else(|| {
            Statistics::new_unknown(file_source.table_schema().table_schema())
        });
        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);
        let new_lines_in_values = new_lines_in_values.unwrap_or(false);

        FileScanConfig {
            object_store_url,
            file_source,
            limit,
            constraints,
            file_groups,
            output_ordering,
            file_compression_type,
            new_lines_in_values,
            batch_size,
            expr_adapter_factory: expr_adapter,
            statistics,
        }
    }
}

impl From<FileScanConfig> for FileScanConfigBuilder {
    fn from(config: FileScanConfig) -> Self {
        Self {
            object_store_url: config.object_store_url,
            file_source: Arc::<dyn FileSource>::clone(&config.file_source),
            file_groups: config.file_groups,
            statistics: Some(config.statistics),
            output_ordering: config.output_ordering,
            file_compression_type: Some(config.file_compression_type),
            new_lines_in_values: Some(config.new_lines_in_values),
            limit: config.limit,
            constraints: Some(config.constraints),
            batch_size: config.batch_size,
            expr_adapter_factory: config.expr_adapter_factory,
        }
    }
}

impl DataSource for FileScanConfig {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store = context.runtime_env().object_store(&self.object_store_url)?;
        let batch_size = self
            .batch_size
            .unwrap_or_else(|| context.session_config().batch_size());

        let source = self.file_source.with_batch_size(batch_size);

        let opener = source.create_file_opener(object_store, self, partition)?;

        let stream = FileStream::new(self, partition, opener, source.metrics())?;
        Ok(Box::pin(cooperative(stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let schema = self.projected_schema().map_err(|_| std::fmt::Error {})?;
                let orderings = get_projected_output_ordering(self, &schema);

                write!(f, "file_groups=")?;
                FileGroupsDisplay(&self.file_groups).fmt_as(t, f)?;

                if !schema.fields().is_empty() {
                    if let Some(projection) = self.file_source.projection() {
                        // This matches what ProjectionExec does.
                        // TODO: can we put this into ProjectionExprs so that it's shared code?
                        let expr: Vec<String> = projection
                            .as_ref()
                            .iter()
                            .map(|proj_expr| {
                                if let Some(column) = proj_expr.expr.as_any().downcast_ref::<datafusion_physical_expr::expressions::Column>() {
                                    if column.name() == proj_expr.alias {
                                        column.name().to_string()
                                    } else {
                                        format!("{} as {}", proj_expr.expr, proj_expr.alias)
                                    }
                                } else {
                                    format!("{} as {}", proj_expr.expr, proj_expr.alias)
                                }
                            })
                            .collect();
                        write!(f, ", projection=[{}]", expr.join(", "))?;
                    } else {
                        write!(f, ", projection={}", ProjectSchemaDisplay(&schema))?;
                    }
                }

                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }

                display_orderings(f, &orderings)?;

                if !self.constraints.is_empty() {
                    write!(f, ", {}", self.constraints)?;
                }

                self.fmt_file_source(t, f)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format={}", self.file_source.file_type())?;
                self.file_source.fmt_extra(t, f)?;
                let num_files = self.file_groups.iter().map(|fg| fg.len()).sum::<usize>();
                writeln!(f, "files={num_files}")?;
                Ok(())
            }
        }
    }

    /// If supported by the underlying [`FileSource`], redistribute files across partitions according to their size.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        let source = self.file_source.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            self,
        )?;

        Ok(source.map(|s| Arc::new(s) as _))
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.file_groups.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        let schema = self.file_source.table_schema().table_schema();
        let mut eq_properties = EquivalenceProperties::new_with_orderings(
            Arc::clone(schema),
            self.output_ordering.clone(),
        )
        .with_constraints(self.constraints.clone());

        if let Some(filter) = self.file_source.filter() {
            // We need to remap column indexes to match the projected schema since that's what the equivalence properties deal with.
            // Note that this will *ignore* any non-projected columns: these don't factor into ordering / equivalence.
            match Self::add_filter_equivalence_info(&filter, &mut eq_properties, schema) {
                Ok(()) => {}
                Err(e) => {
                    warn!("Failed to add filter equivalence info: {e}");
                    #[cfg(debug_assertions)]
                    panic!("Failed to add filter equivalence info: {e}");
                }
            }
        }

        if let Some(projection) = self.file_source.projection() {
            match (
                projection.project_schema(schema),
                projection.projection_mapping(schema),
            ) {
                (Ok(output_schema), Ok(mapping)) => {
                    eq_properties =
                        eq_properties.project(&mapping, Arc::new(output_schema));
                }
                (Err(e), _) | (_, Err(e)) => {
                    warn!("Failed to project equivalence properties: {e}");
                    #[cfg(debug_assertions)]
                    panic!("Failed to project equivalence properties: {e}");
                }
            }
        }

        eq_properties
    }

    fn scheduling_type(&self) -> SchedulingType {
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(partition) = partition {
            // Get statistics for a specific partition
            if let Some(file_group) = self.file_groups.get(partition) {
                if let Some(stat) = file_group.file_statistics(None) {
                    // Project the statistics based on the projection
                    let table_cols_stats = self
                        .projection_indices()
                        .into_iter()
                        .map(|idx| {
                            if idx < self.file_schema().fields().len() {
                                stat.column_statistics[idx].clone()
                            } else {
                                // TODO provide accurate stat for partition column
                                // See https://github.com/apache/datafusion/issues/1186
                                ColumnStatistics::new_unknown()
                            }
                        })
                        .collect();

                    return Ok(Statistics {
                        num_rows: stat.num_rows,
                        total_byte_size: stat.total_byte_size,
                        column_statistics: table_cols_stats,
                    });
                }
            }
            // If no statistics available for this partition, return unknown
            Ok(Statistics::new_unknown(self.projected_schema()?.as_ref()))
        } else {
            // Return aggregate statistics across all partitions
            Ok(self.projected_stats())
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let source = FileScanConfigBuilder::from(self.clone())
            .with_limit(limit)
            .build();
        Some(Arc::new(source))
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.file_source.metrics().clone()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        match self.file_source.try_pushdown_projection(projection)? {
            Some(new_source) => {
                let mut new_file_scan_config = self.clone();
                new_file_scan_config.file_source = new_source;
                Ok(Some(Arc::new(new_file_scan_config) as Arc<dyn DataSource>))
            }
            None => Ok(None),
        }
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // Remap filter Column indices to match the table schema (file + partition columns).
        // This is necessary because filters may have been created against a different schema
        // (e.g., after projection pushdown) and need to be remapped to the table schema
        // before being passed to the file source and ultimately serialized.
        // For example, the filter being pushed down is `c1_c2 > 5` and it was created
        // against the output schema of the this `DataSource` which has projection `c1 + c2 as c1_c2`.
        // Thus we need to rewrite the filter back to `c1 + c2 > 5` before passing it to the file source.
        let table_schema = self.file_source.table_schema().table_schema();
        // If there's a projection with aliases, first map the filters back through
        // the projection expressions before remapping to the table schema.
        let filters_to_remap = if let Some(projection) = self.file_source.projection() {
            use datafusion_physical_plan::projection::update_expr;
            filters
                .into_iter()
                .map(|filter| {
                    update_expr(&filter, projection.as_ref(), true)?.ok_or_else(|| {
                        internal_datafusion_err!(
                            "Failed to map filter expression through projection: {}",
                            filter
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            filters
        };
        // Now remap column indices to match the table schema.
        let remapped_filters: Result<Vec<_>> = filters_to_remap
            .into_iter()
            .map(|filter| reassign_expr_columns(filter, table_schema.as_ref()))
            .collect();
        let remapped_filters = remapped_filters?;

        let result = self
            .file_source
            .try_pushdown_filters(remapped_filters, config)?;
        match result.updated_node {
            Some(new_file_source) => {
                let mut new_file_scan_config = self.clone();
                new_file_scan_config.file_source = new_file_source;
                Ok(FilterPushdownPropagation {
                    filters: result.filters,
                    updated_node: Some(Arc::new(new_file_scan_config) as _),
                })
            }
            None => {
                // If the file source does not support filter pushdown, return the original config
                Ok(FilterPushdownPropagation {
                    filters: result.filters,
                    updated_node: None,
                })
            }
        }
    }
}

impl FileScanConfig {
    /// Get the file schema (schema of the files without partition columns)
    pub fn file_schema(&self) -> &SchemaRef {
        self.file_source.table_schema().file_schema()
    }

    /// Get the table partition columns
    pub fn table_partition_cols(&self) -> &Vec<FieldRef> {
        self.file_source.table_schema().table_partition_cols()
    }

    fn projection_indices(&self) -> Vec<usize> {
        match self.file_source.projection() {
            Some(proj) => proj.ordered_column_indices(),
            None => (0..self.file_schema().fields().len()
                + self.table_partition_cols().len())
                .collect(),
        }
    }

    /// Returns the unprojected table statistics, marking them as inexact if filters are present.
    ///
    /// When filters are pushed down (including pruning predicates and bloom filters),
    /// we can't guarantee the statistics are exact because we don't know how many
    /// rows will be filtered out.
    pub fn statistics(&self) -> Statistics {
        if self.file_source.filter().is_some() {
            self.statistics.clone().to_inexact()
        } else {
            self.statistics.clone()
        }
    }

    fn projected_stats(&self) -> Statistics {
        let statistics = self.statistics();

        let table_cols_stats = self
            .projection_indices()
            .into_iter()
            .map(|idx| {
                if idx < self.file_schema().fields().len() {
                    statistics.column_statistics[idx].clone()
                } else {
                    // TODO provide accurate stat for partition column (#1186)
                    ColumnStatistics::new_unknown()
                }
            })
            .collect();

        Statistics {
            num_rows: statistics.num_rows,
            // TODO correct byte size: https://github.com/apache/datafusion/issues/14936
            total_byte_size: statistics.total_byte_size,
            column_statistics: table_cols_stats,
        }
    }

    pub fn projected_schema(&self) -> Result<Arc<Schema>> {
        let schema = self.file_source.table_schema().table_schema();
        match self.file_source.projection() {
            Some(proj) => Ok(Arc::new(proj.project_schema(schema)?)),
            None => Ok(Arc::clone(schema)),
        }
    }

    fn add_filter_equivalence_info(
        filter: &Arc<dyn PhysicalExpr>,
        eq_properties: &mut EquivalenceProperties,
        schema: &Schema,
    ) -> Result<()> {
        // Gather valid equality pairs from the filter expression
        let equal_pairs = split_conjunction(filter).into_iter().filter_map(|expr| {
            // Ignore any binary expressions that reference non-existent columns in the current schema
            // (e.g. due to unnecessary projections being removed)
            reassign_expr_columns(Arc::clone(expr), schema)
                .ok()
                .and_then(|expr| match expr.as_any().downcast_ref::<BinaryExpr>() {
                    Some(expr) if expr.op() == &Operator::Eq => {
                        Some((Arc::clone(expr.left()), Arc::clone(expr.right())))
                    }
                    _ => None,
                })
        });

        for (lhs, rhs) in equal_pairs {
            eq_properties.add_equal_conditions(lhs, rhs)?
        }

        Ok(())
    }

    pub fn projected_constraints(&self) -> Constraints {
        let indexes = self.projection_indices();
        self.constraints.project(&indexes).unwrap_or_default()
    }

    /// Specifies whether newlines in (quoted) values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    ///
    /// The default behaviour depends on the `datafusion.catalog.newlines_in_values` setting.
    pub fn newlines_in_values(&self) -> bool {
        self.new_lines_in_values
    }

    pub fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.file_source.projection().as_ref().map(|p| {
            p.ordered_column_indices()
                .into_iter()
                .filter(|&i| i < self.file_schema().fields().len())
                .collect::<Vec<_>>()
        })
    }

    /// Splits file groups into new groups based on statistics to enable efficient parallel processing.
    ///
    /// The method distributes files across a target number of partitions while ensuring
    /// files within each partition maintain sort order based on their min/max statistics.
    ///
    /// The algorithm works by:
    /// 1. Takes files sorted by minimum values
    /// 2. For each file:
    ///   - Finds eligible groups (empty or where file's min > group's last max)
    ///   - Selects the smallest eligible group
    ///   - Creates a new group if needed
    ///
    /// # Parameters
    /// * `table_schema`: Schema containing information about the columns
    /// * `file_groups`: The original file groups to split
    /// * `sort_order`: The lexicographical ordering to maintain within each group
    /// * `target_partitions`: The desired number of output partitions
    ///
    /// # Returns
    /// A new set of file groups, where files within each group are non-overlapping with respect to
    /// their min/max statistics and maintain the specified sort order.
    pub fn split_groups_by_statistics_with_target_partitions(
        table_schema: &SchemaRef,
        file_groups: &[FileGroup],
        sort_order: &LexOrdering,
        target_partitions: usize,
    ) -> Result<Vec<FileGroup>> {
        if target_partitions == 0 {
            return Err(internal_datafusion_err!(
                "target_partitions must be greater than 0"
            ));
        }

        let flattened_files = file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .collect::<Vec<_>>();

        if flattened_files.is_empty() {
            return Ok(vec![]);
        }

        let statistics = MinMaxStatistics::new_from_files(
            sort_order,
            table_schema,
            None,
            flattened_files.iter().copied(),
        )?;

        let indices_sorted_by_min = statistics.min_values_sorted();

        // Initialize with target_partitions empty groups
        let mut file_groups_indices: Vec<Vec<usize>> = vec![vec![]; target_partitions];

        for (idx, min) in indices_sorted_by_min {
            if let Some((_, group)) = file_groups_indices
                .iter_mut()
                .enumerate()
                .filter(|(_, group)| {
                    group.is_empty()
                        || min
                            > statistics
                                .max(*group.last().expect("groups should not be empty"))
                })
                .min_by_key(|(_, group)| group.len())
            {
                group.push(idx);
            } else {
                // Create a new group if no existing group fits
                file_groups_indices.push(vec![idx]);
            }
        }

        // Remove any empty groups
        file_groups_indices.retain(|group| !group.is_empty());

        // Assemble indices back into groups of PartitionedFiles
        Ok(file_groups_indices
            .into_iter()
            .map(|file_group_indices| {
                FileGroup::new(
                    file_group_indices
                        .into_iter()
                        .map(|idx| flattened_files[idx].clone())
                        .collect(),
                )
            })
            .collect())
    }

    /// Attempts to do a bin-packing on files into file groups, such that any two files
    /// in a file group are ordered and non-overlapping with respect to their statistics.
    /// It will produce the smallest number of file groups possible.
    pub fn split_groups_by_statistics(
        table_schema: &SchemaRef,
        file_groups: &[FileGroup],
        sort_order: &LexOrdering,
    ) -> Result<Vec<FileGroup>> {
        let flattened_files = file_groups
            .iter()
            .flat_map(FileGroup::iter)
            .collect::<Vec<_>>();
        // First Fit:
        // * Choose the first file group that a file can be placed into.
        // * If it fits into no existing file groups, create a new one.
        //
        // By sorting files by min values and then applying first-fit bin packing,
        // we can produce the smallest number of file groups such that
        // files within a group are in order and non-overlapping.
        //
        // Source: Applied Combinatorics (Keller and Trotter), Chapter 6.8
        // https://www.appliedcombinatorics.org/book/s_posets_dilworth-intord.html

        if flattened_files.is_empty() {
            return Ok(vec![]);
        }

        let statistics = MinMaxStatistics::new_from_files(
            sort_order,
            table_schema,
            None,
            flattened_files.iter().copied(),
        )
        .map_err(|e| {
            e.context("construct min/max statistics for split_groups_by_statistics")
        })?;

        let indices_sorted_by_min = statistics.min_values_sorted();
        let mut file_groups_indices: Vec<Vec<usize>> = vec![];

        for (idx, min) in indices_sorted_by_min {
            let file_group_to_insert = file_groups_indices.iter_mut().find(|group| {
                // If our file is non-overlapping and comes _after_ the last file,
                // it fits in this file group.
                min > statistics.max(
                    *group
                        .last()
                        .expect("groups should be nonempty at construction"),
                )
            });
            match file_group_to_insert {
                Some(group) => group.push(idx),
                None => file_groups_indices.push(vec![idx]),
            }
        }

        // Assemble indices back into groups of PartitionedFiles
        Ok(file_groups_indices
            .into_iter()
            .map(|file_group_indices| {
                file_group_indices
                    .into_iter()
                    .map(|idx| flattened_files[idx].clone())
                    .collect()
            })
            .collect())
    }

    /// Write the data_type based on file_source
    fn fmt_file_source(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        write!(f, ", file_type={}", self.file_source.file_type())?;
        self.file_source.fmt_extra(t, f)
    }

    /// Returns the file_source
    pub fn file_source(&self) -> &Arc<dyn FileSource> {
        &self.file_source
    }
}

impl Debug for FileScanConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "FileScanConfig {{")?;
        write!(f, "object_store_url={:?}, ", self.object_store_url)?;

        write!(f, "statistics={:?}, ", self.statistics())?;

        DisplayAs::fmt_as(self, DisplayFormatType::Verbose, f)?;
        write!(f, "}}")
    }
}

impl DisplayAs for FileScanConfig {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        let schema = self.projected_schema().map_err(|_| std::fmt::Error {})?;
        let orderings = get_projected_output_ordering(self, &schema);

        write!(f, "file_groups=")?;
        FileGroupsDisplay(&self.file_groups).fmt_as(t, f)?;

        if !schema.fields().is_empty() {
            write!(f, ", projection={}", ProjectSchemaDisplay(&schema))?;
        }

        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }

        display_orderings(f, &orderings)?;

        if !self.constraints.is_empty() {
            write!(f, ", {}", self.constraints)?;
        }

        Ok(())
    }
}

/// The various listing tables does not attempt to read all files
/// concurrently, instead they will read files in sequence within a
/// partition.  This is an important property as it allows plans to
/// run against 1000s of files and not try to open them all
/// concurrently.
///
/// However, it means if we assign more than one file to a partition
/// the output sort order will not be preserved as illustrated in the
/// following diagrams:
///
/// When only 1 file is assigned to each partition, each partition is
/// correctly sorted on `(A, B, C)`
///
/// ```text
/// ┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┓
///   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// ┃   ┌───────────────┐     ┌──────────────┐ │   ┌──────────────┐ │   ┌─────────────┐   ┃
///   │ │   1.parquet   │ │ │ │  2.parquet   │   │ │  3.parquet   │   │ │  4.parquet  │ │
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │   │Sort: A, B, C │ │   │Sort: A, B, C│   ┃
///   │ └───────────────┘ │ │ └──────────────┘   │ └──────────────┘   │ └─────────────┘ │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃                                          │                    │                     ┃
///   │                   │ │                    │                    │                 │
/// ┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///      DataFusion           DataFusion           DataFusion           DataFusion
/// ┃    Partition 1          Partition 2          Partition 3          Partition 4       ┃
///  ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///
///                                      DataSourceExec
/// ```
///
/// However, when more than 1 file is assigned to each partition, each
/// partition is NOT correctly sorted on `(A, B, C)`. Once the second
/// file is scanned, the same values for A, B and C can be repeated in
/// the same sorted stream
///
///```text
/// ┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
/// ┃   ┌───────────────┐     ┌──────────────┐ │
///   │ │   1.parquet   │ │ │ │  2.parquet   │   ┃
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///   │ └───────────────┘ │ │ └──────────────┘   ┃
/// ┃   ┌───────────────┐     ┌──────────────┐ │
///   │ │   3.parquet   │ │ │ │  4.parquet   │   ┃
/// ┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///   │ └───────────────┘ │ │ └──────────────┘   ┃
/// ┃                                          │
///   │                   │ │                    ┃
/// ┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///      DataFusion           DataFusion         ┃
/// ┃    Partition 1          Partition 2
///  ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┛
///
///              DataSourceExec
/// ```
fn get_projected_output_ordering(
    base_config: &FileScanConfig,
    projected_schema: &SchemaRef,
) -> Vec<LexOrdering> {
    let projected_orderings =
        project_orderings(&base_config.output_ordering, projected_schema);

    let mut all_orderings = vec![];
    for new_ordering in projected_orderings {
        // Check if any file groups are not sorted
        if base_config.file_groups.iter().any(|group| {
            if group.len() <= 1 {
                // File groups with <= 1 files are always sorted
                return false;
            }

            let indices = base_config
                .file_source
                .projection()
                .as_ref()
                .map(|p| p.ordered_column_indices());

            let statistics = match MinMaxStatistics::new_from_files(
                &new_ordering,
                projected_schema,
                indices.as_deref(),
                group.iter(),
            ) {
                Ok(statistics) => statistics,
                Err(e) => {
                    log::trace!("Error fetching statistics for file group: {e}");
                    // we can't prove that it's ordered, so we have to reject it
                    return true;
                }
            };

            !statistics.is_sorted()
        }) {
            debug!(
                "Skipping specified output ordering {:?}. \
                Some file groups couldn't be determined to be sorted: {:?}",
                base_config.output_ordering[0], base_config.file_groups
            );
            continue;
        }

        all_orderings.push(new_ordering);
    }
    all_orderings
}

/// Convert type to a type suitable for use as a `ListingTable`
/// partition column. Returns `Dictionary(UInt16, val_type)`, which is
/// a reasonable trade off between a reasonable number of partition
/// values and space efficiency.
///
/// This use this to specify types for partition columns. However
/// you MAY also choose not to dictionary-encode the data or to use a
/// different dictionary type.
///
/// Use [`wrap_partition_value_in_dict`] to wrap a [`ScalarValue`] in the same say.
pub fn wrap_partition_type_in_dict(val_type: DataType) -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(val_type))
}

/// Convert a [`ScalarValue`] of partition columns to a type, as
/// described in the documentation of [`wrap_partition_type_in_dict`],
/// which can wrap the types.
pub fn wrap_partition_value_in_dict(val: ScalarValue) -> ScalarValue {
    ScalarValue::Dictionary(Box::new(DataType::UInt16), Box::new(val))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::test_util::col;
    use crate::TableSchema;
    use crate::{
        generate_test_files, test_util::MockSource, tests::aggr_test_schema,
        verify_sort_integrity,
    };

    use arrow::datatypes::Field;
    use datafusion_common::internal_err;
    use datafusion_common::stats::Precision;
    use datafusion_expr::{Operator, SortExpr};
    use datafusion_physical_expr::create_physical_sort_expr;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion_physical_expr::projection::ProjectionExpr;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    #[test]
    fn physical_plan_config_no_projection_tab_cols_as_field() {
        let file_schema = aggr_test_schema();

        // make a table_partition_col as a field
        let table_partition_col =
            Field::new("date", wrap_partition_type_in_dict(DataType::Utf8), true)
                .with_metadata(HashMap::from_iter(vec![(
                    "key_whatever".to_owned(),
                    "value_whatever".to_owned(),
                )]));

        let conf = config_for_projection(
            Arc::clone(&file_schema),
            None,
            Statistics::new_unknown(&file_schema),
            vec![table_partition_col.clone()],
        );

        // verify the proj_schema includes the last column and exactly the same the field it is defined
        let proj_schema = conf.projected_schema().unwrap();
        assert_eq!(proj_schema.fields().len(), file_schema.fields().len() + 1);
        assert_eq!(
            *proj_schema.field(file_schema.fields().len()),
            table_partition_col,
            "partition columns are the last columns and ust have all values defined in created field"
        );
    }

    #[test]
    fn test_projected_file_schema_with_partition_col() {
        let schema = aggr_test_schema();
        let partition_cols = vec![
            (
                "part1".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "part2".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
        ];

        // Projected file schema for config with projection including partition column
        let config = config_for_projection(
            schema.clone(),
            Some(vec![0, 3, 5, schema.fields().len()]),
            Statistics::new_unknown(&schema),
            to_partition_cols(partition_cols),
        );
        let projection = projected_file_schema(&config);

        // Assert partition column filtered out in projected file schema
        let expected_columns = vec!["c1", "c4", "c6"];
        let actual_columns = projection
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(expected_columns, actual_columns);
    }

    /// Projects only file schema, ignoring partition columns
    fn projected_file_schema(config: &FileScanConfig) -> SchemaRef {
        let file_schema = config.file_source.table_schema().file_schema();
        if let Some(file_indices) = config.file_column_projection_indices() {
            Arc::new(file_schema.project(&file_indices).unwrap())
        } else {
            Arc::clone(file_schema)
        }
    }

    #[test]
    fn test_projected_file_schema_without_projection() {
        let schema = aggr_test_schema();
        let partition_cols = vec![
            (
                "part1".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "part2".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
        ];

        // Projected file schema for config without projection
        let config = config_for_projection(
            schema.clone(),
            None,
            Statistics::new_unknown(&schema),
            to_partition_cols(partition_cols),
        );
        let projection = projected_file_schema(&config);

        // Assert projected file schema is equal to file schema
        assert_eq!(projection.fields(), schema.fields());
    }

    #[test]
    fn test_split_groups_by_statistics() -> Result<()> {
        use chrono::TimeZone;
        use datafusion_common::DFSchema;
        use datafusion_expr::execution_props::ExecutionProps;
        use object_store::{path::Path, ObjectMeta};

        struct File {
            name: &'static str,
            date: &'static str,
            statistics: Vec<Option<(Option<f64>, Option<f64>)>>,
        }
        impl File {
            fn new(
                name: &'static str,
                date: &'static str,
                statistics: Vec<Option<(f64, f64)>>,
            ) -> Self {
                Self::new_nullable(
                    name,
                    date,
                    statistics
                        .into_iter()
                        .map(|opt| opt.map(|(min, max)| (Some(min), Some(max))))
                        .collect(),
                )
            }

            fn new_nullable(
                name: &'static str,
                date: &'static str,
                statistics: Vec<Option<(Option<f64>, Option<f64>)>>,
            ) -> Self {
                Self {
                    name,
                    date,
                    statistics,
                }
            }
        }

        struct TestCase {
            name: &'static str,
            file_schema: Schema,
            files: Vec<File>,
            sort: Vec<SortExpr>,
            expected_result: Result<Vec<Vec<&'static str>>, &'static str>,
        }

        use datafusion_expr::col;
        let cases = vec![
            TestCase {
                name: "test sort",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.50, 1.00))]),
                    File::new("2", "2023-01-02", vec![Some((0.00, 1.00))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![vec!["0", "1"], vec!["2"]]),
            },
            // same input but file '2' is in the middle
            // test that we still order correctly
            TestCase {
                name: "test sort with files ordered differently",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("2", "2023-01-02", vec![Some((0.00, 1.00))]),
                    File::new("1", "2023-01-01", vec![Some((0.50, 1.00))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![vec!["0", "1"], vec!["2"]]),
            },
            TestCase {
                name: "reverse sort",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.50, 1.00))]),
                    File::new("2", "2023-01-02", vec![Some((0.00, 1.00))]),
                ],
                sort: vec![col("value").sort(false, true)],
                expected_result: Ok(vec![vec!["1", "0"], vec!["2"]]),
            },
            TestCase {
                name: "nullable sort columns, nulls last",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    true,
                )]),
                files: vec![
                    File::new_nullable("0", "2023-01-01", vec![Some((Some(0.00), Some(0.49)))]),
                    File::new_nullable("1", "2023-01-01", vec![Some((Some(0.50), None))]),
                    File::new_nullable("2", "2023-01-02", vec![Some((Some(0.00), None))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![vec!["0", "1"], vec!["2"]])
            },
            TestCase {
                name: "nullable sort columns, nulls first",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    true,
                )]),
                files: vec![
                    File::new_nullable("0", "2023-01-01", vec![Some((None, Some(0.49)))]),
                    File::new_nullable("1", "2023-01-01", vec![Some((Some(0.50), Some(1.00)))]),
                    File::new_nullable("2", "2023-01-02", vec![Some((None, Some(1.00)))]),
                ],
                sort: vec![col("value").sort(true, true)],
                expected_result: Ok(vec![vec!["0", "1"], vec!["2"]])
            },
            TestCase {
                name: "all three non-overlapping",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.50, 0.99))]),
                    File::new("2", "2023-01-02", vec![Some((1.00, 1.49))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![vec!["0", "1", "2"]]),
            },
            TestCase {
                name: "all three overlapping",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("2", "2023-01-02", vec![Some((0.00, 0.49))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![vec!["0"], vec!["1"], vec!["2"]]),
            },
            TestCase {
                name: "empty input",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![],
                sort: vec![col("value").sort(true, false)],
                expected_result: Ok(vec![]),
            },
            TestCase {
                name: "one file missing statistics",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    false,
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("2", "2023-01-02", vec![None]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Err("construct min/max statistics for split_groups_by_statistics\ncaused by\ncollect min/max values\ncaused by\nget min/max for column: 'value'\ncaused by\nError during planning: statistics not found"),
            },
        ];

        for case in cases {
            let table_schema = Arc::new(Schema::new(
                case.file_schema
                    .fields()
                    .clone()
                    .into_iter()
                    .cloned()
                    .chain(Some(Arc::new(Field::new(
                        "date".to_string(),
                        DataType::Utf8,
                        false,
                    ))))
                    .collect::<Vec<_>>(),
            ));
            let Some(sort_order) = LexOrdering::new(
                case.sort
                    .into_iter()
                    .map(|expr| {
                        create_physical_sort_expr(
                            &expr,
                            &DFSchema::try_from(Arc::clone(&table_schema))?,
                            &ExecutionProps::default(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            ) else {
                return internal_err!("This test should always use an ordering");
            };

            let partitioned_files = FileGroup::new(
                case.files.into_iter().map(From::from).collect::<Vec<_>>(),
            );
            let result = FileScanConfig::split_groups_by_statistics(
                &table_schema,
                std::slice::from_ref(&partitioned_files),
                &sort_order,
            );
            let results_by_name = result
                .as_ref()
                .map(|file_groups| {
                    file_groups
                        .iter()
                        .map(|file_group| {
                            file_group
                                .iter()
                                .map(|file| {
                                    partitioned_files
                                        .iter()
                                        .find_map(|f| {
                                            if f.object_meta == file.object_meta {
                                                Some(
                                                    f.object_meta
                                                        .location
                                                        .as_ref()
                                                        .rsplit('/')
                                                        .next()
                                                        .unwrap()
                                                        .trim_end_matches(".parquet"),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .unwrap()
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                })
                .map_err(|e| e.strip_backtrace().leak() as &'static str);

            assert_eq!(results_by_name, case.expected_result, "{}", case.name);
        }

        return Ok(());

        impl From<File> for PartitionedFile {
            fn from(file: File) -> Self {
                PartitionedFile {
                    object_meta: ObjectMeta {
                        location: Path::from(format!(
                            "data/date={}/{}.parquet",
                            file.date, file.name
                        )),
                        last_modified: chrono::Utc.timestamp_nanos(0),
                        size: 0,
                        e_tag: None,
                        version: None,
                    },
                    partition_values: vec![ScalarValue::from(file.date)],
                    range: None,
                    statistics: Some(Arc::new(Statistics {
                        num_rows: Precision::Absent,
                        total_byte_size: Precision::Absent,
                        column_statistics: file
                            .statistics
                            .into_iter()
                            .map(|stats| {
                                stats
                                    .map(|(min, max)| ColumnStatistics {
                                        min_value: Precision::Exact(
                                            ScalarValue::Float64(min),
                                        ),
                                        max_value: Precision::Exact(
                                            ScalarValue::Float64(max),
                                        ),
                                        ..Default::default()
                                    })
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>(),
                    })),
                    extensions: None,
                    metadata_size_hint: None,
                }
            }
        }
    }

    // sets default for configs that play no role in projections
    fn config_for_projection(
        file_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        statistics: Statistics,
        table_partition_cols: Vec<Field>,
    ) -> FileScanConfig {
        let table_schema = TableSchema::new(
            file_schema,
            table_partition_cols.into_iter().map(Arc::new).collect(),
        );
        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            Arc::new(MockSource::new(table_schema.clone())),
        )
        .with_projection_indices(projection)
        .unwrap()
        .with_statistics(statistics)
        .build()
    }

    /// Convert partition columns from Vec<String DataType> to Vec<Field>
    fn to_partition_cols(table_partition_cols: Vec<(String, DataType)>) -> Vec<Field> {
        table_partition_cols
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), false))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_file_scan_config_builder() {
        let file_schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();

        let table_schema = TableSchema::new(
            Arc::clone(&file_schema),
            vec![Arc::new(Field::new(
                "date",
                wrap_partition_type_in_dict(DataType::Utf8),
                false,
            ))],
        );

        let file_source: Arc<dyn FileSource> =
            Arc::new(MockSource::new(table_schema.clone()));

        // Create a builder with required parameters
        let builder = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_source),
        );

        // Build with various configurations
        let config = builder
            .with_limit(Some(1000))
            .with_projection_indices(Some(vec![0, 1]))
            .unwrap()
            .with_statistics(Statistics::new_unknown(&file_schema))
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "test.parquet".to_string(),
                1024,
            )])])
            .with_output_ordering(vec![[PhysicalSortExpr::new_default(Arc::new(
                Column::new("date", 0),
            ))]
            .into()])
            .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
            .with_newlines_in_values(true)
            .build();

        // Verify the built config has all the expected values
        assert_eq!(config.object_store_url, object_store_url);
        assert_eq!(*config.file_schema(), file_schema);
        assert_eq!(config.limit, Some(1000));
        assert_eq!(
            config
                .file_source
                .projection()
                .as_ref()
                .map(|p| p.column_indices()),
            Some(vec![0, 1])
        );
        assert_eq!(config.table_partition_cols().len(), 1);
        assert_eq!(config.table_partition_cols()[0].name(), "date");
        assert_eq!(config.file_groups.len(), 1);
        assert_eq!(config.file_groups[0].len(), 1);
        assert_eq!(
            config.file_groups[0][0].object_meta.location.as_ref(),
            "test.parquet"
        );
        assert_eq!(
            config.file_compression_type,
            FileCompressionType::UNCOMPRESSED
        );
        assert!(config.new_lines_in_values);
        assert_eq!(config.output_ordering.len(), 1);
    }

    #[test]
    fn equivalence_properties_after_schema_change() {
        let file_schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();

        let table_schema = TableSchema::new(Arc::clone(&file_schema), vec![]);

        // Create a file source with a filter
        let file_source: Arc<dyn FileSource> = Arc::new(
            MockSource::new(table_schema.clone()).with_filter(Arc::new(BinaryExpr::new(
                col("c2", &file_schema).unwrap(),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Int32(Some(10)))),
            ))),
        );

        let config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_source),
        )
        .with_projection_indices(Some(vec![0, 1, 2]))
        .unwrap()
        .build();

        // Simulate projection being updated. Since the filter has already been pushed down,
        // the new projection won't include the filtered column.
        let exprs = ProjectionExprs::new(vec![ProjectionExpr::new(
            col("c1", &file_schema).unwrap(),
            "c1".to_string(),
        )]);
        let data_source = config
            .try_swapping_with_projection(&exprs)
            .unwrap()
            .unwrap();

        // Gather the equivalence properties from the new data source. There should
        // be no equivalence class for column c2 since it was removed by the projection.
        let eq_properties = data_source.eq_properties();
        let eq_group = eq_properties.eq_group();

        for class in eq_group.iter() {
            for expr in class.iter() {
                if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                    assert_ne!(
                        col.name(),
                        "c2",
                        "c2 should not be present in any equivalence class"
                    );
                }
            }
        }
    }

    #[test]
    fn test_file_scan_config_builder_defaults() {
        let file_schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();

        let table_schema = TableSchema::new(Arc::clone(&file_schema), vec![]);

        let file_source: Arc<dyn FileSource> =
            Arc::new(MockSource::new(table_schema.clone()));

        // Create a builder with only required parameters and build without any additional configurations
        let config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_source),
        )
        .build();

        // Verify default values
        assert_eq!(config.object_store_url, object_store_url);
        assert_eq!(*config.file_schema(), file_schema);
        assert_eq!(config.limit, None);
        // When no projection is specified, the file source should have an unprojected projection
        // (i.e., all columns)
        let expected_projection: Vec<usize> = (0..file_schema.fields().len()).collect();
        assert_eq!(
            config
                .file_source
                .projection()
                .as_ref()
                .map(|p| p.column_indices()),
            Some(expected_projection)
        );
        assert!(config.table_partition_cols().is_empty());
        assert!(config.file_groups.is_empty());
        assert_eq!(
            config.file_compression_type,
            FileCompressionType::UNCOMPRESSED
        );
        assert!(!config.new_lines_in_values);
        assert!(config.output_ordering.is_empty());
        assert!(config.constraints.is_empty());

        // Verify statistics are set to unknown
        assert_eq!(config.statistics().num_rows, Precision::Absent);
        assert_eq!(config.statistics().total_byte_size, Precision::Absent);
        assert_eq!(
            config.statistics().column_statistics.len(),
            file_schema.fields().len()
        );
        for stat in config.statistics().column_statistics {
            assert_eq!(stat.distinct_count, Precision::Absent);
            assert_eq!(stat.min_value, Precision::Absent);
            assert_eq!(stat.max_value, Precision::Absent);
            assert_eq!(stat.null_count, Precision::Absent);
        }
    }

    #[test]
    fn test_file_scan_config_builder_new_from() {
        let schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
        let partition_cols = vec![Field::new(
            "date",
            wrap_partition_type_in_dict(DataType::Utf8),
            false,
        )];
        let file = PartitionedFile::new("test_file.parquet", 100);

        let table_schema = TableSchema::new(
            Arc::clone(&schema),
            partition_cols.iter().map(|f| Arc::new(f.clone())).collect(),
        );

        let file_source: Arc<dyn FileSource> =
            Arc::new(MockSource::new(table_schema.clone()));

        // Create a config with non-default values
        let original_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_source),
        )
        .with_projection_indices(Some(vec![0, 2]))
        .unwrap()
        .with_limit(Some(10))
        .with_file(file.clone())
        .with_constraints(Constraints::default())
        .with_newlines_in_values(true)
        .build();

        // Create a new builder from the config
        let new_builder = FileScanConfigBuilder::from(original_config);

        // Build a new config from this builder
        let new_config = new_builder.build();

        // Verify properties match
        let partition_cols = partition_cols.into_iter().map(Arc::new).collect::<Vec<_>>();
        assert_eq!(new_config.object_store_url, object_store_url);
        assert_eq!(*new_config.file_schema(), schema);
        assert_eq!(
            new_config
                .file_source
                .projection()
                .as_ref()
                .map(|p| p.column_indices()),
            Some(vec![0, 2])
        );
        assert_eq!(new_config.limit, Some(10));
        assert_eq!(*new_config.table_partition_cols(), partition_cols);
        assert_eq!(new_config.file_groups.len(), 1);
        assert_eq!(new_config.file_groups[0].len(), 1);
        assert_eq!(
            new_config.file_groups[0][0].object_meta.location.as_ref(),
            "test_file.parquet"
        );
        assert_eq!(new_config.constraints, Constraints::default());
        assert!(new_config.new_lines_in_values);
    }

    #[test]
    fn test_split_groups_by_statistics_with_target_partitions() -> Result<()> {
        use datafusion_common::DFSchema;
        use datafusion_expr::{col, execution_props::ExecutionProps};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));

        // Setup sort expression
        let exec_props = ExecutionProps::new();
        let df_schema = DFSchema::try_from_qualified_schema("test", schema.as_ref())?;
        let sort_expr = [col("value").sort(true, false)];
        let sort_ordering = sort_expr
            .map(|expr| {
                create_physical_sort_expr(&expr, &df_schema, &exec_props).unwrap()
            })
            .into();

        // Test case parameters
        struct TestCase {
            name: String,
            file_count: usize,
            overlap_factor: f64,
            target_partitions: usize,
            expected_partition_count: usize,
        }

        let test_cases = vec![
            // Basic cases
            TestCase {
                name: "no_overlap_10_files_4_partitions".to_string(),
                file_count: 10,
                overlap_factor: 0.0,
                target_partitions: 4,
                expected_partition_count: 4,
            },
            TestCase {
                name: "medium_overlap_20_files_5_partitions".to_string(),
                file_count: 20,
                overlap_factor: 0.5,
                target_partitions: 5,
                expected_partition_count: 5,
            },
            TestCase {
                name: "high_overlap_30_files_3_partitions".to_string(),
                file_count: 30,
                overlap_factor: 0.8,
                target_partitions: 3,
                expected_partition_count: 7,
            },
            // Edge cases
            TestCase {
                name: "fewer_files_than_partitions".to_string(),
                file_count: 3,
                overlap_factor: 0.0,
                target_partitions: 10,
                expected_partition_count: 3, // Should only create as many partitions as files
            },
            TestCase {
                name: "single_file".to_string(),
                file_count: 1,
                overlap_factor: 0.0,
                target_partitions: 5,
                expected_partition_count: 1, // Should create only one partition
            },
            TestCase {
                name: "empty_files".to_string(),
                file_count: 0,
                overlap_factor: 0.0,
                target_partitions: 3,
                expected_partition_count: 0, // Empty result for empty input
            },
        ];

        for case in test_cases {
            println!("Running test case: {}", case.name);

            // Generate files using bench utility function
            let file_groups = generate_test_files(case.file_count, case.overlap_factor);

            // Call the function under test
            let result =
                FileScanConfig::split_groups_by_statistics_with_target_partitions(
                    &schema,
                    &file_groups,
                    &sort_ordering,
                    case.target_partitions,
                )?;

            // Verify results
            println!(
                "Created {} partitions (target was {})",
                result.len(),
                case.target_partitions
            );

            // Check partition count
            assert_eq!(
                result.len(),
                case.expected_partition_count,
                "Case '{}': Unexpected partition count",
                case.name
            );

            // Verify sort integrity
            assert!(
                verify_sort_integrity(&result),
                "Case '{}': Files within partitions are not properly ordered",
                case.name
            );

            // Distribution check for partitions
            if case.file_count > 1 && case.expected_partition_count > 1 {
                let group_sizes: Vec<usize> = result.iter().map(FileGroup::len).collect();
                let max_size = *group_sizes.iter().max().unwrap();
                let min_size = *group_sizes.iter().min().unwrap();

                // Check partition balancing - difference shouldn't be extreme
                let avg_files_per_partition =
                    case.file_count as f64 / case.expected_partition_count as f64;
                assert!(
                    (max_size as f64) < 2.0 * avg_files_per_partition,
                    "Case '{}': Unbalanced distribution. Max partition size {} exceeds twice the average {}",
                    case.name,
                    max_size,
                    avg_files_per_partition
                );

                println!("Distribution - min files: {min_size}, max files: {max_size}");
            }
        }

        // Test error case: zero target partitions
        let empty_groups: Vec<FileGroup> = vec![];
        let err = FileScanConfig::split_groups_by_statistics_with_target_partitions(
            &schema,
            &empty_groups,
            &sort_ordering,
            0,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("target_partitions must be greater than 0"),
            "Expected error for zero target partitions"
        );

        Ok(())
    }

    #[test]
    fn test_partition_statistics_projection() {
        // This test verifies that partition_statistics applies projection correctly.
        // The old implementation had a bug where it returned file group statistics
        // without applying the projection, returning all column statistics instead
        // of just the projected ones.

        use crate::source::DataSourceExec;
        use datafusion_physical_plan::ExecutionPlan;

        // Create a schema with 4 columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("col0", DataType::Int32, false),
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
            Field::new("col3", DataType::Int32, false),
        ]));

        // Create statistics for all 4 columns
        let file_group_stats = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(1024),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics {
                    null_count: Precision::Exact(5),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics {
                    null_count: Precision::Exact(10),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics {
                    null_count: Precision::Exact(15),
                    ..ColumnStatistics::new_unknown()
                },
            ],
        };

        // Create a file group with statistics
        let file_group = FileGroup::new(vec![PartitionedFile::new("test.parquet", 1024)])
            .with_statistics(Arc::new(file_group_stats));

        let table_schema = TableSchema::new(Arc::clone(&schema), vec![]);

        // Create a FileScanConfig with projection: only keep columns 0 and 2
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            Arc::new(MockSource::new(table_schema.clone())),
        )
        .with_projection_indices(Some(vec![0, 2]))
        .unwrap() // Only project columns 0 and 2
        .with_file_groups(vec![file_group])
        .build();

        // Create a DataSourceExec from the config
        let exec = DataSourceExec::from_data_source(config);

        // Get statistics for partition 0
        let partition_stats = exec.partition_statistics(Some(0)).unwrap();

        // Verify that only 2 columns are in the statistics (the projected ones)
        assert_eq!(
            partition_stats.column_statistics.len(),
            2,
            "Expected 2 column statistics (projected), but got {}",
            partition_stats.column_statistics.len()
        );

        // Verify the column statistics are for columns 0 and 2
        assert_eq!(
            partition_stats.column_statistics[0].null_count,
            Precision::Exact(0),
            "First projected column should be col0 with 0 nulls"
        );
        assert_eq!(
            partition_stats.column_statistics[1].null_count,
            Precision::Exact(10),
            "Second projected column should be col2 with 10 nulls"
        );

        // Verify row count and byte size are preserved
        assert_eq!(partition_stats.num_rows, Precision::Exact(100));
        assert_eq!(partition_stats.total_byte_size, Precision::Exact(1024));
    }
}
