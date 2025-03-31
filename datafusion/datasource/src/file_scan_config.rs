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

use std::{
    any::Any, borrow::Cow, collections::HashMap, fmt::Debug, fmt::Formatter,
    fmt::Result as FmtResult, marker::PhantomData, sync::Arc,
};

use arrow::{
    array::{
        ArrayData, ArrayRef, BufferBuilder, DictionaryArray, RecordBatch,
        RecordBatchOptions,
    },
    buffer::Buffer,
    datatypes::{ArrowNativeType, DataType, Field, Schema, SchemaRef, UInt16Type},
};
use datafusion_common::{exec_err, ColumnStatistics, Constraints, Result, Statistics};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_execution::{
    object_store::ObjectStoreUrl, SendableRecordBatchStream, TaskContext,
};
use datafusion_physical_expr::{
    expressions::Column, EquivalenceProperties, LexOrdering, Partitioning,
    PhysicalSortExpr,
};
use datafusion_physical_plan::{
    display::{display_orderings, ProjectSchemaDisplay},
    metrics::ExecutionPlanMetricsSet,
    projection::{all_alias_free_columns, new_projections_for_columns, ProjectionExec},
    DisplayAs, DisplayFormatType, ExecutionPlan,
};
use log::{debug, warn};

use crate::file_groups::FileGroup;
use crate::{
    display::FileGroupsDisplay,
    file::FileSource,
    file_compression_type::FileCompressionType,
    file_stream::FileStream,
    source::{DataSource, DataSourceExec},
    statistics::MinMaxStatistics,
    PartitionedFile,
};

/// The base configurations for a [`DataSourceExec`], the a physical plan for
/// any given file format.
///
/// Use [`Self::build`] to create a [`DataSourceExec`] from a ``FileScanConfig`.
///
/// # Example
/// ```
/// # use std::any::Any;
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Field, Fields, DataType, Schema, SchemaRef};
/// # use object_store::ObjectStore;
/// # use datafusion_common::Statistics;
/// # use datafusion_datasource::file::FileSource;
/// # use datafusion_datasource::file_groups::FileGroup;
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource::file_stream::FileOpener;
/// # use datafusion_datasource::source::DataSourceExec;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_physical_plan::ExecutionPlan;
/// # use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
/// # let file_schema = Arc::new(Schema::new(vec![
/// #  Field::new("c1", DataType::Int32, false),
/// #  Field::new("c2", DataType::Int32, false),
/// #  Field::new("c3", DataType::Int32, false),
/// #  Field::new("c4", DataType::Int32, false),
/// # ]));
/// # // Note: crate mock ParquetSource, as ParquetSource is not in the datasource crate
/// # struct ParquetSource {
/// #    projected_statistics: Option<Statistics>
/// # };
/// # impl FileSource for ParquetSource {
/// #  fn create_file_opener(&self, _: Arc<dyn ObjectStore>, _: &FileScanConfig, _: usize) -> Arc<dyn FileOpener> { unimplemented!() }
/// #  fn as_any(&self) -> &dyn Any { self  }
/// #  fn with_batch_size(&self, _: usize) -> Arc<dyn FileSource> { unimplemented!() }
/// #  fn with_schema(&self, _: SchemaRef) -> Arc<dyn FileSource> { unimplemented!() }
/// #  fn with_projection(&self, _: &FileScanConfig) -> Arc<dyn FileSource> { unimplemented!() }
/// #  fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> { Arc::new(Self {projected_statistics: Some(statistics)} ) }
/// #  fn metrics(&self) -> &ExecutionPlanMetricsSet { unimplemented!() }
/// #  fn statistics(&self) -> datafusion_common::Result<Statistics> { Ok(self.projected_statistics.clone().expect("projected_statistics should be set")) }
/// #  fn file_type(&self) -> &str { "parquet" }
/// #  }
/// # impl ParquetSource {
/// #  fn new() -> Self { Self {projected_statistics: None} }
/// # }
/// // create FileScan config for reading parquet files from file://
/// let object_store_url = ObjectStoreUrl::local_filesystem();
/// let file_source = Arc::new(ParquetSource::new());
/// let config = FileScanConfigBuilder::new(object_store_url, file_schema, file_source)
///   .with_limit(Some(1000))            // read only the first 1000 records
///   .with_projection(Some(vec![2, 3])) // project columns 2 and 3
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
    /// Schema before `projection` is applied. It contains the all columns that may
    /// appear in the files. It does not include table partition columns
    /// that may be added.
    pub file_schema: SchemaRef,
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
    /// Estimated overall statistics of the files, taking `filters` into account.
    /// Defaults to [`Statistics::new_unknown`].
    pub statistics: Statistics,
    /// Columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub projection: Option<Vec<usize>>,
    /// The maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    pub limit: Option<usize>,
    /// The partitioning columns
    pub table_partition_cols: Vec<Field>,
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
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_common::Statistics;
/// # use datafusion_datasource::file::FileSource;
///
/// # fn main() {
/// # fn with_source(file_source: Arc<dyn FileSource>) {
///     // Create a schema for our Parquet files
///     let schema = Arc::new(Schema::new(vec![
///         Field::new("id", DataType::Int32, false),
///         Field::new("value", DataType::Utf8, false),
///     ]));
///
///     // Create a builder for scanning Parquet files from a local filesystem
///     let config = FileScanConfigBuilder::new(
///         ObjectStoreUrl::local_filesystem(),
///         schema,
///         file_source,
///     )
///     // Set a limit of 1000 rows
///     .with_limit(Some(1000))
///     // Project only the first column
///     .with_projection(Some(vec![0]))
///     // Add partition columns
///     .with_table_partition_cols(vec![
///         Field::new("date", DataType::Utf8, false),
///     ])
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
    file_schema: SchemaRef,
    file_source: Arc<dyn FileSource>,

    limit: Option<usize>,
    projection: Option<Vec<usize>>,
    table_partition_cols: Vec<Field>,
    constraints: Option<Constraints>,
    file_groups: Vec<FileGroup>,
    statistics: Option<Statistics>,
    output_ordering: Vec<LexOrdering>,
    file_compression_type: Option<FileCompressionType>,
    new_lines_in_values: Option<bool>,
    batch_size: Option<usize>,
}

impl FileScanConfigBuilder {
    /// Create a new [`FileScanConfigBuilder`] with default settings for scanning files.
    ///
    /// # Parameters:
    /// * `object_store_url`: See [`FileScanConfig::object_store_url`]
    /// * `file_schema`: See [`FileScanConfig::file_schema`]
    /// * `file_source`: See [`FileScanConfig::file_source`]
    pub fn new(
        object_store_url: ObjectStoreUrl,
        file_schema: SchemaRef,
        file_source: Arc<dyn FileSource>,
    ) -> Self {
        Self {
            object_store_url,
            file_schema,
            file_source,
            file_groups: vec![],
            statistics: None,
            output_ordering: vec![],
            file_compression_type: None,
            new_lines_in_values: None,
            limit: None,
            projection: None,
            table_partition_cols: vec![],
            constraints: None,
            batch_size: None,
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

    /// Set the columns on which to project the data. Indexes that are higher than the
    /// number of columns of `file_schema` refer to `table_partition_cols`.
    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Set the partitioning columns
    pub fn with_table_partition_cols(mut self, table_partition_cols: Vec<Field>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
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
    pub fn with_file(self, file: PartitionedFile) -> Self {
        self.with_file_group(FileGroup::new(vec![file]))
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

    /// Build the final [`FileScanConfig`] with all the configured settings.
    ///
    /// This method takes ownership of the builder and returns the constructed `FileScanConfig`.
    /// Any unset optional fields will use their default values.
    pub fn build(self) -> FileScanConfig {
        let Self {
            object_store_url,
            file_schema,
            file_source,
            limit,
            projection,
            table_partition_cols,
            constraints,
            file_groups,
            statistics,
            output_ordering,
            file_compression_type,
            new_lines_in_values,
            batch_size,
        } = self;

        let constraints = constraints.unwrap_or_default();
        let statistics =
            statistics.unwrap_or_else(|| Statistics::new_unknown(&file_schema));

        let file_source = file_source.with_statistics(statistics.clone());
        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);
        let new_lines_in_values = new_lines_in_values.unwrap_or(false);

        FileScanConfig {
            object_store_url,
            file_schema,
            file_source,
            limit,
            projection,
            table_partition_cols,
            constraints,
            file_groups,
            statistics,
            output_ordering,
            file_compression_type,
            new_lines_in_values,
            batch_size,
        }
    }
}

impl From<FileScanConfig> for FileScanConfigBuilder {
    fn from(config: FileScanConfig) -> Self {
        Self {
            object_store_url: config.object_store_url,
            file_schema: config.file_schema,
            file_source: config.file_source,
            file_groups: config.file_groups,
            statistics: Some(config.statistics),
            output_ordering: config.output_ordering,
            file_compression_type: Some(config.file_compression_type),
            new_lines_in_values: Some(config.new_lines_in_values),
            limit: config.limit,
            projection: config.projection,
            table_partition_cols: config.table_partition_cols,
            constraints: Some(config.constraints),
            batch_size: config.batch_size,
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

        let source = self
            .file_source
            .with_batch_size(batch_size)
            .with_schema(Arc::clone(&self.file_schema))
            .with_projection(self);

        let opener = source.create_file_opener(object_store, self, partition);

        let stream = FileStream::new(self, partition, opener, source.metrics())?;
        Ok(Box::pin(stream))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let (schema, _, _, orderings) = self.project();

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
        let (schema, constraints, _, orderings) = self.project();
        EquivalenceProperties::new_with_orderings(schema, orderings.as_slice())
            .with_constraints(constraints)
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_stats())
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
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // This process can be moved into CsvExec, but it would be an overlap of their responsibility.

        // Must be all column references, with no table partition columns (which can not be projected)
        let partitioned_columns_in_proj = projection.expr().iter().any(|(expr, _)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|expr| expr.index() >= self.file_schema.fields().len())
                .unwrap_or(false)
        });

        // If there is any non-column or alias-carrier expression, Projection should not be removed.
        let no_aliases = all_alias_free_columns(projection.expr());

        Ok((no_aliases && !partitioned_columns_in_proj).then(|| {
            let file_scan = self.clone();
            let source = Arc::clone(&file_scan.file_source);
            let new_projections = new_projections_for_columns(
                projection,
                &file_scan
                    .projection
                    .clone()
                    .unwrap_or((0..self.file_schema.fields().len()).collect()),
            );
            DataSourceExec::from_data_source(
                FileScanConfigBuilder::from(file_scan)
                    // Assign projected statistics to source
                    .with_projection(Some(new_projections))
                    .with_source(source)
                    .build(),
            ) as _
        }))
    }
}

impl FileScanConfig {
    /// Create a new [`FileScanConfig`] with default settings for scanning files.
    ///
    /// See example on [`FileScanConfig`]
    ///
    /// No file groups are added by default. See [`Self::with_file`], [`Self::with_file_group`] and
    /// [`Self::with_file_groups`].
    ///
    /// # Parameters:
    /// * `object_store_url`: See [`Self::object_store_url`]
    /// * `file_schema`: See [`Self::file_schema`]
    #[allow(deprecated)] // `new` will be removed same time as `with_source`
    pub fn new(
        object_store_url: ObjectStoreUrl,
        file_schema: SchemaRef,
        file_source: Arc<dyn FileSource>,
    ) -> Self {
        let statistics = Statistics::new_unknown(&file_schema);
        let file_source = file_source.with_statistics(statistics.clone());
        Self {
            object_store_url,
            file_schema,
            file_groups: vec![],
            constraints: Constraints::empty(),
            statistics,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            new_lines_in_values: false,
            file_source: Arc::clone(&file_source),
            batch_size: None,
        }
    }

    /// Set the file source
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_source(mut self, file_source: Arc<dyn FileSource>) -> Self {
        self.file_source = file_source.with_statistics(self.statistics.clone());
        self
    }

    /// Set the table constraints of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Set the statistics of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_statistics(mut self, statistics: Statistics) -> Self {
        self.statistics = statistics.clone();
        self.file_source = self.file_source.with_statistics(statistics);
        self
    }

    fn projection_indices(&self) -> Vec<usize> {
        match &self.projection {
            Some(proj) => proj.clone(),
            None => (0..self.file_schema.fields().len()
                + self.table_partition_cols.len())
                .collect(),
        }
    }

    fn projected_stats(&self) -> Statistics {
        let statistics = self
            .file_source
            .statistics()
            .unwrap_or(self.statistics.clone());

        let table_cols_stats = self
            .projection_indices()
            .into_iter()
            .map(|idx| {
                if idx < self.file_schema.fields().len() {
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

    fn projected_schema(&self) -> Arc<Schema> {
        let table_fields: Vec<_> = self
            .projection_indices()
            .into_iter()
            .map(|idx| {
                if idx < self.file_schema.fields().len() {
                    self.file_schema.field(idx).clone()
                } else {
                    let partition_idx = idx - self.file_schema.fields().len();
                    self.table_partition_cols[partition_idx].clone()
                }
            })
            .collect();

        Arc::new(Schema::new_with_metadata(
            table_fields,
            self.file_schema.metadata().clone(),
        ))
    }

    fn projected_constraints(&self) -> Constraints {
        let indexes = self.projection_indices();

        self.constraints
            .project(&indexes)
            .unwrap_or_else(Constraints::empty)
    }

    /// Set the projection of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Set the limit of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Add a file as a single group
    ///
    /// See [Self::file_groups] for more information.
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    #[allow(deprecated)]
    pub fn with_file(self, file: PartitionedFile) -> Self {
        self.with_file_group(FileGroup::new(vec![file]))
    }

    /// Add the file groups
    ///
    /// See [Self::file_groups] for more information.
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_file_groups(mut self, mut file_groups: Vec<FileGroup>) -> Self {
        self.file_groups.append(&mut file_groups);
        self
    }

    /// Add a new file group
    ///
    /// See [Self::file_groups] for more information
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_file_group(mut self, file_group: FileGroup) -> Self {
        self.file_groups.push(file_group);
        self
    }

    /// Set the partitioning columns of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_table_partition_cols(mut self, table_partition_cols: Vec<Field>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Set the output ordering of the files
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_output_ordering(mut self, output_ordering: Vec<LexOrdering>) -> Self {
        self.output_ordering = output_ordering;
        self
    }

    /// Set the file compression type
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Set the new_lines_in_values property
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_newlines_in_values(mut self, new_lines_in_values: bool) -> Self {
        self.new_lines_in_values = new_lines_in_values;
        self
    }

    /// Set the batch_size property
    #[deprecated(since = "47.0.0", note = "use FileScanConfigBuilder instead")]
    pub fn with_batch_size(mut self, batch_size: Option<usize>) -> Self {
        self.batch_size = batch_size;
        self
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

    /// Project the schema, constraints, and the statistics on the given column indices
    pub fn project(&self) -> (SchemaRef, Constraints, Statistics, Vec<LexOrdering>) {
        if self.projection.is_none() && self.table_partition_cols.is_empty() {
            return (
                Arc::clone(&self.file_schema),
                self.constraints.clone(),
                self.statistics.clone(),
                self.output_ordering.clone(),
            );
        }

        let schema = self.projected_schema();
        let constraints = self.projected_constraints();
        let stats = self.projected_stats();

        let output_ordering = get_projected_output_ordering(self, &schema);

        (schema, constraints, stats, output_ordering)
    }

    pub fn projected_file_column_names(&self) -> Option<Vec<String>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .map(|col_idx| self.file_schema.field(*col_idx).name())
                .cloned()
                .collect()
        })
    }

    /// Projects only file schema, ignoring partition columns
    pub fn projected_file_schema(&self) -> SchemaRef {
        let fields = self.file_column_projection_indices().map(|indices| {
            indices
                .iter()
                .map(|col_idx| self.file_schema.field(*col_idx))
                .cloned()
                .collect::<Vec<_>>()
        });

        fields.map_or_else(
            || Arc::clone(&self.file_schema),
            |f| {
                Arc::new(Schema::new_with_metadata(
                    f,
                    self.file_schema.metadata.clone(),
                ))
            },
        )
    }

    pub fn file_column_projection_indices(&self) -> Option<Vec<usize>> {
        self.projection.as_ref().map(|p| {
            p.iter()
                .filter(|col_idx| **col_idx < self.file_schema.fields().len())
                .copied()
                .collect()
        })
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

    /// Returns a new [`DataSourceExec`] to scan the files specified by this config
    #[deprecated(since = "47.0.0", note = "use DataSourceExec::new instead")]
    pub fn build(self) -> Arc<DataSourceExec> {
        DataSourceExec::from_data_source(self)
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

        write!(f, "statistics={:?}, ", self.statistics)?;

        DisplayAs::fmt_as(self, DisplayFormatType::Verbose, f)?;
        write!(f, "}}")
    }
}

impl DisplayAs for FileScanConfig {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> FmtResult {
        let (schema, _, _, orderings) = self.project();

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

/// A helper that projects partition columns into the file record batches.
///
/// One interesting trick is the usage of a cache for the key buffers of the partition column
/// dictionaries. Indeed, the partition columns are constant, so the dictionaries that represent them
/// have all their keys equal to 0. This enables us to re-use the same "all-zero" buffer across batches,
/// which makes the space consumption of the partition columns O(batch_size) instead of O(record_count).
pub struct PartitionColumnProjector {
    /// An Arrow buffer initialized to zeros that represents the key array of all partition
    /// columns (partition columns are materialized by dictionary arrays with only one
    /// value in the dictionary, thus all the keys are equal to zero).
    key_buffer_cache: ZeroBufferGenerators,
    /// Mapping between the indexes in the list of partition columns and the target
    /// schema. Sorted by index in the target schema so that we can iterate on it to
    /// insert the partition columns in the target record batch.
    projected_partition_indexes: Vec<(usize, usize)>,
    /// The schema of the table once the projection was applied.
    projected_schema: SchemaRef,
}

impl PartitionColumnProjector {
    // Create a projector to insert the partitioning columns into batches read from files
    // - `projected_schema`: the target schema with both file and partitioning columns
    // - `table_partition_cols`: all the partitioning column names
    pub fn new(projected_schema: SchemaRef, table_partition_cols: &[String]) -> Self {
        let mut idx_map = HashMap::new();
        for (partition_idx, partition_name) in table_partition_cols.iter().enumerate() {
            if let Ok(schema_idx) = projected_schema.index_of(partition_name) {
                idx_map.insert(partition_idx, schema_idx);
            }
        }

        let mut projected_partition_indexes: Vec<_> = idx_map.into_iter().collect();
        projected_partition_indexes.sort_by(|(_, a), (_, b)| a.cmp(b));

        Self {
            projected_partition_indexes,
            key_buffer_cache: Default::default(),
            projected_schema,
        }
    }

    // Transform the batch read from the file by inserting the partitioning columns
    // to the right positions as deduced from `projected_schema`
    // - `file_batch`: batch read from the file, with internal projection applied
    // - `partition_values`: the list of partition values, one for each partition column
    pub fn project(
        &mut self,
        file_batch: RecordBatch,
        partition_values: &[ScalarValue],
    ) -> Result<RecordBatch> {
        let expected_cols =
            self.projected_schema.fields().len() - self.projected_partition_indexes.len();

        if file_batch.columns().len() != expected_cols {
            return exec_err!(
                "Unexpected batch schema from file, expected {} cols but got {}",
                expected_cols,
                file_batch.columns().len()
            );
        }

        let mut cols = file_batch.columns().to_vec();
        for &(pidx, sidx) in &self.projected_partition_indexes {
            let p_value =
                partition_values
                    .get(pidx)
                    .ok_or(DataFusionError::Execution(
                        "Invalid partitioning found on disk".to_string(),
                    ))?;

            let mut partition_value = Cow::Borrowed(p_value);

            // check if user forgot to dict-encode the partition value
            let field = self.projected_schema.field(sidx);
            let expected_data_type = field.data_type();
            let actual_data_type = partition_value.data_type();
            if let DataType::Dictionary(key_type, _) = expected_data_type {
                if !matches!(actual_data_type, DataType::Dictionary(_, _)) {
                    warn!("Partition value for column {} was not dictionary-encoded, applied auto-fix.", field.name());
                    partition_value = Cow::Owned(ScalarValue::Dictionary(
                        key_type.clone(),
                        Box::new(partition_value.as_ref().clone()),
                    ));
                }
            }

            cols.insert(
                sidx,
                create_output_array(
                    &mut self.key_buffer_cache,
                    partition_value.as_ref(),
                    file_batch.num_rows(),
                )?,
            )
        }

        RecordBatch::try_new_with_options(
            Arc::clone(&self.projected_schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(file_batch.num_rows())),
        )
        .map_err(Into::into)
    }
}

#[derive(Debug, Default)]
struct ZeroBufferGenerators {
    gen_i8: ZeroBufferGenerator<i8>,
    gen_i16: ZeroBufferGenerator<i16>,
    gen_i32: ZeroBufferGenerator<i32>,
    gen_i64: ZeroBufferGenerator<i64>,
    gen_u8: ZeroBufferGenerator<u8>,
    gen_u16: ZeroBufferGenerator<u16>,
    gen_u32: ZeroBufferGenerator<u32>,
    gen_u64: ZeroBufferGenerator<u64>,
}

/// Generate a arrow [`Buffer`] that contains zero values.
#[derive(Debug, Default)]
struct ZeroBufferGenerator<T>
where
    T: ArrowNativeType,
{
    cache: Option<Buffer>,
    _t: PhantomData<T>,
}

impl<T> ZeroBufferGenerator<T>
where
    T: ArrowNativeType,
{
    const SIZE: usize = size_of::<T>();

    fn get_buffer(&mut self, n_vals: usize) -> Buffer {
        match &mut self.cache {
            Some(buf) if buf.len() >= n_vals * Self::SIZE => {
                buf.slice_with_length(0, n_vals * Self::SIZE)
            }
            _ => {
                let mut key_buffer_builder = BufferBuilder::<T>::new(n_vals);
                key_buffer_builder.advance(n_vals); // keys are all 0
                self.cache.insert(key_buffer_builder.finish()).clone()
            }
        }
    }
}

fn create_dict_array<T>(
    buffer_gen: &mut ZeroBufferGenerator<T>,
    dict_val: &ScalarValue,
    len: usize,
    data_type: DataType,
) -> Result<ArrayRef>
where
    T: ArrowNativeType,
{
    let dict_vals = dict_val.to_array()?;

    let sliced_key_buffer = buffer_gen.get_buffer(len);

    // assemble pieces together
    let mut builder = ArrayData::builder(data_type)
        .len(len)
        .add_buffer(sliced_key_buffer);
    builder = builder.add_child_data(dict_vals.to_data());
    Ok(Arc::new(DictionaryArray::<UInt16Type>::from(
        builder.build().unwrap(),
    )))
}

fn create_output_array(
    key_buffer_cache: &mut ZeroBufferGenerators,
    val: &ScalarValue,
    len: usize,
) -> Result<ArrayRef> {
    if let ScalarValue::Dictionary(key_type, dict_val) = &val {
        match key_type.as_ref() {
            DataType::Int8 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i8,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::Int16 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i16,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::Int32 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i32,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::Int64 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_i64,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::UInt8 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u8,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::UInt16 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u16,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::UInt32 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u32,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            DataType::UInt64 => {
                return create_dict_array(
                    &mut key_buffer_cache.gen_u64,
                    dict_val,
                    len,
                    val.data_type(),
                );
            }
            _ => {}
        }
    }

    val.to_array_of_size(len)
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
///┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┓
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///┃   ┌───────────────┐     ┌──────────────┐ │   ┌──────────────┐ │   ┌─────────────┐   ┃
///  │ │   1.parquet   │ │ │ │  2.parquet   │   │ │  3.parquet   │   │ │  4.parquet  │ │
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │   │Sort: A, B, C │ │   │Sort: A, B, C│   ┃
///  │ └───────────────┘ │ │ └──────────────┘   │ └──────────────┘   │ └─────────────┘ │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃                                          │                    │                     ┃
///  │                   │ │                    │                    │                 │
///┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘  ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///     DataFusion           DataFusion           DataFusion           DataFusion
///┃    Partition 1          Partition 2          Partition 3          Partition 4       ┃
/// ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///
///                                      DataSourceExec
///```
///
/// However, when more than 1 file is assigned to each partition, each
/// partition is NOT correctly sorted on `(A, B, C)`. Once the second
/// file is scanned, the same values for A, B and C can be repeated in
/// the same sorted stream
///
///```text
///┏ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─  ┃
///┃   ┌───────────────┐     ┌──────────────┐ │
///  │ │   1.parquet   │ │ │ │  2.parquet   │   ┃
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///  │ └───────────────┘ │ │ └──────────────┘   ┃
///┃   ┌───────────────┐     ┌──────────────┐ │
///  │ │   3.parquet   │ │ │ │  4.parquet   │   ┃
///┃   │ Sort: A, B, C │     │Sort: A, B, C │ │
///  │ └───────────────┘ │ │ └──────────────┘   ┃
///┃                                          │
///  │                   │ │                    ┃
///┃  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     DataFusion           DataFusion         ┃
///┃    Partition 1          Partition 2
/// ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ━ ┛
///
///              DataSourceExec
///```
fn get_projected_output_ordering(
    base_config: &FileScanConfig,
    projected_schema: &SchemaRef,
) -> Vec<LexOrdering> {
    let mut all_orderings = vec![];
    for output_ordering in &base_config.output_ordering {
        let mut new_ordering = LexOrdering::default();
        for PhysicalSortExpr { expr, options } in output_ordering.iter() {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                let name = col.name();
                if let Some((idx, _)) = projected_schema.column_with_name(name) {
                    // Compute the new sort expression (with correct index) after projection:
                    new_ordering.push(PhysicalSortExpr {
                        expr: Arc::new(Column::new(name, idx)),
                        options: *options,
                    });
                    continue;
                }
            }
            // Cannot find expression in the projected_schema, stop iterating
            // since rest of the orderings are violated
            break;
        }

        // do not push empty entries
        // otherwise we may have `Some(vec![])` at the output ordering.
        if new_ordering.is_empty() {
            continue;
        }

        // Check if any file groups are not sorted
        if base_config.file_groups.iter().any(|group| {
            if group.len() <= 1 {
                // File groups with <= 1 files are always sorted
                return false;
            }

            let statistics = match MinMaxStatistics::new_from_files(
                &new_ordering,
                projected_schema,
                base_config.projection.as_deref(),
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
    use crate::{test_util::MockSource, tests::aggr_test_schema};

    use super::*;
    use arrow::{
        array::{Int32Array, RecordBatch},
        compute::SortOptions,
    };

    use datafusion_common::stats::Precision;
    use datafusion_common::{assert_batches_eq, DFSchema};
    use datafusion_expr::{execution_props::ExecutionProps, SortExpr};
    use datafusion_physical_expr::create_physical_expr;
    use std::collections::HashMap;

    fn create_physical_sort_expr(
        e: &SortExpr,
        input_dfschema: &DFSchema,
        execution_props: &ExecutionProps,
    ) -> Result<PhysicalSortExpr> {
        let SortExpr {
            expr,
            asc,
            nulls_first,
        } = e;
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(expr, input_dfschema, execution_props)?,
            options: SortOptions {
                descending: !asc,
                nulls_first: *nulls_first,
            },
        })
    }

    /// Returns the column names on the schema
    pub fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    #[test]
    fn physical_plan_config_no_projection() {
        let file_schema = aggr_test_schema();
        let conf = config_for_projection(
            Arc::clone(&file_schema),
            None,
            Statistics::new_unknown(&file_schema),
            to_partition_cols(vec![(
                "date".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            )]),
        );

        let (proj_schema, _, proj_statistics, _) = conf.project();
        assert_eq!(proj_schema.fields().len(), file_schema.fields().len() + 1);
        assert_eq!(
            proj_schema.field(file_schema.fields().len()).name(),
            "date",
            "partition columns are the last columns"
        );
        assert_eq!(
            proj_statistics.column_statistics.len(),
            file_schema.fields().len() + 1
        );
        // TODO implement tests for partition column statistics once implemented

        let col_names = conf.projected_file_column_names();
        assert_eq!(col_names, None);

        let col_indices = conf.file_column_projection_indices();
        assert_eq!(col_indices, None);
    }

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
        let (proj_schema, _, _, _) = conf.project();
        assert_eq!(proj_schema.fields().len(), file_schema.fields().len() + 1);
        assert_eq!(
            *proj_schema.field(file_schema.fields().len()),
            table_partition_col,
            "partition columns are the last columns and ust have all values defined in created field"
        );
    }

    #[test]
    fn physical_plan_config_with_projection() {
        let file_schema = aggr_test_schema();
        let conf = config_for_projection(
            Arc::clone(&file_schema),
            Some(vec![file_schema.fields().len(), 0]),
            Statistics {
                num_rows: Precision::Inexact(10),
                // assign the column index to distinct_count to help assert
                // the source statistic after the projection
                column_statistics: (0..file_schema.fields().len())
                    .map(|i| ColumnStatistics {
                        distinct_count: Precision::Inexact(i),
                        ..Default::default()
                    })
                    .collect(),
                total_byte_size: Precision::Absent,
            },
            to_partition_cols(vec![(
                "date".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            )]),
        );

        let (proj_schema, _, proj_statistics, _) = conf.project();
        assert_eq!(
            columns(&proj_schema),
            vec!["date".to_owned(), "c1".to_owned()]
        );
        let proj_stat_cols = proj_statistics.column_statistics;
        assert_eq!(proj_stat_cols.len(), 2);
        // TODO implement tests for proj_stat_cols[0] once partition column
        // statistics are implemented
        assert_eq!(proj_stat_cols[1].distinct_count, Precision::Inexact(0));

        let col_names = conf.projected_file_column_names();
        assert_eq!(col_names, Some(vec!["c1".to_owned()]));

        let col_indices = conf.file_column_projection_indices();
        assert_eq!(col_indices, Some(vec![0]));
    }

    #[test]
    fn partition_column_projector() {
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 2]),
            ("b", &vec![-2, -1, 0]),
            ("c", &vec![10, 11, 12]),
        );
        let partition_cols = vec![
            (
                "year".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "month".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
            (
                "day".to_owned(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ),
        ];
        // create a projected schema
        let statistics = Statistics {
            num_rows: Precision::Inexact(3),
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&file_batch.schema()),
        };

        let conf = config_for_projection(
            file_batch.schema(),
            // keep all cols from file and 2 from partitioning
            Some(vec![
                0,
                1,
                2,
                file_batch.schema().fields().len(),
                file_batch.schema().fields().len() + 2,
            ]),
            statistics.clone(),
            to_partition_cols(partition_cols.clone()),
        );

        let source_statistics = conf.file_source.statistics().unwrap();
        let conf_stats = conf.statistics().unwrap();

        // projection should be reflected in the file source statistics
        assert_eq!(conf_stats.num_rows, Precision::Inexact(3));

        // 3 original statistics + 2 partition statistics
        assert_eq!(conf_stats.column_statistics.len(), 5);

        // file statics should not be modified
        assert_eq!(source_statistics, statistics);
        assert_eq!(source_statistics.column_statistics.len(), 3);

        let (proj_schema, ..) = conf.project();
        // created a projector for that projected schema
        let mut proj = PartitionColumnProjector::new(
            proj_schema,
            &partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
        );

        // project first batch
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::from("2021")),
                    wrap_partition_value_in_dict(ScalarValue::from("10")),
                    wrap_partition_value_in_dict(ScalarValue::from("26")),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = [
            "+---+----+----+------+-----+",
            "| a | b  | c  | year | day |",
            "+---+----+----+------+-----+",
            "| 0 | -2 | 10 | 2021 | 26  |",
            "| 1 | -1 | 11 | 2021 | 26  |",
            "| 2 | 0  | 12 | 2021 | 26  |",
            "+---+----+----+------+-----+",
        ];
        assert_batches_eq!(expected, &[projected_batch]);

        // project another batch that is larger than the previous one
        let file_batch = build_table_i32(
            ("a", &vec![5, 6, 7, 8, 9]),
            ("b", &vec![-10, -9, -8, -7, -6]),
            ("c", &vec![12, 13, 14, 15, 16]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::from("2021")),
                    wrap_partition_value_in_dict(ScalarValue::from("10")),
                    wrap_partition_value_in_dict(ScalarValue::from("27")),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = [
            "+---+-----+----+------+-----+",
            "| a | b   | c  | year | day |",
            "+---+-----+----+------+-----+",
            "| 5 | -10 | 12 | 2021 | 27  |",
            "| 6 | -9  | 13 | 2021 | 27  |",
            "| 7 | -8  | 14 | 2021 | 27  |",
            "| 8 | -7  | 15 | 2021 | 27  |",
            "| 9 | -6  | 16 | 2021 | 27  |",
            "+---+-----+----+------+-----+",
        ];
        assert_batches_eq!(expected, &[projected_batch]);

        // project another batch that is smaller than the previous one
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 3]),
            ("b", &vec![2, 3, 4]),
            ("c", &vec![4, 5, 6]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    wrap_partition_value_in_dict(ScalarValue::from("2021")),
                    wrap_partition_value_in_dict(ScalarValue::from("10")),
                    wrap_partition_value_in_dict(ScalarValue::from("28")),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = [
            "+---+---+---+------+-----+",
            "| a | b | c | year | day |",
            "+---+---+---+------+-----+",
            "| 0 | 2 | 4 | 2021 | 28  |",
            "| 1 | 3 | 5 | 2021 | 28  |",
            "| 3 | 4 | 6 | 2021 | 28  |",
            "+---+---+---+------+-----+",
        ];
        assert_batches_eq!(expected, &[projected_batch]);

        // forgot to dictionary-wrap the scalar value
        let file_batch = build_table_i32(
            ("a", &vec![0, 1, 2]),
            ("b", &vec![-2, -1, 0]),
            ("c", &vec![10, 11, 12]),
        );
        let projected_batch = proj
            .project(
                // file_batch is ok here because we kept all the file cols in the projection
                file_batch,
                &[
                    ScalarValue::from("2021"),
                    ScalarValue::from("10"),
                    ScalarValue::from("26"),
                ],
            )
            .expect("Projection of partition columns into record batch failed");
        let expected = [
            "+---+----+----+------+-----+",
            "| a | b  | c  | year | day |",
            "+---+----+----+------+-----+",
            "| 0 | -2 | 10 | 2021 | 26  |",
            "| 1 | -1 | 11 | 2021 | 26  |",
            "| 2 | 0  | 12 | 2021 | 26  |",
            "+---+----+----+------+-----+",
        ];
        assert_batches_eq!(expected, &[projected_batch]);
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
        let projection = config_for_projection(
            schema.clone(),
            Some(vec![0, 3, 5, schema.fields().len()]),
            Statistics::new_unknown(&schema),
            to_partition_cols(partition_cols),
        )
        .projected_file_schema();

        // Assert partition column filtered out in projected file schema
        let expected_columns = vec!["c1", "c4", "c6"];
        let actual_columns = projection
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(expected_columns, actual_columns);
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
        let projection = config_for_projection(
            schema.clone(),
            None,
            Statistics::new_unknown(&schema),
            to_partition_cols(partition_cols),
        )
        .projected_file_schema();

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
            statistics: Vec<Option<(f64, f64)>>,
        }
        impl File {
            fn new(
                name: &'static str,
                date: &'static str,
                statistics: Vec<Option<(f64, f64)>>,
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
            // reject nullable sort columns
            TestCase {
                name: "no nullable sort columns",
                file_schema: Schema::new(vec![Field::new(
                    "value".to_string(),
                    DataType::Float64,
                    true, // should fail because nullable
                )]),
                files: vec![
                    File::new("0", "2023-01-01", vec![Some((0.00, 0.49))]),
                    File::new("1", "2023-01-01", vec![Some((0.50, 1.00))]),
                    File::new("2", "2023-01-02", vec![Some((0.00, 1.00))]),
                ],
                sort: vec![col("value").sort(true, false)],
                expected_result: Err("construct min/max statistics for split_groups_by_statistics\ncaused by\nbuild min rows\ncaused by\ncreate sorting columns\ncaused by\nError during planning: cannot sort by nullable column")
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
            let sort_order = LexOrdering::from(
                case.sort
                    .into_iter()
                    .map(|expr| {
                        create_physical_sort_expr(
                            &expr,
                            &DFSchema::try_from(table_schema.as_ref().clone())?,
                            &ExecutionProps::default(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            );

            let partitioned_files = FileGroup::new(
                case.files.into_iter().map(From::from).collect::<Vec<_>>(),
            );
            let result = FileScanConfig::split_groups_by_statistics(
                &table_schema,
                &[partitioned_files.clone()],
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
                    statistics: Some(Statistics {
                        num_rows: Precision::Absent,
                        total_byte_size: Precision::Absent,
                        column_statistics: file
                            .statistics
                            .into_iter()
                            .map(|stats| {
                                stats
                                    .map(|(min, max)| ColumnStatistics {
                                        min_value: Precision::Exact(ScalarValue::from(
                                            min,
                                        )),
                                        max_value: Precision::Exact(ScalarValue::from(
                                            max,
                                        )),
                                        ..Default::default()
                                    })
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>(),
                    }),
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
        FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test:///").unwrap(),
            file_schema,
            Arc::new(MockSource::default()),
        )
        .with_projection(projection)
        .with_statistics(statistics)
        .with_table_partition_cols(table_partition_cols)
        .build()
    }

    /// Convert partition columns from Vec<String DataType> to Vec<Field>
    fn to_partition_cols(table_partition_cols: Vec<(String, DataType)>) -> Vec<Field> {
        table_partition_cols
            .iter()
            .map(|(name, dtype)| Field::new(name, dtype.clone(), false))
            .collect::<Vec<_>>()
    }

    /// returns record batch with 3 columns of i32 in memory
    pub fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_file_scan_config_builder() {
        let file_schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
        let file_source: Arc<dyn FileSource> = Arc::new(MockSource::default());

        // Create a builder with required parameters
        let builder = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_schema),
            Arc::clone(&file_source),
        );

        // Build with various configurations
        let config = builder
            .with_limit(Some(1000))
            .with_projection(Some(vec![0, 1]))
            .with_table_partition_cols(vec![Field::new(
                "date",
                wrap_partition_type_in_dict(DataType::Utf8),
                false,
            )])
            .with_constraints(Constraints::empty())
            .with_statistics(Statistics::new_unknown(&file_schema))
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "test.parquet".to_string(),
                1024,
            )])])
            .with_output_ordering(vec![LexOrdering::default()])
            .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
            .with_newlines_in_values(true)
            .build();

        // Verify the built config has all the expected values
        assert_eq!(config.object_store_url, object_store_url);
        assert_eq!(config.file_schema, file_schema);
        assert_eq!(config.limit, Some(1000));
        assert_eq!(config.projection, Some(vec![0, 1]));
        assert_eq!(config.table_partition_cols.len(), 1);
        assert_eq!(config.table_partition_cols[0].name(), "date");
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
    fn test_file_scan_config_builder_defaults() {
        let file_schema = aggr_test_schema();
        let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
        let file_source: Arc<dyn FileSource> = Arc::new(MockSource::default());

        // Create a builder with only required parameters and build without any additional configurations
        let config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&file_schema),
            Arc::clone(&file_source),
        )
        .build();

        // Verify default values
        assert_eq!(config.object_store_url, object_store_url);
        assert_eq!(config.file_schema, file_schema);
        assert_eq!(config.limit, None);
        assert_eq!(config.projection, None);
        assert!(config.table_partition_cols.is_empty());
        assert!(config.file_groups.is_empty());
        assert_eq!(
            config.file_compression_type,
            FileCompressionType::UNCOMPRESSED
        );
        assert!(!config.new_lines_in_values);
        assert!(config.output_ordering.is_empty());
        assert!(config.constraints.is_empty());

        // Verify statistics are set to unknown
        assert_eq!(config.statistics.num_rows, Precision::Absent);
        assert_eq!(config.statistics.total_byte_size, Precision::Absent);
        assert_eq!(
            config.statistics.column_statistics.len(),
            file_schema.fields().len()
        );
        for stat in config.statistics.column_statistics {
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
        let file_source: Arc<dyn FileSource> = Arc::new(MockSource::default());
        let partition_cols = vec![Field::new(
            "date",
            wrap_partition_type_in_dict(DataType::Utf8),
            false,
        )];
        let file = PartitionedFile::new("test_file.parquet", 100);

        // Create a config with non-default values
        let original_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&schema),
            Arc::clone(&file_source),
        )
        .with_projection(Some(vec![0, 2]))
        .with_limit(Some(10))
        .with_table_partition_cols(partition_cols.clone())
        .with_file(file.clone())
        .with_constraints(Constraints::default())
        .with_newlines_in_values(true)
        .build();

        // Create a new builder from the config
        let new_builder = FileScanConfigBuilder::from(original_config);

        // Build a new config from this builder
        let new_config = new_builder.build();

        // Verify properties match
        assert_eq!(new_config.object_store_url, object_store_url);
        assert_eq!(new_config.file_schema, schema);
        assert_eq!(new_config.projection, Some(vec![0, 2]));
        assert_eq!(new_config.limit, Some(10));
        assert_eq!(new_config.table_partition_cols, partition_cols);
        assert_eq!(new_config.file_groups.len(), 1);
        assert_eq!(new_config.file_groups[0].len(), 1);
        assert_eq!(
            new_config.file_groups[0][0].object_meta.location.as_ref(),
            "test_file.parquet"
        );
        assert_eq!(new_config.constraints, Constraints::default());
        assert!(new_config.new_lines_in_values);
    }
}
