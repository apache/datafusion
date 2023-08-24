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

//! The table implementation.

use std::str::FromStr;
use std::{any::Any, sync::Arc};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, SchemaBuilder, SchemaRef};
use arrow_schema::Schema;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_common::{internal_err, plan_err, project_schema, SchemaExt, ToDFSchema};
use datafusion_expr::expr::Sort;
use datafusion_optimizer::utils::conjunction;
use datafusion_physical_expr::{create_physical_expr, LexOrdering, PhysicalSortExpr};
use futures::{future, stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectMeta;

use crate::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use crate::datasource::{
    file_format::{
        arrow::ArrowFormat, avro::AvroFormat, csv::CsvFormat, json::JsonFormat,
        parquet::ParquetFormat, FileFormat,
    },
    get_statistics_with_limit,
    listing::ListingTableUrl,
    TableProvider, TableType,
};
use crate::logical_expr::TableProviderFilterPushDown;
use crate::physical_plan;
use crate::{
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics},
};
use datafusion_common::{FileCompressionType, FileType};

use super::PartitionedFile;

use super::helpers::{expr_applicable_for_cols, pruned_partition_list, split_files};

/// Configuration for creating a [`ListingTable`]
#[derive(Debug, Clone)]
pub struct ListingTableConfig {
    /// Paths on the `ObjectStore` for creating `ListingTable`.
    /// They should share the same schema and object store.
    pub table_paths: Vec<ListingTableUrl>,
    /// Optional `SchemaRef` for the to be created `ListingTable`.
    pub file_schema: Option<SchemaRef>,
    /// Optional `ListingOptions` for the to be created `ListingTable`.
    pub options: Option<ListingOptions>,
}

impl ListingTableConfig {
    /// Creates new [`ListingTableConfig`].
    ///
    /// The [`SchemaRef`] and [`ListingOptions`] are inferred based on
    /// the suffix of the provided `table_paths` first element.
    pub fn new(table_path: ListingTableUrl) -> Self {
        let table_paths = vec![table_path];
        Self {
            table_paths,
            file_schema: None,
            options: None,
        }
    }

    /// Creates new [`ListingTableConfig`] with multiple table paths.
    ///
    /// The [`SchemaRef`] and [`ListingOptions`] are inferred based on
    /// the suffix of the provided `table_paths` first element.
    pub fn new_with_multi_paths(table_paths: Vec<ListingTableUrl>) -> Self {
        Self {
            table_paths,
            file_schema: None,
            options: None,
        }
    }
    /// Add `schema` to [`ListingTableConfig`]
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        Self {
            table_paths: self.table_paths,
            file_schema: Some(schema),
            options: self.options,
        }
    }

    /// Add `listing_options` to [`ListingTableConfig`]
    pub fn with_listing_options(self, listing_options: ListingOptions) -> Self {
        Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
        }
    }

    fn infer_format(path: &str) -> Result<(Arc<dyn FileFormat>, String)> {
        let err_msg = format!("Unable to infer file type from path: {path}");

        let mut exts = path.rsplit('.');

        let mut splitted = exts.next().unwrap_or("");

        let file_compression_type = FileCompressionType::from_str(splitted)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        if file_compression_type.is_compressed() {
            splitted = exts.next().unwrap_or("");
        }

        let file_type = FileType::from_str(splitted)
            .map_err(|_| DataFusionError::Internal(err_msg.to_owned()))?;

        let ext = file_type
            .get_ext_with_compression(file_compression_type.to_owned())
            .map_err(|_| DataFusionError::Internal(err_msg))?;

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::ARROW => Arc::new(ArrowFormat),
            FileType::AVRO => Arc::new(AvroFormat),
            FileType::CSV => Arc::new(
                CsvFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::JSON => Arc::new(
                JsonFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
        };

        Ok((file_format, ext))
    }

    /// Infer `ListingOptions` based on `table_path` suffix.
    pub async fn infer_options(self, state: &SessionState) -> Result<Self> {
        let store = if let Some(url) = self.table_paths.get(0) {
            state.runtime_env().object_store(url)?
        } else {
            return Ok(self);
        };

        let file = self
            .table_paths
            .get(0)
            .unwrap()
            .list_all_files(store.as_ref(), "")
            .next()
            .await
            .ok_or_else(|| DataFusionError::Internal("No files for table".into()))??;

        let (format, file_extension) =
            ListingTableConfig::infer_format(file.location.as_ref())?;

        let listing_options = ListingOptions::new(format)
            .with_file_extension(file_extension)
            .with_target_partitions(state.config().target_partitions());

        Ok(Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
        })
    }

    /// Infer the [`SchemaRef`] based on `table_path` suffix.  Requires `self.options` to be set prior to using.
    pub async fn infer_schema(self, state: &SessionState) -> Result<Self> {
        match self.options {
            Some(options) => {
                let schema = if let Some(url) = self.table_paths.get(0) {
                    options.infer_schema(state, url).await?
                } else {
                    Arc::new(Schema::empty())
                };

                Ok(Self {
                    table_paths: self.table_paths,
                    file_schema: Some(schema),
                    options: Some(options),
                })
            }
            None => internal_err!("No `ListingOptions` set for inferring schema"),
        }
    }

    /// Convenience wrapper for calling `infer_options` and `infer_schema`
    pub async fn infer(self, state: &SessionState) -> Result<Self> {
        self.infer_options(state).await?.infer_schema(state).await
    }
}

#[derive(Debug, Clone)]
///controls how new data should be inserted to a ListingTable
pub enum ListingTableInsertMode {
    ///Data should be appended to an existing file
    AppendToFile,
    ///Data is appended as new files in existing TablePaths
    AppendNewFiles,
    ///Throw an error if insert into is attempted on this table
    Error,
}

impl FromStr for ListingTableInsertMode {
    type Err = DataFusionError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_lowercase();
        match s_lower.as_str() {
            "append_to_file" => Ok(ListingTableInsertMode::AppendToFile),
            "append_new_files" => Ok(ListingTableInsertMode::AppendNewFiles),
            "error" => Ok(ListingTableInsertMode::Error),
            _ => Err(DataFusionError::Plan(format!(
                "Unknown or unsupported insert mode {s}. Supported options are \
                append_to_file, append_new_files, and error."
            ))),
        }
    }
}
/// Options for creating a [`ListingTable`]
#[derive(Clone, Debug)]
pub struct ListingOptions {
    /// A suffix on which files should be filtered (leave empty to
    /// keep all files on the path)
    pub file_extension: String,
    /// The file format
    pub format: Arc<dyn FileFormat>,
    /// The expected partition column names in the folder structure.
    /// See [Self::with_table_partition_cols] for details
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Set true to try to guess statistics from the files.
    /// This can add a lot of overhead as it will usually require files
    /// to be opened and at least partially parsed.
    pub collect_stat: bool,
    /// Group files to avoid that the number of partitions exceeds
    /// this limit
    pub target_partitions: usize,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    ///
    /// DataFusion may take advantage of this ordering to omit sorts
    /// or use more efficient algorithms. Currently sortedness must be
    /// provided if it is known by some external mechanism, but may in
    /// the future be automatically determined, for example using
    /// parquet metadata.
    ///
    /// See <https://github.com/apache/arrow-datafusion/issues/4177>
    /// NOTE: This attribute stores all equivalent orderings (the outer `Vec`)
    ///       where each ordering consists of an individual lexicographic
    ///       ordering (encapsulated by a `Vec<Expr>`). If there aren't
    ///       multiple equivalent orderings, the outer `Vec` will have a
    ///       single element.
    pub file_sort_order: Vec<Vec<Expr>>,
    /// Infinite source means that the input is not guaranteed to end.
    /// Currently, CSV, JSON, and AVRO formats are supported.
    /// In order to support infinite inputs, DataFusion may adjust query
    /// plans (e.g. joins) to run the given query in full pipelining mode.
    pub infinite_source: bool,
    ///This setting controls how inserts to this table should be handled
    pub insert_mode: ListingTableInsertMode,
}

impl ListingOptions {
    /// Creates an options instance with the given format
    /// Default values:
    /// - no file extension filter
    /// - no input partition to discover
    /// - one target partition
    /// - stat collection
    pub fn new(format: Arc<dyn FileFormat>) -> Self {
        Self {
            file_extension: String::new(),
            format,
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
            file_sort_order: vec![],
            infinite_source: false,
            insert_mode: ListingTableInsertMode::AppendToFile,
        }
    }

    /// Set unbounded assumption on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use datafusion::datasource::{listing::ListingOptions, file_format::csv::CsvFormat};
    /// use datafusion::prelude::SessionContext;
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     CsvFormat::default()
    ///   )).with_infinite_source(true);
    ///
    /// assert_eq!(listing_options.infinite_source, true);
    /// ```
    pub fn with_infinite_source(mut self, infinite_source: bool) -> Self {
        self.infinite_source = infinite_source;
        self
    }

    /// Set file extension on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_extension(".parquet");
    ///
    /// assert_eq!(listing_options.file_extension, ".parquet");
    /// ```
    pub fn with_file_extension(mut self, file_extension: impl Into<String>) -> Self {
        self.file_extension = file_extension.into();
        self
    }

    /// Set `table partition columns` on [`ListingOptions`] and returns self.
    ///
    /// "partition columns," used to support [Hive Partitioning], are
    /// columns added to the data that is read, based on the folder
    /// structure where the data resides.
    ///
    /// For example, give the following files in your filesystem:
    ///
    /// ```text
    /// /mnt/nyctaxi/year=2022/month=01/tripdata.parquet
    /// /mnt/nyctaxi/year=2021/month=12/tripdata.parquet
    /// /mnt/nyctaxi/year=2021/month=11/tripdata.parquet
    /// ```
    ///
    /// A [`ListingTable`] created at `/mnt/nyctaxi/` with partition
    /// columns "year" and "month" will include new `year` and `month`
    /// columns while reading the files. The `year` column would have
    /// value `2022` and the `month` column would have value `01` for
    /// the rows read from
    /// `/mnt/nyctaxi/year=2022/month=01/tripdata.parquet`
    ///
    ///# Notes
    ///
    /// - If only one level (e.g. `year` in the example above) is
    /// specified, the other levels are ignored but the files are
    /// still read.
    ///
    /// - Files that don't follow this partitioning scheme will be
    /// ignored.
    ///
    /// - Since the columns have the same value for all rows read from
    /// each individual file (such as dates), they are typically
    /// dictionary encoded for efficiency. You may use
    /// [`wrap_partition_type_in_dict`] to request a
    /// dictionary-encoded type.
    ///
    /// - The partition columns are solely extracted from the file path. Especially they are NOT part of the parquet files itself.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::DataType;
    /// # use datafusion::prelude::col;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// // listing options for files with paths such as  `/mnt/data/col_a=x/col_b=y/data.parquet`
    /// // `col_a` and `col_b` will be included in the data read from those files
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_table_partition_cols(vec![("col_a".to_string(), DataType::Utf8),
    ///       ("col_b".to_string(), DataType::Utf8)]);
    ///
    /// assert_eq!(listing_options.table_partition_cols, vec![("col_a".to_string(), DataType::Utf8),
    ///     ("col_b".to_string(), DataType::Utf8)]);
    /// ```
    ///
    /// [Hive Partitioning]: https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.1.3/bk_system-admin-guide/content/hive_partitioned_tables.html
    /// [`wrap_partition_type_in_dict`]: crate::datasource::physical_plan::wrap_partition_type_in_dict
    pub fn with_table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Set stat collection on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_collect_stat(true);
    ///
    /// assert_eq!(listing_options.collect_stat, true);
    /// ```
    pub fn with_collect_stat(mut self, collect_stat: bool) -> Self {
        self.collect_stat = collect_stat;
        self
    }

    /// Set number of target partitions on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_target_partitions(8);
    ///
    /// assert_eq!(listing_options.target_partitions, 8);
    /// ```
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    /// Set file sort order on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::col;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    ///  // Tell datafusion that the files are sorted by column "a"
    ///  let file_sort_order = vec![vec![
    ///    col("a").sort(true, true)
    ///  ]];
    ///
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_sort_order(file_sort_order.clone());
    ///
    /// assert_eq!(listing_options.file_sort_order, file_sort_order);
    /// ```
    pub fn with_file_sort_order(mut self, file_sort_order: Vec<Vec<Expr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Configure how insertions to this table should be handled.
    pub fn with_insert_mode(mut self, insert_mode: ListingTableInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }

    /// Infer the schema of the files at the given path on the provided object store.
    /// The inferred schema does not include the partitioning columns.
    ///
    /// This method will not be called by the table itself but before creating it.
    /// This way when creating the logical plan we can decide to resolve the schema
    /// locally or ask a remote service to do it (e.g a scheduler).
    pub async fn infer_schema<'a>(
        &'a self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let files: Vec<_> = table_path
            .list_all_files(store.as_ref(), &self.file_extension)
            .try_collect()
            .await?;

        self.format.infer_schema(state, &store, &files).await
    }
}

/// Collected statistics for files
/// Cache is invalided when file size or last modification has changed
#[derive(Default)]
struct StatisticsCache {
    statistics: DashMap<Path, (ObjectMeta, Statistics)>,
}

impl StatisticsCache {
    /// Get `Statistics` for file location. Returns None if file has changed or not found.
    fn get(&self, meta: &ObjectMeta) -> Option<Statistics> {
        self.statistics
            .get(&meta.location)
            .map(|s| {
                let (saved_meta, statistics) = s.value();
                if saved_meta.size != meta.size
                    || saved_meta.last_modified != meta.last_modified
                {
                    // file has changed
                    None
                } else {
                    Some(statistics.clone())
                }
            })
            .unwrap_or(None)
    }

    /// Save collected file statistics
    fn save(&self, meta: ObjectMeta, statistics: Statistics) {
        self.statistics
            .insert(meta.location.clone(), (meta, statistics));
    }
}

/// Reads data from one or more files via an
/// [`ObjectStore`](object_store::ObjectStore). For example, from
/// local files or objects from AWS S3. Implements [`TableProvider`],
/// a DataFusion data source.
///
/// # Features
///
/// 1. Merges schemas if the files have compatible but not indentical schemas
///
/// 2. Hive-style partitioning support, where a path such as
/// `/files/date=1/1/2022/data.parquet` is injected as a `date` column.
///
/// 3. Projection pushdown for formats that support it such as such as
/// Parquet
///
/// # Example
///
/// Here is an example of reading a directory of parquet files using a
/// [`ListingTable`]:
///
/// ```no_run
/// # use datafusion::prelude::SessionContext;
/// # use datafusion::error::Result;
/// # use std::sync::Arc;
/// # use datafusion::datasource::{
/// #   listing::{
/// #      ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
/// #   },
/// #   file_format::parquet::ParquetFormat,
/// # };
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let session_state = ctx.state();
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
///    .infer_schema(&session_state, &table_path)
///    .await?;
///
/// let config = ListingTableConfig::new(table_path)
///   .with_listing_options(listing_options)
///   .with_schema(resolved_schema);
///
/// // Create a new TableProvider
/// let provider = Arc::new(ListingTable::try_new(config)?);
///
/// // This provider can now be read as a dataframe:
/// let df = ctx.read_table(provider.clone());
///
/// // or registered as a named table:
/// ctx.register_table("my_table", provider);
///
/// # Ok(())
/// # }
/// ```
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    /// File fields only
    file_schema: SchemaRef,
    /// File fields + partition columns
    table_schema: SchemaRef,
    options: ListingOptions,
    definition: Option<String>,
    collected_statistics: StatisticsCache,
    infinite_source: bool,
}

impl ListingTable {
    /// Create new [`ListingTable`] that lists the FS to get the files
    /// to scan. See [`ListingTable`] for and example.
    ///
    /// Takes a `ListingTableConfig` as input which requires an `ObjectStore` and `table_path`.
    /// `ListingOptions` and `SchemaRef` are optional.  If they are not
    /// provided the file type is inferred based on the file suffix.
    /// If the schema is provided then it must be resolved before creating the table
    /// and should contain the fields of the file without the table
    /// partitioning columns.
    ///
    pub fn try_new(config: ListingTableConfig) -> Result<Self> {
        let file_schema = config
            .file_schema
            .ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;

        let options = config.options.ok_or_else(|| {
            DataFusionError::Internal("No ListingOptions provided".into())
        })?;

        // Add the partition columns to the file schema
        let mut builder = SchemaBuilder::from(file_schema.fields());
        for (part_col_name, part_col_type) in &options.table_partition_cols {
            builder.push(Field::new(part_col_name, part_col_type.clone(), false));
        }
        let infinite_source = options.infinite_source;

        let table = Self {
            table_paths: config.table_paths,
            file_schema,
            table_schema: Arc::new(builder.finish()),
            options,
            definition: None,
            collected_statistics: Default::default(),
            infinite_source,
        };

        Ok(table)
    }

    /// Specify the SQL definition for this table, if any
    pub fn with_definition(mut self, defintion: Option<String>) -> Self {
        self.definition = defintion;
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

    /// If file_sort_order is specified, creates the appropriate physical expressions
    fn try_create_output_ordering(&self) -> Result<Vec<LexOrdering>> {
        let mut all_sort_orders = vec![];

        for exprs in &self.options.file_sort_order {
            // Construct PhsyicalSortExpr objects from Expr objects:
            let sort_exprs = exprs
            .iter()
            .map(|expr| {
                if let Expr::Sort(Sort { expr, asc, nulls_first }) = expr {
                    if let Expr::Column(col) = expr.as_ref() {
                        let expr = physical_plan::expressions::col(&col.name, self.table_schema.as_ref())?;
                        Ok(PhysicalSortExpr {
                            expr,
                            options: SortOptions {
                                descending: !asc,
                                nulls_first: *nulls_first,
                            },
                        })
                    }
                    else {
                        plan_err!("Expected single column references in output_ordering, got {expr}")
                    }
                } else {
                    plan_err!("Expected Expr::Sort in output_ordering, but got {expr}")
                }
            })
            .collect::<Result<Vec<_>>>()?;
            all_sort_orders.push(sort_exprs);
        }
        Ok(all_sort_orders)
    }
}

#[async_trait]
impl TableProvider for ListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, statistics) =
            self.list_files_for_scan(state, filters, limit).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = project_schema(&schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        // extract types of partition columns
        let table_partition_cols = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| {
                Ok((
                    col.0.to_owned(),
                    self.table_schema
                        .field_with_name(&col.0)?
                        .data_type()
                        .clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                &self.table_schema,
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };

        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };
        // create the execution plan
        self.options
            .format
            .create_physical_plan(
                state,
                FileScanConfig {
                    object_store_url,
                    file_schema: Arc::clone(&self.file_schema),
                    file_groups: partitioned_file_lists,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: self.try_create_output_ordering()?,
                    table_partition_cols,
                    infinite_source: self.infinite_source,
                },
                filters.as_ref(),
            )
            .await
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        if expr_applicable_for_cols(
            &self
                .options
                .table_partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
            filter,
        ) {
            // if filter can be handled by partiton pruning, it is exact
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            // otherwise, we still might be able to handle the filter with file
            // level mechanisms such as Parquet row group pruning.
            Ok(TableProviderFilterPushDown::Inexact)
        }
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Check that the schema of the plan matches the schema of this table.
        if !self.schema().equivalent_names_and_types(&input.schema()) {
            return plan_err!(
                // Return an error if schema of the input query does not match with the table schema.
                "Inserting query must have the same schema with the table."
            );
        }

        if self.table_paths().len() > 1 {
            return plan_err!(
                "Writing to a table backed by multiple partitions is not supported yet"
            );
        }

        // TODO support inserts to sorted tables which preserve sort_order
        // Inserts currently make no effort to preserve sort_order. This could lead to
        // incorrect query results on the table after inserting incorrectly sorted data.
        let unsorted: Vec<Vec<Expr>> = vec![];
        if self.options.file_sort_order != unsorted {
            return Err(
                DataFusionError::NotImplemented(
                    "Writing to a sorted listing table via insert into is not supported yet. \
                    To write to this table in the meantime, register an equivalent table with \
                    file_sort_order = vec![]".into())
            );
        }

        let table_path = &self.table_paths()[0];
        // Get the object store for the table path.
        let store = state.runtime_env().object_store(table_path)?;

        let file_list_stream = pruned_partition_list(
            store.as_ref(),
            table_path,
            &[],
            &self.options.file_extension,
            &self.options.table_partition_cols,
        )
        .await?;

        let file_groups = file_list_stream.try_collect::<Vec<_>>().await?;
        let writer_mode;
        //if we are writing a single output_partition to a table backed by a single file
        //we can append to that file. Otherwise, we can write new files into the directory
        //adding new files to the listing table in order to insert to the table.
        let input_partitions = input.output_partitioning().partition_count();
        match self.options.insert_mode {
            ListingTableInsertMode::AppendToFile => {
                if input_partitions > file_groups.len() {
                    return Err(DataFusionError::Plan(format!(
                        "Cannot append {input_partitions} partitions to {} files!",
                        file_groups.len()
                    )));
                }
                writer_mode =
                    crate::datasource::file_format::write::FileWriterMode::Append;
            }
            ListingTableInsertMode::AppendNewFiles => {
                writer_mode =
                    crate::datasource::file_format::write::FileWriterMode::PutMultipart
            }
            ListingTableInsertMode::Error => {
                return plan_err!(
                    "Invalid plan attempting write to table with TableWriteMode::Error!"
                )
            }
        }

        // Sink related option, apart from format
        let config = FileSinkConfig {
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_groups,
            output_schema: self.schema(),
            table_partition_cols: self.options.table_partition_cols.clone(),
            writer_mode,
            // TODO: when listing table is known to be backed by a single file, this should be false
            per_thread_output: true,
            overwrite,
        };

        self.options()
            .format
            .create_writer_physical_plan(input, state, config)
            .await
    }
}

impl ListingTable {
    /// Get the list of files for a scan as well as the file level statistics.
    /// The list is grouped to let the execution plan know how the files should
    /// be distributed to different threads / executors.
    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a SessionState,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        let store = if let Some(url) = self.table_paths.get(0) {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok((vec![], Statistics::default()));
        };
        // list files (with partitions)
        let file_list = future::try_join_all(self.table_paths.iter().map(|table_path| {
            pruned_partition_list(
                store.as_ref(),
                table_path,
                filters,
                &self.options.file_extension,
                &self.options.table_partition_cols,
            )
        }))
        .await?;

        let file_list = stream::iter(file_list).flatten();

        // collect the statistics if required by the config
        let files = file_list.then(|part_file| async {
            let part_file = part_file?;
            let statistics = if self.options.collect_stat {
                match self.collected_statistics.get(&part_file.object_meta) {
                    Some(statistics) => statistics,
                    None => {
                        let statistics = self
                            .options
                            .format
                            .infer_stats(
                                ctx,
                                &store,
                                self.file_schema.clone(),
                                &part_file.object_meta,
                            )
                            .await?;
                        self.collected_statistics
                            .save(part_file.object_meta.clone(), statistics.clone());
                        statistics
                    }
                }
            } else {
                Statistics::default()
            };
            Ok((part_file, statistics)) as Result<(PartitionedFile, Statistics)>
        });

        let (files, statistics) =
            get_statistics_with_limit(files, self.schema(), limit).await?;

        Ok((
            split_files(files, self.options.target_partitions),
            statistics,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{provider_as_source, MemTable};
    use crate::execution::options::ArrowReadOptions;
    use crate::physical_plan::collect;
    use crate::prelude::*;
    use crate::{
        assert_batches_eq,
        datasource::file_format::{avro::AvroFormat, parquet::ParquetFormat},
        execution::options::ReadOptions,
        logical_expr::{col, lit},
        test::{columns, object_store::register_test_store},
    };
    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::DateTime;
    use datafusion_common::assert_contains;
    use datafusion_common::GetExt;
    use datafusion_expr::LogicalPlanBuilder;
    use rstest::*;
    use std::collections::HashMap;
    use std::fs::File;
    use tempfile::TempDir;

    /// It creates dummy file and checks if it can create unbounded input executors.
    async fn unbounded_table_helper(
        file_type: FileType,
        listing_option: ListingOptions,
        infinite_data: bool,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(
            &ctx,
            &[(&format!("table/file{}", file_type.get_ext()), 100)],
        );

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_path = ListingTableUrl::parse("test:///table/").unwrap();
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_option)
            .with_schema(Arc::new(schema));
        // Create a table
        let table = ListingTable::try_new(config)?;
        // Create executor from table
        let source_exec = table.scan(&ctx.state(), None, &[], None).await?;

        assert_eq!(source_exec.unbounded_output(&[])?, infinite_data);

        Ok(())
    }

    #[tokio::test]
    async fn read_single_file() -> Result<()> {
        let ctx = SessionContext::new();

        let table = load_table(&ctx, "alltypes_plain.parquet").await?;
        let projection = None;
        let exec = table
            .scan(&ctx.state(), projection, &[], None)
            .await
            .expect("Scan table");

        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.output_partitioning().partition_count(), 1);

        // test metadata
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn load_table_stats_by_default() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        let opt = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema = opt.infer_schema(&state, &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;

        let exec = table.scan(&state, None, &[], None).await?;
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn load_table_stats_when_no_stats() -> Result<()> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();

        let opt = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(false);
        let schema = opt.infer_schema(&state, &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;

        let exec = table.scan(&state, None, &[], None).await?;
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_create_output_ordering() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema = options.infer_schema(&state, &table_path).await.unwrap();

        use crate::physical_plan::expressions::col as physical_col;
        use std::ops::Add;

        // (file_sort_order, expected_result)
        let cases = vec![
            (vec![], Ok(vec![])),
            // not a sort expr
            (
                vec![vec![col("string_col")]],
                Err("Expected Expr::Sort in output_ordering, but got string_col"),
            ),
            // sort expr, but non column
            (
                vec![vec![
                    col("int_col").add(lit(1)).sort(true, true),
                ]],
                Err("Expected single column references in output_ordering, got int_col + Int32(1)"),
            ),
            // ok with one column
            (
                vec![vec![col("string_col").sort(true, false)]],
                Ok(vec![vec![PhysicalSortExpr {
                    expr: physical_col("string_col", &schema).unwrap(),
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                }]])

            ),
            // ok with two columns, different options
            (
                vec![vec![
                    col("string_col").sort(true, false),
                    col("int_col").sort(false, true),
                ]],
                Ok(vec![vec![
                    PhysicalSortExpr {
                        expr: physical_col("string_col", &schema).unwrap(),
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    },
                    PhysicalSortExpr {
                        expr: physical_col("int_col", &schema).unwrap(),
                        options: SortOptions {
                            descending: true,
                            nulls_first: true,
                        },
                    },
                ]])

            ),

        ];

        for (file_sort_order, expected_result) in cases {
            let options = options.clone().with_file_sort_order(file_sort_order);

            let config = ListingTableConfig::new(table_path.clone())
                .with_listing_options(options)
                .with_schema(schema.clone());

            let table =
                ListingTable::try_new(config.clone()).expect("Creating the table");
            let ordering_result = table.try_create_output_ordering();

            match (expected_result, ordering_result) {
                (Ok(expected), Ok(result)) => {
                    assert_eq!(expected, result);
                }
                (Err(expected), Err(result)) => {
                    // can't compare the DataFusionError directly
                    let result = result.to_string();
                    let expected = expected.to_string();
                    assert_contains!(result.to_string(), expected);
                }
                (expected_result, ordering_result) => {
                    panic!(
                        "expected: {expected_result:#?}\n\nactual:{ordering_result:#?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn read_empty_table() -> Result<()> {
        let ctx = SessionContext::new();
        let path = String::from("table/p1=v1/file.avro");
        register_test_store(&ctx, &[(&path, 100)]);

        let opt = ListingOptions::new(Arc::new(AvroFormat {}))
            .with_file_extension(FileType::AVRO.get_ext())
            .with_table_partition_cols(vec![(String::from("p1"), DataType::Utf8)])
            .with_target_partitions(4);

        let table_path = ListingTableUrl::parse("test:///table/").unwrap();
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(file_schema);
        let table = ListingTable::try_new(config)?;

        assert_eq!(
            columns(&table.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        // this will filter out the only file in the store
        let filter = Expr::not_eq(col("p1"), lit("v1"));

        let scan = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("Empty execution plan");

        assert!(scan.as_any().is::<EmptyExec>());
        assert_eq!(
            columns(&scan.schema()),
            vec!["a".to_owned(), "p1".to_owned()]
        );

        Ok(())
    }

    #[tokio::test]
    async fn unbounded_csv_table_without_schema() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("dummy.csv");
        File::create(file_path)?;
        let ctx = SessionContext::new();
        let error = ctx
            .register_csv(
                "test",
                tmp_dir.path().to_str().unwrap(),
                CsvReadOptions::new().mark_infinite(true),
            )
            .await
            .unwrap_err();
        match error {
            DataFusionError::Plan(_) => Ok(()),
            val => Err(val),
        }
    }

    #[tokio::test]
    async fn unbounded_json_table_without_schema() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("dummy.json");
        File::create(file_path)?;
        let ctx = SessionContext::new();
        let error = ctx
            .register_json(
                "test",
                tmp_dir.path().to_str().unwrap(),
                NdJsonReadOptions::default().mark_infinite(true),
            )
            .await
            .unwrap_err();
        match error {
            DataFusionError::Plan(_) => Ok(()),
            val => Err(val),
        }
    }

    #[tokio::test]
    async fn unbounded_avro_table_without_schema() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("dummy.avro");
        File::create(file_path)?;
        let ctx = SessionContext::new();
        let error = ctx
            .register_avro(
                "test",
                tmp_dir.path().to_str().unwrap(),
                AvroReadOptions::default().mark_infinite(true),
            )
            .await
            .unwrap_err();
        match error {
            DataFusionError::Plan(_) => Ok(()),
            val => Err(val),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn unbounded_csv_table(
        #[values(true, false)] infinite_data: bool,
    ) -> Result<()> {
        let config = CsvReadOptions::new().mark_infinite(infinite_data);
        let session_config = SessionConfig::new().with_target_partitions(1);
        let listing_options = config.to_listing_options(&session_config);
        unbounded_table_helper(FileType::CSV, listing_options, infinite_data).await
    }

    #[rstest]
    #[tokio::test]
    async fn unbounded_json_table(
        #[values(true, false)] infinite_data: bool,
    ) -> Result<()> {
        let config = NdJsonReadOptions::default().mark_infinite(infinite_data);
        let session_config = SessionConfig::new().with_target_partitions(1);
        let listing_options = config.to_listing_options(&session_config);
        unbounded_table_helper(FileType::JSON, listing_options, infinite_data).await
    }

    #[rstest]
    #[tokio::test]
    async fn unbounded_avro_table(
        #[values(true, false)] infinite_data: bool,
    ) -> Result<()> {
        let config = AvroReadOptions::default().mark_infinite(infinite_data);
        let session_config = SessionConfig::new().with_target_partitions(1);
        let listing_options = config.to_listing_options(&session_config);
        unbounded_table_helper(FileType::AVRO, listing_options, infinite_data).await
    }

    #[tokio::test]
    async fn test_assert_list_files_for_scan_grouping() -> Result<()> {
        // more expected partitions than files
        assert_list_files_for_scan_grouping(
            &[
                "bucket/key-prefix/file0",
                "bucket/key-prefix/file1",
                "bucket/key-prefix/file2",
                "bucket/key-prefix/file3",
                "bucket/key-prefix/file4",
            ],
            "test:///bucket/key-prefix/",
            12,
            5,
        )
        .await?;

        // as many expected partitions as files
        assert_list_files_for_scan_grouping(
            &[
                "bucket/key-prefix/file0",
                "bucket/key-prefix/file1",
                "bucket/key-prefix/file2",
                "bucket/key-prefix/file3",
            ],
            "test:///bucket/key-prefix/",
            4,
            4,
        )
        .await?;

        // more files as expected partitions
        assert_list_files_for_scan_grouping(
            &[
                "bucket/key-prefix/file0",
                "bucket/key-prefix/file1",
                "bucket/key-prefix/file2",
                "bucket/key-prefix/file3",
                "bucket/key-prefix/file4",
            ],
            "test:///bucket/key-prefix/",
            2,
            2,
        )
        .await?;

        // no files => no groups
        assert_list_files_for_scan_grouping(&[], "test:///bucket/key-prefix/", 2, 0)
            .await?;

        // files that don't match the prefix
        assert_list_files_for_scan_grouping(
            &[
                "bucket/key-prefix/file0",
                "bucket/key-prefix/file1",
                "bucket/other-prefix/roguefile",
            ],
            "test:///bucket/key-prefix/",
            10,
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_assert_list_files_for_multi_path() -> Result<()> {
        // more expected partitions than files
        assert_list_files_for_multi_paths(
            &[
                "bucket/key1/file0",
                "bucket/key1/file1",
                "bucket/key1/file2",
                "bucket/key2/file3",
                "bucket/key2/file4",
                "bucket/key3/file5",
            ],
            &["test:///bucket/key1/", "test:///bucket/key2/"],
            12,
            5,
        )
        .await?;

        // as many expected partitions as files
        assert_list_files_for_multi_paths(
            &[
                "bucket/key1/file0",
                "bucket/key1/file1",
                "bucket/key1/file2",
                "bucket/key2/file3",
                "bucket/key2/file4",
                "bucket/key3/file5",
            ],
            &["test:///bucket/key1/", "test:///bucket/key2/"],
            5,
            5,
        )
        .await?;

        // more files as expected partitions
        assert_list_files_for_multi_paths(
            &[
                "bucket/key1/file0",
                "bucket/key1/file1",
                "bucket/key1/file2",
                "bucket/key2/file3",
                "bucket/key2/file4",
                "bucket/key3/file5",
            ],
            &["test:///bucket/key1/"],
            2,
            2,
        )
        .await?;

        // no files => no groups
        assert_list_files_for_multi_paths(&[], &["test:///bucket/key1/"], 2, 0).await?;

        // files that don't match the prefix
        assert_list_files_for_multi_paths(
            &[
                "bucket/key1/file0",
                "bucket/key1/file1",
                "bucket/key1/file2",
                "bucket/key2/file3",
                "bucket/key2/file4",
                "bucket/key3/file5",
            ],
            &["test:///bucket/key3/"],
            2,
            1,
        )
        .await?;
        Ok(())
    }

    async fn load_table(
        ctx: &SessionContext,
        name: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{testdata}/{name}");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let config = ListingTableConfig::new(table_path)
            .infer(&ctx.state())
            .await?;
        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_scan_grouping(
        files: &[&str],
        table_prefix: &str,
        target_partitions: usize,
        output_partitioning: usize,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let format = AvroFormat {};

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension("")
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_path = ListingTableUrl::parse(table_prefix).unwrap();
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let (file_list, _) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    /// Check that the files listed by the table match the specified `output_partitioning`
    /// when the object store contains `files`.
    async fn assert_list_files_for_multi_paths(
        files: &[&str],
        table_prefix: &[&str],
        target_partitions: usize,
        output_partitioning: usize,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let format = AvroFormat {};

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension("")
            .with_target_partitions(target_partitions);

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table_paths = table_prefix
            .iter()
            .map(|t| ListingTableUrl::parse(t).unwrap())
            .collect();
        let config = ListingTableConfig::new_with_multi_paths(table_paths)
            .with_listing_options(opt)
            .with_schema(Arc::new(schema));

        let table = ListingTable::try_new(config)?;

        let (file_list, _) = table.list_files_for_scan(&ctx.state(), &[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    #[test]
    fn test_statistics_cache() {
        let meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: DateTime::parse_from_rfc3339("2022-09-27T22:36:00+02:00")
                .unwrap()
                .into(),
            size: 1024,
            e_tag: None,
        };

        let cache = StatisticsCache::default();
        assert!(cache.get(&meta).is_none());

        cache.save(meta.clone(), Statistics::default());
        assert!(cache.get(&meta).is_some());

        // file size changed
        let mut meta2 = meta.clone();
        meta2.size = 2048;
        assert!(cache.get(&meta2).is_none());

        // file last_modified changed
        let mut meta2 = meta.clone();
        meta2.last_modified = DateTime::parse_from_rfc3339("2022-09-27T22:40:00+02:00")
            .unwrap()
            .into();
        assert!(cache.get(&meta2).is_none());

        // different file
        let mut meta2 = meta;
        meta2.location = Path::from("test2");
        assert!(cache.get(&meta2).is_none());
    }

    #[tokio::test]
    async fn test_insert_into_append_to_json_file() -> Result<()> {
        helper_test_insert_into_append_to_existing_files(
            FileType::JSON,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_json_files() -> Result<()> {
        helper_test_append_new_files_to_table(
            FileType::JSON,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_to_csv_file() -> Result<()> {
        helper_test_insert_into_append_to_existing_files(
            FileType::CSV,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_csv_files() -> Result<()> {
        helper_test_append_new_files_to_table(
            FileType::CSV,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_defaults() -> Result<()> {
        helper_test_append_new_files_to_table(
            FileType::PARQUET,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults() -> Result<()> {
        helper_test_insert_into_sql(
            "csv",
            FileCompressionType::UNCOMPRESSED,
            "OPTIONS (insert_mode 'append_new_files')",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults_header_row() -> Result<()> {
        helper_test_insert_into_sql(
            "csv",
            FileCompressionType::UNCOMPRESSED,
            "WITH HEADER ROW \
            OPTIONS (insert_mode 'append_new_files')",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_json_defaults() -> Result<()> {
        helper_test_insert_into_sql(
            "json",
            FileCompressionType::UNCOMPRESSED,
            "OPTIONS (insert_mode 'append_new_files')",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_defaults() -> Result<()> {
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            None,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_parquet_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.staistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_enabled".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        helper_test_insert_into_sql(
            "parquet",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(config_map),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_session_overrides() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd(5)".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_enabled".into(),
            "false".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.dictionary_page_size_limit".into(),
            "100".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.staistics_enabled".into(),
            "none".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_statistics_size".into(),
            "10".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.max_row_group_size".into(),
            "5".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.created_by".into(),
            "datafusion test".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.column_index_truncate_length".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.data_page_row_count_limit".into(),
            "50".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.encoding".into(),
            "delta_binary_packed".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_enabled".into(),
            "true".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_fpp".into(),
            "0.01".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.bloom_filter_ndv".into(),
            "1000".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.writer_version".into(),
            "2.0".into(),
        );
        config_map.insert(
            "datafusion.execution.parquet.write_batch_size".into(),
            "5".into(),
        );
        helper_test_append_new_files_to_table(
            FileType::PARQUET,
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_parquet_files_invalid_session_fails(
    ) -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert(
            "datafusion.execution.parquet.compression".into(),
            "zstd".into(),
        );
        let e = helper_test_append_new_files_to_table(
            FileType::PARQUET,
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
        )
        .await
        .expect_err("Example should fail!");
        assert_eq!("Error during planning: zstd compression requires specifying a level such as zstd(4)", format!("{e}"));
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_to_parquet_file_fails() -> Result<()> {
        let maybe_err = helper_test_insert_into_append_to_existing_files(
            FileType::PARQUET,
            FileCompressionType::UNCOMPRESSED,
            None,
        )
        .await;
        let _err =
            maybe_err.expect_err("Appending to existing parquet file did not fail!");
        Ok(())
    }

    fn load_empty_schema_table(
        schema: SchemaRef,
        temp_path: &str,
        insert_mode: ListingTableInsertMode,
        file_format: Arc<dyn FileFormat>,
    ) -> Result<Arc<dyn TableProvider>> {
        File::create(temp_path)?;
        let table_path = ListingTableUrl::parse(temp_path).unwrap();

        let listing_options =
            ListingOptions::new(file_format.clone()).with_insert_mode(insert_mode);

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }

    /// Logic of testing inserting into listing table by Appending to existing files
    /// is the same for all formats/options which support this. This helper allows
    /// passing different options to execute the same test with different settings.
    async fn helper_test_insert_into_append_to_existing_files(
        file_type: FileType,
        file_compression_type: FileCompressionType,
        session_config_map: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // Create the initial context, schema, and batch.
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(cfg)?;
                SessionContext::with_config(config)
            }
            None => SessionContext::new(),
        };
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new(
            "column1",
            DataType::Int32,
            false,
        )]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )?;

        // Filename with extension
        let filename = format!(
            "path{}",
            file_type
                .to_owned()
                .get_ext_with_compression(file_compression_type)
                .unwrap()
        );

        // Create a temporary directory and a CSV file within it.
        let tmp_dir = TempDir::new()?;
        let path = tmp_dir.path().join(filename);

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => Arc::new(
                CsvFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::JSON => Arc::new(
                JsonFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat {}),
            FileType::ARROW => Arc::new(ArrowFormat {}),
        };

        let initial_table = load_empty_schema_table(
            schema.clone(),
            path.to_str().unwrap(),
            ListingTableInsertMode::AppendToFile,
            file_format,
        )?;
        session_ctx.register_table("t", initial_table)?;
        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?.build()?;
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", &schema, false)?.build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 6     |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &res);

        // Read the records in the table
        let batches = session_ctx.sql("select * from t").await?.collect().await?;

        // Define the expected result as a vector of strings.
        let expected = [
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "+---------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that only 1 file was added to the table
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, 1);

        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Again, execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 6     |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &res);

        // Open the CSV file, read its contents as a record batch, and collect the batches into a vector.
        let batches = session_ctx.sql("select * from t").await?.collect().await?;

        // Define the expected result after the second append.
        let expected = vec![
            "+---------+",
            "| column1 |",
            "+---------+",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "| 1       |",
            "| 2       |",
            "| 3       |",
            "+---------+",
        ];

        // Assert that the batches read from the file after the second append match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that no additional files were added to the table
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, 1);

        // Return Ok if the function
        Ok(())
    }

    async fn helper_test_append_new_files_to_table(
        file_type: FileType,
        file_compression_type: FileCompressionType,
        session_config_map: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // Create the initial context, schema, and batch.
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(cfg)?;
                SessionContext::with_config(config)
            }
            None => SessionContext::new(),
        };

        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new(
            "column1",
            DataType::Int32,
            false,
        )]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2, 3]))],
        )?;

        // Register appropriate table depending on file_type we want to test
        let tmp_dir = TempDir::new()?;
        match file_type {
            FileType::CSV => {
                session_ctx
                    .register_csv(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        CsvReadOptions::new()
                            .insert_mode(ListingTableInsertMode::AppendNewFiles)
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            FileType::JSON => {
                session_ctx
                    .register_json(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        NdJsonReadOptions::default()
                            .insert_mode(ListingTableInsertMode::AppendNewFiles)
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            FileType::PARQUET => {
                session_ctx
                    .register_parquet(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ParquetReadOptions::default()
                            .insert_mode(ListingTableInsertMode::AppendNewFiles)
                            .schema(schema.as_ref()),
                    )
                    .await?;
            }
            FileType::AVRO => {
                session_ctx
                    .register_avro(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        AvroReadOptions::default()
                            // TODO implement insert_mode for avro
                            //.insert_mode(ListingTableInsertMode::AppendNewFiles)
                            .schema(schema.as_ref()),
                    )
                    .await?;
            }
            FileType::ARROW => {
                session_ctx
                    .register_arrow(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ArrowReadOptions::default()
                            // TODO implement insert_mode for arrow
                            //.insert_mode(ListingTableInsertMode::AppendNewFiles)
                            .schema(schema.as_ref()),
                    )
                    .await?;
            }
        }

        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?
            .repartition(Partitioning::Hash(vec![Expr::Column("column1".into())], 6))?
            .build()?;
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", &schema, false)?.build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 6     |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &res);

        // Read the records in the table
        let batches = session_ctx
            .sql("select count(*) as count from t")
            .await?
            .collect()
            .await?;
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 6     |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that 6 files were added to the table
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, 6);

        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Again, execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        // Insert returns the number of rows written, in our case this would be 6.
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 6     |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &res);

        // Read the contents of the table
        let batches = session_ctx
            .sql("select count(*) AS count from t")
            .await?
            .collect()
            .await?;

        // Define the expected result after the second append.
        let expected = [
            "+-------+",
            "| count |",
            "+-------+",
            "| 12    |",
            "+-------+",
        ];

        // Assert that the batches read from the file after the second append match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that another 6 files were added to the table
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, 12);

        // Return Ok if the function
        Ok(())
    }

    /// tests insert into with end to end sql
    /// create external table + insert into statements
    async fn helper_test_insert_into_sql(
        file_type: &str,
        // TODO test with create statement options such as compression
        _file_compression_type: FileCompressionType,
        external_table_options: &str,
        session_config_map: Option<HashMap<String, String>>,
    ) -> Result<()> {
        // Create the initial context
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(cfg)?;
                SessionContext::with_config(config)
            }
            None => SessionContext::new(),
        };

        // create table
        let tmp_dir = TempDir::new()?;
        let tmp_path = tmp_dir.into_path();
        let str_path = tmp_path.to_str().expect("Temp path should convert to &str");
        session_ctx
            .sql(&format!(
                "create external table foo(a varchar, b varchar, c int) \
                        stored as {file_type} \
                        location '{str_path}' \
                        {external_table_options}"
            ))
            .await?
            .collect()
            .await?;

        // insert data
        session_ctx.sql("insert into foo values ('foo', 'bar', 1),('foo', 'bar', 2), ('foo', 'bar', 3)")
           .await?
           .collect()
           .await?;

        // check count
        let batches = session_ctx
            .sql("select * from foo")
            .await?
            .collect()
            .await?;

        let expected = [
            "+-----+-----+---+",
            "| a   | b   | c |",
            "+-----+-----+---+",
            "| foo | bar | 1 |",
            "| foo | bar | 2 |",
            "| foo | bar | 3 |",
            "+-----+-----+---+",
        ];
        assert_batches_eq!(expected, &batches);

        Ok(())
    }
}
