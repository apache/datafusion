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

use std::collections::HashMap;
use std::{any::Any, str::FromStr, sync::Arc};

use super::helpers::{expr_applicable_for_cols, pruned_partition_list, split_files};
use super::{ListingTableUrl, PartitionedFile};

use crate::datasource::{
    create_ordering,
    file_format::{
        file_compression_type::FileCompressionType, FileFormat, FilePushdownSupport,
    },
    get_statistics_with_limit,
    physical_plan::{FileScanConfig, FileSinkConfig},
};
use crate::execution::context::SessionState;
use datafusion_catalog::TableProvider;
use datafusion_common::{config_err, DataFusionError, Result};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{utils::conjunction, Expr, TableProviderFilterPushDown};
use datafusion_expr::{SortExpr, TableType};
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::{ExecutionPlan, Statistics};

use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use datafusion_common::{
    config_datafusion_err, internal_err, plan_err, project_schema, Constraints,
    SchemaExt, ToDFSchema,
};
use datafusion_execution::cache::{
    cache_manager::FileStatisticsCache, cache_unit::DefaultFileStatisticsCache,
};
use datafusion_physical_expr::{
    create_physical_expr, LexOrdering, PhysicalSortRequirement,
};

use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use futures::{future, stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectStore;

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

    ///Returns a tupe of (file_extension, optional compression_extension)
    ///
    /// For example a path ending with blah.test.csv.gz returns `("csv", Some("gz"))`
    /// For example a path ending with blah.test.csv returns `("csv", None)`
    fn infer_file_extension_and_compression_type(
        path: &str,
    ) -> Result<(String, Option<String>)> {
        let mut exts = path.rsplit('.');

        let splitted = exts.next().unwrap_or("");

        let file_compression_type = FileCompressionType::from_str(splitted)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        if file_compression_type.is_compressed() {
            let splitted2 = exts.next().unwrap_or("");
            Ok((splitted2.to_string(), Some(splitted.to_string())))
        } else {
            Ok((splitted.to_string(), None))
        }
    }

    /// Infer `ListingOptions` based on `table_path` suffix.
    pub async fn infer_options(self, state: &dyn Session) -> Result<Self> {
        let store = if let Some(url) = self.table_paths.first() {
            state.runtime_env().object_store(url)?
        } else {
            return Ok(self);
        };

        let file = self
            .table_paths
            .first()
            .unwrap()
            .list_all_files(state, store.as_ref(), "")
            .await?
            .next()
            .await
            .ok_or_else(|| DataFusionError::Internal("No files for table".into()))??;

        let (file_extension, maybe_compression_type) =
            ListingTableConfig::infer_file_extension_and_compression_type(
                file.location.as_ref(),
            )?;

        let mut format_options = HashMap::new();
        if let Some(ref compression_type) = maybe_compression_type {
            format_options
                .insert("format.compression".to_string(), compression_type.clone());
        }
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_format = state
            .get_file_format_factory(&file_extension)
            .ok_or(config_datafusion_err!(
                "No file_format found with extension {file_extension}"
            ))?
            .create(state, &format_options)?;

        let listing_file_extension =
            if let Some(compression_type) = maybe_compression_type {
                format!("{}.{}", &file_extension, &compression_type)
            } else {
                file_extension
            };

        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(listing_file_extension)
            .with_target_partitions(state.config().target_partitions());

        Ok(Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
        })
    }

    /// Infer the [`SchemaRef`] based on `table_path` suffix.  Requires `self.options` to be set prior to using.
    pub async fn infer_schema(self, state: &dyn Session) -> Result<Self> {
        match self.options {
            Some(options) => {
                let schema = if let Some(url) = self.table_paths.first() {
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
    pub async fn infer(self, state: &dyn Session) -> Result<Self> {
        self.infer_options(state).await?.infer_schema(state).await
    }

    /// Infer the partition columns from the path. Requires `self.options` to be set prior to using.
    pub async fn infer_partitions_from_path(self, state: &dyn Session) -> Result<Self> {
        match self.options {
            Some(options) => {
                let Some(url) = self.table_paths.first() else {
                    return config_err!("No table path found");
                };
                let partitions = options
                    .infer_partitions(state, url)
                    .await?
                    .into_iter()
                    .map(|col_name| {
                        (
                            col_name,
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>();
                let options = options.with_table_partition_cols(partitions);
                Ok(Self {
                    table_paths: self.table_paths,
                    file_schema: self.file_schema,
                    options: Some(options),
                })
            }
            None => config_err!("No `ListingOptions` set for inferring schema"),
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
    /// See <https://github.com/apache/datafusion/issues/4177>
    /// NOTE: This attribute stores all equivalent orderings (the outer `Vec`)
    ///       where each ordering consists of an individual lexicographic
    ///       ordering (encapsulated by a `Vec<Expr>`). If there aren't
    ///       multiple equivalent orderings, the outer `Vec` will have a
    ///       single element.
    pub file_sort_order: Vec<Vec<SortExpr>>,
}

impl ListingOptions {
    /// Creates an options instance with the given format
    /// Default values:
    /// - use default file extension filter
    /// - no input partition to discover
    /// - one target partition
    /// - stat collection
    pub fn new(format: Arc<dyn FileFormat>) -> Self {
        Self {
            file_extension: format.get_ext(),
            format,
            table_partition_cols: vec![],
            collect_stat: true,
            target_partitions: 1,
            file_sort_order: vec![],
        }
    }

    /// Set file extension on [`ListingOptions`] and returns self.
    ///
    /// # Example
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

    /// Optionally set file extension on [`ListingOptions`] and returns self.
    ///
    /// If `file_extension` is `None`, the file extension will not be changed
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    /// let extension = Some(".parquet");
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::default()
    ///   ))
    ///   .with_file_extension_opt(extension);
    ///
    /// assert_eq!(listing_options.file_extension, ".parquet");
    /// ```
    pub fn with_file_extension_opt<S>(mut self, file_extension: Option<S>) -> Self
    where
        S: Into<String>,
    {
        if let Some(file_extension) = file_extension {
            self.file_extension = file_extension.into();
        }
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
    ///   specified, the other levels are ignored but the files are
    ///   still read.
    ///
    /// - Files that don't follow this partitioning scheme will be
    ///   ignored.
    ///
    /// - Since the columns have the same value for all rows read from
    ///   each individual file (such as dates), they are typically
    ///   dictionary encoded for efficiency. You may use
    ///   [`wrap_partition_type_in_dict`] to request a
    ///   dictionary-encoded type.
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
    /// # use datafusion_datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
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
    pub fn with_file_sort_order(mut self, file_sort_order: Vec<Vec<SortExpr>>) -> Self {
        self.file_sort_order = file_sort_order;
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
        state: &dyn Session,
        table_path: &'a ListingTableUrl,
    ) -> Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let files: Vec<_> = table_path
            .list_all_files(state, store.as_ref(), &self.file_extension)
            .await?
            // Empty files cannot affect schema but may throw when trying to read for it
            .try_filter(|object_meta| future::ready(object_meta.size > 0))
            .try_collect()
            .await?;

        let schema = self.format.infer_schema(state, &store, &files).await?;

        Ok(schema)
    }

    /// Infers the partition columns stored in `LOCATION` and compares
    /// them with the columns provided in `PARTITIONED BY` to help prevent
    /// accidental corrupts of partitioned tables.
    ///
    /// Allows specifying partial partitions.
    pub async fn validate_partitions(
        &self,
        state: &dyn Session,
        table_path: &ListingTableUrl,
    ) -> Result<()> {
        if self.table_partition_cols.is_empty() {
            return Ok(());
        }

        if !table_path.is_collection() {
            return plan_err!(
                "Can't create a partitioned table backed by a single file, \
                perhaps the URL is missing a trailing slash?"
            );
        }

        let inferred = self.infer_partitions(state, table_path).await?;

        // no partitioned files found on disk
        if inferred.is_empty() {
            return Ok(());
        }

        let table_partition_names = self
            .table_partition_cols
            .iter()
            .map(|(col_name, _)| col_name.clone())
            .collect_vec();

        if inferred.len() < table_partition_names.len() {
            return plan_err!(
                "Inferred partitions to be {:?}, but got {:?}",
                inferred,
                table_partition_names
            );
        }

        // match prefix to allow creating tables with partial partitions
        for (idx, col) in table_partition_names.iter().enumerate() {
            if &inferred[idx] != col {
                return plan_err!(
                    "Inferred partitions to be {:?}, but got {:?}",
                    inferred,
                    table_partition_names
                );
            }
        }

        Ok(())
    }

    /// Infer the partitioning at the given path on the provided object store.
    /// For performance reasons, it doesn't read all the files on disk
    /// and therefore may fail to detect invalid partitioning.
    pub(crate) async fn infer_partitions(
        &self,
        state: &dyn Session,
        table_path: &ListingTableUrl,
    ) -> Result<Vec<String>> {
        let store = state.runtime_env().object_store(table_path)?;

        // only use 10 files for inference
        // This can fail to detect inconsistent partition keys
        // A DFS traversal approach of the store can help here
        let files: Vec<_> = table_path
            .list_all_files(state, store.as_ref(), &self.file_extension)
            .await?
            .take(10)
            .try_collect()
            .await?;

        let stripped_path_parts = files.iter().map(|file| {
            table_path
                .strip_prefix(&file.location)
                .unwrap()
                .collect_vec()
        });

        let partition_keys = stripped_path_parts
            .map(|path_parts| {
                path_parts
                    .into_iter()
                    .rev()
                    .skip(1) // get parents only; skip the file itself
                    .rev()
                    .map(|s| s.split('=').take(1).collect())
                    .collect_vec()
            })
            .collect_vec();

        match partition_keys.into_iter().all_equal_value() {
            Ok(v) => Ok(v),
            Err(None) => Ok(vec![]),
            Err(Some(diff)) => {
                let mut sorted_diff = [diff.0, diff.1];
                sorted_diff.sort();
                plan_err!("Found mixed partition values on disk {:?}", sorted_diff)
            }
        }
    }
}

/// Reads data from one or more files as a single table.
///
/// Implements [`TableProvider`], a DataFusion data source. The files are read
/// using an  [`ObjectStore`] instance, for example from local files or objects
/// from AWS S3.
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
/// `ListingTable` also supports filter and projection pushdown for formats that
/// support it as such as Parquet.
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
#[derive(Debug)]
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    /// File fields only
    file_schema: SchemaRef,
    /// File fields + partition columns
    table_schema: SchemaRef,
    options: ListingOptions,
    definition: Option<String>,
    collected_statistics: FileStatisticsCache,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
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
            options,
            definition: None,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
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
            cache.unwrap_or(Arc::new(DefaultFileStatisticsCache::default()));
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

    /// If file_sort_order is specified, creates the appropriate physical expressions
    fn try_create_output_ordering(&self) -> Result<Vec<LexOrdering>> {
        create_ordering(&self.table_schema, &self.options.file_sort_order)
    }
}

// Expressions can be used for parttion pruning if they can be evaluated using
// only the partiton columns and there are partition columns.
fn can_be_evaluted_for_partition_pruning(
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // extract types of partition columns
        let table_partition_cols = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| Ok(self.table_schema.field_with_name(&col.0)?.clone()))
            .collect::<Result<Vec<_>>>()?;

        let table_partition_col_names = table_partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        // If the filters can be resolved using only partition cols, there is no need to
        // pushdown it to TableScan, otherwise, `unhandled` pruning predicates will be generated
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });
        // TODO (https://github.com/apache/datafusion/issues/11600) remove downcast_ref from here?
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();

        // We should not limit the number of partitioned files to scan if there are filters and limit
        // at the same time. This is because the limit should be applied after the filters are applied.
        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let (mut partitioned_file_lists, statistics) = self
            .list_files_for_scan(session_state, &partition_filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let output_ordering = self.try_create_output_ordering()?;
        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics(
                        &self.table_schema,
                        &partitioned_file_lists,
                        output_ordering,
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

        let filters = match conjunction(filters.to_vec()) {
            Some(expr) => {
                let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
                let filters = create_physical_expr(
                    &expr,
                    &table_df_schema,
                    state.execution_props(),
                )?;
                Some(filters)
            }
            None => None,
        };

        let Some(object_store_url) =
            self.table_paths.first().map(ListingTableUrl::object_store)
        else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        // create the execution plan
        self.options
            .format
            .create_physical_plan(
                session_state,
                FileScanConfig::new(
                    object_store_url,
                    Arc::clone(&self.file_schema),
                    self.options.format.file_source(),
                )
                .with_file_groups(partitioned_file_lists)
                .with_constraints(self.constraints.clone())
                .with_statistics(statistics)
                .with_projection(projection.cloned())
                .with_limit(limit)
                .with_output_ordering(output_ordering)
                .with_table_partition_cols(table_partition_cols),
                filters.as_ref(),
            )
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let partition_column_names = self
            .options
            .table_partition_cols
            .iter()
            .map(|col| col.0.as_str())
            .collect::<Vec<_>>();
        filters
            .iter()
            .map(|filter| {
                if can_be_evaluted_for_partition_pruning(&partition_column_names, filter)
                {
                    // if filter can be handled by partition pruning, it is exact
                    return Ok(TableProviderFilterPushDown::Exact);
                }

                // if we can't push it down completely with only the filename-based/path-based
                // column names, then we should check if we can do parquet predicate pushdown
                let supports_pushdown = self.options.format.supports_filters_pushdown(
                    &self.file_schema,
                    &self.table_schema,
                    &[filter],
                )?;

                if supports_pushdown == FilePushdownSupport::Supported {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
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

        // TODO (https://github.com/apache/datafusion/issues/11600) remove downcast_ref from here?
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();
        let file_list_stream = pruned_partition_list(
            session_state,
            store.as_ref(),
            table_path,
            &[],
            &self.options.file_extension,
            &self.options.table_partition_cols,
        )
        .await?;

        let file_groups = file_list_stream.try_collect::<Vec<_>>().await?;
        let keep_partition_by_columns =
            state.config_options().execution.keep_partition_by_columns;

        // Sink related option, apart from format
        let config = FileSinkConfig {
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_groups,
            output_schema: self.schema(),
            table_partition_cols: self.options.table_partition_cols.clone(),
            insert_op,
            keep_partition_by_columns,
            file_extension: self.options().format.get_ext(),
        };

        let order_requirements = if !self.options().file_sort_order.is_empty() {
            // Multiple sort orders in outer vec are equivalent, so we pass only the first one
            let orderings = self.try_create_output_ordering()?;
            let Some(ordering) = orderings.first() else {
                return internal_err!(
                    "Expected ListingTable to have a sort order, but none found!"
                );
            };
            // Converts Vec<Vec<SortExpr>> into type required by execution plan to specify its required input ordering
            Some(LexRequirement::new(
                ordering
                    .into_iter()
                    .cloned()
                    .map(PhysicalSortRequirement::from)
                    .collect::<Vec<_>>(),
            ))
        } else {
            None
        };

        self.options()
            .format
            .create_writer_physical_plan(input, session_state, config, order_requirements)
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
    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
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
        let file_list = stream::iter(file_list).flatten();
        // collect the statistics if required by the config
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                if self.options.collect_stat {
                    let statistics =
                        self.do_collect_statistics(ctx, &store, &part_file).await?;
                    Ok((part_file, statistics))
                } else {
                    Ok((
                        part_file,
                        Arc::new(Statistics::new_unknown(&self.file_schema)),
                    ))
                }
            })
            .boxed()
            .buffered(ctx.config_options().execution.meta_fetch_concurrency);

        let (files, statistics) = get_statistics_with_limit(
            files,
            self.schema(),
            limit,
            self.options.collect_stat,
        )
        .await?;

        Ok((
            split_files(files, self.options.target_partitions),
            statistics,
        ))
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
    ) -> Result<Arc<Statistics>> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::avro::AvroFormat;
    use crate::datasource::file_format::csv::CsvFormat;
    use crate::datasource::file_format::json::JsonFormat;
    #[cfg(feature = "parquet")]
    use crate::datasource::file_format::parquet::ParquetFormat;
    use crate::datasource::{provider_as_source, DefaultTableSource, MemTable};
    use crate::execution::options::ArrowReadOptions;
    use crate::prelude::*;
    use crate::{
        assert_batches_eq,
        test::{columns, object_store::register_test_store},
    };
    use datafusion_physical_plan::collect;

    use arrow::compute::SortOptions;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::stats::Precision;
    use datafusion_common::{assert_contains, ScalarValue};
    use datafusion_expr::{BinaryExpr, LogicalPlanBuilder, Operator};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::ExecutionPlanProperties;

    use tempfile::TempDir;

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
        assert_eq!(exec.statistics()?.num_rows, Precision::Exact(8));
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Exact(671));

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn load_table_stats_by_default() -> Result<()> {
        use crate::datasource::file_format::parquet::ParquetFormat;

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
        assert_eq!(exec.statistics()?.num_rows, Precision::Exact(8));
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Exact(671));

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn load_table_stats_when_no_stats() -> Result<()> {
        use crate::datasource::file_format::parquet::ParquetFormat;

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
        assert_eq!(exec.statistics()?.num_rows, Precision::Absent);
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Absent);

        Ok(())
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn test_try_create_output_ordering() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let schema = options.infer_schema(&state, &table_path).await.unwrap();

        use crate::datasource::file_format::parquet::ParquetFormat;
        use datafusion_physical_plan::expressions::col as physical_col;
        use std::ops::Add;

        // (file_sort_order, expected_result)
        let cases = vec![
            (vec![], Ok(vec![])),
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
                Ok(vec![LexOrdering::new(
                        vec![PhysicalSortExpr {
                            expr: physical_col("string_col", &schema).unwrap(),
                            options: SortOptions {
                                descending: false,
                                nulls_first: false,
                            },
                        }],
                )
                ])
            ),
            // ok with two columns, different options
            (
                vec![vec![
                    col("string_col").sort(true, false),
                    col("int_col").sort(false, true),
                ]],
                Ok(vec![LexOrdering::new(
                        vec![
                            PhysicalSortExpr::new_default(physical_col("string_col", &schema).unwrap())
                                        .asc()
                                        .nulls_last(),
                            PhysicalSortExpr::new_default(physical_col("int_col", &schema).unwrap())
                                        .desc()
                                        .nulls_first()
                        ],
                )
                ])
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
            .with_file_extension(AvroFormat.get_ext())
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
            Some(""),
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
            Some(""),
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
            Some(""),
        )
        .await?;

        // no files => no groups
        assert_list_files_for_scan_grouping(
            &[],
            "test:///bucket/key-prefix/",
            2,
            0,
            Some(""),
        )
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
            Some(""),
        )
        .await?;

        // files that don't match the prefix or the default file extention
        assert_list_files_for_scan_grouping(
            &[
                "bucket/key-prefix/file0.avro",
                "bucket/key-prefix/file1.parquet",
                "bucket/other-prefix/roguefile.avro",
            ],
            "test:///bucket/key-prefix/",
            10,
            1,
            None,
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
            Some(""),
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
            Some(""),
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
            Some(""),
        )
        .await?;

        // no files => no groups
        assert_list_files_for_multi_paths(&[], &["test:///bucket/key1/"], 2, 0, Some(""))
            .await?;

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
            Some(""),
        )
        .await?;

        // files that don't match the prefix or the default file ext
        assert_list_files_for_multi_paths(
            &[
                "bucket/key1/file0.avro",
                "bucket/key1/file1.csv",
                "bucket/key1/file2.avro",
                "bucket/key2/file3.csv",
                "bucket/key2/file4.avro",
                "bucket/key3/file5.csv",
            ],
            &["test:///bucket/key1/", "test:///bucket/key3/"],
            2,
            2,
            None,
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
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let format = AvroFormat {};

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension_opt(file_ext)
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
        file_ext: Option<&str>,
    ) -> Result<()> {
        let ctx = SessionContext::new();
        register_test_store(&ctx, &files.iter().map(|f| (*f, 10)).collect::<Vec<_>>());

        let format = AvroFormat {};

        let opt = ListingOptions::new(Arc::new(format))
            .with_file_extension_opt(file_ext)
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

    #[tokio::test]
    async fn test_insert_into_append_new_json_files() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
        helper_test_append_new_files_to_table(
            JsonFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_new_csv_files() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
        helper_test_append_new_files_to_table(
            CsvFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_2_new_parquet_files_defaults() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
        helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_append_1_new_parquet_files_defaults() -> Result<()> {
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("datafusion.execution.batch_size".into(), "20".into());
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "20".into(),
        );
        helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            1,
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults() -> Result<()> {
        helper_test_insert_into_sql("csv", FileCompressionType::UNCOMPRESSED, "", None)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_csv_defaults_header_row() -> Result<()> {
        helper_test_insert_into_sql(
            "csv",
            FileCompressionType::UNCOMPRESSED,
            "",
            Some(HashMap::from([("has_header".into(), "true".into())])),
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_sql_json_defaults() -> Result<()> {
        helper_test_insert_into_sql("json", FileCompressionType::UNCOMPRESSED, "", None)
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
            "datafusion.execution.parquet.statistics_enabled".into(),
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
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
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
        config_map.insert("datafusion.execution.batch_size".into(), "10".into());
        config_map.insert(
            "datafusion.execution.soft_max_rows_per_output_file".into(),
            "10".into(),
        );
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
            "datafusion.execution.parquet.statistics_enabled".into(),
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
            "datafusion.execution.parquet.bloom_filter_on_write".into(),
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
        config_map.insert("datafusion.execution.batch_size".into(), "1".into());
        helper_test_append_new_files_to_table(
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
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
            ParquetFormat::default().get_ext(),
            FileCompressionType::UNCOMPRESSED,
            Some(config_map),
            2,
        )
        .await
        .expect_err("Example should fail!");
        assert_eq!(e.strip_backtrace(), "Invalid or Unsupported Configuration: zstd compression requires specifying a level such as zstd(4)");

        Ok(())
    }

    async fn helper_test_append_new_files_to_table(
        file_type_ext: String,
        file_compression_type: FileCompressionType,
        session_config_map: Option<HashMap<String, String>>,
        expected_n_files_per_insert: usize,
    ) -> Result<()> {
        // Create the initial context, schema, and batch.
        let session_ctx = match session_config_map {
            Some(cfg) => {
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
            }
            None => SessionContext::new(),
        };

        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new(
            "column1",
            DataType::Int32,
            false,
        )]));

        let filter_predicate = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(Expr::Column("column1".into())),
            Operator::GtEq,
            Box::new(Expr::Literal(ScalarValue::Int32(Some(0)))),
        ));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
            ]))],
        )?;

        // Register appropriate table depending on file_type we want to test
        let tmp_dir = TempDir::new()?;
        match file_type_ext.as_str() {
            "csv" => {
                session_ctx
                    .register_csv(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        CsvReadOptions::new()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            "json" => {
                session_ctx
                    .register_json(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        NdJsonReadOptions::default()
                            .schema(schema.as_ref())
                            .file_compression_type(file_compression_type),
                    )
                    .await?;
            }
            "parquet" => {
                session_ctx
                    .register_parquet(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ParquetReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            "avro" => {
                session_ctx
                    .register_avro(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        AvroReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            "arrow" => {
                session_ctx
                    .register_arrow(
                        "t",
                        tmp_dir.path().to_str().unwrap(),
                        ArrowReadOptions::default().schema(schema.as_ref()),
                    )
                    .await?;
            }
            _ => panic!("Unrecognized file extension {file_type_ext}"),
        }

        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(
            schema.clone(),
            vec![vec![batch.clone(), batch.clone()]],
        )?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        let target = session_ctx.table_provider("t").await?;
        let target = Arc::new(DefaultTableSource::new(target));
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?
            .filter(filter_predicate)?
            .build()?;
        // Since logical plan contains a filter, increasing parallelism is helpful.
        // Therefore, we will have 8 partitions in the final plan.
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", target, InsertOp::Append)?
                .build()?;
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
            "| 20    |",
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
            "| 20    |",
            "+-------+",
        ];

        // Assert that the batches read from the file match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert);

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
            "| 20    |",
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
            "| 40    |",
            "+-------+",
        ];

        // Assert that the batches read from the file after the second append match the expected result.
        assert_batches_eq!(expected, &batches);

        // Assert that another `target_partition_number` many files were added to the table.
        let num_files = tmp_dir.path().read_dir()?.count();
        assert_eq!(num_files, expected_n_files_per_insert * 2);

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
                let config = SessionConfig::from_string_hash_map(&cfg)?;
                SessionContext::new_with_config(config)
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

    #[tokio::test]
    async fn test_infer_options_compressed_csv() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/csv/aggregate_test_100.csv.gz", testdata);
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();

        let config = ListingTableConfig::new(table_path);
        let config_with_opts = config.infer_options(&ctx.state()).await?;
        let config_with_schema = config_with_opts.infer_schema(&ctx.state()).await?;

        let schema = config_with_schema.file_schema.unwrap();

        assert_eq!(schema.fields.len(), 13);

        Ok(())
    }
}
