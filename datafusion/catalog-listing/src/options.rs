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

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_catalog::Session;
use datafusion_common::plan_err;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::file_format::FileFormat;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::SortExpr;
use futures::StreamExt;
use futures::{TryStreamExt, future};
use itertools::Itertools;
use std::sync::Arc;

/// Options for creating a [`crate::ListingTable`]
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
    ///
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
    /// - do not collect statistics
    pub fn new(format: Arc<dyn FileFormat>) -> Self {
        Self {
            file_extension: format.get_ext(),
            format,
            table_partition_cols: vec![],
            collect_stat: false,
            target_partitions: 1,
            file_sort_order: vec![],
        }
    }

    /// Set options from [`SessionConfig`] and returns self.
    ///
    /// Currently this sets `target_partitions` and `collect_stat`
    /// but if more options are added in the future that need to be coordinated
    /// they will be synchronized through this method.
    pub fn with_session_config_options(mut self, config: &SessionConfig) -> Self {
        self = self.with_target_partitions(config.target_partitions());
        self = self.with_collect_stat(config.collect_statistics());
        self
    }

    /// Set file extension on [`ListingOptions`] and returns self.
    ///
    /// # Example
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    ///
    /// let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    ///     .with_file_extension(".parquet");
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
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    ///
    /// let extension = Some(".parquet");
    /// let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    ///     .with_file_extension_opt(extension);
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
    /// A [`crate::ListingTable`] created at `/mnt/nyctaxi/` with partition
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
    /// # use datafusion_expr::col;
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
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
    /// [`wrap_partition_type_in_dict`]: datafusion_datasource::file_scan_config::wrap_partition_type_in_dict
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
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    ///
    /// let listing_options =
    ///     ListingOptions::new(Arc::new(ParquetFormat::default())).with_collect_stat(true);
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
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    ///
    /// let listing_options =
    ///     ListingOptions::new(Arc::new(ParquetFormat::default())).with_target_partitions(8);
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
    /// # use datafusion_expr::col;
    /// # use datafusion_catalog_listing::ListingOptions;
    /// # use datafusion_datasource_parquet::file_format::ParquetFormat;
    ///
    /// // Tell datafusion that the files are sorted by column "a"
    /// let file_sort_order = vec![vec![col("a").sort(true, true)]];
    ///
    /// let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
    ///     .with_file_sort_order(file_sort_order.clone());
    ///
    /// assert_eq!(listing_options.file_sort_order, file_sort_order);
    /// ```
    pub fn with_file_sort_order(mut self, file_sort_order: Vec<Vec<SortExpr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Infer the schema of the files at the given path on the provided object store.
    ///
    /// If the table_path contains one or more files (i.e. it is a directory /
    /// prefix of files) their schema is merged by calling [`FileFormat::infer_schema`]
    ///
    /// Note: The inferred schema does not include any partitioning columns.
    ///
    /// This method is called as part of creating a [`crate::ListingTable`].
    pub async fn infer_schema<'a>(
        &'a self,
        state: &dyn Session,
        table_path: &'a ListingTableUrl,
    ) -> datafusion_common::Result<SchemaRef> {
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
    ) -> datafusion_common::Result<()> {
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
    pub async fn infer_partitions(
        &self,
        state: &dyn Session,
        table_path: &ListingTableUrl,
    ) -> datafusion_common::Result<Vec<String>> {
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
                    // Partitions are expected to follow the format "column_name=value", so we
                    // should ignore any path part that cannot be parsed into the expected format
                    .filter(|s| s.contains('='))
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
