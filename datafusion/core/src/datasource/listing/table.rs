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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_physical_expr::PhysicalSortExpr;
use futures::{future, stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectMeta;
use parking_lot::RwLock;

use crate::config::ConfigOptions;
use crate::datasource::file_format::file_type::{FileCompressionType, FileType};
use crate::datasource::{
    file_format::{
        avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat,
        FileFormat,
    },
    get_statistics_with_limit,
    listing::ListingTableUrl,
    TableProvider, TableType,
};
use crate::logical_expr::TableProviderFilterPushDown;
use crate::physical_plan;
use crate::physical_plan::file_format::partition_type_wrap;
use crate::{
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::{
        empty::EmptyExec, file_format::FileScanConfig, project_schema, ExecutionPlan,
        Statistics,
    },
};

use super::PartitionedFile;

use super::helpers::{expr_applicable_for_cols, pruned_partition_list, split_files};

/// Configuration for creating a 'ListingTable'
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
    /// Creates new `ListingTableConfig`.
    /// The `SchemaRef` and `ListingOptions` are inferred based on the suffix of the provided `table_paths` first element.
    pub fn new(table_path: ListingTableUrl) -> Self {
        let table_paths = vec![table_path];
        Self {
            table_paths,
            file_schema: None,
            options: None,
        }
    }

    /// Creates new `ListingTableConfig` with multiple table paths.
    /// The `SchemaRef` and `ListingOptions` are inferred based on the suffix of the provided `table_paths` first element.
    pub fn new_with_multi_paths(table_paths: Vec<ListingTableUrl>) -> Self {
        Self {
            table_paths,
            file_schema: None,
            options: None,
        }
    }
    /// Add `schema` to `ListingTableConfig`
    pub fn with_schema(self, schema: SchemaRef) -> Self {
        Self {
            table_paths: self.table_paths,
            file_schema: Some(schema),
            options: self.options,
        }
    }

    /// Add `listing_options` to `ListingTableConfig`
    pub fn with_listing_options(self, listing_options: ListingOptions) -> Self {
        Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
        }
    }

    fn infer_format(
        config_options: Arc<RwLock<ConfigOptions>>,
        path: &str,
    ) -> Result<(Arc<dyn FileFormat>, String)> {
        let err_msg = format!("Unable to infer file type from path: {}", path);

        let mut exts = path.rsplit('.');

        let mut splitted = exts.next().unwrap_or("");

        let file_compression_type = FileCompressionType::from_str(splitted)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        if file_compression_type != FileCompressionType::UNCOMPRESSED {
            splitted = exts.next().unwrap_or("");
        }

        let file_type = FileType::from_str(splitted)
            .map_err(|_| DataFusionError::Internal(err_msg.to_owned()))?;

        let ext = file_type
            .get_ext_with_compression(file_compression_type.to_owned())
            .map_err(|_| DataFusionError::Internal(err_msg))?;

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::AVRO => Arc::new(AvroFormat::default()),
            FileType::CSV => Arc::new(
                CsvFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::JSON => Arc::new(
                JsonFormat::default().with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::new(config_options)),
        };

        Ok((file_format, ext))
    }

    /// Infer `ListingOptions` based on `table_path` suffix.
    pub async fn infer_options(self, state: &SessionState) -> Result<Self> {
        let store = state
            .runtime_env
            .object_store(self.table_paths.get(0).unwrap())?;

        let file = self
            .table_paths
            .get(0)
            .unwrap()
            .list_all_files(store.as_ref(), "")
            .next()
            .await
            .ok_or_else(|| DataFusionError::Internal("No files for table".into()))??;

        let (format, file_extension) = ListingTableConfig::infer_format(
            state.config_options(),
            file.location.as_ref(),
        )?;

        let listing_options = ListingOptions::new(format)
            .with_file_extension(file_extension)
            .with_target_partitions(state.config.target_partitions());

        Ok(Self {
            table_paths: self.table_paths,
            file_schema: self.file_schema,
            options: Some(listing_options),
        })
    }

    /// Infer `SchemaRef` based on `table_path` suffix.  Requires `self.options` to be set prior to using.
    pub async fn infer_schema(self, ctx: &SessionState) -> Result<Self> {
        match self.options {
            Some(options) => {
                let schema = options
                    .infer_schema(ctx, self.table_paths.get(0).unwrap())
                    .await?;

                Ok(Self {
                    table_paths: self.table_paths,
                    file_schema: Some(schema),
                    options: Some(options),
                })
            }
            None => Err(DataFusionError::Internal(
                "No `ListingOptions` set for inferring schema".into(),
            )),
        }
    }

    /// Convenience wrapper for calling `infer_options` and `infer_schema`
    pub async fn infer(self, ctx: &SessionState) -> Result<Self> {
        self.infer_options(ctx).await?.infer_schema(ctx).await
    }
}

/// Options for creating a `ListingTable`
#[derive(Clone, Debug)]
pub struct ListingOptions {
    /// A suffix on which files should be filtered (leave empty to
    /// keep all files on the path)
    pub file_extension: String,
    /// The file format
    pub format: Arc<dyn FileFormat>,
    /// The expected partition column names in the folder structure.
    /// For example `Vec["a", "b"]` means that the two first levels of
    /// partitioning expected should be named "a" and "b":
    /// - If there is a third level of partitioning it will be ignored.
    /// - Files that don't follow this partitioning will be ignored.
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Set true to try to guess statistics from the files.
    /// This can add a lot of overhead as it will usually require files
    /// to be opened and at least partially parsed.
    pub collect_stat: bool,
    /// Group files to avoid that the number of partitions exceeds
    /// this limit
    pub target_partitions: usize,
    /// Optional pre-known sort order. Must be `SortExpr`s.
    ///
    /// DataFusion may take advantage of this ordering to omit sorts
    /// or use more efficient algorithms. Currently sortedness must be
    /// provided if it is known by some external mechanism, but may in
    /// the future be automatically determined, for example using
    /// parquet metadata.
    ///
    /// See <https://github.com/apache/arrow-datafusion/issues/4177>
    pub file_sort_order: Option<Vec<Expr>>,
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
            file_sort_order: None,
        }
    }

    /// Set file extension on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::new(ctx.config_options())
    ///   ))
    ///   .with_file_extension(".parquet");
    ///
    /// assert_eq!(listing_options.file_extension, ".parquet");
    /// ```
    pub fn with_file_extension(mut self, file_extension: impl Into<String>) -> Self {
        self.file_extension = file_extension.into();
        self
    }

    /// Set table partition column names on [`ListingOptions`] and returns self.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::DataType;
    /// # use datafusion::prelude::{col, SessionContext};
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::new(ctx.config_options())
    ///   ))
    ///   .with_table_partition_cols(vec![("col_a".to_string(), DataType::Utf8),
    ///       ("col_b".to_string(), DataType::Utf8)]);
    ///
    /// assert_eq!(listing_options.table_partition_cols, vec![("col_a".to_string(), DataType::Utf8),
    ///     ("col_b".to_string(), DataType::Utf8)]);
    /// ```
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
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::new(ctx.config_options())
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
    /// # use datafusion::prelude::SessionContext;
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::new(ctx.config_options())
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
    /// # use datafusion::prelude::{col, SessionContext};
    /// # use datafusion::datasource::{listing::ListingOptions, file_format::parquet::ParquetFormat};
    ///
    ///  // Tell datafusion that the files are sorted by column "a"
    ///  let file_sort_order = Some(vec![
    ///    col("a").sort(true, true)
    ///  ]);
    ///
    /// let ctx = SessionContext::new();
    /// let listing_options = ListingOptions::new(Arc::new(
    ///     ParquetFormat::new(ctx.config_options())
    ///   ))
    ///   .with_file_sort_order(file_sort_order.clone());
    ///
    /// assert_eq!(listing_options.file_sort_order, file_sort_order);
    /// ```
    pub fn with_file_sort_order(mut self, file_sort_order: Option<Vec<Expr>>) -> Self {
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
        ctx: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<SchemaRef> {
        let store = ctx.runtime_env.object_store(table_path)?;

        let files: Vec<_> = table_path
            .list_all_files(store.as_ref(), &self.file_extension)
            .try_collect()
            .await?;

        self.format.infer_schema(&store, &files).await
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

/// An implementation of `TableProvider` that uses the object store
/// or file system listing capability to get the list of files.
pub struct ListingTable {
    table_paths: Vec<ListingTableUrl>,
    /// File fields only
    file_schema: SchemaRef,
    /// File fields + partition columns
    table_schema: SchemaRef,
    options: ListingOptions,
    definition: Option<String>,
    collected_statistics: StatisticsCache,
}

impl ListingTable {
    /// Create new table that lists the FS to get the files to scan.
    /// Takes a `ListingTableConfig` as input which requires an `ObjectStore` and `table_path`.
    /// `ListingOptions` and `SchemaRef` are optional.  If they are not
    /// provided the file type is inferred based on the file suffix.
    /// If the schema is provided then it must be resolved before creating the table
    /// and should contain the fields of the file without the table
    /// partitioning columns.
    pub fn try_new(config: ListingTableConfig) -> Result<Self> {
        let file_schema = config
            .file_schema
            .ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;

        let options = config.options.ok_or_else(|| {
            DataFusionError::Internal("No ListingOptions provided".into())
        })?;

        // Add the partition columns to the file schema
        let mut table_fields = file_schema.fields().clone();
        for (part_col_name, part_col_type) in &options.table_partition_cols {
            table_fields.push(Field::new(
                part_col_name,
                partition_type_wrap(part_col_type.clone()),
                false,
            ));
        }

        let table = Self {
            table_paths: config.table_paths,
            file_schema,
            table_schema: Arc::new(Schema::new(table_fields)),
            options,
            definition: None,
            collected_statistics: Default::default(),
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
    fn try_create_output_ordering(&self) -> Result<Option<Vec<PhysicalSortExpr>>> {
        let file_sort_order =
            if let Some(file_sort_order) = self.options.file_sort_order.as_ref() {
                file_sort_order
            } else {
                return Ok(None);
            };

        // convert each expr to a physical sort expr
        let sort_exprs = file_sort_order
            .iter()
            .map(|expr| {
                if let Expr::Sort { expr, asc, nulls_first } = expr {
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
                        Err(DataFusionError::Plan(
                            format!("Only support single column references in output_ordering, got {:?}", expr)
                        ))
                    }
                } else {
                    Err(DataFusionError::Plan(
                        format!("Expected Expr::Sort in output_ordering, but got {:?}", expr)
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(sort_exprs))
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
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, statistics) =
            self.list_files_for_scan(ctx, filters, limit).await?;

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

        // create the execution plan
        self.options
            .format
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: self.table_paths.get(0).unwrap().object_store(),
                    file_schema: Arc::clone(&self.file_schema),
                    file_groups: partitioned_file_lists,
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: self.try_create_output_ordering()?,
                    table_partition_cols,
                    config_options: ctx.config.config_options(),
                },
                filters,
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
        let store = ctx
            .runtime_env
            .object_store(self.table_paths.get(0).unwrap())?;
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
    use crate::datasource::file_format::file_type::GetExt;
    use crate::prelude::SessionContext;
    use crate::{
        datasource::file_format::{avro::AvroFormat, parquet::ParquetFormat},
        logical_expr::{col, lit},
        test::{columns, object_store::register_test_store},
    };
    use arrow::datatypes::DataType;
    use chrono::DateTime;
    use datafusion_common::assert_contains;

    use super::*;

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

        let opt = ListingOptions::new(Arc::new(ParquetFormat::new(ctx.config_options())));
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
    async fn test_try_create_output_ordering() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");
        let table_path = ListingTableUrl::parse(filename).unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::new(ctx.config_options())));
        let schema = options.infer_schema(&state, &table_path).await.unwrap();

        use physical_plan::expressions::col as physical_col;
        use std::ops::Add;

        // (file_sort_order, expected_result)
        let cases = vec![
            (None, Ok(None)),
            // empty list
            (Some(vec![]), Ok(Some(vec![]))),
            // not a sort expr
            (
                Some(vec![col("string_col")]),
                Err("Expected Expr::Sort in output_ordering, but got string_col"),
            ),
            // sort expr, but non column
            (
                Some(vec![
                    col("int_col").add(lit(1)).sort(true, true),
                ]),
                Err("Only support single column references in output_ordering, got int_col + Int32(1)"),
            ),
            // ok with one column
            (
                Some(vec![col("string_col").sort(true, false)]),
                Ok(Some(vec![PhysicalSortExpr {
                    expr: physical_col("string_col", &schema).unwrap(),
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                }]))

            ),
            // ok with two columns, different options
            (
                Some(vec![
                    col("string_col").sort(true, false),
                    col("int_col").sort(false, true),
                ]),
                Ok(Some(vec![
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
                ]))

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
                        "expected: {:#?}\n\nactual:{:#?}",
                        expected_result, ordering_result
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
            .with_table_partition_cols(vec![(
                String::from("p1"),
                partition_type_wrap(DataType::Utf8),
            )])
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
        let filename = format!("{}/{}", testdata, name);
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
}
