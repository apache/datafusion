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

//! A table that uses the files system / table store listing capability
//! to get the list of files to process.

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};

use crate::{
    datasource::format::{self},
    error::Result,
    logical_plan::Expr,
    physical_plan::{common, ExecutionPlan, Statistics},
};

use super::{
    datasource::TableProviderFilterPushDown, format::FileFormat, PartitionedFile,
    TableProvider,
};

/// Options for creating a `ListingTable`
pub struct ListingOptions {
    /// A suffix on which files should be filtered (leave empty to
    /// keep all files on the path)
    pub file_extension: String,
    /// The file format
    pub format: Arc<dyn FileFormat>,
    /// The expected partition column names.
    /// For example `Vec["a", "b"]` means that the two first levels of
    /// partitioning expected should be named "a" and "b":
    /// - If there is a third level of partitioning it will be ignored.
    /// - Files that don't follow this partitioning will be ignored.
    /// Note that only `DataType::Utf8` is supported for the column type.
    /// TODO implement case where partitions.len() > 0
    pub partitions: Vec<String>,
    /// Set true to try to guess statistics from the file parse it.
    /// This can add a lot of overhead as it requires files to
    /// be opened and partially parsed.
    pub collect_stat: bool,
    /// Group files to avoid that the number of partitions
    /// exceeds this limit
    pub max_partitions: usize,
}

impl ListingOptions {
    /// This method will not be called by the table itself but before creating it.
    /// This way when creating the logical plan we can decide to resolve the schema
    /// locally or ask a remote service to do it (e.g a scheduler).
    pub async fn infer_schema(&self, path: &str) -> Result<SchemaRef> {
        let files =
            futures::stream::iter(common::build_file_list(path, &self.file_extension)?);
        let file_schema = self.format.infer_schema(Box::pin(files)).await?;
        // Add the partition columns to the file schema
        let mut fields = file_schema.fields().clone();
        for part in &self.partitions {
            fields.push(Field::new(part, DataType::Utf8, false));
        }
        Ok(Arc::new(Schema::new(fields)))
    }
}

/// An implementation of `TableProvider` that uses the object store
/// or file system listing capability to get the list of files.
pub struct ListingTable {
    path: String,
    schema: SchemaRef,
    options: ListingOptions,
}

impl ListingTable {
    /// Create new table that lists the FS to get the files to scan.
    pub fn new(
        path: impl Into<String>,
        // the schema must be resolved before creating the table
        schema: SchemaRef,
        options: ListingOptions,
    ) -> Self {
        let path: String = path.into();
        Self {
            path,
            schema,
            options,
        }
    }
}

#[async_trait]
impl TableProvider for ListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // list files (with partitions)
        let file_list = pruned_partition_list(
            &self.path,
            filters,
            &self.options.file_extension,
            &self.options.partitions,
        )?;

        // collect the statistics if required by the config
        let mut files = file_list;
        if self.options.collect_stat {
            files = futures::stream::iter(files)
                .then(|file| async {
                    let statistics = self.options.format.infer_stats(&file.path).await?;
                    Ok(PartitionedFile {
                        statistics,
                        path: file.path,
                    }) as Result<PartitionedFile>
                })
                .try_collect::<Vec<_>>()
                .await?;
        }

        let (files, statistics) =
            format::get_statistics_with_limit(&files, self.schema(), limit);

        let partitioned_file_lists = split_files(files, self.options.max_partitions);

        // 2. create the plan
        self.options
            .format
            .create_executor(
                self.schema(),
                partitioned_file_lists,
                statistics,
                projection,
                batch_size,
                filters,
                limit,
            )
            .await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

/// Discover the partitions on the given path and prune out files
/// relative to irrelevant partitions using `filters` expressions
fn pruned_partition_list(
    // registry: &ObjectStoreRegistry,
    path: &str,
    _filters: &[Expr],
    file_extension: &str,
    partition_names: &[String],
) -> Result<Vec<PartitionedFile>> {
    let list_all = || {
        Ok(common::build_file_list(path, file_extension)?
            .into_iter()
            .map(|f| PartitionedFile {
                path: f,
                statistics: Statistics::default(),
            })
            .collect::<Vec<PartitionedFile>>())
    };
    if partition_names.is_empty() {
        list_all()
    } else {
        todo!("use filters to prune partitions")
    }
}

fn split_files(
    partitioned_files: Vec<PartitionedFile>,
    n: usize,
) -> Vec<Vec<PartitionedFile>> {
    let mut chunk_size = partitioned_files.len() / n;
    if partitioned_files.len() % n > 0 {
        chunk_size += 1;
    }
    partitioned_files
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_files() {
        let files = vec![
            PartitionedFile::from("a".to_string()),
            PartitionedFile::from("b".to_string()),
            PartitionedFile::from("c".to_string()),
            PartitionedFile::from("d".to_string()),
            PartitionedFile::from("e".to_string()),
        ];

        let chunks = split_files(files.clone(), 1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = split_files(files.clone(), 2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = split_files(files.clone(), 5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(files, 123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());
    }

    #[tokio::test]
    async fn read_single_file() -> Result<()> {
        let table = load_table("alltypes_plain.parquet").await?;
        let projection = None;
        let exec = table
            .scan(&projection, 1024, &[], None)
            .await
            .expect("Scan table");

        assert_eq!(exec.children().len(), 0);
        assert_eq!(exec.output_partitioning().partition_count(), 1);

        // test metadata
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    // TODO add tests on listing once the ObjectStore abstraction is added

    async fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, name);
        let opt = ListingOptions {
            file_extension: "parquet".to_owned(),
            format: Arc::new(format::parquet::ParquetFormat {
                enable_pruning: true,
            }),
            partitions: vec![],
            max_partitions: 2,
            collect_stat: true,
        };
        // here we resolve the schema locally
        let schema = opt.infer_schema(&filename).await.expect("Infer schema");
        let table = ListingTable::new(&filename, schema, opt);
        Ok(Arc::new(table))
    }
}
