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

//! A table that uses the `ObjectStore` listing capability
//! to get the list of files to process.

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;

use crate::{
    datasource::PartitionedFile,
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_plan::{ExecutionPlan, Statistics},
};

use super::{
    datasource::TableProviderFilterPushDown,
    file_format::{FileFormat, PhysicalPlanConfig},
    get_statistics_with_limit,
    object_store::ObjectStore,
    PartitionedFileStream, TableProvider,
};

/// Options for creating a `ListingTable`
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
    /// Note that only `DataType::Utf8` is supported for the column type.
    /// TODO implement case where partitions.len() > 0
    pub partitions: Vec<String>,
    /// Set true to try to guess statistics from the files.
    /// This can add a lot of overhead as it will usually require files
    /// to be opened and at least partially parsed.
    pub collect_stat: bool,
    /// Group files to avoid that the number of partitions exceeds
    /// this limit
    pub target_partitions: usize,
}

impl ListingOptions {
    /// Creates an options instance with the given format
    /// Default values:
    /// - no file extension filter
    /// - no input partition to discover
    /// - one target partition
    /// - no stat collection
    pub fn new(format: Arc<dyn FileFormat>) -> Self {
        Self {
            file_extension: String::new(),
            format,
            partitions: vec![],
            collect_stat: false,
            target_partitions: 1,
        }
    }

    /// Infer the schema of the files at the given path on the provided object store.
    /// The inferred schema should include the partitioning columns.
    ///
    /// This method will not be called by the table itself but before creating it.
    /// This way when creating the logical plan we can decide to resolve the schema
    /// locally or ask a remote service to do it (e.g a scheduler).
    pub async fn infer_schema<'a>(
        &'a self,
        object_store: Arc<dyn ObjectStore>,
        path: &'a str,
    ) -> Result<SchemaRef> {
        let file_stream = object_store
            .list_file_with_suffix(path, &self.file_extension)
            .await?
            .map(move |file_meta| object_store.file_reader(file_meta?.sized_file));
        let file_schema = self.format.infer_schema(Box::pin(file_stream)).await?;
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
    object_store: Arc<dyn ObjectStore>,
    path: String,
    schema: SchemaRef,
    options: ListingOptions,
}

impl ListingTable {
    /// Create new table that lists the FS to get the files to scan.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: String,
        // the schema must be resolved before creating the table
        schema: SchemaRef,
        options: ListingOptions,
    ) -> Self {
        Self {
            object_store,
            path,
            schema,
            options,
        }
    }

    /// Get object store ref
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
    /// Get path ref
    pub fn path(&self) -> &str {
        &self.path
    }
    /// Get options ref
    pub fn options(&self) -> &ListingOptions {
        &self.options
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
        // TODO object_store_registry should be provided as param here
        let (partitioned_file_lists, statistics) = self
            .list_files_for_scan(
                Arc::clone(&self.object_store),
                &self.path,
                filters,
                limit,
            )
            .await?;
        // create the execution plan
        self.options
            .format
            .create_physical_plan(PhysicalPlanConfig {
                object_store: Arc::clone(&self.object_store),
                schema: self.schema(),
                files: partitioned_file_lists,
                statistics,
                projection: projection.clone(),
                batch_size,
                filters: filters.to_vec(),
                limit,
            })
            .await
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

impl ListingTable {
    async fn list_files_for_scan<'a>(
        &'a self,
        object_store: Arc<dyn ObjectStore>,
        path: &'a str,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        // list files (with partitions)
        let file_list = pruned_partition_list(
            object_store.as_ref(),
            path,
            filters,
            &self.options.file_extension,
            &self.options.partitions,
        )
        .await?;

        // collect the statistics if required by the config
        let files = file_list.then(move |part_file| {
            let object_store = object_store.clone();
            async move {
                let part_file = part_file?;
                let statistics = if self.options.collect_stat {
                    let object_reader = object_store
                        .file_reader(part_file.file_meta.sized_file.clone())?;
                    self.options.format.infer_stats(object_reader).await?
                } else {
                    Statistics::default()
                };
                Ok((part_file, statistics)) as Result<(PartitionedFile, Statistics)>
            }
        });

        let (files, statistics) =
            get_statistics_with_limit(files, self.schema(), limit).await?;

        if files.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No files found at {} with file extension {}",
                self.path, self.options.file_extension,
            )));
        }

        Ok((
            split_files(files, self.options.target_partitions),
            statistics,
        ))
    }
}

/// Discover the partitions on the given path and prune out files
/// relative to irrelevant partitions using `filters` expressions
async fn pruned_partition_list(
    store: &dyn ObjectStore,
    path: &str,
    _filters: &[Expr],
    file_extension: &str,
    partition_names: &[String],
) -> Result<PartitionedFileStream> {
    if partition_names.is_empty() {
        Ok(Box::pin(
            store
                .list_file_with_suffix(path, file_extension)
                .await?
                .map(|f| Ok(PartitionedFile { file_meta: f? })),
        ))
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
    use std::io::Read;

    use futures::AsyncRead;

    use crate::datasource::{
        file_format::{avro::AvroFormat, parquet::ParquetFormat},
        object_store::{
            local::LocalFileSystem, FileMeta, FileMetaStream, ListEntryStream,
            ObjectReader, ObjectStore, SizedFile,
        },
    };

    use super::*;

    #[test]
    fn test_split_files() {
        let new_partitioned_file = |path: &str| PartitionedFile {
            file_meta: FileMeta {
                sized_file: SizedFile {
                    path: path.to_owned(),
                    size: 10,
                },
                last_modified: None,
            },
        };
        let files = vec![
            new_partitioned_file("a"),
            new_partitioned_file("b"),
            new_partitioned_file("c"),
            new_partitioned_file("d"),
            new_partitioned_file("e"),
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

    #[tokio::test]
    async fn file_listings() -> Result<()> {
        assert_partitioning(5, 12, 5).await?;
        assert_partitioning(4, 4, 4).await?;
        assert_partitioning(5, 2, 2).await?;
        assert_partitioning(0, 2, 0).await.expect_err("no files");
        Ok(())
    }

    async fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, name);
        let opt = ListingOptions {
            file_extension: "parquet".to_owned(),
            format: Arc::new(ParquetFormat::default()),
            partitions: vec![],
            target_partitions: 2,
            collect_stat: true,
        };
        // here we resolve the schema locally
        let schema = opt
            .infer_schema(Arc::new(LocalFileSystem {}), &filename)
            .await
            .expect("Infer schema");
        let table =
            ListingTable::new(Arc::new(LocalFileSystem {}), filename, schema, opt);
        Ok(Arc::new(table))
    }

    async fn assert_partitioning(
        files_in_folder: usize,
        target_partitions: usize,
        output_partitioning: usize,
    ) -> Result<()> {
        let mock_store: Arc<dyn ObjectStore> =
            Arc::new(MockObjectStore { files_in_folder });

        let format = AvroFormat {};

        let opt = ListingOptions {
            file_extension: "".to_owned(),
            format: Arc::new(format),
            partitions: vec![],
            target_partitions,
            collect_stat: true,
        };

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table = ListingTable::new(
            Arc::clone(&mock_store),
            "bucket/key-prefix".to_owned(),
            Arc::new(schema),
            opt,
        );

        let (file_list, _) = table
            .list_files_for_scan(mock_store, "bucket/key-prefix", &[], None)
            .await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    #[derive(Debug)]
    struct MockObjectStore {
        pub files_in_folder: usize,
    }

    #[async_trait]
    impl ObjectStore for MockObjectStore {
        async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
            let prefix = prefix.to_owned();
            let files = (0..self.files_in_folder).map(move |i| {
                Ok(FileMeta {
                    sized_file: SizedFile {
                        path: format!("{}file{}", prefix, i),
                        size: 100,
                    },
                    last_modified: None,
                })
            });
            Ok(Box::pin(futures::stream::iter(files)))
        }

        async fn list_dir(
            &self,
            _prefix: &str,
            _delimiter: Option<String>,
        ) -> Result<ListEntryStream> {
            unimplemented!()
        }

        fn file_reader(&self, _file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
            Ok(Arc::new(MockObjectReader {}))
        }
    }

    struct MockObjectReader {}

    #[async_trait]
    impl ObjectReader for MockObjectReader {
        async fn chunk_reader(
            &self,
            _start: u64,
            _length: usize,
        ) -> Result<Box<dyn AsyncRead>> {
            unimplemented!()
        }

        fn sync_chunk_reader(
            &self,
            _start: u64,
            _length: usize,
        ) -> Result<Box<dyn Read + Send + Sync>> {
            unimplemented!()
        }

        fn length(&self) -> u64 {
            unimplemented!()
        }
    }
}
