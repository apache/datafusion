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
    datasource::file_format::{self, PartitionedFile},
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_plan::{ExecutionPlan, Statistics},
};

use super::{
    datasource::TableProviderFilterPushDown,
    file_format::{FileFormat, PartitionedFileStream},
    object_store::ObjectStoreRegistry,
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
    /// Infer the schema of the files at the given path, including the partitioning
    /// columns.
    ///
    /// This method will not be called by the table itself but before creating it.
    /// This way when creating the logical plan we can decide to resolve the schema
    /// locally or ask a remote service to do it (e.g a scheduler).
    pub async fn infer_schema(&self, path: &str) -> Result<SchemaRef> {
        let object_store = self.format.object_store_registry().get_by_uri(path)?;
        let file_stream = object_store
            .list_file_with_suffix(path, &self.file_extension)
            .await?;
        let file_schema = self.format.infer_schema(file_stream).await?;
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
        let (partitioned_file_lists, statistics) =
            self.list_files_for_scan(filters, limit).await?;
        // create the execution plan
        self.options
            .format
            .create_physical_plan(
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

impl ListingTable {
    async fn list_files_for_scan(
        &self,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        // list files (with partitions)
        let file_list = pruned_partition_list(
            self.options.format.object_store_registry(),
            &self.path,
            filters,
            &self.options.file_extension,
            &self.options.partitions,
        )
        .await?;

        // collect the statistics if required by the config
        let files = file_list.then(|part_file| async {
            let part_file = part_file?;
            let statistics = if self.options.collect_stat {
                self.options
                    .format
                    .infer_stats(part_file.file.clone())
                    .await?
            } else {
                Statistics::default()
            };
            Ok((part_file, statistics)) as Result<(PartitionedFile, Statistics)>
        });

        let (files, statistics) =
            file_format::get_statistics_with_limit(files, self.schema(), limit).await?;

        if files.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No files found at {} with file extension {}",
                self.path, self.options.file_extension,
            )));
        }

        Ok((split_files(files, self.options.max_partitions), statistics))
    }
}

/// Discover the partitions on the given path and prune out files
/// relative to irrelevant partitions using `filters` expressions
async fn pruned_partition_list(
    registry: &ObjectStoreRegistry,
    path: &str,
    _filters: &[Expr],
    file_extension: &str,
    partition_names: &[String],
) -> Result<PartitionedFileStream> {
    if partition_names.is_empty() {
        Ok(Box::pin(
            registry
                .get_by_uri(path)?
                .list_file_with_suffix(path, file_extension)
                .await?
                .map(|f| Ok(PartitionedFile { file: f? })),
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
    use crate::datasource::{
        file_format::{avro::AvroFormat, parquet::ParquetFormat},
        object_store::{ListEntryStream, ObjectStore, SizedFile, SizedFileStream},
    };

    use super::*;

    #[test]
    fn test_split_files() {
        let files = vec![
            PartitionedFile {
                file: SizedFile {
                    path: "a".to_owned(),
                    size: 10,
                },
            },
            PartitionedFile {
                file: SizedFile {
                    path: "b".to_owned(),
                    size: 10,
                },
            },
            PartitionedFile {
                file: SizedFile {
                    path: "c".to_owned(),
                    size: 10,
                },
            },
            PartitionedFile {
                file: SizedFile {
                    path: "d".to_owned(),
                    size: 10,
                },
            },
            PartitionedFile {
                file: SizedFile {
                    path: "e".to_owned(),
                    size: 10,
                },
            },
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
            max_partitions: 2,
            collect_stat: true,
        };
        // here we resolve the schema locally
        let schema = opt.infer_schema(&filename).await.expect("Infer schema");
        let table = ListingTable::new(&filename, schema, opt);
        Ok(Arc::new(table))
    }

    async fn assert_partitioning(
        files_in_folder: usize,
        max_partitions: usize,
        output_partitioning: usize,
    ) -> Result<()> {
        let registry = ObjectStoreRegistry::new();
        registry.register_store(
            "mock".to_owned(),
            Arc::new(MockObjectStore { files_in_folder }),
        );

        let format = AvroFormat::new(Arc::new(registry));

        let opt = ListingOptions {
            file_extension: "".to_owned(),
            format: Arc::new(format),
            partitions: vec![],
            max_partitions,
            collect_stat: true,
        };

        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);

        let table = ListingTable::new("mock://bucket/key-prefix", Arc::new(schema), opt);

        let (file_list, _) = table.list_files_for_scan(&[], None).await?;

        assert_eq!(file_list.len(), output_partitioning);

        Ok(())
    }

    #[derive(Debug)]
    struct MockObjectStore {
        pub files_in_folder: usize,
    }

    #[async_trait]
    impl ObjectStore for MockObjectStore {
        async fn list_file(&self, prefix: &str) -> Result<SizedFileStream> {
            let prefix = prefix.to_owned();
            let files = (0..self.files_in_folder).map(move |i| {
                Ok(SizedFile {
                    path: format!("{}file{}", prefix, i),
                    size: 100,
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

        fn file_reader(
            &self,
            _file: SizedFile,
        ) -> Result<Arc<dyn crate::datasource::object_store::ObjectReader>> {
            unimplemented!()
        }
    }
}
