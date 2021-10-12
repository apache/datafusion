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

use arrow::datatypes::SchemaRef;

use crate::{
    datasource::format::{self},
    error::Result,
    logical_plan::{combine_filters, Expr},
    physical_plan::{common, parquet::ParquetExec, ExecutionPlan, Statistics},
};

use super::{PartitionedFile, TableProvider};

/// The supported file types with the associated options.
pub enum FormatOptions {
    /// The Apache Parquet file type.
    Parquet {
        /// Parquet files contain row group statistics in the
        /// metadata section. Set true to parse it. This can
        /// add a lot of overhead as it requires each file to
        /// be opened and partially parsed.
        collect_stat: bool,
        /// Activate statistics based row group level pruning
        enable_pruning: bool,
        /// group files to avoid that the number of partitions
        /// exceeds this limit
        max_partitions: usize,
    },
    /// Row oriented text file with newline as row delimiter.
    Csv {
        /// Set true to indicate that the first line is a header.
        has_header: bool,
        /// The character seprating values within a row.
        delimiter: u8,
        /// If no schema was provided for the table, it will be
        /// infered from the data itself, this limits the number
        /// of lines used in the process.
        schema_infer_max_rec: Option<u64>,
    },
    /// New line delimited JSON.
    Json {
        /// If no schema was provided for the table, it will be
        /// infered from the data itself, this limits the number
        /// of lines used in the process.
        schema_infer_max_rec: Option<u64>,
    },
}

/// Options for creating a `ListingTable`
pub struct ListingOptions {
    /// A suffix on which files should be filtered (leave empty to
    /// keep all files on the path)
    pub extension: String,
    /// The file format
    pub format: FormatOptions,
}

impl ListingOptions {
    /// This method will not be called by the table itself but before creating it.
    /// This way when creating the logical plan we can decide to resolve the schema
    /// locally or ask a remote service to do it (e.g a scheduler).
    pub fn infer_schema(&self, _path: &str) -> Result<SchemaRef> {
        match self {
            ListingOptions {
                format: FormatOptions::Parquet { .. },
                ..
            } => {
                todo!("list one file and get the schema for it")
            }
            _ => todo!("other file formats"),
        }
    }

    fn create_executor(
        &self,
        schema: SchemaRef,
        files: Vec<PartitionedFile>,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self {
            ListingOptions {
                format:
                    FormatOptions::Parquet {
                        collect_stat,
                        enable_pruning,
                        max_partitions,
                    },
                ..
            } => {
                // If enable pruning then combine the filters to build the predicate.
                // If disable pruning then set the predicate to None, thus readers
                // will not prune data based on the statistics.
                let predicate = if *enable_pruning {
                    combine_filters(filters)
                } else {
                    None
                };

                // collect the statistics if required by the config
                let mut files = files;
                if *collect_stat {
                    files = files
                        .into_iter()
                        .map(|file| -> Result<PartitionedFile> {
                            let (_schema, statistics) =
                                format::parquet::fetch_metadata(&file.path)?;
                            // TODO use _schema to check that it is valid or for schema merging
                            Ok(PartitionedFile {
                                statistics,
                                path: file.path,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                }

                let (files, statistics) =
                    format::get_statistics_with_limit(&files, Arc::clone(&schema), limit);

                let partitioned_file_lists = split_files(files, *max_partitions);

                Ok(Arc::new(ParquetExec::try_new_refacto(
                    partitioned_file_lists,
                    statistics,
                    schema,
                    projection.clone(),
                    predicate,
                    limit
                        .map(|l| std::cmp::min(l, batch_size))
                        .unwrap_or(batch_size),
                    limit,
                )?))
            }
            _ => todo!(),
        }
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
    pub fn try_new(
        path: impl Into<String>,
        // the schema must be resolved before creating the table
        schema: SchemaRef,
        options: ListingOptions,
    ) -> Result<Self> {
        let path: String = path.into();
        Ok(Self {
            path,
            schema,
            options,
        })
    }
}

impl TableProvider for ListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 1. list files (with partitions)
        let file_list =
            pruned_partition_list(&self.path, filters, &self.options.extension)?;
        // 2. create the plan
        self.options.create_executor(
            self.schema(),
            file_list,
            projection,
            batch_size,
            filters,
            limit,
        )
    }
}

/// Discover the partitions on the given path and prune out files
/// relative to irrelevant partitions using `filters` expressions
fn pruned_partition_list(
    // registry: &ObjectStoreRegistry,
    path: &str,
    _filters: &[Expr],
    file_extension: &str,
) -> Result<Vec<PartitionedFile>> {
    // TODO: parse folder names first to get partitions and apply the `filters`
    // to list only relevant ones
    Ok(common::build_file_list(path, file_extension)?
        .into_iter()
        .map(|f| PartitionedFile {
            path: f,
            statistics: Statistics::default(),
        })
        .collect())
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
    use futures::StreamExt;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.parquet")?;
        let projection = None;
        let exec = table.scan(&projection, 2, &[], None)?;
        let stream = exec.execute(0).await?;

        let _ = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        // test metadata
        assert_eq!(exec.statistics().num_rows, Some(8));
        assert_eq!(exec.statistics().total_byte_size, Some(671));

        Ok(())
    }

    fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, name);
        let opt = ListingOptions {
            extension: ".parquet".to_owned(),
            format: FormatOptions::Parquet {
                collect_stat: false,
                enable_pruning: true,
                max_partitions: 2,
            },
        };
        // here we resolve the schema locally
        let schema = opt.infer_schema(&filename)?;
        let table = ListingTable::try_new(&filename, schema, opt)?;
        Ok(Arc::new(table))
    }
}
