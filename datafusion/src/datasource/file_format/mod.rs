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

//! Module containing helper methods for the various file formats

pub mod avro;
pub mod csv;
pub mod json;
pub mod parquet;

use std::pin::Pin;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef;
use crate::datasource::{create_max_min_accs, get_col_stats};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::{Accumulator, ExecutionPlan, Statistics};

use async_trait::async_trait;
use futures::{Stream, StreamExt};

use super::object_store::{FileMeta, ObjectReader, ObjectReaderStream, ObjectStore};

/// The configurations to be passed when creating a physical plan for
/// a given file format.
pub struct PhysicalPlanConfig {
    /// Store from which the `files` should be fetched
    pub object_store: Arc<dyn ObjectStore>,
    /// Schema before projection
    pub schema: SchemaRef,
    /// Partitioned fields to process in the executor
    pub files: Vec<Vec<PartitionedFile>>,
    /// Estimated overall statistics of source plan
    pub statistics: Statistics,
    /// Columns on which to project the data
    pub projection: Option<Vec<usize>>,
    /// The maximum number of records per arrow column
    pub batch_size: usize,
    /// The filters that where pushed down to this execution plan
    pub filters: Vec<Expr>,
    /// The minimum number of records required from this source plan
    pub limit: Option<usize>,
}

/// This trait abstracts all the file format specific implementations
/// from the `TableProvider`. This helps code re-utilization accross
/// providers that support the the same file formats.
#[async_trait]
pub trait FileFormat: Send + Sync {
    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(&self, readers: ObjectReaderStream) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    async fn infer_stats(&self, reader: Arc<dyn ObjectReader>) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        conf: PhysicalPlanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Get all files as well as the summary statistic
/// if the optional `limit` is provided, includes only sufficient files
/// needed to read up to `limit` number of rows
/// TODO fix case where `num_rows` and `total_byte_size` are not defined (stat should be None instead of Some(0))
/// TODO move back to crate::datasource::mod.rs once legacy cleaned up
pub async fn get_statistics_with_limit(
    all_files: impl Stream<Item = Result<(PartitionedFile, Statistics)>>,
    schema: SchemaRef,
    limit: Option<usize>,
) -> Result<(Vec<PartitionedFile>, Statistics)> {
    let mut result_files = vec![];

    let mut total_byte_size = 0;
    let mut null_counts = vec![0; schema.fields().len()];
    let mut has_statistics = false;
    let (mut max_values, mut min_values) = create_max_min_accs(&schema);

    let mut num_rows = 0;
    let mut is_exact = true;
    let mut all_files = Box::pin(all_files);
    while let Some(res) = all_files.next().await {
        let (file, file_stats) = res?;
        result_files.push(file);
        is_exact &= file_stats.is_exact;
        num_rows += file_stats.num_rows.unwrap_or(0);
        total_byte_size += file_stats.total_byte_size.unwrap_or(0);
        if let Some(vec) = &file_stats.column_statistics {
            has_statistics = true;
            for (i, cs) in vec.iter().enumerate() {
                null_counts[i] += cs.null_count.unwrap_or(0);

                if let Some(max_value) = &mut max_values[i] {
                    if let Some(file_max) = cs.max_value.clone() {
                        match max_value.update(&[file_max]) {
                            Ok(_) => {}
                            Err(_) => {
                                max_values[i] = None;
                            }
                        }
                    }
                }

                if let Some(min_value) = &mut min_values[i] {
                    if let Some(file_min) = cs.min_value.clone() {
                        match min_value.update(&[file_min]) {
                            Ok(_) => {}
                            Err(_) => {
                                min_values[i] = None;
                            }
                        }
                    }
                }
            }
        }
        if num_rows > limit.unwrap_or(usize::MAX) {
            break;
        }
    }
    // if we still have files in the stream, it means that the limit kicked
    // in and that the statistic could have been different if we processed
    // the files in a different order.
    if all_files.next().await.is_some() {
        is_exact = false;
    }

    let column_stats = if has_statistics {
        Some(get_col_stats(
            &*schema,
            null_counts,
            &mut max_values,
            &mut min_values,
        ))
    } else {
        None
    };

    let statistics = Statistics {
        num_rows: Some(num_rows as usize),
        total_byte_size: Some(total_byte_size as usize),
        column_statistics: column_stats,
        is_exact,
    };

    Ok((result_files, statistics))
}

#[derive(Debug, Clone)]
/// A single file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
/// TODO move back to crate::datasource::mod.rs once legacy cleaned up
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub file_meta: FileMeta,
    // Values of partition columns to be appended to each row
    // pub partition_value: Option<Vec<ScalarValue>>,
    // We may include row group range here for a more fine-grained parallel execution
}

/// Stream of files get listed from object store
pub type PartitionedFileStream =
    Pin<Box<dyn Stream<Item = Result<PartitionedFile>> + Send + Sync + 'static>>;

impl std::fmt::Display for PartitionedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.file_meta)
    }
}

#[derive(Debug, Clone)]
/// A collection of files that should be read in a single task
/// TODO move back to crate::datasource::mod.rs once legacy cleaned up
pub struct FilePartition {
    /// The index of the partition among all partitions
    pub index: usize,
    /// The contained files of the partition
    pub files: Vec<PartitionedFile>,
}

impl std::fmt::Display for FilePartition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let files: Vec<String> = self.files.iter().map(|f| f.to_string()).collect();
        write!(f, "{}", files.join(", "))
    }
}
