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

//! DataFusion data sources

pub mod csv;
pub mod datasource;
pub mod empty;
pub mod json;
pub mod local;
pub mod memory;
pub mod object_store;
pub mod parquet;

pub use self::csv::{CsvFile, CsvReadOptions};
pub use self::datasource::{TableProvider, TableType};
pub use self::memory::MemTable;

use crate::arrow::datatypes::{Schema, SchemaRef};
use crate::datasource::datasource::{ColumnStatistics, Statistics};
use crate::datasource::object_store::ObjectStore;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::expressions::{MaxAccumulator, MinAccumulator};
use crate::physical_plan::Accumulator;
use crate::scalar::ScalarValue;
use std::sync::Arc;

/// Source for table input data
pub(crate) enum Source<R = Box<dyn std::io::Read + Send + Sync + 'static>> {
    /// Path to a single file or a directory containing one of more files
    Path(String),

    /// Read data from a reader
    Reader(std::sync::Mutex<Option<R>>),
}

#[derive(Debug, Clone)]
/// A single file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub file_path: String,
    /// Schema of the file
    pub schema: Schema,
    /// Statistics of the file
    pub statistics: Statistics,
    /// Values of partition columns to be appended to each row
    pub partition_value: Option<Vec<ScalarValue>>,
    /// Schema of partition columns
    pub partition_schema: Option<Schema>,
    // We may include row group range here for a more fine-grained parallel execution
}

impl From<String> for PartitionedFile {
    fn from(file_path: String) -> Self {
        Self {
            file_path,
            schema: Schema::empty(),
            statistics: Default::default(),
            partition_value: None,
            partition_schema: None,
        }
    }
}

impl std::fmt::Display for PartitionedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.file_path)
    }
}

#[derive(Debug, Clone)]
/// A collection of files that should be read in a single task
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

#[derive(Debug, Clone)]
/// All source files with same schema exists in a path
pub struct SourceRootDescriptor {
    /// All source files in the path
    pub partition_files: Vec<PartitionedFile>,
    /// The schema of the files
    pub schema: SchemaRef,
}

/// Builder for ['SourceRootDescriptor'] inside given path
pub trait SourceRootDescBuilder {
    /// Construct a ['SourceRootDescriptor'] from the provided path
    fn get_source_desc(
        path: &str,
        object_store: Arc<dyn ObjectStore>,
        ext: &str,
    ) -> Result<SourceRootDescriptor> {
        let filenames = object_store.list_all_files(path, ext)?;
        if filenames.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "No file (with .{} extension) found at path {}",
                ext, path
            )));
        }

        // build a list of Parquet partitions with statistics and gather all unique schemas
        // used in this data set
        let mut schemas: Vec<Schema> = vec![];

        let partitioned_files = filenames
            .iter()
            .map(|file_path| {
                let pf = Self::get_file_meta(file_path, object_store.clone())?;
                let schema = pf.schema.clone();
                if schemas.is_empty() {
                    schemas.push(schema);
                } else if schema != schemas[0] {
                    // we currently get the schema information from the first file rather than do
                    // schema merging and this is a limitation.
                    // See https://issues.apache.org/jira/browse/ARROW-11017
                    return Err(DataFusionError::Plan(format!(
                        "The file {} have different schema from the first file and DataFusion does \
                        not yet support schema merging",
                        file_path
                    )));
                }
                Ok(pf)
            }).collect::<Result<Vec<PartitionedFile>>>();

        Ok(SourceRootDescriptor {
            partition_files: partitioned_files?,
            schema: Arc::new(schemas.pop().unwrap()),
        })
    }

    /// Get all metadata for a source file, including schema, statistics, partitions, etc.
    fn get_file_meta(
        file_path: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<PartitionedFile>;
}

/// Get all files as well as the summary statistics when a limit is provided
pub fn get_statistics_with_limit(
    source_desc: &SourceRootDescriptor,
    limit: Option<usize>,
) -> (Vec<PartitionedFile>, Statistics) {
    let mut all_files = source_desc.partition_files.clone();
    let schema = source_desc.schema.clone();

    let mut total_byte_size = 0;
    let mut null_counts = vec![0; schema.fields().len()];
    let mut has_statistics = false;
    let (mut max_values, mut min_values) = create_max_min_accs(&schema);

    let mut num_rows = 0;
    let mut num_files = 0;
    for file in &all_files {
        num_files += 1;
        let file_stats = &file.statistics;
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
    all_files.truncate(num_files);

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
    };
    (all_files, statistics)
}

fn create_max_min_accs(
    schema: &Schema,
) -> (Vec<Option<MaxAccumulator>>, Vec<Option<MinAccumulator>>) {
    let max_values: Vec<Option<MaxAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MaxAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    let min_values: Vec<Option<MinAccumulator>> = schema
        .fields()
        .iter()
        .map(|field| MinAccumulator::try_new(field.data_type()).ok())
        .collect::<Vec<_>>();
    (max_values, min_values)
}

fn get_col_stats(
    schema: &Schema,
    null_counts: Vec<usize>,
    max_values: &mut Vec<Option<MaxAccumulator>>,
    min_values: &mut Vec<Option<MinAccumulator>>,
) -> Vec<ColumnStatistics> {
    (0..schema.fields().len())
        .map(|i| {
            let max_value = match &max_values[i] {
                Some(max_value) => max_value.evaluate().ok(),
                None => None,
            };
            let min_value = match &min_values[i] {
                Some(min_value) => min_value.evaluate().ok(),
                None => None,
            };
            ColumnStatistics {
                null_count: Some(null_counts[i] as usize),
                max_value,
                min_value,
                distinct_count: None,
            }
        })
        .collect()
}
