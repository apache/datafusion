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

mod helpers;
mod table;

use datafusion_common::ScalarValue;
use datafusion_data_access::{FileMeta, Result, SizedFile};
use futures::Stream;
use std::pin::Pin;

pub use table::{ListingOptions, ListingTable, ListingTableConfig};

/// Stream of files get listed from object store
pub type PartitionedFileStream =
    Pin<Box<dyn Stream<Item = Result<PartitionedFile>> + Send + Sync + 'static>>;

/// Only scan a subset of Row Groups from the Parquet file whose data "midpoint"
/// lies within the [start, end) byte offsets. This option can be used to scan non-overlapping
/// sections of a Parquet file in parallel.
#[derive(Debug, Clone)]
pub struct FileRange {
    /// Range start
    pub start: i64,
    /// Range end
    pub end: i64,
}

#[derive(Debug, Clone)]
/// A single file or part of a file that should be read, along with its schema, statistics
/// A single file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub file_meta: FileMeta,
    /// Values of partition columns to be appended to each row
    pub partition_values: Vec<ScalarValue>,
    /// An optional file range for a more fine-grained parallel execution
    pub range: Option<FileRange>,
}

impl PartitionedFile {
    /// Create a simple file without metadata or partition
    pub fn new(path: String, size: u64) -> Self {
        Self {
            file_meta: FileMeta {
                sized_file: SizedFile { path, size },
                last_modified: None,
            },
            partition_values: vec![],
            range: None,
        }
    }

    /// Create a file range without metadata or partition
    pub fn new_with_range(path: String, size: u64, start: i64, end: i64) -> Self {
        Self {
            file_meta: FileMeta {
                sized_file: SizedFile { path, size },
                last_modified: None,
            },
            partition_values: vec![],
            range: Some(FileRange { start, end }),
        }
    }
}

impl std::fmt::Display for PartitionedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.file_meta)
    }
}

impl From<FileMeta> for PartitionedFile {
    fn from(file_meta: FileMeta) -> Self {
        PartitionedFile {
            file_meta,
            partition_values: vec![],
            range: None,
        }
    }
}
