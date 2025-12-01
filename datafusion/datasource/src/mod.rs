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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
// Enforce lint rule to prevent needless pass by value
// https://github.com/apache/datafusion/issues/18503
#![deny(clippy::needless_pass_by_value)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! A table that uses the `ObjectStore` listing capability
//! to get the list of files to process.

pub mod decoder;
pub mod display;
pub mod file;
pub mod file_compression_type;
pub mod file_format;
pub mod file_groups;
pub mod file_scan_config;
pub mod file_sink_config;
pub mod file_stream;
pub mod memory;
pub mod projection;
pub mod schema_adapter;
pub mod sink;
pub mod source;
mod statistics;
pub mod table_schema;

#[cfg(test)]
pub mod test_util;

pub mod url;
pub mod write;
pub use self::file::as_file_source;
pub use self::url::ListingTableUrl;
use crate::file_groups::FileGroup;
use chrono::TimeZone;
use datafusion_common::stats::Precision;
use datafusion_common::{exec_datafusion_err, ColumnStatistics, Result};
use datafusion_common::{ScalarValue, Statistics};
use futures::{Stream, StreamExt};
use object_store::{path::Path, ObjectMeta};
use object_store::{GetOptions, GetRange, ObjectStore};
pub use table_schema::TableSchema;
// Remove when add_row_stats is remove
#[allow(deprecated)]
pub use statistics::add_row_stats;
pub use statistics::compute_all_files_statistics;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

/// Stream of files get listed from object store
pub type PartitionedFileStream =
    Pin<Box<dyn Stream<Item = Result<PartitionedFile>> + Send + Sync + 'static>>;

/// Only scan a subset of Row Groups from the Parquet file whose data "midpoint"
/// lies within the [start, end) byte offsets. This option can be used to scan non-overlapping
/// sections of a Parquet file in parallel.
#[derive(Debug, Clone, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct FileRange {
    /// Range start
    pub start: i64,
    /// Range end
    pub end: i64,
}

impl FileRange {
    /// returns true if this file range contains the specified offset
    pub fn contains(&self, offset: i64) -> bool {
        offset >= self.start && offset < self.end
    }
}

#[derive(Debug, Clone)]
/// A single file or part of a file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub object_meta: ObjectMeta,
    /// Values of partition columns to be appended to each row.
    ///
    /// These MUST have the same count, order, and type than the [`table_partition_cols`].
    ///
    /// You may use [`wrap_partition_value_in_dict`] to wrap them if you have used [`wrap_partition_type_in_dict`] to wrap the column type.
    ///
    ///
    /// [`wrap_partition_type_in_dict`]: crate::file_scan_config::wrap_partition_type_in_dict
    /// [`wrap_partition_value_in_dict`]: crate::file_scan_config::wrap_partition_value_in_dict
    /// [`table_partition_cols`]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/file_format/options.rs#L87
    pub partition_values: Vec<ScalarValue>,
    /// An optional file range for a more fine-grained parallel execution
    pub range: Option<FileRange>,
    /// Optional statistics that describe the data in this file if known.
    ///
    /// DataFusion relies on these statistics for planning (in particular to sort file groups),
    /// so if they are incorrect, incorrect answers may result.
    pub statistics: Option<Arc<Statistics>>,
    /// An optional field for user defined per object metadata
    pub extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    /// The estimated size of the parquet metadata, in bytes
    pub metadata_size_hint: Option<usize>,
}

impl PartitionedFile {
    /// Create a simple file without metadata or partition
    pub fn new(path: impl Into<String>, size: u64) -> Self {
        Self {
            object_meta: ObjectMeta {
                location: Path::from(path.into()),
                last_modified: chrono::Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }
    }

    /// Create a file range without metadata or partition
    pub fn new_with_range(path: String, size: u64, start: i64, end: i64) -> Self {
        Self {
            object_meta: ObjectMeta {
                location: Path::from(path),
                last_modified: chrono::Utc.timestamp_nanos(0),
                size,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: Some(FileRange { start, end }),
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }
        .with_range(start, end)
    }

    /// Provide a hint to the size of the file metadata. If a hint is provided
    /// the reader will try and fetch the last `size_hint` bytes of the parquet file optimistically.
    /// Without an appropriate hint, two read may be required to fetch the metadata.
    pub fn with_metadata_size_hint(mut self, metadata_size_hint: usize) -> Self {
        self.metadata_size_hint = Some(metadata_size_hint);
        self
    }

    /// Return a file reference from the given path
    pub fn from_path(path: String) -> Result<Self> {
        let size = std::fs::metadata(path.clone())?.len();
        Ok(Self::new(path, size))
    }

    /// Return the path of this partitioned file
    pub fn path(&self) -> &Path {
        &self.object_meta.location
    }

    /// Update the file to only scan the specified range (in bytes)
    pub fn with_range(mut self, start: i64, end: i64) -> Self {
        self.range = Some(FileRange { start, end });
        self
    }

    /// Update the user defined extensions for this file.
    ///
    /// This can be used to pass reader specific information.
    pub fn with_extensions(
        mut self,
        extensions: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Self {
        self.extensions = Some(extensions);
        self
    }

    // Update the statistics for this file.
    pub fn with_statistics(mut self, statistics: Arc<Statistics>) -> Self {
        self.statistics = Some(statistics);
        self
    }

    /// Check if this file has any statistics.
    /// This returns `true` if the file has any Exact or Inexact statistics
    /// and `false` if all statistics are `Precision::Absent`.
    pub fn has_statistics(&self) -> bool {
        if let Some(stats) = &self.statistics {
            stats.column_statistics.iter().any(|col_stats| {
                col_stats.null_count != Precision::Absent
                    || col_stats.max_value != Precision::Absent
                    || col_stats.min_value != Precision::Absent
                    || col_stats.sum_value != Precision::Absent
                    || col_stats.distinct_count != Precision::Absent
            })
        } else {
            false
        }
    }
}

impl From<ObjectMeta> for PartitionedFile {
    fn from(object_meta: ObjectMeta) -> Self {
        PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }
    }
}

/// Represents the possible outcomes of a range calculation.
///
/// This enum is used to encapsulate the result of calculating the range of
/// bytes to read from an object (like a file) in an object store.
///
/// Variants:
/// - `Range(Option<Range<usize>>)`:
///   Represents a range of bytes to be read. It contains an `Option` wrapping a
///   `Range<usize>`. `None` signifies that the entire object should be read,
///   while `Some(range)` specifies the exact byte range to read.
/// - `TerminateEarly`:
///   Indicates that the range calculation determined no further action is
///   necessary, possibly because the calculated range is empty or invalid.
pub enum RangeCalculation {
    Range(Option<Range<u64>>),
    TerminateEarly,
}

/// Calculates an appropriate byte range for reading from an object based on the
/// provided metadata.
///
/// This asynchronous function examines the [`PartitionedFile`] of an object in an object store
/// and determines the range of bytes to be read. The range calculation may adjust
/// the start and end points to align with meaningful data boundaries (like newlines).
///
/// Returns a `Result` wrapping a [`RangeCalculation`], which is either a calculated byte range or an indication to terminate early.
///
/// Returns an `Error` if any part of the range calculation fails, such as issues in reading from the object store or invalid range boundaries.
pub async fn calculate_range(
    file: &PartitionedFile,
    store: &Arc<dyn ObjectStore>,
    terminator: Option<u8>,
) -> Result<RangeCalculation> {
    let location = &file.object_meta.location;
    let file_size = file.object_meta.size;
    let newline = terminator.unwrap_or(b'\n');

    match file.range {
        None => Ok(RangeCalculation::Range(None)),
        Some(FileRange { start, end }) => {
            let start: u64 = start.try_into().map_err(|_| {
                exec_datafusion_err!("Expect start range to fit in u64, got {start}")
            })?;
            let end: u64 = end.try_into().map_err(|_| {
                exec_datafusion_err!("Expect end range to fit in u64, got {end}")
            })?;

            let start_delta = if start != 0 {
                find_first_newline(store, location, start - 1, file_size, newline).await?
            } else {
                0
            };

            let end_delta = if end != file_size {
                find_first_newline(store, location, end - 1, file_size, newline).await?
            } else {
                0
            };

            let range = start + start_delta..end + end_delta;

            if range.start == range.end {
                return Ok(RangeCalculation::TerminateEarly);
            }

            Ok(RangeCalculation::Range(Some(range)))
        }
    }
}

/// Asynchronously finds the position of the first newline character in a specified byte range
/// within an object, such as a file, in an object store.
///
/// This function scans the contents of the object starting from the specified `start` position
/// up to the `end` position, looking for the first occurrence of a newline character.
/// It returns the position of the first newline relative to the start of the range.
///
/// Returns a `Result` wrapping a `usize` that represents the position of the first newline character found within the specified range. If no newline is found, it returns the length of the scanned data, effectively indicating the end of the range.
///
/// The function returns an `Error` if any issues arise while reading from the object store or processing the data stream.
async fn find_first_newline(
    object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    start: u64,
    end: u64,
    newline: u8,
) -> Result<u64> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };

    let result = object_store.get_opts(location, options).await?;
    let mut result_stream = result.into_stream();

    let mut index = 0;

    while let Some(chunk) = result_stream.next().await.transpose()? {
        if let Some(position) = chunk.iter().position(|&byte| byte == newline) {
            let position = position as u64;
            return Ok(index + position);
        }

        index += chunk.len() as u64;
    }

    Ok(index)
}

/// Generates test files with min-max statistics in different overlap patterns.
///
/// Used by tests and benchmarks.
///
/// # Overlap Factors
///
/// The `overlap_factor` parameter controls how much the value ranges in generated test files overlap:
/// - `0.0`: No overlap between files (completely disjoint ranges)
/// - `0.2`: Low overlap (20% of the range size overlaps with adjacent files)
/// - `0.5`: Medium overlap (50% of ranges overlap)
/// - `0.8`: High overlap (80% of ranges overlap between files)
///
/// # Examples
///
/// With 5 files and different overlap factors showing `[min, max]` ranges:
///
/// overlap_factor = 0.0 (no overlap):
///
/// File 0: [0, 20]
/// File 1: [20, 40]
/// File 2: [40, 60]
/// File 3: [60, 80]
/// File 4: [80, 100]
///
/// overlap_factor = 0.5 (50% overlap):
///
/// File 0: [0, 40]
/// File 1: [20, 60]
/// File 2: [40, 80]
/// File 3: [60, 100]
/// File 4: [80, 120]
///
/// overlap_factor = 0.8 (80% overlap):
///
/// File 0: [0, 100]
/// File 1: [20, 120]
/// File 2: [40, 140]
/// File 3: [60, 160]
/// File 4: [80, 180]
pub fn generate_test_files(num_files: usize, overlap_factor: f64) -> Vec<FileGroup> {
    let mut files = Vec::with_capacity(num_files);
    if num_files == 0 {
        return vec![];
    }
    let range_size = if overlap_factor == 0.0 {
        100 / num_files as i64
    } else {
        (100.0 / (overlap_factor * num_files as f64)).max(1.0) as i64
    };

    for i in 0..num_files {
        let base = (i as f64 * range_size as f64 * (1.0 - overlap_factor)) as i64;
        let min = base as f64;
        let max = (base + range_size) as f64;

        let file = PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(format!("file_{i}.parquet")),
                last_modified: chrono::Utc::now(),
                size: 1000,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: Some(Arc::new(Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Exact(1000),
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
                    min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                }],
            })),
            extensions: None,
            metadata_size_hint: None,
        };
        files.push(file);
    }

    vec![FileGroup::new(files)]
}

// Helper function to verify that files within each group maintain sort order
/// Used by tests and benchmarks
pub fn verify_sort_integrity(file_groups: &[FileGroup]) -> bool {
    for group in file_groups {
        let files = group.iter().collect::<Vec<_>>();
        for i in 1..files.len() {
            let prev_file = files[i - 1];
            let curr_file = files[i];

            // Check if the min value of current file is greater than max value of previous file
            if let (Some(prev_stats), Some(curr_stats)) =
                (&prev_file.statistics, &curr_file.statistics)
            {
                let prev_max = &prev_stats.column_statistics[0].max_value;
                let curr_min = &curr_stats.column_statistics[0].min_value;
                if curr_min.get_value().unwrap() <= prev_max.get_value().unwrap() {
                    return false;
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::ListingTableUrl;
    use arrow::{
        array::{ArrayRef, Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion_execution::object_store::{
        DefaultObjectStoreRegistry, ObjectStoreRegistry,
    };
    use object_store::{local::LocalFileSystem, path::Path};
    use std::{collections::HashMap, ops::Not, sync::Arc};
    use url::Url;

    /// Return a RecordBatch with a single Int32 array with values (0..sz) in a field named "i"
    pub fn make_partition(sz: i32) -> RecordBatch {
        let seq_start = 0;
        let seq_end = sz;
        let values = (seq_start..seq_end).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
        let arr = Arc::new(Int32Array::from(values));

        RecordBatch::try_new(schema, vec![arr as ArrayRef]).unwrap()
    }

    /// Get the schema for the aggregate_test_* csv files
    pub fn aggr_test_schema() -> SchemaRef {
        let mut f1 = Field::new("c1", DataType::Utf8, false);
        f1.set_metadata(HashMap::from_iter(vec![("testing".into(), "test".into())]));
        let schema = Schema::new(vec![
            f1,
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c4", DataType::Int16, false),
            Field::new("c5", DataType::Int32, false),
            Field::new("c6", DataType::Int64, false),
            Field::new("c7", DataType::UInt8, false),
            Field::new("c8", DataType::UInt16, false),
            Field::new("c9", DataType::UInt32, false),
            Field::new("c10", DataType::UInt64, false),
            Field::new("c11", DataType::Float32, false),
            Field::new("c12", DataType::Float64, false),
            Field::new("c13", DataType::Utf8, false),
        ]);

        Arc::new(schema)
    }

    #[test]
    fn test_object_store_listing_url() {
        let listing = ListingTableUrl::parse("file:///").unwrap();
        let store = listing.object_store();
        assert_eq!(store.as_str(), "file:///");

        let listing = ListingTableUrl::parse("s3://bucket/").unwrap();
        let store = listing.object_store();
        assert_eq!(store.as_str(), "s3://bucket/");
    }

    #[test]
    fn test_get_store_hdfs() {
        let sut = DefaultObjectStoreRegistry::default();
        let url = Url::parse("hdfs://localhost:8020").unwrap();
        sut.register_store(&url, Arc::new(LocalFileSystem::new()));
        let url = ListingTableUrl::parse("hdfs://localhost:8020/key").unwrap();
        sut.get_store(url.as_ref()).unwrap();
    }

    #[test]
    fn test_get_store_s3() {
        let sut = DefaultObjectStoreRegistry::default();
        let url = Url::parse("s3://bucket/key").unwrap();
        sut.register_store(&url, Arc::new(LocalFileSystem::new()));
        let url = ListingTableUrl::parse("s3://bucket/key").unwrap();
        sut.get_store(url.as_ref()).unwrap();
    }

    #[test]
    fn test_get_store_file() {
        let sut = DefaultObjectStoreRegistry::default();
        let url = ListingTableUrl::parse("file:///bucket/key").unwrap();
        sut.get_store(url.as_ref()).unwrap();
    }

    #[test]
    fn test_get_store_local() {
        let sut = DefaultObjectStoreRegistry::default();
        let url = ListingTableUrl::parse("../").unwrap();
        sut.get_store(url.as_ref()).unwrap();
    }

    #[test]
    fn test_url_contains() {
        let url = ListingTableUrl::parse("file:///var/data/mytable/").unwrap();

        // standard case with default config
        assert!(url.contains(
            &Path::parse("/var/data/mytable/data.parquet").unwrap(),
            true
        ));

        // standard case with `ignore_subdirectory` set to false
        assert!(url.contains(
            &Path::parse("/var/data/mytable/data.parquet").unwrap(),
            false
        ));

        // as per documentation, when `ignore_subdirectory` is true, we should ignore files that aren't
        // a direct child of the `url`
        assert!(url
            .contains(
                &Path::parse("/var/data/mytable/mysubfolder/data.parquet").unwrap(),
                true
            )
            .not());

        // when we set `ignore_subdirectory` to false, we should not ignore the file
        assert!(url.contains(
            &Path::parse("/var/data/mytable/mysubfolder/data.parquet").unwrap(),
            false
        ));

        // as above, `ignore_subdirectory` is false, so we include the file
        assert!(url.contains(
            &Path::parse("/var/data/mytable/year=2024/data.parquet").unwrap(),
            false
        ));

        // in this case, we include the file even when `ignore_subdirectory` is true because the
        // path segment is a hive partition which doesn't count as a subdirectory for the purposes
        // of `Url::contains`
        assert!(url.contains(
            &Path::parse("/var/data/mytable/year=2024/data.parquet").unwrap(),
            true
        ));

        // testing an empty path with default config
        assert!(url.contains(&Path::parse("/var/data/mytable/").unwrap(), true));

        // testing an empty path with `ignore_subdirectory` set to false
        assert!(url.contains(&Path::parse("/var/data/mytable/").unwrap(), false));
    }
}
