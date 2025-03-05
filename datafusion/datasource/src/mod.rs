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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

//! A table that uses the `ObjectStore` listing capability
//! to get the list of files to process.

pub mod decoder;
pub mod display;
pub mod file;
pub mod file_compression_type;
pub mod file_format;
pub mod file_groups;
pub mod file_meta;
pub mod file_scan_config;
pub mod file_sink_config;
pub mod file_stream;
pub mod memory;
pub mod schema_adapter;
pub mod source;
mod statistics;

#[cfg(test)]
mod test_util;

pub mod url;
pub mod write;
use chrono::TimeZone;
use datafusion_common::Result;
use datafusion_common::{ScalarValue, Statistics};
use file_meta::FileMeta;
use futures::{Stream, StreamExt};
use object_store::{path::Path, ObjectMeta};
use object_store::{GetOptions, GetRange, ObjectStore};
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;

pub use self::url::ListingTableUrl;

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
    /// [`wrap_partition_type_in_dict`]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/physical_plan/file_scan_config.rs#L55
    /// [`wrap_partition_value_in_dict`]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/physical_plan/file_scan_config.rs#L62
    /// [`table_partition_cols`]: https://github.com/apache/datafusion/blob/main/datafusion/core/src/datasource/file_format/options.rs#L190
    pub partition_values: Vec<ScalarValue>,
    /// An optional file range for a more fine-grained parallel execution
    pub range: Option<FileRange>,
    /// Optional statistics that describe the data in this file if known.
    ///
    /// DataFusion relies on these statistics for planning (in particular to sort file groups),
    /// so if they are incorrect, incorrect answers may result.
    pub statistics: Option<Statistics>,
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
                size: size as usize,
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
                size: size as usize,
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
    Range(Option<Range<usize>>),
    TerminateEarly,
}

/// Calculates an appropriate byte range for reading from an object based on the
/// provided metadata.
///
/// This asynchronous function examines the `FileMeta` of an object in an object store
/// and determines the range of bytes to be read. The range calculation may adjust
/// the start and end points to align with meaningful data boundaries (like newlines).
///
/// Returns a `Result` wrapping a `RangeCalculation`, which is either a calculated byte range or an indication to terminate early.
///
/// Returns an `Error` if any part of the range calculation fails, such as issues in reading from the object store or invalid range boundaries.
pub async fn calculate_range(
    file_meta: &FileMeta,
    store: &Arc<dyn ObjectStore>,
    terminator: Option<u8>,
) -> Result<RangeCalculation> {
    let location = file_meta.location();
    let file_size = file_meta.object_meta.size;
    let newline = terminator.unwrap_or(b'\n');

    match file_meta.range {
        None => Ok(RangeCalculation::Range(None)),
        Some(FileRange { start, end }) => {
            let (start, end) = (start as usize, end as usize);

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
///
async fn find_first_newline(
    object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    start: usize,
    end: usize,
    newline: u8,
) -> Result<usize> {
    let options = GetOptions {
        range: Some(GetRange::Bounded(start..end)),
        ..Default::default()
    };

    let result = object_store.get_opts(location, options).await?;
    let mut result_stream = result.into_stream();

    let mut index = 0;

    while let Some(chunk) = result_stream.next().await.transpose()? {
        if let Some(position) = chunk.iter().position(|&byte| byte == newline) {
            return Ok(index + position);
        }

        index += chunk.len();
    }

    Ok(index)
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
