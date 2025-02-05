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

//! Execution plans that read file formats

mod arrow_file;
mod avro;
mod csv;
mod file_groups;
// mod file_scan_config;
mod file_stream;
mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
// mod statistics;

pub(crate) use self::csv::plan_to_csv;
pub(crate) use self::json::plan_to_json;
#[cfg(feature = "parquet")]
pub use self::parquet::{ParquetExec, ParquetFileMetrics, ParquetFileReaderFactory};

pub use arrow_file::ArrowExec;
pub use avro::AvroExec;
pub use csv::{CsvConfig, CsvExec, CsvExecBuilder, CsvOpener};
pub use datafusion_catalog_listing::file_scan_config::*;
use datafusion_expr::dml::InsertOp;
pub use file_groups::FileGroupPartitioner;
pub use file_stream::{FileOpenFuture, FileOpener, FileStream, OnError};
pub use json::{JsonOpener, NdJsonExec};

use std::{ops::Range, sync::Arc};

use super::{file_format::write::demux::start_demuxer_task, listing::ListingTableUrl};
use crate::datasource::file_format::write::demux::DemuxedStreamReceiver;
use crate::datasource::{
    listing::{FileRange, PartitionedFile},
    object_store::ObjectStoreUrl,
};
use crate::error::Result;
use crate::physical_plan::DisplayAs;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::insert::DataSink;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::{path::Path, GetOptions, GetRange, ObjectMeta, ObjectStore};

/// General behaviors for files that do `DataSink` operations
#[async_trait]
pub trait FileSink: DataSink {
    /// Retrieves the file sink configuration.
    fn config(&self) -> &FileSinkConfig;

    /// Spawns writer tasks and joins them to perform file writing operations.
    /// Is a critical part of `FileSink` trait, since it's the very last step for `write_all`.
    ///
    /// This function handles the process of writing data to files by:
    /// 1. Spawning tasks for writing data to individual files.
    /// 2. Coordinating the tasks using a demuxer to distribute data among files.
    /// 3. Collecting results using `tokio::join`, ensuring that all tasks complete successfully.
    ///
    /// # Parameters
    /// - `context`: The execution context (`TaskContext`) that provides resources
    ///   like memory management and runtime environment.
    /// - `demux_task`: A spawned task that handles demuxing, responsible for splitting
    ///   an input [`SendableRecordBatchStream`] into dynamically determined partitions.
    ///   See `start_demuxer_task()`
    /// - `file_stream_rx`: A receiver that yields streams of record batches and their
    ///   corresponding file paths for writing. See `start_demuxer_task()`
    /// - `object_store`: A handle to the object store where the files are written.
    ///
    /// # Returns
    /// - `Result<u64>`: Returns the total number of rows written across all files.
    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64>;

    /// File sink implementation of the [`DataSink::write_all`] method.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let config = self.config();
        let object_store = context
            .runtime_env()
            .object_store(&config.object_store_url)?;
        let (demux_task, file_stream_rx) = start_demuxer_task(config, data, context);
        self.spawn_writer_tasks_and_join(
            context,
            demux_task,
            file_stream_rx,
            object_store,
        )
        .await
    }
}

/// The base configurations to provide when creating a physical plan for
/// writing to any given file format.
pub struct FileSinkConfig {
    /// Object store URL, used to get an ObjectStore instance
    pub object_store_url: ObjectStoreUrl,
    /// A vector of [`PartitionedFile`] structs, each representing a file partition
    pub file_groups: Vec<PartitionedFile>,
    /// Vector of partition paths
    pub table_paths: Vec<ListingTableUrl>,
    /// The schema of the output file
    pub output_schema: SchemaRef,
    /// A vector of column names and their corresponding data types,
    /// representing the partitioning columns for the file
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Controls how new data should be written to the file, determining whether
    /// to append to, overwrite, or replace records in existing files.
    pub insert_op: InsertOp,
    /// Controls whether partition columns are kept for the file
    pub keep_partition_by_columns: bool,
    /// File extension without a dot(.)
    pub file_extension: String,
}

impl FileSinkConfig {
    /// Get output schema
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

/// A single file or part of a file that should be read, along with its schema, statistics
pub struct FileMeta {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub object_meta: ObjectMeta,
    /// An optional file range for a more fine-grained parallel execution
    pub range: Option<FileRange>,
    /// An optional field for user defined per object metadata
    pub extensions: Option<Arc<dyn std::any::Any + Send + Sync>>,
    /// Size hint for the metadata of this file
    pub metadata_size_hint: Option<usize>,
}

impl FileMeta {
    /// The full path to the object
    pub fn location(&self) -> &Path {
        &self.object_meta.location
    }
}

impl From<ObjectMeta> for FileMeta {
    fn from(object_meta: ObjectMeta) -> Self {
        Self {
            object_meta,
            range: None,
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
enum RangeCalculation {
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
async fn calculate_range(
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
    use super::*;
    use crate::physical_plan::{DefaultDisplay, VerboseDisplay};

    use crate::datasource::physical_plan::FileGroupDisplay;
    use crate::datasource::schema_adapter::{
        DefaultSchemaAdapterFactory, SchemaAdapterFactory,
    };
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Float64Type, UInt32Type};
    use arrow_array::{
        BinaryArray, BooleanArray, Float32Array, Int32Array, Int64Array, RecordBatch,
        StringArray, UInt64Array,
    };
    use arrow_schema::{Field, Schema};
    use chrono::Utc;

    #[test]
    fn schema_mapping_map_batch() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt32, true),
            Field::new("c3", DataType::Float64, true),
        ]));

        let adapter = DefaultSchemaAdapterFactory
            .create(table_schema.clone(), table_schema.clone());

        let file_schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt64, true),
            Field::new("c3", DataType::Float32, true),
        ]);

        let (mapping, _) = adapter.map_schema(&file_schema).expect("map schema failed");

        let c1 = StringArray::from(vec!["hello", "world"]);
        let c2 = UInt64Array::from(vec![9_u64, 5_u64]);
        let c3 = Float32Array::from(vec![2.0_f32, 7.0_f32]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
        )
        .unwrap();

        let mapped_batch = mapping.map_batch(batch).unwrap();

        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 3);
        assert_eq!(mapped_batch.num_rows(), 2);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<UInt32Type>();
        let c3 = mapped_batch.column(2).as_primitive::<Float64Type>();

        assert_eq!(c1.value(0), "hello");
        assert_eq!(c1.value(1), "world");
        assert_eq!(c2.value(0), 9_u32);
        assert_eq!(c2.value(1), 5_u32);
        assert_eq!(c3.value(0), 2.0_f64);
        assert_eq!(c3.value(1), 7.0_f64);
    }

    #[test]
    fn schema_adapter_map_schema_with_projection() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Int32, true),
            Field::new("c4", DataType::Float32, true),
        ]));

        let file_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("c1", DataType::Boolean, true),
            Field::new("c2", DataType::Float32, true),
            Field::new("c3", DataType::Binary, true),
            Field::new("c4", DataType::Int64, true),
        ]);

        let indices = vec![1, 2, 4];
        let schema = SchemaRef::from(table_schema.project(&indices).unwrap());
        let adapter = DefaultSchemaAdapterFactory.create(schema, table_schema.clone());
        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let c1 = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let c2 = Float32Array::from(vec![Some(2.0_f32), Some(7.0_f32), Some(3.0_f32)]);
        let c3 = BinaryArray::from_opt_vec(vec![
            Some(b"hallo"),
            Some(b"danke"),
            Some(b"super"),
        ]);
        let c4 = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![
                Arc::new(id),
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
            ],
        )
        .unwrap();
        let rows_num = batch.num_rows();
        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        assert_eq!(
            mapped_batch.schema(),
            Arc::new(table_schema.project(&indices).unwrap())
        );
        assert_eq!(mapped_batch.num_columns(), indices.len());
        assert_eq!(mapped_batch.num_rows(), rows_num);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<Float64Type>();
        let c4 = mapped_batch.column(2).as_primitive::<Float32Type>();

        assert_eq!(c1.value(0), "true");
        assert_eq!(c1.value(1), "false");
        assert_eq!(c1.value(2), "true");

        assert_eq!(c2.value(0), 2.0_f64);
        assert_eq!(c2.value(1), 7.0_f64);
        assert_eq!(c2.value(2), 3.0_f64);

        assert_eq!(c4.value(0), 1.0_f32);
        assert_eq!(c4.value(1), 2.0_f32);
        assert_eq!(c4.value(2), 3.0_f32);
    }

    #[test]
    fn file_groups_display_empty() {
        let expected = "{0 groups: []}";
        assert_eq!(DefaultDisplay(FileGroupsDisplay(&[])).to_string(), expected);
    }

    #[test]
    fn file_groups_display_one() {
        let files = [vec![partitioned_file("foo"), partitioned_file("bar")]];

        let expected = "{1 group: [[foo, bar]]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_many_default() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![],
        ];

        let expected = "{3 groups: [[foo, bar], [baz], []]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_many_verbose() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![],
        ];

        let expected = "{3 groups: [[foo, bar], [baz], []]}";
        assert_eq!(
            VerboseDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_too_many_default() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![partitioned_file("qux")],
            vec![partitioned_file("quux")],
            vec![partitioned_file("quuux")],
            vec![partitioned_file("quuuux")],
            vec![],
        ];

        let expected = "{7 groups: [[foo, bar], [baz], [qux], [quux], [quuux], ...]}";
        assert_eq!(
            DefaultDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_groups_display_too_many_verbose() {
        let files = [
            vec![partitioned_file("foo"), partitioned_file("bar")],
            vec![partitioned_file("baz")],
            vec![partitioned_file("qux")],
            vec![partitioned_file("quux")],
            vec![partitioned_file("quuux")],
            vec![partitioned_file("quuuux")],
            vec![],
        ];

        let expected =
            "{7 groups: [[foo, bar], [baz], [qux], [quux], [quuux], [quuuux], []]}";
        assert_eq!(
            VerboseDisplay(FileGroupsDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_many_default() {
        let files = vec![partitioned_file("foo"), partitioned_file("bar")];

        let expected = "[foo, bar]";
        assert_eq!(
            DefaultDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_too_many_default() {
        let files = vec![
            partitioned_file("foo"),
            partitioned_file("bar"),
            partitioned_file("baz"),
            partitioned_file("qux"),
            partitioned_file("quux"),
            partitioned_file("quuux"),
        ];

        let expected = "[foo, bar, baz, qux, quux, ...]";
        assert_eq!(
            DefaultDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    #[test]
    fn file_group_display_too_many_verbose() {
        let files = vec![
            partitioned_file("foo"),
            partitioned_file("bar"),
            partitioned_file("baz"),
            partitioned_file("qux"),
            partitioned_file("quux"),
            partitioned_file("quuux"),
        ];

        let expected = "[foo, bar, baz, qux, quux, quuux]";
        assert_eq!(
            VerboseDisplay(FileGroupDisplay(&files)).to_string(),
            expected
        );
    }

    /// create a PartitionedFile for testing
    fn partitioned_file(path: &str) -> PartitionedFile {
        let object_meta = ObjectMeta {
            location: Path::parse(path).unwrap(),
            last_modified: Utc::now(),
            size: 42,
            e_tag: None,
            version: None,
        };

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
