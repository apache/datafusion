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

use crate::write::demux::{start_demuxer_task, DemuxedStreamReceiver};
use crate::{ListingTableUrl, PartitionedFile};
use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::insert::DataSink;
use object_store::ObjectStore;
use std::sync::Arc;

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
