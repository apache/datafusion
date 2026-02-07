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

//! Execution plan for reading JSON files (line-delimited and array formats)

use std::any::Any;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::file_format::JsonDecoder;
use crate::utils::{ChannelReader, JsonArrayToNdjsonReader};

use datafusion_common::error::{DataFusionError, Result};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::{
    ListingTableUrl, PartitionedFile, RangeCalculation, as_file_source, calculate_range,
};
use datafusion_physical_plan::projection::ProjectionExprs;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use arrow::array::RecordBatch;
use arrow::json::ReaderBuilder;
use arrow::{datatypes::SchemaRef, json};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use futures::{Stream, StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::ReceiverStream;

/// Channel buffer size for streaming JSON array processing.
/// With ~128KB average chunk size, 128 chunks ≈ 16MB buffer.
const CHANNEL_BUFFER_SIZE: usize = 128;

/// Buffer size for JsonArrayToNdjsonReader (2MB each, 4MB total for input+output)
const JSON_CONVERTER_BUFFER_SIZE: usize = 2 * 1024 * 1024;

// ============================================================================
// JsonArrayStream - Custom stream wrapper to hold SpawnedTask handles
// ============================================================================

/// A stream wrapper that holds SpawnedTask handles to keep them alive
/// until the stream is fully consumed or dropped.
///
/// This ensures cancel-safety: when the stream is dropped, the tasks
/// are properly aborted via SpawnedTask's Drop implementation.
struct JsonArrayStream {
    inner: ReceiverStream<std::result::Result<RecordBatch, arrow::error::ArrowError>>,
    /// Task that reads from object store and sends bytes to channel.
    /// Kept alive until stream is consumed or dropped.
    _read_task: SpawnedTask<()>,
    /// Task that parses JSON and sends RecordBatches.
    /// Kept alive until stream is consumed or dropped.
    _parse_task: SpawnedTask<()>,
}

impl Stream for JsonArrayStream {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
// ============================================================================
// JsonOpener and JsonSource
// ============================================================================

/// A [`FileOpener`] that opens a JSON file and yields a [`FileOpenFuture`]
pub struct JsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    /// When `true` (default), expects newline-delimited JSON (NDJSON).
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    newline_delimited: bool,
}

impl JsonOpener {
    /// Returns a [`JsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
        newline_delimited: bool,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            file_compression_type,
            object_store,
            newline_delimited,
        }
    }
}

/// JsonSource holds the extra configuration that is necessary for [`JsonOpener`]
#[derive(Clone)]
pub struct JsonSource {
    table_schema: datafusion_datasource::TableSchema,
    batch_size: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    projection: SplitProjection,
    /// When `true` (default), expects newline-delimited JSON (NDJSON).
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    newline_delimited: bool,
}

impl JsonSource {
    /// Initialize a JsonSource with the provided schema
    pub fn new(table_schema: impl Into<datafusion_datasource::TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            newline_delimited: true,
        }
    }

    /// Set whether to read as newline-delimited JSON.
    ///
    /// When `true` (default), expects newline-delimited format.
    /// When `false`, expects JSON array format `[{...}, {...}]`.
    pub fn with_newline_delimited(mut self, newline_delimited: bool) -> Self {
        self.newline_delimited = newline_delimited;
        self
    }
}

impl From<JsonSource> for Arc<dyn FileSource> {
    fn from(source: JsonSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for JsonSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Get the projected file schema for JsonOpener
        let file_schema = self.table_schema.file_schema();
        let projected_schema =
            Arc::new(file_schema.project(&self.projection.file_indices)?);

        let mut opener = Arc::new(JsonOpener {
            batch_size: self
                .batch_size
                .expect("Batch size must set before creating opener"),
            projected_schema,
            file_compression_type: base_config.file_compression_type,
            object_store,
            newline_delimited: self.newline_delimited,
        }) as Arc<dyn FileOpener>;

        // Wrap with ProjectionOpener
        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;

        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &datafusion_datasource::TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "json"
    }
}

impl FileOpener for JsonOpener {
    /// Open a partitioned JSON file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// Else `file_meta.range` is `Some(FileRange{start, end})`, which corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// Note: JSON array format does not support range-based scanning.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = Arc::clone(&self.object_store);
        let schema = Arc::clone(&self.projected_schema);
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();
        let newline_delimited = self.newline_delimited;

        // JSON array format requires reading the complete file
        if !newline_delimited && partitioned_file.range.is_some() {
            return Err(DataFusionError::NotImplemented(
                "JSON array format does not support range-based file scanning. \
                 Disable repartition_file_scans or use newline-delimited JSON format."
                    .to_string(),
            ));
        }

        Ok(Box::pin(async move {
            let calculated_range =
                calculate_range(&partitioned_file, &store, None).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    );
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store
                .get_opts(&partitioned_file.object_meta.location, options)
                .await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let bytes = match partitioned_file.range {
                        None => file_compression_type.convert_read(file)?,
                        Some(_) => {
                            file.seek(SeekFrom::Start(result.range.start as _))?;
                            let limit = result.range.end - result.range.start;
                            file_compression_type.convert_read(file.take(limit))?
                        }
                    };

                    if newline_delimited {
                        // NDJSON: use BufReader directly
                        let reader = BufReader::new(bytes);
                        let arrow_reader = ReaderBuilder::new(schema)
                            .with_batch_size(batch_size)
                            .build(reader)?;

                        Ok(futures::stream::iter(arrow_reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    } else {
                        // JSON array format: wrap with streaming converter
                        let ndjson_reader = JsonArrayToNdjsonReader::with_capacity(
                            bytes,
                            JSON_CONVERTER_BUFFER_SIZE,
                        );
                        let arrow_reader = ReaderBuilder::new(schema)
                            .with_batch_size(batch_size)
                            .build(ndjson_reader)?;

                        Ok(futures::stream::iter(arrow_reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    }
                }
                GetResultPayload::Stream(s) => {
                    if newline_delimited {
                        // Newline-delimited JSON (NDJSON) streaming reader
                        let s = s.map_err(DataFusionError::from);
                        let decoder = ReaderBuilder::new(schema)
                            .with_batch_size(batch_size)
                            .build_decoder()?;
                        let input =
                            file_compression_type.convert_stream(s.boxed())?.fuse();
                        let stream = deserialize_stream(
                            input,
                            DecoderDeserializer::new(JsonDecoder::new(decoder)),
                        );
                        Ok(stream.map_err(Into::into).boxed())
                    } else {
                        // JSON array format: streaming conversion with channel-based byte transfer
                        //
                        // Architecture:
                        // 1. Async task reads from object store stream, decompresses, sends to channel
                        // 2. Blocking task receives bytes, converts JSON array to NDJSON, parses to Arrow
                        // 3. RecordBatches are sent back via another channel
                        //
                        // Memory budget (~32MB):
                        // - sync_channel: CHANNEL_BUFFER_SIZE chunks (~16MB)
                        // - JsonArrayToNdjsonReader: 2 × JSON_CONVERTER_BUFFER_SIZE (~4MB)
                        // - Arrow JsonReader internal buffer (~8MB)
                        // - Miscellaneous (~4MB)

                        let s = s.map_err(DataFusionError::from);
                        let decompressed_stream =
                            file_compression_type.convert_stream(s.boxed())?;

                        // Channel for bytes: async producer -> sync consumer
                        let (byte_tx, byte_rx) =
                            std::sync::mpsc::sync_channel::<bytes::Bytes>(
                                CHANNEL_BUFFER_SIZE,
                            );

                        // Channel for results: sync producer -> async consumer
                        let (result_tx, result_rx) = tokio::sync::mpsc::channel(2);

                        // Async task: read from object store stream and send bytes to channel
                        // Store the SpawnedTask to keep it alive until stream is dropped
                        let read_task = SpawnedTask::spawn(async move {
                            tokio::pin!(decompressed_stream);
                            while let Some(chunk) = decompressed_stream.next().await {
                                match chunk {
                                    Ok(bytes) => {
                                        if byte_tx.send(bytes).is_err() {
                                            break; // Consumer dropped
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Error reading JSON stream: {e}");
                                        break;
                                    }
                                }
                            }
                            // byte_tx dropped here, signals EOF to ChannelReader
                        });

                        // Blocking task: receive bytes from channel and parse JSON
                        // Store the SpawnedTask to keep it alive until stream is dropped
                        let parse_task = SpawnedTask::spawn_blocking(move || {
                            let channel_reader = ChannelReader::new(byte_rx);
                            let ndjson_reader = JsonArrayToNdjsonReader::with_capacity(
                                channel_reader,
                                JSON_CONVERTER_BUFFER_SIZE,
                            );

                            match ReaderBuilder::new(schema)
                                .with_batch_size(batch_size)
                                .build(ndjson_reader)
                            {
                                Ok(arrow_reader) => {
                                    for batch_result in arrow_reader {
                                        if result_tx.blocking_send(batch_result).is_err()
                                        {
                                            break; // Receiver dropped
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = result_tx.blocking_send(Err(e));
                                }
                            }
                            // result_tx dropped here, closes the stream
                        });

                        // Wrap in JsonArrayStream to keep tasks alive until stream is consumed
                        let stream = JsonArrayStream {
                            inner: ReceiverStream::new(result_rx),
                            _read_task: read_task,
                            _parse_task: parse_task,
                        };

                        Ok(stream.map(|r| r.map_err(Into::into)).boxed())
                    }
                }
            }
        }))
    }
}

pub async fn plan_to_json(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let writer_buffer_size = task_ctx
        .session_config()
        .options()
        .execution
        .objectstore_writer_buffer_size;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.json", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer =
                BufWriter::with_capacity(storeref, file.clone(), writer_buffer_size);

            let mut buffer = Vec::with_capacity(1024);
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = json::LineDelimitedWriter::new(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
            }

            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion_datasource::FileRange;
    use futures::TryStreamExt;
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    /// Helper to create a test schema
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn test_json_array_from_file() -> Result<()> {
        // Test reading JSON array format from a file
        let json_data = r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]"#;

        let store = Arc::new(InMemory::new());
        let path = Path::from("test.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_from_stream() -> Result<()> {
        // Test reading JSON array format from object store stream (simulates S3)
        let json_data = r#"[{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}, {"id": 3, "name": "charlie"}]"#;

        // Use InMemory store which returns Stream payload
        let store = Arc::new(InMemory::new());
        let path = Path::from("test_stream.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            2, // small batch size to test multiple batches
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_nested_objects() -> Result<()> {
        // Test JSON array with nested objects and arrays
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::Utf8, true),
        ]));

        let json_data = r#"[
            {"id": 1, "data": "{\"nested\": true}"},
            {"id": 2, "data": "[1, 2, 3]"}
        ]"#;

        let store = Arc::new(InMemory::new());
        let path = Path::from("nested.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            schema,
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_empty() -> Result<()> {
        // Test empty JSON array
        let json_data = "[]";

        let store = Arc::new(InMemory::new());
        let path = Path::from("empty.json");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_range_not_supported() {
        // Test that range-based scanning returns error for JSON array format
        let store = Arc::new(InMemory::new());
        let path = Path::from("test.json");
        store
            .put(&path, PutPayload::from_static(b"[]"))
            .await
            .unwrap();

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false, // JSON array format
        );

        let meta = store.head(&path).await.unwrap();
        let mut file = PartitionedFile::new(path.to_string(), meta.size);
        file.range = Some(FileRange { start: 0, end: 10 });

        let result = opener.open(file);
        match result {
            Ok(_) => panic!("Expected error for range-based JSON array scanning"),
            Err(e) => {
                assert!(
                    e.to_string().contains("does not support range-based"),
                    "Unexpected error message: {e}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_ndjson_still_works() -> Result<()> {
        // Ensure NDJSON format still works correctly
        let json_data =
            "{\"id\": 1, \"name\": \"alice\"}\n{\"id\": 2, \"name\": \"bob\"}\n";

        let store = Arc::new(InMemory::new());
        let path = Path::from("test.ndjson");
        store
            .put(&path, PutPayload::from_static(json_data.as_bytes()))
            .await?;

        let opener = JsonOpener::new(
            1024,
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            true, // NDJSON format
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_large_file() -> Result<()> {
        // Test with a larger JSON array to verify streaming works
        let mut json_data = String::from("[");
        for i in 0..1000 {
            if i > 0 {
                json_data.push(',');
            }
            json_data.push_str(&format!(r#"{{"id": {i}, "name": "user{i}"}}"#));
        }
        json_data.push(']');

        let store = Arc::new(InMemory::new());
        let path = Path::from("large.json");
        store
            .put(&path, PutPayload::from(Bytes::from(json_data)))
            .await?;

        let opener = JsonOpener::new(
            100, // batch size of 100
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let stream = opener.open(file)?.await?;
        let batches: Vec<_> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1000);

        // Should have multiple batches due to batch_size=100
        assert!(batches.len() >= 10);

        Ok(())
    }

    #[tokio::test]
    async fn test_json_array_stream_cancellation() -> Result<()> {
        // Test that cancellation works correctly (tasks are aborted when stream is dropped)
        let mut json_data = String::from("[");
        for i in 0..10000 {
            if i > 0 {
                json_data.push(',');
            }
            json_data.push_str(&format!(r#"{{"id": {i}, "name": "user{i}"}}"#));
        }
        json_data.push(']');

        let store = Arc::new(InMemory::new());
        let path = Path::from("cancel_test.json");
        store
            .put(&path, PutPayload::from(Bytes::from(json_data)))
            .await?;

        let opener = JsonOpener::new(
            10, // small batch size
            test_schema(),
            FileCompressionType::UNCOMPRESSED,
            store.clone(),
            false,
        );

        let meta = store.head(&path).await?;
        let file = PartitionedFile::new(path.to_string(), meta.size);

        let mut stream = opener.open(file)?.await?;

        // Read only first batch, then drop the stream (simulating cancellation)
        let first_batch = stream.next().await;
        assert!(first_batch.is_some());

        // Drop the stream - this should abort the spawned tasks via SpawnedTask's Drop
        drop(stream);

        // Give tasks time to be aborted
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // If we reach here without hanging, cancellation worked
        Ok(())
    }
}
