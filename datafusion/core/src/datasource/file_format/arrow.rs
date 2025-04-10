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

//! [`ArrowFormat`]: Apache Arrow [`FileFormat`] abstractions
//!
//! Works with files following the [Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use super::file_compression_type::FileCompressionType;
use super::write::demux::DemuxedStreamReceiver;
use super::write::{create_writer, SharedBuffer};
use super::FileFormatFactory;
use crate::datasource::file_format::write::get_writer_schema;
use crate::datasource::file_format::FileFormat;
use crate::datasource::physical_plan::{ArrowSource, FileSink, FileSinkConfig};
use crate::error::Result;
use crate::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::{root_as_message, CompressionType};
use datafusion_catalog::Session;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    not_impl_err, DataFusionError, GetExt, Statistics, DEFAULT_ARROW_EXTENSION,
};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexRequirement;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_datasource::source::DataSourceExec;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};
use tokio::io::AsyncWriteExt;

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// If the buffered Arrow data exceeds this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

#[derive(Default, Debug)]
/// Factory struct used to create [ArrowFormat]
pub struct ArrowFormatFactory;

impl ArrowFormatFactory {
    /// Creates an instance of [ArrowFormatFactory]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for ArrowFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ArrowFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for ArrowFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_ARROW_EXTENSION[1..].to_string()
    }
}

/// Arrow `FileFormat` implementation.
#[derive(Default, Debug)]
pub struct ArrowFormat;

#[async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        ArrowFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => Err(DataFusionError::Internal(
                "Arrow FileFormat does not support compression.".into(),
            )),
        }
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(mut file, _) => {
                    let reader = FileReader::try_new(&mut file, None)?;
                    reader.schema()
                }
                GetResultPayload::Stream(stream) => {
                    infer_schema_from_file_stream(stream).await?
                }
            };
            schemas.push(schema.as_ref().clone());
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = Arc::new(ArrowSource::default());
        let config = FileScanConfigBuilder::from(conf)
            .with_source(source)
            .build();

        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Arrow format");
        }

        let sink = Arc::new(ArrowFileSink::new(conf));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(ArrowSource::default())
    }
}

/// Implements [`FileSink`] for writing to arrow_ipc files
struct ArrowFileSink {
    config: FileSinkConfig,
}

impl ArrowFileSink {
    fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl FileSink for ArrowFileSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        _context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let mut file_write_tasks: JoinSet<std::result::Result<usize, DataFusionError>> =
            JoinSet::new();

        let ipc_options =
            IpcWriteOptions::try_new(64, false, arrow_ipc::MetadataVersion::V5)?
                .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let shared_buffer = SharedBuffer::new(INITIAL_BUFFER_BYTES);
            let mut arrow_writer = arrow_ipc::writer::FileWriter::try_new_with_options(
                shared_buffer.clone(),
                &get_writer_schema(&self.config),
                ipc_options.clone(),
            )?;
            let mut object_store_writer = create_writer(
                FileCompressionType::UNCOMPRESSED,
                &path,
                Arc::clone(&object_store),
            )
            .await?;
            file_write_tasks.spawn(async move {
                let mut row_count = 0;
                while let Some(batch) = rx.recv().await {
                    row_count += batch.num_rows();
                    arrow_writer.write(&batch)?;
                    let mut buff_to_flush = shared_buffer.buffer.try_lock().unwrap();
                    if buff_to_flush.len() > BUFFER_FLUSH_BYTES {
                        object_store_writer
                            .write_all(buff_to_flush.as_slice())
                            .await?;
                        buff_to_flush.clear();
                    }
                }
                arrow_writer.finish()?;
                let final_buff = shared_buffer.buffer.try_lock().unwrap();

                object_store_writer.write_all(final_buff.as_slice()).await?;
                object_store_writer.shutdown().await?;
                Ok(row_count)
            });
        }

        let mut row_count = 0;
        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    row_count += r?;
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        demux_task
            .join_unwind()
            .await
            .map_err(DataFusionError::ExecutionJoin)??;
        Ok(row_count as u64)
    }
}

impl Debug for ArrowFileSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArrowFileSink").finish()
    }
}

impl DisplayAs for ArrowFileSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ArrowFileSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: arrow")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl DataSink for ArrowFileSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.config.output_schema()
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        FileSink::write_all(self, data, context).await
    }
}

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

/// Custom implementation of inferring schema. Should eventually be moved upstream to arrow-rs.
/// See <https://github.com/apache/arrow-rs/issues/5021>
async fn infer_schema_from_file_stream(
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
) -> Result<SchemaRef> {
    // Expected format:
    // <magic number "ARROW1"> - 6 bytes
    // <empty padding bytes [to 8 byte boundary]> - 2 bytes
    // <continuation: 0xFFFFFFFF> - 4 bytes, not present below v0.15.0
    // <metadata_size: int32> - 4 bytes
    // <metadata_flatbuffer: bytes>
    // <rest of file bytes>

    // So in first read we need at least all known sized sections,
    // which is 6 + 2 + 4 + 4 = 16 bytes.
    let bytes = collect_at_least_n_bytes(&mut stream, 16, None).await?;

    // Files should start with these magic bytes
    if bytes[0..6] != ARROW_MAGIC {
        return Err(ArrowError::ParseError(
            "Arrow file does not contain correct header".to_string(),
        ))?;
    }

    // Since continuation marker bytes added in later versions
    let (meta_len, rest_of_bytes_start_index) = if bytes[8..12] == CONTINUATION_MARKER {
        (&bytes[12..16], 16)
    } else {
        (&bytes[8..12], 12)
    };

    let meta_len = [meta_len[0], meta_len[1], meta_len[2], meta_len[3]];
    let meta_len = i32::from_le_bytes(meta_len);

    // Read bytes for Schema message
    let block_data = if bytes[rest_of_bytes_start_index..].len() < meta_len as usize {
        // Need to read more bytes to decode Message
        let mut block_data = Vec::with_capacity(meta_len as usize);
        // In case we had some spare bytes in our initial read chunk
        block_data.extend_from_slice(&bytes[rest_of_bytes_start_index..]);
        let size_to_read = meta_len as usize - block_data.len();
        let block_data =
            collect_at_least_n_bytes(&mut stream, size_to_read, Some(block_data)).await?;
        Cow::Owned(block_data)
    } else {
        // Already have the bytes we need
        let end_index = meta_len as usize + rest_of_bytes_start_index;
        let block_data = &bytes[rest_of_bytes_start_index..end_index];
        Cow::Borrowed(block_data)
    };

    // Decode Schema message
    let message = root_as_message(&block_data).map_err(|err| {
        ArrowError::ParseError(format!("Unable to read IPC message as metadata: {err:?}"))
    })?;
    let ipc_schema = message.header_as_schema().ok_or_else(|| {
        ArrowError::IpcError("Unable to read IPC message as schema".to_string())
    })?;
    let schema = fb_to_schema(ipc_schema);

    Ok(Arc::new(schema))
}

async fn collect_at_least_n_bytes(
    stream: &mut BoxStream<'static, object_store::Result<Bytes>>,
    n: usize,
    extend_from: Option<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut buf = extend_from.unwrap_or_else(|| Vec::with_capacity(n));
    // If extending existing buffer then ensure we read n additional bytes
    let n = n + buf.len();
    while let Some(bytes) = stream.next().await.transpose()? {
        buf.extend_from_slice(&bytes);
        if buf.len() >= n {
            break;
        }
    }
    if buf.len() < n {
        return Err(ArrowError::ParseError(
            "Unexpected end of byte stream for Arrow IPC file".to_string(),
        ))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::SessionContext;

    use chrono::DateTime;
    use object_store::{chunked::ChunkedStore, memory::InMemory, path::Path};

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        let mut bytes = std::fs::read("tests/data/example.arrow")?;
        bytes.truncate(bytes.len() - 20); // mangle end to show we don't need to read whole file
        let location = Path::parse("example.arrow")?;
        let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        in_memory_store.put(&location, bytes.into()).await?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let object_meta = ObjectMeta {
            location,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let arrow_format = ArrowFormat {};
        let expected = vec!["f0: Int64", "f1: Utf8", "f2: Boolean"];

        // Test chunk sizes where too small so we keep having to read more bytes
        // And when large enough that first read contains all we need
        for chunk_size in [7, 3000] {
            let store = Arc::new(ChunkedStore::new(in_memory_store.clone(), chunk_size));
            let inferred_schema = arrow_format
                .infer_schema(
                    &state,
                    &(store.clone() as Arc<dyn ObjectStore>),
                    std::slice::from_ref(&object_meta),
                )
                .await?;
            let actual_fields = inferred_schema
                .fields()
                .iter()
                .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>();
            assert_eq!(expected, actual_fields);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_short_stream() -> Result<()> {
        let mut bytes = std::fs::read("tests/data/example.arrow")?;
        bytes.truncate(20); // should cause error that file shorter than expected
        let location = Path::parse("example.arrow")?;
        let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        in_memory_store.put(&location, bytes.into()).await?;

        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let object_meta = ObjectMeta {
            location,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let arrow_format = ArrowFormat {};

        let store = Arc::new(ChunkedStore::new(in_memory_store.clone(), 7));
        let err = arrow_format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                std::slice::from_ref(&object_meta),
            )
            .await;

        assert!(err.is_err());
        assert_eq!(
            "Arrow error: Parser error: Unexpected end of byte stream for Arrow IPC file",
            err.unwrap_err().to_string()
        );

        Ok(())
    }
}
