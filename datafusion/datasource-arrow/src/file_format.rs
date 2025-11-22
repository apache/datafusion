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
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{Seek, SeekFrom};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::convert::fb_to_schema;
use arrow::ipc::reader::{FileReader, StreamReader};
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::{root_as_message, CompressionType};
use datafusion_common::error::Result;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    internal_datafusion_err, not_impl_err, DataFusionError, GetExt, Statistics,
    DEFAULT_ARROW_EXTENSION,
};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::{
    get_writer_schema, ObjectWriterBuilder, SharedBuffer,
};
use datafusion_datasource::TableSchema;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::LexRequirement;

use crate::source::ArrowSource;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, GetOptions, GetRange, GetResultPayload, ObjectMeta, ObjectStore,
};
use tokio::io::AsyncWriteExt;

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// If the buffered Arrow data exceeds this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

/// Factory struct used to create [`ArrowFormat`]
#[derive(Default, Debug)]
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

/// Arrow [`FileFormat`] implementation.
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
            _ => Err(internal_datafusion_err!(
                "Arrow FileFormat does not support compression."
            )),
        }
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
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
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    match FileReader::try_new(&mut file, None) {
                        Ok(reader) => reader.schema(),
                        Err(file_error) => {
                            // not in the file format, but FileReader read some bytes
                            // while trying to parse the file and so we need to rewind
                            // it to the beginning of the file
                            file.seek(SeekFrom::Start(0))?;
                            match StreamReader::try_new(&mut file, None) {
                                Ok(reader) => reader.schema(),
                                Err(stream_error) => {
                                    return Err(internal_datafusion_err!("Failed to parse Arrow file as either file format or stream format. File format error: {file_error}. Stream format error: {stream_error}"));
                                }
                            }
                        }
                    }
                }
                GetResultPayload::Stream(stream) => infer_stream_schema(stream).await?,
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
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store = state.runtime_env().object_store(&conf.object_store_url)?;
        let object_location = &conf
            .file_groups
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .files()
            .first()
            .ok_or_else(|| internal_datafusion_err!("No files found in file group"))?
            .object_meta
            .location;

        let table_schema = TableSchema::new(
            Arc::clone(conf.file_schema()),
            conf.table_partition_cols().clone(),
        );

        let mut source: Arc<dyn FileSource> =
            match is_object_in_arrow_ipc_file_format(object_store, object_location).await
            {
                Ok(true) => Arc::new(ArrowSource::new_file_source(table_schema)),
                Ok(false) => Arc::new(ArrowSource::new_stream_file_source(table_schema)),
                Err(e) => Err(e)?,
            };

        // Preserve projection from the original file source
        if let Some(projection) = conf.file_source.projection() {
            if let Some(new_source) = source.try_pushdown_projection(projection)? {
                source = new_source;
            }
        }

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

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(ArrowSource::new_file_source(table_schema))
    }
}

/// Implements [`FileSink`] for Arrow IPC files
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
        context: &Arc<TaskContext>,
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
            let mut object_store_writer = ObjectWriterBuilder::new(
                FileCompressionType::UNCOMPRESSED,
                &path,
                Arc::clone(&object_store),
            )
            .with_buffer_size(Some(
                context
                    .session_config()
                    .options()
                    .execution
                    .objectstore_writer_buffer_size,
            ))
            .build()?;
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
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;
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

// Custom implementation of inferring schema. Should eventually be moved upstream to arrow-rs.
// See <https://github.com/apache/arrow-rs/issues/5021>

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

async fn infer_stream_schema(
    mut stream: BoxStream<'static, object_store::Result<Bytes>>,
) -> Result<SchemaRef> {
    // IPC streaming format.
    // See https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
    //
    //   <SCHEMA>
    //   <DICTIONARY 0>
    //   ...
    //   <DICTIONARY k - 1>
    //   <RECORD BATCH 0>
    //   ...
    //   <DICTIONARY x DELTA>
    //   ...
    //   <DICTIONARY y DELTA>
    //   ...
    //   <RECORD BATCH n - 1>
    //   <EOS [optional]: 0xFFFFFFFF 0x00000000>

    // The streaming format is made up of a sequence of encapsulated messages.
    // See https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
    //
    //   <continuation: 0xFFFFFFFF>  (added in v0.15.0)
    //   <metadata_size: int32>
    //   <metadata_flatbuffer: bytes>
    //   <padding>
    //   <message body>
    //
    // The first message is the schema.

    // IPC file format is a wrapper around the streaming format with indexing information.
    // See https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
    //
    //   <magic number "ARROW1">
    //   <empty padding bytes [to 8 byte boundary]>
    //   <STREAMING FORMAT with EOS>
    //   <FOOTER>
    //   <FOOTER SIZE: int32>
    //   <magic number "ARROW1">

    // For the purposes of this function, the arrow "preamble" is the magic number, padding,
    // and the continuation marker. 16 bytes covers the preamble and metadata length
    // no matter which version or format is used.
    let bytes = extend_bytes_to_n_length_from_stream(vec![], 16, &mut stream).await?;

    // The preamble length is everything before the metadata length
    let preamble_len = if bytes[0..6] == ARROW_MAGIC {
        // File format starts with magic number "ARROW1"
        if bytes[8..12] == CONTINUATION_MARKER {
            // Continuation marker was added in v0.15.0
            12
        } else {
            // File format before v0.15.0
            8
        }
    } else if bytes[0..4] == CONTINUATION_MARKER {
        // Stream format after v0.15.0 starts with continuation marker
        4
    } else {
        // Stream format before v0.15.0 does not have a preamble
        0
    };

    let meta_len_bytes: [u8; 4] = bytes[preamble_len..preamble_len + 4]
        .try_into()
        .map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to read IPC message metadata length: {err:?}"
            ))
        })?;

    let meta_len = i32::from_le_bytes([
        meta_len_bytes[0],
        meta_len_bytes[1],
        meta_len_bytes[2],
        meta_len_bytes[3],
    ]);

    if meta_len < 0 {
        return Err(ArrowError::ParseError(
            "IPC message metadata length is negative".to_string(),
        )
        .into());
    }

    let bytes = extend_bytes_to_n_length_from_stream(
        bytes,
        preamble_len + 4 + (meta_len as usize),
        &mut stream,
    )
    .await?;

    let message = root_as_message(&bytes[preamble_len + 4..]).map_err(|err| {
        ArrowError::ParseError(format!("Unable to read IPC message metadata: {err:?}"))
    })?;
    let fb_schema = message.header_as_schema().ok_or_else(|| {
        ArrowError::IpcError("Unable to read IPC message schema".to_string())
    })?;
    let schema = fb_to_schema(fb_schema);

    Ok(Arc::new(schema))
}

async fn extend_bytes_to_n_length_from_stream(
    bytes: Vec<u8>,
    n: usize,
    stream: &mut BoxStream<'static, object_store::Result<Bytes>>,
) -> Result<Vec<u8>> {
    if bytes.len() >= n {
        return Ok(bytes);
    }

    let mut buf = bytes;

    while let Some(b) = stream.next().await.transpose()? {
        buf.extend_from_slice(&b);

        if buf.len() >= n {
            break;
        }
    }

    if buf.len() < n {
        return Err(ArrowError::ParseError(
            "Unexpected end of byte stream for Arrow IPC file".to_string(),
        )
        .into());
    }

    Ok(buf)
}

async fn is_object_in_arrow_ipc_file_format(
    store: Arc<dyn ObjectStore>,
    object_location: &Path,
) -> Result<bool> {
    let get_opts = GetOptions {
        range: Some(GetRange::Bounded(0..6)),
        ..Default::default()
    };
    let bytes = store
        .get_opts(object_location, get_opts)
        .await?
        .bytes()
        .await?;
    Ok(bytes.len() >= 6 && bytes[0..6] == ARROW_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::DateTime;
    use datafusion_common::config::TableOptions;
    use datafusion_common::DFSchema;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use object_store::{chunked::ChunkedStore, memory::InMemory, path::Path};

    struct MockSession {
        config: SessionConfig,
        runtime_env: Arc<RuntimeEnv>,
    }

    impl MockSession {
        fn new() -> Self {
            Self {
                config: SessionConfig::new(),
                runtime_env: Arc::new(RuntimeEnv::default()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Session for MockSession {
        fn session_id(&self) -> &str {
            unimplemented!()
        }

        fn config(&self) -> &SessionConfig {
            &self.config
        }

        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
        }

        fn create_physical_expr(
            &self,
            _expr: Expr,
            _df_schema: &DFSchema,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            unimplemented!()
        }

        fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
            unimplemented!()
        }

        fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
            unimplemented!()
        }

        fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
            unimplemented!()
        }

        fn runtime_env(&self) -> &Arc<RuntimeEnv> {
            &self.runtime_env
        }

        fn execution_props(&self) -> &ExecutionProps {
            unimplemented!()
        }

        fn as_any(&self) -> &dyn Any {
            unimplemented!()
        }

        fn table_options(&self) -> &TableOptions {
            unimplemented!()
        }

        fn table_options_mut(&mut self) -> &mut TableOptions {
            unimplemented!()
        }

        fn task_ctx(&self) -> Arc<TaskContext> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        for file in ["example.arrow", "example_stream.arrow"] {
            let mut bytes = std::fs::read(format!("tests/data/{file}"))?;
            bytes.truncate(bytes.len() - 20); // mangle end to show we don't need to read whole file
            let location = Path::parse(file)?;
            let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            in_memory_store.put(&location, bytes.into()).await?;

            let state = MockSession::new();
            let object_meta = ObjectMeta {
                location,
                last_modified: DateTime::default(),
                size: u64::MAX,
                e_tag: None,
                version: None,
            };

            let arrow_format = ArrowFormat {};
            let expected = vec!["f0: Int64", "f1: Utf8", "f2: Boolean"];

            // Test chunk sizes where too small so we keep having to read more bytes
            // And when large enough that first read contains all we need
            for chunk_size in [7, 3000] {
                let store =
                    Arc::new(ChunkedStore::new(in_memory_store.clone(), chunk_size));
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
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_short_stream() -> Result<()> {
        for file in ["example.arrow", "example_stream.arrow"] {
            let mut bytes = std::fs::read(format!("tests/data/{file}"))?;
            bytes.truncate(20); // should cause error that file shorter than expected
            let location = Path::parse(file)?;
            let in_memory_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            in_memory_store.put(&location, bytes.into()).await?;

            let state = MockSession::new();
            let object_meta = ObjectMeta {
                location,
                last_modified: DateTime::default(),
                size: u64::MAX,
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
            assert_eq!( "Arrow error: Parser error: Unexpected end of byte stream for Arrow IPC file", err.unwrap_err().to_string().lines().next().unwrap());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_format_detection_file_format() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test.arrow");

        let file_bytes = std::fs::read("tests/data/example.arrow")?;
        store.put(&path, file_bytes.into()).await?;

        let is_file = is_object_in_arrow_ipc_file_format(store.clone(), &path).await?;
        assert!(is_file, "Should detect file format");
        Ok(())
    }

    #[tokio::test]
    async fn test_format_detection_stream_format() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("test_stream.arrow");

        let stream_bytes = std::fs::read("tests/data/example_stream.arrow")?;
        store.put(&path, stream_bytes.into()).await?;

        let is_file = is_object_in_arrow_ipc_file_format(store.clone(), &path).await?;

        assert!(!is_file, "Should detect stream format (not file)");

        Ok(())
    }

    #[tokio::test]
    async fn test_format_detection_corrupted_file() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("corrupted.arrow");

        store
            .put(&path, Bytes::from(vec![0x43, 0x4f, 0x52, 0x41]).into())
            .await?;

        let is_file = is_object_in_arrow_ipc_file_format(store.clone(), &path).await?;

        assert!(
            !is_file,
            "Corrupted file should not be detected as file format"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_format_detection_empty_file() -> Result<()> {
        let store = Arc::new(InMemory::new());
        let path = Path::from("empty.arrow");

        store.put(&path, Bytes::new().into()).await?;

        let result = is_object_in_arrow_ipc_file_format(store.clone(), &path).await;

        // currently errors because it tries to read 0..6 from an empty file
        assert!(result.is_err(), "Empty file should error");

        Ok(())
    }
}
