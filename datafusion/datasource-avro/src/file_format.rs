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

//! Apache Avro [`FileFormat`] abstractions
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::{fmt, io};

use crate::read_avro_schema_from_reader;
use crate::source::AvroSource;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow_avro::errors::AvroError;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::{AvroWriter, WriterBuilder};
use datafusion_common::GetExt;
use datafusion_common::internal_err;
use datafusion_common::not_impl_err;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{DEFAULT_AVRO_EXTENSION, Diagnostic};
use datafusion_common::{DataFusionError, Result, Statistics, internal_datafusion_err};
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::{
    ObjectWriterBuilder, SharedBuffer, get_writer_schema,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use async_trait::async_trait;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};
use tokio::io::AsyncWriteExt;

/// Initial writing buffer size. Note this is just a size hint for efficiency. It
/// will grow beyond the set value if needed.
const INITIAL_BUFFER_BYTES: usize = 1048576;

/// If the buffered Avro data exceeds this size, it is flushed to object store
const BUFFER_FLUSH_BYTES: usize = 1024000;

#[derive(Default)]
/// Factory struct used to create [`AvroFormat`]
pub struct AvroFormatFactory;

impl AvroFormatFactory {
    /// Creates an instance of [`AvroFormatFactory`]
    pub fn new() -> Self {
        Self {}
    }
}

impl FileFormatFactory for AvroFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AvroFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AvroFormat)
    }
}

impl Debug for AvroFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroFormatFactory").finish()
    }
}

impl GetExt for AvroFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".avro" -> "avro"
        DEFAULT_AVRO_EXTENSION[1..].to_string()
    }
}

/// Avro [`FileFormat`] implementation.
#[derive(Default, Debug)]
pub struct AvroFormat;

#[async_trait]
impl FileFormat for AvroFormat {
    fn get_ext(&self) -> String {
        AvroFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        match file_compression_type.get_variant() {
            CompressionTypeVariant::UNCOMPRESSED => Ok(ext),
            _ => internal_err!("Avro FileFormat does not support compression."),
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
                GetResultPayload::File(mut file, _) => {
                    read_avro_schema_from_reader(&mut file)?
                }
                GetResultPayload::Stream(_) => {
                    // TODO: Fetching entire file to get schema is potentially wasteful
                    let data = r.bytes().await?;
                    read_avro_schema_from_reader(&mut data.as_ref())?
                }
            };
            schemas.push(schema);
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for Avro format");
        }

        let sink = Arc::new(AvroFileSink::new(conf));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(
        &self,
        table_schema: datafusion_datasource::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(AvroSource::new(table_schema))
    }
}

/// Implements [`FileSink`] for Avro Object Container Files
struct AvroFileSink {
    config: FileSinkConfig,
}

impl AvroFileSink {
    fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl FileSink for AvroFileSink {
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

        let writer_schema = get_writer_schema(&self.config);
        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let shared_buffer = SharedBuffer::new(INITIAL_BUFFER_BYTES);
            let mut avro_writer: AvroWriter<SharedBuffer> =
                WriterBuilder::new(writer_schema.as_ref().clone())
                    .build::<_, AvroOcfFormat>(shared_buffer.clone())
                    .map_err(|e| {
                        internal_datafusion_err!("Failed to create Avro writer: {e}")
                    })?;
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
                    avro_writer
                        .write(&batch)
                        .map_err(|e| internal_datafusion_err!("{e}"))?;
                    let mut buff_to_flush = shared_buffer.buffer.try_lock().unwrap();
                    if buff_to_flush.len() > BUFFER_FLUSH_BYTES {
                        object_store_writer
                            .write_all(buff_to_flush.as_slice())
                            .await?;
                        buff_to_flush.clear();
                    }
                }
                if let Err(e) = avro_writer.finish() {
                    return Err(match e {
                        AvroError::NYI(e) => DataFusionError::NotImplemented(e),
                        AvroError::EOF(e) => DataFusionError::IoError(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            e,
                        )),
                        AvroError::ArrowError(e) => DataFusionError::ArrowError(e, None),
                        AvroError::External(e) => DataFusionError::External(e),
                        AvroError::IoError(msg, e) => DataFusionError::IoError(e)
                            .with_diagnostic(Diagnostic::new_error(msg, None)),
                        _ => internal_datafusion_err!("{e}"),
                    });
                }
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

impl Debug for AvroFileSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AvroFileSink").finish()
    }
}

impl DisplayAs for AvroFileSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AvroFileSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: avro")?;
                write!(f, "file={}", self.config.original_url)
            }
        }
    }
}

#[async_trait]
impl DataSink for AvroFileSink {
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
