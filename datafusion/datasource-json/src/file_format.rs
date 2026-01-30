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

//! [`JsonFormat`]: Line delimited and array JSON [`FileFormat`] abstractions

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::source::JsonSource;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::json;
use arrow::json::reader::{ValueIter, infer_json_schema_from_iterator};
use bytes::{Buf, Bytes};
use datafusion_common::config::{ConfigField, ConfigFileType, JsonOptions};
use datafusion_common::file_options::json_writer::JsonWriterOptions;
use datafusion_common::{
    DEFAULT_JSON_EXTENSION, GetExt, Result, Statistics, not_impl_err,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::TableSchema;
use datafusion_datasource::decoder::Decoder;
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{
    DEFAULT_SCHEMA_INFER_MAX_RECORD, FileFormat, FileFormatFactory,
};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::write::BatchSerializer;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use crate::utils::JsonArrayToNdjsonReader;
use async_trait::async_trait;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

#[derive(Default)]
/// Factory struct used to create [JsonFormat]
pub struct JsonFormatFactory {
    /// the options carried by format factory
    pub options: Option<JsonOptions>,
}

impl JsonFormatFactory {
    /// Creates an instance of [JsonFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [JsonFormatFactory] with customized default options
    pub fn new_with_options(options: JsonOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl FileFormatFactory for JsonFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let json_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::JSON);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.json
            }
            Some(json_options) => {
                let mut json_options = json_options.clone();
                for (k, v) in format_options {
                    json_options.set(k, v)?;
                }
                json_options
            }
        };

        Ok(Arc::new(JsonFormat::default().with_options(json_options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(JsonFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for JsonFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_JSON_EXTENSION[1..].to_string()
    }
}

impl Debug for JsonFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonFormatFactory")
            .field("options", &self.options)
            .finish()
    }
}

/// JSON `FileFormat` implementation supporting both line-delimited and array formats.
///
/// # Supported Formats
///
/// ## Line-Delimited JSON (default, `newline_delimited = true`)
/// ```text
/// {"key1": 1, "key2": "val"}
/// {"key1": 2, "key2": "vals"}
/// ```
///
/// ## JSON Array Format (`newline_delimited = false`)
/// ```text
/// [
///     {"key1": 1, "key2": "val"},
///     {"key1": 2, "key2": "vals"}
/// ]
/// ```
///
/// Note: JSON array format is processed using streaming conversion,
/// which is memory-efficient even for large files.
#[derive(Debug, Default)]
pub struct JsonFormat {
    options: JsonOptions,
}

impl JsonFormat {
    /// Set JSON options
    pub fn with_options(mut self, options: JsonOptions) -> Self {
        self.options = options;
        self
    }

    /// Retrieve JSON options
    pub fn options(&self) -> &JsonOptions {
        &self.options
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.options.schema_infer_max_rec = Some(max_rec);
        self
    }

    /// Set a [`FileCompressionType`] of JSON
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.options.compression = file_compression_type.into();
        self
    }

    /// Set whether to read as newline-delimited JSON (NDJSON).
    ///
    /// When `true` (default), expects newline-delimited format:
    /// ```text
    /// {"a": 1}
    /// {"a": 2}
    /// ```
    ///
    /// When `false`, expects JSON array format:
    /// ```text
    /// [{"a": 1}, {"a": 2}]
    /// ```
    pub fn with_newline_delimited(mut self, newline_delimited: bool) -> Self {
        self.options.newline_delimited = newline_delimited;
        self
    }

    /// Returns whether this format expects newline-delimited JSON.
    pub fn is_newline_delimited(&self) -> bool {
        self.options.newline_delimited
    }
}

/// Infer schema from JSON array format using streaming conversion.
///
/// This function converts JSON array format to NDJSON on-the-fly and uses
/// arrow-json's schema inference. It properly tracks the number of records
/// processed for correct `records_to_read` management.
///
/// # Returns
/// A tuple of (Schema, records_consumed) where records_consumed is the
/// number of records that were processed for schema inference.
fn infer_schema_from_json_array<R: Read>(
    reader: R,
    max_records: usize,
) -> Result<(Schema, usize)> {
    let ndjson_reader = JsonArrayToNdjsonReader::new(reader);

    let iter = ValueIter::new(ndjson_reader, None);
    let mut count = 0;

    let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
        let should_take = count < max_records;
        if should_take {
            count += 1;
        }
        should_take
    }))?;

    Ok((schema, count))
}

#[async_trait]
impl FileFormat for JsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        JsonFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        Some(self.options.compression.into())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self
            .options
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let file_compression_type = FileCompressionType::from(self.options.compression);
        let newline_delimited = self.options.newline_delimited;

        for object in objects {
            // Early exit if we've read enough records
            if records_to_read == 0 {
                break;
            }

            let r = store.as_ref().get(&object.location).await?;

            let (schema, records_consumed) = match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let reader = BufReader::new(decoder);

                    if newline_delimited {
                        // NDJSON: use ValueIter directly
                        let iter = ValueIter::new(reader, None);
                        let mut count = 0;
                        let schema =
                            infer_json_schema_from_iterator(iter.take_while(|_| {
                                let should_take = count < records_to_read;
                                if should_take {
                                    count += 1;
                                }
                                should_take
                            }))?;
                        (schema, count)
                    } else {
                        // JSON array format: use streaming converter
                        infer_schema_from_json_array(reader, records_to_read)?
                    }
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let reader = BufReader::new(decoder);

                    if newline_delimited {
                        let iter = ValueIter::new(reader, None);
                        let mut count = 0;
                        let schema =
                            infer_json_schema_from_iterator(iter.take_while(|_| {
                                let should_take = count < records_to_read;
                                if should_take {
                                    count += 1;
                                }
                                should_take
                            }))?;
                        (schema, count)
                    } else {
                        // JSON array format: use streaming converter
                        infer_schema_from_json_array(reader, records_to_read)?
                    }
                }
            };

            schemas.push(schema);
            // Correctly decrement records_to_read
            records_to_read = records_to_read.saturating_sub(records_consumed);
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
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
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(FileCompressionType::from(
                self.options.compression,
            ))
            .build();
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
            return not_impl_err!("Overwrites are not implemented yet for Json");
        }

        let writer_options = JsonWriterOptions::try_from(&self.options)?;

        let sink = Arc::new(JsonSink::new(conf, writer_options));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            JsonSource::new(table_schema)
                .with_newline_delimited(self.options.newline_delimited),
        )
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Define a struct for serializing Json records to a stream
pub struct JsonSerializer {}

impl JsonSerializer {
    /// Constructor for the JsonSerializer object
    pub fn new() -> Self {
        Self {}
    }
}

impl BatchSerializer for JsonSerializer {
    fn serialize(&self, batch: RecordBatch, _initial: bool) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(4096);
        let mut writer = json::LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch)?;
        Ok(Bytes::from(buffer))
    }
}

/// Implements [`DataSink`] for writing to a Json file.
pub struct JsonSink {
    /// Config options for writing data
    config: FileSinkConfig,
    /// Writer options for underlying Json writer
    writer_options: JsonWriterOptions,
}

impl Debug for JsonSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonSink").finish()
    }
}

impl DisplayAs for JsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "JsonSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: json")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

impl JsonSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, writer_options: JsonWriterOptions) -> Self {
        Self {
            config,
            writer_options,
        }
    }

    /// Retrieve the writer options
    pub fn writer_options(&self) -> &JsonWriterOptions {
        &self.writer_options
    }
}

#[async_trait]
impl FileSink for JsonSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        context: &Arc<TaskContext>,
        demux_task: SpawnedTask<Result<()>>,
        file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<u64> {
        let serializer = Arc::new(JsonSerializer::new()) as _;
        spawn_writer_tasks_and_join(
            context,
            serializer,
            self.writer_options.compression.into(),
            self.writer_options.compression_level,
            object_store,
            demux_task,
            file_stream_rx,
        )
        .await
    }
}

#[async_trait]
impl DataSink for JsonSink {
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

#[derive(Debug)]
pub struct JsonDecoder {
    inner: json::reader::Decoder,
}

impl JsonDecoder {
    pub fn new(decoder: json::reader::Decoder) -> Self {
        Self { inner: decoder }
    }
}

impl Decoder for JsonDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.inner.decode(buf)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        false
    }
}
