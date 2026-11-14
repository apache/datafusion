// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`JsonFormat`]: Line delimited and array JSON [`FileFormat`] abstractions

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::source::JsonSource;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema, SchemaRef};
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
use object_store::{GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt};

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

/// Returns `true` if `data_type` is, or contains at any nesting level, a
/// [`DataType::Map`].
fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_map(field.data_type()),
        DataType::Struct(fields) => {
            fields.iter().any(|field| contains_map(field.data_type()))
        }
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_map(field.data_type())),
        DataType::Dictionary(_, value_type) => contains_map(value_type),
        DataType::RunEndEncoded(_, values) => contains_map(values.data_type()),
        _ => false,
    }
}

/// Creates a newline-delimited JSON writer suited to `schema`.
///
/// By default, `arrow-json` omits any NULL value from the output. For a
/// struct field or a top level column that is lossless because a reader fills
/// the missing key back in from the schema. A `MAP` is different: the entry
/// key *is* the data, so omitting an entry whose value is NULL silently drops
/// the key and changes the cardinality of the map
/// (`{color: red, size: NULL}` would be written as `{"color":"red"}`).
///
/// Therefore, when the schema contains a `MAP` (at any nesting level), NULLs
/// are written explicitly, e.g. `{"color":"red","size":null}`. Schemas
/// without a `MAP` keep the previous, more compact output.
pub(crate) fn line_delimited_writer<W: std::io::Write>(
    writer: W,
    schema: &Schema,
) -> json::LineDelimitedWriter<W> {
    let explicit_nulls = schema
        .fields()
        .iter()
        .any(|field| contains_map(field.data_type()));
    json::WriterBuilder::new()
        .with_explicit_nulls(explicit_nulls)
        .build::<_, json::writer::LineDelimited>(writer)
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
        let mut writer = line_delimited_writer(&mut buffer, batch.schema_ref());
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
                write!(f, "JsonSink(file_groups=")?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: json")?;
                write!(f, "file={}", self.config.original_url)
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

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        exec: &DataSinkExec,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        use protobuf::physical_plan_node::PhysicalPlanType;

        // Keep an exhaustive guard in the active hook while centralizing the
        // field mapping in the exhaustive `TryFrom<&JsonSink>` below.
        let Self {
            config: _,
            writer_options: _,
        } = self;

        let input = ctx.encode_child(exec.input())?;
        let sort_order = exec.encode_sort_order(ctx)?;
        let sink = protobuf::JsonSink::try_from(self)?;
        let node = protobuf::JsonSinkExecNode {
            input: Some(Box::new(input)),
            sink: Some(sink),
            sink_schema: Some(exec.schema().as_ref().try_into()?),
            sort_order,
        };
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::JsonSink(Box::new(node))),
        }))
    }
}

#[cfg(feature = "proto")]
impl TryFrom<&JsonSink> for datafusion_proto_models::protobuf::JsonSink {
    type Error = datafusion_common::DataFusionError;

    fn try_from(value: &JsonSink) -> Result<Self> {
        // Keep this public conversion exhaustive like the active serde hook.
        let JsonSink {
            config,
            writer_options,
        } = value;
        Ok(Self {
            config: Some(config.try_into()?),
            writer_options: Some(writer_options.try_into()?),
        })
    }
}

#[cfg(feature = "proto")]
impl TryFrom<&datafusion_proto_models::protobuf::JsonSink> for JsonSink {
    type Error = datafusion_common::DataFusionError;

    fn try_from(value: &datafusion_proto_models::protobuf::JsonSink) -> Result<Self> {
        // Exhaustive destructure: new wire fields must be explicitly restored.
        let datafusion_proto_models::protobuf::JsonSink {
            config,
            writer_options,
        } = value;
        let config = FileSinkConfig::try_from(config.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "JsonSink is missing required field 'config'"
            )
        })?)?;
        let writer_options = writer_options
            .as_ref()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "JsonSink is missing required field 'writer_options'"
                )
            })?
            .try_into()?;

        Ok(Self::new(config, writer_options))
    }
}

#[cfg(feature = "proto")]
impl JsonSink {
    /// Reconstructs a [`DataSinkExec`] containing a `JsonSink` from protobuf.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_proto_models::protobuf;

        let sink_node = datafusion_physical_plan::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::JsonSink,
            "JsonSink",
        );
        // Exhaustive destructure: a new field on `JsonSinkExecNode` is a
        // compile error here rather than a silently ignored wire field.
        let protobuf::JsonSinkExecNode {
            input,
            sink,
            // The output schema is recomputed by `DataSinkExec::new`.
            sink_schema: _,
            sort_order,
        } = sink_node.as_ref();

        let input =
            ctx.decode_required_child(input.as_deref(), "JsonSinkExecNode", "input")?;
        let proto_sink = sink.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "JsonSinkExecNode is missing required field 'sink'"
            )
        })?;
        let data_sink = JsonSink::try_from(proto_sink)?;
        let sort_order = DataSinkExec::decode_sort_order(
            sort_order.as_ref(),
            ctx,
            input.schema().as_ref(),
        )?;

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(data_sink),
            sort_order,
        )))
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

/// Encode a [`JsonFormatFactory`]'s options as their protobuf form.
///
/// The reverse direction is `TryFrom<&protobuf::JsonOptions> for JsonOptions` in
/// `datafusion-proto-models`: `JsonOptions` is a `datafusion-common` type, so
/// that half cannot live here.
#[cfg(feature = "proto")]
impl From<&JsonFormatFactory> for datafusion_proto_models::protobuf::JsonOptions {
    fn from(factory: &JsonFormatFactory) -> Self {
        if let Some(options) = &factory.options {
            datafusion_proto_models::protobuf::JsonOptions {
                compression: options.compression as i32,
                schema_infer_max_rec: options.schema_infer_max_rec.map(|v| v as u64),
                compression_level: options.compression_level,
                newline_delimited: Some(options.newline_delimited),
            }
        } else {
            datafusion_proto_models::protobuf::JsonOptions::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, AsArray, Int64Array, ListArray, MapBuilder, MapFieldNames,
        StringArray, StringBuilder,
    };
    use arrow::datatypes::{Field, Fields};

    /// Builds a batch with an `id` column and an `attrs` MAP(Utf8, Utf8) column
    /// holding `{color: red, size: NULL, note: NULL}` and `{flag: NULL}`.
    fn map_batch() -> RecordBatch {
        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        builder.keys().append_value("color");
        builder.values().append_value("red");
        builder.keys().append_value("size");
        builder.values().append_null();
        builder.keys().append_value("note");
        builder.values().append_null();
        builder.append(true).unwrap();
        builder.keys().append_value("flag");
        builder.values().append_null();
        builder.append(true).unwrap();
        let attrs: ArrayRef = Arc::new(builder.finish());

        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("attrs", attrs.data_type().clone(), true),
        ]));
        RecordBatch::try_new(schema, vec![id, attrs]).unwrap()
    }

    #[test]
    fn test_contains_map() {
        let map = map_batch().schema().field(1).data_type().clone();
        assert!(contains_map(&map));
        assert!(!contains_map(&DataType::Int64));
        assert!(!contains_map(&DataType::Utf8));

        let item = Arc::new(Field::new("item", map.clone(), true));
        assert!(contains_map(&DataType::List(Arc::clone(&item))));
        assert!(contains_map(&DataType::LargeList(Arc::clone(&item))));
        assert!(contains_map(&DataType::FixedSizeList(item, 2)));
        assert!(contains_map(&DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("m", map, true),
        ]))));
        assert!(!contains_map(&DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int64, true),
        ]))));
        assert!(!contains_map(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int64,
            true
        )))));
    }

    #[test]
    fn test_serialize_map_keeps_null_values() {
        let bytes = JsonSerializer::new().serialize(map_batch(), true).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"id\":1,\"attrs\":{\"color\":\"red\",\"size\":null,\"note\":null}}\n\
             {\"id\":2,\"attrs\":{\"flag\":null}}\n"
        );
    }

    #[test]
    fn test_serialize_map_round_trip() {
        let batch = map_batch();
        let bytes = JsonSerializer::new()
            .serialize(batch.clone(), true)
            .unwrap();

        let mut reader = json::ReaderBuilder::new(batch.schema())
            .build(std::io::Cursor::new(bytes))
            .unwrap();
        let read = reader.next().unwrap().unwrap();
        assert_eq!(read.num_rows(), 2);

        let attrs = read.column(1).as_map();
        // Cardinality (number of entries per row) is preserved: 3 and 1.
        assert_eq!(attrs.value_length(0), 3);
        assert_eq!(attrs.value_length(1), 1);
        let values = attrs.values().as_string::<i32>();
        assert_eq!(values.null_count(), 3);
        assert_eq!(attrs.keys().as_string::<i32>().value(1), "size");
    }

    #[test]
    fn test_serialize_map_nested_in_list() {
        let map_batch = map_batch();
        let map_array = map_batch.column(1);
        let item = Arc::new(Field::new("item", map_array.data_type().clone(), true));
        let list = ListArray::new(
            Arc::clone(&item),
            arrow::buffer::OffsetBuffer::from_lengths([2]),
            Arc::clone(map_array),
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "maps",
            DataType::List(item),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap();

        let bytes = JsonSerializer::new().serialize(batch, true).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"maps\":[{\"color\":\"red\",\"size\":null,\"note\":null},{\"flag\":null}]}\n"
        );
    }

    #[test]
    fn test_serialize_without_map_still_omits_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])),
                Arc::new(StringArray::from(vec![None, Some("x")])),
            ],
        )
        .unwrap();

        let bytes = JsonSerializer::new().serialize(batch, true).unwrap();
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            "{\"a\":1}\n{\"b\":\"x\"}\n"
        );
    }
}
