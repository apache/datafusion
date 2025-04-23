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

//! [`CsvFormat`], Comma Separated Value (CSV) [`FileFormat`] abstractions

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;

use crate::source::CsvSource;

use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion_common::config::{ConfigField, ConfigFileType, CsvOptions};
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::{
    exec_err, not_impl_err, DataFusionError, GetExt, Result, Statistics,
    DEFAULT_CSV_EXTENSION,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::decoder::Decoder;
use datafusion_datasource::display::FileGroupDisplay;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::{
    FileFormat, FileFormatFactory, DEFAULT_SCHEMA_INFER_MAX_RECORD,
};
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::orchestration::spawn_writer_tasks_and_join;
use datafusion_datasource::write::BatchSerializer;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_session::Session;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion_datasource::source::DataSourceExec;
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{delimited::newline_delimited_stream, ObjectMeta, ObjectStore};
use regex::Regex;

#[derive(Default)]
/// Factory used to create [`CsvFormat`]
pub struct CsvFormatFactory {
    /// the options for csv file read
    pub options: Option<CsvOptions>,
}

impl CsvFormatFactory {
    /// Creates an instance of [`CsvFormatFactory`]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [`CsvFormatFactory`] with customized default options
    pub fn new_with_options(options: CsvOptions) -> Self {
        Self {
            options: Some(options),
        }
    }
}

impl Debug for CsvFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvFormatFactory")
            .field("options", &self.options)
            .finish()
    }
}

impl FileFormatFactory for CsvFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let csv_options = match &self.options {
            None => {
                let mut table_options = state.default_table_options();
                table_options.set_config_format(ConfigFileType::CSV);
                table_options.alter_with_string_hash_map(format_options)?;
                table_options.csv
            }
            Some(csv_options) => {
                let mut csv_options = csv_options.clone();
                for (k, v) in format_options {
                    csv_options.set(k, v)?;
                }
                csv_options
            }
        };

        Ok(Arc::new(CsvFormat::default().with_options(csv_options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(CsvFormat::default())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for CsvFormatFactory {
    fn get_ext(&self) -> String {
        // Removes the dot, i.e. ".csv" -> "csv"
        DEFAULT_CSV_EXTENSION[1..].to_string()
    }
}

/// Character Separated Value [`FileFormat`] implementation.
#[derive(Debug, Default)]
pub struct CsvFormat {
    options: CsvOptions,
}

impl CsvFormat {
    /// Return a newline delimited stream from the specified file on
    /// Stream, decompressing if necessary
    /// Each returned `Bytes` has a whole number of newline delimited rows
    async fn read_to_delimited_chunks<'a>(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> BoxStream<'a, Result<Bytes>> {
        // stream to only read as many rows as needed into memory
        let stream = store
            .get(&object.location)
            .await
            .map_err(DataFusionError::ObjectStore);
        let stream = match stream {
            Ok(stream) => self
                .read_to_delimited_chunks_from_stream(
                    stream
                        .into_stream()
                        .map_err(DataFusionError::ObjectStore)
                        .boxed(),
                )
                .await
                .map_err(DataFusionError::from)
                .left_stream(),
            Err(e) => {
                futures::stream::once(futures::future::ready(Err(e))).right_stream()
            }
        };
        stream.boxed()
    }

    /// Convert a stream of bytes into a stream of of [`Bytes`] containing newline
    /// delimited CSV records, while accounting for `\` and `"`.
    pub async fn read_to_delimited_chunks_from_stream<'a>(
        &self,
        stream: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        let file_compression_type: FileCompressionType = self.options.compression.into();
        let decoder = file_compression_type.convert_stream(stream);
        let stream = match decoder {
            Ok(decoded_stream) => {
                newline_delimited_stream(decoded_stream.map_err(|e| match e {
                    DataFusionError::ObjectStore(e) => e,
                    err => object_store::Error::Generic {
                        store: "read to delimited chunks failed",
                        source: Box::new(err),
                    },
                }))
                .map_err(DataFusionError::from)
                .left_stream()
            }
            Err(e) => {
                futures::stream::once(futures::future::ready(Err(e))).right_stream()
            }
        };
        stream.boxed()
    }

    /// Set the csv options
    pub fn with_options(mut self, options: CsvOptions) -> Self {
        self.options = options;
        self
    }

    /// Retrieve the csv options
    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.options.schema_infer_max_rec = Some(max_rec);
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.options.has_header = Some(has_header);
        self
    }

    /// Set the regex to use for null values in the CSV reader.
    /// - default to treat empty values as null.
    pub fn with_null_regex(mut self, null_regex: Option<String>) -> Self {
        self.options.null_regex = null_regex;
        self
    }

    /// Returns `Some(true)` if the first line is a header, `Some(false)` if
    /// it is not, and `None` if it is not specified.
    pub fn has_header(&self) -> Option<bool> {
        self.options.has_header
    }

    /// Lines beginning with this byte are ignored.
    pub fn with_comment(mut self, comment: Option<u8>) -> Self {
        self.options.comment = comment;
        self
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.options.delimiter = delimiter;
        self
    }

    /// The quote character in a row.
    /// - default to '"'
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.options.quote = quote;
        self
    }

    /// The escape character in a row.
    /// - default is None
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.options.escape = escape;
        self
    }

    /// The character used to indicate the end of a row.
    /// - default to None (CRLF)
    pub fn with_terminator(mut self, terminator: Option<u8>) -> Self {
        self.options.terminator = terminator;
        self
    }

    /// Specifies whether newlines in (quoted) values are supported.
    ///
    /// Parsing newlines in quoted values may be affected by execution behaviour such as
    /// parallel file scanning. Setting this to `true` ensures that newlines in values are
    /// parsed successfully, which may reduce performance.
    ///
    /// The default behaviour depends on the `datafusion.catalog.newlines_in_values` setting.
    pub fn with_newlines_in_values(mut self, newlines_in_values: bool) -> Self {
        self.options.newlines_in_values = Some(newlines_in_values);
        self
    }

    /// Set a `FileCompressionType` of CSV
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.options.compression = file_compression_type.into();
        self
    }

    /// The delimiter character.
    pub fn delimiter(&self) -> u8 {
        self.options.delimiter
    }

    /// The quote character.
    pub fn quote(&self) -> u8 {
        self.options.quote
    }

    /// The escape character.
    pub fn escape(&self) -> Option<u8> {
        self.options.escape
    }
}

#[derive(Debug)]
pub struct CsvDecoder {
    inner: arrow::csv::reader::Decoder,
}

impl CsvDecoder {
    pub fn new(decoder: arrow::csv::reader::Decoder) -> Self {
        Self { inner: decoder }
    }
}

impl Decoder for CsvDecoder {
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError> {
        self.inner.decode(buf)
    }

    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.inner.flush()
    }

    fn can_flush_early(&self) -> bool {
        self.inner.capacity() == 0
    }
}

impl Debug for CsvSerializer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvSerializer")
            .field("header", &self.header)
            .finish()
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        CsvFormatFactory::new().get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];

        let mut records_to_read = self
            .options
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);

        for object in objects {
            let stream = self.read_to_delimited_chunks(store, object).await;
            let (schema, records_read) = self
                .infer_schema_from_stream(state, records_to_read, stream)
                .await
                .map_err(|err| {
                    DataFusionError::Context(
                        format!("Error when processing CSV file {}", &object.location),
                        Box::new(err),
                    )
                })?;
            records_to_read -= records_read;
            schemas.push(schema);
            if records_to_read == 0 {
                break;
            }
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
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Consult configuration options for default values
        let has_header = self
            .options
            .has_header
            .unwrap_or(state.config_options().catalog.has_header);
        let newlines_in_values = self
            .options
            .newlines_in_values
            .unwrap_or(state.config_options().catalog.newlines_in_values);

        let conf_builder = FileScanConfigBuilder::from(conf)
            .with_file_compression_type(self.options.compression.into())
            .with_newlines_in_values(newlines_in_values);

        let source = Arc::new(
            CsvSource::new(has_header, self.options.delimiter, self.options.quote)
                .with_escape(self.options.escape)
                .with_terminator(self.options.terminator)
                .with_comment(self.options.comment),
        );

        let config = conf_builder.with_source(source).build();

        Ok(DataSourceExec::from_data_source(config))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Overwrites are not implemented yet for CSV");
        }

        // `has_header` and `newlines_in_values` fields of CsvOptions may inherit
        // their values from session from configuration settings. To support
        // this logic, writer options are built from the copy of `self.options`
        // with updated values of these special fields.
        let has_header = self
            .options()
            .has_header
            .unwrap_or(state.config_options().catalog.has_header);
        let newlines_in_values = self
            .options()
            .newlines_in_values
            .unwrap_or(state.config_options().catalog.newlines_in_values);

        let options = self
            .options()
            .clone()
            .with_has_header(has_header)
            .with_newlines_in_values(newlines_in_values);

        let writer_options = CsvWriterOptions::try_from(&options)?;

        let sink = Arc::new(CsvSink::new(conf, writer_options));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)) as _)
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(CsvSource::default())
    }
}

impl CsvFormat {
    /// Return the inferred schema reading up to records_to_read from a
    /// stream of delimited chunks returning the inferred schema and the
    /// number of lines that were read
    pub async fn infer_schema_from_stream(
        &self,
        state: &dyn Session,
        mut records_to_read: usize,
        stream: impl Stream<Item = Result<Bytes>>,
    ) -> Result<(Schema, usize)> {
        let mut total_records_read = 0;
        let mut column_names = vec![];
        let mut column_type_possibilities = vec![];
        let mut record_number = -1;

        pin_mut!(stream);

        while let Some(chunk) = stream.next().await.transpose()? {
            record_number += 1;
            let first_chunk = record_number == 0;
            let mut format = arrow::csv::reader::Format::default()
                .with_header(
                    first_chunk
                        && self
                            .options
                            .has_header
                            .unwrap_or(state.config_options().catalog.has_header),
                )
                .with_delimiter(self.options.delimiter)
                .with_quote(self.options.quote);

            if let Some(null_regex) = &self.options.null_regex {
                let regex = Regex::new(null_regex.as_str())
                    .expect("Unable to parse CSV null regex.");
                format = format.with_null_regex(regex);
            }

            if let Some(escape) = self.options.escape {
                format = format.with_escape(escape);
            }

            if let Some(comment) = self.options.comment {
                format = format.with_comment(comment);
            }

            let (Schema { fields, .. }, records_read) =
                format.infer_schema(chunk.reader(), Some(records_to_read))?;

            records_to_read -= records_read;
            total_records_read += records_read;

            if first_chunk {
                // set up initial structures for recording inferred schema across chunks
                (column_names, column_type_possibilities) = fields
                    .into_iter()
                    .map(|field| {
                        let mut possibilities = HashSet::new();
                        if records_read > 0 {
                            // at least 1 data row read, record the inferred datatype
                            possibilities.insert(field.data_type().clone());
                        }
                        (field.name().clone(), possibilities)
                    })
                    .unzip();
            } else {
                if fields.len() != column_type_possibilities.len() {
                    return exec_err!(
                            "Encountered unequal lengths between records on CSV file whilst inferring schema. \
                             Expected {} fields, found {} fields at record {}",
                            column_type_possibilities.len(),
                            fields.len(),
                            record_number + 1
                        );
                }

                column_type_possibilities.iter_mut().zip(&fields).for_each(
                    |(possibilities, field)| {
                        possibilities.insert(field.data_type().clone());
                    },
                );
            }

            if records_to_read == 0 {
                break;
            }
        }

        let schema = build_schema_helper(column_names, &column_type_possibilities);
        Ok((schema, total_records_read))
    }
}

fn build_schema_helper(names: Vec<String>, types: &[HashSet<DataType>]) -> Schema {
    let fields = names
        .into_iter()
        .zip(types)
        .map(|(field_name, data_type_possibilities)| {
            // ripped from arrow::csv::reader::infer_reader_schema_with_csv_options
            // determine data type based on possible types
            // if there are incompatible types, use DataType::Utf8
            match data_type_possibilities.len() {
                1 => Field::new(
                    field_name,
                    data_type_possibilities.iter().next().unwrap().clone(),
                    true,
                ),
                2 => {
                    if data_type_possibilities.contains(&DataType::Int64)
                        && data_type_possibilities.contains(&DataType::Float64)
                    {
                        // we have an integer and double, fall down to double
                        Field::new(field_name, DataType::Float64, true)
                    } else {
                        // default to Utf8 for conflicting datatypes (e.g bool and int)
                        Field::new(field_name, DataType::Utf8, true)
                    }
                }
                _ => Field::new(field_name, DataType::Utf8, true),
            }
        })
        .collect::<Fields>();
    Schema::new(fields)
}

impl Default for CsvSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Define a struct for serializing CSV records to a stream
pub struct CsvSerializer {
    // CSV writer builder
    builder: WriterBuilder,
    // Flag to indicate whether there will be a header
    header: bool,
}

impl CsvSerializer {
    /// Constructor for the CsvSerializer object
    pub fn new() -> Self {
        Self {
            builder: WriterBuilder::new(),
            header: true,
        }
    }

    /// Method for setting the CSV writer builder
    pub fn with_builder(mut self, builder: WriterBuilder) -> Self {
        self.builder = builder;
        self
    }

    /// Method for setting the CSV writer header status
    pub fn with_header(mut self, header: bool) -> Self {
        self.header = header;
        self
    }
}

impl BatchSerializer for CsvSerializer {
    fn serialize(&self, batch: RecordBatch, initial: bool) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(4096);
        let builder = self.builder.clone();
        let header = self.header && initial;
        let mut writer = builder.with_header(header).build(&mut buffer);
        writer.write(&batch)?;
        drop(writer);
        Ok(Bytes::from(buffer))
    }
}

/// Implements [`DataSink`] for writing to a CSV file.
pub struct CsvSink {
    /// Config options for writing data
    config: FileSinkConfig,
    writer_options: CsvWriterOptions,
}

impl Debug for CsvSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvSink").finish()
    }
}

impl DisplayAs for CsvSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CsvSink(file_groups=",)?;
                FileGroupDisplay(&self.config.file_group).fmt_as(t, f)?;
                write!(f, ")")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format: csv")?;
                write!(f, "file={}", &self.config.original_url)
            }
        }
    }
}

impl CsvSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig, writer_options: CsvWriterOptions) -> Self {
        Self {
            config,
            writer_options,
        }
    }

    /// Retrieve the writer options
    pub fn writer_options(&self) -> &CsvWriterOptions {
        &self.writer_options
    }
}

#[async_trait]
impl FileSink for CsvSink {
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
        let builder = self.writer_options.writer_options.clone();
        let header = builder.header();
        let serializer = Arc::new(
            CsvSerializer::new()
                .with_builder(builder)
                .with_header(header),
        ) as _;
        spawn_writer_tasks_and_join(
            context,
            serializer,
            self.writer_options.compression.into(),
            object_store,
            demux_task,
            file_stream_rx,
        )
        .await
    }
}

#[async_trait]
impl DataSink for CsvSink {
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
