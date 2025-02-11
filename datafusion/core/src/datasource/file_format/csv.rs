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

use super::write::orchestration::spawn_writer_tasks_and_join;
use super::{
    Decoder, DecoderDeserializer, FileFormat, FileFormatFactory,
    DEFAULT_SCHEMA_INFER_MAX_RECORD,
};
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::write::demux::DemuxedStreamReceiver;
use crate::datasource::file_format::write::BatchSerializer;
use crate::datasource::physical_plan::{
    CsvSource, FileGroupDisplay, FileScanConfig, FileSink, FileSinkConfig,
};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::insert::{DataSink, DataSinkExec};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, Statistics,
};

use arrow::array::RecordBatch;
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion_catalog::Session;
use datafusion_common::config::{ConfigField, ConfigFileType, CsvOptions};
use datafusion_common::file_options::csv_writer::CsvWriterOptions;
use datafusion_common::{
    exec_err, not_impl_err, DataFusionError, GetExt, DEFAULT_CSV_EXTENSION,
};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexRequirement;

use crate::datasource::data_source::FileSource;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{delimited::newline_delimited_stream, ObjectMeta, ObjectStore};
use regex::Regex;

#[derive(Default)]
/// Factory struct used to create [CsvFormatFactory]
pub struct CsvFormatFactory {
    /// the options for csv file read
    pub options: Option<CsvOptions>,
}

impl CsvFormatFactory {
    /// Creates an instance of [CsvFormatFactory]
    pub fn new() -> Self {
        Self { options: None }
    }

    /// Creates an instance of [CsvFormatFactory] with customized default options
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
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
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
        // Removes the dot, i.e. ".parquet" -> "parquet"
        DEFAULT_CSV_EXTENSION[1..].to_string()
    }
}

/// Character Separated Value `FileFormat` implementation.
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

    async fn read_to_delimited_chunks_from_stream<'a>(
        &self,
        stream: BoxStream<'a, Result<Bytes>>,
    ) -> BoxStream<'a, Result<Bytes>> {
        let file_compression_type: FileCompressionType = self.options.compression.into();
        let decoder = file_compression_type.convert_stream(stream);
        let steam = match decoder {
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
        steam.boxed()
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
pub(crate) struct CsvDecoder {
    inner: arrow::csv::reader::Decoder,
}

impl CsvDecoder {
    pub(crate) fn new(decoder: arrow::csv::reader::Decoder) -> Self {
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

impl From<arrow::csv::reader::Decoder> for DecoderDeserializer<CsvDecoder> {
    fn from(decoder: arrow::csv::reader::Decoder) -> Self {
        DecoderDeserializer::new(CsvDecoder::new(decoder))
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
        mut conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        conf.file_compression_type = self.options.compression.into();
        // Consult configuration options for default values
        let has_header = self
            .options
            .has_header
            .unwrap_or(state.config_options().catalog.has_header);
        let newlines_in_values = self
            .options
            .newlines_in_values
            .unwrap_or(state.config_options().catalog.newlines_in_values);
        conf.new_lines_in_values = newlines_in_values;

        let source = Arc::new(
            CsvSource::new(has_header, self.options.delimiter, self.options.quote)
                .with_escape(self.options.escape)
                .with_terminator(self.options.terminator)
                .with_comment(self.options.comment),
        );
        conf = conf.with_source(source);

        Ok(conf.new_exec())
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
    async fn infer_schema_from_stream(
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
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
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

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use super::*;
    use crate::assert_batches_eq;
    use crate::datasource::file_format::file_compression_type::FileCompressionType;
    use crate::datasource::file_format::test_util::VariableStream;
    use crate::datasource::file_format::{
        BatchDeserializer, DecoderDeserializer, DeserializerOutput,
    };
    use crate::datasource::listing::ListingOptions;
    use crate::execution::session_state::SessionStateBuilder;
    use crate::physical_plan::collect;
    use crate::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use crate::test_util::arrow_test_data;

    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::compute::concat_batches;
    use arrow::csv::ReaderBuilder;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion_common::cast::as_string_array;
    use datafusion_common::internal_err;
    use datafusion_common::stats::Precision;
    use datafusion_expr::{col, lit};

    use chrono::DateTime;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use regex::Regex;
    use rstest::*;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        // skip column 9 that overflows the automatically discovered column type of i64 (u64 would work)
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, None, true).await?;
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(12, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 50 /* 100/2 */);

        // test metadata
        assert_eq!(exec.statistics()?.num_rows, Precision::Absent);
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, Some(1), true).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let projection = None;
        let root = "./tests/data/csv";
        let format = CsvFormat::default().with_has_header(true);
        let exec = scan_format(
            &state,
            &format,
            root,
            "aggregate_test_100_with_nulls.csv",
            projection,
            None,
        )
        .await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Utf8",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8",
                "c14: Null",
                "c15: Utf8"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema_with_null_regex() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        let projection = None;
        let root = "./tests/data/csv";
        let format = CsvFormat::default()
            .with_has_header(true)
            .with_null_regex(Some("^NULL$|^$".to_string()));
        let exec = scan_format(
            &state,
            &format,
            root,
            "aggregate_test_100_with_nulls.csv",
            projection,
            None,
        )
        .await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Utf8",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8",
                "c14: Null",
                "c15: Null"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_char_column() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, None, true).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(100, batches[0].num_rows());

        let array = as_string_array(batches[0].column(0))?;
        let mut values: Vec<&str> = vec![];
        for i in 0..5 {
            values.push(array.value(i));
        }

        assert_eq!(vec!["c", "d", "b", "a", "b"], values);

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_stream() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let variable_object_store =
            Arc::new(VariableStream::new(Bytes::from("1,2,3,4,5\n"), 200));
        let object_meta = ObjectMeta {
            location: Path::parse("/")?,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let num_rows_to_read = 100;
        let csv_format = CsvFormat::default()
            .with_has_header(false)
            .with_schema_infer_max_rec(num_rows_to_read);
        let inferred_schema = csv_format
            .infer_schema(
                &state,
                &(variable_object_store.clone() as Arc<dyn ObjectStore>),
                &[object_meta],
            )
            .await?;

        let actual_fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "column_1: Int64",
                "column_2: Int64",
                "column_3: Int64",
                "column_4: Int64",
                "column_5: Int64"
            ],
            actual_fields
        );
        // ensuring on csv infer that it won't try to read entire file
        // should only read as many rows as was configured in the CsvFormat
        assert_eq!(
            num_rows_to_read,
            variable_object_store.get_iterations_detected()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_infer_schema_escape_chars() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let variable_object_store = Arc::new(VariableStream::new(
            Bytes::from(
                r#"c1,c2,c3,c4
0.3,"Here, is a comma\"",third,3
0.31,"double quotes are ok, "" quote",third again,9
0.314,abc,xyz,27"#,
            ),
            1,
        ));
        let object_meta = ObjectMeta {
            location: Path::parse("/")?,
            last_modified: DateTime::default(),
            size: usize::MAX,
            e_tag: None,
            version: None,
        };

        let num_rows_to_read = 3;
        let csv_format = CsvFormat::default()
            .with_has_header(true)
            .with_schema_infer_max_rec(num_rows_to_read)
            .with_quote(b'"')
            .with_escape(Some(b'\\'));

        let inferred_schema = csv_format
            .infer_schema(
                &state,
                &(variable_object_store.clone() as Arc<dyn ObjectStore>),
                &[object_meta],
            )
            .await?;

        let actual_fields: Vec<_> = inferred_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();

        assert_eq!(
            vec!["c1: Float64", "c2: Utf8", "c3: Utf8", "c4: Int64",],
            actual_fields
        );
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn query_compress_data(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let mut cfg = SessionConfig::new();
        cfg.options_mut().catalog.has_header = true;
        let session_state = SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .build();
        let integration = LocalFileSystem::new_with_prefix(arrow_test_data()).unwrap();
        let path = Path::from("csv/aggregate_test_100.csv");
        let csv = CsvFormat::default().with_has_header(true);
        let records_to_read = csv
            .options()
            .schema_infer_max_rec
            .unwrap_or(DEFAULT_SCHEMA_INFER_MAX_RECORD);
        let store = Arc::new(integration) as Arc<dyn ObjectStore>;
        let original_stream = store.get(&path).await?;

        //convert original_stream to compressed_stream for next step
        let compressed_stream =
            file_compression_type.to_owned().convert_to_compress_stream(
                original_stream
                    .into_stream()
                    .map_err(DataFusionError::from)
                    .boxed(),
            );

        //prepare expected schema for assert_eq
        let expected = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Int64, true),
            Field::new("c3", DataType::Int64, true),
            Field::new("c4", DataType::Int64, true),
            Field::new("c5", DataType::Int64, true),
            Field::new("c6", DataType::Int64, true),
            Field::new("c7", DataType::Int64, true),
            Field::new("c8", DataType::Int64, true),
            Field::new("c9", DataType::Int64, true),
            Field::new("c10", DataType::Utf8, true),
            Field::new("c11", DataType::Float64, true),
            Field::new("c12", DataType::Float64, true),
            Field::new("c13", DataType::Utf8, true),
        ]);

        let compressed_csv = csv.with_file_compression_type(file_compression_type);

        //convert compressed_stream to decoded_stream
        let decoded_stream = compressed_csv
            .read_to_delimited_chunks_from_stream(compressed_stream.unwrap())
            .await;
        let (schema, records_read) = compressed_csv
            .infer_schema_from_stream(&session_state, records_to_read, decoded_stream)
            .await?;

        assert_eq!(expected, schema);
        assert_eq!(100, records_read);
        Ok(())
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn query_compress_csv() -> Result<()> {
        let ctx = SessionContext::new();

        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .file_compression_type(FileCompressionType::GZIP)
            .file_extension("csv.gz");
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv.gz", arrow_test_data()),
                csv_options,
            )
            .await?;

        let record_batch = df
            .filter(col("c1").eq(lit("a")).and(col("c2").gt(lit("4"))))?
            .select_columns(&["c2", "c3"])?
            .collect()
            .await?;
        #[rustfmt::skip]
            let expected = ["+----+------+",
            "| c2 | c3   |",
            "+----+------+",
            "| 5  | 36   |",
            "| 5  | -31  |",
            "| 5  | -101 |",
            "+----+------+"];
        assert_batches_eq!(expected, &record_batch);
        Ok(())
    }

    async fn get_exec(
        state: &dyn Session,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        has_header: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let root = format!("{}/csv", arrow_test_data());
        let format = CsvFormat::default().with_has_header(has_header);
        scan_format(state, &format, &root, file_name, projection, limit).await
    }

    #[tokio::test]
    async fn test_csv_serializer() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv", arrow_test_data()),
                CsvReadOptions::default().has_header(true),
            )
            .await?;
        let batches = df
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(10))?
            .collect()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        let serializer = CsvSerializer::new();
        let bytes = serializer.serialize(batch, true)?;
        assert_eq!(
            "c2,c3\n2,1\n5,-40\n1,29\n1,-85\n5,-82\n4,-111\n3,104\n3,13\n1,38\n4,-38\n",
            String::from_utf8(bytes.into()).unwrap()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_csv_serializer_no_header() -> Result<()> {
        let ctx = SessionContext::new();
        let df = ctx
            .read_csv(
                &format!("{}/csv/aggregate_test_100.csv", arrow_test_data()),
                CsvReadOptions::default().has_header(true),
            )
            .await?;
        let batches = df
            .select_columns(&["c2", "c3"])?
            .limit(0, Some(10))?
            .collect()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        let serializer = CsvSerializer::new().with_header(false);
        let bytes = serializer.serialize(batch, true)?;
        assert_eq!(
            "2,1\n5,-40\n1,29\n1,-85\n5,-82\n4,-111\n3,104\n3,13\n1,38\n4,-38\n",
            String::from_utf8(bytes.into()).unwrap()
        );
        Ok(())
    }

    /// Explain the `sql` query under `ctx` to make sure the underlying csv scan is parallelized
    /// e.g. "DataSourceExec: file_groups={2 groups:" in plan means 2 DataSourceExec runs concurrently
    async fn count_query_csv_partitions(
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<usize> {
        let df = ctx.sql(&format!("EXPLAIN {sql}")).await?;
        let result = df.collect().await?;
        let plan = format!("{}", &pretty_format_batches(&result)?);

        let re = Regex::new(r"DataSourceExec: file_groups=\{(\d+) group").unwrap();

        if let Some(captures) = re.captures(&plan) {
            if let Some(match_) = captures.get(1) {
                let n_partitions = match_.as_str().parse::<usize>().unwrap();
                return Ok(n_partitions);
            }
        }

        internal_err!("query contains no DataSourceExec")
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_basic(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        let query = "select sum(c2) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+--------------+",
            "| sum(aggr.c2) |",
            "+--------------+",
            "| 285          |",
            "+--------------+"];
        assert_batches_eq!(expected, &query_result);
        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn test_csv_parallel_compressed(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .file_compression_type(FileCompressionType::GZIP)
            .file_extension("csv.gz");
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv.gz"),
            csv_options,
        )
        .await?;

        let query = "select sum(c3) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+--------------+",
            "| sum(aggr.c3) |",
            "+--------------+",
            "| 781          |",
            "+--------------+"];
        assert_batches_eq!(expected, &query_result);
        assert_eq!(1, actual_partitions); // Compressed csv won't be scanned in parallel

        Ok(())
    }

    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_newlines_in_values(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let csv_options = CsvReadOptions::default()
            .has_header(true)
            .newlines_in_values(true);
        let ctx = SessionContext::new_with_config(config);
        let testdata = arrow_test_data();
        ctx.register_csv(
            "aggr",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            csv_options,
        )
        .await?;

        let query = "select sum(c3) from aggr;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+--------------+",
            "| sum(aggr.c3) |",
            "+--------------+",
            "| 781          |",
            "+--------------+"];
        assert_batches_eq!(expected, &query_result);
        assert_eq!(1, actual_partitions); // csv won't be scanned in parallel when newlines_in_values is set

        Ok(())
    }

    /// Read a single empty csv file
    ///
    /// empty_0_byte.csv:
    /// (file is empty)
    #[tokio::test]
    async fn test_csv_empty_file() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "empty",
            "tests/data/empty_0_byte.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        #[rustfmt::skip]
        let expected = ["++",
            "++"];
        assert_batches_eq!(expected, &query_result);

        Ok(())
    }

    /// Read a single empty csv file with header
    ///
    /// empty.csv:
    /// c1,c2,c3
    #[tokio::test]
    async fn test_csv_empty_with_header() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "empty",
            "tests/data/empty.csv",
            CsvReadOptions::new().has_header(true),
        )
        .await?;

        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        #[rustfmt::skip]
        let expected = ["++",
            "++"];
        assert_batches_eq!(expected, &query_result);

        Ok(())
    }

    /// Read multiple empty csv files
    ///
    /// all_empty
    /// ├── empty0.csv
    /// ├── empty1.csv
    /// └── empty2.csv
    ///
    /// empty0.csv/empty1.csv/empty2.csv:
    /// (file is empty)
    #[tokio::test]
    async fn test_csv_multiple_empty_files() -> Result<()> {
        // Testing that partitioning doesn't break with empty files
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let file_format = Arc::new(CsvFormat::default().with_has_header(false));
        let listing_options = ListingOptions::new(file_format.clone())
            .with_file_extension(file_format.get_ext());
        ctx.register_listing_table(
            "empty",
            "tests/data/empty_files/all_empty/",
            listing_options,
            None,
            None,
        )
        .await
        .unwrap();

        // Require a predicate to enable repartition for the optimizer
        let query = "select * from empty where random() > 0.5;";
        let query_result = ctx.sql(query).await?.collect().await?;

        #[rustfmt::skip]
        let expected = ["++",
            "++"];
        assert_batches_eq!(expected, &query_result);

        Ok(())
    }

    /// Read multiple csv files (some are empty) in parallel
    ///
    /// some_empty
    /// ├── a_empty.csv
    /// ├── b.csv
    /// ├── c_empty.csv
    /// ├── d.csv
    /// └── e_empty.csv
    ///
    /// a_empty.csv/c_empty.csv/e_empty.csv:
    /// (file is empty)
    ///
    /// b.csv/d.csv:
    /// 1\n
    /// 1\n
    /// 1\n
    /// 1\n
    /// 1\n
    #[rstest(n_partitions, case(1), case(2), case(3), case(4))]
    #[tokio::test]
    async fn test_csv_parallel_some_file_empty(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        let file_format = Arc::new(CsvFormat::default().with_has_header(false));
        let listing_options = ListingOptions::new(file_format.clone())
            .with_file_extension(file_format.get_ext());
        ctx.register_listing_table(
            "empty",
            "tests/data/empty_files/some_empty",
            listing_options,
            None,
            None,
        )
        .await
        .unwrap();

        // Require a predicate to enable repartition for the optimizer
        let query = "select sum(column_1) from empty where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+---------------------+",
            "| sum(empty.column_1) |",
            "+---------------------+",
            "| 10                  |",
            "+---------------------+"];
        assert_batches_eq!(expected, &query_result);
        assert_eq!(n_partitions, actual_partitions); // Won't get partitioned if all files are empty

        Ok(())
    }

    /// Parallel scan on a csv file with only 1 byte in each line
    /// Testing partition byte range land on line boundaries
    ///
    /// one_col.csv:
    /// 5\n
    /// 5\n
    /// (...10 rows total)
    #[rstest(n_partitions, case(1), case(2), case(3), case(5), case(10), case(32))]
    #[tokio::test]
    async fn test_csv_parallel_one_col(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);

        ctx.register_csv(
            "one_col",
            "tests/data/one_col.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select sum(column_1) from one_col where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+-----------------------+",
            "| sum(one_col.column_1) |",
            "+-----------------------+",
            "| 50                    |",
            "+-----------------------+"];

        let file_size = std::fs::metadata("tests/data/one_col.csv")?.len() as usize;
        // A 20-Byte file at most get partitioned into 20 chunks
        let expected_partitions = if n_partitions <= file_size {
            n_partitions
        } else {
            file_size
        };
        assert_batches_eq!(expected, &query_result);
        assert_eq!(expected_partitions, actual_partitions);

        Ok(())
    }

    /// Parallel scan on a csv file with 2 wide rows
    /// The byte range of a partition might be within some line
    ///
    /// wode_rows.csv:
    /// 1, 1, ..., 1\n (100 columns total)
    /// 2, 2, ..., 2\n
    #[rstest(n_partitions, case(1), case(2), case(10), case(16))]
    #[tokio::test]
    async fn test_csv_parallel_wide_rows(n_partitions: usize) -> Result<()> {
        let config = SessionConfig::new()
            .with_repartition_file_scans(true)
            .with_repartition_file_min_size(0)
            .with_target_partitions(n_partitions);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_csv(
            "wide_rows",
            "tests/data/wide_rows.csv",
            CsvReadOptions::new().has_header(false),
        )
        .await?;

        let query = "select sum(column_1) + sum(column_33) + sum(column_50) + sum(column_77) + sum(column_100) as sum_of_5_cols from wide_rows where column_1 > 0;";
        let query_result = ctx.sql(query).await?.collect().await?;
        let actual_partitions = count_query_csv_partitions(&ctx, query).await?;

        #[rustfmt::skip]
        let expected = ["+---------------+",
            "| sum_of_5_cols |",
            "+---------------+",
            "| 15            |",
            "+---------------+"];
        assert_batches_eq!(expected, &query_result);
        assert_eq!(n_partitions, actual_partitions);

        Ok(())
    }

    #[rstest]
    fn test_csv_deserializer_with_finish(
        #[values(1, 5, 17)] batch_size: usize,
        #[values(0, 5, 93)] line_count: usize,
    ) -> Result<()> {
        let schema = csv_schema();
        let generator = CsvBatchGenerator::new(batch_size, line_count);
        let mut deserializer = csv_deserializer(batch_size, &schema);

        for data in generator {
            deserializer.digest(data);
        }
        deserializer.finish();

        let batch_count = line_count.div_ceil(batch_size);

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..batch_count {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {:?}", output);
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?;
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::InputExhausted);

        let expected = csv_expected_batch(schema, line_count)?;

        assert_eq!(
            expected.clone(),
            all_batches.clone(),
            "Expected:\n{}\nActual:\n{}",
            pretty_format_batches(&[expected])?,
            pretty_format_batches(&[all_batches])?,
        );

        Ok(())
    }

    #[rstest]
    fn test_csv_deserializer_without_finish(
        #[values(1, 5, 17)] batch_size: usize,
        #[values(0, 5, 93)] line_count: usize,
    ) -> Result<()> {
        let schema = csv_schema();
        let generator = CsvBatchGenerator::new(batch_size, line_count);
        let mut deserializer = csv_deserializer(batch_size, &schema);

        for data in generator {
            deserializer.digest(data);
        }

        let batch_count = line_count / batch_size;

        let mut all_batches = RecordBatch::new_empty(schema.clone());
        for _ in 0..batch_count {
            let output = deserializer.next()?;
            let DeserializerOutput::RecordBatch(batch) = output else {
                panic!("Expected RecordBatch, got {:?}", output);
            };
            all_batches = concat_batches(&schema, &[all_batches, batch])?;
        }
        assert_eq!(deserializer.next()?, DeserializerOutput::RequiresMoreData);

        let expected = csv_expected_batch(schema, batch_count * batch_size)?;

        assert_eq!(
            expected.clone(),
            all_batches.clone(),
            "Expected:\n{}\nActual:\n{}",
            pretty_format_batches(&[expected])?,
            pretty_format_batches(&[all_batches])?,
        );

        Ok(())
    }

    struct CsvBatchGenerator {
        batch_size: usize,
        line_count: usize,
        offset: usize,
    }

    impl CsvBatchGenerator {
        fn new(batch_size: usize, line_count: usize) -> Self {
            Self {
                batch_size,
                line_count,
                offset: 0,
            }
        }
    }

    impl Iterator for CsvBatchGenerator {
        type Item = Bytes;

        fn next(&mut self) -> Option<Self::Item> {
            // Return `batch_size` rows per batch:
            let mut buffer = Vec::new();
            for _ in 0..self.batch_size {
                if self.offset >= self.line_count {
                    break;
                }
                buffer.extend_from_slice(&csv_line(self.offset));
                self.offset += 1;
            }

            (!buffer.is_empty()).then(|| buffer.into())
        }
    }

    fn csv_expected_batch(
        schema: SchemaRef,
        line_count: usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut c1 = Vec::with_capacity(line_count);
        let mut c2 = Vec::with_capacity(line_count);
        let mut c3 = Vec::with_capacity(line_count);
        let mut c4 = Vec::with_capacity(line_count);

        for i in 0..line_count {
            let (int_value, float_value, bool_value, char_value) = csv_values(i);
            c1.push(int_value);
            c2.push(float_value);
            c3.push(bool_value);
            c4.push(char_value);
        }

        let expected = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(c1)),
                Arc::new(Float64Array::from(c2)),
                Arc::new(BooleanArray::from(c3)),
                Arc::new(StringArray::from(c4)),
            ],
        )?;
        Ok(expected)
    }

    fn csv_line(line_number: usize) -> Bytes {
        let (int_value, float_value, bool_value, char_value) = csv_values(line_number);
        format!(
            "{},{},{},{}\n",
            int_value, float_value, bool_value, char_value
        )
        .into()
    }

    fn csv_values(line_number: usize) -> (i32, f64, bool, String) {
        let int_value = line_number as i32;
        let float_value = line_number as f64;
        let bool_value = line_number % 2 == 0;
        let char_value = format!("{}-string", line_number);
        (int_value, float_value, bool_value, char_value)
    }

    fn csv_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Boolean, true),
            Field::new("c4", DataType::Utf8, true),
        ]))
    }

    fn csv_deserializer(
        batch_size: usize,
        schema: &Arc<Schema>,
    ) -> impl BatchDeserializer<Bytes> {
        let decoder = ReaderBuilder::new(schema.clone())
            .with_batch_size(batch_size)
            .build_decoder();
        DecoderDeserializer::new(CsvDecoder::new(decoder))
    }
}
