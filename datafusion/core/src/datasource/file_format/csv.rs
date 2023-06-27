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

//! CSV format abstractions

use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::{self, datatypes::SchemaRef};
use arrow_array::RecordBatch;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream::BoxStream;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use object_store::{delimited::newline_delimited_stream, ObjectMeta, ObjectStore};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::FileFormat;
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::file_format::FileWriterMode;
use crate::datasource::file_format::{
    AbortMode, AbortableWrite, AsyncPutWriter, BatchSerializer, MultiPart,
    DEFAULT_SCHEMA_INFER_MAX_RECORD,
};
use crate::datasource::physical_plan::{
    CsvExec, FileGroupDisplay, FileMeta, FileScanConfig, FileSinkConfig,
};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::insert::{DataSink, InsertExec};
use crate::physical_plan::{DisplayAs, DisplayFormatType, Statistics};
use crate::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

/// The default file extension of csv files
pub const DEFAULT_CSV_EXTENSION: &str = ".csv";
/// Character Separated Value `FileFormat` implementation.
#[derive(Debug)]
pub struct CsvFormat {
    has_header: bool,
    delimiter: u8,
    schema_infer_max_rec: Option<usize>,
    file_compression_type: FileCompressionType,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_rec: Some(DEFAULT_SCHEMA_INFER_MAX_RECORD),
            has_header: true,
            delimiter: b',',
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl CsvFormat {
    /// Return a newline delimited stream from the specified file on
    /// Stream, decompressing if necessary
    /// Each returned `Bytes` has a whole number of newline delimited rows
    async fn read_to_delimited_chunks(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> BoxStream<'static, Result<Bytes>> {
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

    async fn read_to_delimited_chunks_from_stream(
        &self,
        stream: BoxStream<'static, Result<Bytes>>,
    ) -> BoxStream<'static, Result<Bytes>> {
        let file_compression_type = self.file_compression_type.to_owned();
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

    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: Option<usize>) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// True if the first line is a header.
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Set a `FileCompressionType` of CSV
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// The delimiter character.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];

        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);

        for object in objects {
            let stream = self.read_to_delimited_chunks(store, object).await;
            let (schema, records_read) = self
                .infer_schema_from_stream(records_to_read, stream)
                .await?;
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
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = CsvExec::new(
            conf,
            self.has_header,
            self.delimiter,
            self.file_compression_type.to_owned(),
        );
        Ok(Arc::new(exec))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let sink_schema = conf.output_schema().clone();
        let sink = Arc::new(CsvSink::new(
            conf,
            self.has_header,
            self.delimiter,
            self.file_compression_type.clone(),
        ));

        Ok(Arc::new(InsertExec::new(input, sink, sink_schema)) as _)
    }
}

impl CsvFormat {
    /// Return the inferred schema reading up to records_to_read from a
    /// stream of delimited chunks returning the inferred schema and the
    /// number of lines that were read
    async fn infer_schema_from_stream(
        &self,
        mut records_to_read: usize,
        stream: impl Stream<Item = Result<Bytes>>,
    ) -> Result<(Schema, usize)> {
        let mut total_records_read = 0;
        let mut column_names = vec![];
        let mut column_type_possibilities = vec![];
        let mut first_chunk = true;

        pin_mut!(stream);

        while let Some(chunk) = stream.next().await.transpose()? {
            let format = arrow::csv::reader::Format::default()
                .with_header(self.has_header && first_chunk)
                .with_delimiter(self.delimiter);

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
                first_chunk = false;
            } else {
                if fields.len() != column_type_possibilities.len() {
                    return Err(DataFusionError::Execution(
                        format!(
                            "Encountered unequal lengths between records on CSV file whilst inferring schema. \
                             Expected {} records, found {} records",
                            column_type_possibilities.len(),
                            fields.len()
                        )
                    ));
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
    // Inner buffer for avoiding reallocation
    buffer: Vec<u8>,
    // Flag to indicate whether there will be a header
    header: bool,
}

impl CsvSerializer {
    /// Constructor for the CsvSerializer object
    pub fn new() -> Self {
        Self {
            builder: WriterBuilder::new(),
            header: true,
            buffer: Vec::with_capacity(4096),
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

#[async_trait]
impl BatchSerializer for CsvSerializer {
    async fn serialize(&mut self, batch: RecordBatch) -> Result<Bytes> {
        let builder = self.builder.clone();
        let mut writer = builder.has_headers(self.header).build(&mut self.buffer);
        writer.write(&batch)?;
        drop(writer);
        self.header = false;
        Ok(Bytes::from(self.buffer.drain(..).collect::<Vec<u8>>()))
    }
}

async fn check_for_errors<T, W: AsyncWrite + Unpin + Send>(
    result: Result<T>,
    writers: &mut [AbortableWrite<W>],
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            // Abort all writers before returning the error:
            for writer in writers {
                let mut abort_future = writer.abort_writer();
                if let Ok(abort_future) = &mut abort_future {
                    let _ = abort_future.await;
                }
                // Ignore errors that occur during abortion,
                // We do try to abort all writers before returning error.
            }
            // After aborting writers return original error.
            Err(e)
        }
    }
}

/// Implements [`DataSink`] for writing to a CSV file.
struct CsvSink {
    /// Config options for writing data
    config: FileSinkConfig,
    has_header: bool,
    delimiter: u8,
    file_compression_type: FileCompressionType,
}

impl Debug for CsvSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvSink")
            .field("has_header", &self.has_header)
            .field("delimiter", &self.delimiter)
            .field("file_compression_type", &self.file_compression_type)
            .finish()
    }
}

impl DisplayAs for CsvSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "CsvSink(writer_mode={:?}, file_groups=",
                    self.config.writer_mode
                )?;
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl CsvSink {
    fn new(
        config: FileSinkConfig,
        has_header: bool,
        delimiter: u8,
        file_compression_type: FileCompressionType,
    ) -> Self {
        Self {
            config,
            has_header,
            delimiter,
            file_compression_type,
        }
    }

    // Create a write for Csv files
    async fn create_writer(
        &self,
        file_meta: FileMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
        let object = &file_meta.object_meta;
        match self.config.writer_mode {
            // If the mode is append, call the store's append method and return wrapped in
            // a boxed trait object.
            FileWriterMode::Append => {
                let writer = object_store
                    .append(&object.location)
                    .await
                    .map_err(DataFusionError::ObjectStore)?;
                let writer = AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::Append,
                );
                Ok(writer)
            }
            // If the mode is put, create a new AsyncPut writer and return it wrapped in
            // a boxed trait object
            FileWriterMode::Put => {
                let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
                let writer = AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::Put,
                );
                Ok(writer)
            }
            // If the mode is put multipart, call the store's put_multipart method and
            // return the writer wrapped in a boxed trait object.
            FileWriterMode::PutMultipart => {
                let (multipart_id, writer) = object_store
                    .put_multipart(&object.location)
                    .await
                    .map_err(DataFusionError::ObjectStore)?;
                Ok(AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::MultiPart(MultiPart::new(
                        object_store,
                        multipart_id,
                        object.location.clone(),
                    )),
                ))
            }
        }
    }
}

#[async_trait]
impl DataSink for CsvSink {
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.config.file_groups.len();

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        // Construct serializer and writer for each file group
        let mut serializers = vec![];
        let mut writers = vec![];
        for file_group in &self.config.file_groups {
            // In append mode, consider has_header flag only when file is empty (at the start).
            // For other modes, use has_header flag as is.
            let header = self.has_header
                && (!matches!(&self.config.writer_mode, FileWriterMode::Append)
                    || file_group.object_meta.size == 0);
            let builder = WriterBuilder::new().with_delimiter(self.delimiter);
            let serializer = CsvSerializer::new()
                .with_builder(builder)
                .with_header(header);
            serializers.push(serializer);

            let file = file_group.clone();
            let writer = self
                .create_writer(file.object_meta.clone().into(), object_store.clone())
                .await?;
            writers.push(writer);
        }

        let mut idx = 0;
        let mut row_count = 0;
        // Map errors to DatafusionError.
        let err_converter =
            |_| DataFusionError::Internal("Unexpected FileSink Error".to_string());
        while let Some(maybe_batch) = data.next().await {
            // Write data to files in a round robin fashion:
            idx = (idx + 1) % num_partitions;
            let serializer = &mut serializers[idx];
            let batch = check_for_errors(maybe_batch, &mut writers).await?;
            row_count += batch.num_rows();
            let bytes =
                check_for_errors(serializer.serialize(batch).await, &mut writers).await?;
            let writer = &mut writers[idx];
            check_for_errors(
                writer.write_all(&bytes).await.map_err(err_converter),
                &mut writers,
            )
            .await?;
        }
        // Perform cleanup:
        let n_writers = writers.len();
        for idx in 0..n_writers {
            check_for_errors(
                writers[idx].shutdown().await.map_err(err_converter),
                &mut writers,
            )
            .await?;
        }
        Ok(row_count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use super::*;
    use crate::assert_batches_eq;
    use crate::datasource::file_format::test_util::VariableStream;
    use crate::physical_plan::collect;
    use crate::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use crate::test_util::arrow_test_data;
    use arrow::compute::concat_batches;
    use bytes::Bytes;
    use chrono::DateTime;
    use datafusion_common::cast::as_string_array;
    use datafusion_expr::{col, lit};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use rstest::*;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        // skip column 9 that overflows the automaticly discovered column type of i64 (u64 would work)
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]);
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;
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
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3]);
        let exec =
            get_exec(&state, "aggregate_test_100.csv", projection, Some(1)).await?;
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
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;

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
                "c10: Int64",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8"
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
        let exec = get_exec(&state, "aggregate_test_100.csv", projection, None).await?;

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
        };

        let num_rows_to_read = 100;
        let csv_format = CsvFormat {
            has_header: false,
            schema_infer_max_rec: Some(num_rows_to_read),
            ..Default::default()
        };
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

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn query_compress_data(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let integration = LocalFileSystem::new_with_prefix(arrow_test_data()).unwrap();

        let path = Path::from("csv/aggregate_test_100.csv");
        let csv = CsvFormat::default().with_has_header(true);
        let records_to_read = csv.schema_infer_max_rec.unwrap_or(usize::MAX);
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
            Field::new("c10", DataType::Int64, true),
            Field::new("c11", DataType::Float64, true),
            Field::new("c12", DataType::Float64, true),
            Field::new("c13", DataType::Utf8, true),
        ]);

        let compressed_csv =
            csv.with_file_compression_type(file_compression_type.clone());

        //convert compressed_stream to decoded_stream
        let decoded_stream = compressed_csv
            .read_to_delimited_chunks_from_stream(compressed_stream.unwrap())
            .await;
        let (schema, records_read) = compressed_csv
            .infer_schema_from_stream(records_to_read, decoded_stream)
            .await?;

        assert_eq!(expected, schema);
        assert_eq!(100, records_read);
        Ok(())
    }

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
            let expected = vec![
            "+----+------+",
            "| c2 | c3   |",
            "+----+------+",
            "| 5  | 36   |",
            "| 5  | -31  |",
            "| 5  | -101 |",
            "+----+------+",
        ];
        assert_batches_eq!(expected, &record_batch);
        Ok(())
    }

    async fn get_exec(
        state: &SessionState,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let root = format!("{}/csv", crate::test_util::arrow_test_data());
        let format = CsvFormat::default();
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
        let mut serializer = CsvSerializer::new();
        let bytes = serializer.serialize(batch).await?;
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
        let mut serializer = CsvSerializer::new().with_header(false);
        let bytes = serializer.serialize(batch).await?;
        assert_eq!(
            "2,1\n5,-40\n1,29\n1,-85\n5,-82\n4,-111\n3,104\n3,13\n1,38\n4,-38\n",
            String::from_utf8(bytes.into()).unwrap()
        );
        Ok(())
    }
}
