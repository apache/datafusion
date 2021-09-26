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

//! Execution plan for reading CSV files

use crate::error::{DataFusionError, Result};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::{common, source::Source, Partitioning};
use arrow::io::csv;
use futures::StreamExt;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;

use futures::Stream;
use std::any::Any;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use super::{
    DisplayFormatType, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use async_trait::async_trait;

/// CSV file read option
#[derive(Copy, Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".csv".
    pub file_extension: &'a str,
}

impl<'a> CsvReadOptions<'a> {
    /// Create a CSV read option with default presets
    pub fn new() -> Self {
        Self {
            has_header: true,
            schema: None,
            schema_infer_max_records: 1000,
            delimiter: b',',
            file_extension: ".csv",
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Specify the file extension for CSV file selection
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Configure delimiter setting with Option, None value will be ignored
    pub fn delimiter_option(mut self, delimiter: Option<u8>) -> Self {
        if let Some(d) = delimiter {
            self.delimiter = d;
        }
        self
    }

    /// Specify schema to use for CSV read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }
}

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    /// Where the data comes from.
    source: Source,
    /// Schema representing the CSV file
    schema: SchemaRef,
    /// Does the CSV file have a header?
    has_header: bool,
    /// An optional column delimiter. Defaults to `b','`
    delimiter: Option<u8>,
    /// File extension
    file_extension: String,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Schema after the projection has been applied
    projected_schema: SchemaRef,
    /// Batch size
    batch_size: usize,
    /// Limit in nr. of rows
    limit: Option<usize>,
}

/// Infer schema from a list of CSV files by reading through first n records
/// with `max_read_records` controlling the maximum number of records to read.
///
/// Files will be read in the given order untill n records have been reached.
///
/// If `max_read_records` is not set, all files will be read fully to infer the schema.
pub fn infer_schema_from_files(
    files: &[String],
    delimiter: u8,
    max_read_records: Option<usize>,
    has_header: bool,
) -> Result<Schema> {
    let mut schemas = vec![];
    let mut records_to_read = max_read_records.unwrap_or(std::usize::MAX);

    for fname in files.iter() {
        let mut reader = csv::read::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .from_path(fname)
            .map_err(ArrowError::from_external_error)?;

        let schema = csv::read::infer_schema(
            &mut reader,
            Some(records_to_read),
            has_header,
            &csv::read::infer,
        )?;

        schemas.push(schema);
        records_to_read -= records_to_read;
        if records_to_read == 0 {
            break;
        }
    }

    Ok(Schema::try_merge(schemas)?)
}

impl CsvExec {
    /// Create a new execution plan for reading a set of CSV files
    pub fn try_new(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file_extension = String::from(options.file_extension);

        let filenames = common::build_file_list(path, file_extension.as_str())?;
        if filenames.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "No files found at {path} with file extension {file_extension}",
                path = path,
                file_extension = file_extension.as_str()
            )));
        }

        let schema = match options.schema {
            Some(s) => s.clone(),
            None => CsvExec::try_infer_schema(&filenames, &options)?,
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            source: Source::PartitionedFiles {
                path: path.to_string(),
                filenames,
            },
            schema: Arc::new(schema),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            file_extension,
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
            limit,
        })
    }
    /// Create a new execution plan for reading from a reader
    pub fn try_new_from_reader(
        reader: impl Read + Send + Sync + 'static,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s.clone(),
            None => {
                return Err(DataFusionError::Execution(
                    "The schema must be provided in options when reading from a reader"
                        .to_string(),
                ));
            }
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()),
        };

        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema: Arc::new(schema),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            file_extension: String::new(),
            projection,
            projected_schema: Arc::new(projected_schema),
            batch_size,
            limit,
        })
    }

    /// Path to directory containing partitioned CSV files with the same schema
    pub fn path(&self) -> &str {
        self.source.path()
    }

    /// The individual files under path
    pub fn filenames(&self) -> &[String] {
        self.source.filenames()
    }

    /// Does the CSV file have a header?
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// An optional column delimiter. Defaults to `b','`
    pub fn delimiter(&self) -> Option<&u8> {
        self.delimiter.as_ref()
    }

    /// File extension
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Get the schema of the CSV file
    pub fn file_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Optional projection for which columns to load
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Infer schema for given CSV dataset
    pub fn try_infer_schema(
        filenames: &[String],
        options: &CsvReadOptions,
    ) -> Result<Schema> {
        Ok(infer_schema_from_files(
            filenames,
            options.delimiter,
            Some(options.schema_infer_max_records),
            options.has_header,
        )?)
    }
}

type Payload = ArrowResult<RecordBatch>;

fn producer_task<R: Read>(
    reader: R,
    response_tx: Sender<Payload>,
    limit: usize,
    batch_size: usize,
    delimiter: u8,
    has_header: bool,
    projection: &[usize],
    schema: Arc<Schema>,
) -> Result<()> {
    let mut reader = csv::read::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(has_header)
        .from_reader(reader);

    let mut current_read = 0;
    let mut rows = vec![csv::read::ByteRecord::default(); batch_size];
    while current_read < limit {
        let batch_size = batch_size.min(limit - current_read);
        let rows_read = csv::read::read_rows(&mut reader, 0, &mut rows[..batch_size])?;
        current_read += rows_read;

        let batch = deserialize(&rows[..rows_read], projection, schema.clone());
        response_tx
            .blocking_send(batch)
            .map_err(|x| DataFusionError::Execution(format!("{}", x)))?;
        if rows_read < batch_size {
            break;
        }
    }
    Ok(())
}

// CPU-intensive task
fn deserialize(
    rows: &[csv::read::ByteRecord],
    projection: &[usize],
    schema: SchemaRef,
) -> ArrowResult<RecordBatch> {
    csv::read::deserialize_batch(
        rows,
        schema.fields(),
        Some(projection),
        0,
        csv::read::deserialize_column,
    )
}

#[async_trait]
impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(match &self.source {
            Source::PartitionedFiles { filenames, .. } => filenames.len(),
            Source::Reader(_) => 1,
        })
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let batch_size = self.batch_size;
        let delimiter = self.delimiter.unwrap_or(b","[0]);
        let has_header = self.has_header;

        let projection = match &self.projection {
            Some(p) => p.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        let schema = self.schema.clone();

        match &self.source {
            Source::PartitionedFiles { filenames, .. } => {
                let path = filenames[partition].clone();

                let (response_tx, response_rx): (Sender<Payload>, Receiver<Payload>) =
                    channel(2);

                task::spawn_blocking(move || {
                    let reader = std::fs::File::open(path).unwrap();
                    producer_task(
                        reader,
                        response_tx,
                        limit,
                        batch_size,
                        delimiter,
                        has_header,
                        &projection,
                        schema,
                    )
                    .unwrap()
                });

                Ok(Box::pin(CsvStream::new(
                    self.projected_schema.clone(),
                    ReceiverStream::new(response_rx),
                )))
            }
            Source::Reader(rdr) => {
                if partition != 0 {
                    Err(DataFusionError::Internal(
                        "Only partition 0 is valid when CSV comes from a reader"
                            .to_string(),
                    ))
                } else if let Some(reader) = rdr.lock().unwrap().take() {
                    let (response_tx, response_rx): (Sender<Payload>, Receiver<Payload>) =
                        channel(2);

                    task::spawn_blocking(move || {
                        producer_task(
                            reader,
                            response_tx,
                            limit,
                            batch_size,
                            delimiter,
                            has_header,
                            &projection,
                            schema,
                        )
                        .unwrap()
                    });

                    Ok(Box::pin(CsvStream::new(
                        self.schema.clone(),
                        ReceiverStream::new(response_rx),
                    )))
                } else {
                    Err(DataFusionError::Execution(
                        "Error reading CSV: Data can only be read a single time when the source is a reader"
                            .to_string(),
                    ))
                }
            }
        }
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CsvExec: source={}, has_header={}",
                    self.source, self.has_header
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // TODO stats: handle statistics
        Statistics::default()
    }
}

/// Iterator over batches
struct CsvStream {
    schema: SchemaRef,
    receiver: ReceiverStream<Payload>,
}
impl CsvStream {
    /// Create an iterator for a CSV file
    pub fn new(schema: SchemaRef, receiver: ReceiverStream<Payload>) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for CsvStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.receiver.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for CsvStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::aggr_test_schema;
    use futures::StreamExt;

    #[tokio::test]
    async fn csv_exec_with_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            Some(vec![0, 2, 4]),
            1024,
            None,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(13, csv.file_schema().fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_without_projection() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.file_schema().fields().len());
        assert_eq!(13, csv.schema().fields().len());
        let mut it = csv.execute(0).await?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(13, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c2", batch_schema.field(1).name());
        assert_eq!("c3", batch_schema.field(2).name());
        Ok(())
    }

    #[tokio::test]
    async fn csv_exec_with_reader() -> Result<()> {
        let schema = aggr_test_schema();
        let testdata = crate::test_util::arrow_test_data();
        let filename = "aggregate_test_100.csv";
        let path = format!("{}/csv/{}", testdata, filename);
        let buf = std::fs::read(path).unwrap();
        let rdr = std::io::Cursor::new(buf);
        let csv = CsvExec::try_new_from_reader(
            rdr,
            CsvReadOptions::new().schema(&schema),
            Some(vec![0, 2, 4]),
            1024,
            None,
        )?;
        assert_eq!(13, csv.schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(13, csv.file_schema().fields().len());
        assert_eq!(3, csv.schema().fields().len());
        let mut stream = csv.execute(0).await?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        let batch_schema = batch.schema();
        assert_eq!(3, batch_schema.fields().len());
        assert_eq!("c1", batch_schema.field(0).name());
        assert_eq!("c3", batch_schema.field(1).name());
        assert_eq!("c5", batch_schema.field(2).name());
        Ok(())
    }
}
