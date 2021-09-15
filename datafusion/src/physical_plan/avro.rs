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

//! Execution plan for reading line-delimited Avro files
#[cfg(feature = "avro")]
use super::RecordBatchStream;
use super::{common, source::Source, ExecutionPlan, Partitioning};
use crate::avro_to_arrow::read_avro_schema_from_reader;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::{DisplayFormatType, Statistics};
use arrow::datatypes::{Schema, SchemaRef};
#[cfg(feature = "avro")]
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
#[cfg(feature = "avro")]
use futures::Stream;
use std::fs::File;
use std::{any::Any, io::Seek};
use std::{
    io::Read,
    sync::{Arc, Mutex},
};
#[cfg(feature = "avro")]
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Line-delimited Avro read options
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".avro".
    pub file_extension: &'a str,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: ".avro",
        }
    }
}

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}
/// Execution plan for scanning Avro data source
#[derive(Debug)]
pub struct AvroExec {
    source: Source<Box<dyn SeekRead + Send + Sync>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    file_extension: String,
    batch_size: usize,
    limit: Option<usize>,
}

impl AvroExec {
    /// Create a new execution plan for reading from a path
    pub fn try_from_path(
        path: &str,
        options: AvroReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file_extension = options.file_extension.to_string();

        let filenames = common::build_file_list(path, &file_extension)?;

        if filenames.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "No files found at {path} with file extension {file_extension}",
                path = path,
                file_extension = file_extension.as_str()
            )));
        }

        let schema = match options.schema {
            Some(s) => s,
            None => Arc::new(AvroExec::try_read_schema(filenames.as_slice())?),
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Ok(Self {
            source: Source::PartitionedFiles {
                path: path.to_string(),
                filenames,
            },
            schema,
            projected_schema,
            file_extension,
            projection,
            batch_size,
            limit,
        })
    }
    /// Create a new execution plan for reading from a reader
    pub fn try_new_from_reader(
        reader: impl Read + Seek + Send + Sync + 'static,
        options: AvroReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s,
            None => {
                return Err(DataFusionError::Execution(
                    "The schema must be provided in options when reading from a reader"
                        .to_string(),
                ));
            }
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            file_extension: String::new(),
            projection,
            projected_schema,
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

    /// File extension
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Get the schema of the avro file
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

    /// Read schema for given Avro dataset
    pub fn try_read_schema(filenames: &[String]) -> Result<Schema> {
        let mut schemas = Vec::new();
        for filename in filenames {
            let mut file = File::open(filename)?;
            let schema = read_avro_schema_from_reader(&mut file)?;
            schemas.push(schema);
        }

        Ok(Schema::try_merge(schemas)?)
    }
}

#[async_trait]
impl ExecutionPlan for AvroExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(match &self.source {
            Source::PartitionedFiles { filenames, .. } => filenames.len(),
            Source::Reader(_) => 1,
        })
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        } else if let Source::PartitionedFiles { filenames, path } = &self.source {
            Ok(Arc::new(Self {
                source: Source::PartitionedFiles {
                    filenames: filenames.clone(),
                    path: path.clone(),
                },
                schema: self.schema.clone(),
                projection: self.projection.clone(),
                projected_schema: self.projected_schema.clone(),
                batch_size: self.batch_size,
                limit: self.limit,
                file_extension: self.file_extension.clone(),
            }))
        } else {
            Err(DataFusionError::Internal(
                "AvroExec with reader source cannot be used with `with_new_children`"
                    .to_string(),
            ))
        }
    }

    #[cfg(not(feature = "avro"))]
    async fn execute(
        &self,
        _partition: usize,
    ) -> Result<super::SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }

    #[cfg(feature = "avro")]
    async fn execute(
        &self,
        partition: usize,
    ) -> Result<super::SendableRecordBatchStream> {
        let mut builder = crate::avro_to_arrow::ReaderBuilder::new()
            .with_schema(self.schema.clone())
            .with_batch_size(self.batch_size);
        if let Some(proj) = &self.projection {
            builder = builder.with_projection(
                proj.iter()
                    .map(|col_idx| self.schema.field(*col_idx).name())
                    .cloned()
                    .collect(),
            );
        }
        match &self.source {
            Source::PartitionedFiles { filenames, .. } => {
                let file = File::open(&filenames[partition])?;

                Ok(Box::pin(AvroStream::new(builder.build(file)?, self.limit)))
            }
            Source::Reader(rdr) => {
                if partition != 0 {
                    Err(DataFusionError::Internal(
                        "Only partition 0 is valid when Avro comes from a reader"
                            .to_string(),
                    ))
                } else if let Some(rdr) = rdr.lock().unwrap().take() {
                    Ok(Box::pin(AvroStream::new(builder.build(rdr)?, self.limit)))
                } else {
                    Err(DataFusionError::Execution(
                        "Error reading Avro: Data can only be read a single time when the source is a reader"
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
                    "AvroExec: source={}, batch_size={}, limit={:?}",
                    self.source, self.batch_size, self.limit
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(feature = "avro")]
struct AvroStream<'a, R: Read> {
    reader: crate::avro_to_arrow::Reader<'a, R>,
    remain: Option<usize>,
}

#[cfg(feature = "avro")]
impl<'a, R: Read> AvroStream<'a, R> {
    fn new(reader: crate::avro_to_arrow::Reader<'a, R>, limit: Option<usize>) -> Self {
        Self {
            reader,
            remain: limit,
        }
    }
}

#[cfg(feature = "avro")]
impl<R: Read + Unpin> Stream for AvroStream<'_, R> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(remain) = self.remain.as_mut() {
            if *remain < 1 {
                return Poll::Ready(None);
            }
        }

        Poll::Ready(match self.reader.next() {
            Ok(Some(item)) => {
                if let Some(remain) = self.remain.as_mut() {
                    if *remain >= item.num_rows() {
                        *remain -= item.num_rows();
                        Some(Ok(item))
                    } else {
                        let len = *remain;
                        *remain = 0;
                        Some(Ok(RecordBatch::try_new(
                            item.schema(),
                            item.columns()
                                .iter()
                                .map(|column| column.slice(0, len))
                                .collect(),
                        )?))
                    }
                } else {
                    Some(Ok(item))
                }
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
    }
}

#[cfg(feature = "avro")]
impl<R: Read + Unpin> RecordBatchStream for AvroStream<'_, R> {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[cfg(feature = "avro")]
    async fn test() -> Result<()> {
        use futures::StreamExt;

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let avro_exec = AvroExec::try_from_path(
            &filename,
            AvroReadOptions::default(),
            Some(vec![0, 1, 2]),
            1024,
            None,
        )?;
        assert_eq!(avro_exec.output_partitioning().partition_count(), 1);

        let mut results = avro_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[tokio::test]
    #[cfg(not(feature = "avro"))]
    async fn test() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let avro_exec = AvroExec::try_from_path(
            &filename,
            AvroReadOptions::default(),
            Some(vec![0, 1, 2]),
            1024,
            None,
        );
        assert!(matches!(
            avro_exec,
            Err(DataFusionError::NotImplemented(msg))
            if msg == *"cannot read avro schema without the 'avro' feature enabled"
        ));

        Ok(())
    }
}
