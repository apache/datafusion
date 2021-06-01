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

//! Execution plan for reading line-delimited JSON files
use async_trait::async_trait;
use futures::Stream;

use super::{common, source::Source, ExecutionPlan, Partitioning, RecordBatchStream};
use crate::error::{DataFusionError, Result};
use arrow::json::reader::{infer_json_schema_from_iterator, ValueIter};
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::Result as ArrowResult,
    json,
    record_batch::RecordBatch,
};
use std::fs::File;
use std::{any::Any, io::Seek};
use std::{
    io::{BufReader, Read},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// Line-delimited JSON read options
#[derive(Clone)]
pub struct NdJsonReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".json".
    pub file_extension: &'a str,
}

impl<'a> Default for NdJsonReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: 1000,
            file_extension: ".json",
        }
    }
}

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}
/// Execution plan for scanning NdJson data source
#[derive(Debug)]
pub struct NdJsonExec {
    source: Source<Box<dyn SeekRead + Send + Sync>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    file_extension: String,
    batch_size: usize,
    limit: Option<usize>,
}

impl NdJsonExec {
    /// Create a new execution plan for reading from a path
    pub fn try_new(
        path: &str,
        options: NdJsonReadOptions,
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
            None => Arc::new(NdJsonExec::try_infer_schema(
                filenames.clone(),
                Some(options.schema_infer_max_records),
            )?),
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
            file_extension,
            projection,
            projected_schema,
            batch_size,
            limit,
        })
    }
    /// Create a new execution plan for reading from a reader
    pub fn try_new_from_reader(
        reader: impl Read + Seek + Send + Sync + 'static,
        options: NdJsonReadOptions,
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
        mut filenames: Vec<String>,
        max_records: Option<usize>,
    ) -> Result<Schema> {
        let mut schemas = Vec::new();
        let mut records_to_read = max_records.unwrap_or(usize::MAX);
        while records_to_read > 0 && !filenames.is_empty() {
            let file = File::open(filenames.pop().unwrap())?;
            let mut reader = BufReader::new(file);
            let iter = ValueIter::new(&mut reader, None);
            let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
                let should_take = records_to_read > 0;
                records_to_read -= 1;
                should_take
            }))?;
            schemas.push(schema);
        }

        Ok(Schema::try_merge(schemas)?)
    }
}

#[async_trait]
impl ExecutionPlan for NdJsonExec {
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
                "NdJsonExec with reader source cannot be used with `with_new_children`"
                    .to_string(),
            ))
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<super::SendableRecordBatchStream> {
        let mut builder = json::ReaderBuilder::new()
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

                Ok(Box::pin(NdJsonStream::new(
                    builder.build(file)?,
                    self.limit,
                )))
            }
            Source::Reader(rdr) => {
                if partition != 0 {
                    Err(DataFusionError::Internal(
                        "Only partition 0 is valid when CSV comes from a reader"
                            .to_string(),
                    ))
                } else if let Some(rdr) = rdr.lock().unwrap().take() {
                    Ok(Box::pin(NdJsonStream::new(builder.build(rdr)?, self.limit)))
                } else {
                    Err(DataFusionError::Execution(
                        "Error reading CSV: Data can only be read a single time when the source is a reader"
                            .to_string(),
                    ))
                }
            }
        }
    }
}

struct NdJsonStream<R: Read> {
    reader: json::Reader<R>,
    remain: Option<usize>,
}

impl<R: Read> NdJsonStream<R> {
    fn new(reader: json::Reader<R>, limit: Option<usize>) -> Self {
        Self {
            reader,
            remain: limit,
        }
    }
}

impl<R: Read + Unpin> Stream for NdJsonStream<R> {
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

impl<R: Read + Unpin> RecordBatchStream for NdJsonStream<R> {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    const TEST_DATA_BASE: &str = "tests/jsons";

    #[tokio::test]
    async fn nd_json_exec_file_without_projection() -> Result<()> {
        use arrow::datatypes::DataType;
        let path = format!("{}/1.json", TEST_DATA_BASE);
        let exec = NdJsonExec::try_new(&path, Default::default(), None, 1024, Some(3))?;
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 4);

        // a,b,c,d should be inferred
        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap();

        assert_eq!(
            inferred_schema.field_with_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert!(matches!(
            inferred_schema.field_with_name("b").unwrap().data_type(),
            DataType::List(_)
        ));
        assert_eq!(
            inferred_schema.field_with_name("d").unwrap().data_type(),
            &DataType::Utf8
        );

        let mut it = exec.execute(0).await?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 3);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);

        Ok(())
    }

    #[tokio::test]
    async fn nd_json_exec_file_projection() -> Result<()> {
        let path = format!("{}/1.json", TEST_DATA_BASE);
        let exec =
            NdJsonExec::try_new(&path, Default::default(), Some(vec![0, 2]), 1024, None)?;
        let inferred_schema = exec.schema();
        assert_eq!(inferred_schema.fields().len(), 2);

        inferred_schema.field_with_name("a").unwrap();
        inferred_schema.field_with_name("b").unwrap_err();
        inferred_schema.field_with_name("c").unwrap();
        inferred_schema.field_with_name("d").unwrap_err();

        let mut it = exec.execute(0).await?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 4);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), -10);
        assert_eq!(values.value(2), 2);
        Ok(())
    }

    #[tokio::test]
    async fn nd_json_exec_from_reader() -> Result<()> {
        let content = r#"{"a":"aaa", "b":[2.0, 1.3, -6.1], "c":[false, true], "d":"4"}
{"a":"bbb", "b":[2.0, 1.3, -6.1], "c":[true, true], "d":"4"}"#;
        let cur = std::io::Cursor::new(content);
        let mut bufrdr = std::io::BufReader::new(cur);
        let schema =
            arrow::json::reader::infer_json_schema_from_seekable(&mut bufrdr, None)?;
        let exec = NdJsonExec::try_new_from_reader(
            bufrdr,
            NdJsonReadOptions {
                schema: Some(Arc::new(schema)),
                ..Default::default()
            },
            None,
            1024,
            Some(1),
        )?;

        let mut it = exec.execute(0).await?;
        let batch = it.next().await.unwrap()?;

        assert_eq!(batch.num_rows(), 1);

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "aaa");

        Ok(())
    }
}
