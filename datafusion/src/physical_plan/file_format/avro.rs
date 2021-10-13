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
use crate::datasource::object_store::ObjectStore;
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};
#[cfg(feature = "avro")]
use crate::physical_plan::RecordBatchStream;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::{Schema, SchemaRef};
#[cfg(feature = "avro")]
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_trait::async_trait;
#[cfg(feature = "avro")]
use futures::Stream;
use std::any::Any;
use std::sync::Arc;
#[cfg(feature = "avro")]
use std::{
    io::Read,
    pin::Pin,
    task::{Context, Poll},
};

/// Execution plan for scanning Avro data source
#[derive(Debug, Clone)]
pub struct AvroExec {
    object_store: Arc<dyn ObjectStore>,
    files: Vec<PartitionedFile>,
    statistics: Statistics,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    batch_size: usize,
    limit: Option<usize>,
}

impl AvroExec {
    /// Create a new JSON reader execution plan provided file list and schema
    /// TODO: support partitiond file list (Vec<Vec<PartitionedFile>>)
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        statistics: Statistics,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = match &projection {
            None => Arc::clone(&schema),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Self {
            object_store,
            files,
            statistics,
            schema,
            projection,
            projected_schema,
            batch_size,
            limit,
        }
    }
    /// List of data files
    pub fn files(&self) -> &[PartitionedFile] {
        &self.files
    }
    /// The schema before projection
    pub fn file_schema(&self) -> &SchemaRef {
        &self.schema
    }
    /// Optional projection for which columns to load
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }
    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
    /// Limit in nr. of rows
    pub fn limit(&self) -> Option<usize> {
        self.limit
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
        Partitioning::UnknownPartitioning(self.files.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
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

    #[cfg(not(feature = "avro"))]
    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "Cannot execute avro plan without avro feature enabled".to_string(),
        ))
    }

    #[cfg(feature = "avro")]
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let file = self
            .object_store
            .file_reader(self.files[partition].file_meta.sized_file.clone())?
            .sync_reader()?;

        let proj = self.projection.as_ref().map(|p| {
            p.iter()
                .map(|col_idx| self.schema.field(*col_idx).name())
                .cloned()
                .collect()
        });

        let avro_reader = crate::avro_to_arrow::Reader::try_new(
            file,
            self.schema(),
            self.batch_size,
            proj,
        )?;

        Ok(Box::pin(AvroStream::new(avro_reader, self.limit)))
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
                    "AvroExec: files=[{}], batch_size={}, limit={:?}",
                    self.files
                        .iter()
                        .map(|f| f.file_meta.path())
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.batch_size,
                    self.limit,
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
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
#[cfg(feature = "avro")]
mod tests {

    use crate::datasource::object_store::local::{
        local_file_meta, local_object_reader_stream, LocalFileSystem,
    };

    use super::*;

    #[tokio::test]
    async fn test() -> Result<()> {
        use futures::StreamExt;

        use crate::datasource::file_format::{avro::AvroFormat, FileFormat};

        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let avro_exec = AvroExec::new(
            Arc::new(LocalFileSystem {}),
            vec![PartitionedFile {
                file_meta: local_file_meta(filename.clone()),
            }],
            Statistics::default(),
            AvroFormat {}
                .infer_schema(local_object_reader_stream(vec![filename]))
                .await?,
            Some(vec![0, 1, 2]),
            1024,
            None,
        );
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
}
