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

//! A generic stream over file format readers that can be used by
//! any file format that read its files from start to end.
//!
//! Note: Most traits here need to be marked `Sync + Send` to be
//! compliant with the `SendableRecordBatchStream` trait.

use crate::{
    datasource::{object_store::ObjectStore, PartitionedFile},
    error::Result as DataFusionResult,
    physical_plan::RecordBatchStream,
};
use arrow::{
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::Stream;
use std::{
    io::Read,
    iter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub type FileIter =
    Box<dyn Iterator<Item = DataFusionResult<Box<dyn Read + Send + Sync>>> + Send + Sync>;
pub type BatchIter = Box<dyn Iterator<Item = ArrowResult<RecordBatch>> + Send + Sync>;

/// A closure that creates a file format reader (iterator over `RecordBatch`) from a `Read` object
/// and an optional number of required records.
pub trait FormatReaderOpener:
    FnMut(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter + Send + Unpin + 'static
{
}

impl<T> FormatReaderOpener for T where
    T: FnMut(Box<dyn Read + Send + Sync>, &Option<usize>) -> BatchIter
        + Send
        + Unpin
        + 'static
{
}

/// A stream that iterates record batch by record batch, file over file.
pub struct FileStream<F: FormatReaderOpener> {
    /// An iterator over record batches of the last file returned by file_iter
    batch_iter: BatchIter,
    /// An iterator over input files
    file_iter: FileIter,
    /// The stream schema (file schema after projection)
    schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_reader: F,
}

impl<F: FormatReaderOpener> FileStream<F> {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        file_reader: F,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Self {
        let read_iter = files.into_iter().map(move |f| -> DataFusionResult<_> {
            object_store
                .file_reader(f.file_meta.sized_file)?
                .sync_reader()
        });

        Self {
            file_iter: Box::new(read_iter),
            batch_iter: Box::new(iter::empty()),
            remain: limit,
            schema,
            file_reader,
        }
    }

    /// Acts as a flat_map of record batches over files.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        match self.batch_iter.next() {
            Some(batch) => Some(batch),
            None => match self.file_iter.next() {
                Some(Ok(f)) => {
                    self.batch_iter = (self.file_reader)(f, &self.remain);
                    self.next_batch()
                }
                Some(Err(e)) => Some(Err(ArrowError::ExternalError(Box::new(e)))),
                None => None,
            },
        }
    }
}

impl<F: FormatReaderOpener> Stream for FileStream<F> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // check if finished or no limit
        match self.remain {
            Some(r) if r == 0 => return Poll::Ready(None),
            None => return Poll::Ready(self.get_mut().next_batch()),
            Some(r) => r,
        };

        Poll::Ready(match self.as_mut().next_batch() {
            Some(Ok(item)) => {
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
            other => other,
        })
    }
}

impl<F: FormatReaderOpener> RecordBatchStream for FileStream<F> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{
        error::Result,
        test::{make_partition, object_store::TestObjectStore},
    };

    /// helper that creates a stream of 2 files with the same pair of batches in each ([0,1,2] and [0,1])
    async fn create_and_collect(limit: Option<usize>) -> Vec<RecordBatch> {
        let records = vec![make_partition(3), make_partition(2)];

        let source_schema = records[0].schema();

        let reader = move |_file, _remain: &Option<usize>| {
            // this reader returns the same batch regardless of the file
            Box::new(records.clone().into_iter().map(Ok)) as BatchIter
        };

        let file_stream = FileStream::new(
            TestObjectStore::new_arc(&[("mock_file1", 10), ("mock_file2", 20)]),
            vec![
                PartitionedFile::new("mock_file1".to_owned(), 10),
                PartitionedFile::new("mock_file2".to_owned(), 20),
            ],
            reader,
            source_schema,
            limit,
        );

        file_stream
            .map(|b| b.expect("No error expected in stream"))
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn without_limit() -> Result<()> {
        let batches = create_and_collect(None).await;

        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_between_files() -> Result<()> {
        let batches = create_and_collect(Some(5)).await;
        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "+---+",
        ], &batches);

        Ok(())
    }

    #[tokio::test]
    async fn with_limit_at_middle_of_batch() -> Result<()> {
        let batches = create_and_collect(Some(6)).await;
        #[rustfmt::skip]
        crate::assert_batches_eq!(&[
            "+---+",
            "| i |",
            "+---+",
            "| 0 |",
            "| 1 |",
            "| 2 |",
            "| 0 |",
            "| 1 |",
            "| 0 |",
            "+---+",
        ], &batches);

        Ok(())
    }
}
