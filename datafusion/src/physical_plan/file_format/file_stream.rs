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
    physical_plan::RecordBatchStream,
    scalar::ScalarValue,
};
use arrow::{
    array::{ArrayData, ArrayRef, DictionaryArray, UInt8BufferBuilder},
    buffer::Buffer,
    datatypes::{DataType, SchemaRef, UInt8Type},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use futures::Stream;
use std::{
    collections::HashMap,
    io::Read,
    iter,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

pub type FileIter = Box<dyn Iterator<Item = PartitionedFile> + Send + Sync>;
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
    /// Partitioning column values for the current batch_iter
    partition_values: Vec<ScalarValue>,
    /// An iterator over input files.
    file_iter: FileIter,
    /// The stream schema (file schema including partition columns and after
    /// projection).
    schema: SchemaRef,
    /// The remaining number of records to parse, None if no limit
    remain: Option<usize>,
    /// A closure that takes a reader and an optional remaining number of lines
    /// (before reaching the limit) and returns a batch iterator. If the file reader
    /// is not capable of limiting the number of records in the last batch, the file
    /// stream will take care of truncating it.
    file_reader: F,
    /// A buffer initialized to zeros that represents the key array of all partition
    /// columns (partition columns are materialized by dictionary arrays with only one
    /// value in the dictionary, thus all the keys are equal to zero).
    key_buffer_cache: Option<Buffer>,
    /// mapping between the indexes in the list of partition columns and the target
    /// schema.
    projected_partition_indexes: HashMap<usize, usize>,
    /// the store from which to source the files.
    object_store: Arc<dyn ObjectStore>,
}

impl<F: FormatReaderOpener> FileStream<F> {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        file_reader: F,
        schema: SchemaRef,
        limit: Option<usize>,
        table_partition_cols: Vec<String>,
    ) -> Self {
        let mut projected_partition_indexes = HashMap::new();
        for (partition_idx, partition_name) in table_partition_cols.iter().enumerate() {
            if let Ok(schema_idx) = schema.index_of(partition_name) {
                projected_partition_indexes.insert(partition_idx, schema_idx);
            }
        }

        Self {
            file_iter: Box::new(files.into_iter()),
            batch_iter: Box::new(iter::empty()),
            partition_values: vec![],
            remain: limit,
            schema,
            file_reader,
            key_buffer_cache: None,
            projected_partition_indexes,
            object_store,
        }
    }

    /// Acts as a flat_map of record batches over files. Adds the partitioning
    /// Columns to the returned record batches.
    fn next_batch(&mut self) -> Option<ArrowResult<RecordBatch>> {
        let expected_cols =
            self.schema.fields().len() - self.projected_partition_indexes.len();
        match self.batch_iter.next() {
            Some(Ok(batch)) if batch.columns().len() == expected_cols => {
                let mut cols = batch.columns().to_vec();
                for (&pidx, &sidx) in &self.projected_partition_indexes {
                    cols.insert(
                        sidx,
                        create_dict_array(
                            &mut self.key_buffer_cache,
                            &self.partition_values[pidx],
                            batch.num_rows(),
                        ),
                    )
                }
                Some(RecordBatch::try_new(self.schema(), cols))
            }
            Some(Ok(batch)) => Some(Err(ArrowError::SchemaError(format!(
                "Unexpected batch schema from file, expected {} cols but got {}",
                expected_cols,
                batch.columns().len()
            )))),
            Some(Err(e)) => Some(Err(e)),
            None => match self.file_iter.next() {
                Some(f) => {
                    self.partition_values = f.partition_values;
                    self.object_store
                        .file_reader(f.file_meta.sized_file)
                        .and_then(|r| r.sync_reader())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                        .and_then(|f| {
                            self.batch_iter = (self.file_reader)(f, &self.remain);
                            self.next_batch().transpose()
                        })
                        .transpose()
                }
                None => None,
            },
        }
    }
}

fn create_dict_array(
    key_buffer_cache: &mut Option<Buffer>,
    val: &ScalarValue,
    len: usize,
) -> ArrayRef {
    // build value dictionary
    let dict_vals = val.to_array();

    // build keys array
    let sliced_key_buffer = match key_buffer_cache {
        Some(buf) if buf.len() >= len => buf.slice(buf.len() - len),
        _ => {
            let mut key_buffer_builder = UInt8BufferBuilder::new(len);
            key_buffer_builder.advance(len); // keys are all 0
            key_buffer_cache.insert(key_buffer_builder.finish()).clone()
        }
    };

    // create data type
    let data_type =
        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(val.get_datatype()));

    // assemble pieces together
    let mut builder = ArrayData::builder(data_type)
        .len(len)
        .add_buffer(sliced_key_buffer);
    builder = builder.add_child_data(dict_vals.data().clone());
    Arc::new(DictionaryArray::<UInt8Type>::from(builder.build().unwrap()))
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
            vec![],
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
