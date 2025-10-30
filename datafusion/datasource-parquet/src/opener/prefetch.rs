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

//! [`EagerRowGroupPrefetchStream`] prefetches Parquet RowGroups in the background.

use arrow::array::RecordBatch;
use datafusion_common::{exec_err, DataFusionError};
use datafusion_common_runtime::SpawnedTask;
use futures::{ready, Future, Stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::errors::ParquetError;
use std::pin::pin;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{channel, Receiver};

/// Eagerly prefetches RowGroups from the underlying stream
pub(crate) struct EagerRowGroupPrefetchStream {
    /// Channel receiver for prefetched row groups
    receiver: Receiver<
        datafusion_common::Result<Option<ParquetRecordBatchReader>, ParquetError>,
    >,
    /// Background task that drives prefetching
    prefetch_task: Option<SpawnedTask<parquet::errors::Result<()>>>,
    /// Active reader, if any
    parquet_record_batch_reader: Option<ParquetRecordBatchReader>,
}

impl EagerRowGroupPrefetchStream {
    /// Create a new prefetching stream, that prefetches up to `prefetch_row_groups` at once
    pub fn new<T>(stream: ParquetRecordBatchStream<T>, prefetch_row_groups: usize) -> Self
    where
        T: AsyncFileReader + Unpin + Send + 'static,
    {
        let (sender, receiver) = channel(prefetch_row_groups);

        let prefetch_task = SpawnedTask::spawn(async move {
            let mut stream = stream;
            loop {
                match stream.next_row_group().await {
                    Ok(Some(reader)) => {
                        // Stop prefetching if the receiver is dropped
                        if sender.send(Ok(Some(reader))).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        // Send the error, ignore errors if the receiver is dropped as
                        // there is nowhere to send the errors to
                        let _ = sender.send(Ok(None)).await;
                        break;
                    }
                    Err(err) => {
                        // Send the error, ignore errors if the receiver is dropped as
                        // there is nowhere to send the errors to
                        let _ = sender.send(Err(err)).await;
                        break;
                    }
                }
            }
            Ok(())
        });

        Self {
            receiver,
            prefetch_task: Some(prefetch_task),
            parquet_record_batch_reader: None,
        }
    }
}

impl Stream for EagerRowGroupPrefetchStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            // If we have an active reader, try to read from it first
            if let Some(mut reader) = self.parquet_record_batch_reader.take() {
                match reader.next() {
                    Some(result) => {
                        // Return the reader to self for the next poll
                        self.parquet_record_batch_reader = Some(reader);
                        let result = result.map_err(DataFusionError::from);
                        return Poll::Ready(Some(result));
                    }
                    None => {
                        // Reader is exhausted, continue to prefetching the next row group
                    }
                }
            }

            match ready!(Pin::new(&mut self.receiver).poll_recv(cx)) {
                Some(Ok(Some(reader))) => {
                    self.parquet_record_batch_reader = Some(reader);
                }
                Some(Ok(None)) => {
                    return Poll::Ready(None);
                }
                Some(Err(err)) => {
                    return Poll::Ready(Some(Err(DataFusionError::from(err))));
                }
                // end of stream from producer task
                None => {
                    if let Some(handle) = self.prefetch_task.take() {
                        match ready!(pin!(handle).poll(cx)) {
                            Ok(Ok(())) => return Poll::Ready(None),
                            Ok(Err(err)) => {
                                return Poll::Ready(Some(Err(DataFusionError::from(err))))
                            }
                            Err(err) => {
                                return Poll::Ready(Some(exec_err!(
                                    "Eager prefetch task panicked: {err}"
                                )));
                            }
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}
