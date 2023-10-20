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

use crate::metrics::BaselineMetrics;
use crate::sorts::cursor::CursorValues;
use crate::RecordBatchStream;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{memory_pool::MemoryReservation, SendableRecordBatchStream};
use futures::Stream;
use std::marker::Send;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use super::merge::SortPreservingMergeStream;
use super::stream::CursorStream;

pub(crate) struct SortPreservingCascadeStream {
    /// If the stream has encountered an error, or fetch is reached
    aborted: bool,

    /// The sorted input streams to merge together
    /// TODO: this will become the root of the cascade tree
    cascade: SendableRecordBatchStream,

    /// used to record execution metrics
    metrics: BaselineMetrics,

    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,
}

impl SortPreservingCascadeStream {
    pub(crate) fn new<C: CursorValues + Send + Unpin + 'static>(
        streams: CursorStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            aborted: false,
            metrics: metrics.clone(),
            schema: schema.clone(),
            cascade: Box::pin(SortPreservingMergeStream::new(
                streams,
                schema,
                metrics,
                batch_size,
                fetch,
                reservation,
            )),
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        match ready!(self.cascade.as_mut().poll_next(cx)) {
            None => Poll::Ready(None),
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            Some(Ok(res)) => Poll::Ready(Some(Ok(res))),
        }
    }
}

impl Stream for SortPreservingCascadeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.record_poll(poll)
    }
}

impl RecordBatchStream for SortPreservingCascadeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
