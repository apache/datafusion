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

//! Merge that deals with an arbitrary size of spilled files.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::{EmptyRecordBatchStream, SpillManager};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::{internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::stream::RecordBatchStreamAdapter;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::{Stream, StreamExt};

enum State {
    /// Had an error
    Aborted,

    /// Stream did not start yet or between passes
    Uninitialized,

    /// In progress of merging multiple sorted streams
    MultiLevel {
        stream: SendableRecordBatchStream,
        in_progress_file: InProgressSpillFile,
    },

    /// This is the last level of the merge, just pass through the stream
    Passthrough(SendableRecordBatchStream),
}

pub struct MultiLevelSortPreservingMergeStream {
    schema: SchemaRef,
    spill_manager: SpillManager,
    sorted_spill_files: Vec<RefCountedTempFile>,
    sorted_streams: Vec<SendableRecordBatchStream>,
    expr: Arc<LexOrdering>,
    metrics: BaselineMetrics,
    batch_size: usize,
    reservation: MemoryReservation,
    fetch: Option<usize>,
    enable_round_robin_tie_breaker: bool,

    /// The number of blocking threads to use for merging sorted streams
    max_blocking_threads: usize,

    /// The current state of the stream
    state: State,
}

impl MultiLevelSortPreservingMergeStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spill_manager: SpillManager,
        schema: SchemaRef,
        sorted_spill_files: Vec<RefCountedTempFile>,
        sorted_streams: Vec<SendableRecordBatchStream>,
        expr: LexOrdering,
        metrics: BaselineMetrics,
        batch_size: usize,
        reservation: MemoryReservation,

        max_blocking_threads: Option<usize>,

        fetch: Option<usize>,
        enable_round_robin_tie_breaker: bool,
    ) -> Result<Self> {
        // TODO - add a check to see the actual number of available blocking threads
        let max_blocking_threads = max_blocking_threads.unwrap_or(128);

        if max_blocking_threads <= 1 {
            return internal_err!("max_blocking_threads must be greater than 1");
        }

        Ok(Self {
            spill_manager,
            schema,
            sorted_spill_files,
            sorted_streams,
            expr: Arc::new(expr),
            metrics,
            batch_size,
            reservation,
            fetch,
            state: State::Uninitialized,
            enable_round_robin_tie_breaker,
            max_blocking_threads,
        })
    }

    fn created_sorted_stream(&mut self) -> Result<SendableRecordBatchStream> {
        let mut sorted_streams = mem::take(&mut self.sorted_streams);

        match (self.sorted_spill_files.len(), sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            )))),

            // Only in-memory stream
            (0, 1) => Ok(sorted_streams.into_iter().next().unwrap()),

            // Only single sorted spill file so stream it
            (1, 0) => self
                .spill_manager
                .read_spill_as_stream(self.sorted_spill_files.drain(..).next().unwrap()),

            // Need to merge multiple streams
            (_, _) => {
                let sorted_spill_files_to_read =
                    self.sorted_spill_files.len().min(self.max_blocking_threads);

                for spill in self.sorted_spill_files.drain(..sorted_spill_files_to_read) {
                    let stream = self.spill_manager.read_spill_as_stream(spill)?;
                    sorted_streams.push(Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&self.schema),
                        stream,
                    )));
                }

                let mut builder = StreamingMergeBuilder::new()
                    .with_schema(Arc::clone(&self.schema))
                    .with_expressions(self.expr.deref())
                    .with_batch_size(self.batch_size)
                    .with_fetch(self.fetch)
                    .with_round_robin_tie_breaker(self.enable_round_robin_tie_breaker)
                    .with_streams(sorted_streams)
                    .with_reservation(self.reservation.new_empty());

                // Only add the metrics to the last run
                if self.sorted_spill_files.is_empty() {
                    builder = builder.with_metrics(self.metrics.clone())
                }

                builder.build()
            }
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                State::Aborted => return Poll::Ready(None),
                State::Uninitialized => {
                    let stream = self.created_sorted_stream()?;

                    if self.sorted_spill_files.is_empty() {
                        self.state = State::Passthrough(stream);
                    } else {
                        let in_progress_file =
                            self.spill_manager.create_in_progress_file("spill")?;

                        self.state = State::MultiLevel {
                            stream,
                            in_progress_file,
                        };
                    }
                }
                State::MultiLevel {
                    stream,
                    in_progress_file,
                } => {
                    'write_sorted_run: loop {
                        match futures::ready!(stream.poll_next_unpin(cx)) {
                            // This stream is finished.
                            None => {
                                // finish the file and add it to the sorted spill files
                                if let Some(sorted_spill_file) =
                                    in_progress_file.finish()?
                                {
                                    self.sorted_spill_files.push(sorted_spill_file);
                                }

                                // Reset the state to create a stream from the current sorted spill files
                                self.state = State::Uninitialized;

                                break 'write_sorted_run;
                            }
                            Some(Err(e)) => {
                                self.state = State::Aborted;

                                // Abort
                                return Poll::Ready(Some(Err(e)));
                            }
                            Some(Ok(batch)) => {
                                // Got a batch, write it to file
                                in_progress_file.append_batch(&batch)?;
                            }
                        }
                    }
                }

                // Last
                State::Passthrough(s) => return s.poll_next_unpin(cx),
            }
        }
    }
}

impl Stream for MultiLevelSortPreservingMergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}

impl RecordBatchStream for MultiLevelSortPreservingMergeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
