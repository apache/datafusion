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

//! Create a stream that do a multi level merge stream

use crate::metrics::BaselineMetrics;
use crate::{EmptyRecordBatchStream, SpillManager};
use arrow::array::RecordBatch;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};

use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::stream::RecordBatchStreamAdapter;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::TryStreamExt;
use futures::{Stream, StreamExt};

/// Merges a stream of sorted cursors and record batches into a single sorted stream
pub(crate) struct MultiLevelMergeBuilder {
    spill_manager: SpillManager,
    schema: SchemaRef,
    sorted_spill_files: Vec<SortedSpillFile>,
    sorted_streams: Vec<SendableRecordBatchStream>,
    expr: LexOrdering,
    metrics: BaselineMetrics,
    batch_size: usize,
    reservation: MemoryReservation,
    fetch: Option<usize>,
    enable_round_robin_tie_breaker: bool,

    // This is for avoiding double reservation of memory from our side and the sort preserving merge stream
    // side.
    // and doing a lot of code changes to avoid accounting for the memory used by the streams
    unbounded_memory_pool: Arc<dyn MemoryPool>,
}

impl Debug for MultiLevelMergeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiLevelMergeBuilder")
    }
}

impl MultiLevelMergeBuilder {
    pub(crate) fn new(
        spill_manager: SpillManager,
        schema: SchemaRef,
        sorted_spill_files: Vec<SortedSpillFile>,
        sorted_streams: Vec<SendableRecordBatchStream>,
        expr: LexOrdering,
        metrics: BaselineMetrics,
        batch_size: usize,
        reservation: MemoryReservation,
        fetch: Option<usize>,
        enable_round_robin_tie_breaker: bool,
    ) -> Self {
        Self {
            spill_manager,
            schema,
            sorted_spill_files,
            sorted_streams,
            expr,
            metrics,
            batch_size,
            reservation,
            enable_round_robin_tie_breaker,
            fetch,
            unbounded_memory_pool: Arc::new(UnboundedMemoryPool::default()),
        }
    }

    pub(crate) fn create_spillable_merge_stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(self.create_stream()).try_flatten(),
        ))
    }

    async fn create_stream(mut self) -> Result<SendableRecordBatchStream> {
        loop {
            // Hold this for the lifetime of the stream
            let mut current_memory_reservation = self.reservation.new_empty();
            let mut stream =
                self.create_sorted_stream(&mut current_memory_reservation)?;

            // TODO - add a threshold for number of files to disk even if empty and reading from disk so
            //        we can avoid the memory reservation

            // If no spill files are left, we can return the stream as this is the last sorted run
            // TODO - We can write to disk before reading it back to avoid having multiple streams in memory
            if self.sorted_spill_files.is_empty() {
                // Attach the memory reservation to the stream as we are done with it
                // but because we replaced the memory reservation of the merge stream, we must hold
                // this to make sure we have enough memory
                return Ok(Box::pin(StreamAttachedReservation::new(
                    stream,
                    current_memory_reservation,
                )));
            }

            // Need to sort to a spill file
            let Some((spill_file, max_record_batch_memory)) = self
                .spill_manager
                .spill_record_batch_stream_by_size(
                    &mut stream,
                    self.batch_size,
                    "MultiLevelMergeBuilder intermediate spill",
                )
                .await?
            else {
                continue;
            };

            // Add the spill file
            self.sorted_spill_files.push(SortedSpillFile {
                file: spill_file,
                max_record_batch_memory,
            });
        }
    }

    fn create_sorted_stream(
        &mut self,
        memory_reservation: &mut MemoryReservation,
    ) -> Result<SendableRecordBatchStream> {
        match (self.sorted_spill_files.len(), self.sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            )))),

            // Only in-memory stream, return that
            (0, 1) => Ok(self.sorted_streams.remove(0)),

            // Only single sorted spill file so return it
            (1, 0) => {
                let spill_file = self.sorted_spill_files.remove(0);

                self.spill_manager
                    .read_spill_as_stream(spill_file.file)
            }

            // Only in memory streams, so merge them all in a single pass
            (0, _) => {
                let sorted_stream = mem::take(&mut self.sorted_streams);
                self.create_new_merge_sort(
                    sorted_stream,
                    // If we have no sorted spill files left, this is the last run
                    true,
                )
            }

            // Need to merge multiple streams
            (_, _) => {
                // Don't account for existing streams memory
                // as we are not holding the memory for them
                let mut sorted_streams = mem::take(&mut self.sorted_streams);

                let (sorted_spill_files, buffer_size) = self
                    .get_sorted_spill_files_to_merge(
                        2,
                        // we must have at least 2 streams to merge
                        2_usize.saturating_sub(sorted_streams.len()),
                        memory_reservation,
                    )?;

                for spill in sorted_spill_files {
                    let stream = self
                        .spill_manager
                        .clone()
                        .with_batch_read_buffer_capacity(buffer_size)
                        .read_spill_as_stream(spill.file)?;
                    sorted_streams.push(stream);
                }

                self.create_new_merge_sort(
                    sorted_streams,
                    // If we have no sorted spill files left, this is the last run
                    self.sorted_spill_files.is_empty(),
                )
            }
        }
    }

    fn create_new_merge_sort(
        &mut self,
        streams: Vec<SendableRecordBatchStream>,
        is_output: bool,
    ) -> Result<SendableRecordBatchStream> {
        StreamingMergeBuilder::new()
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr)
            .with_batch_size(self.batch_size)
            .with_fetch(self.fetch)
            .with_metrics(if is_output {
                // Only add the metrics to the last run
                self.metrics.clone()
            } else {
                self.metrics.intermediate()
            })
            .with_round_robin_tie_breaker(self.enable_round_robin_tie_breaker)
            .with_streams(streams)
            // Don't track memory used by this stream as we reserve that memory by worst case sceneries
            // (reserving memory for the biggest batch in each stream)
            // This is a hack
            .with_reservation(
                MemoryConsumer::new("merge stream mock memory")
                    .register(&self.unbounded_memory_pool),
            )
            .build()
    }

    /// Return the sorted spill files to use for the next phase, and the buffer size
    /// This will try to get as many spill files as possible to merge, and if we don't have enough streams
    /// it will try to reduce the buffer size until we have enough streams to merge
    /// otherwise it will return an error
    fn get_sorted_spill_files_to_merge(
        &mut self,
        buffer_size: usize,
        minimum_number_of_required_streams: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<(Vec<SortedSpillFile>, usize)> {
        assert_ne!(buffer_size, 0, "Buffer size must be greater than 0");
        let mut number_of_spills_to_read_for_current_phase = 0;

        for spill in &self.sorted_spill_files {
            // For memory pools that are not shared this is good, for other this is not
            // and there should be some upper limit to memory reservation so we won't starve the system
            match reservation.try_grow(spill.max_record_batch_memory * buffer_size) {
                Ok(_) => {
                    number_of_spills_to_read_for_current_phase += 1;
                }
                // If we can't grow the reservation, we need to stop
                Err(err) => {
                    // We must have at least 2 streams to merge, so if we don't have enough memory
                    // fail
                    if minimum_number_of_required_streams
                        > number_of_spills_to_read_for_current_phase
                    {
                        // Free the memory we reserved for this merge as we either try again or fail
                        reservation.free();
                        if buffer_size > 1 {
                            // Try again with smaller buffer size, it will be slower but at least we can merge
                            return self.get_sorted_spill_files_to_merge(
                                buffer_size - 1,
                                minimum_number_of_required_streams,
                                reservation,
                            );
                        }

                        return Err(err);
                    }

                    // We reached the maximum amount of memory we can use
                    // for this merge
                    break;
                }
            }
        }

        let spills = self
            .sorted_spill_files
            .drain(..number_of_spills_to_read_for_current_phase)
            .collect::<Vec<_>>();

        Ok((spills, buffer_size))
    }
}

struct StreamAttachedReservation {
    stream: SendableRecordBatchStream,
    reservation: MemoryReservation,
}

impl StreamAttachedReservation {
    fn new(stream: SendableRecordBatchStream, reservation: MemoryReservation) -> Self {
        Self {
            stream,
            reservation,
        }
    }
}

impl Stream for StreamAttachedReservation {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let res = self.stream.poll_next_unpin(cx);

        match res {
            Poll::Ready(res) => {
                match res {
                    Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
                    Some(Err(err)) => {
                        // Had an error so drop the data
                        self.reservation.free();
                        Poll::Ready(Some(Err(err)))
                    }
                    None => {
                        // Stream is done so free the memory
                        self.reservation.free();

                        Poll::Ready(None)
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for StreamAttachedReservation {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}
