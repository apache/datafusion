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
use datafusion_common::{Result, internal_err, resources_err};
use datafusion_execution::memory_pool::MemoryReservation;

use crate::sorts::builder::try_grow_reservation_to_at_least;
use crate::sorts::sort::get_reserved_bytes_for_record_batch_size;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::stream::RecordBatchStreamAdapter;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::TryStreamExt;
use futures::{Stream, StreamExt};

/// Merges a stream of sorted cursors and record batches into a single sorted stream
///
/// This is a wrapper around [`SortPreservingMergeStream`](crate::sorts::merge::SortPreservingMergeStream)
/// that provide it the sorted streams/files to merge while making sure we can merge them in memory.
/// In case we can't merge all of them in a single pass we will spill the intermediate results to disk
/// and repeat the process.
///
/// ## High level Algorithm
/// 1. Get the maximum amount of sorted in-memory streams and spill files we can merge with the available memory
/// 2. Sort them to a sorted stream
/// 3. Do we have more spill files to merge?
///  - Yes: write that sorted stream to a spill file,
///    add that spill file back to the spill files to merge and
///    repeat the process
///
///  - No: return that sorted stream as the final output stream
///
/// ```text
/// Initial State: Multiple sorted streams + spill files
///      ┌───────────┐
///      │  Phase 1  │
///      └───────────┘
/// ┌──Can hold in memory─┐
/// │   ┌──────────────┐  │
/// │   │  In-memory   │
/// │   │sorted stream │──┼────────┐
/// │   │      1       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │
/// │   │  In-memory   │           │
/// │   │sorted stream │──┼────────┤
/// │   │      2       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │
/// │   │  In-memory   │           │
/// │   │sorted stream │──┼────────┤
/// │   │      3       │  │        │
///     └──────────────┘  │        │
/// │   ┌──────────────┐  │        │            ┌───────────┐
/// │   │ Sorted Spill │           │            │  Phase 2  │
/// │   │    file 1    │──┼────────┤            └───────────┘
/// │   └──────────────┘  │        │
///  ──── ──── ──── ──── ─┘        │       ┌──Can hold in memory─┐
///                                │       │                     │
///     ┌──────────────┐           │       │   ┌──────────────┐
///     │ Sorted Spill │           │       │   │ Sorted Spill │  │
///     │    file 2    │──────────────────────▶│    file 2    │──┼─────┐
///     └──────────────┘           │           └──────────────┘  │     │
///     ┌──────────────┐           │       │   ┌──────────────┐  │     │
///     │ Sorted Spill │           │       │   │ Sorted Spill │        │
///     │    file 3    │──────────────────────▶│    file 3    │──┼─────┤
///     └──────────────┘           │       │   └──────────────┘  │     │
///     ┌──────────────┐           │           ┌──────────────┐  │     │
///     │ Sorted Spill │           │       │   │ Sorted Spill │  │     │
///     │    file 4    │──────────────────────▶│    file 4    │────────┤          ┌───────────┐
///     └──────────────┘           │       │   └──────────────┘  │     │          │  Phase 3  │
///                                │       │                     │     │          └───────────┘
///                                │        ──── ──── ──── ──── ─┘     │     ┌──Can hold in memory─┐
///                                │                                   │     │                     │
///     ┌──────────────┐           │           ┌──────────────┐        │     │  ┌──────────────┐
///     │ Sorted Spill │           │           │ Sorted Spill │        │     │  │ Sorted Spill │   │
///     │    file 5    │──────────────────────▶│    file 5    │────────────────▶│    file 5    │───┼───┐
///     └──────────────┘           │           └──────────────┘        │     │  └──────────────┘   │   │
///                                │                                   │     │                     │   │
///                                │           ┌──────────────┐        │     │  ┌──────────────┐       │
///                                │           │ Sorted Spill │        │     │  │ Sorted Spill │   │   │       ┌── ─── ─── ─── ─── ─── ─── ──┐
///                                └──────────▶│    file 6    │────────────────▶│    file 6    │───┼───┼──────▶         Output Stream
///                                            └──────────────┘        │     │  └──────────────┘   │   │       └── ─── ─── ─── ─── ─── ─── ──┘
///                                                                    │     │                     │   │
///                                                                    │     │  ┌──────────────┐       │
///                                                                    │     │  │ Sorted Spill │   │   │
///                                                                    └───────▶│    file 7    │───┼───┘
///                                                                          │  └──────────────┘   │
///                                                                          │                     │
///                                                                          └─ ──── ──── ──── ────
/// ```
///
/// ## Memory Management Strategy
///
/// This multi-level merge make sure that we can handle any amount of data to sort as long as
/// we have enough memory to merge at least 2 streams at a time, even when individual record
/// batches are skewed (very wide).
///
/// 1. **Worst-Case Memory Reservation**: Reserves memory based on the largest
///    batch size encountered in each spill file to merge, ensuring sufficient memory is always
///    available during merge operations.
/// 2. **Adaptive Buffer Sizing**: Reduces buffer sizes when memory is constrained
/// 3. **Spill-to-Disk**: Spill to disk when we cannot merge all files in memory
/// 4. **Re-spilling Skewed Runs**: If even at the smallest read-buffer size we still cannot
///    reserve memory for the minimum of 2 streams - because a single run's largest batch is so
///    wide that two streams' worth of reservation exceeds the budget - the larger of the two
///    runs is re-spilled with each batch sliced in half. This shrinks its largest batch,
///    lowering the per-stream reservation, and the merge pass is retried. The merge output
///    batch size is halved as well so the merged run cannot rebuild a full-size batch and
///    reintroduce the skew. If a batch cannot be split any further (a single row wider than the
///    budget), the merge surfaces `ResourcesExhausted` instead of looping forever.
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
}

impl Debug for MultiLevelMergeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiLevelMergeBuilder")
    }
}

impl MultiLevelMergeBuilder {
    #[expect(clippy::too_many_arguments)]
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
            let mut stream = match self.merge_sorted_runs_within_mem_limit()? {
                MergeStep::Stream(stream) => stream,
                MergeStep::SplitThenRetry(index) => {
                    // Couldn't reserve memory for the minimum of 2 streams. Re-spill the
                    // larger of the two we're trying to merge with half its batch size so
                    // its largest batch shrinks, lowering the per-stream reservation, then
                    // retry. Makes the merge resilient to skewed (very wide) rows.
                    self.split_spill_file_in_half(index).await?;
                    continue;
                }
            };

            // TODO - add a threshold for number of files to disk even if empty and reading from disk so
            //        we can avoid the memory reservation

            // If no spill files are left, we can return the stream as this is the last sorted run
            // TODO - We can write to disk before reading it back to avoid having multiple streams in memory
            if self.sorted_spill_files.is_empty() {
                assert!(
                    self.sorted_streams.is_empty(),
                    "We should not have any sorted streams left"
                );

                return Ok(stream);
            }

            // Need to sort to a spill file
            let Some((spill_file, max_record_batch_memory)) = self
                .spill_manager
                .spill_record_batch_stream_and_return_max_batch_memory(
                    &mut stream,
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

    /// This tries to create a stream that merges the most sorted streams and sorted spill files
    /// as possible within the memory limit.
    fn merge_sorted_runs_within_mem_limit(&mut self) -> Result<MergeStep> {
        match (self.sorted_spill_files.len(), self.sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => Ok(MergeStep::Stream(Box::pin(EmptyRecordBatchStream::new(
                Arc::clone(&self.schema),
            )))),

            // Only in-memory stream, return that
            (0, 1) => Ok(MergeStep::Stream(self.sorted_streams.remove(0))),

            // Only single sorted spill file so return it
            (1, 0) => {
                let spill_file = self.sorted_spill_files.remove(0);

                // Not reserving any memory for this disk as we are not holding it in memory
                Ok(MergeStep::Stream(
                    self.spill_manager
                        .read_spill_as_stream(spill_file.file, None)?,
                ))
            }

            // Only in memory streams, so merge them all in a single pass
            (0, _) => {
                let sorted_stream = mem::take(&mut self.sorted_streams);
                Ok(MergeStep::Stream(self.create_new_merge_sort(
                    sorted_stream,
                    // If we have no sorted spill files left, this is the last run
                    true,
                    true,
                )?))
            }

            // Need to merge multiple streams
            (_, _) => {
                // Transfer any pre-reserved bytes (from sort_spill_reservation_bytes)
                // to the merge memory reservation. This prevents starvation when
                // concurrent sort partitions compete for pool memory: the pre-reserved
                // bytes cover spill file buffer reservations without additional pool
                // allocation.
                let mut memory_reservation = self.reservation.take();

                // Compute the minimum before taking the in-memory streams so that, if we
                // need to re-spill and retry, `self.sorted_streams` is left untouched.
                let minimum_number_of_required_streams =
                    2_usize.saturating_sub(self.sorted_streams.len());

                let (sorted_spill_files, buffer_size) = match self
                    .get_sorted_spill_files_to_merge(
                        2,
                        // we must have at least 2 streams to merge
                        minimum_number_of_required_streams,
                        &mut memory_reservation,
                    )? {
                    SpillFilesToMerge::Ready(sorted_spill_files, buffer_size) => {
                        (sorted_spill_files, buffer_size)
                    }
                    // Not enough memory to seat 2 streams. Re-spill the blocking file
                    // smaller and retry. `get_sorted_spill_files_to_merge` already freed
                    // the reservation and `self.sorted_streams` is untouched, so the
                    // retry starts clean.
                    SpillFilesToMerge::SplitThenRetry(index) => {
                        return Ok(MergeStep::SplitThenRetry(index));
                    }
                };

                // Don't account for existing streams memory
                // as we are not holding the memory for them
                let mut sorted_streams = mem::take(&mut self.sorted_streams);

                let is_only_merging_memory_streams = sorted_spill_files.is_empty();

                // If no spill files were selected (e.g. all too large for
                // available memory but enough in-memory streams exist),
                // return the pre-reserved bytes to self.reservation so
                // create_new_merge_sort can transfer them to the merge
                // stream's BatchBuilder.
                if is_only_merging_memory_streams {
                    mem::swap(&mut self.reservation, &mut memory_reservation);
                }

                for spill in sorted_spill_files {
                    let stream = self
                        .spill_manager
                        .clone()
                        .with_batch_read_buffer_capacity(buffer_size)
                        .read_spill_as_stream(
                            spill.file,
                            Some(spill.max_record_batch_memory),
                        )?;
                    sorted_streams.push(stream);
                }
                let merge_sort_stream = self.create_new_merge_sort(
                    sorted_streams,
                    // If we have no sorted spill files left, this is the last run
                    self.sorted_spill_files.is_empty(),
                    is_only_merging_memory_streams,
                )?;

                // If we're only merging memory streams, we don't need to attach the memory reservation
                // as it's empty
                if is_only_merging_memory_streams {
                    assert_eq!(
                        memory_reservation.size(),
                        0,
                        "when only merging memory streams, we should not have any memory reservation and let the merge sort handle the memory"
                    );

                    Ok(MergeStep::Stream(merge_sort_stream))
                } else {
                    // Attach the memory reservation to the stream to make sure we have enough memory
                    // throughout the merge process as we bypassed the memory pool for the merge sort stream
                    Ok(MergeStep::Stream(Box::pin(StreamAttachedReservation::new(
                        merge_sort_stream,
                        memory_reservation,
                    ))))
                }
            }
        }
    }

    fn create_new_merge_sort(
        &mut self,
        streams: Vec<SendableRecordBatchStream>,
        is_output: bool,
        all_in_memory: bool,
    ) -> Result<SendableRecordBatchStream> {
        let mut builder = StreamingMergeBuilder::new()
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
            .with_streams(streams);

        if !all_in_memory {
            // Don't track memory used by this stream as we reserve that memory by worst case sceneries
            // (reserving memory for the biggest batch in each stream)
            // TODO - avoid this hack as this can be broken easily when `SortPreservingMergeStream`
            //        changes the implementation to use more/less memory
            builder = builder.with_bypass_mempool();
        } else {
            // If we are only merging in-memory streams, we need to use the memory reservation
            // because we don't know the maximum size of the batches in the streams.
            // Use take() to transfer any pre-reserved bytes so the merge can use them
            // as its initial budget without additional pool allocation.
            builder = builder.with_reservation(self.reservation.take());
        }

        builder.build()
    }

    /// Return the sorted spill files to use for the next phase, and the buffer size
    /// This will try to get as many spill files as possible to merge, and if we don't have enough streams
    /// it will try to reduce the buffer size until we have enough streams to merge
    /// otherwise it will return an error
    fn get_sorted_spill_files_to_merge(
        &mut self,
        buffer_len: usize,
        minimum_number_of_required_streams: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<SpillFilesToMerge> {
        assert_ne!(buffer_len, 0, "Buffer length must be greater than 0");
        let mut number_of_spills_to_read_for_current_phase = 0;
        // Track total memory needed for spill file buffers. When the
        // reservation has pre-reserved bytes (from sort_spill_reservation_bytes),
        // those bytes cover the first N spill files without additional pool
        // allocation, preventing starvation under memory pressure.
        let mut total_needed: usize = 0;

        for spill in &self.sorted_spill_files {
            let per_spill = get_reserved_bytes_for_record_batch_size(
                spill.max_record_batch_memory,
                // Size will be the same as the sliced size, bc it is a spilled batch.
                spill.max_record_batch_memory,
            ) * buffer_len;
            total_needed += per_spill;

            // For memory pools that are not shared this is good, for other
            // this is not and there should be some upper limit to memory
            // reservation so we won't starve the system.
            match try_grow_reservation_to_at_least(reservation, total_needed) {
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
                        if buffer_len > 1 {
                            // Try again with smaller buffer size, it will be slower but at least we can merge
                            return self.get_sorted_spill_files_to_merge(
                                buffer_len - 1,
                                minimum_number_of_required_streams,
                                reservation,
                            );
                        }

                        // buffer_len == 1 and we still can't seat the minimum of 2 streams.
                        if number_of_spills_to_read_for_current_phase == 0 {
                            // We couldn't even reserve a single stream - one record batch
                            // is larger than the whole merge budget. That's the lone-batch
                            // case, not the 2-stream merge skew we rescue here - surface it.
                            return Err(err);
                        }

                        // We seated one stream (index 0) but not the second (index 1, the
                        // batch that just failed to reserve). Those are by definition the
                        // only two streams we are trying to merge, so re-spill the larger
                        // of them with a smaller batch size and retry, the smaller max
                        // batch lowers the per-stream reservation enough to seat both.
                        let split_index = usize::from(
                            self.sorted_spill_files[1].max_record_batch_memory
                                > self.sorted_spill_files[0].max_record_batch_memory,
                        );
                        return Ok(SpillFilesToMerge::SplitThenRetry(split_index));
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

        Ok(SpillFilesToMerge::Ready(spills, buffer_len))
    }

    /// Re-spill the spill file at `index` with half its batch size, putting it back
    /// at the same position. We read the file back and re-spill it through the normal
    /// spill API (which owns batch layout).
    /// Slicing each batch in two halves the largest written batch,
    /// which lowers the per-stream merge reservation so the
    /// next attempt can seat both streams. One stream's worth of memory is reserved
    /// for the duration and freed afterwards. Makes the merge resilient to skew.
    async fn split_spill_file_in_half(&mut self, index: usize) -> Result<()> {
        log::debug!(
            "2 spilled streams could not be loaded into memory for merge \
        (requires 2x of the largest batch from both), re-spilling the larger of the two with half \
        the batch size to reduce memory needs for the next merge attempt, \
        setting batch_size to half to proceed with merge"
        );

        // Extract the target in O(1) instead of `remove(index)`, which would shift
        // every following spill file. Swap it to the back and pop it; the matching
        // swap after re-spilling restores the original order, so the vec ends up
        // exactly as it started, just with the target file shrunk.
        let last = self.sorted_spill_files.len() - 1;
        self.sorted_spill_files.swap(index, last);
        let target = self
            .sorted_spill_files
            .pop()
            .expect("index is in bounds, so the vec is non-empty");
        let old_max = target.max_record_batch_memory;

        // Reserve enough to hold a single stream of this file while we re-spill it.
        let reservation = self.reservation.new_empty();
        reservation
            .try_grow(get_reserved_bytes_for_record_batch_size(old_max, old_max))?;

        let source = self
            .spill_manager
            .read_spill_as_stream(target.file, Some(old_max))?;
        // Re-spill with half the batch size: slice every batch in two. The spill
        // writer owns the batch layout, we only change how many rows per batch.
        let mut halved: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                source.flat_map(|batch| {
                    futures::stream::iter(match batch {
                        Ok(batch) => split_batch_in_half(batch)
                            .into_iter()
                            .map(Ok)
                            .collect::<Vec<_>>(),
                        Err(e) => vec![Err(e)],
                    })
                }),
            ));

        let result = self
            .spill_manager
            .spill_record_batch_stream_and_return_max_batch_memory(
                &mut halved,
                "MultiLevelMergeBuilder split skewed spill",
            )
            .await?;

        reservation.free();

        let Some((file, new_max)) = result else {
            return internal_err!("re-spilling a skewed spill file produced no data");
        };

        // If halving could not reduce the largest batch (e.g. a single row that is
        // itself wider than the budget), there is nothing more we can do - surface
        // the out-of-memory condition instead of looping forever.
        if new_max >= old_max {
            return resources_err!(
                "Cannot merge sorted runs: a single record batch of {old_max} bytes \
                 exceeds the available merge memory and cannot be split further"
            );
        }

        // Also halve the merge output batch size so the next merge pass emits
        // narrower batches. Otherwise the merged stream would rebuild a full-size
        // (potentially giant) batch and, when spilled back as an intermediate run,
        // reintroduce the exact skew we just resolved.
        self.batch_size = (self.batch_size / 2).max(1);

        // Push the re-spilled (smaller) file and swap it back into `index`, undoing
        // the swap-to-back above so the order is preserved.
        self.sorted_spill_files.push(SortedSpillFile {
            file,
            max_record_batch_memory: new_max,
        });
        let last = self.sorted_spill_files.len() - 1;
        self.sorted_spill_files.swap(index, last);

        Ok(())
    }
}

/// Outcome of trying to reserve memory for one multi-level merge pass.
enum SpillFilesToMerge {
    /// Enough memory: the spill files to read this pass and the read-ahead buffer size.
    Ready(Vec<SortedSpillFile>, usize),
    /// Could not seat the minimum of 2 streams. Re-spill the spill file at this index
    /// with a smaller (halved) batch size, then retry the pass.
    SplitThenRetry(usize),
}

/// What one iteration of the multi-level merge loop should do next.
enum MergeStep {
    /// A merged stream is ready to be consumed (and possibly spilled back).
    Stream(SendableRecordBatchStream),
    /// Re-spill the spill file at this index smaller, then retry the merge step.
    SplitThenRetry(usize),
}

/// Slice `batch` into two row-halves so a re-spill writes batches half the size.
fn split_batch_in_half(batch: RecordBatch) -> Vec<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return vec![batch];
    }
    let mid = num_rows / 2;
    vec![batch.slice(0, mid), batch.slice(mid, num_rows - mid)]
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::PhysicalSortExpr;
    use arrow::array::{AsArray, Int64Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use datafusion_execution::runtime_env::RuntimeEnv;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn build_spill_manager(env: &Arc<RuntimeEnv>, schema: &SchemaRef) -> SpillManager {
        SpillManager::new(
            Arc::clone(env),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(schema),
        )
    }

    /// Spill `values` (which must already be sorted) as a single sorted run and
    /// return it as a `SortedSpillFile` carrying its recorded largest-batch memory.
    fn make_sorted_spill_file(
        spill_manager: &SpillManager,
        schema: &SchemaRef,
        values: Vec<i64>,
    ) -> SortedSpillFile {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let batches: Vec<Result<RecordBatch>> = vec![Ok(batch)];
        let (file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches.into_iter(),
                "test input run",
            )
            .unwrap()
            .expect("spill should produce a file");
        SortedSpillFile {
            file,
            max_record_batch_memory,
        }
    }

    fn build_merge_builder(
        spill_manager: SpillManager,
        schema: SchemaRef,
        sorted_spill_files: Vec<SortedSpillFile>,
        pool: &Arc<dyn MemoryPool>,
        batch_size: usize,
    ) -> MultiLevelMergeBuilder {
        let reservation = MemoryConsumer::new("test merge").register(pool);
        let expr: LexOrdering =
            [PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))].into();
        MultiLevelMergeBuilder::new(
            spill_manager,
            schema,
            sorted_spill_files,
            vec![],
            expr,
            BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            batch_size,
            reservation,
            None,
            false,
        )
    }

    /// Two sorted runs whose largest batches are too big to both
    /// be seated in the merge budget at once are re-spilled (halved) until they
    /// fit, and the merge then completes with fully sorted, complete output.
    #[tokio::test]
    async fn skewed_runs_are_respilled_so_the_merge_fits() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);

        let n: i64 = 16384;
        let f0 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let f1 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let m = f0.max_record_batch_memory.max(f1.max_record_batch_memory);

        // Seating two streams needs ~4*m (2*m each), which does NOT fit, but the
        // budget is large enough once a run is halved. The rescue keeps halving
        // the blocking run until two streams fit (here, after one halving).
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(m * 7 / 2));

        let builder = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![f0, f1],
            &pool,
            8192,
        );
        let stream = builder.create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows,
            (2 * n) as usize,
            "the merge must emit every input row"
        );

        let merged = concat_batches(&schema, &batches)?;
        let col = merged.column(0).as_primitive::<Int64Type>();
        for i in 1..col.len() {
            assert!(
                col.value(i - 1) <= col.value(i),
                "merge output must be sorted: {} > {} at {i}",
                col.value(i - 1),
                col.value(i),
            );
        }

        Ok(())
    }

    /// Tests the `new_max >= old_max` guard: a single-row run cannot be split
    /// any smaller, so re-spilling it does not shrink the largest batch and the
    /// rescue surfaces `ResourcesExhausted` rather than looping forever.
    #[tokio::test]
    async fn respilling_an_unsplittable_run_surfaces_resources_exhausted() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);

        // A one-row run: `split_batch_in_half` returns it unchanged, so the
        // re-spilled file's largest batch cannot drop below the original.
        let f0 = make_sorted_spill_file(&spill_manager, &schema, vec![42]);

        // Ample budget so the only possible failure is the un-splittable guard,
        // not the single-stream reservation itself.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let mut builder =
            build_merge_builder(spill_manager, schema, vec![f0], &pool, 1024);

        let err = builder
            .split_spill_file_in_half(0)
            .await
            .expect_err("re-spilling a one-row run cannot shrink it");
        assert!(
            err.to_string().contains("cannot be split further"),
            "expected the un-splittable guard error, got: {err}"
        );

        Ok(())
    }

    /// Proves the re-spill also halves the merge output batch size: after one
    /// re-spill the merged run is emitted in 4096-row batches (not the original
    /// 8192), so it cannot rebuild a full-size batch and reintroduce the skew.
    #[tokio::test]
    async fn respill_halves_the_merge_output_batch_size() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);

        let n: i64 = 16384;
        let f0 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let f1 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let m = f0.max_record_batch_memory.max(f1.max_record_batch_memory);

        // 3.5*m forces exactly one re-spill (split one run, then both fit), which
        // halves the merge output batch size.
        let initial_batch_size = 8192;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(m * 7 / 2));

        let builder = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![f0, f1],
            &pool,
            initial_batch_size,
        );
        let stream = builder.create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        // All rows are still present.
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, (2 * n) as usize);

        // The largest emitted batch is the halved size, not the original 8192 —
        // without halving `self.batch_size` the merge would rebuild 8192-row batches.
        let expected_batch_size = initial_batch_size / 2;
        let max_batch_rows = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
        assert_eq!(
            max_batch_rows, expected_batch_size,
            "after one re-spill the merge must emit {expected_batch_size}-row \
             batches, got a largest batch of {max_batch_rows} rows"
        );

        Ok(())
    }
}
