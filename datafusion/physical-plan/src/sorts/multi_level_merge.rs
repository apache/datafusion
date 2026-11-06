// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
use datafusion_execution::memory_pool::{MemoryReservation, MergeMemoryPool};

use crate::sorts::builder::{
    interleave_memory_size_from_sliced, schema_supports_byte_target,
    spill_output_batch_targets, try_grow_reservation_to_at_least,
};
use crate::sorts::sort::get_reserved_bytes_for_record_batch_size;
#[cfg(test)]
use crate::sorts::streaming_merge::SortedSpillFile;
use crate::sorts::streaming_merge::{SpillFileWithMemory, StreamingMergeBuilder};
use crate::spill::gc_view_arrays;
use crate::spill::spill_manager::GetSlicedSize;
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};
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
///    lowering the per-stream reservation, and the merge pass is retried. The re-spilled run
///    is tracked alongside a per-run batch-size limit equal to half the batch size it was
///    written with, so any later merge that includes it caps its output batch size to match -
///    otherwise the merged run could rebuild a full-size batch and reintroduce the skew.
///    Crucially the global merge batch size is *not* lowered, so re-spilling more than one run
///    does not compound the reduction. If a batch cannot be split any further (a single row
///    wider than the budget), the merge surfaces `ResourcesExhausted` instead of looping
///    forever.
pub(crate) struct MultiLevelMergeBuilder {
    spill_manager: SpillManager,
    schema: SchemaRef,
    /// Sorted runs still to be merged. Each run is paired with the batch-size limit a
    /// merge consuming it must cap its output at. Runs written at the full batch size
    /// carry `batch_size`. A run re-spilled smaller to resolve skew carries its halved
    /// limit (see [`Self::split_spill_file_in_half`]). Tracking it here keeps this limit
    /// out of the public [`SortedSpillFile`](super::streaming_merge::SortedSpillFile), so no external caller has to set it.
    sorted_spill_files: Vec<(SpillFileWithMemory, usize)>,
    sorted_streams: Vec<SendableRecordBatchStream>,
    expr: LexOrdering,
    metrics: BaselineMetrics,
    batch_size: usize,
    reservation: MemoryReservation,
    /// Workspace retained across retries and intermediate spill passes.
    merge_pool: Option<Arc<MergeMemoryPool>>,
    /// Leave memory for the aggregate consuming the merged spill rows.
    reserve_replay_headroom: bool,
    /// Avoid merging more runs than needed before the final replay pass.
    size_intermediate_merges: bool,
    fetch: Option<usize>,
    enable_round_robin_tie_breaker: bool,
}

impl Debug for MultiLevelMergeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiLevelMergeBuilder")
    }
}

impl MultiLevelMergeBuilder {
    #[cfg(test)]
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
        Self::new_with_memory(
            spill_manager,
            schema,
            sorted_spill_files.into_iter().map(Into::into).collect(),
            sorted_streams,
            expr,
            metrics,
            batch_size,
            reservation,
            fetch,
            enable_round_robin_tie_breaker,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new_with_memory(
        spill_manager: SpillManager,
        schema: SchemaRef,
        sorted_spill_files: Vec<SpillFileWithMemory>,
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
            // Initial runs are written at the full batch size, so they impose no cap
            // on later merges - record `batch_size` as their (unconstrained) limit.
            sorted_spill_files: sorted_spill_files
                .into_iter()
                .map(|file| (file, batch_size))
                .collect(),
            sorted_streams,
            expr,
            metrics,
            batch_size,
            reservation,
            merge_pool: None,
            reserve_replay_headroom: false,
            size_intermediate_merges: false,
            enable_round_robin_tie_breaker,
            fetch,
        }
    }

    pub(super) fn with_merge_pool(mut self, pool: Option<Arc<MergeMemoryPool>>) -> Self {
        self.merge_pool = pool;
        self
    }

    /// Leave replay headroom while selecting merge buffers. Temporary splitting
    /// workspace can still use the full pool because replay has not started.
    pub(super) fn with_replay_headroom(mut self, reserve: bool) -> Self {
        self.reserve_replay_headroom = reserve;
        self
    }

    pub(super) fn with_intermediate_merge_sizing(
        mut self,
        min_spill_batch_rows: Option<usize>,
    ) -> Self {
        // Short inputs can produce larger batches when merged, reducing the next
        // pass's fan-in. Fetch can also leave intermediate runs with short batches.
        self.size_intermediate_merges = self.fetch.is_none()
            && min_spill_batch_rows.is_some_and(|rows| rows >= self.batch_size);
        self
    }

    pub(crate) fn create_spillable_merge_stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(self.create_stream()).try_flatten(),
        ))
    }

    async fn create_stream(mut self) -> Result<SendableRecordBatchStream> {
        let mut allow_minimum_without_headroom = false;
        loop {
            let (mut stream, batch_size_limit) = match self
                .merge_sorted_runs_within_mem_limit(allow_minimum_without_headroom)?
            {
                MergeStep::Stream {
                    stream,
                    batch_size_limit,
                } => (stream, batch_size_limit),
                MergeStep::SplitThenRetry(index) => {
                    // Couldn't reserve memory for the minimum of 2 streams. Re-spill
                    // the larger of the two we're trying to merge with half its batch
                    // size so its largest batch shrinks, lowering the per-stream
                    // reservation, then retry. Makes the merge resilient to skewed
                    // (very wide) rows.
                    let retry_unsplittable =
                        self.reserve_replay_headroom && !allow_minimum_without_headroom;
                    if !self
                        .split_spill_file_in_half(index, retry_unsplittable)
                        .await?
                    {
                        // A single row may prevent leaving replay headroom while
                        // the minimum merge still fits the actual shared pool.
                        allow_minimum_without_headroom = true;
                    }
                    continue;
                }
            };
            allow_minimum_without_headroom = false;

            // TODO - add a threshold for number of files to disk even if empty and reading from disk so
            //        we can avoid the memory reservation

            // If no spill files are left, we can return the stream as this is the last sorted run
            // TODO - We can write to disk before reading it back to avoid having multiple streams in memory
            if self.sorted_spill_files.is_empty() {
                assert!(
                    self.sorted_streams.is_empty(),
                    "We should not have any sorted streams left"
                );

                // The final pass has its buffer budget and needs no future spill
                // workspace. Keep live reservations, but release the idle floor.
                if let Some(pool) = &self.merge_pool {
                    pool.release_unused();
                }

                return Ok(stream);
            }

            // Need to sort to a spill file
            let Some((spill_file, max_record_batch_memory, max_batch_rows)) = self
                .spill_manager
                .spill_record_batch_stream_with_memory(
                    &mut stream,
                    "MultiLevelMergeBuilder intermediate spill",
                )
                .await?
            else {
                continue;
            };

            // Offset overflow can flush partial batches even without a fetch limit.
            // Keep sizing disabled once a run cannot fill its output batch limit.
            self.size_intermediate_merges &= max_batch_rows >= batch_size_limit;

            // Add the spill file paired with the batch-size limit of the merge that
            // produced it: if that merge consumed a shrunk (skew-resolved) run, its
            // output was capped and this intermediate run is likewise capped, so a
            // later pass that re-merges it won't rebuild an oversized batch.
            self.sorted_spill_files.push((
                SpillFileWithMemory {
                    file: spill_file,
                    max_record_batch_memory: max_record_batch_memory.input,
                    progress: max_record_batch_memory.progress,
                },
                batch_size_limit,
            ));
        }
    }

    // Mixed memory/spill inputs have no common input grant from which shared
    // View capacity can be credited. Keep their pre-existing untargeted fallback
    // instead of repeatedly charging the same shared buffers to construction.
    fn supports_byte_target(&self) -> bool {
        schema_supports_byte_target(&self.schema)
            && (self.sorted_streams.is_empty()
                || !self
                    .schema
                    .fields()
                    .iter()
                    .any(|field| super::output_memory::contains_view(field.data_type())))
    }

    /// This tries to create a stream that merges the most sorted streams and sorted spill files
    /// as possible within the memory limit.
    fn merge_sorted_runs_within_mem_limit(
        &mut self,
        allow_minimum_without_headroom: bool,
    ) -> Result<MergeStep> {
        match (self.sorted_spill_files.len(), self.sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => {
                let empty_stream =
                    Box::pin(EmptyRecordBatchStream::new(Arc::clone(&self.schema)));
                Ok(MergeStep::Stream {
                    stream: self.observe_output(empty_stream),
                    batch_size_limit: self.batch_size,
                })
            }

            // Only in-memory stream, return that
            (0, 1) => {
                let output_stream = self.sorted_streams.remove(0);
                Ok(MergeStep::Stream {
                    stream: self.observe_output(output_stream),
                    batch_size_limit: self.batch_size,
                })
            }

            // Only single sorted spill file so return it
            (1, 0) => {
                let (spill_file, batch_size) = self.sorted_spill_files.remove(0);

                // Not reserving any memory for this disk as we are not holding it in memory
                let output_stream = self
                    .spill_manager
                    .read_spill_as_stream(spill_file.file, None)?;

                Ok(MergeStep::Stream {
                    stream: self.observe_output(output_stream),
                    batch_size_limit: batch_size,
                })
            }

            // Only in memory streams, so merge them all in a single pass. In-memory
            // runs are never shrunk for skew, so this merge runs at the full batch
            // size and its output carries no limit.
            (0, _) => {
                let sorted_stream = mem::take(&mut self.sorted_streams);
                // No need to wrap with observed stream since merge sort will update the observed metrics
                Ok(MergeStep::Stream {
                    stream: self.create_new_merge_sort(
                        sorted_stream,
                        // If we have no sorted spill files left, this is the last run
                        true,
                        true,
                        self.batch_size,
                        None,
                        None,
                        false,
                    )?,
                    batch_size_limit: self.batch_size,
                })
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

                let (mut sorted_spill_files, buffer_size, output_headroom) = match self
                    .get_sorted_spill_files_to_merge(
                        2,
                        // we must have at least 2 streams to merge
                        minimum_number_of_required_streams,
                        &mut memory_reservation,
                        allow_minimum_without_headroom,
                    )? {
                    SpillFilesToMerge::Ready(
                        sorted_spill_files,
                        buffer_size,
                        output_headroom,
                    ) => (sorted_spill_files, buffer_size, output_headroom),
                    // Not enough memory to seat 2 streams. Re-spill the blocking file
                    // smaller and retry. `get_sorted_spill_files_to_merge` already freed
                    // the reservation and `self.sorted_streams` is untouched, so the
                    // retry starts clean.
                    SpillFilesToMerge::SplitThenRetry(index) => {
                        return Ok(MergeStep::SplitThenRetry(index));
                    }
                };

                let is_only_merging_memory_streams = sorted_spill_files.is_empty();
                let supports_byte_target = self.supports_byte_target();
                // A row-progress grant may exceed the preferred output size.
                // Keep that space reserved without inflating the prefix target;
                // BatchBuilder permits one row to exceed this target after E admission.
                let target_batch_bytes =
                    if !is_only_merging_memory_streams && supports_byte_target {
                        let preferred = sorted_spill_files
                            .iter()
                            .try_fold(0usize, |max, (file, rows)| {
                                spill_output_batch_targets(
                                    &self.schema,
                                    *rows,
                                    file.max_record_batch_memory,
                                )
                                .map(|(_, preferred)| max.max(preferred))
                            })
                            .ok_or_else(|| {
                                datafusion_common::resources_datafusion_err!(
                                    "Spill merge output target exceeds usize::MAX"
                                )
                            })?;
                        Some(output_headroom.min(preferred))
                    } else {
                        None
                    };
                if is_only_merging_memory_streams {
                    debug_assert_eq!(target_batch_bytes, None);
                    debug_assert_eq!(output_headroom, 0);
                } else if supports_byte_target {
                    debug_assert!(
                        target_batch_bytes
                            .is_some_and(|target| target <= output_headroom)
                    );
                } else {
                    debug_assert_eq!(target_batch_bytes, None);
                    debug_assert_eq!(output_headroom, 0);
                }
                let output_construction_reservation =
                    target_batch_bytes.map(|_| memory_reservation.split(output_headroom));

                if self.size_intermediate_merges
                    && self.sorted_streams.is_empty()
                    && !self.sorted_spill_files.is_empty()
                {
                    // The next admission starts with two read buffers per run.
                    // Fit both those buffers and any replay headroom in the
                    // existing grant: another partition may take every spare
                    // byte while this intermediate pass writes to disk.
                    let final_fan_in = sorted_spill_files.len() * buffer_size / 2;
                    let final_fan_in = if self.reserve_replay_headroom {
                        final_fan_in / 2
                    } else {
                        final_fan_in
                    };
                    let needed = self.sorted_spill_files.len() + sorted_spill_files.len()
                        - final_fan_in
                        + 1;
                    if needed < sorted_spill_files.len() {
                        let (first, first_limit) = &sorted_spill_files[0];
                        // A wider pending run or a smaller output batch limit can
                        // change the next pass's fan-in and add intermediate work.
                        // Apply this sizing heuristic only to uniform run budgets.
                        let uniform_runs = sorted_spill_files
                            .iter()
                            .chain(&self.sorted_spill_files)
                            .all(|(file, limit)| {
                                file.max_record_batch_memory
                                    == first.max_record_batch_memory
                                    && limit == first_limit
                            });
                        if uniform_runs {
                            let unused = sorted_spill_files.split_off(needed);
                            self.sorted_spill_files.splice(0..0, unused);
                            // Keep the grant through intermediate EOF and the
                            // spill write, then transfer it to the next admission.
                            // This does not reserve additional pool memory.
                            // Output construction has its own reservation; retain the
                            // input grant here while the intermediate stream owns its output.
                            self.reservation = memory_reservation.take();
                        }
                    }
                }

                // Don't account for existing streams memory
                // as we are not holding the memory for them
                let shared_input_buffers_reserved = self.sorted_streams.is_empty();
                let mut sorted_streams = mem::take(&mut self.sorted_streams);

                // If no spill files were selected (e.g. all too large for
                // available memory but enough in-memory streams exist),
                // return the pre-reserved bytes to self.reservation so
                // create_new_merge_sort can transfer them to the merge
                // stream's BatchBuilder.
                if is_only_merging_memory_streams {
                    mem::swap(&mut self.reservation, &mut memory_reservation);
                }

                // Cap the merge output at the smallest limit among the runs we're
                // about to merge. Runs that were shrunk for skew carry a smaller limit,
                // if none do, every run carries `self.batch_size` and the merge runs at
                // the full batch size. The output stream is tagged with the same limit
                // (see the `MergeStep::Stream` returns below) so a re-spilled
                // intermediate run stays shrunk and won't rebuild an oversized batch on
                // a later pass.
                let mut output_batch_size = self.batch_size;
                for (spill, batch_size_limit) in sorted_spill_files {
                    let stream = self
                        .spill_manager
                        .clone()
                        .with_batch_read_buffer_capacity(buffer_size)
                        .read_spill_as_stream(
                            spill.file,
                            Some(spill.max_record_batch_memory),
                        )?;
                    output_batch_size = output_batch_size.min(batch_size_limit);
                    sorted_streams.push(stream);
                }
                let merge_sort_stream = self.create_new_merge_sort(
                    sorted_streams,
                    // If we have no sorted spill files left, this is the last run
                    self.sorted_spill_files.is_empty(),
                    is_only_merging_memory_streams,
                    output_batch_size,
                    target_batch_bytes,
                    output_construction_reservation,
                    shared_input_buffers_reserved,
                )?;

                // If we're only merging memory streams, we don't need to attach the memory reservation
                // as it's empty
                if is_only_merging_memory_streams {
                    assert_eq!(
                        memory_reservation.size(),
                        0,
                        "when only merging memory streams, we should not have any memory reservation and let the merge sort handle the memory"
                    );

                    Ok(MergeStep::Stream {
                        stream: merge_sort_stream,
                        batch_size_limit: output_batch_size,
                    })
                } else {
                    // Attach the memory reservation to the stream to make sure we have enough memory
                    // throughout the merge process as we bypassed the memory pool for the merge sort stream
                    Ok(MergeStep::Stream {
                        stream: Box::pin(StreamAttachedReservation::new(
                            merge_sort_stream,
                            memory_reservation,
                        )),
                        batch_size_limit: output_batch_size,
                    })
                }
            }
        }
    }

    #[expect(clippy::too_many_arguments)]
    fn create_new_merge_sort(
        &mut self,
        streams: Vec<SendableRecordBatchStream>,
        is_output: bool,
        all_in_memory: bool,
        output_batch_size: usize,
        target_batch_bytes: Option<usize>,
        output_construction_reservation: Option<MemoryReservation>,
        shared_input_buffers_reserved: bool,
    ) -> Result<SendableRecordBatchStream> {
        let mut builder = StreamingMergeBuilder::new()
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr)
            .with_batch_size(output_batch_size)
            .with_target_batch_bytes(target_batch_bytes)
            .with_output_construction_reservation(output_construction_reservation)
            // A pure spill merge holds its read grant through output construction.
            // Its metadata must bound decoded batches, and consumed batches must
            // be drained before a reader replacement. Mixed memory/spill inputs
            // have no such common grant: retain conservative shared admission.
            .with_reserved_shared_input_buffers(shared_input_buffers_reserved)
            // Intermediate spills and sort output keep construction workspace between
            // polls. Only final aggregate replay shares that headroom with its consumer.
            .with_retained_output_construction_reservation(
                !self.reserve_replay_headroom || !is_output,
            )
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
        allow_minimum_without_headroom: bool,
    ) -> Result<SpillFilesToMerge> {
        assert_ne!(buffer_len, 0, "Buffer length must be greater than 0");
        let mut number_of_spills_to_read_for_current_phase = 0;
        let configured_fan_in = self
            .spill_manager
            .env()
            .disk_manager
            .max_spill_merge_fan_in();
        let supports_byte_target = self.supports_byte_target();
        let max_spill_files = effective_spill_merge_fan_in(configured_fan_in);
        let recursive = supports_byte_target
            && crate::sorts::schema_needs_recursive_read_bound(self.schema.as_ref());
        // The row-progress proof applies only to writer-measured pure spills.
        // Unknown public files and memory streams retain the conservative path.
        let measured_progress = recursive
            && self.sorted_streams.is_empty()
            && self
                .sorted_spill_files
                .iter()
                .all(|(file, _)| file.progress.is_some());
        let max_spill_files = if recursive && !measured_progress {
            max_spill_files.min(2_usize.saturating_sub(self.sorted_streams.len()))
        } else {
            max_spill_files
        };
        // Track total memory needed for spill file buffers. When the
        // reservation has pre-reserved bytes (from sort_spill_reservation_bytes),
        // those bytes cover the first N spill files without additional pool
        // allocation, preventing starvation under memory pressure.
        let mut total_needed: usize = 0;
        let mut accepted_memory: usize = 0;
        let mut output_headroom: usize = 0;
        let mut minimum_output_headroom: usize = 0;
        let mut preferred_output_headroom: usize = 0;
        let mut use_preferred_output_headroom = true;
        let mut selected_buffer_len = buffer_len;
        let mut workspace_sum = 0usize;
        let mut max_row_content = 0usize;
        for (spill, max_batch_rows) in &self.sorted_spill_files {
            if number_of_spills_to_read_for_current_phase >= max_spill_files
                || (allow_minimum_without_headroom
                    && number_of_spills_to_read_for_current_phase
                        >= minimum_number_of_required_streams)
            {
                break;
            }

            // Two batches per read slot, independently of construction. Check
            // each operation rather than allowing a large user budget to wrap.
            let per_spill = spill
                .max_record_batch_memory
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_mul(buffer_len))
                .ok_or_else(|| {
                    datafusion_common::resources_datafusion_err!(
                        "Spill merge read grant exceeds usize::MAX"
                    )
                })?;
            total_needed = total_needed.checked_add(per_spill).ok_or_else(|| {
                datafusion_common::resources_datafusion_err!(
                    "Spill merge read grant exceeds usize::MAX"
                )
            })?;
            // With at most one current batch per input, sum(W_i) covers the
            // estimator. Twice that covers input-dependent reference/mapping
            // tables during interleave; max(P_i) covers one row's content.
            // Keep this floor even when preferred or replay headroom is dropped.
            let progress_floor = if measured_progress {
                let stats = spill.progress.expect("all selected files have statistics");
                workspace_sum =
                    workspace_sum.checked_add(stats.workspace).ok_or_else(|| {
                        datafusion_common::resources_datafusion_err!(
                            "Spill merge workspace exceeds usize::MAX"
                        )
                    })?;
                max_row_content = max_row_content.max(stats.row_content);
                workspace_sum
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(max_row_content))
                    .ok_or_else(|| {
                        datafusion_common::resources_datafusion_err!(
                            "Spill merge single-row headroom exceeds usize::MAX"
                        )
                    })?
            } else {
                0
            };
            let (candidate_minimum_output_headroom, candidate_preferred_output_headroom) =
                if supports_byte_target {
                    let (minimum, preferred) = spill_output_batch_targets(
                        &self.schema,
                        *max_batch_rows,
                        spill.max_record_batch_memory,
                    )
                    .ok_or_else(|| {
                        datafusion_common::DataFusionError::ResourcesExhausted(
                            "Spill merge output headroom exceeds usize::MAX".to_string(),
                        )
                    })?;
                    (
                        minimum_output_headroom.max(minimum).max(progress_floor),
                        preferred_output_headroom.max(preferred).max(progress_floor),
                    )
                } else {
                    (0, 0)
                };

            // If a run cannot shrink, allow only the minimum merge without
            // replay headroom. Disable read-ahead and still ask the pool for
            // every byte used by the merge.
            let skip_headroom = allow_minimum_without_headroom
                && buffer_len == 1
                && number_of_spills_to_read_for_current_phase
                    < minimum_number_of_required_streams;
            let check_headroom = self.reserve_replay_headroom && !skip_headroom;
            // Output construction is charged separately from the input buffers.
            // Sort output retains it across batches. Aggregate replay releases it
            // after building each batch, so replay and construction can reuse the
            // larger of their two headroom requirements.
            let mut selected_output_headroom = if use_preferred_output_headroom {
                candidate_preferred_output_headroom
            } else {
                candidate_minimum_output_headroom
            };
            let mut try_admission = |candidate_output_headroom: usize| {
                let required_headroom = if check_headroom {
                    total_needed.max(candidate_output_headroom)
                } else {
                    candidate_output_headroom
                };
                match total_needed.checked_add(required_headroom) {
                    Some(with_headroom) => {
                        try_grow_reservation_to_at_least(reservation, with_headroom)
                    }
                    None => resources_err!("Spill merge headroom exceeds usize::MAX"),
                }
            };
            let mut admission = try_admission(selected_output_headroom);
            if admission.is_err()
                && selected_output_headroom > candidate_minimum_output_headroom
            {
                use_preferred_output_headroom = false;
                selected_output_headroom = candidate_minimum_output_headroom;
                admission = try_admission(selected_output_headroom);
            }
            match admission {
                Ok(_) => {
                    number_of_spills_to_read_for_current_phase += 1;
                    accepted_memory = total_needed;
                    minimum_output_headroom = candidate_minimum_output_headroom;
                    preferred_output_headroom = candidate_preferred_output_headroom;
                    output_headroom = selected_output_headroom;
                }
                // If we can't grow the reservation, we need to stop
                Err(_) => {
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
                                allow_minimum_without_headroom,
                            );
                        }

                        // buffer_len == 1 and we still can't seat the minimum of 2 streams.
                        if number_of_spills_to_read_for_current_phase == 0 {
                            if check_headroom {
                                // Replay has not started, so splitting may use the
                                // full pool to leave room for replay afterward.
                                return Ok(SpillFilesToMerge::SplitThenRetry(0));
                            }
                            // If the input buffers alone fit, re-spill smaller to make room
                            // for output construction; otherwise return the input
                            // reservation error.
                            match try_grow_reservation_to_at_least(
                                reservation,
                                total_needed,
                            ) {
                                Ok(()) => {
                                    reservation.free();
                                    return Ok(SpillFilesToMerge::SplitThenRetry(0));
                                }
                                Err(input_err) => {
                                    reservation.free();
                                    return Err(input_err);
                                }
                            }
                        }

                        // We seated one stream (index 0) but not the second (index 1, the
                        // batch that just failed to reserve). Those are by definition the
                        // only two streams we are trying to merge, so re-spill the larger
                        // of them with a smaller batch size and retry, the smaller max
                        // batch lowers the per-stream reservation enough to seat both.
                        let split_index = usize::from(
                            self.sorted_spill_files[1].0.max_record_batch_memory
                                > self.sorted_spill_files[0].0.max_record_batch_memory,
                        );
                        return Ok(SpillFilesToMerge::SplitThenRetry(split_index));
                    }

                    // A shallower read queue can avoid an intermediate write.
                    // Try only a complete, measured recursive spill merge: an
                    // unsuccessful probe leaves this selection and grant intact.
                    if buffer_len == 2
                        && measured_progress
                        && !allow_minimum_without_headroom
                        && self.sorted_spill_files.len() <= max_spill_files
                        && let Some((read, output)) =
                            self.try_admit_complete_recursive_merge(reservation)
                    {
                        number_of_spills_to_read_for_current_phase =
                            self.sorted_spill_files.len();
                        accepted_memory = read;
                        output_headroom = output;
                        selected_buffer_len = 1;
                    }

                    // We reached the maximum amount of memory we can use for
                    // this merge, or admitted all remaining inputs at depth one.
                    break;
                }
            }
        }

        if number_of_spills_to_read_for_current_phase > 0 {
            // `total_needed` may include a rejected candidate. Keep only the input
            // buffers that were admitted plus output-construction headroom. When no
            // disk stream was admitted, keep the caller's pre-reserved bytes so they
            // can be transferred to the in-memory merge's BatchBuilder.
            let Some(retained) = accepted_memory.checked_add(output_headroom) else {
                return resources_err!("Spill merge headroom exceeds usize::MAX");
            };
            if reservation.size() > retained {
                reservation.shrink(reservation.size() - retained);
            }
        }

        let spills = self
            .sorted_spill_files
            .drain(..number_of_spills_to_read_for_current_phase)
            .collect::<Vec<_>>();

        Ok(SpillFilesToMerge::Ready(
            spills,
            selected_buffer_len,
            output_headroom,
        ))
    }

    /// Probe all measured pure-spill inputs at read depth one. The caller
    /// establishes the recursive-layout and fan-in limits before using this.
    /// Arithmetic or pool rejection is only a failed optional probe; neither
    /// the original reservation nor the file selection changes on failure.
    fn try_admit_complete_recursive_merge(
        &self,
        reservation: &mut MemoryReservation,
    ) -> Option<(usize, usize)> {
        let mut read = 0usize;
        let mut workspace = 0usize;
        let mut row_content = 0usize;
        let mut output = 0usize;
        for (file, rows) in &self.sorted_spill_files {
            read = read.checked_add(file.max_record_batch_memory.checked_mul(2)?)?;
            let stats = file.progress?;
            workspace = workspace.checked_add(stats.workspace)?;
            row_content = row_content.max(stats.row_content);
            let (minimum, _) = spill_output_batch_targets(
                &self.schema,
                *rows,
                file.max_record_batch_memory,
            )?;
            output = output.max(minimum);
        }
        output = output.max(workspace.checked_mul(2)?.checked_add(row_content)?);
        let headroom = if self.reserve_replay_headroom {
            read.max(output)
        } else {
            output
        };
        let required = read.checked_add(headroom)?;
        // Grow only a deficit in the existing grant, not a second reservation.
        try_grow_reservation_to_at_least(reservation, required).ok()?;
        Some((read, output))
    }

    /// Re-spill the spill file at `index` with half its batch size, putting it back
    /// at the same position. We read the file back and re-spill it through the normal
    /// spill API (which owns batch layout), slicing every batch in two, which halves
    /// the largest written batch and so lowers the per-stream merge reservation enough
    /// for the next attempt to seat both streams. One stream's worth of memory is
    /// reserved for the duration and freed afterwards. Makes the merge resilient to skew.
    ///
    /// Instead of halving the *global* merge batch size (which would compound when more
    /// than one run is re-spilled), the shrunk run records its own smaller batch-size
    /// limit (tracked alongside the run in `sorted_spill_files`), so only merges that
    /// actually consume it pay the reduced batch size.
    ///
    /// Returns whether the largest batch shrank. If `retry_unsplittable` is true,
    /// restore an unchanged run for one minimum-merge admission attempt.
    async fn split_spill_file_in_half(
        &mut self,
        index: usize,
        retry_unsplittable: bool,
    ) -> Result<bool> {
        log::debug!(
            "2 spilled streams could not be loaded into memory for merge \
        (requires 2x of the largest batch from both), re-spilling the larger of the two with half \
        the batch size to reduce memory needs for the next merge attempt. the shrunk run carries \
        a halved batch-size limit so only merges consuming it use the smaller batch size"
        );

        // Extract the target in O(1) instead of `remove(index)`, which would shift
        // every following spill file. Swap it to the back and pop it; the matching
        // swap after re-spilling restores the original order, so the vec ends up
        // exactly as it started, just with the target file shrunk.
        // `old_batch_size` is the batch size this run was written with (the full merge
        // batch size unless it was already shrunk once). Halving it caps the next merge
        // that reads this run so the merged output can't rebuild a full-size batch.
        let last = self.sorted_spill_files.len() - 1;
        self.sorted_spill_files.swap(index, last);
        let (mut target, old_batch_size) = self
            .sorted_spill_files
            .pop()
            .expect("index is in bounds, so the vec is non-empty");
        let old_max = target.max_record_batch_memory;
        let mut max_batch_rows = old_batch_size;

        // Reserve enough to hold a single stream of this file while we re-spill it.
        let reservation = self.reservation.new_empty();
        reservation
            .try_grow(get_reserved_bytes_for_record_batch_size(old_max, old_max))?;

        if self.reserve_replay_headroom {
            // A maximum-sized singleton cannot shrink. Find it without writing
            // another file: the original runs may already fill the disk quota.
            // Use an unbuffered reader so no background read outlives this guard.
            let recursive =
                crate::sorts::schema_needs_recursive_read_bound(self.schema.as_ref());
            let mut source = self
                .spill_manager
                .read_spill_with_memory_bound(Arc::clone(&target.file), old_max)?;
            let mut all_singletons = true;
            let mut max_is_singleton = false;
            let mut decoded_max = 0;
            max_batch_rows = 0;
            while let Some(batch) = tokio::task::coop::cooperative(source.next()).await {
                let batch = batch?;
                max_batch_rows = max_batch_rows.max(batch.num_rows());
                all_singletons &= batch.num_rows() == 1;
                let decoded_output_size = if recursive {
                    source
                        .last_decoded_memory_bound()
                        .expect("the rescan enables encoded bounds")
                } else {
                    let sliced_size = batch.get_sliced_size()?;
                    interleave_memory_size_from_sliced(&batch, sliced_size)
                        .unwrap_or(sliced_size)
                };
                decoded_max = decoded_max.max(decoded_output_size);
                if batch.num_rows() == 1 {
                    let gc_output_size = if recursive {
                        decoded_output_size
                    } else {
                        let gc_batch = gc_view_arrays(&batch)?;
                        let gc_sliced_size = gc_batch.get_sliced_size()?;
                        interleave_memory_size_from_sliced(&gc_batch, gc_sliced_size)
                            .unwrap_or(gc_sliced_size)
                    };
                    if gc_output_size >= old_max {
                        max_is_singleton = true;
                        break;
                    }
                }
            }
            // IPC can discard spare view-buffer capacity included in `old_max`.
            // A complete scan can correct that estimate without rewriting the
            // file. Recursive layouts use the portable encoded-message read
            // bound; legacy flat layouts retain their pre-GC size calculation.
            let shrank = !max_is_singleton && decoded_max < old_max;
            if shrank || all_singletons || max_is_singleton {
                if !shrank && !retry_unsplittable {
                    return resources_err!(
                        "Cannot merge sorted runs: a single record batch of {old_max} bytes \
                         exceeds the available merge memory and cannot be split further"
                    );
                }
                if shrank {
                    target.max_record_batch_memory = decoded_max;
                }
                let batch_size_limit = if all_singletons || max_is_singleton {
                    1
                } else {
                    old_batch_size.min(max_batch_rows).max(1)
                };
                self.sorted_spill_files.push((target, batch_size_limit));
                self.sorted_spill_files.swap(index, last);
                return Ok(shrank);
            }
        }

        let source =
            if crate::sorts::schema_needs_recursive_read_bound(self.schema.as_ref()) {
                // Keep the recursive reader within the split guard's two-batch grant:
                // a queued read-ahead batch must not survive alongside the current
                // decoded batch and its replacement during a rewrite.
                self.spill_manager
                    .read_spill_as_stream_unbuffered(target.file, Some(old_max))?
            } else {
                self.spill_manager
                    .read_spill_as_stream(target.file, Some(old_max))?
            };
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
            .spill_record_batch_stream_with_memory(
                &mut halved,
                "MultiLevelMergeBuilder split skewed spill",
            )
            .await?;

        reservation.free();

        let Some((file, memory, new_max_batch_rows)) = result else {
            return internal_err!("re-spilling a skewed spill file produced no data");
        };

        // If halving cannot reduce the largest batch, only a requested retry
        // against the actual pool can make progress. The caller permits that
        // retry once before surfacing this error.
        let new_max = memory.input;
        let shrank = new_max < old_max;
        if !shrank && !retry_unsplittable {
            return resources_err!(
                "Cannot merge sorted runs: a single record batch of {old_max} bytes \
                 exceeds the available merge memory and cannot be split further"
            );
        }

        // Record the halved batch size as a *per-run* limit rather than lowering the
        // global batch size. Merges that don't touch this run keep the full batch
        // size. a merge that reads it caps its output at this limit so the merged run
        // can't rebuild a full-size batch and reintroduce the skew. Actual batches
        // can be shorter than the configured limit; use their size so the merge
        // cannot accumulate extra split batches outside its reservation.
        let new_batch_size_limit = if shrank {
            (old_batch_size / 2).min(max_batch_rows.div_ceil(2)).max(1)
        } else {
            // Skipping headroom only covers indivisible input rows, not a
            // larger output batch formed by concatenating those rows.
            1
        };
        self.size_intermediate_merges &= new_max_batch_rows >= new_batch_size_limit;

        // Push the re-spilled (smaller) file and swap it back into `index`, undoing
        // the swap-to-back above so the order is preserved.
        self.sorted_spill_files.push((
            SpillFileWithMemory {
                file,
                max_record_batch_memory: new_max,
                progress: memory.progress,
            },
            new_batch_size_limit,
        ));
        let last = self.sorted_spill_files.len() - 1;
        self.sorted_spill_files.swap(index, last);

        Ok(shrank)
    }

    fn observe_output(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        Box::pin(ObservedStream::new(stream, self.metrics.clone(), None))
    }
}

/// Outcome of trying to reserve memory for one multi-level merge pass.
enum SpillFilesToMerge {
    /// Enough memory: the spill files to read this pass (each paired with its
    /// batch-size limit), the read-ahead buffer size, and the reserved output
    /// construction headroom.
    Ready(Vec<(SpillFileWithMemory, usize)>, usize, usize),
    /// Could not seat the minimum of 2 streams. Re-spill the spill file at this index
    /// with a smaller (halved) batch size, then retry the pass.
    SplitThenRetry(usize),
}

/// What one iteration of the multi-level merge loop should do next.
enum MergeStep {
    /// A merged stream is ready to be consumed (and possibly spilled back).
    Stream {
        stream: SendableRecordBatchStream,
        /// The batch-size limit to stamp on the run if this stream is re-spilled as an
        /// intermediate result: the batch size its merge ran at. It equals the full
        /// merge batch size unless the merge consumed a skew-resolved run, in which
        /// case it is that run's smaller limit so the re-spilled result stays capped
        /// and can't rebuild an oversized batch.
        batch_size_limit: usize,
    },
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

fn effective_spill_merge_fan_in(configured_fan_in: usize) -> usize {
    if configured_fan_in == 0 {
        usize::MAX
    } else {
        configured_fan_in.max(2)
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

#[cfg(test)]
#[path = "multi_level_merge/progress_tests.rs"]
mod progress_tests;

#[cfg(test)]
#[path = "multi_level_merge/intermediate_sizing_tests.rs"]
mod intermediate_sizing_tests;

#[cfg(test)]
mod tests {
    use super::*;

    use crate::expressions::PhysicalSortExpr;
    use crate::spill::get_record_batch_memory_size;
    use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Int64Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };
    use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion_physical_expr::expressions::{Column, col};
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };

    pub(super) fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
    }

    fn binary_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Binary, false)]))
    }

    pub(super) fn build_spill_manager(
        env: &Arc<RuntimeEnv>,
        schema: &SchemaRef,
    ) -> SpillManager {
        SpillManager::new(
            Arc::clone(env),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(schema),
        )
    }

    /// Spill `values` (which must already be sorted) as a single sorted run and
    /// return it as a `SortedSpillFile` carrying its recorded largest-batch memory.
    pub(super) fn make_sorted_spill_file(
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

    fn make_binary_value(value: usize, width: usize) -> Vec<u8> {
        let prefix = format!("{value:04}-");
        let mut bytes = prefix.into_bytes();
        bytes.resize(width, b'x');
        bytes
    }

    fn make_sorted_binary_spill_file(
        spill_manager: &SpillManager,
        schema: &SchemaRef,
        values: Vec<Vec<u8>>,
    ) -> SortedSpillFile {
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(BinaryArray::from_iter_values(values))],
        )
        .unwrap();
        let batches: Vec<Result<RecordBatch>> = vec![Ok(batch)];
        let (file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches.into_iter(),
                "test binary input run",
            )
            .unwrap()
            .expect("spill should produce a file");
        SortedSpillFile {
            file,
            max_record_batch_memory,
        }
    }

    pub(super) fn build_merge_builder(
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

    #[rstest::rstest]
    #[case::full_batches(8, 128, 6)]
    #[case::replay_headroom_limits_sizing(9, 128, 7)]
    #[case::unchanged_selection(13, 128, 7)]
    #[case::short_batches(9, 64, 7)]
    #[tokio::test]
    async fn intermediate_merge_sizing_preserves_final_output(
        #[values(false, true)] size_intermediate_merges: bool,
        #[case] runs: usize,
        #[case] rows: usize,
        #[case] sized_intermediate_inputs: usize,
    ) -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spilled_rows = spill_manager.metrics.spilled_rows.clone();
        let files: Vec<_> = (0..runs)
            .map(|run| {
                make_sorted_spill_file(
                    &spill_manager,
                    &schema,
                    (0..rows).map(|row| (row * runs + run) as i64).collect(),
                )
            })
            .collect();
        let batch_memory = files[0].max_record_batch_memory;
        // Ordinary admission seats seven inputs with read-ahead and equal replay
        // headroom. The retained buffer grant can admit three final inputs plus
        // their replay headroom, so eight runs need six merged inputs. Nine or
        // thirteen runs require all seven and exercise the unchanged selection.
        // Short input batches can grow when merged, so they must keep the original
        // selection to avoid adding a pass with the larger output batch budget.
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(8 * 7 * batch_memory));
        let mut builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, 128)
                .with_replay_headroom(true);
        if size_intermediate_merges {
            builder = builder.with_intermediate_merge_sizing(Some(rows));
        }
        let stream = builder.create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let merged = concat_batches(&schema, &batches)?;
        let expected = Int64Array::from_iter_values(0..(runs * rows) as i64);
        assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
        let intermediate_inputs = if size_intermediate_merges {
            sized_intermediate_inputs
        } else {
            7
        };
        assert_eq!(spilled_rows.value(), (runs + intermediate_inputs) * rows);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        assert_eq!(env.disk_manager.used_disk_space(), 0);
        Ok(())
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn intermediate_merge_sizing_preserves_heterogeneous_runs(
        #[values(false, true)] size_intermediate_merges: bool,
    ) -> Result<()> {
        use arrow::array::StringArray;
        use arrow::buffer::{Buffer, OffsetBuffer};

        let env = Arc::new(RuntimeEnv::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let spill_manager = build_spill_manager(&env, &schema);
        let spilled_rows = spill_manager.metrics.spilled_rows.clone();
        let rows = 128;
        let mut files = Vec::new();
        for run in 0..8 {
            let width = if run == 6 { 256 } else { 16 };
            let payload = StringArray::new(
                OffsetBuffer::new(
                    (0..=rows)
                        .map(|row| (row * width) as i32)
                        .collect::<Vec<_>>()
                        .into(),
                ),
                Buffer::from(vec![b'x'; rows * width]),
                None,
            );
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (run * rows..(run + 1) * rows).map(|value| value as i64),
                    )),
                    Arc::new(payload),
                ],
            )?;
            let (file, max_record_batch_memory) = spill_manager
                .spill_record_batch_iter_and_return_max_batch_memory(
                    std::iter::once(Ok(batch)),
                    "heterogeneous input run",
                )?
                .unwrap();
            files.push(SortedSpillFile {
                file,
                max_record_batch_memory,
            });
        }
        let small = files[0].max_record_batch_memory;
        let wide = files[6].max_record_batch_memory;
        // Admit six small inputs first, then the wide input and one small input.
        // Trimming the first merge to three wrongly assumes its fan-in will also
        // fit the wider suffix, adding a later rewrite of the small runs.
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(8 * (wide + small + small / 2)));
        let builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, rows)
                .with_replay_headroom(true)
                .with_intermediate_merge_sizing(size_intermediate_merges.then_some(rows));
        let batches: Vec<RecordBatch> = builder
            .create_spillable_merge_stream()
            .try_collect()
            .await?;
        let merged = concat_batches(&schema, &batches)?;
        let expected = Int64Array::from_iter_values(0..(8 * rows) as i64);
        assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
        let small_payload = "x".repeat(16);
        let wide_payload = "x".repeat(256);
        let expected_payload = StringArray::from_iter_values((0..8).flat_map(|run| {
            std::iter::repeat_n(
                if run == 6 {
                    &wide_payload
                } else {
                    &small_payload
                },
                rows,
            )
        }));
        assert_eq!(merged.column(1).as_string::<i32>(), &expected_payload);
        assert_eq!(spilled_rows.value(), 16 * rows);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        assert_eq!(env.disk_manager.used_disk_space(), 0);
        Ok(())
    }

    /// Two sorted runs whose largest batches are too big to both
    /// be seated in the merge budget at once are re-spilled (halved) until they
    /// fit, and the merge then completes with fully sorted, complete output.
    #[tokio::test]
    async fn skewed_runs_reuse_retained_workspace_across_retries() -> Result<()> {
        // These budgets require one and two re-spills, respectively, including
        // the construction headroom for the merge output.
        for (budget_halves, expected_splits) in [(9, 1), (7, 2)] {
            let capacity = 1024 * 1024;
            let parent: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(capacity));
            let env = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&parent))
                .build_arc()?;
            let schema = test_schema();
            let spill_manager = build_spill_manager(&env, &schema);
            let spill_count = spill_manager.metrics.spill_file_count.clone();
            let n: i64 = 16384;
            let f0 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
            let f1 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
            let m = f0.max_record_batch_memory.max(f1.max_record_batch_memory);
            let workspace = m * budget_halves / 2;
            assert!(workspace < capacity);
            let merge_pool = Arc::new(MergeMemoryPool::new(
                Arc::clone(&parent),
                MemoryConsumer::new("merge workspace"),
            ));
            let pool: Arc<dyn MemoryPool> =
                Arc::clone(&merge_pool) as Arc<dyn MemoryPool>;
            let batch_size = 8192;
            let mut builder = build_merge_builder(
                spill_manager,
                Arc::clone(&schema),
                vec![f0, f1],
                &pool,
                batch_size,
            )
            .with_merge_pool(Some(Arc::clone(&merge_pool)));
            builder.reservation.try_grow(workspace)?;
            merge_pool.retain(workspace);
            let contender = MemoryConsumer::new("contender").register(&parent);
            contender.try_grow(capacity - workspace)?;

            // Drive each retry so its freed child reservation cannot silently
            // release the workspace and reacquire it from the parent pool.
            for _ in 0..expected_splits {
                let MergeStep::SplitThenRetry(index) =
                    builder.merge_sorted_runs_within_mem_limit(false)?
                else {
                    panic!("the merge must re-spill a skewed run");
                };
                assert_eq!(parent.reserved(), capacity);
                assert!(contender.try_grow(1).is_err());
                builder.split_spill_file_in_half(index, false).await?;
                assert_eq!(parent.reserved(), capacity);
                assert!(contender.try_grow(1).is_err());
            }

            let final_bytes: usize = builder
                .sorted_spill_files
                .iter()
                .map(|(file, _)| {
                    get_reserved_bytes_for_record_batch_size(
                        file.max_record_batch_memory,
                        file.max_record_batch_memory,
                    )
                })
                .sum();
            let output_headroom = builder
                .sorted_spill_files
                .iter()
                .map(|(file, _)| file.max_record_batch_memory)
                .max()
                .unwrap();
            assert!(final_bytes < workspace && workspace < 2 * final_bytes);

            let mut stream = builder.create_spillable_merge_stream();
            let first = stream.try_next().await?.expect("nonempty merge");
            assert_eq!(spill_count.value(), 2 + expected_splits);
            assert_eq!(merge_pool.reserved(), final_bytes + output_headroom);
            assert_eq!(
                parent.reserved(),
                contender.size() + final_bytes + output_headroom
            );

            let mut batches = vec![first];
            while let Some(batch) = stream.try_next().await? {
                batches.push(batch);
            }
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).max(),
                Some(batch_size / 2),
                "splitting both runs must halve, not quarter, the output batch size"
            );
            let merged = concat_batches(&schema, &batches)?;
            let expected =
                Int64Array::from_iter_values((0..n).flat_map(|value| [value, value]));
            assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);

            // The pool stays alive: EOF must release live bytes and the idle floor.
            assert_eq!(merge_pool.reserved(), 0);
            assert_eq!(parent.reserved(), contender.size());
            drop(stream);
            assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
            assert_eq!(env.disk_manager.used_disk_space(), 0);
            drop(contender);
            assert_eq!(parent.reserved(), 0);
        }
        Ok(())
    }

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

    #[rstest::rstest]
    #[tokio::test]
    async fn short_list_row_keeps_construction_space_with_minimal_fan_in(
        #[values(2, 3)] partitions: usize,
        #[values(false, true)] fits_row: bool,
        #[values(false, true)] replay: bool,
    ) -> Result<()> {
        use arrow::array::{ListArray, StringArray};
        use arrow::buffer::OffsetBuffer;
        let child_rows = 50_000;
        let inputs = (0..partitions)
            .map(|partition| {
                let keys: ArrayRef = Arc::new(Int64Array::from(vec![partition as i64]));
                let values: ArrayRef = Arc::new(StringArray::from_iter_values(
                    std::iter::repeat_n("x", child_rows),
                ));
                let list: ArrayRef = Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Utf8, false)),
                    OffsetBuffer::new(vec![0, child_rows as i32].into()),
                    values,
                    None,
                ));
                RecordBatch::try_from_iter(vec![("x", keys), ("list", list)]).unwrap()
            })
            .collect::<Vec<_>>();
        let schema = inputs[0].schema();
        let manager = build_spill_manager(&Arc::new(RuntimeEnv::default()), &schema);
        let mut spills = Vec::new();
        let mut decoded = Vec::new();
        for input in &inputs {
            let (file, memory) = manager
                .spill_record_batch_iter_with_memory(
                    std::iter::once(Ok(input.clone())),
                    "short List input",
                )?
                .unwrap();
            decoded.push(
                manager
                    .read_spill_as_stream(Arc::clone(&file), Some(memory.input))?
                    .try_collect::<Vec<_>>()
                    .await?
                    .remove(0),
            );
            spills.push(SpillFileWithMemory {
                file,
                max_record_batch_memory: memory.input,
                progress: memory.progress,
            });
        }
        let read = spills[0].max_record_batch_memory;
        let mut size = crate::sorts::output_memory::InterleaveSize::new(
            (0..schema.fields().len()).map(|column| {
                decoded
                    .iter()
                    .take(2)
                    .map(|batch| batch.column(column).as_ref())
                    .collect()
            }),
        )
        .unwrap();
        size.push(0, 0).unwrap();
        let construction = size.new_bytes().unwrap();
        let workspace = decoded
            .iter()
            .take(2)
            .enumerate()
            .map(|(i, batch)| {
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        crate::sorts::output_memory::workspace_bytes(
                            array.as_ref(),
                            i == 0,
                        )
                        .unwrap()
                    })
                    .sum::<usize>()
            })
            .sum::<usize>();
        assert!(construction > read);
        let limit = if fits_row {
            7 * read
        } else {
            4 * read + construction / 2
        };
        if fits_row {
            assert!(4 * read + construction.max(workspace) <= limit);
            if partitions == 3 {
                assert!(6 * read + construction.max(workspace) > limit);
            }
        } else {
            assert!(4 * read + construction.max(workspace) > limit);
        }
        eprintln!(
            "short List: inputs={partitions}, M_read={read}, W={workspace}, E={construction}, initial_headroom={read}, pool={limit}"
        );
        drop(size);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        let result: Result<Vec<RecordBatch>> = MultiLevelMergeBuilder::new_with_memory(
            manager,
            Arc::clone(&schema),
            spills,
            vec![],
            LexOrdering::new(vec![PhysicalSortExpr::new_default(col("x", &schema)?)])
                .unwrap(),
            BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            1024,
            MemoryConsumer::new("merge").register(&pool),
            None,
            true,
        )
        .with_replay_headroom(replay)
        .create_spillable_merge_stream()
        .try_collect()
        .await;
        if fits_row {
            assert_eq!(
                concat_batches(&schema, &result?)?,
                concat_batches(&schema, &inputs)?
            );
        } else {
            assert!(matches!(
                result,
                Err(datafusion_common::DataFusionError::ResourcesExhausted(_))
            ));
        }
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn three_view_spills_complete_with_verified_read_bounds_in_five_mib()
    -> Result<()> {
        use arrow::array::StringViewArray;
        let rows = 8192;
        let inputs = (0..3)
            .map(|partition| {
                let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| row * 3 + partition),
                ));
                let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
                    (0..rows).map(|row| format!("{row:08}{}", "x".repeat(55))),
                ));
                RecordBatch::try_from_iter(vec![("x", keys), ("value", values)]).unwrap()
            })
            .collect::<Vec<_>>();
        let schema = inputs[0].schema();
        let env = Arc::new(RuntimeEnv::default());
        let manager = build_spill_manager(&env, &schema);
        let mut spills = Vec::new();
        for input in &inputs {
            let (file, recorded) = manager
                .spill_record_batch_iter_and_return_max_batch_memory(
                    std::iter::once(Ok(input.clone())),
                    "three View inputs",
                )?
                .unwrap();
            let read: Vec<RecordBatch> = manager
                .read_spill_as_stream(Arc::clone(&file), Some(recorded))?
                .try_collect()
                .await?;
            let actual = read.iter().map(get_record_batch_memory_size).max().unwrap();
            assert!(
                actual <= recorded,
                "initial recorded {recorded}, decoded {actual}"
            );
            eprintln!("initial View M={recorded}, actual={actual}");
            spills.push(SortedSpillFile {
                file,
                max_record_batch_memory: recorded,
            });
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(5 * 1024 * 1024));
        let mut builder = build_merge_builder(
            manager.clone(),
            Arc::clone(&schema),
            spills,
            &pool,
            1024,
        );
        let MergeStep::Stream {
            mut stream,
            batch_size_limit,
        } = builder.merge_sorted_runs_within_mem_limit(false)?
        else {
            panic!("two initial streams must fit");
        };
        assert_eq!(builder.sorted_spill_files.len(), 1);
        let first_grant = pool.reserved();
        let (file, recorded, _) = manager
            .spill_record_batch_stream_and_return_max_batch_stats(
                &mut stream,
                "verified intermediate View run",
            )
            .await?
            .unwrap();
        drop(stream);
        let read: Vec<RecordBatch> = manager
            .read_spill_as_stream(Arc::clone(&file), Some(recorded))?
            .try_collect()
            .await?;
        let actual = read.iter().map(get_record_batch_memory_size).max().unwrap();
        assert!(
            actual <= recorded,
            "intermediate recorded {recorded}, decoded {actual}"
        );
        assert!(
            recorded < 512 * 1024,
            "do not reuse pre-GC input capacities"
        );
        eprintln!(
            "intermediate View M={recorded}, actual={actual}, first grant={first_grant}"
        );
        drop(read);
        builder.sorted_spill_files.push((
            SortedSpillFile {
                file,
                max_record_batch_memory: recorded,
            }
            .into(),
            batch_size_limit,
        ));
        let outputs: Vec<RecordBatch> = builder
            .create_spillable_merge_stream()
            .try_collect()
            .await?;
        assert!(outputs.len() < 64, "do not regress to one-row output");
        let output = concat_batches(&schema, &outputs)?;
        let ordering =
            LexOrdering::new([PhysicalSortExpr::new_default(col("x", &schema)?)])
                .unwrap();
        let expected = crate::sorts::sort::sort_batch(
            &concat_batches(&schema, &inputs)?,
            &ordering,
            None,
        )?;
        assert_eq!(output, expected);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn mixed_view_inputs_keep_the_untargeted_fallback(
        #[values(2, 3)] partitions: usize,
        #[values("view", "list", "struct", "large_list_struct")] shape: &str,
    ) -> Result<()> {
        use arrow::array::{LargeListArray, ListArray, StringViewArray, StructArray};
        use arrow::buffer::{Buffer, OffsetBuffer};
        let rows = 128;
        let batches = (0..partitions)
            .map(|partition| {
                let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|row| (row * partitions + partition) as i64),
                ));
                let view = arrow_data::ByteView::new(20, b"xxxx").as_u128();
                let values: ArrayRef = Arc::new(
                    StringViewArray::try_new(
                        vec![view; rows].into(),
                        vec![Buffer::from(vec![b'x'; 16 * 1024])],
                        None,
                    )
                    .unwrap(),
                );
                let values = if shape == "list" {
                    Arc::new(ListArray::new(
                        Arc::new(Field::new_list_field(DataType::Utf8View, false)),
                        OffsetBuffer::new((0..=rows as i32).collect::<Vec<_>>().into()),
                        values,
                        None,
                    )) as ArrayRef
                } else if shape == "struct" || shape == "large_list_struct" {
                    let fields =
                        vec![Arc::new(Field::new("child", DataType::Utf8View, false))]
                            .into();
                    let values: ArrayRef =
                        Arc::new(StructArray::new(fields, vec![values], None));
                    if shape == "large_list_struct" {
                        Arc::new(LargeListArray::new(
                            Arc::new(Field::new_list_field(
                                values.data_type().clone(),
                                false,
                            )),
                            OffsetBuffer::new(
                                (0..=rows as i64).collect::<Vec<_>>().into(),
                            ),
                            values,
                            None,
                        )) as ArrayRef
                    } else {
                        values
                    }
                } else {
                    values
                };
                RecordBatch::try_from_iter(vec![("x", keys), ("view", values)]).unwrap()
            })
            .collect::<Vec<_>>();
        let schema = batches[0].schema();
        let env = Arc::new(RuntimeEnv::default());
        let spill_manager = build_spill_manager(&env, &schema);
        let mut spills = Vec::new();
        let mut maximum = 0;
        for batch in &batches[..partitions - 1] {
            let file = spill_manager
                .spill_record_batch_and_finish(
                    std::slice::from_ref(batch),
                    "mixed View input",
                )?
                .unwrap();
            let decoded: Vec<RecordBatch> = spill_manager
                .read_spill_as_stream(Arc::clone(&file), None)?
                .try_collect()
                .await?;
            let size = decoded
                .iter()
                .map(get_record_batch_memory_size)
                .max()
                .unwrap()
                .next_power_of_two();
            maximum = maximum.max(size);
            spills.push(SortedSpillFile {
                file,
                max_record_batch_memory: size,
            });
        }
        let memory_batch = batches.last().unwrap().clone();
        let memory_bytes = get_record_batch_memory_size(&memory_batch);
        // Cover the read grant and any intermediate rewritten run. Tight-pool
        // retry behavior is checked separately with fixed decoded inputs.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
            memory_bytes + (4 * partitions + 1) * maximum,
        ));
        let input = MemoryConsumer::new("memory input").register(&pool);
        input.try_grow(memory_bytes)?;
        let mut builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), spills, &pool, rows);
        builder
            .sorted_streams
            .push(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter([Ok(memory_batch)]),
            )));
        assert!(!builder.supports_byte_target());
        let mut stream = builder.create_spillable_merge_stream();
        let outputs: Vec<RecordBatch> = stream.by_ref().try_collect().await?;
        assert!(outputs.len() <= partitions + 1);
        let output = concat_batches(&schema, &outputs)?;
        assert_eq!(output.schema(), schema);
        assert_eq!(output.num_rows(), rows * partitions);
        for row in 0..output.num_rows() {
            assert_eq!(
                output.column(0).as_primitive::<Int64Type>().value(row),
                row as i64
            );
            let value = if shape == "list" {
                output
                    .column(1)
                    .as_list::<i32>()
                    .values()
                    .as_string_view()
                    .value(row)
            } else if shape == "struct" {
                output
                    .column(1)
                    .as_struct()
                    .column(0)
                    .as_string_view()
                    .value(row)
            } else if shape == "large_list_struct" {
                output
                    .column(1)
                    .as_list::<i64>()
                    .values()
                    .as_struct()
                    .column(0)
                    .as_string_view()
                    .value(row)
            } else {
                output.column(1).as_string_view().value(row)
            };
            assert_eq!(value, "xxxxxxxxxxxxxxxxxxxx");
        }
        drop(stream);
        assert_eq!(pool.reserved(), memory_bytes);
        input.free();
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A run can fit the pool during splitting but leave no room for replay.
    /// Splitting must shrink the merge buffers to half the available memory.
    #[rstest::rstest]
    #[tokio::test]
    async fn replay_headroom_splits_an_oversized_first_run(
        #[values(128, 129, 4096)] rows: i64,
    ) -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let first = make_sorted_spill_file(&spill_manager, &schema, (0..rows).collect());
        let second =
            make_sorted_spill_file(&spill_manager, &schema, (rows..2 * rows).collect());
        let merge_limit = first.max_record_batch_memory;
        // One original run fills the pool. Splitting may use that space before
        // replay starts, but the final merge must leave half the pool for replay.
        let pool_size = 2 * merge_limit;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
        let builder = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![first, second],
            &pool,
            8192,
        )
        .with_replay_headroom(true);
        let mut stream = builder.create_spillable_merge_stream();
        let mut batches = vec![];
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            // Short input runs must not rebuild a larger output batch than
            // the attached merge reservation can hold.
            assert!(get_record_batch_memory_size(&batch) <= pool.reserved());
            batches.push(batch);
            assert!(pool.reserved() <= merge_limit);
        }
        let merged = concat_batches(&schema, &batches)?;
        let values = merged.column(0).as_primitive::<Int64Type>();
        assert_eq!(values.len(), (2 * rows) as usize);
        for (expected, value) in values.values().iter().enumerate() {
            assert_eq!(*value, expected as i64);
        }
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn replay_headroom_allows_only_an_indivisible_minimum() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = (0..3)
            .map(|value| make_sorted_spill_file(&spill_manager, &schema, vec![value]))
            .collect::<Vec<_>>();
        let batch_memory = spills[0].max_record_batch_memory;
        // Leave enough room for the one-row output-construction reservation in
        // addition to the minimum spill read buffers.
        let pool_size = 6 * batch_memory + size_of::<i64>();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
        let mut builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), spills, &pool, 8192)
                .with_replay_headroom(true);

        assert!(!builder.split_spill_file_in_half(0, true).await?);
        assert_eq!(builder.sorted_spill_files.len(), 3);
        // Actual batches contain one row even though the nominal size is 8192.
        assert_eq!(builder.sorted_spill_files[0].1, 1);
        let mut reservation = builder.reservation.new_empty();
        let SpillFilesToMerge::Ready(spills, buffer_len, output_headroom) =
            builder.get_sorted_spill_files_to_merge(2, 2, &mut reservation, true)?
        else {
            panic!("minimum merge should fit the pool");
        };
        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(builder.sorted_spill_files.len(), 1);
        assert_eq!(output_headroom, batch_memory);
        assert_eq!(reservation.size(), 5 * batch_memory);
        builder.sorted_spill_files.splice(0..0, spills);
        reservation.free();

        let mut stream = builder.create_spillable_merge_stream();
        let mut values = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            assert_eq!(batch.num_rows(), 1);
            values.push(batch.column(0).as_primitive::<Int64Type>().value(0));
        }
        assert_eq!(values, vec![0, 1, 2]);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn replay_headroom_keeps_aligned_singleton_memory() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Boolean, false)]));
        let spill_manager = build_spill_manager(&env, &schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )?;
        let (file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                std::iter::once(Ok::<_, datafusion_common::DataFusionError>(batch)),
                "aligned singleton",
            )?
            .unwrap();
        assert_eq!(max_record_batch_memory, 64);
        let spill = SortedSpillFile {
            file,
            max_record_batch_memory,
        };
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(4 * max_record_batch_memory));
        let mut builder =
            build_merge_builder(spill_manager, schema, vec![spill], &pool, 8192)
                .with_replay_headroom(true);

        assert!(!builder.split_spill_file_in_half(0, true).await?);
        assert_eq!(
            builder.sorted_spill_files[0].0.max_record_batch_memory,
            max_record_batch_memory
        );
        assert_eq!(builder.sorted_spill_files[0].1, 1);
        drop(builder);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn replay_headroom_is_released_after_rejected_candidate() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = (0..3)
            .map(|value| make_sorted_spill_file(&spill_manager, &schema, vec![value]))
            .collect::<Vec<_>>();
        let batch_memory = spills[0].max_record_batch_memory;
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(10 * batch_memory));
        let mut builder = build_merge_builder(spill_manager, schema, spills, &pool, 1)
            .with_replay_headroom(true);
        let mut reservation = builder.reservation.new_empty();
        let SpillFilesToMerge::Ready(spills, buffer_len, output_headroom) =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut reservation, false)?
        else {
            panic!("two streams and replay headroom should fit the pool");
        };
        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(builder.sorted_spill_files.len(), 1);
        assert_eq!(output_headroom, batch_memory);
        // The third candidate failed admission. Release its temporary replay
        // space, retaining two inputs and one output-construction batch.
        assert_eq!(reservation.size(), 5 * batch_memory);
        let replay = builder.reservation.new_empty();
        replay.try_grow(5 * batch_memory)?;
        assert_eq!(pool.reserved(), 10 * batch_memory);
        drop((replay, reservation, spills, builder));
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    /// Small real spill data with conservative metadata matching the admission
    /// boundary from the existing eight-run List fixture.
    fn recursive_admission_builder(
        runs: usize,
        capacity: usize,
        recursive: bool,
        known: bool,
    ) -> Result<(MultiLevelMergeBuilder, Arc<dyn MemoryPool>)> {
        use arrow::array::{ListArray, StringArray};
        use arrow::buffer::OffsetBuffer;
        let key: ArrayRef = Arc::new(Int64Array::from(vec![0, 1]));
        let batch = if recursive {
            let child: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(DataType::Utf8, false)),
                OffsetBuffer::new(vec![0, 1, 2].into()),
                child,
                None,
            ));
            RecordBatch::try_from_iter(vec![("x", key), ("list", list)])?
        } else {
            RecordBatch::try_from_iter(vec![("x", key)])?
        };
        let schema = batch.schema();
        let env = Arc::new(RuntimeEnv::default());
        let manager = build_spill_manager(&env, &schema);
        let (file, memory) = manager
            .spill_record_batch_iter_with_memory(
                std::iter::once(Ok(batch)),
                "depth-one admission input",
            )?
            .unwrap();
        assert!(memory.input <= 65536);
        let files = (0..runs)
            .map(|_| SpillFileWithMemory {
                file: Arc::clone(&file),
                max_record_batch_memory: 65536,
                progress: if known { memory.progress } else { None },
            })
            .collect();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(capacity));
        let reservation = MemoryConsumer::new("depth-one admission").register(&pool);
        let order = LexOrdering::new([PhysicalSortExpr::new_default(col("x", &schema)?)])
            .unwrap();
        let builder = MultiLevelMergeBuilder::new_with_memory(
            manager,
            schema,
            files,
            vec![],
            order,
            BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            64,
            reservation,
            None,
            false,
        );
        Ok((builder, pool))
    }

    #[rstest::rstest]
    #[case::complete_at_depth_one((8, 2*1024*1024, true, true, false, false, 0, false, 8, 1))]
    #[case::depth_two_already_completes((8, 4*1024*1024, true, true, false, false, 0, false, 8, 2))]
    #[case::depth_one_cannot_complete((16, 1024*1024, true, true, false, false, 0, false, 3, 2))]
    #[case::flat_unchanged((8, 2*1024*1024, false, true, false, false, 0, false, 7, 2))]
    #[case::unknown_unchanged((8, 2*1024*1024, true, false, false, false, 0, false, 2, 2))]
    #[case::fan_in_limit((8, 2*1024*1024, true, true, false, false, 4, false, 4, 2))]
    #[case::forced_minimum_unchanged((8, 2*1024*1024, true, true, false, true, 0, false, 2, 2))]
    #[case::replay_preserves_headroom((8, 2*1024*1024, true, true, true, false, 0, false, 8, 1))]
    #[case::mixed_unchanged((8, 2*1024*1024, true, true, false, false, 0, true, 1, 2))]
    fn recursive_final_merge_read_depth(
        #[case] case: (
            usize,
            usize,
            bool,
            bool,
            bool,
            bool,
            usize,
            bool,
            usize,
            usize,
        ),
    ) -> Result<()> {
        let (
            runs,
            capacity,
            recursive,
            known,
            replay,
            minimum_only,
            fan_in,
            mixed,
            expected_runs,
            expected_depth,
        ) = case;
        let (mut builder, pool) =
            recursive_admission_builder(runs, capacity, recursive, known)?;
        builder.reserve_replay_headroom = replay;
        builder
            .spill_manager
            .env()
            .disk_manager
            .set_max_spill_merge_fan_in(fan_in);
        if mixed {
            builder
                .sorted_streams
                .push(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                    &builder.schema,
                ))));
        }
        let files_before = builder
            .sorted_spill_files
            .iter()
            .map(|(f, _)| Arc::clone(&f.file))
            .collect::<Vec<_>>();
        let mut reservation = builder.reservation.new_empty();
        // A successful probe must reuse this grant, then trim it to depth one.
        let before = if recursive
            && known
            && !mixed
            && !replay
            && !minimum_only
            && fan_in == 0
            && runs == 8
            && capacity == 2 * 1024 * 1024
        {
            7 * 4 * 65536 + 65536
        } else {
            0
        };
        reservation.try_grow(before)?;
        let SpillFilesToMerge::Ready(files, depth, output) = builder
            .get_sorted_spill_files_to_merge(2, 2, &mut reservation, minimum_only)?
        else {
            panic!("expected an admitted merge")
        };
        assert_eq!((files.len(), depth), (expected_runs, expected_depth));
        assert_eq!(builder.sorted_spill_files.len(), runs - expected_runs);
        for ((file, _), before) in files
            .iter()
            .chain(&builder.sorted_spill_files)
            .zip(&files_before)
        {
            assert!(Arc::ptr_eq(&file.file, before));
        }
        let read = expected_runs * 2 * expected_depth * 65536;
        assert_eq!(reservation.size(), read + output);
        assert_eq!(pool.reserved(), reservation.size());
        if expected_depth == 1 {
            let workspace = files
                .iter()
                .map(|(f, _)| f.progress.unwrap().workspace)
                .sum::<usize>();
            let row = files
                .iter()
                .map(|(f, _)| f.progress.unwrap().row_content)
                .max()
                .unwrap();
            assert!(output >= row + 2 * workspace);
            assert!(read + if replay { read.max(output) } else { output } <= capacity);
            assert_eq!(reservation.size(), 1114112);
            if before > 0 {
                assert!(reservation.size() < before);
            }
        }
        drop((files, reservation, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn complete_probe_keeps_row_progress_above_minimum_target() -> Result<()> {
        let (mut builder, pool) =
            recursive_admission_builder(8, 2 * 1024 * 1024, true, true)?;
        for (file, _) in &mut builder.sorted_spill_files {
            file.progress.as_mut().unwrap().row_content = 200000;
        }
        let workspace = builder
            .sorted_spill_files
            .iter()
            .map(|(f, _)| f.progress.unwrap().workspace)
            .sum::<usize>();
        let mut reservation = builder.reservation.new_empty();
        let SpillFilesToMerge::Ready(files, depth, output) =
            builder.get_sorted_spill_files_to_merge(2, 2, &mut reservation, false)?
        else {
            panic!("all inputs fit at depth one")
        };
        assert_eq!((files.len(), depth), (8, 1));
        assert_eq!(output, 200000 + 2 * workspace);
        assert!(output > 65536);
        assert_eq!(reservation.size(), 8 * 2 * 65536 + output);
        drop((files, reservation, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn rejected_complete_probe_keeps_original_grant_and_files() -> Result<()> {
        let (mut builder, pool) =
            recursive_admission_builder(16, 1024 * 1024, true, true)?;
        let mut reservation = builder.reservation.new_empty();
        let before = 3 * 4 * 65536 + 65536;
        reservation.try_grow(before)?;
        let files = builder
            .sorted_spill_files
            .iter()
            .map(|(f, _)| Arc::clone(&f.file))
            .collect::<Vec<_>>();
        assert!(
            builder
                .try_admit_complete_recursive_merge(&mut reservation)
                .is_none()
        );
        assert_eq!(reservation.size(), before);
        assert_eq!(pool.reserved(), before);
        // Overflow in optional statistics must be just as non-destructive.
        builder.sorted_spill_files[15]
            .0
            .progress
            .as_mut()
            .unwrap()
            .workspace = usize::MAX;
        assert!(
            builder
                .try_admit_complete_recursive_merge(&mut reservation)
                .is_none()
        );
        assert_eq!(reservation.size(), before);
        assert_eq!(pool.reserved(), before);
        assert_eq!(builder.sorted_spill_files.len(), files.len());
        for ((f, _), before) in builder.sorted_spill_files.iter().zip(&files) {
            assert!(Arc::ptr_eq(&f.file, before));
        }
        drop((files, reservation, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recursive_final_merge_with_real_prefetch_queue() -> Result<()> {
        let (builder, pool) =
            recursive_admission_builder(8, 2 * 1024 * 1024, true, true)?;
        let schema = Arc::clone(&builder.schema);
        let initial = builder
            .spill_manager
            .read_spill_as_stream(
                Arc::clone(&builder.sorted_spill_files[0].0.file),
                None,
            )?
            .try_collect::<Vec<_>>()
            .await?;
        let expected = concat_batches(
            &schema,
            &(0..8).map(|_| initial[0].clone()).collect::<Vec<_>>(),
        )?;
        let expected = crate::sorts::sort::sort_batch(&expected, &builder.expr, None)?;
        let output = builder
            .create_spillable_merge_stream()
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(concat_batches(&schema, &output)?, expected);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn second_spill_can_lower_all_output_headroom_to_minimum() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = vec![
            make_sorted_spill_file(&spill_manager, &schema, vec![1]),
            make_sorted_spill_file(&spill_manager, &schema, vec![2]),
        ];
        let minimum_output = spills[0].max_record_batch_memory;
        let per_spill =
            get_reserved_bytes_for_record_batch_size(minimum_output, minimum_output);
        let preferred_output = 100 * size_of::<i64>();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(per_spill + preferred_output));
        let mut builder = build_merge_builder(spill_manager, schema, spills, &pool, 100);
        let mut reservation = builder.reservation.new_empty();

        let SpillFilesToMerge::Ready(spills, buffer_len, output_headroom) =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut reservation, false)?
        else {
            panic!("both streams fit only after lowering all output headroom");
        };

        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(output_headroom, minimum_output);
        assert_eq!(reservation.size(), 2 * per_spill + minimum_output);
        drop((spills, reservation, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn rejected_first_spill_preserves_memory_merge_reservation() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spill = make_sorted_spill_file(&spill_manager, &schema, vec![1]);
        let reserved = spill.max_record_batch_memory;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(reserved));
        let mut builder =
            build_merge_builder(spill_manager, schema, vec![spill], &pool, 1);
        let mut reservation = builder.reservation.new_empty();
        reservation.try_grow(reserved)?;

        let SpillFilesToMerge::Ready(spills, buffer_len, output_headroom) =
            builder.get_sorted_spill_files_to_merge(2, 0, &mut reservation, false)?
        else {
            panic!("the existing in-memory streams should be merged without a spill");
        };
        assert_eq!(buffer_len, 2);
        assert!(spills.is_empty());
        assert_eq!(output_headroom, 0);
        assert_eq!(reservation.size(), reserved);
        assert_eq!(builder.sorted_spill_files.len(), 1);

        drop((reservation, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn output_headroom_failure_respills_fittable_input() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spill = make_sorted_spill_file(&spill_manager, &schema, vec![1, 2]);
        let input_memory = get_reserved_bytes_for_record_batch_size(
            spill.max_record_batch_memory,
            spill.max_record_batch_memory,
        );
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(input_memory));
        let mut builder =
            build_merge_builder(spill_manager, schema, vec![spill], &pool, 2);
        let mut reservation = builder.reservation.new_empty();

        let result =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut reservation, true)?;
        assert!(matches!(result, SpillFilesToMerge::SplitThenRetry(0)));
        assert_eq!(reservation.size(), 0);
        assert_eq!(builder.sorted_spill_files.len(), 1);

        drop(builder);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn unsupported_spill_schema_uses_input_only_admission() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let array: ArrayRef = Arc::new(arrow::array::DictionaryArray::<
            arrow::datatypes::Int32Type,
        >::from_iter(["a", "b"]));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            array.data_type().clone(),
            false,
        )]));
        let spill_manager = build_spill_manager(&env, &schema);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        let mut spills = Vec::new();
        for _ in 0..2 {
            let (file, max_record_batch_memory) = spill_manager
                .spill_record_batch_iter_and_return_max_batch_memory(
                    std::iter::once(Ok(batch.clone())),
                    "dictionary input",
                )?
                .unwrap();
            spills.push(SortedSpillFile {
                file,
                max_record_batch_memory,
            });
        }
        let second = spills.pop().unwrap();
        let first = spills.pop().unwrap();
        let input_memory = [&first, &second]
            .into_iter()
            .map(|spill| {
                get_reserved_bytes_for_record_batch_size(
                    spill.max_record_batch_memory,
                    spill.max_record_batch_memory,
                )
            })
            .sum::<usize>();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(input_memory));
        let mut builder =
            build_merge_builder(spill_manager, schema, vec![first, second], &pool, 8192);
        let mut reservation = builder.reservation.new_empty();

        let SpillFilesToMerge::Ready(spills, buffer_len, output_headroom) =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut reservation, false)?
        else {
            panic!("unsupported schemas must not reserve output headroom");
        };

        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(output_headroom, 0);
        assert_eq!(reservation.size(), input_memory);
        drop((spills, reservation, builder));
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn replay_headroom_still_enforces_the_actual_pool() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let first = make_sorted_spill_file(&spill_manager, &schema, vec![1]);
        let second = make_sorted_spill_file(&spill_manager, &schema, vec![2]);
        let batch_memory = first.max_record_batch_memory;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(3 * batch_memory));
        let builder =
            build_merge_builder(spill_manager, schema, vec![first, second], &pool, 8192)
                .with_replay_headroom(true);
        let mut stream = builder.create_spillable_merge_stream();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("cannot be split further"));
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn spill_merge_output_construction_uses_the_real_pool() -> Result<()> {
        let output_budget = size_of::<i64>();
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let first = make_sorted_spill_file(&spill_manager, &schema, vec![1]);
        let second = make_sorted_spill_file(&spill_manager, &schema, vec![2]);
        let read_reservation = [&first, &second]
            .into_iter()
            .map(|spill| {
                get_reserved_bytes_for_record_batch_size(
                    spill.max_record_batch_memory,
                    spill.max_record_batch_memory,
                ) * 2
            })
            .sum::<usize>();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(read_reservation + output_budget));
        let builder = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![first, second],
            &pool,
            8192,
        );
        let stream = builder.create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let merged = concat_batches(&schema, &batches)?;
        let values = merged.column(0).as_primitive::<Int64Type>();
        assert_eq!(values.values(), &[1, 2]);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn spill_merge_applies_byte_target_to_wide_binary_output() -> Result<()> {
        const ROWS_PER_RUN: usize = 8;
        const VALUE_WIDTH: usize = 16 * 1024;

        let env = Arc::new(RuntimeEnv::default());
        let schema = binary_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let first = make_sorted_binary_spill_file(
            &spill_manager,
            &schema,
            (0..ROWS_PER_RUN)
                .map(|row| make_binary_value(row * 2, VALUE_WIDTH))
                .collect(),
        );
        let second = make_sorted_binary_spill_file(
            &spill_manager,
            &schema,
            (0..ROWS_PER_RUN)
                .map(|row| make_binary_value(row * 2 + 1, VALUE_WIDTH))
                .collect(),
        );
        let target = first
            .max_record_batch_memory
            .max(second.max_record_batch_memory);
        let unbounded_output = get_record_batch_memory_size(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from_iter_values(
                (0..2 * ROWS_PER_RUN).map(|row| make_binary_value(row, VALUE_WIDTH)),
            ))],
        )?);
        assert!(
            unbounded_output > target,
            "test setup must distinguish unbounded merge output from the byte target"
        );

        let read_reservation = [&first, &second]
            .into_iter()
            .map(|spill| {
                get_reserved_bytes_for_record_batch_size(
                    spill.max_record_batch_memory,
                    spill.max_record_batch_memory,
                ) * 2
            })
            .sum::<usize>();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(
            read_reservation + unbounded_output * 2,
        ));
        let stream = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![first, second],
            &pool,
            8192,
        )
        .create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;

        assert!(
            batches.len() > 1,
            "wide binary output must be split into byte-targeted batches"
        );
        for batch in &batches {
            assert!(
                get_record_batch_memory_size(batch) <= target,
                "output batch has {} bytes, above target {target}",
                get_record_batch_memory_size(batch),
            );
        }

        let merged = concat_batches(&schema, &batches)?;
        let values = merged
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let actual = (0..values.len())
            .map(|row| values.value(row).to_vec())
            .collect::<Vec<_>>();
        let expected = (0..2 * ROWS_PER_RUN)
            .map(|row| make_binary_value(row, VALUE_WIDTH))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[rstest::rstest]
    #[case(1, false, DataType::Utf8)]
    #[case(8192, true, DataType::Utf8)]
    #[case(8192, true, DataType::Utf8View)]
    #[tokio::test]
    async fn indivisible_spill_merge_needs_no_extra_disk_space(
        #[case] batch_size: usize,
        #[case] mixed_batches: bool,
        #[case] data_type: DataType,
    ) -> Result<()> {
        use arrow::array::{ArrayRef, StringArray, StringViewArray};

        const KEY_BYTES: usize = 300_000;
        const POOL_BYTES: usize = 2 * 1024 * 1024;
        let schema = Arc::new(Schema::new(vec![Field::new("x", data_type, false)]));
        let make_runs = |manager: &SpillManager| -> Result<Vec<SortedSpillFile>> {
            (0..2)
                .map(|run| {
                    let batches = (0..8).map(|batch| {
                        // A small two-row batch before the large singleton
                        // prevents treating the first batch as representative.
                        let keys = if mixed_batches && batch == 0 {
                            vec![format!("{run:04}"), format!("{:04}", run + 2)]
                        } else {
                            let key = run + 2 * (batch + usize::from(mixed_batches));
                            vec![format!("{key:04}{}", "x".repeat(KEY_BYTES - 4))]
                        };
                        let values: ArrayRef = match schema.field(0).data_type() {
                            DataType::Utf8 => Arc::new(StringArray::from(keys)),
                            DataType::Utf8View => Arc::new(StringViewArray::from(keys)),
                            _ => unreachable!(),
                        };
                        RecordBatch::try_new(Arc::clone(&schema), vec![values])
                            .map_err(Into::into)
                    });
                    let (file, max_record_batch_memory) = manager
                        .spill_record_batch_iter_and_return_max_batch_memory(
                            batches,
                            "indivisible input run",
                        )?
                        .unwrap();
                    Ok(SortedSpillFile {
                        file,
                        max_record_batch_memory,
                    })
                })
                .collect()
        };

        // Calibrate the quota to exactly the original IPC files, with no room
        // for even a replacement header. Keep several batches in each run so
        // read-ahead cannot retire the original before the unnecessary write.
        let calibration = Arc::new(RuntimeEnv::default());
        let runs = make_runs(&build_spill_manager(&calibration, &schema))?;
        let quota = runs.iter().map(|run| run.file.size().unwrap()).sum();
        drop(runs);
        let env = RuntimeEnvBuilder::new()
            .with_max_temp_directory_size(quota)
            .build_arc()?;
        let manager = build_spill_manager(&env, &schema);
        let runs = make_runs(&manager)?;
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, quota);
        assert!(4 * runs[0].max_record_batch_memory > POOL_BYTES / 2);
        assert!(4 * runs[0].max_record_batch_memory <= POOL_BYTES);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(POOL_BYTES));
        let builder = build_merge_builder(manager, schema, runs, &pool, batch_size)
            .with_replay_headroom(true);
        let mut stream = builder.create_spillable_merge_stream();
        let mut expected = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            assert_eq!(batch.num_rows(), 1);
            let values = arrow::compute::cast(batch.column(0), &DataType::Utf8)?;
            let key = values.as_string::<i32>().value(0);
            assert_eq!(key[..4].parse::<usize>().unwrap(), expected);
            expected += 1;
        }
        assert_eq!(expected, if mixed_batches { 18 } else { 16 });
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        let progress = env.disk_manager.spilling_progress();
        assert_eq!(progress.current_bytes, 0);
        assert_eq!(progress.active_files_count, 0);
        Ok(())
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn small_view_spill_merge_needs_no_extra_disk_space(
        #[values(3, 6)] pool_batches: usize,
        #[values(false, true)] mixed_batches: bool,
    ) -> Result<()> {
        use arrow::array::StringViewBuilder;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Utf8View,
            false,
        )]));
        let make_runs = |manager: &SpillManager| -> Result<Vec<SortedSpillFile>> {
            (0..2)
                .map(|run| {
                    let batches = (0..8).map(|batch| {
                        // A non-inline value retains the builder's 8 KiB block,
                        // but IPC stores only its used bytes. Small view buffers
                        // are below the spill writer's compaction threshold.
                        let key = format!("{:04}xxxxxxxxx", run + 2 * batch);
                        let mut values = StringViewBuilder::with_capacity(1)
                            .with_fixed_block_size(8192);
                        values.append_value(key);
                        // A larger final batch requires scanning the entire run
                        // before replacing its maximum decoded size.
                        if mixed_batches && batch == 7 {
                            values.append_value(format!(
                                "{:04}xxxxxxxxx",
                                run + 2 * (batch + 1)
                            ));
                        }
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![Arc::new(values.finish())],
                        )
                        .map_err(Into::into)
                    });
                    let (file, max_record_batch_memory) = manager
                        .spill_record_batch_iter_and_return_max_batch_memory(
                            batches,
                            "small view singleton run",
                        )?
                        .unwrap();
                    // Preserve an old writer's stale full-block estimate. New
                    // writers record the smaller physical IPC bound, so deriving
                    // the test's pool from that value would change this fixture.
                    Ok(SortedSpillFile {
                        file,
                        max_record_batch_memory: max_record_batch_memory.max(8192 + 32),
                    })
                })
                .collect()
        };

        // Fill the quota with the original runs. Multiple batches prevent
        // read-ahead from retiring a run before an unnecessary replacement write.
        let calibration = Arc::new(RuntimeEnv::default());
        let runs = make_runs(&build_spill_manager(&calibration, &schema))?;
        let quota = runs.iter().map(|run| run.file.size().unwrap()).sum();
        drop(runs);
        let env = RuntimeEnvBuilder::new()
            .with_max_temp_directory_size(quota)
            .build_arc()?;
        let manager = build_spill_manager(&env, &schema);
        let runs = make_runs(&manager)?;
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, quota);
        let stored_max = runs[0].max_record_batch_memory;
        let mut source = manager.read_spill_as_stream_unbuffered(
            Arc::clone(&runs[0].file),
            Some(stored_max),
        )?;
        let batch = source.next().await.unwrap()?;
        assert_eq!(batch.num_rows(), 1);
        assert!(gc_view_arrays(&batch)?.get_sliced_size()? < stored_max);
        drop((source, batch));

        // The stale estimate needs four batches for the minimum merge. Test
        // limits below and above that estimate: both can fit the decoded rows.
        let pool_bytes = pool_batches * stored_max;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_bytes));
        let builder = build_merge_builder(manager, schema, runs, &pool, 8192)
            .with_replay_headroom(true);
        let mut stream = builder.create_spillable_merge_stream();
        let mut expected = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if mixed_batches {
                assert!(batch.num_rows() <= 2);
            } else {
                assert_eq!(batch.num_rows(), 1);
            }
            for key in batch.column(0).as_string_view().iter() {
                assert_eq!(key.unwrap(), format!("{expected:04}xxxxxxxxx"));
                expected += 1;
            }
        }
        assert_eq!(expected, if mixed_batches { 18 } else { 16 });
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        let progress = env.disk_manager.spilling_progress();
        assert_eq!(progress.current_bytes, 0);
        assert_eq!(progress.active_files_count, 0);
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
            .split_spill_file_in_half(0, false)
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

        // 4.5*m forces exactly one re-spill once the output-construction
        // headroom is included (split one run, then both fit).
        let initial_batch_size = 8192;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(m * 9 / 2));

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

        // The largest emitted batch is the halved size, not the original 8192: the
        // shrunk run carries a halved batch-size limit, and the final pass consumes
        // it, so the merge output is capped there. Without the per-run limit the merge
        // would rebuild 8192-row batches.
        let expected_batch_size = initial_batch_size / 2;
        let max_batch_rows = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
        assert_eq!(
            max_batch_rows, expected_batch_size,
            "after one re-spill the merge must emit {expected_batch_size}-row \
             batches, got a largest batch of {max_batch_rows} rows"
        );

        Ok(())
    }

    /// Same as [`respill_halves_the_merge_output_batch_size`], but under a budget tight
    /// enough that *both* runs must be re-spilled before the merge fits - the scenario
    /// where the batch-size reduction could compound. Because the reduction is tracked
    /// per-run (each run capped at half) rather than by halving the global batch size on
    /// every split, the merged output is emitted in 4096-row batches - half, not a
    /// quarter. A global-halving implementation would have halved once per re-spill and
    /// emitted 2048-row batches.
    #[tokio::test]
    async fn respilling_two_skewed_runs_halves_the_output_without_compounding()
    -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);

        let n: i64 = 16384;
        let f0 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let f1 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let m = f0.max_record_batch_memory.max(f1.max_record_batch_memory);

        // 3.5*m is tight enough that even after halving one run the inputs plus
        // output-construction headroom still don't fit, so *both* runs are
        // re-spilled once before the merge succeeds. This is exactly the scenario
        // where a compounding, global-halving implementation would drive the output
        // batch size down to a quarter.
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

        // Each run was re-spilled once, so each is capped at half the original batch
        // size and the merge caps its output at that half - NOT a quarter. A global
        // halving-per-split implementation would have emitted 2048-row batches here.
        let expected_batch_size = initial_batch_size / 2;
        let max_batch_rows = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
        assert_eq!(
            max_batch_rows, expected_batch_size,
            "two re-spills must halve (not quarter) the output: expected \
             {expected_batch_size}-row batches, got a largest batch of \
             {max_batch_rows} rows"
        );

        Ok(())
    }

    #[test]
    fn spill_merge_fan_in_is_unlimited_by_default() {
        assert_eq!(effective_spill_merge_fan_in(0), usize::MAX);
    }

    #[test]
    fn spill_merge_fan_in_preserves_merge_progress() {
        assert_eq!(effective_spill_merge_fan_in(1), 2);
        assert_eq!(effective_spill_merge_fan_in(2), 2);
        assert_eq!(effective_spill_merge_fan_in(8), 8);
    }

    #[test]
    fn spill_merge_phase_respects_configured_fan_in() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let runtime = RuntimeEnvBuilder::new()
            .with_max_spill_merge_fan_in(2)
            .build_arc()?;
        let spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );
        let sorted_spill_files = (0..4)
            .map(|idx| {
                Ok(SortedSpillFile {
                    file: runtime
                        .disk_manager
                        .create_tmp_file(&format!("spill fan-in test {idx}"))?,
                    max_record_batch_memory: 1,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let expr = LexOrdering::new([PhysicalSortExpr::new_default(col("a", &schema)?)])
            .unwrap();
        let reservation =
            MemoryConsumer::new("spill_merge_phase_respects_configured_fan_in")
                .register(&runtime.memory_pool);
        let metrics = BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut builder = MultiLevelMergeBuilder::new(
            spill_manager,
            schema,
            sorted_spill_files,
            vec![],
            expr,
            metrics,
            1024,
            reservation,
            None,
            false,
        );
        let mut merge_reservation = MemoryConsumer::new("spill_merge_fan_in_phase")
            .register(&runtime.memory_pool);

        let (spills, buffer_len) = match builder.get_sorted_spill_files_to_merge(
            1,
            2,
            &mut merge_reservation,
            false,
        )? {
            SpillFilesToMerge::Ready(spills, buffer_len, _) => (spills, buffer_len),
            SpillFilesToMerge::SplitThenRetry(index) => {
                panic!("expected ready spill files, got retry for index {index}")
            }
        };

        assert_eq!(spills.len(), 2);
        assert_eq!(buffer_len, 1);
        assert_eq!(builder.sorted_spill_files.len(), 2);

        Ok(())
    }
}
