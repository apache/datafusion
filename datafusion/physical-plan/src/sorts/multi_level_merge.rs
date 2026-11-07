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
use datafusion_common::{Result, resources_err};
use datafusion_execution::memory_pool::{MemoryPool, MemoryReservation, MergeMemoryPool};

use crate::sorts::builder::try_grow_reservation_to_at_least;
use crate::sorts::sort::get_reserved_bytes_for_record_batch_size;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
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
/// The merge relies on one property of its inputs: every batch of a sorted spill
/// run is at most the bound its [`SpillManager`] sets (see
/// [`max_spill_batch_bytes`]), unless it holds a single row or a column that
/// cannot be split by rows (see [`SpillManager::with_max_batch_bytes`]). The writers of sorted runs (the sort, the aggregate, and this merge for
/// its intermediate runs) get it from their [`SpillManager`], which splits larger
/// batches as it writes them. The merge keeps its own output to the same bound,
/// so an intermediate run, and the stream it returns, have it too.
///
/// 1. **Worst-Case Memory Reservation**: Reserves memory for the largest batch of
///    each run it reads, times the number of batches read ahead.
/// 2. **Adaptive Buffer Sizing**: Reads fewer batches ahead when memory is
///    constrained.
/// 3. **Headroom**: A run is admitted only if the pool could give as much again
///    as the merge takes beyond the workspace it already holds. That much is left
///    for the consumer of the merge (the aggregate replaying its spills) and for
///    other operators sharing the pool. Only the two runs a pass needs, with one
///    batch read ahead, may use the whole pool.
/// 4. **Spill-to-Disk**: Spill to disk when we cannot merge all runs in memory.
///    An intermediate pass merges only as many runs as needed for the remaining
///    runs to fit one final pass.
///
/// If two runs do not fit even with one batch read ahead (single rows or columns
/// that cannot be split, or a pool smaller than the bound was chosen for), the
/// merge returns `ResourcesExhausted`.
pub(crate) struct MultiLevelMergeBuilder {
    spill_manager: SpillManager,
    schema: SchemaRef,
    sorted_spill_files: Vec<SortedSpillFile>,
    sorted_streams: Vec<SendableRecordBatchStream>,
    expr: LexOrdering,
    metrics: BaselineMetrics,
    batch_size: usize,
    reservation: MemoryReservation,
    /// Workspace retained across intermediate spill passes.
    merge_pool: Option<Arc<MergeMemoryPool>>,
    fetch: Option<usize>,
    enable_round_robin_tie_breaker: bool,
}

/// Largest in-memory size of a batch in a sorted spill run, such that the
/// smallest merge (two runs, one batch read ahead from each) fits in
/// `merge_memory`. Each batch is charged twice its size (see
/// [`get_reserved_bytes_for_record_batch_size`]).
///
/// The sort passes `sort_spill_reservation_bytes`, the workspace it holds for
/// its merge. The aggregate, which holds no workspace, also passes half of the
/// memory it held when it spilled.
pub(crate) fn max_spill_batch_bytes(merge_memory: usize) -> usize {
    const MIN_SPILL_BATCH_BYTES: usize = 64 * 1024;
    let per_batch = get_reserved_bytes_for_record_batch_size(1, 1);
    (merge_memory / (MIN_SPILL_MERGE_FAN_IN * per_batch)).max(MIN_SPILL_BATCH_BYTES)
}

/// A merge pass needs at least two inputs to make progress.
const MIN_SPILL_MERGE_FAN_IN: usize = 2;

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
            merge_pool: None,
            enable_round_robin_tie_breaker,
            fetch,
        }
    }

    pub(super) fn with_merge_pool(mut self, pool: Option<Arc<MergeMemoryPool>>) -> Self {
        self.merge_pool = pool;
        self
    }

    pub(crate) fn create_spillable_merge_stream(self) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::once(self.create_stream()).try_flatten(),
        ))
    }

    async fn create_stream(mut self) -> Result<SendableRecordBatchStream> {
        loop {
            let mut stream = self.merge_sorted_runs_within_mem_limit()?;

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
            let Some((file, max_record_batch_memory)) = self
                .spill_manager
                .spill_record_batch_stream_and_return_max_batch_memory(
                    &mut stream,
                    "MultiLevelMergeBuilder intermediate spill",
                )
                .await?
            else {
                continue;
            };

            self.sorted_spill_files.push(SortedSpillFile {
                file,
                max_record_batch_memory,
            });
        }
    }

    /// This tries to create a stream that merges the most sorted streams and sorted spill files
    /// as possible within the memory limit.
    fn merge_sorted_runs_within_mem_limit(
        &mut self,
    ) -> Result<SendableRecordBatchStream> {
        match (self.sorted_spill_files.len(), self.sorted_streams.len()) {
            // No data so empty batch
            (0, 0) => {
                let empty_stream =
                    Box::pin(EmptyRecordBatchStream::new(Arc::clone(&self.schema)));
                Ok(self.observe_output(empty_stream))
            }

            // Only in-memory stream, return that
            (0, 1) => {
                let output_stream = self.sorted_streams.remove(0);
                Ok(self.observe_output(output_stream))
            }

            // Only single sorted spill file so return it
            (1, 0) => {
                let spill_file = self.sorted_spill_files.remove(0);

                // Not reserving any memory for this disk as we are not holding it in memory
                let output_stream = self
                    .spill_manager
                    .read_spill_as_stream(spill_file.file, None)?;
                Ok(self.observe_output(output_stream))
            }

            // Only in memory streams, so merge them all in a single pass
            (0, _) => {
                let sorted_stream = mem::take(&mut self.sorted_streams);
                // No need to wrap with observed stream since merge sort will update the observed metrics
                self.create_new_merge_sort(
                    sorted_stream,
                    // If we have no sorted spill files left, this is the last run
                    true,
                    true,
                )
            }

            // Need to merge multiple streams
            (_, _) => {
                // Transfer any pre-reserved bytes (from sort_spill_reservation_bytes)
                // to the merge memory reservation. This prevents starvation when
                // concurrent sort partitions compete for pool memory: the pre-reserved
                // bytes cover spill file buffer reservations without additional pool
                // allocation.
                let mut memory_reservation = self.reservation.take();

                let (mut sorted_spill_files, buffer_size) = self
                    .get_sorted_spill_files_to_merge(
                        2,
                        // we must have at least 2 streams to merge
                        MIN_SPILL_MERGE_FAN_IN.saturating_sub(self.sorted_streams.len()),
                        &mut memory_reservation,
                    )?;

                if self.fetch.is_none()
                    && self.sorted_streams.is_empty()
                    && !self.sorted_spill_files.is_empty()
                {
                    // This is an intermediate pass. Merge only as many runs as
                    // needed for the rest to fit the final pass, which starts
                    // with two read buffers per run and leaves headroom. Size it
                    // within the grant this pass holds: another partition may
                    // take every spare byte while this pass writes to disk.
                    let widest = sorted_spill_files
                        .iter()
                        .chain(&self.sorted_spill_files)
                        .map(|run| run.max_record_batch_memory)
                        .max()
                        .unwrap_or_default();
                    let per_run =
                        2 * get_reserved_bytes_for_record_batch_size(widest, widest);
                    let final_fan_in = (memory_reservation.size() / per_run.max(1) / 2)
                        .min(self.max_fan_in());
                    let needed =
                        (self.sorted_spill_files.len() + sorted_spill_files.len() + 1)
                            .saturating_sub(final_fan_in)
                            .max(MIN_SPILL_MERGE_FAN_IN);
                    if needed < sorted_spill_files.len() {
                        let unused = sorted_spill_files.split_off(needed);
                        self.sorted_spill_files.splice(0..0, unused);
                        // Keep the grant through intermediate EOF and the
                        // spill write, then transfer it to the next admission.
                        // This does not reserve additional pool memory.
                        self.reservation = memory_reservation.take();
                    }
                }

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

                    Ok(merge_sort_stream)
                } else {
                    // Attach the memory reservation to the stream to make sure we have enough memory
                    // throughout the merge process as we bypassed the memory pool for the merge sort stream
                    Ok(Box::pin(StreamAttachedReservation::new(
                        merge_sort_stream,
                        memory_reservation,
                    )))
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
            .with_max_batch_bytes(self.spill_manager.max_batch_bytes())
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

    /// Returns the sorted spill files to read in the next pass, and how many
    /// batches to read ahead from each.
    ///
    /// Takes as many runs as fit, at least `minimum_number_of_required_streams`.
    /// Every run must leave as much again as the merge takes beyond the
    /// workspace it already holds (see the type docs). If the minimum does not
    /// fit, retries with fewer batches read ahead; with one batch read ahead the
    /// minimum only has to fit the pool, or the pool's error is returned.
    fn get_sorted_spill_files_to_merge(
        &mut self,
        buffer_len: usize,
        minimum_number_of_required_streams: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<(Vec<SortedSpillFile>, usize)> {
        assert_ne!(buffer_len, 0, "Buffer length must be greater than 0");
        let max_spill_files = self.max_fan_in();
        // Workspace the merge already holds (`sort_spill_reservation_bytes`,
        // retained by the sort's merge pool) takes nothing from others.
        let owned = self
            .merge_pool
            .as_ref()
            .map_or_else(|| reservation.size(), |pool| pool.reserved());
        let mut number_of_spills = 0;
        let mut accepted_memory: usize = 0;

        for spill in &self.sorted_spill_files {
            if number_of_spills >= max_spill_files {
                break;
            }

            let total_needed = accepted_memory
                + get_reserved_bytes_for_record_batch_size(
                    spill.max_record_batch_memory,
                    // Size will be the same as the sliced size, bc it is a spilled batch.
                    spill.max_record_batch_memory,
                ) * buffer_len;
            // Only the minimum merge with one batch read ahead may use the
            // whole pool: without it, the merge cannot make progress at all.
            let admission = if buffer_len == 1
                && number_of_spills < minimum_number_of_required_streams
            {
                try_grow_reservation_to_at_least(reservation, total_needed)
            } else {
                match total_needed.checked_add(total_needed.saturating_sub(owned)) {
                    Some(with_headroom) => {
                        try_grow_reservation_to_at_least(reservation, with_headroom)
                    }
                    None => resources_err!("Spill merge headroom exceeds usize::MAX"),
                }
            };
            match admission {
                Ok(()) => {
                    number_of_spills += 1;
                    accepted_memory = total_needed;
                }
                Err(err) if number_of_spills < minimum_number_of_required_streams => {
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
                    return Err(err.context(format!(
                        "Cannot merge two sorted spill runs: each needs {} bytes \
                         to read one batch; consider increasing the memory limit",
                        get_reserved_bytes_for_record_batch_size(
                            spill.max_record_batch_memory,
                            spill.max_record_batch_memory
                        )
                    )));
                }
                // We reached the maximum amount of memory we can use for this merge
                Err(_) => break,
            }
        }

        // Keep only the buffers that were admitted, releasing the headroom. If
        // no run was admitted, the reservation stays with the in-memory merge.
        if number_of_spills > 0 && reservation.size() > accepted_memory {
            reservation.shrink(reservation.size() - accepted_memory);
        }

        let spills = self
            .sorted_spill_files
            .drain(..number_of_spills)
            .collect::<Vec<_>>();

        Ok((spills, buffer_len))
    }

    /// The most runs one pass may merge (`max_spill_merge_fan_in`).
    fn max_fan_in(&self) -> usize {
        effective_spill_merge_fan_in(
            self.spill_manager
                .env()
                .disk_manager
                .max_spill_merge_fan_in(),
        )
    }

    fn observe_output(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        Box::pin(ObservedStream::new(stream, self.metrics.clone(), None))
    }
}

fn effective_spill_merge_fan_in(configured_fan_in: usize) -> usize {
    if configured_fan_in == 0 {
        usize::MAX
    } else {
        configured_fan_in.max(MIN_SPILL_MERGE_FAN_IN)
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
#[path = "multi_level_merge/intermediate_sizing_tests.rs"]
mod intermediate_sizing_tests;

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
    use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion_physical_expr::expressions::{Column, col};
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };

    pub(super) fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]))
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
    #[case::headroom_limits_sizing(9, 128, 7)]
    #[case::unchanged_selection(13, 128, 7)]
    #[case::short_batches(9, 64, 7)]
    #[tokio::test]
    async fn intermediate_merge_sizing_preserves_final_output(
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
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(8 * 7 * batch_memory));
        let builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, 128);
        let stream = builder.create_spillable_merge_stream();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let merged = concat_batches(&schema, &batches)?;
        let expected = Int64Array::from_iter_values(0..(runs * rows) as i64);
        assert_eq!(merged.column(0).as_primitive::<Int64Type>(), &expected);
        assert_eq!(
            spilled_rows.value(),
            (runs + sized_intermediate_inputs) * rows
        );
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        assert_eq!(env.disk_manager.used_disk_space(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn intermediate_merge_sizing_preserves_heterogeneous_runs() -> Result<()> {
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
            build_merge_builder(spill_manager, Arc::clone(&schema), files, &pool, rows);
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

    /// Runs written through a manager with a byte bound merge in a pool that
    /// could not hold two of their batches had they been written whole.
    #[tokio::test]
    async fn bounded_runs_merge_in_a_pool_smaller_than_two_whole_batches() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let n: i64 = 16384;
        let whole = build_spill_manager(&env, &schema);
        let whole_batch = make_sorted_spill_file(&whole, &schema, (0..n).collect())
            .max_record_batch_memory;
        let bound = whole_batch / 4;
        let spill_manager =
            build_spill_manager(&env, &schema).with_max_batch_bytes(bound);
        let f0 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        let f1 = make_sorted_spill_file(&spill_manager, &schema, (0..n).collect());
        assert!(f0.max_record_batch_memory <= bound);
        assert!(f1.max_record_batch_memory <= bound);

        // Merging the whole batches needs 4 * whole_batch with one batch read
        // ahead, more than this pool.
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(whole_batch * 7 / 2));
        let builder = build_merge_builder(
            spill_manager,
            Arc::clone(&schema),
            vec![f0, f1],
            &pool,
            8192,
        );
        let batches: Vec<RecordBatch> = builder
            .create_spillable_merge_stream()
            .try_collect()
            .await?;
        for batch in &batches {
            // The merge output keeps to the same bound, give or take a row.
            assert!(crate::spill::get_record_batch_memory_size(batch) <= bound + 64);
        }
        let merged = concat_batches(&schema, &batches)?;
        let col = merged.column(0).as_primitive::<Int64Type>();
        assert_eq!(col.len(), (2 * n) as usize);
        assert!(col.values().windows(2).all(|w| w[0] <= w[1]));
        drop(batches);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// The two runs a pass needs only have to fit the pool, not the headroom.
    #[tokio::test]
    async fn minimum_merge_needs_no_headroom() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = (0..3)
            .map(|value| make_sorted_spill_file(&spill_manager, &schema, vec![value]))
            .collect::<Vec<_>>();
        let batch_memory = spills[0].max_record_batch_memory;
        let pool_size = 6 * batch_memory;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
        let mut builder =
            build_merge_builder(spill_manager, Arc::clone(&schema), spills, &pool, 8192);

        let mut reservation = builder.reservation.new_empty();
        let (spills, buffer_len) =
            builder.get_sorted_spill_files_to_merge(2, 2, &mut reservation)?;
        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(builder.sorted_spill_files.len(), 1);
        assert_eq!(reservation.size(), 4 * batch_memory);
        builder.sorted_spill_files.splice(0..0, spills);
        reservation.free();

        let mut stream = builder.create_spillable_merge_stream();
        let mut values: Vec<i64> = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            values.extend(batch.column(0).as_primitive::<Int64Type>().values());
        }
        assert_eq!(values, vec![0, 1, 2]);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    /// Without workspace (the aggregate's merge), reading two batches ahead
    /// must also leave as much again for the replay; otherwise the merge reads
    /// one batch ahead.
    #[test]
    fn read_ahead_needs_headroom() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = (0..2)
            .map(|value| make_sorted_spill_file(&spill_manager, &schema, vec![value]))
            .collect::<Vec<_>>();
        let batch_memory = spills[0].max_record_batch_memory;
        // Two batches read ahead from two runs take 8 batches of the pool's 9.
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(9 * batch_memory));
        let mut builder = build_merge_builder(spill_manager, schema, spills, &pool, 1);
        let mut reservation = builder.reservation.new_empty();
        let (spills, buffer_len) =
            builder.get_sorted_spill_files_to_merge(2, 2, &mut reservation)?;
        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(reservation.size(), 4 * batch_memory);
        drop((reservation, spills, builder));
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn headroom_is_released_after_rejected_candidate() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let spills = (0..3)
            .map(|value| make_sorted_spill_file(&spill_manager, &schema, vec![value]))
            .collect::<Vec<_>>();
        let batch_memory = spills[0].max_record_batch_memory;
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(10 * batch_memory));
        let mut builder = build_merge_builder(spill_manager, schema, spills, &pool, 1);
        let mut reservation = builder.reservation.new_empty();
        let (spills, buffer_len) =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut reservation)?;
        assert_eq!(buffer_len, 1);
        assert_eq!(spills.len(), 2);
        assert_eq!(builder.sorted_spill_files.len(), 1);
        // The third candidate failed its 12-batch reservation. Release the
        // successful probe's spare four batches, retaining only two inputs.
        assert_eq!(reservation.size(), 4 * batch_memory);
        let replay = builder.reservation.new_empty();
        replay.try_grow(6 * batch_memory)?;
        assert_eq!(pool.reserved(), 10 * batch_memory);
        drop((replay, reservation, spills, builder));
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
        Ok(())
    }

    #[tokio::test]
    async fn minimum_merge_still_enforces_the_actual_pool() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let schema = test_schema();
        let spill_manager = build_spill_manager(&env, &schema);
        let first = make_sorted_spill_file(&spill_manager, &schema, vec![1]);
        let second = make_sorted_spill_file(&spill_manager, &schema, vec![2]);
        let batch_memory = first.max_record_batch_memory;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(3 * batch_memory));
        let builder =
            build_merge_builder(spill_manager, schema, vec![first, second], &pool, 8192);
        let mut stream = builder.create_spillable_merge_stream();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(
            matches!(
                error.find_root(),
                datafusion_common::DataFusionError::ResourcesExhausted(_)
            ),
            "{error}"
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(env.disk_manager.spilling_progress().current_bytes, 0);
        assert_eq!(env.disk_manager.spilling_progress().active_files_count, 0);
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

        let (spills, buffer_len) =
            builder.get_sorted_spill_files_to_merge(1, 2, &mut merge_reservation)?;

        assert_eq!(spills.len(), 2);
        assert_eq!(buffer_len, 1);
        assert_eq!(builder.sorted_spill_files.len(), 2);

        Ok(())
    }
}
