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

//! Sort that deals with an arbitrary size of the input.
//! It will do in-memory sorting if it has enough memory budget
//! but spills to disk if needed.

use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::common::spawn_buffered;
use crate::execution_plan::{Boundedness, CardinalityEffect, EmissionType};
use crate::expressions::PhysicalSortExpr;
use crate::filter_pushdown::{
    ChildFilterDescription, FilterDescription, FilterPushdownPhase,
};
use crate::limit::LimitStream;
use crate::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput, SpillMetrics,
    SplitMetrics,
};
use crate::projection::{make_with_child, update_ordering, ProjectionExec};
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::get_record_batch_memory_size;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::{GetSlicedSize, SpillManager};
use crate::stream::BatchSplitStream;
use crate::stream::RecordBatchStreamAdapter;
use crate::topk::TopK;
use crate::topk::TopKDynamicFilters;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, SendableRecordBatchStream,
    Statistics,
};

use arrow::array::{Array, RecordBatch, RecordBatchOptions, StringViewArray};
use arrow::compute::{concat_batches, lexsort_to_indices, take_arrays};
use arrow::datatypes::SchemaRef;
use datafusion_common::config::SpillCompression;
use datafusion_common::{
    assert_or_internal_err, internal_datafusion_err, unwrap_or_internal_err,
    DataFusionError, Result,
};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::{lit, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_expr::PhysicalExpr;

use futures::{StreamExt, TryStreamExt};
use log::{debug, trace};

struct ExternalSorterMetrics {
    /// metrics
    baseline: BaselineMetrics,

    spill_metrics: SpillMetrics,

    split_metrics: SplitMetrics,
}

impl ExternalSorterMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            spill_metrics: SpillMetrics::new(metrics, partition),
            split_metrics: SplitMetrics::new(metrics, partition),
        }
    }
}

/// Sorts an arbitrary sized, unsorted, stream of [`RecordBatch`]es to
/// a total order. Depending on the input size and memory manager
/// configuration, writes intermediate results to disk ("spills")
/// using Arrow IPC format.
///
/// # Algorithm
///
/// 1. get a non-empty new batch from input
///
/// 2. check with the memory manager there is sufficient space to
///    buffer the batch in memory.
///
/// 2.1 if memory is sufficient, buffer batch in memory, go to 1.
///
/// 2.2 if no more memory is available, sort all buffered batches and
///     spill to file.  buffer the next batch in memory, go to 1.
///
/// 3. when input is exhausted, merge all in memory batches and spills
///    to get a total order.
///
/// # When data fits in available memory
///
/// If there is sufficient memory, data is sorted in memory to produce the output
///
/// ```text
///    ┌─────┐
///    │  2  │
///    │  3  │
///    │  1  │─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///    │  4  │
///    │  2  │                  │
///    └─────┘                  ▼
///    ┌─────┐
///    │  1  │              In memory
///    │  4  │─ ─ ─ ─ ─ ─▶ sort/merge  ─ ─ ─ ─ ─▶  total sorted output
///    │  1  │
///    └─────┘                  ▲
///      ...                    │
///
///    ┌─────┐                  │
///    │  4  │
///    │  3  │─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///    └─────┘
///
/// in_mem_batches
/// ```
///
/// # When data does not fit in available memory
///
///  When memory is exhausted, data is first sorted and written to one
///  or more spill files on disk:
///
/// ```text
///    ┌─────┐                               .─────────────────.
///    │  2  │                              (                   )
///    │  3  │                              │`─────────────────'│
///    │  1  │─ ─ ─ ─ ─ ─ ─                 │  ┌────┐           │
///    │  4  │             │                │  │ 1  │░          │
///    │  2  │                              │  │... │░          │
///    └─────┘             ▼                │  │ 4  │░  ┌ ─ ─   │
///    ┌─────┐                              │  └────┘░    1  │░ │
///    │  1  │         In memory            │   ░░░░░░  │    ░░ │
///    │  4  │─ ─ ▶   sort/merge    ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─▶ ... │░ │
///    │  1  │     and write to file        │           │    ░░ │
///    └─────┘                              │             4  │░ │
///      ...               ▲                │           └░─░─░░ │
///                        │                │            ░░░░░░ │
///    ┌─────┐                              │.─────────────────.│
///    │  4  │             │                (                   )
///    │  3  │─ ─ ─ ─ ─ ─ ─                  `─────────────────'
///    └─────┘
///
/// in_mem_batches                                  spills
///                                         (file on disk in Arrow
///                                               IPC format)
/// ```
///
/// Once the input is completely read, the spill files are read and
/// merged with any in memory batches to produce a single total sorted
/// output:
///
/// ```text
///   .─────────────────.
///  (                   )
///  │`─────────────────'│
///  │  ┌────┐           │
///  │  │ 1  │░          │
///  │  │... │─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─
///  │  │ 4  │░ ┌────┐   │           │
///  │  └────┘░ │ 1  │░  │           ▼
///  │   ░░░░░░ │    │░  │
///  │          │... │─ ─│─ ─ ─ ▶ merge  ─ ─ ─▶  total sorted output
///  │          │    │░  │
///  │          │ 4  │░  │           ▲
///  │          └────┘░  │           │
///  │           ░░░░░░  │
///  │.─────────────────.│           │
///  (                   )
///   `─────────────────'            │
///         spills
///                                  │
///
///                                  │
///
///     ┌─────┐                      │
///     │  1  │
///     │  4  │─ ─ ─ ─               │
///     └─────┘       │
///       ...                   In memory
///                   └ ─ ─ ─▶  sort/merge
///     ┌─────┐
///     │  4  │                      ▲
///     │  3  │─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     └─────┘
///
///  in_mem_batches
/// ```
struct ExternalSorter {
    // ========================================================================
    // PROPERTIES:
    // Fields that define the sorter's configuration and remain constant
    // ========================================================================
    /// Schema of the output (and the input)
    schema: SchemaRef,
    /// Sort expressions
    expr: LexOrdering,
    /// The target number of rows for output batches
    batch_size: usize,
    /// If the in size of buffered memory batches is below this size,
    /// the data will be concatenated and sorted in place rather than
    /// sort/merged.
    sort_in_place_threshold_bytes: usize,

    // ========================================================================
    // STATE BUFFERS:
    // Fields that hold intermediate data during sorting
    // ========================================================================
    /// Unsorted input batches stored in the memory buffer
    in_mem_batches: Vec<RecordBatch>,

    /// During external sorting, in-memory intermediate data will be appended to
    /// this file incrementally. Once finished, this file will be moved to [`Self::finished_spill_files`].
    ///
    /// this is a tuple of:
    /// 1. `InProgressSpillFile` - the file that is being written to
    /// 2. `max_record_batch_memory` - the maximum memory usage of a single batch in this spill file.
    in_progress_spill_file: Option<(InProgressSpillFile, usize)>,
    /// If data has previously been spilled, the locations of the spill files (in
    /// Arrow IPC format)
    /// Within the same spill file, the data might be chunked into multiple batches,
    /// and ordered by sort keys.
    finished_spill_files: Vec<SortedSpillFile>,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Fields related to managing execution resources and monitoring performance.
    // ========================================================================
    /// Runtime metrics
    metrics: ExternalSorterMetrics,
    /// A handle to the runtime to get spill files
    runtime: Arc<RuntimeEnv>,
    /// Reservation for in_mem_batches
    reservation: MemoryReservation,
    spill_manager: SpillManager,

    /// Reservation for the merging of in-memory batches. If the sort
    /// might spill, `sort_spill_reservation_bytes` will be
    /// pre-reserved to ensure there is some space for this sort/merge.
    merge_reservation: MemoryReservation,
    /// How much memory to reserve for performing in-memory sort/merges
    /// prior to spilling.
    sort_spill_reservation_bytes: usize,
}

impl ExternalSorter {
    // TODO: make a builder or some other nicer API to avoid the
    // clippy warning
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        expr: LexOrdering,
        batch_size: usize,
        sort_spill_reservation_bytes: usize,
        sort_in_place_threshold_bytes: usize,
        // Configured via `datafusion.execution.spill_compression`.
        spill_compression: SpillCompression,
        metrics: &ExecutionPlanMetricsSet,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<Self> {
        let metrics = ExternalSorterMetrics::new(metrics, partition_id);
        let reservation = MemoryConsumer::new(format!("ExternalSorter[{partition_id}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        let merge_reservation =
            MemoryConsumer::new(format!("ExternalSorterMerge[{partition_id}]"))
                .register(&runtime.memory_pool);

        let spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            metrics.spill_metrics.clone(),
            Arc::clone(&schema),
        )
        .with_compression_type(spill_compression);

        Ok(Self {
            schema,
            in_mem_batches: vec![],
            in_progress_spill_file: None,
            finished_spill_files: vec![],
            expr,
            metrics,
            reservation,
            spill_manager,
            merge_reservation,
            runtime,
            batch_size,
            sort_spill_reservation_bytes,
            sort_in_place_threshold_bytes,
        })
    }

    /// Appends an unsorted [`RecordBatch`] to `in_mem_batches`
    ///
    /// Updates memory usage metrics, and possibly triggers spilling to disk
    async fn insert_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }

        self.reserve_memory_for_merge()?;
        self.reserve_memory_for_batch_and_maybe_spill(&input)
            .await?;

        self.in_mem_batches.push(input);
        Ok(())
    }

    fn spilled_before(&self) -> bool {
        !self.finished_spill_files.is_empty()
    }

    /// Returns the final sorted output of all batches inserted via
    /// [`Self::insert_batch`] as a stream of [`RecordBatch`]es.
    ///
    /// This process could either be:
    ///
    /// 1. An in-memory sort/merge (if the input fit in memory)
    ///
    /// 2. A combined streaming merge incorporating both in-memory
    ///    batches and data from spill files on disk.
    async fn sort(&mut self) -> Result<SendableRecordBatchStream> {
        // Release the memory reserved for merge back to the pool so
        // there is some left when `in_mem_sort_stream` requests an
        // allocation.
        self.merge_reservation.free();

        if self.spilled_before() {
            // Sort `in_mem_batches` and spill it first. If there are many
            // `in_mem_batches` and the memory limit is almost reached, merging
            // them with the spilled files at the same time might cause OOM.
            if !self.in_mem_batches.is_empty() {
                self.sort_and_spill_in_mem_batches().await?;
            }

            StreamingMergeBuilder::new()
                .with_sorted_spill_files(std::mem::take(&mut self.finished_spill_files))
                .with_spill_manager(self.spill_manager.clone())
                .with_schema(Arc::clone(&self.schema))
                .with_expressions(&self.expr.clone())
                .with_metrics(self.metrics.baseline.clone())
                .with_batch_size(self.batch_size)
                .with_fetch(None)
                .with_reservation(self.merge_reservation.new_empty())
                .build()
        } else {
            self.in_mem_sort_stream(self.metrics.baseline.clone())
        }
    }

    /// How much memory is buffered in this `ExternalSorter`?
    fn used(&self) -> usize {
        self.reservation.size()
    }

    /// How many bytes have been spilled to disk?
    fn spilled_bytes(&self) -> usize {
        self.metrics.spill_metrics.spilled_bytes.value()
    }

    /// How many rows have been spilled to disk?
    fn spilled_rows(&self) -> usize {
        self.metrics.spill_metrics.spilled_rows.value()
    }

    /// How many spill files have been created?
    fn spill_count(&self) -> usize {
        self.metrics.spill_metrics.spill_file_count.value()
    }

    /// Appending globally sorted batches to the in-progress spill file, and clears
    /// the `globally_sorted_batches` (also its memory reservation) afterwards.
    async fn consume_and_spill_append(
        &mut self,
        globally_sorted_batches: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        if globally_sorted_batches.is_empty() {
            return Ok(());
        }

        // Lazily initialize the in-progress spill file
        if self.in_progress_spill_file.is_none() {
            self.in_progress_spill_file =
                Some((self.spill_manager.create_in_progress_file("Sorting")?, 0));
        }

        Self::organize_stringview_arrays(globally_sorted_batches)?;

        debug!("Spilling sort data of ExternalSorter to disk whilst inserting");

        let batches_to_spill = std::mem::take(globally_sorted_batches);
        self.reservation.free();

        let (in_progress_file, max_record_batch_size) =
            self.in_progress_spill_file.as_mut().ok_or_else(|| {
                internal_datafusion_err!("In-progress spill file should be initialized")
            })?;

        for batch in batches_to_spill {
            in_progress_file.append_batch(&batch)?;

            *max_record_batch_size =
                (*max_record_batch_size).max(batch.get_sliced_size()?);
        }

        assert_or_internal_err!(
            globally_sorted_batches.is_empty(),
            "This function consumes globally_sorted_batches, so it should be empty after taking."
        );

        Ok(())
    }

    /// Finishes the in-progress spill file and moves it to the finished spill files.
    async fn spill_finish(&mut self) -> Result<()> {
        let (mut in_progress_file, max_record_batch_memory) =
            self.in_progress_spill_file.take().ok_or_else(|| {
                internal_datafusion_err!("Should be called after `spill_append`")
            })?;
        let spill_file = in_progress_file.finish()?;

        if let Some(spill_file) = spill_file {
            self.finished_spill_files.push(SortedSpillFile {
                file: spill_file,
                max_record_batch_memory,
            });
        }

        Ok(())
    }

    /// Reconstruct `globally_sorted_batches` to organize the payload buffers of each
    /// `StringViewArray` in sequential order by calling `gc()` on them.
    ///
    /// Note this is a workaround until <https://github.com/apache/arrow-rs/issues/7185> is
    /// available
    ///
    /// # Rationale
    /// After (merge-based) sorting, all batches will be sorted into a single run,
    /// but physically this sorted run is chunked into many small batches. For
    /// `StringViewArray`s inside each sorted run, their inner buffers are not
    /// re-constructed by default, leading to non-sequential payload locations
    /// (permutated by `interleave()` Arrow kernel). A single payload buffer might
    /// be shared by multiple `RecordBatch`es.
    /// When writing each batch to disk, the writer has to write all referenced buffers,
    /// because they have to be read back one by one to reduce memory usage. This
    /// causes extra disk reads and writes, and potentially execution failure.
    ///
    /// # Example
    /// Before sorting:
    /// batch1 -> buffer1
    /// batch2 -> buffer2
    ///
    /// sorted_batch1 -> buffer1
    ///               -> buffer2
    /// sorted_batch2 -> buffer1
    ///               -> buffer2
    ///
    /// Then when spilling each batch, the writer has to write all referenced buffers
    /// repeatedly.
    fn organize_stringview_arrays(
        globally_sorted_batches: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        let mut organized_batches = Vec::with_capacity(globally_sorted_batches.len());

        for batch in globally_sorted_batches.drain(..) {
            let mut new_columns: Vec<Arc<dyn Array>> =
                Vec::with_capacity(batch.num_columns());

            let mut arr_mutated = false;
            for array in batch.columns() {
                if let Some(string_view_array) =
                    array.as_any().downcast_ref::<StringViewArray>()
                {
                    let new_array = string_view_array.gc();
                    new_columns.push(Arc::new(new_array));
                    arr_mutated = true;
                } else {
                    new_columns.push(Arc::clone(array));
                }
            }

            let organized_batch = if arr_mutated {
                RecordBatch::try_new(batch.schema(), new_columns)?
            } else {
                batch
            };

            organized_batches.push(organized_batch);
        }

        *globally_sorted_batches = organized_batches;

        Ok(())
    }

    /// Sorts the in-memory batches and merges them into a single sorted run, then writes
    /// the result to spill files.
    async fn sort_and_spill_in_mem_batches(&mut self) -> Result<()> {
        assert_or_internal_err!(
            !self.in_mem_batches.is_empty(),
            "in_mem_batches must not be empty when attempting to sort and spill"
        );

        // Release the memory reserved for merge back to the pool so
        // there is some left when `in_mem_sort_stream` requests an
        // allocation. At the end of this function, memory will be
        // reserved again for the next spill.
        self.merge_reservation.free();

        let mut sorted_stream =
            self.in_mem_sort_stream(self.metrics.baseline.intermediate())?;
        // After `in_mem_sort_stream()` is constructed, all `in_mem_batches` is taken
        // to construct a globally sorted stream.
        assert_or_internal_err!(
            self.in_mem_batches.is_empty(),
            "in_mem_batches should be empty after constructing sorted stream"
        );
        // 'global' here refers to all buffered batches when the memory limit is
        // reached. This variable will buffer the sorted batches after
        // sort-preserving merge and incrementally append to spill files.
        let mut globally_sorted_batches: Vec<RecordBatch> = vec![];

        while let Some(batch) = sorted_stream.next().await {
            let batch = batch?;
            let sorted_size = get_reserved_byte_for_record_batch(&batch);
            if self.reservation.try_grow(sorted_size).is_err() {
                // Although the reservation is not enough, the batch is
                // already in memory, so it's okay to combine it with previously
                // sorted batches, and spill together.
                globally_sorted_batches.push(batch);
                self.consume_and_spill_append(&mut globally_sorted_batches)
                    .await?; // reservation is freed in spill()
            } else {
                globally_sorted_batches.push(batch);
            }
        }

        // Drop early to free up memory reserved by the sorted stream, otherwise the
        // upcoming `self.reserve_memory_for_merge()` may fail due to insufficient memory.
        drop(sorted_stream);

        self.consume_and_spill_append(&mut globally_sorted_batches)
            .await?;
        self.spill_finish().await?;

        // Sanity check after spilling
        let buffers_cleared_property =
            self.in_mem_batches.is_empty() && globally_sorted_batches.is_empty();
        assert_or_internal_err!(
            buffers_cleared_property,
            "in_mem_batches and globally_sorted_batches should be cleared before"
        );

        // Reserve headroom for next sort/merge
        self.reserve_memory_for_merge()?;

        Ok(())
    }

    /// Consumes in_mem_batches returning a sorted stream of
    /// batches. This proceeds in one of two ways:
    ///
    /// # Small Datasets
    ///
    /// For "smaller" datasets, the data is first concatenated into a
    /// single batch and then sorted. This is often faster than
    /// sorting and then merging.
    ///
    /// ```text
    ///        ┌─────┐
    ///        │  2  │
    ///        │  3  │
    ///        │  1  │─ ─ ─ ─ ┐            ┌─────┐
    ///        │  4  │                     │  2  │
    ///        │  2  │        │            │  3  │
    ///        └─────┘                     │  1  │             sorted output
    ///        ┌─────┐        ▼            │  4  │                stream
    ///        │  1  │                     │  2  │
    ///        │  4  │─ ─▶ concat ─ ─ ─ ─ ▶│  1  │─ ─ ▶  sort  ─ ─ ─ ─ ─▶
    ///        │  1  │                     │  4  │
    ///        └─────┘        ▲            │  1  │
    ///          ...          │            │ ... │
    ///                                    │  4  │
    ///        ┌─────┐        │            │  3  │
    ///        │  4  │                     └─────┘
    ///        │  3  │─ ─ ─ ─ ┘
    ///        └─────┘
    ///     in_mem_batches
    /// ```
    ///
    /// # Larger datasets
    ///
    /// For larger datasets, the batches are first sorted individually
    /// and then merged together.
    ///
    /// ```text
    ///      ┌─────┐                ┌─────┐
    ///      │  2  │                │  1  │
    ///      │  3  │                │  2  │
    ///      │  1  │─ ─▶  sort  ─ ─▶│  2  │─ ─ ─ ─ ─ ┐
    ///      │  4  │                │  3  │
    ///      │  2  │                │  4  │          │
    ///      └─────┘                └─────┘               sorted output
    ///      ┌─────┐                ┌─────┐          ▼       stream
    ///      │  1  │                │  1  │
    ///      │  4  │─ ▶  sort  ─ ─ ▶│  1  ├ ─ ─ ▶ merge  ─ ─ ─ ─▶
    ///      │  1  │                │  4  │
    ///      └─────┘                └─────┘          ▲
    ///        ...       ...         ...             │
    ///
    ///      ┌─────┐                ┌─────┐          │
    ///      │  4  │                │  3  │
    ///      │  3  │─ ▶  sort  ─ ─ ▶│  4  │─ ─ ─ ─ ─ ┘
    ///      └─────┘                └─────┘
    ///
    ///   in_mem_batches
    /// ```
    fn in_mem_sort_stream(
        &mut self,
        metrics: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        if self.in_mem_batches.is_empty() {
            return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                &self.schema,
            ))));
        }

        // The elapsed compute timer is updated when the value is dropped.
        // There is no need for an explicit call to drop.
        let elapsed_compute = metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        // Please pay attention that any operation inside of `in_mem_sort_stream` will
        // not perform any memory reservation. This is for avoiding the need of handling
        // reservation failure and spilling in the middle of the sort/merge. The memory
        // space for batches produced by the resulting stream will be reserved by the
        // consumer of the stream.

        if self.in_mem_batches.len() == 1 {
            let batch = self.in_mem_batches.swap_remove(0);
            let reservation = self.reservation.take();
            return self.sort_batch_stream(batch, metrics, reservation, true);
        }

        // If less than sort_in_place_threshold_bytes, concatenate and sort in place
        if self.reservation.size() < self.sort_in_place_threshold_bytes {
            // Concatenate memory batches together and sort
            let batch = concat_batches(&self.schema, &self.in_mem_batches)?;
            self.in_mem_batches.clear();
            self.reservation
                .try_resize(get_reserved_byte_for_record_batch(&batch))
                .map_err(Self::err_with_oom_context)?;
            let reservation = self.reservation.take();
            return self.sort_batch_stream(batch, metrics, reservation, true);
        }

        let streams = std::mem::take(&mut self.in_mem_batches)
            .into_iter()
            .map(|batch| {
                let metrics = self.metrics.baseline.intermediate();
                let reservation = self
                    .reservation
                    .split(get_reserved_byte_for_record_batch(&batch));
                let input = self.sort_batch_stream(
                    batch,
                    metrics,
                    reservation,
                    // Passing false as `StreamingMergeBuilder` will split the
                    // stream into batches of `self.batch_size` rows.
                    false,
                )?;
                Ok(spawn_buffered(input, 1))
            })
            .collect::<Result<_>>()?;

        StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(Arc::clone(&self.schema))
            .with_expressions(&self.expr.clone())
            .with_metrics(metrics)
            .with_batch_size(self.batch_size)
            .with_fetch(None)
            .with_reservation(self.merge_reservation.new_empty())
            .build()
    }

    /// Sorts a single `RecordBatch` into a single stream.
    ///
    /// `reservation` accounts for the memory used by this batch and
    /// is released when the sort is complete
    ///
    /// passing `split` true will return a [`BatchSplitStream`] where each batch maximum row count
    /// will be `self.batch_size`.
    /// If `split` is false, the stream will return a single batch
    fn sort_batch_stream(
        &self,
        batch: RecordBatch,
        metrics: BaselineMetrics,
        reservation: MemoryReservation,
        mut split: bool,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(
            get_reserved_byte_for_record_batch(&batch),
            reservation.size()
        );

        split = split && batch.num_rows() > self.batch_size;

        let schema = batch.schema();

        let expressions = self.expr.clone();
        let stream = futures::stream::once(async move {
            let _timer = metrics.elapsed_compute().timer();

            let sorted = sort_batch(&batch, &expressions, None)?;

            (&sorted).record_output(&metrics);
            drop(batch);
            drop(reservation);
            Ok(sorted)
        });

        let mut output: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        if split {
            output = Box::pin(BatchSplitStream::new(
                output,
                self.batch_size,
                self.metrics.split_metrics.clone(),
            ));
        }

        Ok(output)
    }

    /// If this sort may spill, pre-allocates
    /// `sort_spill_reservation_bytes` of memory to guarantee memory
    /// left for the in memory sort/merge.
    fn reserve_memory_for_merge(&mut self) -> Result<()> {
        // Reserve headroom for next merge sort
        if self.runtime.disk_manager.tmp_files_enabled() {
            let size = self.sort_spill_reservation_bytes;
            if self.merge_reservation.size() != size {
                self.merge_reservation
                    .try_resize(size)
                    .map_err(Self::err_with_oom_context)?;
            }
        }

        Ok(())
    }

    /// Reserves memory to be able to accommodate the given batch.
    /// If memory is scarce, tries to spill current in-memory batches to disk first.
    async fn reserve_memory_for_batch_and_maybe_spill(
        &mut self,
        input: &RecordBatch,
    ) -> Result<()> {
        let size = get_reserved_byte_for_record_batch(input);

        match self.reservation.try_grow(size) {
            Ok(_) => Ok(()),
            Err(e) => {
                if self.in_mem_batches.is_empty() {
                    return Err(Self::err_with_oom_context(e));
                }

                // Spill and try again.
                self.sort_and_spill_in_mem_batches().await?;
                self.reservation
                    .try_grow(size)
                    .map_err(Self::err_with_oom_context)
            }
        }
    }

    /// Wraps the error with a context message suggesting settings to tweak.
    /// This is meant to be used with DataFusionError::ResourcesExhausted only.
    fn err_with_oom_context(e: DataFusionError) -> DataFusionError {
        match e {
            DataFusionError::ResourcesExhausted(_) => e.context(
                "Not enough memory to continue external sort. \
                    Consider increasing the memory limit, or decreasing sort_spill_reservation_bytes"
            ),
            // This is not an OOM error, so just return it as is.
            _ => e,
        }
    }
}

/// Estimate how much memory is needed to sort a `RecordBatch`.
///
/// This is used to pre-reserve memory for the sort/merge. The sort/merge process involves
/// creating sorted copies of sorted columns in record batches for speeding up comparison
/// in sorting and merging. The sorted copies are in either row format or array format.
/// Please refer to cursor.rs and stream.rs for more details. No matter what format the
/// sorted copies are, they will use more memory than the original record batch.
pub(crate) fn get_reserved_byte_for_record_batch_size(record_batch_size: usize) -> usize {
    // 2x may not be enough for some cases, but it's a good start.
    // If 2x is not enough, user can set a larger value for `sort_spill_reservation_bytes`
    // to compensate for the extra memory needed.
    record_batch_size * 2
}

/// Estimate how much memory is needed to sort a `RecordBatch`.
fn get_reserved_byte_for_record_batch(batch: &RecordBatch) -> usize {
    get_reserved_byte_for_record_batch_size(get_record_batch_memory_size(batch))
}

impl Debug for ExternalSorter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_rows", &self.spilled_rows())
            .field("spill_count", &self.spill_count())
            .finish()
    }
}

pub fn sort_batch(
    batch: &RecordBatch,
    expressions: &LexOrdering,
    fetch: Option<usize>,
) -> Result<RecordBatch> {
    let sort_columns = expressions
        .iter()
        .map(|expr| expr.evaluate_to_sort_column(batch))
        .collect::<Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, fetch)?;
    let mut columns = take_arrays(batch.columns(), &indices, None)?;

    // The columns may be larger than the unsorted columns in `batch` especially for variable length
    // data types due to exponential growth when building the sort columns. We shrink the columns
    // to prevent memory reservation failures, as well as excessive memory allocation when running
    // merges in `SortPreservingMergeStream`.
    columns.iter_mut().for_each(|c| {
        c.shrink_to_fit();
    });

    let options = RecordBatchOptions::new().with_row_count(Some(indices.len()));
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &options,
    )?)
}

/// Sort execution plan.
///
/// Support sorting datasets that are larger than the memory allotted
/// by the memory manager, by spilling to disk.
#[derive(Debug, Clone)]
pub struct SortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: LexOrdering,
    /// Containing all metrics set created during sort
    metrics_set: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan. If false, the input partitions
    /// will be sorted and merged into a single output partition.
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
    /// Normalized common sort prefix between the input and the sort expressions (only used with fetch)
    common_sort_prefix: Vec<PhysicalSortExpr>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
    /// Filter matching the state of the sort for dynamic filter pushdown.
    /// If `fetch` is `Some`, this will also be set and a TopK operator may be used.
    /// If `fetch` is `None`, this will be `None`.
    filter: Option<Arc<RwLock<TopKDynamicFilters>>>,
}

impl SortExec {
    /// Create a new sort execution plan that produces a single,
    /// sorted output partition.
    pub fn new(expr: LexOrdering, input: Arc<dyn ExecutionPlan>) -> Self {
        let preserve_partitioning = false;
        let (cache, sort_prefix) =
            Self::compute_properties(&input, expr.clone(), preserve_partitioning)
                .unwrap();
        Self {
            expr,
            input,
            metrics_set: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
            fetch: None,
            common_sort_prefix: sort_prefix,
            cache,
            filter: None,
        }
    }

    /// Whether this `SortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this sort exec
    ///
    /// If `preserve_partitioning` is true, sorts each partition
    /// individually, producing one sorted stream for each input partition.
    ///
    /// If `preserve_partitioning` is false, sorts and merges all
    /// input partitions producing a single, sorted partition.
    pub fn with_preserve_partitioning(mut self, preserve_partitioning: bool) -> Self {
        self.preserve_partitioning = preserve_partitioning;
        self.cache = self
            .cache
            .with_partitioning(Self::output_partitioning_helper(
                &self.input,
                self.preserve_partitioning,
            ));
        self
    }

    /// Add or reset `self.filter` to a new `TopKDynamicFilters`.
    fn create_filter(&self) -> Arc<RwLock<TopKDynamicFilters>> {
        let children = self
            .expr
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
            DynamicFilterPhysicalExpr::new(children, lit(true)),
        ))))
    }

    fn cloned(&self) -> Self {
        SortExec {
            input: Arc::clone(&self.input),
            expr: self.expr.clone(),
            metrics_set: self.metrics_set.clone(),
            preserve_partitioning: self.preserve_partitioning,
            common_sort_prefix: self.common_sort_prefix.clone(),
            fetch: self.fetch,
            cache: self.cache.clone(),
            filter: self.filter.clone(),
        }
    }

    /// Modify how many rows to include in the result
    ///
    /// If None, then all rows will be returned, in sorted order.
    /// If Some, then only the top `fetch` rows will be returned.
    /// This can reduce the memory pressure required by the sort
    /// operation since rows that are not going to be included
    /// can be dropped.
    pub fn with_fetch(&self, fetch: Option<usize>) -> Self {
        let mut cache = self.cache.clone();
        // If the SortExec can emit incrementally (that means the sort requirements
        // and properties of the input match), the SortExec can generate its result
        // without scanning the entire input when a fetch value exists.
        let is_pipeline_friendly = matches!(
            self.cache.emission_type,
            EmissionType::Incremental | EmissionType::Both
        );
        if fetch.is_some() && is_pipeline_friendly {
            cache = cache.with_boundedness(Boundedness::Bounded);
        }
        let filter = fetch.is_some().then(|| {
            // If we already have a filter, keep it. Otherwise, create a new one.
            self.filter.clone().unwrap_or_else(|| self.create_filter())
        });
        let mut new_sort = self.cloned();
        new_sort.fetch = fetch;
        new_sort.cache = cache;
        new_sort.filter = filter;
        new_sort
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }

    /// If `Some(fetch)`, limits output to only the first "fetch" items
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn output_partitioning_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Partitioning {
        // Get output partitioning:
        if preserve_partitioning {
            input.output_partitioning().clone()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    /// It also returns the common sort prefix between the input and the sort expressions.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        preserve_partitioning: bool,
    ) -> Result<(PlanProperties, Vec<PhysicalSortExpr>)> {
        let (sort_prefix, sort_satisfied) = input
            .equivalence_properties()
            .extract_common_sort_prefix(sort_exprs.clone())?;

        // The emission type depends on whether the input is already sorted:
        // - If already fully sorted, we can emit results in the same way as the input
        // - If not sorted, we must wait until all data is processed to emit results (Final)
        let emission_type = if sort_satisfied {
            input.pipeline_behavior()
        } else {
            EmissionType::Final
        };

        // The boundedness depends on whether the input is already sorted:
        // - If already sorted, we have the same property as the input
        // - If not sorted and input is unbounded, we require infinite memory and generates
        //   unbounded data (not practical).
        // - If not sorted and input is bounded, then the SortExec is bounded, too.
        let boundedness = if sort_satisfied {
            input.boundedness()
        } else {
            match input.boundedness() {
                Boundedness::Unbounded { .. } => Boundedness::Unbounded {
                    requires_infinite_memory: true,
                },
                bounded => bounded,
            }
        };

        // Calculate equivalence properties; i.e. reset the ordering equivalence
        // class with the new ordering:
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(sort_exprs)?;

        // Get output partitioning:
        let output_partitioning =
            Self::output_partitioning_helper(input, preserve_partitioning);

        Ok((
            PlanProperties::new(
                eq_properties,
                output_partitioning,
                emission_type,
                boundedness,
            ),
            sort_prefix,
        ))
    }
}

impl DisplayAs for SortExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let preserve_partitioning = self.preserve_partitioning;
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "SortExec: TopK(fetch={fetch}), expr=[{}], preserve_partitioning=[{preserve_partitioning}]", self.expr)?;
                        if let Some(filter) = &self.filter {
                            if let Ok(current) = filter.read().expr().current() {
                                if !current.eq(&lit(true)) {
                                    write!(f, ", filter=[{current}]")?;
                                }
                            }
                        }
                        if !self.common_sort_prefix.is_empty() {
                            write!(f, ", sort_prefix=[")?;
                            let mut first = true;
                            for sort_expr in &self.common_sort_prefix {
                                if first {
                                    first = false;
                                } else {
                                    write!(f, ", ")?;
                                }
                                write!(f, "{sort_expr}")?;
                            }
                            write!(f, "]")
                        } else {
                            Ok(())
                        }
                    }
                    None => write!(f, "SortExec: expr=[{}], preserve_partitioning=[{preserve_partitioning}]", self.expr),
                }
            }
            DisplayFormatType::TreeRender => match self.fetch {
                Some(fetch) => {
                    writeln!(f, "{}", self.expr)?;
                    writeln!(f, "limit={fetch}")
                }
                None => {
                    writeln!(f, "{}", self.expr)
                }
            },
        }
    }
}

impl ExecutionPlan for SortExec {
    fn name(&self) -> &'static str {
        match self.fetch {
            Some(_) => "SortExec(TopK)",
            None => "SortExec",
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.preserve_partitioning {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            // global sort
            // TODO support RangePartition and OrderedDistribution
            vec![Distribution::SinglePartition]
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut new_sort = self.cloned();
        assert!(
            children.len() == 1,
            "SortExec should have exactly one child"
        );
        new_sort.input = Arc::clone(&children[0]);
        // Recompute the properties based on the new input since they may have changed
        let (cache, sort_prefix) = Self::compute_properties(
            &new_sort.input,
            new_sort.expr.clone(),
            new_sort.preserve_partitioning,
        )?;
        new_sort.cache = cache;
        new_sort.common_sort_prefix = sort_prefix;

        Ok(Arc::new(new_sort))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children().into_iter().cloned().collect();
        let new_sort = self.with_new_children(children)?;
        let mut new_sort = new_sort
            .as_any()
            .downcast_ref::<SortExec>()
            .expect("cloned 1 lines above this line, we know the type")
            .clone();
        // Our dynamic filter and execution metrics are the state we need to reset.
        new_sort.filter = Some(new_sort.create_filter());
        new_sort.metrics_set = ExecutionPlanMetricsSet::new();

        Ok(Arc::new(new_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let mut input = self.input.execute(partition, Arc::clone(&context))?;

        let execution_options = &context.session_config().options().execution;

        trace!("End SortExec's input.execute for partition: {partition}");

        let sort_satisfied = self
            .input
            .equivalence_properties()
            .ordering_satisfy(self.expr.clone())?;

        match (sort_satisfied, self.fetch.as_ref()) {
            (true, Some(fetch)) => Ok(Box::pin(LimitStream::new(
                input,
                0,
                Some(*fetch),
                BaselineMetrics::new(&self.metrics_set, partition),
            ))),
            (true, None) => Ok(input),
            (false, Some(fetch)) => {
                let filter = self.filter.clone();
                let mut topk = TopK::try_new(
                    partition,
                    input.schema(),
                    self.common_sort_prefix.clone(),
                    self.expr.clone(),
                    *fetch,
                    context.session_config().batch_size(),
                    context.runtime_env(),
                    &self.metrics_set,
                    Arc::clone(&unwrap_or_internal_err!(filter)),
                )?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema(),
                    futures::stream::once(async move {
                        while let Some(batch) = input.next().await {
                            let batch = batch?;
                            topk.insert_batch(batch)?;
                            if topk.finished {
                                break;
                            }
                        }
                        topk.emit()
                    })
                    .try_flatten(),
                )))
            }
            (false, None) => {
                let mut sorter = ExternalSorter::new(
                    partition,
                    input.schema(),
                    self.expr.clone(),
                    context.session_config().batch_size(),
                    execution_options.sort_spill_reservation_bytes,
                    execution_options.sort_in_place_threshold_bytes,
                    context.session_config().spill_compression(),
                    &self.metrics_set,
                    context.runtime_env(),
                )?;
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    self.schema(),
                    futures::stream::once(async move {
                        while let Some(batch) = input.next().await {
                            let batch = batch?;
                            sorter.insert_batch(batch).await?;
                        }
                        sorter.sort().await
                    })
                    .try_flatten(),
                )))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if !self.preserve_partitioning() {
            return self
                .input
                .partition_statistics(None)?
                .with_fetch(self.fetch, 0, 1);
        }
        self.input
            .partition_statistics(partition)?
            .with_fetch(self.fetch, 0, 1)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(SortExec::with_fetch(self, limit)))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        if self.fetch.is_none() {
            CardinalityEffect::Equal
        } else {
            CardinalityEffect::LowerEqual
        }
    }

    /// Tries to swap the projection with its input [`SortExec`]. If it can be done,
    /// it returns the new swapped version having the [`SortExec`] as the top plan.
    /// Otherwise, it returns None.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        let Some(updated_exprs) = update_ordering(self.expr.clone(), projection.expr())?
        else {
            return Ok(None);
        };

        Ok(Some(Arc::new(
            SortExec::new(updated_exprs, make_with_child(projection, self.input())?)
                .with_fetch(self.fetch())
                .with_preserve_partitioning(self.preserve_partitioning()),
        )))
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &datafusion_common::config::ConfigOptions,
    ) -> Result<FilterDescription> {
        if !matches!(phase, FilterPushdownPhase::Post) {
            return FilterDescription::from_children(parent_filters, &self.children());
        }

        let mut child =
            ChildFilterDescription::from_child(&parent_filters, self.input())?;

        if let Some(filter) = &self.filter {
            if config.optimizer.enable_topk_dynamic_filter_pushdown {
                child = child.with_self_filter(filter.read().expr());
            }
        }

        Ok(FilterDescription::new().with_child(child))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;
    use crate::coalesce_partitions::CoalescePartitionsExec;
    use crate::collect;
    use crate::execution_plan::Boundedness;
    use crate::expressions::col;
    use crate::test;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::TestMemoryExec;
    use crate::test::{assert_is_pending, make_partition};

    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_common::cast::as_primitive_array;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{DataFusionError, Result, ScalarValue};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::RecordBatchStream;
    use datafusion_physical_expr::expressions::{Column, Literal};
    use datafusion_physical_expr::EquivalenceProperties;

    use futures::{FutureExt, Stream};
    use insta::assert_snapshot;

    #[derive(Debug, Clone)]
    pub struct SortedUnboundedExec {
        schema: Schema,
        batch_size: u64,
        cache: PlanProperties,
    }

    impl DisplayAs for SortedUnboundedExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default
                | DisplayFormatType::Verbose
                | DisplayFormatType::TreeRender => write!(f, "UnboundableExec",).unwrap(),
            }
            Ok(())
        }
    }

    impl SortedUnboundedExec {
        fn compute_properties(schema: SchemaRef) -> PlanProperties {
            let mut eq_properties = EquivalenceProperties::new(schema);
            eq_properties.add_ordering([PhysicalSortExpr::new_default(Arc::new(
                Column::new("c1", 0),
            ))]);
            PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )
        }
    }

    impl ExecutionPlan for SortedUnboundedExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            Ok(Box::pin(SortedUnboundedStream {
                schema: Arc::new(self.schema.clone()),
                batch_size: self.batch_size,
                offset: 0,
            }))
        }
    }

    #[derive(Debug)]
    pub struct SortedUnboundedStream {
        schema: SchemaRef,
        batch_size: u64,
        offset: u64,
    }

    impl Stream for SortedUnboundedStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            let batch = SortedUnboundedStream::create_record_batch(
                Arc::clone(&self.schema),
                self.offset,
                self.batch_size,
            );
            self.offset += self.batch_size;
            Poll::Ready(Some(Ok(batch)))
        }
    }

    impl RecordBatchStream for SortedUnboundedStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl SortedUnboundedStream {
        fn create_record_batch(
            schema: SchemaRef,
            offset: u64,
            batch_size: u64,
        ) -> RecordBatch {
            let values = (0..batch_size).map(|i| offset + i).collect::<Vec<_>>();
            let array = UInt64Array::from(values);
            let array_ref: ArrayRef = Arc::new(array);
            RecordBatch::try_new(schema, vec![array_ref]).unwrap()
        }
    }

    #[tokio::test]
    async fn test_in_mem_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let partitions = 4;
        let csv = test::scan_partitioned(partitions);
        let schema = csv.schema();

        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            Arc::new(CoalescePartitionsExec::new(csv)),
        ));

        let result = collect(sort_exec, Arc::clone(&task_ctx)).await?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 400);
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_spill() -> Result<()> {
        // trigger spill w/ 100 batches
        let session_config = SessionConfig::new();
        let sort_spill_reservation_bytes = session_config
            .options()
            .execution
            .sort_spill_reservation_bytes;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(sort_spill_reservation_bytes + 12288, 1.0)
            .build_arc()?;
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime),
        );

        // The input has 100 partitions, each partition has a batch containing 100 rows.
        // Each row has a single Int32 column with values 0..100. The total size of the
        // input is roughly 40000 bytes.
        let partitions = 100;
        let input = test::scan_partitioned(partitions);
        let schema = input.schema();

        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            Arc::new(CoalescePartitionsExec::new(input)),
        ));

        let result = collect(
            Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>,
            Arc::clone(&task_ctx),
        )
        .await?;

        assert_eq!(result.len(), 2);

        // Now, validate metrics
        let metrics = sort_exec.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 10000);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let spill_count = metrics.spill_count().unwrap();
        let spilled_rows = metrics.spilled_rows().unwrap();
        let spilled_bytes = metrics.spilled_bytes().unwrap();
        // Processing 40000 bytes of data using 12288 bytes of memory requires 3 spills
        // unless we do something really clever. It will spill roughly 9000+ rows and 36000
        // bytes. We leave a little wiggle room for the actual numbers.
        assert!((3..=10).contains(&spill_count));
        assert!((9000..=10000).contains(&spilled_rows));
        assert!((38000..=44000).contains(&spilled_bytes));

        let columns = result[0].columns();

        let i = as_primitive_array::<Int32Type>(&columns[0])?;
        assert_eq!(i.value(0), 0);
        assert_eq!(i.value(i.len() - 1), 81);
        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_reservation_error() -> Result<()> {
        // Pick a memory limit and sort_spill_reservation that make the first batch reservation fail.
        // These values assume that the ExternalSorter will reserve 800 bytes for the first batch.
        let expected_batch_reservation = 800;
        let merge_reservation: usize = 0; // Set to 0 for simplicity
        let memory_limit: usize = expected_batch_reservation + merge_reservation - 1; // Just short of what we need

        let session_config =
            SessionConfig::new().with_sort_spill_reservation_bytes(merge_reservation);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .build_arc()?;
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime),
        );

        let plan = test::scan_partitioned(1);

        // Read the first record batch to assert that our memory limit and sort_spill_reservation
        // settings trigger the test scenario.
        {
            let mut stream = plan.execute(0, Arc::clone(&task_ctx))?;
            let first_batch = stream.next().await.unwrap()?;
            let batch_reservation = get_reserved_byte_for_record_batch(&first_batch);

            assert_eq!(batch_reservation, expected_batch_reservation);
            assert!(memory_limit < (merge_reservation + batch_reservation));
        }

        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr::new_default(col("i", &plan.schema())?)].into(),
            plan,
        ));

        let result = collect(Arc::clone(&sort_exec) as _, Arc::clone(&task_ctx)).await;

        let err = result.unwrap_err();
        assert!(
            matches!(err, DataFusionError::Context(..)),
            "Assertion failed: expected a Context error, but got: {err:?}"
        );

        // Assert that the context error is wrapping a resources exhausted error.
        assert!(
            matches!(err.find_root(), DataFusionError::ResourcesExhausted(_)),
            "Assertion failed: expected a ResourcesExhausted error, but got: {err:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_spill_utf8_strings() -> Result<()> {
        let session_config = SessionConfig::new()
            .with_batch_size(100)
            .with_sort_in_place_threshold_bytes(20 * 1024)
            .with_sort_spill_reservation_bytes(100 * 1024);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(500 * 1024, 1.0)
            .build_arc()?;
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(session_config)
                .with_runtime(runtime),
        );

        // The input has 200 partitions, each partition has a batch containing 100 rows.
        // Each row has a single Utf8 column, the Utf8 string values are roughly 42 bytes.
        // The total size of the input is roughly 8.4 KB.
        let input = test::scan_partitioned_utf8(200);
        let schema = input.schema();

        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr {
                expr: col("i", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            Arc::new(CoalescePartitionsExec::new(input)),
        ));

        let result = collect(Arc::clone(&sort_exec) as _, Arc::clone(&task_ctx)).await?;

        let num_rows = result.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(num_rows, 20000);

        // Now, validate metrics
        let metrics = sort_exec.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 20000);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let spill_count = metrics.spill_count().unwrap();
        let spilled_rows = metrics.spilled_rows().unwrap();
        let spilled_bytes = metrics.spilled_bytes().unwrap();

        // This test case is processing 840KB of data using 400KB of memory. Note
        // that buffered batches can't be dropped until all sorted batches are
        // generated, so we can only buffer `sort_spill_reservation_bytes` of sorted
        // batches.
        // The number of spills is roughly calculated as:
        //  `number_of_batches / (sort_spill_reservation_bytes / batch_size)`

        // If this assertion fail with large spill count, make sure the following
        // case does not happen:
        // During external sorting, one sorted run should be spilled to disk in a
        // single file, due to memory limit we might need to append to the file
        // multiple times to spill all the data. Make sure we're not writing each
        // appending as a separate file.
        assert!((4..=8).contains(&spill_count));
        assert!((15000..=20000).contains(&spilled_rows));
        assert!((900000..=1000000).contains(&spilled_bytes));

        // Verify that the result is sorted
        let concated_result = concat_batches(&schema, &result)?;
        let columns = concated_result.columns();
        let string_array = as_string_array(&columns[0]);
        for i in 0..string_array.len() - 1 {
            assert!(string_array.value(i) <= string_array.value(i + 1));
        }

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_fetch_memory_calculation() -> Result<()> {
        // This test mirrors down the size from the example above.
        let avg_batch_size = 400;
        let partitions = 4;

        // A tuple of (fetch, expect_spillage)
        let test_options = vec![
            // Since we don't have a limit (and the memory is less than the total size of
            // all the batches we are processing, we expect it to spill.
            (None, true),
            // When we have a limit however, the buffered size of batches should fit in memory
            // since it is much lower than the total size of the input batch.
            (Some(1), false),
        ];

        for (fetch, expect_spillage) in test_options {
            let session_config = SessionConfig::new();
            let sort_spill_reservation_bytes = session_config
                .options()
                .execution
                .sort_spill_reservation_bytes;

            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(
                    sort_spill_reservation_bytes + avg_batch_size * (partitions - 1),
                    1.0,
                )
                .build_arc()?;
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_runtime(runtime)
                    .with_session_config(session_config),
            );

            let csv = test::scan_partitioned(partitions);
            let schema = csv.schema();

            let sort_exec = Arc::new(
                SortExec::new(
                    [PhysicalSortExpr {
                        expr: col("i", &schema)?,
                        options: SortOptions::default(),
                    }]
                    .into(),
                    Arc::new(CoalescePartitionsExec::new(csv)),
                )
                .with_fetch(fetch),
            );

            let result =
                collect(Arc::clone(&sort_exec) as _, Arc::clone(&task_ctx)).await?;
            assert_eq!(result.len(), 1);

            let metrics = sort_exec.metrics().unwrap();
            let did_it_spill = metrics.spill_count().unwrap_or(0) > 0;
            assert_eq!(did_it_spill, expect_spillage, "with fetch: {fetch:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let field_metadata: HashMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(field_metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![3, 2, 1].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![data])?;
        let input =
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?;

        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            input,
        ));

        let result: Vec<RecordBatch> = collect(sort_exec, task_ctx).await?;

        let expected_data: ArrayRef =
            Arc::new(vec![1, 2, 3].into_iter().map(Some).collect::<UInt64Array>());
        let expected_batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![expected_data])?;

        // Data is correct
        assert_eq!(&vec![expected_batch], &result);

        // explicitly ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_mixed_types() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(3)]),
                    Some(vec![Some(1)]),
                    Some(vec![Some(6), None]),
                    Some(vec![Some(5)]),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                },
            ]
            .into(),
            TestMemoryExec::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)?,
        ));

        assert_eq!(DataType::Int32, *sort_exec.schema().field(0).data_type());
        assert_eq!(
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            *sort_exec.schema().field(1).data_type()
        );

        let result: Vec<RecordBatch> =
            collect(Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>, task_ctx).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert_eq!(result.len(), 1);

        let expected = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![None, Some(1), Some(2), Some(2)])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    Some(vec![Some(6), None]),
                    Some(vec![Some(5)]),
                    Some(vec![Some(3)]),
                ])),
            ],
        )?;

        assert_eq!(expected, result[0]);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    None,
                    None,
                    Some(f32::NAN),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(200.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(SortExec::new(
            [
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ]
            .into(),
            TestMemoryExec::try_new_exec(&[vec![batch]], schema, None)?,
        ));

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> =
            collect(Arc::clone(&sort_exec) as Arc<dyn ExecutionPlan>, task_ctx).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0])?;
        let b = as_primitive_array::<Float64Type>(&columns[1])?;

        // convert result to strings to allow comparing to expected result containing NaN
        let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
            .map(|i| {
                let aval = if a.is_valid(i) {
                    Some(a.value(i).to_string())
                } else {
                    None
                };
                let bval = if b.is_valid(i) {
                    Some(b.value(i).to_string())
                } else {
                    None
                };
                (aval, bval)
            })
            .collect();

        let expected: Vec<(Option<String>, Option<String>)> = vec![
            (None, Some("10".to_owned())),
            (None, Some("20".to_owned())),
            (Some("NaN".to_owned()), Some("100".to_owned())),
            (Some("NaN".to_owned()), Some("200".to_owned())),
            (Some("3".to_owned()), Some("NaN".to_owned())),
            (Some("2".to_owned()), None),
            (Some("1".to_owned()), Some("NaN".to_owned())),
            (Some("1".to_owned()), None),
        ];

        assert_eq!(expected, result);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(SortExec::new(
            [PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }]
            .into(),
            blocking_exec,
        ));

        let fut = collect(sort_exec, Arc::clone(&task_ctx));
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            task_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[test]
    fn test_empty_sort_batch() {
        let schema = Arc::new(Schema::empty());
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let batch =
            RecordBatch::try_new_with_options(Arc::clone(&schema), vec![], &options)
                .unwrap();

        let expressions = [PhysicalSortExpr {
            expr: Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
            options: SortOptions::default(),
        }]
        .into();

        let result = sort_batch(&batch, &expressions, None).unwrap();
        assert_eq!(result.num_rows(), 1);
    }

    #[tokio::test]
    async fn topk_unbounded_source() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Schema::new(vec![Field::new("c1", DataType::UInt64, false)]);
        let source = SortedUnboundedExec {
            schema: schema.clone(),
            batch_size: 2,
            cache: SortedUnboundedExec::compute_properties(Arc::new(schema.clone())),
        };
        let mut plan = SortExec::new(
            [PhysicalSortExpr::new_default(Arc::new(Column::new(
                "c1", 0,
            )))]
            .into(),
            Arc::new(source),
        );
        plan = plan.with_fetch(Some(9));

        let batches = collect(Arc::new(plan), task_ctx).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
            +----+
            | c1 |
            +----+
            | 0  |
            | 1  |
            | 2  |
            | 3  |
            | 4  |
            | 5  |
            | 6  |
            | 7  |
            | 8  |
            +----+
            "#);
        Ok(())
    }

    #[tokio::test]
    async fn should_return_stream_with_batches_in_the_requested_size() -> Result<()> {
        let batch_size = 100;

        let create_task_ctx = |_: &[RecordBatch]| {
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_sort_in_place_threshold_bytes(usize::MAX),
            )
        };

        // Smaller than batch size and require more than a single batch to get the requested batch size
        test_sort_output_batch_size(10, batch_size / 4, create_task_ctx).await?;

        // Not evenly divisible by batch size
        test_sort_output_batch_size(10, batch_size + 7, create_task_ctx).await?;

        // Evenly divisible by batch size and is larger than 2 output batches
        test_sort_output_batch_size(10, batch_size * 3, create_task_ctx).await?;

        Ok(())
    }

    #[tokio::test]
    async fn should_return_stream_with_batches_in_the_requested_size_when_sorting_in_place(
    ) -> Result<()> {
        let batch_size = 100;

        let create_task_ctx = |_: &[RecordBatch]| {
            TaskContext::default().with_session_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_sort_in_place_threshold_bytes(usize::MAX - 1),
            )
        };

        // Smaller than batch size and require more than a single batch to get the requested batch size
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size / 4, create_task_ctx).await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        // Not evenly divisible by batch size
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size + 7, create_task_ctx).await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        // Evenly divisible by batch size and is larger than 2 output batches
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size * 3, create_task_ctx).await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_return_stream_with_batches_in_the_requested_size_when_having_a_single_batch(
    ) -> Result<()> {
        let batch_size = 100;

        let create_task_ctx = |_: &[RecordBatch]| {
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size))
        };

        // Smaller than batch size and require more than a single batch to get the requested batch size
        {
            let metrics = test_sort_output_batch_size(
                // Single batch
                1,
                batch_size / 4,
                create_task_ctx,
            )
            .await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        // Not evenly divisible by batch size
        {
            let metrics = test_sort_output_batch_size(
                // Single batch
                1,
                batch_size + 7,
                create_task_ctx,
            )
            .await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        // Evenly divisible by batch size and is larger than 2 output batches
        {
            let metrics = test_sort_output_batch_size(
                // Single batch
                1,
                batch_size * 3,
                create_task_ctx,
            )
            .await?;

            assert_eq!(
                metrics.spill_count(),
                Some(0),
                "Expected no spills when sorting in place"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn should_return_stream_with_batches_in_the_requested_size_when_having_to_spill(
    ) -> Result<()> {
        let batch_size = 100;

        let create_task_ctx = |generated_batches: &[RecordBatch]| {
            let batches_memory = generated_batches
                .iter()
                .map(|b| b.get_array_memory_size())
                .sum::<usize>();

            TaskContext::default()
                .with_session_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        // To make sure there is no in place sorting
                        .with_sort_in_place_threshold_bytes(1)
                        .with_sort_spill_reservation_bytes(1),
                )
                .with_runtime(
                    RuntimeEnvBuilder::default()
                        .with_memory_limit(batches_memory, 1.0)
                        .build_arc()
                        .unwrap(),
                )
        };

        // Smaller than batch size and require more than a single batch to get the requested batch size
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size / 4, create_task_ctx).await?;

            assert_ne!(metrics.spill_count().unwrap(), 0, "expected to spill");
        }

        // Not evenly divisible by batch size
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size + 7, create_task_ctx).await?;

            assert_ne!(metrics.spill_count().unwrap(), 0, "expected to spill");
        }

        // Evenly divisible by batch size and is larger than 2 batches
        {
            let metrics =
                test_sort_output_batch_size(10, batch_size * 3, create_task_ctx).await?;

            assert_ne!(metrics.spill_count().unwrap(), 0, "expected to spill");
        }

        Ok(())
    }

    async fn test_sort_output_batch_size(
        number_of_batches: usize,
        batch_size_to_generate: usize,
        create_task_ctx: impl Fn(&[RecordBatch]) -> TaskContext,
    ) -> Result<MetricsSet> {
        let batches = (0..number_of_batches)
            .map(|_| make_partition(batch_size_to_generate as i32))
            .collect::<Vec<_>>();
        let task_ctx = create_task_ctx(batches.as_slice());

        let expected_batch_size = task_ctx.session_config().batch_size();

        let (mut output_batches, metrics) =
            run_sort_on_input(task_ctx, "i", batches).await?;

        let last_batch = output_batches.pop().unwrap();

        for batch in output_batches {
            assert_eq!(batch.num_rows(), expected_batch_size);
        }

        let mut last_expected_batch_size =
            (batch_size_to_generate * number_of_batches) % expected_batch_size;
        if last_expected_batch_size == 0 {
            last_expected_batch_size = expected_batch_size;
        }
        assert_eq!(last_batch.num_rows(), last_expected_batch_size);

        Ok(metrics)
    }

    async fn run_sort_on_input(
        task_ctx: TaskContext,
        order_by_col: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let task_ctx = Arc::new(task_ctx);

        // let task_ctx = env.
        let schema = batches[0].schema();
        let ordering: LexOrdering = [PhysicalSortExpr {
            expr: col(order_by_col, &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }]
        .into();
        let sort_exec: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
            ordering.clone(),
            TestMemoryExec::try_new_exec(std::slice::from_ref(&batches), schema, None)?,
        ));

        let sorted_batches =
            collect(Arc::clone(&sort_exec), Arc::clone(&task_ctx)).await?;

        let metrics = sort_exec.metrics().expect("sort have metrics");

        // assert output
        {
            let input_batches_concat = concat_batches(batches[0].schema_ref(), &batches)?;
            let sorted_input_batch = sort_batch(&input_batches_concat, &ordering, None)?;

            let sorted_batches_concat =
                concat_batches(sorted_batches[0].schema_ref(), &sorted_batches)?;

            assert_eq!(sorted_input_batch, sorted_batches_concat);
        }

        Ok((sorted_batches, metrics))
    }
}
