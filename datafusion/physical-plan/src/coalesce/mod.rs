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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result, assert_or_internal_err};
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};

/// Concatenate multiple [`RecordBatch`]es and apply a limit while accounting
/// for every retained batch in a [`MemoryReservation`].
///
/// # Memory accounting
///
/// The reservation always mirrors [`BatchCoalescer::size`], which covers both
/// in-progress buffers and completed batches. The baseline allocation is
/// enforced at construction, and later growth caused by [`Self::push_batch`]
/// and [`Self::finish`] surfaces as a [`DataFusionError::ResourcesExhausted`]
/// error. Internal spill-capable callers may instead flush a partial batch on
/// memory pressure. Note that completed
/// batches retained without copying (see
/// [`BatchCoalescer::with_biggest_coalesce_batch_size`]) are measured with
/// [`RecordBatch::get_array_memory_size`], which counts the full backing
/// buffers of shared or sliced arrays and may therefore over-report.
///
/// [`DataFusionError::ResourcesExhausted`]: datafusion_common::DataFusionError::ResourcesExhausted
#[derive(Debug)]
pub struct LimitedBatchCoalescer {
    /// The arrow structure that builds the output batches
    inner: BatchCoalescer,
    reservation: MemoryReservation,
    target_batch_size: usize,
    /// Complete the partial batch instead of erroring when reservation growth
    /// is rejected. The caller must immediately drain the completed batches.
    flush_on_memory_pressure: bool,
    /// Drop reusable Arrow capacity after pressure-generated batches are drained.
    reset_after_drain: bool,
    /// Total number of rows accepted so far
    total_rows: usize,
    /// Limit: maximum number of rows to fetch, `None` means fetch all rows
    fetch: Option<usize>,
    /// Indicates if the coalescer is finished
    finished: bool,
}

/// Status returned by [`LimitedBatchCoalescer::push_batch`]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushBatchStatus {
    /// The limit has **not** been reached, and more batches can be pushed
    Continue,
    /// The limit **has** been reached after processing this batch
    /// The caller should call [`LimitedBatchCoalescer::finish`]
    /// to flush any buffered rows and stop pushing more batches.
    LimitReached,
}

impl LimitedBatchCoalescer {
    /// Create a coalescer that does **not** account for the batches it
    /// retains: they are charged to a private, unbounded pool invisible to
    /// the query's [`MemoryPool`].
    ///
    /// # Arguments
    /// - `schema` - the schema of the output batches
    /// - `target_batch_size` - the minimum number of rows for each
    ///   output batch (until limit reached)
    /// - `fetch` - the maximum number of rows to fetch, `None` means fetch all rows
    #[deprecated(
        since = "55.0.0",
        note = "use `new_with_reservation` so retained batches are charged to the query's memory pool"
    )]
    pub fn new(
        schema: SchemaRef,
        target_batch_size: usize,
        fetch: Option<usize>,
    ) -> Self {
        let untracked_pool: Arc<dyn MemoryPool> =
            Arc::new(UnboundedMemoryPool::default());
        // The reservation's registration keeps the private pool alive.
        let reservation = MemoryConsumer::new("LimitedBatchCoalescer(untracked)")
            .register(&untracked_pool);
        Self::new_with_reservation(schema, target_batch_size, fetch, reservation)
            .expect("target batch size must be greater than zero")
    }

    /// Create a coalescer whose retained input and output batches are charged to
    /// an empty `reservation`.
    ///
    /// # Errors
    /// Returns an error if `target_batch_size` is zero, `reservation` is not
    /// empty, or its pool cannot accommodate the coalescer's baseline allocation.
    pub fn new_with_reservation(
        schema: SchemaRef,
        target_batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        assert_or_internal_err!(
            target_batch_size > 0,
            "LimitedBatchCoalescer: target batch size must be greater than zero"
        );
        assert_or_internal_err!(
            reservation.size() == 0,
            "LimitedBatchCoalescer: reservation must be empty"
        );
        let inner = BatchCoalescer::new(schema, target_batch_size)
            .with_biggest_coalesce_batch_size(Some(target_batch_size / 2));
        reservation.try_resize(inner.size())?;
        Ok(Self {
            inner,
            reservation,
            target_batch_size,
            flush_on_memory_pressure: false,
            reset_after_drain: false,
            total_rows: 0,
            fetch,
            finished: false,
        })
    }

    /// Flush partial batches when the pool rejects reservation growth. The
    /// caller must immediately drain and either send or spill completed batches.
    pub(crate) fn with_flush_on_memory_pressure(mut self) -> Self {
        self.flush_on_memory_pressure = true;
        self
    }

    /// Sync the reservation to the coalescer's current size without enforcing
    /// the pool limit. Used after allocation and on shrink-only paths.
    fn reconcile_reservation(&self) {
        self.reservation.resize(self.inner.size());
    }

    /// Sync the reservation to the coalescer's current size, returning an
    /// error if growth would exceed the pool limit.
    ///
    /// On error the reservation is still synced infallibly so it keeps
    /// reflecting the memory actually held while the error propagates. Unless
    /// configured to flush on pressure, the caller must treat the error as
    /// terminal and drop this coalescer to release the reservation.
    fn try_reconcile_reservation(&self) -> Result<()> {
        let actual_size = self.inner.size();
        let result = self.reservation.try_resize(actual_size);
        if result.is_err() {
            self.reservation.resize(actual_size);
        }
        result
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Pushes the next [`RecordBatch`] into the coalescer and returns its status.
    ///
    /// # Errors
    /// Returns an error if called after [`Self::finish`], if the internal push
    /// operation fails, or if the memory pool cannot accommodate the newly
    /// buffered data. Errors are terminal: the caller should stop pushing and
    /// drop the coalescer to release its reservation.
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<PushBatchStatus> {
        assert_or_internal_err!(
            !self.finished,
            "LimitedBatchCoalescer: cannot push batch after finish"
        );

        let remaining = self
            .fetch
            .map(|fetch| fetch.saturating_sub(self.total_rows));
        if remaining == Some(0) {
            return Ok(PushBatchStatus::LimitReached);
        }

        let limit_reached =
            remaining.is_some_and(|remaining| batch.num_rows() >= remaining);
        let accepted_rows = remaining
            .map(|remaining| remaining.min(batch.num_rows()))
            .unwrap_or_else(|| batch.num_rows());
        if accepted_rows == 0 {
            return Ok(PushBatchStatus::Continue);
        }

        let accepted = if accepted_rows == batch.num_rows() {
            batch
        } else {
            batch.slice(0, accepted_rows)
        };
        let result = self.inner.push_batch(accepted);
        let reconciled = self.try_reconcile_reservation();
        result?;
        match reconciled {
            Ok(()) => {}
            Err(DataFusionError::ResourcesExhausted(_))
                if self.flush_on_memory_pressure =>
            {
                let result = self.inner.finish_buffered_batch();
                self.reconcile_reservation();
                result?;
                self.reset_after_drain = true;
            }
            Err(error) => return Err(error),
        }
        self.total_rows += accepted_rows;

        Ok(if limit_reached {
            PushBatchStatus::LimitReached
        } else {
            PushBatchStatus::Continue
        })
    }

    /// Return true if there is no data buffered or completed.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Complete the current buffered batch without preventing future pushes.
    ///
    /// # Errors
    /// See [`Self::push_batch`]; errors are terminal.
    pub(crate) fn flush_buffered_batch(&mut self) -> Result<()> {
        assert_or_internal_err!(
            !self.finished,
            "LimitedBatchCoalescer: cannot flush after finish"
        );
        let result = self.inner.finish_buffered_batch();
        let reconciled = self.try_reconcile_reservation();
        result?;
        match reconciled {
            Err(DataFusionError::ResourcesExhausted(_))
                if self.flush_on_memory_pressure =>
            {
                self.reset_after_drain = true;
                Ok(())
            }
            result => result,
        }
    }

    /// Complete the current buffered batch and finish the coalescer.
    pub fn finish(&mut self) -> Result<()> {
        if !self.finished {
            self.flush_buffered_batch()?;
            self.finished = true;
        }
        Ok(())
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    fn reset_if_drained_after_pressure(&mut self) {
        if self.reset_after_drain && self.inner.is_empty() {
            self.inner = BatchCoalescer::new(self.inner.schema(), self.target_batch_size)
                .with_biggest_coalesce_batch_size(Some(self.target_batch_size / 2));
            self.reset_after_drain = false;
            self.reconcile_reservation();
        }
    }

    /// Return the next completed batch and an owned reservation for its charge.
    /// Dropping the returned reservation releases the charge.
    pub(crate) fn next_completed_batch_with_reservation(
        &mut self,
    ) -> Option<(RecordBatch, MemoryReservation)> {
        let batch = self.inner.next_completed_batch()?;
        let charged_bytes = batch.get_array_memory_size();
        let reservation = self.reservation.split(charged_bytes);
        self.reset_if_drained_after_pressure();
        Some((batch, reservation))
    }

    /// Return the next completed batch and release its reservation.
    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        let batch = self.inner.next_completed_batch()?;
        self.reservation.shrink(batch.get_array_memory_size());
        self.reset_if_drained_after_pressure();
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use std::sync::Arc;

    use arrow::array::UInt32Array;
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    #[test]
    fn test_coalesce() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // expected output is batches of exactly 21 rows (except for the final batch)
            .with_target_batch_size(21)
            .with_expected_output_sizes(vec![21, 21, 21, 17])
            .run()
    }

    #[test]
    fn test_coalesce_with_fetch_larger_than_input_size() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // input is 10 batches x 8 rows (80 rows) with fetch limit of 100
            // expected to behave the same as `test_concat_batches`
            .with_target_batch_size(21)
            .with_fetch(Some(100))
            .with_expected_output_sizes(vec![21, 21, 21, 17])
            .run();
    }

    #[test]
    fn test_coalesce_with_fetch_less_than_input_size() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // input is 10 batches x 8 rows (80 rows) with fetch limit of 50
            .with_target_batch_size(21)
            .with_fetch(Some(50))
            .with_expected_output_sizes(vec![21, 21, 8])
            .run();
    }

    #[test]
    fn test_coalesce_with_fetch_less_than_target_and_no_remaining_rows() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // input is 10 batches x 8 rows (80 rows) with fetch limit of 48
            .with_target_batch_size(24)
            .with_fetch(Some(48))
            .with_expected_output_sizes(vec![24, 24])
            .run();
    }

    #[test]
    fn test_coalesce_with_fetch_less_target_batch_size() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // input is 10 batches x 8 rows (80 rows) with fetch limit of 10
            .with_target_batch_size(21)
            .with_fetch(Some(10))
            .with_expected_output_sizes(vec![10])
            .run();
    }

    #[test]
    fn test_coalesce_single_large_batch_over_fetch() {
        let large_batch = uint32_batch(0..100);
        Test::new()
            .with_batch(large_batch)
            .with_target_batch_size(20)
            .with_fetch(Some(7))
            .with_expected_output_sizes(vec![7])
            .run()
    }

    #[test]
    fn reservation_tracks_buffered_completed_and_drained_size() {
        let batch = uint32_batch(0..2);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap();

        let baseline = pool.reserved();
        assert!(baseline > 0, "the empty Arrow coalescer retains capacity");
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());

        assert_eq!(
            coalescer.push_batch(batch.clone()).unwrap(),
            PushBatchStatus::Continue
        );
        let buffered_size = pool.reserved();
        assert!(
            buffered_size > baseline,
            "buffered arrays must increase the reservation"
        );
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());

        coalescer.finish().unwrap();
        let completed_size = pool.reserved();
        assert!(
            completed_size > baseline,
            "completed output must remain reserved"
        );
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());

        assert_eq!(coalescer.next_completed_batch().unwrap(), batch);
        assert_eq!(pool.reserved(), baseline);
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());
    }

    #[test]
    fn dropping_coalescer_releases_baseline_and_buffered_reservation() {
        let batch = uint32_batch(0..2);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap();

        let baseline = pool.reserved();
        assert!(baseline > 0, "the empty Arrow coalescer retains capacity");
        coalescer.push_batch(batch).unwrap();
        assert!(pool.reserved() > baseline);

        drop(coalescer);
        assert_eq!(pool.reserved(), 0, "drop releases all retained memory");
    }

    #[test]
    fn dequeue_transfers_reservation_until_returned_reservation_is_dropped() {
        let batch = uint32_batch(0..2);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap();

        let baseline = pool.reserved();
        coalescer.push_batch(batch.clone()).unwrap();
        coalescer.finish().unwrap();
        let completed_size = pool.reserved();

        let (actual, batch_reservation) =
            coalescer.next_completed_batch_with_reservation().unwrap();
        assert_eq!(actual, batch);
        assert_eq!(batch_reservation.size(), actual.get_array_memory_size());
        assert!(
            batch_reservation.size() > 0,
            "the dequeued batch must remain charged"
        );
        assert_eq!(batch_reservation.size(), completed_size - baseline);
        assert_eq!(pool.reserved(), completed_size);

        drop(batch_reservation);
        assert_eq!(pool.reserved(), baseline);
        drop(coalescer);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn zero_target_batch_size_is_rejected_at_construction() {
        let batch = uint32_batch(0..1);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let error = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            0,
            None,
            reservation,
        )
        .unwrap_err();
        assert!(error.to_string().contains("target batch size"));
    }

    #[test]
    fn nonempty_reservation_is_rejected_at_construction() {
        let batch = uint32_batch(0..1);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        reservation.grow(1);

        let error = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap_err();
        assert!(error.to_string().contains("reservation must be empty"));
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn construction_fails_when_baseline_exceeds_pool_limit() {
        let batch = uint32_batch(0..1);
        let unbounded: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerBaseline").register(&unbounded);
        let coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap();
        let baseline = unbounded.reserved();
        drop(coalescer);

        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(baseline.saturating_sub(1)));
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let error = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            None,
            reservation,
        )
        .unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    #[expect(deprecated)]
    fn deprecated_new_works_without_a_memory_pool() {
        let batch = uint32_batch(0..2);
        let mut coalescer = LimitedBatchCoalescer::new(batch.schema(), 4, None);
        coalescer.push_batch(batch.clone()).unwrap();
        coalescer.finish().unwrap();
        assert_eq!(coalescer.next_completed_batch().unwrap(), batch);
    }

    #[test]
    fn push_batch_fails_when_pool_limit_exceeded() {
        let batch = uint32_batch(0..1024);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(512));
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            8,
            None,
            reservation,
        )
        .unwrap();

        let err = coalescer.push_batch(batch).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted, got {err:?}"
        );
        // While the error propagates, the reservation keeps reflecting the
        // memory actually held, even beyond the pool limit.
        assert!(pool.reserved() > 512);
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());

        drop(coalescer);
        assert_eq!(pool.reserved(), 0, "drop releases all retained memory");
    }

    #[test]
    fn bypass_batches_are_accounted_and_released() {
        // A batch larger than half the target bypasses coalescing (see
        // `BatchCoalescer::with_biggest_coalesce_batch_size`) and is retained
        // as-is; it must still be charged to the reservation while queued.
        let batch = uint32_batch(0..100);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            8,
            None,
            reservation,
        )
        .unwrap();
        let baseline = pool.reserved();

        assert_eq!(
            coalescer.push_batch(batch.clone()).unwrap(),
            PushBatchStatus::Continue
        );
        assert_eq!(pool.reserved(), baseline + batch.get_array_memory_size());

        assert_eq!(coalescer.next_completed_batch().unwrap(), batch);
        assert_eq!(pool.reserved(), baseline);
    }

    #[test]
    fn fetch_limit_truncates_and_reservation_drains_to_baseline() {
        let batch = uint32_batch(0..5);
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation =
            MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
        let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
            batch.schema(),
            4,
            Some(3),
            reservation,
        )
        .unwrap();
        let baseline = pool.reserved();

        assert_eq!(
            coalescer.push_batch(batch.clone()).unwrap(),
            PushBatchStatus::LimitReached
        );
        assert!(pool.reserved() > baseline, "truncated rows stay charged");
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());
        let reserved_at_limit = pool.reserved();

        // Further pushes are ignored once the limit is reached.
        assert_eq!(
            coalescer.push_batch(batch).unwrap(),
            PushBatchStatus::LimitReached
        );
        assert_eq!(pool.reserved(), reserved_at_limit);
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());
        coalescer.finish().unwrap();

        let mut rows = 0;
        while let Some(out) = coalescer.next_completed_batch() {
            rows += out.num_rows();
        }
        assert_eq!(rows, 3, "fetch limit must truncate the accepted rows");
        assert_eq!(pool.reserved(), baseline);
        assert_eq!(coalescer.reservation.size(), coalescer.inner.size());
    }

    /// Test for [`LimitedBatchCoalescer`]
    ///
    /// Pushes the input batches to the coalescer and verifies that the resulting
    /// batches have the expected number of rows and contents.
    #[derive(Debug, Clone, Default)]
    struct Test {
        /// Batches to feed to the coalescer. Tests must have at least one
        /// schema
        input_batches: Vec<RecordBatch>,
        /// Expected output sizes of the resulting batches
        expected_output_sizes: Vec<usize>,
        /// target batch size
        target_batch_size: usize,
        /// Fetch (limit)
        fetch: Option<usize>,
    }

    impl Test {
        fn new() -> Self {
            Self::default()
        }

        /// Set the target batch size
        fn with_target_batch_size(mut self, target_batch_size: usize) -> Self {
            self.target_batch_size = target_batch_size;
            self
        }

        /// Set the fetch (limit)
        fn with_fetch(mut self, fetch: Option<usize>) -> Self {
            self.fetch = fetch;
            self
        }

        /// Extend the input batches with `batch`
        fn with_batch(mut self, batch: RecordBatch) -> Self {
            self.input_batches.push(batch);
            self
        }

        /// Extends the input batches with `batches`
        fn with_batches(
            mut self,
            batches: impl IntoIterator<Item = RecordBatch>,
        ) -> Self {
            self.input_batches.extend(batches);
            self
        }

        /// Extends `sizes` to expected output sizes
        fn with_expected_output_sizes(
            mut self,
            sizes: impl IntoIterator<Item = usize>,
        ) -> Self {
            self.expected_output_sizes.extend(sizes);
            self
        }

        /// Runs the test -- see documentation on [`Test`] for details
        fn run(self) {
            let Self {
                input_batches,
                target_batch_size,
                fetch,
                expected_output_sizes,
            } = self;

            let schema = input_batches[0].schema();

            // create a single large input batch for output comparison
            let single_input_batch = concat_batches(&schema, &input_batches).unwrap();

            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
            let reservation =
                MemoryConsumer::new("LimitedBatchCoalescerTest").register(&pool);
            let mut coalescer = LimitedBatchCoalescer::new_with_reservation(
                Arc::clone(&schema),
                target_batch_size,
                fetch,
                reservation,
            )
            .unwrap();

            let mut output_batches = vec![];
            for batch in input_batches {
                match coalescer.push_batch(batch).unwrap() {
                    PushBatchStatus::Continue => {
                        // continue pushing batches
                    }
                    PushBatchStatus::LimitReached => {
                        break;
                    }
                }
            }
            coalescer.finish().unwrap();
            while let Some(batch) = coalescer.next_completed_batch() {
                output_batches.push(batch);
            }

            let actual_output_sizes: Vec<usize> =
                output_batches.iter().map(|b| b.num_rows()).collect();
            assert_eq!(
                expected_output_sizes, actual_output_sizes,
                "Unexpected number of rows in output batches\n\
                Expected\n{expected_output_sizes:#?}\nActual:{actual_output_sizes:#?}"
            );

            // make sure we got the expected number of output batches and content
            let mut starting_idx = 0;
            assert_eq!(expected_output_sizes.len(), output_batches.len());
            for (i, (expected_size, batch)) in
                expected_output_sizes.iter().zip(output_batches).enumerate()
            {
                assert_eq!(
                    *expected_size,
                    batch.num_rows(),
                    "Unexpected number of rows in Batch {i}"
                );

                // compare the contents of the batch (using `==` compares the
                // underlying memory layout too)
                let expected_batch =
                    single_input_batch.slice(starting_idx, *expected_size);
                let batch_strings = batch_to_pretty_strings(&batch);
                let expected_batch_strings = batch_to_pretty_strings(&expected_batch);
                let batch_strings = batch_strings.lines().collect::<Vec<_>>();
                let expected_batch_strings =
                    expected_batch_strings.lines().collect::<Vec<_>>();
                assert_eq!(
                    expected_batch_strings, batch_strings,
                    "Unexpected content in Batch {i}:\
                    \n\nExpected:\n{expected_batch_strings:#?}\n\nActual:\n{batch_strings:#?}"
                );
                starting_idx += *expected_size;
            }
        }
    }

    /// Return a batch of  UInt32 with the specified range
    fn uint32_batch(range: Range<u32>) -> RecordBatch {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from_iter_values(range))],
        )
        .unwrap()
    }

    fn batch_to_pretty_strings(batch: &RecordBatch) -> String {
        arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .unwrap()
            .to_string()
    }
}
