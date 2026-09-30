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

use arrow::array::{Array, BooleanArray, RecordBatch};
use arrow::compute::{BatchCoalescer, prep_null_mask_filter};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, assert_or_internal_err};

/// Concatenate multiple [`RecordBatch`]es and apply a limit
///
/// See [`BatchCoalescer`] for more details on how this works.
#[derive(Debug)]
pub struct LimitedBatchCoalescer {
    /// The arrow structure that builds the output batches
    inner: BatchCoalescer,
    /// Total number of rows returned so far
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
    /// Create a new `BatchCoalescer`
    ///
    /// # Arguments
    /// - `schema` - the schema of the output batches
    /// - `target_batch_size` - the minimum number of rows for each
    ///   output batch (until limit reached)
    /// - `fetch` - the maximum number of rows to fetch, `None` means fetch all rows
    pub fn new(
        schema: SchemaRef,
        target_batch_size: usize,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            inner: BatchCoalescer::new(schema, target_batch_size)
                .with_biggest_coalesce_batch_size(Some(target_batch_size / 2)),
            total_rows: 0,
            fetch,
            finished: false,
        }
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// Pushes the next [`RecordBatch`] into the coalescer and returns its status.
    ///
    /// # Arguments
    /// * `batch` - The [`RecordBatch`] to append.
    ///
    /// # Returns
    /// * [`PushBatchStatus::Continue`] - More batches can still be pushed.
    /// * [`PushBatchStatus::LimitReached`] - The row limit was reached after processing
    ///   this batch. The caller should call [`Self::finish`] before retrieving the
    ///   remaining buffered batches.
    ///
    /// # Errors
    /// Returns an error if called after [`Self::finish`] or if the internal push
    /// operation fails.
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<PushBatchStatus> {
        assert_or_internal_err!(
            !self.finished,
            "LimitedBatchCoalescer: cannot push batch after finish"
        );

        // if we are at the limit, return LimitReached
        if let Some(fetch) = self.fetch {
            // limit previously reached
            if self.total_rows >= fetch {
                return Ok(PushBatchStatus::LimitReached);
            }

            // limit now reached
            if self.total_rows + batch.num_rows() >= fetch {
                // Limit is reached
                let remaining_rows = fetch - self.total_rows;
                debug_assert!(remaining_rows > 0);

                let batch_head = batch.slice(0, remaining_rows);
                self.total_rows += batch_head.num_rows();
                self.inner.push_batch(batch_head)?;
                return Ok(PushBatchStatus::LimitReached);
            }
        }

        // Limit not reached, push the entire batch
        self.total_rows += batch.num_rows();
        self.inner.push_batch(batch)?;

        Ok(PushBatchStatus::Continue)
    }

    /// Emit an already materialized batch without copying it into another batch.
    /// Buffered rows are emitted first, preserving order. Fetch-clipped batches
    /// keep the ordinary buffering policy because their visible output is smaller.
    pub(crate) fn push_batch_without_coalescing(
        &mut self,
        batch: RecordBatch,
    ) -> Result<PushBatchStatus> {
        assert_or_internal_err!(
            !self.finished,
            "LimitedBatchCoalescer: cannot push batch after finish"
        );
        if batch.num_rows() == 0
            || self.fetch.is_some_and(|fetch| {
                fetch.saturating_sub(self.total_rows) < batch.num_rows()
            })
        {
            return self.push_batch(batch);
        }

        self.inner.finish_buffered_batch()?;
        let threshold = self.inner.biggest_coalesce_batch_size();
        self.inner.set_biggest_coalesce_batch_size(Some(0));
        let result = self.push_batch(batch);
        self.inner.set_biggest_coalesce_batch_size(threshold);
        result
    }

    /// Pushes the next [`RecordBatch`] into the coalescer after applying `filter`,
    /// avoiding a separate materialization pass compared to calling
    /// [`filter_record_batch`] followed by [`Self::push_batch`].
    ///
    /// [`filter_record_batch`]: arrow::compute::filter_record_batch
    pub fn push_batch_with_filter(
        &mut self,
        batch: RecordBatch,
        filter: &BooleanArray,
    ) -> Result<PushBatchStatus> {
        assert_or_internal_err!(
            !self.finished,
            "LimitedBatchCoalescer: cannot push batch after finish"
        );

        let Some(fetch) = self.fetch else {
            self.inner.push_batch_with_filter(batch, filter)?;
            return Ok(PushBatchStatus::Continue);
        };

        if self.total_rows >= fetch {
            return Ok(PushBatchStatus::LimitReached);
        }

        let selected_count = filter.true_count();
        if self.total_rows + selected_count >= fetch {
            let remaining = fetch - self.total_rows;
            let mask = match filter.null_count() {
                0 => filter.clone(),
                _ => prep_null_mask_filter(filter),
            };
            let end = mask
                .values()
                .set_indices()
                .nth(remaining - 1)
                .map_or(0, |i| i + 1);
            self.total_rows += remaining;
            self.inner
                .push_batch_with_filter(batch.slice(0, end), &mask.slice(0, end))?;
            return Ok(PushBatchStatus::LimitReached);
        }

        self.total_rows += selected_count;
        self.inner.push_batch_with_filter(batch, filter)?;
        Ok(PushBatchStatus::Continue)
    }

    /// Return true if there is no data buffered
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Complete the current buffered batch and finish the coalescer
    ///
    /// Any subsequent calls to `push_batch()` will return an Err
    pub fn finish(&mut self) -> Result<()> {
        self.inner.finish_buffered_batch()?;
        self.finished = true;
        Ok(())
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    /// Return the next completed batch, if any
    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        self.inner.next_completed_batch()
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
    fn bypass_preserves_order_and_restores_coalescing() -> Result<()> {
        let prefix = uint32_batch(0..2);
        let large = uint32_batch(2..6);
        let mut coalescer = LimitedBatchCoalescer::new(prefix.schema(), 16, None);
        coalescer.push_batch(prefix)?;
        coalescer.push_batch_without_coalescing(large.clone())?;
        assert_next_batch_values(&mut coalescer, vec![0, 1]);
        let output = coalescer.next_completed_batch().unwrap();
        assert!(Arc::ptr_eq(output.column(0), large.column(0)));
        assert_eq!(coalescer.inner.biggest_coalesce_batch_size(), Some(8));

        coalescer.push_batch(uint32_batch(6..8))?;
        coalescer.push_batch(uint32_batch(8..10))?;
        assert!(coalescer.next_completed_batch().is_none());
        coalescer.finish()?;
        assert_next_batch_values(&mut coalescer, vec![6, 7, 8, 9]);
        assert!(coalescer.next_completed_batch().is_none());
        assert!(coalescer.push_batch_without_coalescing(large).is_err());
        assert_eq!(coalescer.inner.biggest_coalesce_batch_size(), Some(8));
        Ok(())
    }

    #[test]
    fn bypass_preserves_fetch_boundaries() -> Result<()> {
        for fetch in [0, 1, 2, 4, 6, 8] {
            let prefix = uint32_batch(0..2);
            let mut coalescer =
                LimitedBatchCoalescer::new(prefix.schema(), 16, Some(fetch));
            coalescer.push_batch(prefix)?;
            if coalescer.push_batch_without_coalescing(uint32_batch(2..6))?
                == PushBatchStatus::Continue
            {
                coalescer.push_batch(uint32_batch(6..10))?;
            }
            assert_eq!(coalescer.inner.biggest_coalesce_batch_size(), Some(8));
            assert_eq!(coalescer.total_rows, fetch);
            coalescer.finish()?;
            let mut actual = Vec::new();
            let mut sizes = Vec::new();
            while let Some(batch) = coalescer.next_completed_batch() {
                sizes.push(batch.num_rows());
                actual.extend_from_slice(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap()
                        .values(),
                );
            }
            assert_eq!(actual, (0..fetch as u32).collect::<Vec<_>>());
            let expected_sizes = match fetch {
                0 => vec![],
                1 | 2 | 4 => vec![fetch],
                6 => vec![2, 4],
                8 => vec![2, 4, 2],
                _ => unreachable!(),
            };
            assert_eq!(sizes, expected_sizes);
        }
        Ok(())
    }

    #[test]
    fn bypass_empty_and_zero_column_batches() -> Result<()> {
        use arrow::record_batch::RecordBatchOptions;

        let prefix = uint32_batch(0..2);
        let mut coalescer = LimitedBatchCoalescer::new(prefix.schema(), 16, None);
        coalescer.push_batch(prefix)?;
        coalescer.push_batch_without_coalescing(uint32_batch(2..2))?;
        assert!(coalescer.next_completed_batch().is_none());
        coalescer.finish()?;
        assert_next_batch_values(&mut coalescer, vec![0, 1]);

        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )?;
        let mut coalescer = LimitedBatchCoalescer::new(schema, 16, None);
        coalescer.push_batch_without_coalescing(batch)?;
        assert_eq!(coalescer.next_completed_batch().unwrap().num_rows(), 3);
        assert_eq!(coalescer.total_rows, 3);
        assert_eq!(coalescer.inner.biggest_coalesce_batch_size(), Some(8));
        Ok(())
    }

    #[test]
    fn bypass_flush_error_preserves_threshold() -> Result<()> {
        use arrow::array::{DictionaryArray, Int8Array, StringArray};
        use arrow::datatypes::Int8Type;

        let batch = |start: i32| {
            let dictionary = DictionaryArray::<Int8Type>::new(
                Int8Array::from_iter_values(0..100),
                Arc::new(StringArray::from_iter_values(
                    (start..start + 100).map(|value| value.to_string()),
                )),
            );
            RecordBatch::try_from_iter(vec![("d", Arc::new(dictionary) as _)])
        };
        let first = batch(0)?;
        let mut coalescer = LimitedBatchCoalescer::new(first.schema(), 1000, None);
        coalescer.push_batch(first)?;
        coalescer.push_batch(batch(100)?)?;
        assert!(
            coalescer
                .push_batch_without_coalescing(batch(200)?)
                .is_err()
        );
        assert_eq!(coalescer.inner.biggest_coalesce_batch_size(), Some(500));
        assert_eq!(coalescer.total_rows, 200);
        Ok(())
    }

    #[test]
    fn test_push_batch_with_filter_nulls_and_fetch() {
        let batch = uint32_batch(0..8);
        let mut coalescer = LimitedBatchCoalescer::new(batch.schema(), 100, Some(3));
        let filter = BooleanArray::from(vec![
            None,
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(true),
            None,
            Some(true),
        ]);

        assert_eq!(
            coalescer.push_batch_with_filter(batch, &filter).unwrap(),
            PushBatchStatus::LimitReached,
        );
        coalescer.finish().unwrap();
        assert_next_batch_values(&mut coalescer, vec![1, 4, 5]);
    }

    #[test]
    fn test_push_batch_with_filter_fetch_boundaries() {
        let batch1 = uint32_batch(0..4);
        let batch2 = uint32_batch(4..8);
        let mut coalescer = LimitedBatchCoalescer::new(batch1.schema(), 100, Some(3));

        assert_eq!(
            coalescer
                .push_batch_with_filter(
                    batch1,
                    &BooleanArray::from(vec![true, false, true, false]),
                )
                .unwrap(),
            PushBatchStatus::Continue,
        );
        assert_eq!(
            coalescer
                .push_batch_with_filter(
                    batch2,
                    &BooleanArray::from(vec![true, true, true, true]),
                )
                .unwrap(),
            PushBatchStatus::LimitReached,
        );
        coalescer.finish().unwrap();
        assert_next_batch_values(&mut coalescer, vec![0, 2, 4]);

        let batch = uint32_batch(0..4);
        let mut coalescer = LimitedBatchCoalescer::new(batch.schema(), 100, Some(2));
        assert_eq!(
            coalescer
                .push_batch_with_filter(
                    batch,
                    &BooleanArray::from(vec![true, false, true, false]),
                )
                .unwrap(),
            PushBatchStatus::LimitReached,
        );
        assert_eq!(
            coalescer
                .push_batch_with_filter(
                    uint32_batch(4..8),
                    &BooleanArray::from(vec![true, true, true, true]),
                )
                .unwrap(),
            PushBatchStatus::LimitReached,
        );
        coalescer.finish().unwrap();
        assert_next_batch_values(&mut coalescer, vec![0, 2]);

        let batch = uint32_batch(0..4);
        let mut coalescer = LimitedBatchCoalescer::new(batch.schema(), 100, Some(0));
        assert_eq!(
            coalescer
                .push_batch_with_filter(
                    batch,
                    &BooleanArray::from(vec![true, true, true, true]),
                )
                .unwrap(),
            PushBatchStatus::LimitReached,
        );
        coalescer.finish().unwrap();
        assert!(coalescer.next_completed_batch().is_none());
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

            let mut coalescer =
                LimitedBatchCoalescer::new(Arc::clone(&schema), target_batch_size, fetch);

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

    fn assert_next_batch_values(
        coalescer: &mut LimitedBatchCoalescer,
        expected: Vec<u32>,
    ) {
        let output = coalescer.next_completed_batch().unwrap();
        let expected = UInt32Array::from(expected);
        assert_eq!(output.column(0).as_ref(), &expected as &dyn Array);
    }

    fn batch_to_pretty_strings(batch: &RecordBatch) -> String {
        arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .unwrap()
            .to_string()
    }
}
