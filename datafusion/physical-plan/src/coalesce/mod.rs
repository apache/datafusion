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

use arrow::array::RecordBatch;
use arrow::compute::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use datafusion_common::{assert_or_internal_err, Result};

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

    fn batch_to_pretty_strings(batch: &RecordBatch) -> String {
        arrow::util::pretty::pretty_format_batches(std::slice::from_ref(batch))
            .unwrap()
            .to_string()
    }
}
