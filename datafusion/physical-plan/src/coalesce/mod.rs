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
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

/// Concatenate multiple [`RecordBatch`]es
///
/// `BatchCoalescer` concatenates multiple small [`RecordBatch`]es, produced by
/// operations such as `FilterExec` and `RepartitionExec`, into larger ones for
/// more efficient processing by subsequent operations.
///
/// # Background
///
/// Generally speaking, larger [`RecordBatch`]es are more efficient to process
/// than smaller record batches (until the CPU cache is exceeded) because there
/// is fixed processing overhead per batch. DataFusion tries to operate on
/// batches of `target_batch_size` rows to amortize this overhead
///
/// ```text
/// ┌────────────────────┐
/// │    RecordBatch     │
/// │   num_rows = 23    │
/// └────────────────────┘                 ┌────────────────────┐
///                                        │                    │
/// ┌────────────────────┐     Coalesce    │                    │
/// │                    │      Batches    │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 50    │  ─ ─ ─ ─ ─ ─ ▶  │                    │
/// │                    │                 │    RecordBatch     │
/// │                    │                 │   num_rows = 106   │
/// └────────────────────┘                 │                    │
///                                        │                    │
/// ┌────────────────────┐                 │                    │
/// │                    │                 │                    │
/// │    RecordBatch     │                 │                    │
/// │   num_rows = 33    │                 └────────────────────┘
/// │                    │
/// └────────────────────┘
/// ```
///
/// # Notes:
///
/// 1. Output rows are produced in the same order as the input rows
///
/// 2. The output is a sequence of batches, with all but the last being at least
///    `target_batch_size` rows.
///
/// 3. Eventually this may also be able to handle other optimizations such as a
///    combined filter/coalesce operation.
///
#[derive(Debug)]
pub struct BatchCoalescer {
    /// The input schema
    schema: SchemaRef,
    /// Minimum number of rows for coalesces batches
    target_batch_size: usize,
    /// Total number of rows returned so far
    total_rows: usize,
    /// Buffered batches
    buffer: Vec<RecordBatch>,
    /// Buffered row count
    buffered_rows: usize,
    /// Limit: maximum number of rows to fetch, `None` means fetch all rows
    fetch: Option<usize>,
}

impl BatchCoalescer {
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
            schema,
            target_batch_size,
            total_rows: 0,
            buffer: vec![],
            buffered_rows: 0,
            fetch,
        }
    }

    /// Return the schema of the output batches
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Push next batch, and returns [`CoalescerState`] indicating the current
    /// state of the buffer.
    pub fn push_batch(&mut self, batch: RecordBatch) -> CoalescerState {
        if self.limit_reached(&batch) {
            CoalescerState::LimitReached
        } else if self.target_reached(batch) {
            CoalescerState::TargetReached
        } else {
            CoalescerState::Continue
        }
    }

    /// Return true if the there is no data buffered
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Checks if the buffer will reach the specified limit after getting
    /// `batch`.
    ///
    /// If fetch would be exceeded, slices the received batch, updates the
    /// buffer with it, and returns `true`.
    ///
    /// Otherwise: does nothing and returns `false`.
    fn limit_reached(&mut self, batch: &RecordBatch) -> bool {
        match self.fetch {
            Some(fetch) if self.total_rows + batch.num_rows() >= fetch => {
                // Limit is reached
                let remaining_rows = fetch - self.total_rows;
                debug_assert!(remaining_rows > 0);

                let batch = batch.slice(0, remaining_rows);
                self.buffered_rows += batch.num_rows();
                self.total_rows = fetch;
                self.buffer.push(batch);
                true
            }
            _ => false,
        }
    }

    /// Updates the buffer with the given batch.
    ///
    /// If the target batch size is reached, returns `true`. Otherwise, returns
    /// `false`.
    fn target_reached(&mut self, batch: RecordBatch) -> bool {
        if batch.num_rows() == 0 {
            false
        } else {
            self.total_rows += batch.num_rows();
            self.buffered_rows += batch.num_rows();
            self.buffer.push(batch);
            self.buffered_rows >= self.target_batch_size
        }
    }

    /// Concatenates and returns all buffered batches, and clears the buffer.
    pub fn finish_batch(&mut self) -> datafusion_common::Result<RecordBatch> {
        let batch = concat_batches(&self.schema, &self.buffer)?;
        self.buffer.clear();
        self.buffered_rows = 0;
        Ok(batch)
    }
}

/// Indicates the state of the [`BatchCoalescer`] buffer after the
/// [`BatchCoalescer::push_batch()`] operation.
///
/// The caller should take different actions, depending on the variant returned.
pub enum CoalescerState {
    /// Neither the limit nor the target batch size is reached.
    ///
    /// Action: continue pushing batches.
    Continue,
    /// The limit has been reached.
    ///
    /// Action: call [`BatchCoalescer::finish_batch()`] to get the final
    /// buffered results as a batch and finish the query.
    LimitReached,
    /// The specified minimum number of rows a batch should have is reached.
    ///
    /// Action: call [`BatchCoalescer::finish_batch()`] to get the current
    /// buffered results as a batch and then continue pushing batches.
    TargetReached,
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;

    use arrow::array::UInt32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_coalesce() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // expected output is batches of at least 20 rows (except for the final batch)
            .with_target_batch_size(21)
            .with_expected_output_sizes(vec![24, 24, 24, 8])
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
            .with_expected_output_sizes(vec![24, 24, 24, 8])
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
            .with_expected_output_sizes(vec![24, 24, 2])
            .run();
    }

    #[test]
    fn test_coalesce_with_fetch_less_than_target_and_no_remaining_rows() {
        let batch = uint32_batch(0..8);
        Test::new()
            .with_batches(std::iter::repeat_n(batch, 10))
            // input is 10 batches x 8 rows (80 rows) with fetch limit of 48
            .with_target_batch_size(21)
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

    /// Test for [`BatchCoalescer`]
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
                BatchCoalescer::new(Arc::clone(&schema), target_batch_size, fetch);

            let mut output_batches = vec![];
            for batch in input_batches {
                match coalescer.push_batch(batch) {
                    CoalescerState::Continue => {}
                    CoalescerState::LimitReached => {
                        output_batches.push(coalescer.finish_batch().unwrap());
                        break;
                    }
                    CoalescerState::TargetReached => {
                        coalescer.buffered_rows = 0;
                        output_batches.push(coalescer.finish_batch().unwrap());
                    }
                }
            }
            if coalescer.buffered_rows != 0 {
                output_batches.extend(coalescer.buffer);
            }

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
        arrow::util::pretty::pretty_format_batches(&[batch.clone()])
            .unwrap()
            .to_string()
    }
}
