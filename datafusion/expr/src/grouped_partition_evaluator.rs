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

//! Partition evaluation module

use arrow::array::{ArrayRef, AsArray, Int32Array, Int64Array, RecordBatch, UInt64Array};
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use arrow::buffer::{Buffer, MutableBuffer, ScalarBuffer};
use arrow::compute::BatchCoalescer;
use arrow::datatypes::UInt64Type;
use crate::window_state::WindowAggState;

/// Hold a range for a partition in a batch (start..end) and whether this partition is the same as the previous one
/// (e.g. between batches)
#[derive(Clone)]
pub struct PartitionRange {
    /// The start index of the slice (inclusive)
    /// When limit is used, the start might not begin from the last end
    pub start: usize,

    /// The end index of the slice (exclusive)
    pub end: usize,

    /// Whether this slice point to the same partition as the previous one (between batches)
    pub is_same_as_before: bool,
}

impl PartitionRange {
    fn len(&self) -> usize {
        self.end - self.start
    }
}


/// Evaluator for a window function that **does not need the entire partition** to emit value
///
/// and when the input is sorted on the partition keys and order by keys
///
/// Examples of such cases are:
/// - `row_number`
/// - `count` where window frame is `range unbounded preceding current row`
/// - `sum` where window frame is `range unbounded preceding current row`
/// etc..
pub trait SortedPartitionEvaluatorThatCanEmitRightAway: Debug + Send + std::any::Any {
    /// num_rows in case the partition does not get any input
    fn evaluate(&mut self, input: &[ArrayRef], partitions: &[PartitionRange], num_rows: usize) -> Result<ArrayRef>;

    /// When the entire input is a single partition
    fn evaluate_single_partition(&mut self, input: &[ArrayRef], partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(partition.start, 0);
        assert_eq!(partition.end, num_rows);

        self.evaluate(input, std::slice::from_ref(partition), num_rows)
    }

    /// When a single batch contain the end of the last partition and the start of a new one
    fn evaluate_boundry_partition(&mut self, input: &[ArrayRef], two_partitions: &[PartitionRange; 2], num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(two_partitions[0].start, 0);
        assert_eq!(two_partitions[0].end, num_rows);

        assert!(two_partitions[0].is_same_as_before);
        assert!(!two_partitions[1].is_same_as_before);

        self.evaluate(input, two_partitions, num_rows)
    }

    /// When each row is a single partition (except the first partition which may not be - e.g. boundary)
    ///
    ///
    fn evaluate_every_row_is_single_partition(&mut self, input: &[ArrayRef], first_partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(first_partition.start, 0);
        let mut partitions = Vec::with_capacity(num_rows - first_partition.len() + 1);

        partitions.push(first_partition.clone());

        for i in first_partition.end..num_rows {
            partitions.push(PartitionRange {
                start: i,
                end: i + 1,
                is_same_as_before: false,
            });
        }

        self.evaluate(input, &partitions, num_rows)
    }

    /// TODO - Size on the heap? or including stack? since size_of will get the size I think but not sure for Box<dyn>
    fn size(&self) -> usize;
}


/// Super optimized row number for sorted input
#[derive(Debug)]
struct RowNumberSorted {
    current_row: u64,
}

impl SortedPartitionEvaluatorThatCanEmitRightAway for RowNumberSorted {
    fn evaluate(&mut self, input: &[ArrayRef], partitions: &[PartitionRange], num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 0);
        assert_ne!(num_rows, 0);
        assert_ne!(partitions.len(), 0);
        debug_assert_eq!(partitions.iter().map(|p| p.len()).sum(), num_rows);

        let mut output = vec![0; num_rows];

        if !partitions[0].is_same_as_before {
            self.current_row = 0;
        }

        {
            let mut output_slice = output.as_mut_slice();

            for partition in partitions {
                for i in partition.start..partition.end {
                    self.current_row += 1;
                    output_slice[i] = self.current_row;
                }
                self.current_row = 0;
            }
        }

        self.current_row = output[output.len() - 1];

        Ok(Arc::new(UInt64Array::from(output)))
    }

    fn evaluate_single_partition(&mut self, input: &[ArrayRef], partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 0);
        assert_ne!(num_rows, 0);
        assert_eq!(partition.len(), num_rows);

        if !partition.is_same_as_before {
            self.current_row = 0;
        }

        let start = self.current_row + 1;

        let output = UInt64Array::from((start..start + (num_rows as u64)).collect::<Vec<u64>>());

        self.current_row += num_rows as u64;

        Ok(Arc::new(output))
    }

    fn evaluate_boundry_partition(&mut self, input: &[ArrayRef], two_partitions: &[PartitionRange; 2], num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 0);
        assert_ne!(num_rows, 0);
        assert_eq!(two_partitions[0].len() + two_partitions[1].len(), num_rows);

        assert!(two_partitions[0].is_same_as_before);
        assert!(!two_partitions[1].is_same_as_before);

        let start = self.current_row + 1;

        let partition_one_range = (start..start + (two_partitions[0].len() as u64));
        let partition_two_range = 1..(1 + two_partitions[1].len() as u64);


        let result_vec =  partition_one_range.chain(partition_two_range).collect::<Vec<_>>();
        self.current_row = result_vec[result_vec.len() - 1];

        let output = UInt64Array::from(result_vec);

        Ok(Arc::new(output))
    }

    fn evaluate_every_row_is_single_partition(&mut self, input: &[ArrayRef], first_partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 0);
        assert_ne!(num_rows, 0);

        let values = if !first_partition.is_same_as_before && first_partition.len() == 1 {
            vec![1; num_rows]
        } else if first_partition.len() == 1 {
            let mut values = vec![1; num_rows];
            values[0] = self.current_row + 1;
            values
        } else {
            let start = self.current_row + 1;

            let first_partition_range = (start..start + (first_partition.len() as u64));
            let result = first_partition_range.chain(std::iter::repeat_n(1, num_rows - first_partition.len() + 1)).collect::<Vec<u64>>();

            assert_eq!(result.len(), num_rows);

            result
        };

        self.current_row = 1;
        Ok(Arc::new(UInt64Array::from(values)))
    }

    fn size(&self) -> usize {
        0
    }
}

/// Super optimized sum for sorted input
#[derive(Debug)]
struct SumSortedGrowingWindow {
    current_sum: u64,
}

impl SortedPartitionEvaluatorThatCanEmitRightAway for SumSortedGrowingWindow {
    fn evaluate(&mut self, input: &[ArrayRef], partitions: &[PartitionRange], num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 1);
        assert_ne!(num_rows, 0);
        assert_ne!(partitions.len(), 0);
        debug_assert_eq!(partitions.iter().map(|p| p.len()).sum(), num_rows);

        let input = input[0].as_primitive::<UInt64Type>();

        let mut output = vec![0; num_rows];
        let mut input_iter = input.iter();

        if !partitions[0].is_same_as_before {
            self.current_sum = 0;
        }

        {
            let mut shift = 0;
            let mut output_slice = output.as_mut_slice();
            for partition in partitions {
                for (i, value) in input_iter.by_ref().take(partition.len()).enumerate() {
                    self.current_sum += value.unwrap_or_default();
                    output_slice[i + shift] = self.current_sum;
                }
                self.current_sum = 0;
                shift += partition.len();
            }
        }

        self.current_sum = output[output.len() - 1];

        Ok(Arc::new(UInt64Array::from(output)))
    }

    fn evaluate_single_partition(&mut self, input: &[ArrayRef], partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 1);
        assert_ne!(num_rows, 0);
        assert_eq!(partition.len(), num_rows);
        let input = input[0].as_primitive::<UInt64Type>();

        if !partition.is_same_as_before {
            self.current_sum = 0;
        }

        let output: UInt64Array = input.iter().map(|x| {
            // null will be 0
            self.current_sum += x.unwrap_or_default();
            self.current_sum
        }).collect();

        Ok(Arc::new(output))
    }

    fn evaluate_every_row_is_single_partition(&mut self, input: &[ArrayRef], first_partition: &PartitionRange, num_rows: usize) -> Result<ArrayRef> {
        assert_eq!(input.len(), 1);
        assert_ne!(num_rows, 0);
        let input = input[0].as_primitive::<UInt64Type>();

        let values = if !first_partition.is_same_as_before && first_partition.len() == 1 {
            input.clone()
        } else if first_partition.len() == 1 {
            todo!()
        } else {
            todo!()
        };

        // TODO - is this the right thing? what about nulls
        self.current_sum = values.iter().last().copied().unwrap_or_default();
        Ok(Arc::new(UInt64Array::from(values)))
    }

    fn size(&self) -> usize {
        0
    }
}

/// Evaluator for a window function that needs the entire **partition** in order to emit values
///
/// and when the input is sorted on the partition keys and order by keys
pub trait SortedPartitionEvaluator: Debug + Send + std::any::Any {
    /// evaluate the input and if have entire partitions finished, it will return the result
    fn evaluate(&mut self, batch: &[ArrayRef], partitions: &[PartitionRange], num_rows: usize) -> Result<ArrayRef>;

    /// Mark as finished so emit all the remaining partitions
    fn finish(&mut self) -> Result<Vec<ArrayRef>>;

    fn size(&self) -> usize;
}

/// Partition evaluator for Window Functions but work on multiple partitions at the same time
///
/// # Background
///
/// An implementation of this trait is created and used for each
/// partition defined by an `OVER` clause and is instantiated by
/// the DataFusion runtime.
///
/// For example, evaluating `window_func(val) OVER (PARTITION BY col)`
/// on the following data:
///
/// ```text
/// col | val
/// --- + ----
///  A  | 10
///  A  | 10
///  C  | 20
///  D  | 30
///  D  | 30
/// ```
///
/// Will instantiate three `PartitionEvaluator`s, one each for the
/// partitions defined by `col=A`, `col=B`, and `col=C`.
///
/// ```text
/// col | val
/// --- + ----
///  A  | 10     <--- partition 1
///  A  | 10
///
/// col | val
/// --- + ----
///  C  | 20     <--- partition 2
///
/// col | val
/// --- + ----
///  D  | 30     <--- partition 3
///  D  | 30
/// ```
///
/// Different methods on this trait will be called depending on the
/// capabilities described by [`supports_bounded_execution`],
/// [`uses_window_frame`], and [`include_rank`],
///
/// When implementing a new `PartitionEvaluator`, implement
/// corresponding evaluator according to table below.
///
/// # Implementation Table
///
/// |[`uses_window_frame`]|[`supports_bounded_execution`]|[`include_rank`]|function_to_implement|
/// |---|---|----|----|
/// |false (default)      |false (default)               |false (default)   | [`evaluate_all`]           |
/// |false                |true                          |false             | [`evaluate`]               |
/// |false                |true/false                    |true              | [`evaluate_all_with_rank`] |
/// |true                 |true/false                    |true/false        | [`evaluate`]               |
///
/// [`evaluate`]: Self::evaluate
/// [`evaluate_all`]: Self::evaluate_all
/// [`evaluate_all_with_rank`]: Self::evaluate_all_with_rank
/// [`uses_window_frame`]: Self::uses_window_frame
/// [`include_rank`]: Self::include_rank
/// [`supports_bounded_execution`]: Self::supports_bounded_execution
///
/// For more background, please also see the [User defined Window Functions in DataFusion blog]
///
/// [User defined Window Functions in DataFusion blog]: https://datafusion.apache.org/blog/2025/04/19/user-defined-window-functions
pub trait PartitionEvaluator: Debug + Send + std::any::Any {
    /// When the window frame has a fixed beginning (e.g UNBOUNDED
    /// PRECEDING), some functions such as FIRST_VALUE, LAST_VALUE and
    /// NTH_VALUE do not need the (unbounded) input once they have
    /// seen a certain amount of input.
    ///
    /// `memoize` is called after each input batch is processed, and
    /// such functions can save whatever they need and modify
    /// [`WindowAggState`] appropriately to allow rows to be pruned
    fn memoize(&mut self, _state: &mut WindowAggState) -> Result<()> {
        Ok(())
    }

    /// If `uses_window_frame` flag is `false`. This method is used to
    /// calculate required range for the window function during
    /// stateful execution.
    ///
    /// Generally there is no required range, hence by default this
    /// returns smallest range(current row). e.g seeing current row is
    /// enough to calculate window result (such as row_number, rank,
    /// etc)
    fn get_range(&self, idx: usize, _n_rows: usize) -> Result<Range<usize>> {
        if self.uses_window_frame() {
            exec_err!("Range should be calculated from window frame")
        } else {
            Ok(Range {
                start: idx,
                end: idx + 1,
            })
        }
    }

    /// Get whether evaluator needs future data for its result (if so returns `false`) or not
    fn is_causal(&self) -> bool {
        false
    }

    /// Evaluate a window function on an entire input partition.
    ///
    /// This function is called once per input *partition* for window
    /// functions that *do not use* values from the window frame,
    /// such as `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`,
    /// `CUME_DIST`, `LEAD`, `LAG`).
    ///
    /// It produces the result of all rows in a single pass. It
    /// expects to receive the entire partition as the `value` and
    /// must produce an output column with one output row for every
    /// input row.
    ///
    /// `num_rows` is required to correctly compute the output in case
    /// `values.len() == 0`
    ///
    /// Implementing this function is an optimization: certain window
    /// functions are not affected by the window frame definition or
    /// the query doesn't have a frame, and `evaluate` skips the
    /// (costly) window frame boundary calculation and the overhead of
    /// calling `evaluate` for each output row.
    ///
    /// For example, the `LAG` built in window function does not use
    /// the values of its window frame (it can be computed in one shot
    /// on the entire partition with `Self::evaluate_all` regardless of the
    /// window defined in the `OVER` clause)
    ///
    /// ```sql
    /// lag(x, 1) OVER (ORDER BY z ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)
    /// ```
    ///
    /// However, `avg()` computes the average in the window and thus
    /// does use its window frame
    ///
    /// ```sql
    /// avg(x) OVER (PARTITION BY y ORDER BY z ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)
    /// ```
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        // When window frame boundaries are not used and evaluator supports bounded execution
        // We can calculate evaluate result by repeatedly calling `self.evaluate` `num_rows` times
        // If user wants to implement more efficient version, this method should be overwritten
        // Default implementation may behave suboptimally (For instance `NumRowEvaluator` overwrites it)
        if !self.uses_window_frame() && self.supports_bounded_execution() {
            let res = (0..num_rows)
                .map(|idx| self.evaluate(values, &self.get_range(idx, num_rows)?))
                .collect::<Result<Vec<_>>>()?;
            ScalarValue::iter_to_array(res)
        } else {
            not_impl_err!("evaluate_all is not implemented by default")
        }
    }

    /// Evaluate window function on a range of rows in an input
    /// partition.
    ///
    /// This is the simplest and most general function to implement
    /// but also the least performant as it creates output one row at
    /// a time. It is typically much faster to implement stateful
    /// evaluation using one of the other specialized methods on this
    /// trait.
    ///
    /// Returns a [`ScalarValue`] that is the value of the window
    /// function within `range` for the entire partition. Argument
    /// `values` contains the evaluation result of function arguments
    /// and evaluation results of ORDER BY expressions. If function has a
    /// single argument, `values[1..]` will contain ORDER BY expression results.
    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        not_impl_err!("evaluate is not implemented by default")
    }

    /// [`PartitionEvaluator::evaluate_all_with_rank`] is called for window
    /// functions that only need the rank of a row within its window
    /// frame.
    ///
    /// Evaluate the partition evaluator against the partition using
    /// the row ranks. For example, `RANK(col)` produces
    ///
    /// ```text
    /// col | rank
    /// --- + ----
    ///  A  | 1
    ///  A  | 1
    ///  C  | 3
    ///  D  | 4
    ///  D  | 4
    /// ```
    ///
    /// For this case, `num_rows` would be `5` and the
    /// `ranks_in_partition` would be called with
    ///
    /// ```text
    /// [
    ///   (0,1),
    ///   (2,2),
    ///   (3,4),
    /// ]
    /// ```
    fn evaluate_all_with_rank(
        &self,
        _num_rows: usize,
        _ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        not_impl_err!("evaluate_partition_with_rank is not implemented by default")
    }

    /// TODO - depend if the data is sorted
    ///
    /// If no need to wait for the whole partition to be evaluated before emitting any value
    /// for example row_number does not need
    fn can_emit_right_away(&self) -> bool {
        false
    }

    /// Can the window function be incrementally computed using
    /// bounded memory?
    ///
    /// See the table on [`Self`] for what functions to implement
    fn supports_bounded_execution(&self) -> bool {
        false
    }

    /// Does the window function use the values from the window frame,
    /// if one is specified?
    ///
    /// See the table on [`Self`] for what functions to implement
    fn uses_window_frame(&self) -> bool {
        false
    }

    /// Can this function be evaluated with (only) rank
    ///
    /// See the table on [`Self`] for what functions to implement
    fn include_rank(&self) -> bool {
        false
    }
}
