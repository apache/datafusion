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

use arrow::array::ArrayRef;
use datafusion_common::{Result, ScalarValue, exec_err, not_impl_err};
use std::fmt::Debug;
use std::ops::Range;

use crate::window_state::WindowAggState;

/// Partition evaluator for Window Functions
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
pub trait PartitionEvaluator: Debug + Send {
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
