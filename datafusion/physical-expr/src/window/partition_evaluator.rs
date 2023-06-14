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

use crate::window::WindowAggState;
use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use std::fmt::Debug;
use std::ops::Range;

/// Partition evaluator for Window Functions
///
/// # Background
///
/// An implementation of this trait is created and used for each
/// partition defined by an `OVER` clause and is instantiated by
/// [`BuiltInWindowFunctionExpr::create_evaluator`]
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
/// capabilities described by [`BuiltInWindowFunctionExpr`]:
///
/// # Stateless `PartitionEvaluator`
///
/// In this case, either [`Self::evaluate_all`] or [`Self::evaluate_with_rank_all`] is called with values for the
/// entire partition.
///
/// # Stateful `PartitionEvaluator`
///
/// In this case, [`Self::evaluate`] is called to calculate
/// the results of the window function incrementally for each new
/// batch.
///
/// For example, when computing `ROW_NUMBER` incrementally,
/// [`Self::evaluate`] will be called multiple times with
/// different batches. For all batches after the first, the output
/// `row_number` must start from last `row_number` produced for the
/// previous batch. The previous row number is saved and restored as
/// the state.
///
/// [`BuiltInWindowFunctionExpr`]: crate::window::BuiltInWindowFunctionExpr
/// [`BuiltInWindowFunctionExpr::create_evaluator`]: crate::window::BuiltInWindowFunctionExpr::create_evaluator
/// When implementing a new `PartitionEvaluator`,
/// `uses_window_frame` and `supports_bounded_execution` flags determine which evaluation method will be called
/// during runtime. Implement corresponding evaluator according to table below.
/// |uses_window_frame|supports_bounded_execution|function_to_implement|
/// |---|---|----|
/// |false|false|`evaluate_all` (if we were to implement `PERCENT_RANK` it would end up in this quadrant, we cannot produce any result without seeing whole data)|
/// |false|true|`evaluate` (optionally can also implement `evaluate_all` for more optimized implementation. However, there will be default implementation that is suboptimal) . If we were to implement `ROW_NUMBER` it will end up in this quadrant. Example `OddRowNumber` showcases this use case|
/// |true|false|`evaluate` (I think as long as `uses_window_frame` is `true`. There is no way for `supports_bounded_execution` to be false). I couldn't come up with any example for this quadrant |
/// |true|true|`evaluate`. If we were to implement `FIRST_VALUE`, it would end up in this quadrant|.
pub trait PartitionEvaluator: Debug + Send {
    /// Updates the internal state for window function
    ///
    /// Only used for stateful evaluation
    ///
    /// `state`: is useful to update internal state for window function.
    /// `idx`: is the index of last row for which result is calculated.
    /// `range_columns`: is the result of order by column values. It is used to calculate rank boundaries
    /// `sort_partition_points`: is the boundaries of each rank in the range_column. It is used to update rank.
    fn update_state(
        &mut self,
        _state: &WindowAggState,
        _idx: usize,
        _range_columns: &[ArrayRef],
        _sort_partition_points: &[Range<usize>],
    ) -> Result<()> {
        // If we do not use state, update_state does nothing
        Ok(())
    }

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

    /// Gets the range where the window function result is calculated.
    ///
    /// `idx`: is the index of last row for which result is calculated.
    /// `n_rows`: is the number of rows of the input record batch (Used during bounds check)
    fn get_range(&self, _idx: usize, _n_rows: usize) -> Result<Range<usize>> {
        Err(DataFusionError::NotImplemented(
            "get_range is not implemented for this window function".to_string(),
        ))
    }

    /// Called for window functions that *do not use* values from the
    /// the window frame, such as `ROW_NUMBER`, `RANK`, `DENSE_RANK`,
    /// `PERCENT_RANK`, `CUME_DIST`, `LEAD`, `LAG`). It produces result
    /// of all rows in a single pass. It expects to receive whole table data
    /// as a single batch.
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        // When window frame boundaries are not used and evaluator supports bounded execution
        // We can calculate evaluate result by repeatedly calling `self.evaluate` `num_rows` times
        // If user wants to implement more efficient version, this method should be overwritten
        // Default implementation may behave suboptimally (For instance `NumRowEvaluator` overwrites it)
        if !self.uses_window_frame() && self.supports_bounded_execution() {
            let res = (0..num_rows)
                .map(|_idx| self.evaluate(values, &Range { start: 0, end: 1 }))
                .collect::<Result<Vec<_>>>()?;
            ScalarValue::iter_to_array(res.into_iter())
        } else {
            Err(DataFusionError::NotImplemented(
                "evaluate_all is not implemented by default".into(),
            ))
        }
    }

    /// Evaluate window function result inside given range.
    ///
    /// Only used for stateful evaluation
    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate is not implemented by default".into(),
        ))
    }

    /// [`PartitionEvaluator::evaluate_with_rank_all`] is called for window
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
    ///  D  | 5
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
    fn evaluate_with_rank_all(
        &self,
        _num_rows: usize,
        _ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition_with_rank is not implemented by default".into(),
        ))
    }

    /// Can the window function be incrementally computed using
    /// bounded memory?
    ///
    /// If this function returns true, implement [`PartitionEvaluator::evaluate`]
    fn supports_bounded_execution(&self) -> bool {
        false
    }

    /// Does the window function use the values from its window frame?
    ///
    /// If this function returns true, implement [`PartitionEvaluator::evaluate`]
    fn uses_window_frame(&self) -> bool {
        false
    }

    /// Can this function be evaluated with (only) rank
    ///
    /// If `include_rank` is true, implement [`PartitionEvaluator::evaluate_with_rank_all`]
    fn include_rank(&self) -> bool {
        false
    }
}
