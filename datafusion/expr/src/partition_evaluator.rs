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

use crate::window_frame_state::WindowAggState;
use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;


/// Trait for the state managed by this partition evaluator
///
/// This follows the existing pattern, but maybe we can improve it :thinking:

pub trait PartitionState {
    /// Returns the aggregate expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

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
/// In this case, [`Self::evaluate`], [`Self::evaluate_with_rank`] or
/// [`Self::evaluate_inside_range`] is called with values for the
/// entire partition.
///
/// # Stateful `PartitionEvaluator`
///
/// In this case, [`Self::evaluate_stateful`] is called to calculate
/// the results of the window function incrementally for each new
/// batch, saving and restoring any state needed to do so as
/// [`BuiltinWindowState`].
///
/// For example, when computing `ROW_NUMBER` incrementally,
/// [`Self::evaluate_stateful`] will be called multiple times with
/// different batches. For all batches after the first, the output
/// `row_number` must start from last `row_number` produced for the
/// previous batch. The previous row number is saved and restored as
/// the state.
///
/// [`BuiltInWindowFunctionExpr`]: crate::window::BuiltInWindowFunctionExpr
/// [`BuiltInWindowFunctionExpr::create_evaluator`]: crate::window::BuiltInWindowFunctionExpr::create_evaluator
pub trait PartitionEvaluator: Debug + Send {
    /// Can this evaluator be evaluated with (only) rank
    ///
    /// If `include_rank` is true, then [`Self::evaluate_with_rank`]
    /// will be called for each partition, which includes the
    /// `rank`.
    fn include_rank(&self) -> bool {
        false
    }

    /// Returns the internal state of the window function, if any
    fn state(&self) -> Result<Option<Box<dyn PartitionState>>> {
        Ok(None)
    }

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

    /// Sets the internal state for window function
    ///
    /// Only used for stateful evaluation
    fn set_state(&mut self, state: Box<dyn PartitionState>) -> Result<()> {
        Err(DataFusionError::NotImplemented(
            "set_state is not implemented for this window function".to_string(),
        ))
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
    /// `PERCENT_RANK`, `CUME_DIST`, `LEAD`, `LAG`).
    fn evaluate(&self, _values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate is not implemented by default".into(),
        ))
    }

    /// Evaluate window function result inside given range.
    ///
    /// Only used for stateful evaluation
    fn evaluate_stateful(&mut self, _values: &[ArrayRef]) -> Result<ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_stateful is not implemented by default".into(),
        ))
    }

    /// [`PartitionEvaluator::evaluate_with_rank`] is called for window
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
    ///
    /// See [`Self::include_rank`] for more details
    fn evaluate_with_rank(
        &self,
        _num_rows: usize,
        _ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition_with_rank is not implemented by default".into(),
        ))
    }

    /// Called for window functions that use values from window frame,
    /// such as `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE` and produce a
    /// single value for every row in the partition.
    ///
    /// Returns a [`ScalarValue`] that is the value of the window function for the entire partition
    fn evaluate_inside_range(
        &self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_inside_range is not implemented by default".into(),
        ))
    }
}
