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

use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::array::BooleanArray;
use arrow::array::{Array, ArrayRef, new_empty_array};
use arrow::compute::SortOptions;
use arrow::compute::filter as arrow_filter;
use arrow::compute::kernels::sort::SortColumn;
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::hash_utils::RandomState;
use datafusion_common::utils::compare_rows;
use datafusion_common::{
    Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err, exec_err,
    internal_err,
};
use datafusion_expr::window_state::{
    PartitionBatchState, WindowAggState, WindowFrameContext, WindowFrameStateGroups,
};
use datafusion_expr::{Accumulator, PartitionEvaluator, WindowFrame, WindowFrameBound};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use indexmap::IndexMap;

/// Common trait for [window function] implementations
///
/// # Aggregate Window Expressions
///
/// These expressions take the form
///
/// ```text
/// OVER({ROWS | RANGE| GROUPS} BETWEEN UNBOUNDED PRECEDING AND ...)
/// ```
///
/// For example, cumulative window frames uses `PlainAggregateWindowExpr`.
///
/// # Non Aggregate Window Expressions
///
/// The expressions have the form
///
/// ```text
/// OVER({ROWS | RANGE| GROUPS} BETWEEN M {PRECEDING| FOLLOWING} AND ...)
/// ```
///
/// For example, sliding window frames use [`SlidingAggregateWindowExpr`].
///
/// [window function]: https://en.wikipedia.org/wiki/Window_function_(SQL)
/// [`PlainAggregateWindowExpr`]: crate::window::PlainAggregateWindowExpr
/// [`SlidingAggregateWindowExpr`]: crate::window::SlidingAggregateWindowExpr
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// The field of the final result of this window function.
    fn field(&self) -> Result<FieldRef>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "WindowExpr: default name"
    }

    /// Expressions that are passed to the WindowAccumulator.
    /// Functions which take a single input argument, such as `sum`, return a single [`datafusion_expr::expr::Expr`],
    /// others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Evaluate the window function arguments against the batch and return
    /// array ref, normally the resulting `Vec` is a single element one.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        evaluate_expressions_to_arrays(&self.expressions(), batch)
    }

    /// Evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the window function against the batch. This function facilitates
    /// stateful, bounded-memory implementations.
    ///
    /// `eval_ctx` carries stream-level (cross-partition) information; see
    /// [`WindowEvalContext`].
    fn evaluate_stateful(
        &self,
        _partition_batches: &PartitionBatches,
        _window_agg_state: &mut PartitionWindowAggStates,
        _eval_ctx: &WindowEvalContext<'_>,
    ) -> Result<()> {
        internal_err!("evaluate_stateful is not implemented for {}", self.name())
    }

    /// Expressions that's from the window function's partition by clause, empty if absent
    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>];

    /// Expressions that's from the window function's order by clause, empty if absent
    fn order_by(&self) -> &[PhysicalSortExpr];

    /// Get order by columns, empty if absent
    fn order_by_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.order_by()
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect()
    }

    /// Get the window frame of this [WindowExpr].
    fn get_window_frame(&self) -> &Arc<WindowFrame>;

    /// Return a flag indicating whether this [WindowExpr] can run with
    /// bounded memory.
    fn uses_bounded_memory(&self) -> bool;

    /// Get the reverse expression of this [WindowExpr].
    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>>;

    /// Creates a new instance of the window function evaluator.
    ///
    /// Returns `WindowFn::Builtin` for built-in window functions (e.g., ROW_NUMBER, RANK)
    /// or `WindowFn::Aggregate` for aggregate window functions (e.g., SUM, AVG).
    fn create_window_fn(&self) -> Result<WindowFn>;

    /// Returns all expressions used in the [`WindowExpr`].
    /// These expressions are (1) function arguments, (2) partition by expressions, (3) order by expressions.
    fn all_expressions(&self) -> WindowPhysicalExpressions {
        let args = self.expressions();
        let partition_by_exprs = self.partition_by().to_vec();
        let order_by_exprs = self
            .order_by()
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect();
        WindowPhysicalExpressions {
            args,
            partition_by_exprs,
            order_by_exprs,
        }
    }

    /// Rewrites [`WindowExpr`], with new expressions given. The argument should be consistent
    /// with the return value of the [`WindowExpr::all_expressions`] method.
    /// Returns `Some(Arc<dyn WindowExpr>)` if re-write is supported, otherwise returns `None`.
    fn with_new_expressions(
        &self,
        _args: Vec<Arc<dyn PhysicalExpr>>,
        _partition_bys: Vec<Arc<dyn PhysicalExpr>>,
        _order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn WindowExpr>> {
        None
    }
}

/// Stores the physical expressions used inside the `WindowExpr`.
pub struct WindowPhysicalExpressions {
    /// Window function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// PARTITION BY expressions
    pub partition_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// ORDER BY expressions
    pub order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

/// Extension trait that adds common functionality to [`AggregateWindowExpr`]s
pub trait AggregateWindowExpr: WindowExpr {
    /// Get the accumulator for the window expression. Note that distinct
    /// window expressions may return distinct accumulators; e.g. sliding
    /// (non-sliding) expressions will return sliding (normal) accumulators.
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// Optional FILTER (WHERE ...) predicate for this window aggregate.
    fn filter_expr(&self) -> Option<&Arc<dyn PhysicalExpr>>;

    /// Given current range and the last range, calculates the accumulator
    /// result for the range of interest.
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<ScalarValue>;

    /// Indicates whether this window function always produces the same result
    /// for all rows in the partition.
    fn is_constant_in_partition(&self) -> bool;

    /// Evaluates the window function against the batch.
    fn aggregate_evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut accumulator = self.get_accumulator()?;
        let mut last_range = Range { start: 0, end: 0 };
        let sort_options = self.order_by().iter().map(|o| o.options).collect();
        let mut window_frame_ctx =
            WindowFrameContext::new(Arc::clone(self.get_window_frame()), sort_options);
        self.get_result_column(
            &mut accumulator,
            batch,
            None,
            &mut last_range,
            &mut window_frame_ctx,
            0,
            false,
        )
    }

    /// Statefully evaluates the window function against the batch. Maintains
    /// state so that it can work incrementally over multiple chunks.
    fn aggregate_evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
        eval_ctx: &WindowEvalContext<'_>,
    ) -> Result<()> {
        let field = self.field()?;
        let out_type = field.data_type();
        // Every partition consults the same most recent input row, so its
        // ORDER BY values can be evaluated once, outside the per-partition
        // loop.
        let most_recent_row_order_bys = eval_ctx
            .most_recent_row
            .map(|batch| self.order_by_columns(batch))
            .transpose()?
            .map(get_orderby_values);
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let accumulator = self.get_accumulator()?;
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(out_type)?,
                        window_fn: WindowFn::Aggregate(accumulator),
                        published: false,
                    },
                );
            }
            let window_state = window_agg_state
                .get_mut(partition_row)
                .ok_or_else(|| exec_datafusion_err!("Cannot find state"))?;
            let WindowFn::Aggregate(accumulator) = &mut window_state.window_fn else {
                unreachable!()
            };
            let state = &mut window_state.state;
            let record_batch = &partition_batch_state.record_batch;

            // Skip partitions that cannot produce anything new until they
            // either receive rows or reach their end.
            if state.is_up_to_date_with(partition_batch_state) {
                continue;
            }

            // If there is no window state context, initialize it.
            let window_frame_ctx = state.window_frame_ctx.get_or_insert_with(|| {
                let sort_options = self.order_by().iter().map(|o| o.options).collect();
                WindowFrameContext::new(Arc::clone(self.get_window_frame()), sort_options)
            });
            let out_col = self.get_result_column(
                accumulator,
                record_batch,
                most_recent_row_order_bys.as_deref(),
                // Start search from the last range
                &mut state.window_frame_range,
                window_frame_ctx,
                state.last_calculated_index,
                !partition_batch_state.is_end,
            )?;
            state.update(&out_col, partition_batch_state)?;
        }
        Ok(())
    }

    /// Calculates the window expression result for the given record batch.
    /// Assumes that `record_batch` belongs to a single partition.
    ///
    /// # Arguments
    /// * `accumulator`: The accumulator to use for the calculation.
    /// * `record_batch`: batch belonging to the current partition (see [`PartitionBatchState`]).
    /// * `most_recent_row_order_bys`: ORDER BY values of the most recent input
    ///   row, if available (see [`WindowExpr::evaluate_stateful`]).
    /// * `last_range`: The last range of rows that were processed (see [`WindowAggState`]).
    /// * `window_frame_ctx`: Details about the window frame (see [`WindowFrameContext`]).
    /// * `idx`: The index of the current row in the record batch.
    /// * `not_end`: is the current row not the end of the partition (see [`PartitionBatchState`]).
    #[expect(clippy::too_many_arguments)]
    fn get_result_column(
        &self,
        accumulator: &mut Box<dyn Accumulator>,
        record_batch: &RecordBatch,
        most_recent_row_order_bys: Option<&[ArrayRef]>,
        last_range: &mut Range<usize>,
        window_frame_ctx: &mut WindowFrameContext,
        mut idx: usize,
        not_end: bool,
    ) -> Result<ArrayRef> {
        let values = self.evaluate_args(record_batch)?;

        // Evaluate filter mask once per record batch if present
        let filter_mask_arr: Option<ArrayRef> = match self.filter_expr() {
            Some(expr) => {
                let value = expr.evaluate(record_batch)?;
                Some(value.into_array(record_batch.num_rows())?)
            }
            None => None,
        };

        // Borrow boolean view from the owned array
        let filter_mask: Option<&BooleanArray> = match filter_mask_arr.as_deref() {
            Some(arr) => Some(as_boolean_array(arr)?),
            None => None,
        };

        if self.is_constant_in_partition() {
            if not_end {
                let field = self.field()?;
                let out_type = field.data_type();
                return Ok(new_empty_array(out_type));
            }
            let values = if let Some(mask) = filter_mask {
                // Apply mask to all argument arrays before a single update
                filter_arrays(&values, mask)?
            } else {
                values
            };
            accumulator.update_batch(&values)?;
            let value = accumulator.evaluate()?;
            return value.to_array_of_size(record_batch.num_rows());
        }
        let order_bys = get_orderby_values(self.order_by_columns(record_batch)?);

        // We iterate on each row to perform a running calculation.
        let length = values[0].len();
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        let is_causal = self.get_window_frame().is_causal();
        while idx < length {
            // Start search from the last_range. This squeezes searched range.
            let cur_range =
                window_frame_ctx.calculate_range(&order_bys, last_range, length, idx)?;
            // Exit if the range is non-causal and extends all the way:
            if cur_range.end == length
                && !is_causal
                && not_end
                && !is_end_bound_safe(
                    window_frame_ctx,
                    &order_bys,
                    most_recent_row_order_bys,
                    self.order_by(),
                    idx,
                )?
            {
                break;
            }
            let value = self.get_aggregate_result_inside_range(
                last_range,
                &cur_range,
                &values,
                accumulator,
                filter_mask,
            )?;
            // Update last range
            *last_range = cur_range;
            row_wise_results.push(value);
            idx += 1;
        }

        if row_wise_results.is_empty() {
            let field = self.field()?;
            let out_type = field.data_type();
            Ok(new_empty_array(out_type))
        } else {
            ScalarValue::iter_to_array(row_wise_results)
        }
    }
}

/// Filters a single array with the provided boolean mask.
pub(crate) fn filter_array(array: &ArrayRef, mask: &BooleanArray) -> Result<ArrayRef> {
    arrow_filter(array.as_ref(), mask)
        .map(|a| a as ArrayRef)
        .map_err(|e| arrow_datafusion_err!(e))
}

/// Filters a list of arrays with the provided boolean mask.
pub(crate) fn filter_arrays(
    arrays: &[ArrayRef],
    mask: &BooleanArray,
) -> Result<Vec<ArrayRef>> {
    arrays.iter().map(|arr| filter_array(arr, mask)).collect()
}

/// Determines whether the end bound calculation for a window frame context is
/// safe, meaning that the end bound stays the same, regardless of future data,
/// based on the current sort expressions and ORDER BY columns. This function
/// delegates work to specific functions for each frame type.
///
/// # Parameters
///
/// * `window_frame_ctx`: The context of the window frame being evaluated.
/// * `order_bys`: A slice of `ArrayRef` representing the ORDER BY columns.
/// * `most_recent_order_bys`: An optional reference to the most recent ORDER BY
///   columns.
/// * `sort_exprs`: Defines the lexicographical ordering in question.
/// * `idx`: The current index in the window frame.
///
/// # Returns
///
/// A `Result` which is `Ok(true)` if the end bound is safe, `Ok(false)` otherwise.
pub(crate) fn is_end_bound_safe(
    window_frame_ctx: &WindowFrameContext,
    order_bys: &[ArrayRef],
    most_recent_order_bys: Option<&[ArrayRef]>,
    sort_exprs: &[PhysicalSortExpr],
    idx: usize,
) -> Result<bool> {
    if sort_exprs.is_empty() {
        // Early return if no sort expressions are present:
        return Ok(false);
    }

    match window_frame_ctx {
        WindowFrameContext::Rows(window_frame) => {
            is_end_bound_safe_for_rows(&window_frame.end_bound)
        }
        WindowFrameContext::Range { window_frame, .. } => is_end_bound_safe_for_range(
            &window_frame.end_bound,
            &order_bys[0],
            most_recent_order_bys.map(|items| &items[0]),
            &sort_exprs[0].options,
            idx,
        ),
        WindowFrameContext::Groups {
            window_frame,
            state,
        } => is_end_bound_safe_for_groups(
            &window_frame.end_bound,
            state,
            &order_bys[0],
            most_recent_order_bys.map(|items| &items[0]),
            &sort_exprs[0].options,
        ),
    }
}

/// For row-based window frames, determines whether the end bound calculation
/// is safe, which is trivially the case for `Preceding` and `CurrentRow` bounds.
/// For 'Following' bounds, it compares the bound value to zero to ensure that
/// it doesn't extend beyond the current row.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for row-based window frames.
fn is_end_bound_safe_for_rows(end_bound: &WindowFrameBound) -> Result<bool> {
    if let WindowFrameBound::Following(value) = end_bound {
        let zero = ScalarValue::new_zero(&value.data_type());
        Ok(zero.map(|zero| value.eq(&zero)).unwrap_or(false))
    } else {
        Ok(true)
    }
}

/// For row-based window frames, determines whether the end bound calculation
/// is safe by comparing it against specific values (zero, current row). It uses
/// the `is_row_ahead` helper function to determine if the current row is ahead
/// of the most recent row based on the ORDER BY column and sorting options.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
/// * `orderby_col`: Reference to the column used for ordering.
/// * `most_recent_ob_col`: Optional reference to the most recent order-by column.
/// * `sort_options`: The sorting options used in the window frame.
/// * `idx`: The current index in the window frame.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for range-based window frames.
fn is_end_bound_safe_for_range(
    end_bound: &WindowFrameBound,
    orderby_col: &ArrayRef,
    most_recent_ob_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
    idx: usize,
) -> Result<bool> {
    match end_bound {
        WindowFrameBound::Preceding(value) => {
            let zero = ScalarValue::new_zero(&value.data_type())?;
            if value.eq(&zero) {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(true)
            }
        }
        WindowFrameBound::CurrentRow => {
            is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
        }
        WindowFrameBound::Following(delta) => {
            let Some(most_recent_ob_col) = most_recent_ob_col else {
                return Ok(false);
            };
            let most_recent_row_value =
                ScalarValue::try_from_array(most_recent_ob_col, 0)?;
            let current_row_value = ScalarValue::try_from_array(orderby_col, idx)?;

            if sort_options.descending {
                current_row_value
                    .sub(delta)
                    .map(|value| value > most_recent_row_value)
            } else {
                current_row_value
                    .add(delta)
                    .map(|value| most_recent_row_value > value)
            }
        }
    }
}

/// For group-based window frames, determines whether the end bound calculation
/// is safe by considering the group offset and whether the current row is ahead
/// of the most recent row in terms of sorting. It checks if the end bound is
/// within the bounds of the current group based on group end indices.
///
/// # Parameters
///
/// * `end_bound`: Reference to the window frame bound in question.
/// * `state`: The state of the window frame for group calculations.
/// * `orderby_col`: Reference to the column used for ordering.
/// * `most_recent_ob_col`: Optional reference to the most recent order-by column.
/// * `sort_options`: The sorting options used in the window frame.
///
/// # Returns
///
/// A `Result` indicating whether the end bound is safe for group-based window frames.
fn is_end_bound_safe_for_groups(
    end_bound: &WindowFrameBound,
    state: &WindowFrameStateGroups,
    orderby_col: &ArrayRef,
    most_recent_ob_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
) -> Result<bool> {
    match end_bound {
        WindowFrameBound::Preceding(value) => {
            let zero = ScalarValue::new_zero(&value.data_type())?;
            if value.eq(&zero) {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(true)
            }
        }
        WindowFrameBound::CurrentRow => {
            is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
        }
        WindowFrameBound::Following(ScalarValue::UInt64(Some(offset))) => {
            let delta = state.group_end_indices.len() - state.current_group_idx;
            if delta == (*offset as usize) + 1 {
                is_row_ahead(orderby_col, most_recent_ob_col, sort_options)
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

/// This utility function checks whether `current_cols` is ahead of the `old_cols`
/// in terms of `sort_options`.
fn is_row_ahead(
    old_col: &ArrayRef,
    current_col: Option<&ArrayRef>,
    sort_options: &SortOptions,
) -> Result<bool> {
    let Some(current_col) = current_col else {
        return Ok(false);
    };
    if old_col.is_empty() || current_col.is_empty() {
        return Ok(false);
    }
    let last_value = ScalarValue::try_from_array(old_col, old_col.len() - 1)?;
    let current_value = ScalarValue::try_from_array(current_col, 0)?;
    let cmp = compare_rows(&[current_value], &[last_value], &[*sort_options])?;
    Ok(cmp.is_gt())
}

/// Get order by expression results inside `order_by_columns`.
pub(crate) fn get_orderby_values(order_by_columns: Vec<SortColumn>) -> Vec<ArrayRef> {
    order_by_columns.into_iter().map(|s| s.values).collect()
}

/// State for incrementally evaluating a window function
/// within a partition, created by [`WindowExpr::create_window_fn`].
#[derive(Debug)]
pub enum WindowFn {
    /// A "normal" window function, such as `lead` or `lag`, evaluated via a
    /// [`PartitionEvaluator`]. Despite the name, it is used for all window
    /// functions that are not aggregate functions.
    Builtin(Box<dyn PartitionEvaluator>),
    /// An aggregate function used as a window function, such as `avg` or
    /// `sum`, which is evaluated via an [`Accumulator`].
    Aggregate(Box<dyn Accumulator>),
}

/// Key for IndexMap for each unique partition
///
/// For instance, if window frame is `OVER(PARTITION BY a,b)`,
/// PartitionKey would consist of unique `[a,b]` pairs
pub type PartitionKey = Vec<ScalarValue>;

/// Stream-level context passed to [`WindowExpr::evaluate_stateful`].
///
/// This carries information that spans all partitions of the input, as
/// opposed to the per-partition state in [`PartitionBatches`] and
/// [`PartitionWindowAggStates`]. It is `non_exhaustive` so that fields can
/// be added without breaking implementors; construct it with
/// [`Default::default`] and the `with_*` builder methods.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct WindowEvalContext<'a> {
    /// A single-row batch containing the most recent input row, whichever
    /// partition that row belongs to. It is `Some` only when the input is
    /// ordered by the first ORDER BY column across partitions (`Linear`
    /// mode), in which case no future input row -- in any partition -- can
    /// precede it in that column; implementations can use this bound to
    /// decide whether pending window frames can be finalized before their
    /// partition receives more data.
    pub most_recent_row: Option<&'a RecordBatch>,
}

impl<'a> WindowEvalContext<'a> {
    /// Sets the most recent input row (see [`Self::most_recent_row`]).
    pub fn with_most_recent_row(mut self, batch: Option<&'a RecordBatch>) -> Self {
        self.most_recent_row = batch;
        self
    }
}

#[derive(Debug)]
pub struct WindowState {
    pub state: WindowAggState,
    pub window_fn: WindowFn,
    /// True once [`Self::aggregate_state`] has been called on this entry.
    /// Guards against a second destructive [`Accumulator::state`] read: the
    /// method itself errors on second call, and the observer loop in
    /// `BoundedWindowAggStream::publish_finalized_states` uses this as an
    /// early-skip so it doesn't attempt one. Independent of `state.is_end`,
    /// which is a group-closed signal that the pruning path also reads.
    pub published: bool,
}

impl WindowState {
    /// [`Accumulator::state`] if this window function is an aggregate, `None`
    /// otherwise (built-in functions like `row_number`, `rank`, `lead`/`lag`
    /// have no serializable accumulator state).
    ///
    /// [`Accumulator::state`] takes `&mut self` and its trait doc calls out
    /// that "this function should not be called twice, otherwise it will
    /// result in potentially non-deterministic behavior." Several built-in
    /// impls (`median`, `percentile_cont`, `string_agg`,
    /// `min_max_bytes`/`min_max_struct`) `std::mem::take` their internal
    /// buffers on call — a second call returns *empty* state, not the same
    /// state, so a downstream prefix-merge would silently lose every value
    /// the accumulator had ingested.
    ///
    /// Enforced at this layer: on first call we set [`Self::published`] and
    /// return the state; any later call errors rather than performing a
    /// destructive re-read.
    pub fn aggregate_state(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        if self.published {
            return exec_err!(
                "WindowState::aggregate_state called more than once; \
                 Accumulator::state is a destructive read for several \
                 built-in aggregates and a second call would silently lose data"
            );
        }
        let state = match &mut self.window_fn {
            WindowFn::Aggregate(accumulator) => Some(accumulator.state()?),
            WindowFn::Builtin(_) => None,
        };
        self.published = true;
        Ok(state)
    }
}

pub type PartitionWindowAggStates = IndexMap<PartitionKey, WindowState, RandomState>;

/// The IndexMap (i.e. an ordered HashMap) where record batches are separated for each partition.
pub type PartitionBatches = IndexMap<PartitionKey, PartitionBatchState, RandomState>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::window::window_expr::{WindowFn, WindowState, is_row_ahead};

    use arrow::array::{ArrayRef, Float64Array};
    use arrow::compute::SortOptions;
    use arrow::datatypes::DataType;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{Accumulator, window_state::WindowAggState};

    /// Minimal [`Accumulator`] whose `state()` records how many times it was
    /// called by returning the count as its single state element. Any second
    /// call would surface (were it allowed to happen) as `[UInt64(2)]`
    /// instead of `[UInt64(1)]`.
    #[derive(Debug)]
    struct CallCountingAccumulator {
        calls: usize,
    }

    impl Accumulator for CallCountingAccumulator {
        fn update_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
        fn evaluate(&mut self) -> Result<ScalarValue> {
            Ok(ScalarValue::Null)
        }
        fn size(&self) -> usize {
            size_of::<Self>()
        }
        fn state(&mut self) -> Result<Vec<ScalarValue>> {
            self.calls += 1;
            Ok(vec![ScalarValue::UInt64(Some(self.calls as u64))])
        }
        fn merge_batch(&mut self, _states: &[ArrayRef]) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn aggregate_state_errors_on_second_call() -> Result<()> {
        // `Accumulator::state()` is a destructive read for several built-in
        // aggregates (median, percentile_cont, string_agg, min_max_bytes/
        // min_max_struct all `mem::take` their internal buffers). Its trait
        // doc says "should not be called twice"; `WindowState::aggregate_state`
        // enforces that at this layer by returning an error rather than
        // performing the second read.
        let acc: Box<dyn Accumulator> = Box::new(CallCountingAccumulator { calls: 0 });
        let mut ws = WindowState {
            state: WindowAggState::new(&DataType::UInt64)?,
            window_fn: WindowFn::Aggregate(acc),
            published: false,
        };
        let first = ws.aggregate_state()?;
        assert_eq!(first, Some(vec![ScalarValue::UInt64(Some(1))]));
        assert!(ws.published, "published must flip on successful publish");
        let err = ws.aggregate_state().unwrap_err().to_string();
        assert!(
            err.contains("called more than once"),
            "expected second-call error, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn test_is_row_ahead() -> Result<()> {
        let old_values: ArrayRef =
            Arc::new(Float64Array::from(vec![5.0, 7.0, 8.0, 9., 10.]));

        let new_values1: ArrayRef = Arc::new(Float64Array::from(vec![11.0]));
        let new_values2: ArrayRef = Arc::new(Float64Array::from(vec![10.0]));

        assert!(is_row_ahead(
            &old_values,
            Some(&new_values1),
            &SortOptions {
                descending: false,
                nulls_first: false
            }
        )?);
        assert!(!is_row_ahead(
            &old_values,
            Some(&new_values2),
            &SortOptions {
                descending: false,
                nulls_first: false
            }
        )?);

        Ok(())
    }
}
