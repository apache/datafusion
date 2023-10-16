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

use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::kernels::sort::SortColumn;
use arrow::compute::SortOptions;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::window_state::{
    PartitionBatchState, WindowAggState, WindowFrameContext,
};
use datafusion_expr::PartitionEvaluator;
use datafusion_expr::{Accumulator, WindowFrame};
use indexmap::IndexMap;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

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
    fn field(&self) -> Result<Field>;

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
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// Evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// Evaluate the window function against the batch. This function facilitates
    /// stateful, bounded-memory implementations.
    fn evaluate_stateful(
        &self,
        _partition_batches: &PartitionBatches,
        _window_agg_state: &mut PartitionWindowAggStates,
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
            .collect::<Result<Vec<SortColumn>>>()
    }

    /// Get the window frame of this [WindowExpr].
    fn get_window_frame(&self) -> &Arc<WindowFrame>;

    /// Return a flag indicating whether this [WindowExpr] can run with
    /// bounded memory.
    fn uses_bounded_memory(&self) -> bool;

    /// Get the reverse expression of this [WindowExpr].
    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>>;
}

/// Extension trait that adds common functionality to [`AggregateWindowExpr`]s
pub trait AggregateWindowExpr: WindowExpr {
    /// Get the accumulator for the window expression. Note that distinct
    /// window expressions may return distinct accumulators; e.g. sliding
    /// (non-sliding) expressions will return sliding (normal) accumulators.
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// Given current range and the last range, calculates the accumulator
    /// result for the range of interest.
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
    ) -> Result<ScalarValue>;

    /// Evaluates the window function against the batch.
    fn aggregate_evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut accumulator = self.get_accumulator()?;
        let mut last_range = Range { start: 0, end: 0 };
        let sort_options: Vec<SortOptions> =
            self.order_by().iter().map(|o| o.options).collect();
        let mut window_frame_ctx =
            WindowFrameContext::new(self.get_window_frame().clone(), sort_options);
        self.get_result_column(
            &mut accumulator,
            batch,
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
    ) -> Result<()> {
        let field = self.field()?;
        let out_type = field.data_type();
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let accumulator = self.get_accumulator()?;
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(out_type)?,
                        window_fn: WindowFn::Aggregate(accumulator),
                    },
                );
            };
            let window_state =
                window_agg_state.get_mut(partition_row).ok_or_else(|| {
                    DataFusionError::Execution("Cannot find state".to_string())
                })?;
            let accumulator = match &mut window_state.window_fn {
                WindowFn::Aggregate(accumulator) => accumulator,
                _ => unreachable!(),
            };
            let state = &mut window_state.state;
            let record_batch = &partition_batch_state.record_batch;

            // If there is no window state context, initialize it.
            let window_frame_ctx = state.window_frame_ctx.get_or_insert_with(|| {
                let sort_options: Vec<SortOptions> =
                    self.order_by().iter().map(|o| o.options).collect();
                WindowFrameContext::new(self.get_window_frame().clone(), sort_options)
            });
            let out_col = self.get_result_column(
                accumulator,
                record_batch,
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
    fn get_result_column(
        &self,
        accumulator: &mut Box<dyn Accumulator>,
        record_batch: &RecordBatch,
        last_range: &mut Range<usize>,
        window_frame_ctx: &mut WindowFrameContext,
        mut idx: usize,
        not_end: bool,
    ) -> Result<ArrayRef> {
        let values = self.evaluate_args(record_batch)?;
        let order_bys = get_orderby_values(self.order_by_columns(record_batch)?);
        // We iterate on each row to perform a running calculation.
        let length = values[0].len();
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        while idx < length {
            // Start search from the last_range. This squeezes searched range.
            let cur_range =
                window_frame_ctx.calculate_range(&order_bys, last_range, length, idx)?;
            // Exit if the range extends all the way:
            if cur_range.end == length && not_end {
                break;
            }
            let value = self.get_aggregate_result_inside_range(
                last_range,
                &cur_range,
                &values,
                accumulator,
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
/// Get order by expression results inside `order_by_columns`.
pub(crate) fn get_orderby_values(order_by_columns: Vec<SortColumn>) -> Vec<ArrayRef> {
    order_by_columns.into_iter().map(|s| s.values).collect()
}

#[derive(Debug)]
pub enum WindowFn {
    Builtin(Box<dyn PartitionEvaluator>),
    Aggregate(Box<dyn Accumulator>),
}

/// State for the RANK(percent_rank, rank, dense_rank) built-in window function.
#[derive(Debug, Clone, Default)]
pub struct RankState {
    /// The last values for rank as these values change, we increase n_rank
    pub last_rank_data: Vec<ScalarValue>,
    /// The index where last_rank_boundary is started
    pub last_rank_boundary: usize,
    /// Keep the number of entries in current rank
    pub current_group_count: usize,
    /// Rank number kept from the start
    pub n_rank: usize,
}

/// State for the 'ROW_NUMBER' built-in window function.
#[derive(Debug, Clone, Default)]
pub struct NumRowsState {
    pub n_rows: usize,
}

/// Tag to differentiate special use cases of the NTH_VALUE built-in window function.
#[derive(Debug, Copy, Clone)]
pub enum NthValueKind {
    First,
    Last,
    Nth(u32),
}

#[derive(Debug, Clone)]
pub struct NthValueState {
    pub range: Range<usize>,
    // In certain cases, we can finalize the result early. Consider this usage:
    // ```
    //  FIRST_VALUE(increasing_col) OVER window AS my_first_value
    //  WINDOW (ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS window
    // ```
    // The result will always be the first entry in the table. We can store such
    // early-finalizing results and then just reuse them as necessary. This opens
    // opportunities to prune our datasets.
    pub finalized_result: Option<ScalarValue>,
    pub kind: NthValueKind,
}

/// Key for IndexMap for each unique partition
///
/// For instance, if window frame is `OVER(PARTITION BY a,b)`,
/// PartitionKey would consist of unique `[a,b]` pairs
pub type PartitionKey = Vec<ScalarValue>;

#[derive(Debug)]
pub struct WindowState {
    pub state: WindowAggState,
    pub window_fn: WindowFn,
}
pub type PartitionWindowAggStates = IndexMap<PartitionKey, WindowState>;

/// The IndexMap (i.e. an ordered HashMap) where record batches are separated for each partition.
pub type PartitionBatches = IndexMap<PartitionKey, PartitionBatchState>;
