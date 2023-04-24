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

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::window_frame_state::WindowFrameContext;
use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::kernels::sort::SortColumn;
use arrow::compute::{concat, SortOptions};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, WindowFrame};
use indexmap::IndexMap;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// A window expression that:
/// * knows its resulting field
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`](std::any::Any) so that it can be
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
        Err(DataFusionError::Internal(format!(
            "evaluate_stateful is not implemented for {}",
            self.name()
        )))
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

    /// Get sort columns that can be used for peer evaluation, empty if absent
    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        let order_by_columns = self.order_by_columns(batch)?;
        Ok(order_by_columns)
    }

    /// Get values columns (argument of Window Function)
    /// and order by columns (columns of the ORDER BY expression) used in evaluators
    fn get_values_orderbys(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<(Vec<ArrayRef>, Vec<ArrayRef>)> {
        let values = self.evaluate_args(record_batch)?;
        let order_by_columns = self.order_by_columns(record_batch)?;
        let order_bys: Vec<ArrayRef> =
            order_by_columns.iter().map(|s| s.values.clone()).collect();
        Ok((values, order_bys))
    }

    /// Get the window frame of this [WindowExpr].
    fn get_window_frame(&self) -> &Arc<WindowFrame>;

    /// Return a flag indicating whether this [WindowExpr] can run with
    /// bounded memory.
    fn uses_bounded_memory(&self) -> bool;

    /// Get the reverse expression of this [WindowExpr].
    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>>;
}

/// Trait for different `AggregateWindowExpr`s (`PlainAggregateWindowExpr`, `SlidingAggregateWindowExpr`)
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
        let (values, order_bys) = self.get_values_orderbys(record_batch)?;
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
            ScalarValue::iter_to_array(row_wise_results.into_iter())
        }
    }
}

/// Reverses the ORDER BY expression, which is useful during equivalent window
/// expression construction. For instance, 'ORDER BY a ASC, NULLS LAST' turns into
/// 'ORDER BY a DESC, NULLS FIRST'.
pub fn reverse_order_bys(order_bys: &[PhysicalSortExpr]) -> Vec<PhysicalSortExpr> {
    order_bys
        .iter()
        .map(|e| PhysicalSortExpr {
            expr: e.expr.clone(),
            options: !e.options,
        })
        .collect()
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

#[derive(Debug, Clone, Default)]
pub struct LeadLagState {
    pub idx: usize,
}

#[derive(Debug, Clone, Default)]
pub enum BuiltinWindowState {
    Rank(RankState),
    NumRows(NumRowsState),
    NthValue(NthValueState),
    LeadLag(LeadLagState),
    #[default]
    Default,
}

#[derive(Debug)]
pub struct WindowAggState {
    /// The range that we calculate the window function
    pub window_frame_range: Range<usize>,
    pub window_frame_ctx: Option<WindowFrameContext>,
    /// The index of the last row that its result is calculated inside the partition record batch buffer.
    pub last_calculated_index: usize,
    /// The offset of the deleted row number
    pub offset_pruned_rows: usize,
    /// Stores the results calculated by window frame
    pub out_col: ArrayRef,
    /// Keeps track of how many rows should be generated to be in sync with input record_batch.
    // (For each row in the input record batch we need to generate a window result).
    pub n_row_result_missing: usize,
    /// flag indicating whether we have received all data for this partition
    pub is_end: bool,
}

impl WindowAggState {
    pub fn prune_state(&mut self, n_prune: usize) {
        self.window_frame_range = Range {
            start: self.window_frame_range.start - n_prune,
            end: self.window_frame_range.end - n_prune,
        };
        self.last_calculated_index -= n_prune;
        self.offset_pruned_rows += n_prune;

        match self.window_frame_ctx.as_mut() {
            // Rows have no state do nothing
            Some(WindowFrameContext::Rows(_)) => {}
            Some(WindowFrameContext::Range { .. }) => {}
            Some(WindowFrameContext::Groups { state, .. }) => {
                let mut n_group_to_del = 0;
                for (_, end_idx) in &state.group_end_indices {
                    if n_prune < *end_idx {
                        break;
                    }
                    n_group_to_del += 1;
                }
                state.group_end_indices.drain(0..n_group_to_del);
                state
                    .group_end_indices
                    .iter_mut()
                    .for_each(|(_, start_idx)| *start_idx -= n_prune);
                state.current_group_idx -= n_group_to_del;
            }
            None => {}
        };
    }
}

impl WindowAggState {
    pub fn update(
        &mut self,
        out_col: &ArrayRef,
        partition_batch_state: &PartitionBatchState,
    ) -> Result<()> {
        self.last_calculated_index += out_col.len();
        self.out_col = concat(&[&self.out_col, &out_col])?;
        self.n_row_result_missing =
            partition_batch_state.record_batch.num_rows() - self.last_calculated_index;
        self.is_end = partition_batch_state.is_end;
        Ok(())
    }
}

/// State for each unique partition determined according to PARTITION BY column(s)
#[derive(Debug)]
pub struct PartitionBatchState {
    /// The record_batch belonging to current partition
    pub record_batch: RecordBatch,
    /// Flag indicating whether we have received all data for this partition
    pub is_end: bool,
    /// Number of rows emitted for each partition
    pub n_out_row: usize,
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

impl WindowAggState {
    pub fn new(out_type: &DataType) -> Result<Self> {
        let empty_out_col = ScalarValue::try_from(out_type)?.to_array_of_size(0);
        Ok(Self {
            window_frame_range: Range { start: 0, end: 0 },
            window_frame_ctx: None,
            last_calculated_index: 0,
            offset_pruned_rows: 0,
            out_col: empty_out_col,
            n_row_result_missing: 0,
            is_end: false,
        })
    }
}
