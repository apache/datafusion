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

//! Physical exec for built-in window function expressions.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use super::window_frame_state::WindowFrameContext;
use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::window::window_expr::{
    reverse_order_bys, BuiltinWindowState, NthValueKind, NthValueState, WindowFn,
};
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, WindowAggState, WindowState,
};
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::{concat, SortOptions};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{WindowFrame, WindowFrameUnits};

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    expr: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl BuiltInWindowExpr {
    /// create a new built-in window function expression
    pub fn new(
        expr: Arc<dyn BuiltInWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        Self {
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }

    /// Get BuiltInWindowFunction expr of BuiltInWindowExpr
    pub fn get_built_in_func_expr(&self) -> &Arc<dyn BuiltInWindowFunctionExpr> {
        &self.expr
    }
}

impl WindowExpr for BuiltInWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.expr.name()
    }

    fn field(&self) -> Result<Field> {
        self.expr.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.expressions()
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let evaluator = self.expr.create_evaluator()?;
        let num_rows = batch.num_rows();
        if self.expr.uses_window_frame() {
            let sort_options: Vec<SortOptions> =
                self.order_by.iter().map(|o| o.options).collect();
            let mut row_wise_results = vec![];

            let (values, order_bys) = self.get_values_orderbys(batch)?;
            let mut window_frame_ctx = WindowFrameContext::new(
                &self.window_frame,
                sort_options,
                Range { start: 0, end: 0 },
            );
            // We iterate on each row to calculate window frame range and and window function result
            for idx in 0..num_rows {
                let range =
                    window_frame_ctx.calculate_range(&order_bys, num_rows, idx)?;
                let value = evaluator.evaluate_inside_range(&values, &range)?;
                row_wise_results.push(value);
            }
            ScalarValue::iter_to_array(row_wise_results.into_iter())
        } else if evaluator.include_rank() {
            let columns = self.sort_columns(batch)?;
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &columns)?;
            evaluator.evaluate_with_rank(num_rows, &sort_partition_points)
        } else {
            let (values, _) = self.get_values_orderbys(batch)?;
            evaluator.evaluate(&values, num_rows)
        }
    }

    /// Evaluate the window function against the batch. This function facilitates
    /// stateful, bounded-memory implementations.
    fn evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        let field = self.expr.field()?;
        let out_type = field.data_type();
        let sort_options = self.order_by.iter().map(|o| o.options).collect::<Vec<_>>();
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let evaluator = self.expr.create_evaluator()?;
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(out_type)?,
                        window_fn: WindowFn::Builtin(evaluator),
                    },
                );
            };
            let window_state =
                window_agg_state.get_mut(partition_row).ok_or_else(|| {
                    DataFusionError::Execution("Cannot find state".to_string())
                })?;
            let evaluator = match &mut window_state.window_fn {
                WindowFn::Builtin(evaluator) => evaluator,
                _ => unreachable!(),
            };
            let mut state = &mut window_state.state;
            state.is_end = partition_batch_state.is_end;

            let (values, order_bys) =
                self.get_values_orderbys(&partition_batch_state.record_batch)?;

            // We iterate on each row to perform a running calculation.
            let record_batch = &partition_batch_state.record_batch;
            let num_rows = record_batch.num_rows();
            let last_range = state.window_frame_range.clone();
            let mut window_frame_ctx = WindowFrameContext::new(
                &self.window_frame,
                sort_options.clone(),
                // Start search from the last range
                last_range,
            );
            let sort_partition_points = if evaluator.include_rank() {
                let columns = self.sort_columns(record_batch)?;
                self.evaluate_partition_points(num_rows, &columns)?
            } else {
                vec![]
            };
            let mut row_wise_results: Vec<ScalarValue> = vec![];
            let mut last_range = state.window_frame_range.clone();
            for idx in state.last_calculated_index..num_rows {
                state.window_frame_range = if self.expr.uses_window_frame() {
                    window_frame_ctx.calculate_range(&order_bys, num_rows, idx)
                } else {
                    evaluator.get_range(state, num_rows)
                }?;
                evaluator.update_state(state, &order_bys, &sort_partition_points)?;

                let frame_range = &state.window_frame_range;
                // Exit if the range extends all the way:
                if frame_range.end == num_rows && !state.is_end {
                    break;
                }
                row_wise_results.push(evaluator.evaluate_stateful(&values)?);
                last_range.clone_from(frame_range);
                state.last_calculated_index += 1;
            }
            state.window_frame_range = last_range;
            let out_col = if row_wise_results.is_empty() {
                new_empty_array(out_type)
            } else {
                ScalarValue::iter_to_array(row_wise_results.into_iter())?
            };

            state.out_col = concat(&[&state.out_col, &out_col])?;
            state.n_row_result_missing = num_rows - state.last_calculated_index;
            if self.window_frame.start_bound.is_unbounded() {
                let mut evaluator_state = evaluator.state()?;
                if let BuiltinWindowState::NthValue(nth_value_state) =
                    &mut evaluator_state
                {
                    memoize_nth_value(state, nth_value_state)?;
                    evaluator.set_state(&evaluator_state)?;
                }
            }
        }
        Ok(())
    }

    fn get_window_frame(&self) -> &Arc<WindowFrame> {
        &self.window_frame
    }

    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>> {
        self.expr.reverse_expr().map(|reverse_expr| {
            Arc::new(BuiltInWindowExpr::new(
                reverse_expr,
                &self.partition_by.clone(),
                &reverse_order_bys(&self.order_by),
                Arc::new(self.window_frame.reverse()),
            )) as _
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        // NOTE: Currently, groups queries do not support the bounded memory variant.
        self.expr.supports_bounded_execution()
            && (!self.expr.uses_window_frame()
                || !(self.window_frame.end_bound.is_unbounded()
                    || matches!(self.window_frame.units, WindowFrameUnits::Groups)))
    }
}

// When the window frame has a fixed beginning (e.g UNBOUNDED PRECEDING), for
// FIRST_VALUE, LAST_VALUE and NTH_VALUE functions: we can memoize result.
// Once result is calculated it will always stay same. Hence, we do not
// need to keep past data as we process the entire dataset. This feature
// enables us to prune rows from  table.
fn memoize_nth_value(
    state: &mut WindowAggState,
    nth_value_state: &mut NthValueState,
) -> Result<()> {
    let out = &state.out_col;
    let size = out.len();
    let (is_prunable, new_prunable) = match nth_value_state.kind {
        NthValueKind::First => {
            let n_range = state.window_frame_range.end - state.window_frame_range.start;
            (n_range > 0 && size > 0, true)
        }
        NthValueKind::Last => (true, false),
        NthValueKind::Nth(n) => {
            let n_range = state.window_frame_range.end - state.window_frame_range.start;
            (n_range >= (n as usize) && size >= (n as usize), true)
        }
    };
    if is_prunable {
        if nth_value_state.finalized_result.is_none() && new_prunable {
            let result = ScalarValue::try_from_array(out, size - 1)?;
            nth_value_state.finalized_result = Some(result);
        }
        if state.window_frame_range.end > 0 {
            state.window_frame_range.start = state.window_frame_range.end - 1;
        }
    }
    Ok(())
}
