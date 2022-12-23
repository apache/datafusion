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

use super::window_frame_state::WindowFrameContext;
use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::window::window_expr::{
    reverse_order_bys, BuiltinWindowState, WindowFn, WindowFunctionState,
};
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, WindowAggState, WindowState,
};
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::WindowFrame;
use std::any::Any;
use std::sync::Arc;

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

            let length = batch.num_rows();
            let (values, order_bys) = self.get_values_orderbys(batch)?;
            let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
            // We iterate on each row to calculate window frame range and and window function result
            for idx in 0..length {
                let range = window_frame_ctx.calculate_range(
                    &order_bys,
                    &sort_options,
                    num_rows,
                    idx,
                )?;
                let value = evaluator.evaluate_inside_range(&values, range)?;
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

    /// evaluate the window function values against the batch
    fn evaluate_bounded(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let evaluator = self.expr.create_evaluator()?;
                let field = self.expr.field()?;
                let out_type = field.data_type();
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(
                            out_type,
                            WindowFunctionState::BuiltinWindowState(
                                BuiltinWindowState::Default,
                            ),
                        )?,
                        window_fn: WindowFn::Builtin(evaluator),
                    },
                );
            };
            let window_state = window_agg_state.get_mut(partition_row).unwrap();
            let evaluator = match &mut window_state.window_fn {
                WindowFn::Builtin(evaluator) => evaluator,
                _ => unreachable!(),
            };
            let mut state = &mut window_state.state;
            state.is_end = partition_batch_state.is_end;
            let num_rows = partition_batch_state.record_batch.num_rows();

            let columns = self.sort_columns(&partition_batch_state.record_batch)?;
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &columns)?;
            let (values, order_bys) =
                self.get_values_orderbys(&partition_batch_state.record_batch)?;

            // We iterate on each row to perform a running calculation.
            // First, current_range_of_sliding_window is calculated, then it is compared with last_range.
            let mut row_wise_results: Vec<ScalarValue> = vec![];
            let mut last_range = state.current_range_of_sliding_window.clone();
            let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
            for idx in state.last_calculated_index..num_rows {
                state.current_range_of_sliding_window = if !self.expr.uses_window_frame()
                {
                    evaluator.get_range(state, num_rows)?
                } else {
                    window_frame_ctx.calculate_range(
                        &order_bys,
                        &sort_options,
                        num_rows,
                        idx,
                    )?
                };
                evaluator.update_state(state, &order_bys, &sort_partition_points)?;
                // exit if range end index is length, need kind of flag to stop
                if state.current_range_of_sliding_window.end == num_rows
                    && !partition_batch_state.is_end
                {
                    state.current_range_of_sliding_window = last_range.clone();
                    break;
                }
                if state.current_range_of_sliding_window.start
                    == state.current_range_of_sliding_window.end
                {
                    // We produce None if the window is empty.
                    row_wise_results
                        .push(ScalarValue::try_from(self.expr.field()?.data_type())?)
                } else {
                    let res = evaluator.evaluate_bounded(&values)?;
                    row_wise_results.push(res);
                }
                last_range = state.current_range_of_sliding_window.clone();
                state.last_calculated_index = idx + 1;
            }
            state.current_range_of_sliding_window = last_range;
            let out_col = if !row_wise_results.is_empty() {
                ScalarValue::iter_to_array(row_wise_results.into_iter())?
            } else {
                let a = ScalarValue::try_from(self.expr.field()?.data_type())?;
                a.to_array_of_size(0)
            };

            state.out_col = concat(&[&state.out_col, &out_col])?;
            state.n_row_result_missing = num_rows - state.last_calculated_index;
            state.window_function_state =
                WindowFunctionState::BuiltinWindowState(evaluator.state()?);
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

    fn can_run_bounded(&self) -> bool {
        if self.expr.uses_window_frame() {
            self.expr.bounded_exec_supported()
                && !self.window_frame.start_bound.is_unbounded()
                && !self.window_frame.end_bound.is_unbounded()
        } else {
            self.expr.bounded_exec_supported()
        }
    }
}
