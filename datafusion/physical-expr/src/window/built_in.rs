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

use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::window::window_expr::WindowFn;
use crate::window::{PartitionBatches, PartitionWindowAggStates, WindowState};
use crate::{expressions::PhysicalSortExpr, reverse_order_bys, PhysicalExpr};
use arrow::array::{new_empty_array, ArrayRef};
use arrow::compute::SortOptions;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::window_state::WindowFrameContext;
use datafusion_expr::WindowFrame;

/// A window expr that takes the form of a [`BuiltInWindowFunctionExpr`].
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
        let mut evaluator = self.expr.create_evaluator()?;
        let num_rows = batch.num_rows();
        if evaluator.uses_window_frame() {
            let sort_options: Vec<SortOptions> =
                self.order_by.iter().map(|o| o.options).collect();
            let mut row_wise_results = vec![];

            let (values, order_bys) = self.get_values_orderbys(batch)?;
            let mut window_frame_ctx =
                WindowFrameContext::new(self.window_frame.clone(), sort_options);
            let mut last_range = Range { start: 0, end: 0 };
            // We iterate on each row to calculate window frame range and and window function result
            for idx in 0..num_rows {
                let range = window_frame_ctx.calculate_range(
                    &order_bys,
                    &last_range,
                    num_rows,
                    idx,
                )?;
                let value = evaluator.evaluate(&values, &range, idx)?;
                row_wise_results.push(value);
                last_range = range;
            }
            ScalarValue::iter_to_array(row_wise_results.into_iter())
        } else if evaluator.include_rank() {
            let columns = self.sort_columns(batch)?;
            let sort_partition_points = evaluate_partition_ranges(num_rows, &columns)?;
            evaluator.evaluate_all_with_rank(num_rows, &sort_partition_points)
        } else {
            let (values, _) = self.get_values_orderbys(batch)?;
            evaluator.evaluate_all(&values, num_rows)
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
            let window_state =
                if let Some(window_state) = window_agg_state.get_mut(partition_row) {
                    window_state
                } else {
                    let evaluator = self.expr.create_evaluator()?;
                    window_agg_state
                        .entry(partition_row.clone())
                        .or_insert(WindowState {
                            state: WindowAggState::new(out_type)?,
                            window_fn: WindowFn::Builtin(evaluator),
                        })
                };
            let evaluator = match &mut window_state.window_fn {
                WindowFn::Builtin(evaluator) => evaluator,
                _ => unreachable!(),
            };
            let state = &mut window_state.state;

            let (mut values, order_bys) =
                self.get_values_orderbys(&partition_batch_state.record_batch)?;
            values.extend(order_bys.clone());

            // We iterate on each row to perform a running calculation.
            let record_batch = &partition_batch_state.record_batch;
            let num_rows = record_batch.num_rows();
            let sort_partition_points = if evaluator.include_rank() {
                let columns = self.sort_columns(record_batch)?;
                evaluate_partition_ranges(num_rows, &columns)?
            } else {
                vec![]
            };
            let mut row_wise_results: Vec<ScalarValue> = vec![];
            for idx in state.last_calculated_index..num_rows {
                let frame_range = if evaluator.uses_window_frame() {
                    state
                        .window_frame_ctx
                        .get_or_insert_with(|| {
                            WindowFrameContext::new(
                                self.window_frame.clone(),
                                sort_options.clone(),
                            )
                        })
                        .calculate_range(
                            &order_bys,
                            // Start search from the last range
                            &state.window_frame_range,
                            num_rows,
                            idx,
                        )
                } else {
                    evaluator.get_range(idx, num_rows)
                }?;

                // Exit if the range extends all the way:
                if frame_range.end == num_rows && !partition_batch_state.is_end {
                    break;
                }
                // Update last range
                state.window_frame_range = frame_range;
                // evaluator.update_state(state, idx, &order_bys, &sort_partition_points)?;
                row_wise_results.push(evaluator.evaluate(
                    &values,
                    &state.window_frame_range,
                    idx,
                )?);
            }
            let out_col = if row_wise_results.is_empty() {
                new_empty_array(out_type)
            } else {
                ScalarValue::iter_to_array(row_wise_results.into_iter())?
            };

            state.update(&out_col, partition_batch_state)?;
            if self.window_frame.start_bound.is_unbounded() {
                evaluator.memoize(state)?;
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
        if let Ok(evaluator) = self.expr.create_evaluator() {
            evaluator.supports_bounded_execution()
                && (!evaluator.uses_window_frame()
                    || !self.window_frame.end_bound.is_unbounded())
        } else {
            false
        }
    }
}
