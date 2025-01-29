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

//! Physical exec for standard window function expressions.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use super::{StandardWindowFunctionExpr, WindowExpr};
use crate::window::window_expr::{get_orderby_values, WindowFn};
use crate::window::{PartitionBatches, PartitionWindowAggStates, WindowState};
use crate::{reverse_order_bys, EquivalenceProperties, PhysicalExpr};
use arrow::array::{new_empty_array, ArrayRef};
use arrow::compute::SortOptions;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_state::{WindowAggState, WindowFrameContext};
use datafusion_expr::WindowFrame;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

/// A window expr that takes the form of a [`StandardWindowFunctionExpr`].
#[derive(Debug)]
pub struct StandardWindowExpr {
    expr: Arc<dyn StandardWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: LexOrdering,
    window_frame: Arc<WindowFrame>,
}

impl StandardWindowExpr {
    /// create a new standard window function expression
    pub fn new(
        expr: Arc<dyn StandardWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &LexOrdering,
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        Self {
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.clone(),
            window_frame,
        }
    }

    /// Get StandardWindowFunction expr of StandardWindowExpr
    pub fn get_standard_func_expr(&self) -> &Arc<dyn StandardWindowFunctionExpr> {
        &self.expr
    }

    /// Adds any equivalent orderings generated by the `self.expr` to `builder`.
    ///
    /// If `self.expr` doesn't have an ordering, ordering equivalence properties
    /// are not updated. Otherwise, ordering equivalence properties are updated
    /// by the ordering of `self.expr`.
    pub fn add_equal_orderings(&self, eq_properties: &mut EquivalenceProperties) {
        let schema = eq_properties.schema();
        if let Some(fn_res_ordering) = self.expr.get_result_ordering(schema) {
            add_new_ordering_expr_with_partition_by(
                eq_properties,
                fn_res_ordering,
                &self.partition_by,
            );
        }
    }
}

impl WindowExpr for StandardWindowExpr {
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

    fn order_by(&self) -> &LexOrdering {
        self.order_by.as_ref()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let mut evaluator = self.expr.create_evaluator()?;
        let num_rows = batch.num_rows();
        if evaluator.uses_window_frame() {
            let sort_options: Vec<SortOptions> =
                self.order_by.iter().map(|o| o.options).collect();
            let mut row_wise_results = vec![];

            let mut values = self.evaluate_args(batch)?;
            let order_bys = get_orderby_values(self.order_by_columns(batch)?);
            let n_args = values.len();
            values.extend(order_bys);
            let order_bys_ref = &values[n_args..];

            let mut window_frame_ctx =
                WindowFrameContext::new(Arc::clone(&self.window_frame), sort_options);
            let mut last_range = Range { start: 0, end: 0 };
            // We iterate on each row to calculate window frame range and and window function result
            for idx in 0..num_rows {
                let range = window_frame_ctx.calculate_range(
                    order_bys_ref,
                    &last_range,
                    num_rows,
                    idx,
                )?;
                let value = evaluator.evaluate(&values, &range)?;
                row_wise_results.push(value);
                last_range = range;
            }
            ScalarValue::iter_to_array(row_wise_results)
        } else if evaluator.include_rank() {
            let columns = self.order_by_columns(batch)?;
            let sort_partition_points = evaluate_partition_ranges(num_rows, &columns)?;
            evaluator.evaluate_all_with_rank(num_rows, &sort_partition_points)
        } else {
            let values = self.evaluate_args(batch)?;
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

            let batch_ref = &partition_batch_state.record_batch;
            let mut values = self.evaluate_args(batch_ref)?;
            let order_bys = if evaluator.uses_window_frame() || evaluator.include_rank() {
                get_orderby_values(self.order_by_columns(batch_ref)?)
            } else {
                vec![]
            };
            let n_args = values.len();
            values.extend(order_bys);
            let order_bys_ref = &values[n_args..];

            // We iterate on each row to perform a running calculation.
            let record_batch = &partition_batch_state.record_batch;
            let num_rows = record_batch.num_rows();
            let mut row_wise_results: Vec<ScalarValue> = vec![];
            let is_causal = if evaluator.uses_window_frame() {
                self.window_frame.is_causal()
            } else {
                evaluator.is_causal()
            };
            for idx in state.last_calculated_index..num_rows {
                let frame_range = if evaluator.uses_window_frame() {
                    state
                        .window_frame_ctx
                        .get_or_insert_with(|| {
                            WindowFrameContext::new(
                                Arc::clone(&self.window_frame),
                                sort_options.clone(),
                            )
                        })
                        .calculate_range(
                            order_bys_ref,
                            // Start search from the last range
                            &state.window_frame_range,
                            num_rows,
                            idx,
                        )
                } else {
                    evaluator.get_range(idx, num_rows)
                }?;

                // Exit if the range is non-causal and extends all the way:
                if frame_range.end == num_rows
                    && !is_causal
                    && !partition_batch_state.is_end
                {
                    break;
                }
                // Update last range
                state.window_frame_range = frame_range;
                row_wise_results
                    .push(evaluator.evaluate(&values, &state.window_frame_range)?);
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
            Arc::new(StandardWindowExpr::new(
                reverse_expr,
                &self.partition_by.clone(),
                reverse_order_bys(self.order_by.as_ref()).as_ref(),
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

/// Adds new ordering expression into the existing ordering equivalence class based on partition by information.
pub(crate) fn add_new_ordering_expr_with_partition_by(
    eqp: &mut EquivalenceProperties,
    expr: PhysicalSortExpr,
    partition_by: &Vec<Arc<dyn PhysicalExpr>>,
) {
    if partition_by.is_empty() {
        // In the absence of a PARTITION BY, ordering of `self.expr` is global:
        eqp.add_new_orderings([LexOrdering::new(vec![expr])]);
    } else {
        // If we have a PARTITION BY, standard functions can not introduce
        // a global ordering unless the existing ordering is compatible
        // with PARTITION BY expressions. To elaborate, when PARTITION BY
        // expressions and existing ordering expressions are equal (w.r.t.
        // set equality), we can prefix the ordering of `self.expr` with
        // the existing ordering.
        let (mut ordering, _) = eqp.find_longest_permutation(partition_by);
        if ordering.len() == partition_by.len() {
            ordering.push(expr);
            eqp.add_new_orderings([ordering]);
        }
    }
}
