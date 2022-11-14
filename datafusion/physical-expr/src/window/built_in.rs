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

use super::BuiltInWindowFunctionExpr;
use super::WindowExpr;
use crate::window::window_expr::PartitionBatches;
use crate::window::{PartitionWindowAggStates, WindowAggState};
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use arrow::array::Array;
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::utils::WindowSortKeys;
use datafusion_expr::WindowFrame;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    expr: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<Arc<WindowFrame>>,
}

impl BuiltInWindowExpr {
    /// create a new built-in window function expression
    pub fn new(
        expr: Arc<dyn BuiltInWindowFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<Arc<WindowFrame>>,
    ) -> Self {
        Self {
            expr,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
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
        let evaluator = self.expr.create_evaluator(batch)?;
        let num_rows = batch.num_rows();
        let partition_columns = self.partition_columns(batch)?;
        let partition_points =
            self.evaluate_partition_points(num_rows, &partition_columns)?;

        let results = if evaluator.uses_window_frame() {
            let sort_options: Vec<SortOptions> =
                self.order_by.iter().map(|o| o.options).collect();
            let columns = self.sort_columns(batch)?;
            let order_columns: Vec<&ArrayRef> =
                columns.iter().map(|s| &s.values).collect();
            // Sort values, this will make the same partitions consecutive. Also, within the partition
            // range, values will be sorted.
            let order_bys = &order_columns[self.partition_by.len()..];
            let window_frame = if !order_bys.is_empty() && self.window_frame.is_none() {
                // OVER (ORDER BY a) case
                // We create an implicit window for ORDER BY.
                Some(Arc::new(WindowFrame::default()))
            } else {
                self.window_frame.clone()
            };
            let mut row_wise_results = vec![];
            for partition_range in &partition_points {
                let length = partition_range.end - partition_range.start;
                let slice_order_bys = order_bys
                    .iter()
                    .map(|v| v.slice(partition_range.start, length))
                    .collect::<Vec<_>>();
                // We iterate on each row to calculate window frame range and and window function result
                for idx in 0..length {
                    let range = self.calculate_range(
                        &window_frame,
                        &slice_order_bys,
                        &sort_options,
                        num_rows,
                        idx,
                        &vec![],
                    )?;
                    let range = Range {
                        start: partition_range.start + range.start,
                        end: partition_range.start + range.end,
                    };
                    let value = evaluator.evaluate_inside_range(range)?;
                    row_wise_results.push(value.to_array());
                }
            }
            row_wise_results
        } else if evaluator.include_rank() {
            let columns = self.sort_columns(batch)?;
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &columns)?;
            evaluator.evaluate_with_rank(partition_points, sort_partition_points)?
        } else {
            evaluator.evaluate(partition_points)?
        };
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        concat(&results).map_err(DataFusionError::ArrowError)
    }

    /// evaluate the window function values against the batch
    fn evaluate_stream(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
        window_sort_keys: &WindowSortKeys,
        is_end: bool,
    ) -> Result<()> {
        let window_sort_keys =
            window_sort_keys[self.partition_by.len()..window_sort_keys.len()].to_vec();
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        for (partition_row, partition_batch) in partition_batches.iter() {
            let mut evaluator = self.expr.create_evaluator(partition_batch)?;
            let field = self.expr.field()?;
            let out_type = field.data_type();
            let mut state = if let Some(state) = window_agg_state.get(partition_row) {
                state.clone()
            } else {
                let state = WindowAggState::new(out_type)?;
                window_agg_state.insert(partition_row.clone(), state.clone());
                state.clone()
            };
            evaluator.set_state(&state.builtin_window_state)?;
            let num_rows = partition_batch.num_rows();

            let columns = self.sort_columns(partition_batch)?;
            let sort_partition_points =
                self.evaluate_partition_points(num_rows, &columns)?;
            let order_bys: Vec<ArrayRef> =
                columns.iter().map(|s| s.values.clone()).collect();
            let order_bys = order_bys[self.partition_by.len()..order_bys.len()].to_vec();

            // We iterate on each row to perform a running calculation.
            // First, cur_range is calculated, then it is compared with last_range.
            let mut row_wise_results: Vec<ScalarValue> = vec![];
            let mut last_range = state.cur_range.clone();
            for idx in state.last_idx..num_rows {
                state.cur_range = if !evaluator.uses_window_frame() {
                    evaluator.get_range(&state)?
                } else {
                    self.calculate_range(
                        &self.window_frame,
                        &order_bys,
                        &sort_options,
                        num_rows,
                        idx,
                        &window_sort_keys,
                    )?
                };

                evaluator.update_state(&state, &order_bys, &sort_partition_points)?;
                // exit if range end index is length, need kind of flag to stop
                if state.cur_range.end == num_rows && !is_end {
                    state.cur_range = last_range.clone();
                    break;
                }
                if state.cur_range.start == state.cur_range.end {
                    // We produce None if the window is empty.
                    row_wise_results
                        .push(ScalarValue::try_from(self.expr.field()?.data_type())?)
                } else {
                    let res = evaluator.evaluate_stream(&state)?;
                    row_wise_results.push(res);
                }
                last_range = state.cur_range.clone();
                state.last_idx = idx + 1;
            }
            state.cur_range = last_range;
            let out_col = if !row_wise_results.is_empty() {
                ScalarValue::iter_to_array(row_wise_results.into_iter())?
            } else {
                let a = ScalarValue::try_from(self.expr.field()?.data_type())?;
                a.to_array_of_size(0)
            };

            state.out_col = concat(&[&state.out_col, &out_col])?;
            state.n_row_result_missing = num_rows - state.last_idx;

            state.builtin_window_state = evaluator.state()?;
            window_agg_state.insert(partition_row.clone(), state.clone());
        }
        Ok(())
    }
}
