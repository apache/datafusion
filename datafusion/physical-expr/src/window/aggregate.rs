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

//! Physical exec for aggregate window function expressions.

use std::any::Any;
use std::collections::HashMap;
use std::iter::IntoIterator;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::utils::WindowSortKeys;
use datafusion_expr::{Accumulator, WindowFrame};

use crate::window::window_expr::{
    AggregateWindowAccumulatorState, WindowAccumulatorResult,
};
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<Arc<WindowFrame>>,
}

impl AggregateWindowExpr {
    /// create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<dyn AggregateExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<Arc<WindowFrame>>,
    ) -> Self {
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }
}

/// peer based evaluation based on the fact that batch is pre-sorted given the sort columns
/// and then per partition point we'll evaluate the peer group (e.g. SUM or MAX gives the same
/// results for peers) and concatenate the results.

impl WindowExpr for AggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        self.aggregate.field()
    }

    fn name(&self) -> &str {
        self.aggregate.name()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.aggregate.expressions()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_columns = self.partition_columns(batch)?;
        let partition_points =
            self.evaluate_partition_points(batch.num_rows(), &partition_columns)?;
        let values = self.evaluate_args(batch)?;

        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        let columns = self.sort_columns(batch)?;
        let order_columns: Vec<&ArrayRef> = columns.iter().map(|s| &s.values).collect();
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
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        for partition_range in &partition_points {
            let mut accumulator = self.aggregate.create_accumulator()?;
            let length = partition_range.end - partition_range.start;
            let slice_order_bys = order_bys
                .iter()
                .map(|v| v.slice(partition_range.start, length))
                .collect::<Vec<_>>();
            let value_slice = values
                .iter()
                .map(|v| v.slice(partition_range.start, length))
                .collect::<Vec<_>>();

            let mut last_range: (usize, usize) = (0, 0);

            // We iterate on each row to perform a running calculation.
            // First, cur_range is calculated, then it is compared with last_range.
            for i in 0..length {
                let cur_range = self.calculate_range(
                    &window_frame,
                    &slice_order_bys,
                    &sort_options,
                    length,
                    i,
                    &vec![],
                )?;
                let value = if cur_range.0 == cur_range.1 {
                    // We produce None if the window is empty.
                    ScalarValue::try_from(self.aggregate.field()?.data_type())?
                } else {
                    // Accumulate any new rows that have entered the window:
                    let update_bound = cur_range.1 - last_range.1;
                    if update_bound > 0 {
                        let update: Vec<ArrayRef> = value_slice
                            .iter()
                            .map(|v| v.slice(last_range.1, update_bound))
                            .collect();
                        accumulator.update_batch(&update)?
                    }
                    // Remove rows that have now left the window:
                    let retract_bound = cur_range.0 - last_range.0;
                    if retract_bound > 0 {
                        let retract: Vec<ArrayRef> = value_slice
                            .iter()
                            .map(|v| v.slice(last_range.0, retract_bound))
                            .collect();
                        accumulator.retract_batch(&retract)?
                    }
                    accumulator.evaluate()?
                };
                row_wise_results.push(value);
                last_range = cur_range;
            }
        }
        ScalarValue::iter_to_array(row_wise_results.into_iter())
    }

    fn evaluate_stream(
        &self,
        batch_state: &HashMap<Vec<ScalarValue>, (u64, RecordBatch)>,
        window_accumulators: &mut HashMap<
            Vec<ScalarValue>,
            AggregateWindowAccumulatorState,
        >,
        window_sort_keys: &WindowSortKeys,
        is_end: bool,
    ) -> Result<Vec<WindowAccumulatorResult>> {
        let window_sort_keys =
            window_sort_keys[self.partition_by.len()..window_sort_keys.len()].to_vec();
        let mut results = vec![];
        for (partition_row, (ts, partition_batch)) in batch_state.iter() {
            let mut accumulator = self.aggregate.create_accumulator()?;
            let mut state = if let Some(state) = window_accumulators.get(partition_row) {
                let aggregate_state = state.aggregate_state.clone();
                accumulator.set_state(aggregate_state)?;
                state.clone()
            } else {
                let state = AggregateWindowAccumulatorState::default();
                window_accumulators.insert(partition_row.clone(), state.clone());
                state.clone()
            };
            let num_rows = partition_batch.num_rows();

            let values = self.evaluate_args(partition_batch)?;

            let columns = self.sort_columns(partition_batch)?;
            let order_bys: Vec<ArrayRef> =
                columns.iter().map(|s| s.values.clone()).collect();
            let order_bys = order_bys[self.partition_by.len()..order_bys.len()].to_vec();

            let res = self.calculate_running_window(
                &mut state,
                &mut accumulator,
                &values,
                &order_bys,
                is_end,
                &window_sort_keys,
            )?;
            let res = WindowAccumulatorResult {
                partition_id: partition_row.clone(),
                col: res,
                num_rows,
            };
            window_accumulators.insert(partition_row.clone(), state.clone());
            results.push((*ts, res));
        }
        results.sort_by(|(ts_l, _), (ts_r, _)| ts_l.partial_cmp(ts_r).unwrap());
        let results = results
            .into_iter()
            .map(|(_ts, elem)| elem)
            .collect::<Vec<WindowAccumulatorResult>>();
        Ok(results)
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn calculate_running_window(
        &self,
        state: &mut AggregateWindowAccumulatorState,
        accumulator: &mut Box<dyn Accumulator>,
        value_slice: &[ArrayRef],
        order_bys: &[ArrayRef],
        is_end: bool,
        window_sort_keys: &WindowSortKeys,
    ) -> Result<Option<ArrayRef>> {
        // We iterate on each row to perform a running calculation.
        // First, cur_range is calculated, then it is compared with last_range.
        let length = value_slice[0].len();
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        for i in state.last_idx..length {
            state.cur_range = self.calculate_range(
                &self.window_frame,
                order_bys,
                &sort_options,
                length,
                i,
                window_sort_keys,
            )?;
            // exit if range end index is length, need kind of flag to stop
            if state.cur_range.1 == length && !is_end {
                state.cur_range = state.last_range;
                break;
            }
            if state.cur_range.0 == state.cur_range.1 {
                // We produce None if the window is empty.
                row_wise_results
                    .push(ScalarValue::try_from(self.aggregate.field()?.data_type())?)
            } else {
                // Accumulate any new rows that have entered the window:
                let update_bound = state.cur_range.1 - state.last_range.1;
                if update_bound > 0 {
                    let update: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(state.last_range.1, update_bound))
                        .collect();
                    accumulator.update_batch(&update)?
                }
                // Remove rows that have now left the window:
                let retract_bound = state.cur_range.0 - state.last_range.0;
                if retract_bound > 0 {
                    let retract: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(state.last_range.0, retract_bound))
                        .collect();
                    accumulator.retract_batch(&retract)?
                }
                let res = accumulator.evaluate()?;
                row_wise_results.push(res);
            }
            state.last_range = state.cur_range;
            state.last_idx = i + 1;
        }
        state.aggregate_state = accumulator.state()?;

        if !row_wise_results.is_empty() {
            Ok(Some(ScalarValue::iter_to_array(
                row_wise_results.into_iter(),
            )?))
        } else {
            Ok(None)
        }
    }
}
