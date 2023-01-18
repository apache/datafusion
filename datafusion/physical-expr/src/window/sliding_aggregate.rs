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
use std::iter::IntoIterator;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, WindowFrame, WindowFrameUnits};

use crate::window::window_expr::{reverse_order_bys, WindowFn, WindowFunctionState};
use crate::window::{
    AggregateWindowExpr, PartitionBatches, PartitionWindowAggStates, WindowAggState,
    WindowState,
};
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};

use super::window_frame_state::WindowFrameContext;

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct SlidingAggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl SlidingAggregateWindowExpr {
    /// create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<dyn AggregateExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }

    /// Get aggregate expr of AggregateWindowExpr
    pub fn get_aggregate_expr(&self) -> &Arc<dyn AggregateExpr> {
        &self.aggregate
    }
}

/// peer based evaluation based on the fact that batch is pre-sorted given the sort columns
/// and then per partition point we'll evaluate the peer group (e.g. SUM or MAX gives the same
/// results for peers) and concatenate the results.

impl WindowExpr for SlidingAggregateWindowExpr {
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
        let mut accumulator = self.aggregate.create_sliding_accumulator()?;

        let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
        let mut last_range = Range { start: 0, end: 0 };
        let mut idx = 0;
        self.get_result_column(
            &mut accumulator,
            batch,
            &mut window_frame_ctx,
            &mut last_range,
            &mut idx,
            true,
        )
    }

    fn evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        let field = self.aggregate.field()?;
        let out_type = field.data_type();
        for (partition_row, partition_batch_state) in partition_batches.iter() {
            if !window_agg_state.contains_key(partition_row) {
                let accumulator = self.aggregate.create_sliding_accumulator()?;
                window_agg_state.insert(
                    partition_row.clone(),
                    WindowState {
                        state: WindowAggState::new(
                            out_type,
                            WindowFunctionState::AggregateState(vec![]),
                        )?,
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
            let mut state = &mut window_state.state;
            state.is_end = partition_batch_state.is_end;

            let mut idx = state.last_calculated_index;
            let mut last_range = state.window_frame_range.clone();
            let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
            let out_col = self.get_result_column(
                accumulator,
                &partition_batch_state.record_batch,
                &mut window_frame_ctx,
                &mut last_range,
                &mut idx,
                state.is_end,
            )?;
            state.last_calculated_index = idx;
            state.window_frame_range = last_range.clone();

            state.out_col = concat(&[&state.out_col, &out_col])?;
            let num_rows = partition_batch_state.record_batch.num_rows();
            state.n_row_result_missing = num_rows - state.last_calculated_index;

            state.window_function_state =
                WindowFunctionState::AggregateState(accumulator.state()?);
        }
        Ok(())
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn get_window_frame(&self) -> &Arc<WindowFrame> {
        &self.window_frame
    }

    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>> {
        self.aggregate.reverse_expr().map(|reverse_expr| {
            let reverse_window_frame = self.window_frame.reverse();
            if reverse_window_frame.start_bound.is_unbounded() {
                Arc::new(AggregateWindowExpr::new(
                    reverse_expr,
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            } else {
                Arc::new(SlidingAggregateWindowExpr::new(
                    reverse_expr,
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            }
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        // NOTE: Currently, groups queries do not support the bounded memory variant.
        self.aggregate.supports_bounded_execution()
            && !self.window_frame.start_bound.is_unbounded()
            && !self.window_frame.end_bound.is_unbounded()
            && !matches!(self.window_frame.units, WindowFrameUnits::Groups)
    }
}

impl SlidingAggregateWindowExpr {
    /// For given range calculate accumulator result inside range on value_slice and
    /// update accumulator state
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
    ) -> Result<ScalarValue> {
        let value = if cur_range.start == cur_range.end {
            // We produce None if the window is empty.
            ScalarValue::try_from(self.aggregate.field()?.data_type())?
        } else {
            // Accumulate any new rows that have entered the window:
            let update_bound = cur_range.end - last_range.end;
            if update_bound > 0 {
                let update: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.end, update_bound))
                    .collect();
                accumulator.update_batch(&update)?
            }
            // Remove rows that have now left the window:
            let retract_bound = cur_range.start - last_range.start;
            if retract_bound > 0 {
                let retract: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.start, retract_bound))
                    .collect();
                accumulator.retract_batch(&retract)?
            }
            accumulator.evaluate()?
        };
        Ok(value)
    }

    fn get_result_column(
        &self,
        accumulator: &mut Box<dyn Accumulator>,
        record_batch: &RecordBatch,
        window_frame_ctx: &mut WindowFrameContext,
        last_range: &mut Range<usize>,
        idx: &mut usize,
        is_end: bool,
    ) -> Result<ArrayRef> {
        let (values, order_bys) = self.get_values_orderbys(record_batch)?;
        // We iterate on each row to perform a running calculation.
        let length = values[0].len();
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        let field = self.aggregate.field()?;
        let out_type = field.data_type();
        while *idx < length {
            let cur_range = window_frame_ctx.calculate_range(
                &order_bys,
                &sort_options,
                length,
                *idx,
            )?;
            // Exit if range end index is length, need kind of flag to stop
            if cur_range.end == length && !is_end {
                break;
            }
            let value = self.get_aggregate_result_inside_range(
                last_range,
                &cur_range,
                &values,
                accumulator,
            )?;
            row_wise_results.push(value);
            last_range.start = cur_range.start;
            last_range.end = cur_range.end;
            *idx += 1;
        }
        Ok(if row_wise_results.is_empty() {
            ScalarValue::try_from(out_type)?.to_array_of_size(0)
        } else {
            ScalarValue::iter_to_array(row_wise_results.into_iter())?
        })
    }
}
