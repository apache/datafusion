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
use std::ops::Range;
use std::sync::Arc;

use crate::aggregate::AggregateFunctionExpr;
use crate::window::standard::add_new_ordering_expr_with_partition_by;
use crate::window::window_expr::{AggregateWindowExpr, WindowFn};
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, SlidingAggregateWindowExpr, WindowExpr,
};
use crate::{EquivalenceProperties, PhysicalExpr};

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

/// A window expr that takes the form of an aggregate function.
///
/// See comments on [`WindowExpr`] for more details.
#[derive(Debug)]
pub struct PlainAggregateWindowExpr {
    aggregate: Arc<AggregateFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
    is_constant_in_partition: bool,
}

impl PlainAggregateWindowExpr {
    /// Create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<AggregateFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
    ) -> Self {
        let is_constant_in_partition =
            Self::is_window_constant_in_partition(order_by, &window_frame);
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
            is_constant_in_partition,
        }
    }

    /// Get aggregate expr of AggregateWindowExpr
    pub fn get_aggregate_expr(&self) -> &AggregateFunctionExpr {
        &self.aggregate
    }

    pub fn add_equal_orderings(
        &self,
        eq_properties: &mut EquivalenceProperties,
        window_expr_index: usize,
    ) -> Result<()> {
        if let Some(expr) = self
            .get_aggregate_expr()
            .get_result_ordering(window_expr_index)
        {
            add_new_ordering_expr_with_partition_by(
                eq_properties,
                expr,
                &self.partition_by,
            )?;
        }
        Ok(())
    }

    // Returns true if every row in the partition has the same window frame. This allows
    // for preventing bound + function calculation for every row due to the values being the
    // same.
    //
    // This occurs when both bounds fall under either condition below:
    //  1. Bound is unbounded (`Preceding` or `Following`)
    //  2. Bound is `CurrentRow` while using `Range` units with no order by clause
    //  This results in an invalid range specification. Following PostgreSQL’s convention,
    //  we interpret this as the entire partition being used for the current window frame.
    fn is_window_constant_in_partition(
        order_by: &[PhysicalSortExpr],
        window_frame: &WindowFrame,
    ) -> bool {
        let is_constant_bound = |bound: &WindowFrameBound| match bound {
            WindowFrameBound::CurrentRow => {
                window_frame.units == WindowFrameUnits::Range && order_by.is_empty()
            }
            _ => bound.is_unbounded(),
        };

        is_constant_bound(&window_frame.start_bound)
            && is_constant_bound(&window_frame.end_bound)
    }
}

/// peer based evaluation based on the fact that batch is pre-sorted given the sort columns
/// and then per partition point we'll evaluate the peer group (e.g. SUM or MAX gives the same
/// results for peers) and concatenate the results.
impl WindowExpr for PlainAggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<FieldRef> {
        Ok(self.aggregate.field())
    }

    fn name(&self) -> &str {
        self.aggregate.name()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.aggregate.expressions()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.aggregate_evaluate(batch)
    }

    fn evaluate_stateful(
        &self,
        partition_batches: &PartitionBatches,
        window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        self.aggregate_evaluate_stateful(partition_batches, window_agg_state)?;

        // Update window frame range for each partition. As we know that
        // non-sliding aggregations will never call `retract_batch`, this value
        // can safely increase, and we can remove "old" parts of the state.
        // This enables us to run queries involving UNBOUNDED PRECEDING frames
        // using bounded memory for suitable aggregations.
        for partition_row in partition_batches.keys() {
            let window_state =
                window_agg_state.get_mut(partition_row).ok_or_else(|| {
                    DataFusionError::Execution("Cannot find state".to_string())
                })?;
            let state = &mut window_state.state;
            if self.window_frame.start_bound.is_unbounded() {
                state.window_frame_range.start =
                    state.window_frame_range.end.saturating_sub(1);
            }
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
            if reverse_window_frame.is_ever_expanding() {
                Arc::new(PlainAggregateWindowExpr::new(
                    Arc::new(reverse_expr),
                    &self.partition_by.clone(),
                    &self
                        .order_by
                        .iter()
                        .map(|e| e.reverse())
                        .collect::<Vec<_>>(),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            } else {
                Arc::new(SlidingAggregateWindowExpr::new(
                    Arc::new(reverse_expr),
                    &self.partition_by.clone(),
                    &self
                        .order_by
                        .iter()
                        .map(|e| e.reverse())
                        .collect::<Vec<_>>(),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            }
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        !self.window_frame.end_bound.is_unbounded()
    }

    fn create_window_fn(&self) -> Result<WindowFn> {
        Ok(WindowFn::Aggregate(self.get_accumulator()?))
    }
}

impl AggregateWindowExpr for PlainAggregateWindowExpr {
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.aggregate.create_accumulator()
    }

    /// For a given range, calculate accumulation result inside the range on
    /// `value_slice` and update accumulator state.
    // We assume that `cur_range` contains `last_range` and their start points
    // are same. In summary if `last_range` is `Range{start: a,end: b}` and
    // `cur_range` is `Range{start: a1, end: b1}`, it is guaranteed that a1=a and b1>=b.
    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
    ) -> Result<ScalarValue> {
        if cur_range.start == cur_range.end {
            self.aggregate
                .default_value(self.aggregate.field().data_type())
        } else {
            // Accumulate any new rows that have entered the window:
            let update_bound = cur_range.end - last_range.end;
            // A non-sliding aggregation only processes new data, it never
            // deals with expiring data as its starting point is always the
            // same point (i.e. the beginning of the table/frame). Hence, we
            // do not call `retract_batch`.
            if update_bound > 0 {
                let update: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.end, update_bound))
                    .collect();
                accumulator.update_batch(&update)?
            }
            accumulator.evaluate()
        }
    }

    fn is_constant_in_partition(&self) -> bool {
        self.is_constant_in_partition
    }
}
