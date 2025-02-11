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

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, WindowFrame};

use crate::aggregate::AggregateFunctionExpr;
use crate::window::window_expr::AggregateWindowExpr;
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, SlidingAggregateWindowExpr, WindowExpr,
};
use crate::{expressions::PhysicalSortExpr, reverse_order_bys, PhysicalExpr};

/// A window expr that takes the form of an aggregate function.
///
/// See comments on [`WindowExpr`] for more details.
#[derive(Debug)]
pub struct PlainAggregateWindowExpr {
    aggregate: Arc<AggregateFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl PlainAggregateWindowExpr {
    /// Create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<AggregateFunctionExpr>,
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
    pub fn get_aggregate_expr(&self) -> &AggregateFunctionExpr {
        &self.aggregate
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

    fn field(&self) -> Result<Field> {
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
            if reverse_window_frame.start_bound.is_unbounded() {
                Arc::new(PlainAggregateWindowExpr::new(
                    Arc::new(reverse_expr),
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            } else {
                Arc::new(SlidingAggregateWindowExpr::new(
                    Arc::new(reverse_expr),
                    &self.partition_by.clone(),
                    &reverse_order_bys(&self.order_by),
                    Arc::new(self.window_frame.reverse()),
                )) as _
            }
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        !self.window_frame.end_bound.is_unbounded()
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
}
