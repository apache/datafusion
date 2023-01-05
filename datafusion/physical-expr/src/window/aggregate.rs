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
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::{WindowFrame, WindowFrameUnits};

use crate::window::window_expr::reverse_order_bys;
use crate::window::SlidingAggregateWindowExpr;
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};

use super::window_frame_state::WindowFrameContext;

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
}

impl AggregateWindowExpr {
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
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();
        let mut row_wise_results: Vec<ScalarValue> = vec![];

        let mut accumulator = self.aggregate.create_accumulator()?;
        let length = batch.num_rows();
        let (values, order_bys) = self.get_values_orderbys(batch)?;

        let mut window_frame_ctx = WindowFrameContext::new(&self.window_frame);
        let mut last_range = Range { start: 0, end: 0 };

        // We iterate on each row to perform a running calculation.
        // First, cur_range is calculated, then it is compared with last_range.
        for i in 0..length {
            let cur_range =
                window_frame_ctx.calculate_range(&order_bys, &sort_options, length, i)?;
            let value = if cur_range.end == cur_range.start {
                // We produce None if the window is empty.
                ScalarValue::try_from(self.aggregate.field()?.data_type())?
            } else {
                // Accumulate any new rows that have entered the window:
                let update_bound = cur_range.end - last_range.end;
                if update_bound > 0 {
                    let update: Vec<ArrayRef> = values
                        .iter()
                        .map(|v| v.slice(last_range.end, update_bound))
                        .collect();
                    accumulator.update_batch(&update)?
                }
                accumulator.evaluate()?
            };
            row_wise_results.push(value);
            last_range = cur_range;
        }

        ScalarValue::iter_to_array(row_wise_results.into_iter())
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
