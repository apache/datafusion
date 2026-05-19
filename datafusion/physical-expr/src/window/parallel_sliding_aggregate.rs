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

//! Physical expression for fixed-size ROWS aggregate windows.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use crate::aggregate::AggregateFunctionExpr;
use crate::window::window_expr::{AggregateWindowExpr, WindowFn, filter_array};
use crate::window::{PartitionBatches, PartitionWindowAggStates, WindowExpr};
use crate::{PhysicalExpr, expressions::PhysicalSortExpr};

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, internal_err, not_impl_err,
};
use datafusion_expr::{
    Accumulator, WindowBatchArgs, WindowFrame, WindowFrameBound, WindowFrameUnits,
};

#[derive(Debug, Clone)]
struct RowsWindowHalo {
    preceding: usize,
    following: usize,
}

/// A fixed `ROWS` aggregate window expression for finite frames.
#[derive(Debug, Clone)]
pub struct ParallelSlidingAggregateWindowExpr {
    aggregate: Arc<AggregateFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Arc<WindowFrame>,
    filter: Option<Arc<dyn PhysicalExpr>>,
    rows_window_halo: RowsWindowHalo,
}

impl ParallelSlidingAggregateWindowExpr {
    pub fn try_new(
        aggregate: Arc<AggregateFunctionExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Arc<WindowFrame>,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        let rows_window_halo = finite_rows_window_halo(&window_frame)?;
        Ok(Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
            filter,
            rows_window_halo,
        })
    }

    /// Get the [AggregateFunctionExpr] of this object.
    pub fn get_aggregate_expr(&self) -> &AggregateFunctionExpr {
        &self.aggregate
    }

    /// Number of preceding and following halo rows needed to evaluate this
    /// expression on an independently processed tile.
    pub fn rows_window_halo(&self) -> (usize, usize) {
        (
            self.rows_window_halo.preceding,
            self.rows_window_halo.following,
        )
    }

    /// Evaluate this expression for the non-halo output range inside `batch`
    /// using the accumulator's vectorized window API.
    pub fn evaluate_window_batch(
        &self,
        batch: &RecordBatch,
        output_start: usize,
        output_end: usize,
    ) -> Result<ArrayRef> {
        let accumulator = self.get_accumulator()?;
        if !accumulator.supports_window_batch() {
            return internal_err!(
                "window accumulator for {} does not support vectorized window evaluation",
                self.name()
            );
        }

        let args = WindowBatchArgs::new(
            output_start,
            output_end,
            self.rows_window_halo.preceding,
            self.rows_window_halo.following,
        );
        args.validate(batch.num_rows())?;

        let values = self.evaluate_args(batch)?;
        let filter_mask_arr: Option<ArrayRef> = match self.filter_expr() {
            Some(expr) => {
                let value = expr.evaluate(batch)?;
                Some(value.into_array(batch.num_rows())?)
            }
            None => None,
        };
        let filter_mask = match filter_mask_arr.as_deref() {
            Some(arr) => Some(as_boolean_array(arr)?),
            None => None,
        };

        let output = accumulator
            .evaluate_window_batch(&values, filter_mask, args)?
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "window accumulator for {} did not return vectorized window output",
                    self.name()
                )
            })?;

        if output.len() != args.output_len() {
            return internal_err!(
                "window accumulator for {} returned {} rows, expected {} rows",
                self.name(),
                output.len(),
                args.output_len()
            );
        }

        Ok(output)
    }
}

impl WindowExpr for ParallelSlidingAggregateWindowExpr {
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
        _partition_batches: &PartitionBatches,
        _window_agg_state: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        internal_err!("evaluate_stateful is not implemented for {}", self.name())
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
        self.aggregate.reverse_expr().and_then(|reverse_expr| {
            let reverse_window_frame = Arc::new(self.window_frame.reverse());
            let reverse_order_by = self
                .order_by
                .iter()
                .map(|e| e.reverse())
                .collect::<Vec<_>>();
            Self::try_new(
                Arc::new(reverse_expr),
                &self.partition_by,
                &reverse_order_by,
                reverse_window_frame,
                self.filter.clone(),
            )
            .ok()
            .map(|expr| Arc::new(expr) as Arc<dyn WindowExpr>)
        })
    }

    fn uses_bounded_memory(&self) -> bool {
        !self.window_frame.end_bound.is_unbounded()
    }

    fn with_new_expressions(
        &self,
        args: Vec<Arc<dyn PhysicalExpr>>,
        partition_bys: Vec<Arc<dyn PhysicalExpr>>,
        order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn WindowExpr>> {
        debug_assert_eq!(self.order_by.len(), order_by_exprs.len());

        let new_order_by = self
            .order_by
            .iter()
            .zip(order_by_exprs)
            .map(|(req, new_expr)| PhysicalSortExpr {
                expr: new_expr,
                options: req.options,
            })
            .collect();
        Some(Arc::new(ParallelSlidingAggregateWindowExpr {
            aggregate: self
                .aggregate
                .with_new_expressions(args, vec![])
                .map(Arc::new)?,
            partition_by: partition_bys,
            order_by: new_order_by,
            window_frame: Arc::clone(&self.window_frame),
            filter: self.filter.clone(),
            rows_window_halo: self.rows_window_halo.clone(),
        }))
    }

    fn create_window_fn(&self) -> Result<WindowFn> {
        Ok(WindowFn::Aggregate(self.get_accumulator()?))
    }
}

impl AggregateWindowExpr for ParallelSlidingAggregateWindowExpr {
    fn get_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.aggregate.create_sliding_accumulator()
    }

    fn filter_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.filter.as_ref()
    }

    fn get_aggregate_result_inside_range(
        &self,
        last_range: &Range<usize>,
        cur_range: &Range<usize>,
        value_slice: &[ArrayRef],
        accumulator: &mut Box<dyn Accumulator>,
        filter_mask: Option<&BooleanArray>,
    ) -> Result<ScalarValue> {
        if cur_range.start == cur_range.end {
            self.aggregate
                .default_value(self.aggregate.field().data_type())
        } else {
            let update_bound = cur_range.end - last_range.end;
            if update_bound > 0 {
                let slice_mask =
                    filter_mask.map(|m| m.slice(last_range.end, update_bound));
                let update: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.end, update_bound))
                    .map(|arr| match &slice_mask {
                        Some(m) => filter_array(&arr, m),
                        None => Ok(arr),
                    })
                    .collect::<Result<Vec<_>>>()?;
                accumulator.update_batch(&update)?
            }

            let retract_bound = cur_range.start - last_range.start;
            if retract_bound > 0 {
                let slice_mask =
                    filter_mask.map(|m| m.slice(last_range.start, retract_bound));
                let retract: Vec<ArrayRef> = value_slice
                    .iter()
                    .map(|v| v.slice(last_range.start, retract_bound))
                    .map(|arr| match &slice_mask {
                        Some(m) => filter_array(&arr, m),
                        None => Ok(arr),
                    })
                    .collect::<Result<Vec<_>>>()?;
                accumulator.retract_batch(&retract)?
            }
            accumulator.evaluate()
        }
    }

    fn is_constant_in_partition(&self) -> bool {
        false
    }
}

fn finite_rows_window_halo(window_frame: &WindowFrame) -> Result<RowsWindowHalo> {
    if window_frame.units != WindowFrameUnits::Rows {
        return not_impl_err!(
            "parallel sliding aggregate only supports ROWS window frames"
        );
    }

    let start = finite_rows_bound_halo(&window_frame.start_bound)?;
    let end = finite_rows_bound_halo(&window_frame.end_bound)?;

    Ok(RowsWindowHalo {
        preceding: start.0.max(end.0),
        following: start.1.max(end.1),
    })
}

fn finite_rows_bound_halo(bound: &WindowFrameBound) -> Result<(usize, usize)> {
    match bound {
        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(offset))) => Ok((
            usize::try_from(*offset).map_err(|_| {
                exec_datafusion_err!(
                    "ROWS PRECEDING offset {offset} does not fit in usize"
                )
            })?,
            0,
        )),
        WindowFrameBound::CurrentRow => Ok((0, 0)),
        WindowFrameBound::Following(ScalarValue::UInt64(Some(offset))) => Ok((
            0,
            usize::try_from(*offset).map_err(|_| {
                exec_datafusion_err!(
                    "ROWS FOLLOWING offset {offset} does not fit in usize"
                )
            })?,
        )),
        _ => not_impl_err!(
            "parallel sliding aggregate only supports finite ROWS window frame bounds"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::aggregate::AggregateExprBuilder;
    use crate::expressions::col;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_functions_aggregate::average::avg_udaf;

    #[test]
    fn rejects_non_finite_rows_frames() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v1",
            DataType::Float64,
            false,
        )]));
        let value = col("v1", &schema)?;
        let aggregate = Arc::new(
            AggregateExprBuilder::new(avg_udaf(), vec![Arc::clone(&value)])
                .schema(Arc::clone(&schema))
                .alias("moving_avg")
                .build()?,
        );
        let order_by = vec![PhysicalSortExpr::new_default(value)];
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        ));

        assert!(
            ParallelSlidingAggregateWindowExpr::try_new(
                aggregate,
                &[],
                &order_by,
                window_frame,
                None
            )
            .is_err()
        );
        Ok(())
    }
}
