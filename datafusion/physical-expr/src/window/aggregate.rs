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
use std::cmp::min;
use std::iter::IntoIterator;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::{concat, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};

use datafusion_common::bisect::bisect;
use datafusion_common::scalar::TryFromValue;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{Accumulator, WindowFrameBound};
use datafusion_expr::{WindowFrame, WindowFrameUnits};

use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<WindowFrame>,
}

impl AggregateWindowExpr {
    /// create a new aggregate window function expression
    pub fn new(
        aggregate: Arc<dyn AggregateExpr>,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &[PhysicalSortExpr],
        window_frame: Option<WindowFrame>,
    ) -> Self {
        Self {
            aggregate,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }
    }

    /// create a new accumulator based on the underlying aggregation function
    fn create_accumulator(&self) -> Result<AggregateWindowAccumulator> {
        let accumulator = self.aggregate.create_accumulator()?;
        let window_frame = self.window_frame;
        let partition_by = self.partition_by().to_vec();
        let order_by = self.order_by.to_vec();
        let field = self.aggregate.field()?;

        Ok(AggregateWindowAccumulator {
            accumulator,
            window_frame,
            partition_by,
            order_by,
            field,
        })
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
        let num_rows = batch.num_rows();
        let partition_points =
            self.evaluate_partition_points(num_rows, &self.partition_columns(batch)?)?;
        let values = self.evaluate_args(batch)?;

        let columns = self.sort_columns(batch)?;
        let array_refs: Vec<&ArrayRef> = columns.iter().map(|s| &s.values).collect();
        // Sort values, this will make the same partitions consecutive. Also, within the partition
        // range, values will be sorted.
        let results = partition_points
            .iter()
            .map(|partition_range| {
                let mut window_accumulators = self.create_accumulator()?;
                Ok(vec![window_accumulators.scan(
                    &values,
                    &array_refs,
                    partition_range,
                )?])
            })
            .collect::<Result<Vec<Vec<ArrayRef>>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<ArrayRef>>();
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        concat(&results).map_err(DataFusionError::ArrowError)
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }
}

fn calculate_index_of_row<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
    range_columns: &[ArrayRef],
    sort_options: &[SortOptions],
    idx: usize,
    delta: u64,
) -> Result<usize> {
    let current_row_values = range_columns
        .iter()
        .map(|col| ScalarValue::try_from_array(col, idx))
        .collect::<Result<Vec<ScalarValue>>>()?;
    let end_range = if delta == 0 {
        current_row_values
    } else {
        let is_descending: bool = sort_options
            .first()
            .ok_or_else(|| DataFusionError::Internal("Array is empty".to_string()))?
            .descending;

        current_row_values
            .iter()
            .map(|value| {
                if value.is_null() {
                    return Ok(value.clone());
                };
                let offset = ScalarValue::try_from_value(&value.get_datatype(), delta)?;
                if SEARCH_SIDE == is_descending {
                    // TODO: ADD overflow check
                    value.add(&offset)
                } else if value.is_unsigned() && value < &offset {
                    ScalarValue::try_from_value(&value.get_datatype(), 0)
                } else {
                    value.sub(&offset)
                }
            })
            .collect::<Result<Vec<ScalarValue>>>()?
    };
    // `BISECT_SIDE` true means bisect_left, false means bisect_right
    bisect::<BISECT_SIDE>(range_columns, &end_range, sort_options)
}

/// We use start and end bounds to calculate current row's starting and ending range.
/// This function supports different modes, but we currently do not support window calculation for GROUPS inside window frames.
fn calculate_current_window(
    window_frame: &WindowFrame,
    range_columns: &[ArrayRef],
    sort_options: &[SortOptions],
    length: usize,
    idx: usize,
) -> Result<(usize, usize)> {
    match window_frame.units {
        WindowFrameUnits::Range => {
            let start = match window_frame.start_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(None) => Ok(0),
                WindowFrameBound::Preceding(Some(n)) => {
                    calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        n,
                    )
                }
                WindowFrameBound::CurrentRow => calculate_index_of_row::<true, true>(
                    range_columns,
                    sort_options,
                    idx,
                    0,
                ),
                WindowFrameBound::Following(Some(n)) => {
                    calculate_index_of_row::<true, false>(
                        range_columns,
                        sort_options,
                        idx,
                        n,
                    )
                }
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => {
                    Err(DataFusionError::Internal(format!(
                        "Frame start cannot be UNBOUNDED FOLLOWING '{:?}'",
                        window_frame
                    )))
                }
            };
            let end = match window_frame.end_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(None) => {
                    Err(DataFusionError::Internal(format!(
                        "Frame end cannot be UNBOUNDED PRECEDING '{:?}'",
                        window_frame
                    )))
                }
                WindowFrameBound::Preceding(Some(n)) => {
                    calculate_index_of_row::<false, true>(
                        range_columns,
                        sort_options,
                        idx,
                        n,
                    )
                }
                WindowFrameBound::CurrentRow => calculate_index_of_row::<false, false>(
                    range_columns,
                    sort_options,
                    idx,
                    0,
                ),
                WindowFrameBound::Following(Some(n)) => {
                    calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        n,
                    )
                }
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => Ok(length),
            };
            Ok((start?, end?))
        }
        WindowFrameUnits::Rows => {
            let start = match window_frame.start_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(None) => Ok(0),
                WindowFrameBound::Preceding(Some(n)) => {
                    if idx >= n as usize {
                        Ok(idx - n as usize)
                    } else {
                        Ok(0)
                    }
                }
                WindowFrameBound::CurrentRow => Ok(idx),
                WindowFrameBound::Following(Some(n)) => Ok(min(idx + n as usize, length)),
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => {
                    Err(DataFusionError::Internal(format!(
                        "Frame start cannot be UNBOUNDED FOLLOWING '{:?}'",
                        window_frame
                    )))
                }
            };
            let end = match window_frame.end_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(None) => {
                    Err(DataFusionError::Internal(format!(
                        "Frame end cannot be UNBOUNDED PRECEDING '{:?}'",
                        window_frame
                    )))
                }
                WindowFrameBound::Preceding(Some(n)) => {
                    if idx >= n as usize {
                        Ok(idx - n as usize + 1)
                    } else {
                        Ok(0)
                    }
                }
                WindowFrameBound::CurrentRow => Ok(idx + 1),
                WindowFrameBound::Following(Some(n)) => {
                    Ok(min(idx + n as usize + 1, length))
                }
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => Ok(length),
            };
            Ok((start?, end?))
        }
        WindowFrameUnits::Groups => Err(DataFusionError::NotImplemented(
            "Window frame for groups is not implemented".to_string(),
        )),
    }
}

/// Aggregate window accumulator utilizes the accumulator from aggregation and do a accumulative sum
/// across evaluation arguments based on peer equivalences. It uses many information to calculate
/// correct running window.
#[derive(Debug)]
struct AggregateWindowAccumulator {
    accumulator: Box<dyn Accumulator>,
    window_frame: Option<WindowFrame>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    field: Field,
}

impl AggregateWindowAccumulator {
    /// This function constructs a simple window frame with a single ORDER BY.
    fn implicit_order_by_window() -> WindowFrame {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::Following(Some(0)),
        }
    }
    /// This function calculates the aggregation on all rows in `value_slice`.
    /// Returns an array of size `len`.
    fn calculate_whole_table(
        &mut self,
        value_slice: &[ArrayRef],
        length: usize,
    ) -> Result<ArrayRef> {
        self.accumulator.update_batch(value_slice)?;
        let value = self.accumulator.evaluate()?;
        Ok(value.to_array_of_size(length))
    }

    /// This function calculates the running window logic for the rows in `value_range` of `value_slice`.
    /// We maintain the accumulator state via `update_batch` and `retract_batch` functions.
    /// Note that not all aggregators implement `retract_batch` just yet.
    fn calculate_running_window(
        &mut self,
        value_slice: &[ArrayRef],
        order_bys: &[&ArrayRef],
        value_range: &Range<usize>,
    ) -> Result<ArrayRef> {
        // We iterate on each row to perform a running calculation.
        // First, cur_range is calculated, then it is compared with last_range.
        let length = value_range.end - value_range.start;
        let slice_order_columns = order_bys
            .iter()
            .map(|v| v.slice(value_range.start, length))
            .collect::<Vec<_>>();
        let sort_options: Vec<SortOptions> =
            self.order_by.iter().map(|o| o.options).collect();

        let updated_zero_offset_value_range = Range {
            start: 0,
            end: length,
        };
        let mut row_wise_results: Vec<ScalarValue> = vec![];
        let mut last_range: (usize, usize) = (
            updated_zero_offset_value_range.start,
            updated_zero_offset_value_range.start,
        );

        for i in 0..length {
            let window_frame = self.window_frame.as_ref().ok_or_else(|| {
                DataFusionError::Internal(
                    "Window frame cannot be empty to calculate window ranges".to_string(),
                )
            })?;
            let cur_range = calculate_current_window(
                window_frame,
                &slice_order_columns,
                &sort_options,
                length,
                i,
            )?;

            if cur_range.0 == cur_range.1 {
                // We produce None if the window is empty.
                row_wise_results.push(ScalarValue::try_from(self.field.data_type())?)
            } else {
                // Accumulate any new rows that have entered the window:
                let update_bound = cur_range.1 - last_range.1;
                if update_bound > 0 {
                    let update: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(last_range.1, update_bound))
                        .collect();
                    self.accumulator.update_batch(&update)?
                }
                // Remove rows that have now left the window:
                let retract_bound = cur_range.0 - last_range.0;
                if retract_bound > 0 {
                    let retract: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(last_range.0, retract_bound))
                        .collect();
                    self.accumulator.retract_batch(&retract)?
                }
                row_wise_results.push(self.accumulator.evaluate()?);
            }
            last_range = cur_range;
        }
        ScalarValue::iter_to_array(row_wise_results.into_iter())
    }

    fn scan(
        &mut self,
        values: &[ArrayRef],
        order_bys: &[&ArrayRef],
        value_range: &Range<usize>,
    ) -> Result<ArrayRef> {
        if value_range.is_empty() {
            return Err(DataFusionError::Internal(
                "Value range cannot be empty".to_owned(),
            ));
        }
        let length = value_range.end - value_range.start;
        let value_slice = values
            .iter()
            .map(|v| v.slice(value_range.start, length))
            .collect::<Vec<_>>();
        let order_columns = &order_bys[self.partition_by.len()..order_bys.len()].to_vec();
        match (order_columns.len(), self.window_frame) {
            (0, None) => {
                // OVER () case
                self.calculate_whole_table(&value_slice, length)
            }
            (_n, None) => {
                // OVER (ORDER BY a) case
                // We create an implicit window for ORDER BY.
                self.window_frame =
                    Some(AggregateWindowAccumulator::implicit_order_by_window());

                self.calculate_running_window(&value_slice, order_columns, value_range)
            }
            (0, Some(frame)) => {
                match frame.units {
                    WindowFrameUnits::Range => {
                        // OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) case
                        self.calculate_whole_table(&value_slice, length)
                    }
                    WindowFrameUnits::Rows => {
                        // OVER (ROWS BETWEEN X PRECEDING AND Y FOLLOWING) case
                        self.calculate_running_window(
                            &value_slice,
                            order_bys,
                            value_range,
                        )
                    }
                    WindowFrameUnits::Groups => Err(DataFusionError::NotImplemented(
                        "Window frame for groups is not implemented".to_string(),
                    )),
                }
            }
            // OVER (ORDER BY a ROWS/RANGE BETWEEN X PRECEDING AND Y FOLLOWING) case
            (_n, _) => {
                self.calculate_running_window(&value_slice, order_columns, value_range)
            }
        }
    }
}
