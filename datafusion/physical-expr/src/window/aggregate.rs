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

use crate::window::partition_evaluator::find_ranges_in_range;
use crate::{expressions::PhysicalSortExpr, PhysicalExpr};
use crate::{window::WindowExpr, AggregateExpr};
use arrow::array::{Array, Float64Array};
use arrow::compute::{cast, concat};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::bisect::{bisect_left_arrow, bisect_right_arrow};
use datafusion_common::from_slice::FromSlice;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::logical_plan::builder::union_with_alias;
use datafusion_expr::AggregateState::Scalar;
use datafusion_expr::{Accumulator, WindowFrameBound};
use datafusion_expr::{WindowFrame, WindowFrameUnits};
use std::any::Any;
use std::cmp::{max, min};
use std::fmt::Display;
use std::iter::IntoIterator;
use std::ops::Range;
use std::sync::Arc;

pub fn combine_ranges(value_ranges: &[Range<usize>]) -> Range<usize> {
    // make ranges single
    let mut glob_range = Range {
        start: usize::MAX,
        end: usize::MIN,
    };
    for value_range in value_ranges {
        if value_range.start < glob_range.start {
            glob_range.start = value_range.start;
        }
        if value_range.end > glob_range.end {
            glob_range.end = value_range.end;
        }
    }
    glob_range
}

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

    /// the aggregate window function operates based on window frame, and by default the mode is
    /// "range".
    fn evaluation_mode(&self) -> WindowFrameUnits {
        self.window_frame.unwrap_or_default().units
    }

    /// create a new accumulator based on the underlying aggregation function
    fn create_accumulator(&self) -> Result<AggregateWindowAccumulator> {
        let accumulator = self.aggregate.create_accumulator()?;
        let window_frame = self.window_frame;
        let order_by = self.order_by().to_vec();
        let partition_by = self.partition_by().to_vec();
        let field = self.aggregate.field().unwrap();
        Ok(AggregateWindowAccumulator {
            accumulator,
            window_frame,
            order_by,
            partition_by,
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
        // let sort_partition_points = self.evaluate_partition_points(num_rows, &self.sort_columns(batch)?)?;
        let values = self.evaluate_args(batch)?;

        let columns = self.sort_columns(batch)?;
        let array_refs: Vec<&ArrayRef> = columns.iter().map(|s| &s.values).collect();
        // Sort values, this will make the same partitions consecutive.
        let results = partition_points
            .iter()
            .map(|partition_range| {
                let mut window_accumulators = self.create_accumulator()?;
                let res =
                    window_accumulators.scan(&values, &array_refs, &partition_range);
                Ok(vec![res.unwrap()])
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

/// This method gets vector of columns and finds the last value that lower than the target value in a sorted array.
/// E.g
/// target = 3
/// arr = [1,1,1,2,2,3,3,4]
/// returns 6
fn calculate_index_of_last_unequal_row(
    range_columns: &&Vec<&[f64]>,
    following: f64,
    idx: usize,
) -> Result<usize> {
    let current_row_values: Vec<f64> = range_columns
        .iter()
        .map(|col| *col.get(idx).unwrap())
        .collect::<Vec<_>>();
    let end_range: Vec<f64> = current_row_values
        .iter()
        .map(|value| *value + following)
        .collect::<Vec<_>>();
    let end = bisect_right_arrow(&range_columns, end_range)?;
    Ok(end)
}
/// This method gets vector of columns and finds the first value that greater than the target value in a sorted array.
/// E.g
/// target = 3
/// arr = [1,1,1,2,2,3,3,4]
/// returns 5
fn calculate_index_of_first_unequal_row(
    range_columns: &&Vec<&[f64]>,
    preceding: f64,
    idx: usize,
) -> Result<usize> {
    let current_row_values: Vec<&f64> = range_columns
        .iter()
        .map(|col| col.get(idx).unwrap())
        .collect::<Vec<_>>();
    let start_range: Vec<f64> = current_row_values
        .iter()
        .map(|value| *value - preceding)
        .collect::<Vec<_>>();
    let start = bisect_left_arrow(&range_columns, start_range)?;
    Ok(start)
}
/// If we need a Null in returns, thi function provides it.
fn get_none_type(field: &Field) -> Result<ScalarValue> {
    match field.data_type() {
        DataType::Int64 => Ok(ScalarValue::Int64(None)),
        DataType::Int8 => Ok(ScalarValue::Int8(None)),
        DataType::Int16 => Ok(ScalarValue::Int16(None)),
        DataType::Int32 => Ok(ScalarValue::Int32(None)),
        DataType::UInt8 => Ok(ScalarValue::UInt8(None)),
        DataType::UInt16 => Ok(ScalarValue::UInt16(None)),
        DataType::UInt64 => Ok(ScalarValue::UInt64(None)),
        DataType::Float32 => Ok(ScalarValue::Float32(None)),
        DataType::Float64 => Ok(ScalarValue::Float64(None)),
        _ => Err(DataFusionError::Internal(format!(
            "None type not supported for type '{:?}'",
            field.data_type()
        ))),
    }
}

/// We use start and end bounds to calculate current row's starting and ending range. This function
// can be support different modes.
fn calculate_current_window(
    window_frame: WindowFrame,
    range_columns: &Vec<&[f64]>,
    len: usize,
    idx: usize,
) -> Result<(usize, usize)> {
    match window_frame.units {
        WindowFrameUnits::Range => {
            let start = match window_frame.start_bound {
                // UNBOUNDED PRECEDING case
                WindowFrameBound::Preceding(None) => Ok(0),
                WindowFrameBound::Preceding(Some(n)) => {
                    calculate_index_of_first_unequal_row(&range_columns, n as f64, idx)
                }
                WindowFrameBound::CurrentRow => {
                    calculate_index_of_first_unequal_row(&range_columns, 0., idx)
                }
                WindowFrameBound::Following(Some(n)) => {
                    calculate_index_of_first_unequal_row(&range_columns, -(n as f64), idx)
                }
                _ => Err(DataFusionError::Internal(format!(
                    "Error during parsing arguments of '{:?}'",
                    window_frame
                ))),
            };
            let end = match window_frame.end_bound {
                WindowFrameBound::Preceding(Some(n)) => {
                    calculate_index_of_last_unequal_row(&range_columns, -(n as f64), idx)
                }
                WindowFrameBound::Following(Some(n)) => {
                    calculate_index_of_last_unequal_row(&range_columns, n as f64, idx)
                }
                WindowFrameBound::CurrentRow => {
                    calculate_index_of_last_unequal_row(&range_columns, 0., idx)
                }
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => Ok(len),
                _ => Err(DataFusionError::Internal(format!(
                    "Error during parsing arguments of '{:?}'",
                    window_frame
                ))),
            };
            Ok((start?, end?))
        }
        WindowFrameUnits::Rows => {
            let start = match window_frame.start_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(None) => Ok(0),
                WindowFrameBound::Preceding(Some(n)) => match idx >= n as usize {
                    true => Ok(idx - n as usize),
                    false => Ok(0),
                },
                WindowFrameBound::CurrentRow => Ok(idx),
                WindowFrameBound::Following(Some(n)) => Ok(min(idx + n as usize, len)),
                _ => Err(DataFusionError::Internal(format!(
                    "Error during parsing arguments of '{:?}'",
                    window_frame
                ))),
            };
            let end = match window_frame.end_bound {
                WindowFrameBound::Preceding(Some(n)) => match idx >= n as usize {
                    true => Ok(idx - n as usize + 1),
                    false => Ok(0),
                },
                WindowFrameBound::CurrentRow => Ok(idx + 1),
                WindowFrameBound::Following(Some(n)) => {
                    Ok(min(idx + n as usize + 1, len))
                }
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(None) => Ok(len),
                _ => Err(DataFusionError::Internal(format!(
                    "Error during parsing arguments of '{:?}'",
                    window_frame
                ))),
            };
            Ok((start?, end?))
        }
        WindowFrameUnits::Groups => Err(DataFusionError::Internal(format!(
            "Window frame for groups is not implemented"
        ))),
    }
}

/// Aggregate window accumulator utilizes the accumulator from aggregation and do a accumulative sum
/// across evaluation arguments based on peer equivalences. It uses many information to calculate
/// correct running window.
#[derive(Debug)]
struct AggregateWindowAccumulator {
    accumulator: Box<dyn Accumulator>,
    window_frame: Option<WindowFrame>,
    order_by: Vec<PhysicalSortExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    field: Field,
}

impl AggregateWindowAccumulator {
    /// An ORDER BY is
    fn implicit_order_by_window() -> WindowFrame {
        // OVER(ORDER BY <field>)  case
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::Following(Some(0)),
        }
    }
    /// It calculates the whole aggregation result and copy into an array of table size.
    fn calculate_whole_table(
        &mut self,
        value_slice: &Vec<ArrayRef>,
        len: usize,
    ) -> Result<ArrayRef> {
        self.accumulator.update_batch(&value_slice)?;
        let value = self.accumulator.evaluate()?;
        Ok(value.to_array_of_size(len))
    }

    /// It calculates the running window logic.
    fn calculate_running_window(
        &mut self,
        value_slice: &Vec<ArrayRef>,
        order_bys: &Vec<&ArrayRef>,
        value_range: &Range<usize>,
    ) -> Result<ArrayRef> {
        let len = value_range.end - value_range.start;
        let order_columns = order_bys
            .iter()
            .map(|v| v.slice(value_range.start, value_range.end - value_range.start))
            .map(|array| cast(&array, &DataType::Float64).unwrap())
            .collect::<Vec<_>>();
        let cast_float64_order_columns = order_columns
            .iter()
            .map(|item| {
                item.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
            })
            .collect::<Vec<_>>();
        let updated_zero_offset_value_range = Range {
            start: 0,
            end: value_range.end - value_range.start,
        };
        let mut row_wise_results = vec![];
        let mut last_range: (usize, usize) = (
            updated_zero_offset_value_range.start,
            updated_zero_offset_value_range.start,
        );

        /// We iterate each row to calculate its corresponding window. It is a running
        /// window calculation. First, cur_range calculated, then it is compared with last_range.
        /// We increment the accumulator by update and retract.
        /// P.s: We did not implement retract_batch logic for all aggregators.
        for i in 0..len {
            let cur_range = calculate_current_window(
                self.window_frame.unwrap(),
                &cast_float64_order_columns,
                len,
                i,
            )?;

            match cur_range.1 - cur_range.0 {
                // We produce None if the window is empty.
                0 => row_wise_results.push(get_none_type(&self.field)?),
                // If the new window is not empty, we
                _ => {
                    let update: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(last_range.1, cur_range.1 - last_range.1))
                        .collect();
                    let retract: Vec<ArrayRef> = value_slice
                        .iter()
                        .map(|v| v.slice(last_range.0, cur_range.0 - last_range.0))
                        .collect();
                    self.accumulator.update_batch(&update)?;
                    self.accumulator.retract_batch(&retract)?;
                    row_wise_results.push(self.accumulator.evaluate()?)
                }
            }
            last_range = cur_range;
        }

        let array = ScalarValue::iter_to_array(row_wise_results.into_iter());
        // res.push(array)
        array
    }

    fn scan(
        &mut self,
        values: &[ArrayRef],
        order_bys: &Vec<&ArrayRef>,
        value_range: &Range<usize>,
    ) -> Result<ArrayRef> {
        if value_range.is_empty() {
            return Err(DataFusionError::Internal(
                "Value range cannot be empty".to_owned(),
            ));
        }
        let len = value_range.end - value_range.start;
        let value_slice = values
            .iter()
            .map(|v| v.slice(value_range.start, len))
            .collect::<Vec<_>>();
        let wanted_order_columns =
            &order_bys[self.partition_by.len()..order_bys.len()].to_vec();
        match (wanted_order_columns.len(), self.window_frame) {
            (0, None) => {
                // OVER() case
                self.calculate_whole_table(&value_slice, len)
            }
            (_n, None) => {
                // OVER(ORDER BY a) case
                // We create an implicit window for ORDER BY.
                self.window_frame =
                    Some(AggregateWindowAccumulator::implicit_order_by_window());

                self.calculate_running_window(
                    &value_slice,
                    wanted_order_columns,
                    &value_range,
                )
            }
            (0, Some(frame)) => {
                match frame.units {
                    WindowFrameUnits::Range => {
                        // OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
                        self.calculate_whole_table(&value_slice, len)
                    }
                    WindowFrameUnits::Rows => {
                        // OVER(ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING )
                        self.calculate_running_window(
                            &value_slice,
                            order_bys,
                            &value_range,
                        )
                    }
                    WindowFrameUnits::Groups => Err(DataFusionError::Internal(format!(
                        "Window frame for groups is not implemented"
                    ))),
                }
            }
            // OVER(ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING )
            (_n, _) => self.calculate_running_window(
                &value_slice,
                wanted_order_columns,
                &value_range,
            ),
        }
    }
}
