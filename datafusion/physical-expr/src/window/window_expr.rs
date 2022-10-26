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

use crate::{PhysicalExpr, PhysicalSortExpr};
use arrow::compute::kernels::partition::lexicographical_partition_ranges;
use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::record_batch::RecordBatch;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::bisect::bisect;
use datafusion_common::scalar::TryFromValue;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use std::any::Any;
use std::cmp::min;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use datafusion_expr::WindowFrameBound;
use datafusion_expr::{WindowFrame, WindowFrameUnits};

/// A window expression that:
/// * knows its resulting field
pub trait WindowExpr: Send + Sync + Debug {
    /// Returns the window expression as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this window function.
    fn field(&self) -> Result<Field>;

    /// Human readable name such as `"MIN(c2)"` or `"RANK()"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "WindowExpr: default name"
    }

    /// expressions that are passed to the WindowAccumulator.
    /// Functions which take a single input argument, such as `sum`, return a single [`datafusion_expr::expr::Expr`],
    /// others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// evaluate the window function arguments against the batch and return
    /// array ref, normally the resulting vec is a single element one.
    fn evaluate_args(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
        self.expressions()
            .iter()
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect()
    }

    /// evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;

    /// evaluate the partition points given the sort columns; if the sort columns are
    /// empty then the result will be a single element vec of the whole column rows.
    fn evaluate_partition_points(
        &self,
        num_rows: usize,
        partition_columns: &[SortColumn],
    ) -> Result<Vec<Range<usize>>> {
        if partition_columns.is_empty() {
            Ok(vec![Range {
                start: 0,
                end: num_rows,
            }])
        } else {
            Ok(lexicographical_partition_ranges(partition_columns)
                .map_err(DataFusionError::ArrowError)?
                .collect::<Vec<_>>())
        }
    }

    /// expressions that's from the window function's partition by clause, empty if absent
    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>];

    /// expressions that's from the window function's order by clause, empty if absent
    fn order_by(&self) -> &[PhysicalSortExpr];

    /// get partition columns that can be used for partitioning, empty if absent
    fn partition_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        self.partition_by()
            .iter()
            .map(|expr| {
                PhysicalSortExpr {
                    expr: expr.clone(),
                    options: SortOptions::default(),
                }
                .evaluate_to_sort_column(batch)
            })
            .collect()
    }

    /// get sort columns that can be used for peer evaluation, empty if absent
    fn sort_columns(&self, batch: &RecordBatch) -> Result<Vec<SortColumn>> {
        let mut sort_columns = self.partition_columns(batch)?;
        let order_by_columns = self
            .order_by()
            .iter()
            .map(|e| e.evaluate_to_sort_column(batch))
            .collect::<Result<Vec<SortColumn>>>()?;
        sort_columns.extend(order_by_columns);
        Ok(sort_columns)
    }

    /// We use start and end bounds to calculate current row's starting and ending range.
    /// This function supports different modes, but we currently do not support window calculation for GROUPS inside window frames.
    fn calculate_range(
        &self,
        window_frame: &Option<WindowFrame>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)> {
        match (range_columns.len(), window_frame) {
            (_, Some(window_frame)) => {
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
                            WindowFrameBound::CurrentRow => {
                                if range_columns.is_empty() {
                                    Ok(0)
                                } else {
                                    calculate_index_of_row::<true, true>(
                                        range_columns,
                                        sort_options,
                                        idx,
                                        0,
                                    )
                                }
                            }
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
                            WindowFrameBound::CurrentRow => {
                                if range_columns.is_empty() {
                                    Ok(length)
                                } else {
                                    calculate_index_of_row::<false, false>(
                                        range_columns,
                                        sort_options,
                                        idx,
                                        0,
                                    )
                                }
                            }
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
                            WindowFrameBound::Following(Some(n)) => {
                                Ok(min(idx + n as usize, length))
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
            (_, None) => Ok((0, length)),
        }
    }

    // OVER (ORDER BY a) case implicitly cast to OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    fn implicit_order_by_window(&self) -> WindowFrame {
        WindowFrame {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: WindowFrameBound::CurrentRow,
        }
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
                    // TODO: Handle positive overflows
                    value.add(&offset)
                } else if value.is_unsigned() && value < &offset {
                    ScalarValue::try_from_value(&value.get_datatype(), 0)
                } else {
                    // TODO: Handle negative overflows
                    value.sub(&offset)
                }
            })
            .collect::<Result<Vec<ScalarValue>>>()?
    };
    // `BISECT_SIDE` true means bisect_left, false means bisect_right
    bisect::<BISECT_SIDE>(range_columns, &end_range, sort_options)
}
