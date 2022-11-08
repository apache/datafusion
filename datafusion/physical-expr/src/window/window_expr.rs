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
use datafusion_common::{DataFusionError, Result, ScalarValue};
use std::any::Any;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion_expr::utils::WindowSortKeys;
use datafusion_expr::{Accumulator, AggregateState, WindowFrameBound};
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

    /// evaluate the window function values against the batch
    fn evaluate_stream(
        &self,
        _batch_state: &HashMap<Vec<ScalarValue>, (u64, RecordBatch)>,
        _window_accumulators: &mut HashMap<
            Vec<ScalarValue>,
            AggregateWindowAccumulatorState,
        >,
        window_sort_keys: &WindowSortKeys,
        is_end: bool,
    ) -> Result<Vec<WindowAccumulatorResult>> {
        Err(DataFusionError::Internal(
            "evaluate stream is not implemented".to_string(),
        ))
    }

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
        window_frame: &Option<Arc<WindowFrame>>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        window_sort_keys: &WindowSortKeys,
    ) -> Result<(usize, usize)> {
        if let Some(window_frame) = window_frame {
            match window_frame.units {
                WindowFrameUnits::Range => {
                    let window_frame = if !window_sort_keys.is_empty() {
                        let physical_sorting = window_sort_keys.first().unwrap();
                        if let Some(column_info) = physical_sorting.column_info {
                            let is_descending: bool = sort_options
                                .first()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Array is empty".to_string(),
                                    )
                                })?
                                .descending;
                            let is_physical_descending = !column_info.is_ascending;
                            if is_physical_descending != is_descending {
                                let new_window_frame =
                                    get_reversed_window_frame(&window_frame)?;
                                // println!("old window frame: {:?}", window_frame);
                                // println!("new window frame: {:?}", new_window_frame);

                                // println!(
                                //     "is_descending: {:?}, is_physical_descending: {:?}",
                                //     is_descending, is_physical_descending
                                // );
                                new_window_frame
                            } else {
                                Arc::clone(window_frame)
                            }
                        } else {
                            Arc::clone(window_frame)
                        }
                    } else {
                        Arc::clone(window_frame)
                    };

                    let start = match &window_frame.start_bound {
                        // UNBOUNDED PRECEDING
                        WindowFrameBound::Preceding(n) => {
                            if n.is_null() {
                                0
                            } else {
                                calculate_index_of_row::<true, true>(
                                    range_columns,
                                    sort_options,
                                    idx,
                                    Some(n),
                                    window_sort_keys,
                                )?
                            }
                        }
                        WindowFrameBound::CurrentRow => {
                            if range_columns.is_empty() {
                                0
                            } else {
                                calculate_index_of_row::<true, true>(
                                    range_columns,
                                    sort_options,
                                    idx,
                                    None,
                                    window_sort_keys,
                                )?
                            }
                        }
                        WindowFrameBound::Following(n) => {
                            calculate_index_of_row::<true, false>(
                                range_columns,
                                sort_options,
                                idx,
                                Some(n),
                                window_sort_keys,
                            )?
                        }
                    };
                    let end = match &window_frame.end_bound {
                        WindowFrameBound::Preceding(n) => {
                            calculate_index_of_row::<false, true>(
                                range_columns,
                                sort_options,
                                idx,
                                Some(n),
                                window_sort_keys,
                            )?
                        }
                        WindowFrameBound::CurrentRow => {
                            if range_columns.is_empty() {
                                length
                            } else {
                                calculate_index_of_row::<false, false>(
                                    range_columns,
                                    sort_options,
                                    idx,
                                    None,
                                    window_sort_keys,
                                )?
                            }
                        }
                        WindowFrameBound::Following(n) => {
                            if n.is_null() {
                                // UNBOUNDED FOLLOWING
                                length
                            } else {
                                calculate_index_of_row::<false, false>(
                                    range_columns,
                                    sort_options,
                                    idx,
                                    Some(n),
                                    window_sort_keys,
                                )?
                            }
                        }
                    };
                    Ok((start, end))
                }
                WindowFrameUnits::Rows => {
                    let start = match window_frame.start_bound {
                        // UNBOUNDED PRECEDING
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => 0,
                        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                            if idx >= n as usize {
                                idx - n as usize
                            } else {
                                0
                            }
                        }
                        WindowFrameBound::Preceding(_) => {
                            return Err(DataFusionError::Internal(
                                "Rows should be Uint".to_string(),
                            ))
                        }
                        WindowFrameBound::CurrentRow => idx,
                        // UNBOUNDED FOLLOWING
                        WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                            return Err(DataFusionError::Internal(format!(
                                "Frame start cannot be UNBOUNDED FOLLOWING '{:?}'",
                                window_frame
                            )))
                        }
                        WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                            min(idx + n as usize, length)
                        }
                        WindowFrameBound::Following(_) => {
                            return Err(DataFusionError::Internal(
                                "Rows should be Uint".to_string(),
                            ))
                        }
                    };
                    let end = match window_frame.end_bound {
                        // UNBOUNDED PRECEDING
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => {
                            return Err(DataFusionError::Internal(format!(
                                "Frame end cannot be UNBOUNDED PRECEDING '{:?}'",
                                window_frame
                            )))
                        }
                        WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                            if idx >= n as usize {
                                idx - n as usize + 1
                            } else {
                                0
                            }
                        }
                        WindowFrameBound::Preceding(_) => {
                            return Err(DataFusionError::Internal(
                                "Rows should be Uint".to_string(),
                            ))
                        }
                        WindowFrameBound::CurrentRow => idx + 1,
                        // UNBOUNDED FOLLOWING
                        WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
                        WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                            min(idx + n as usize + 1, length)
                        }
                        WindowFrameBound::Following(_) => {
                            return Err(DataFusionError::Internal(
                                "Rows should be Uint".to_string(),
                            ))
                        }
                    };
                    Ok((start, end))
                }
                WindowFrameUnits::Groups => Err(DataFusionError::NotImplemented(
                    "Window frame for groups is not implemented".to_string(),
                )),
            }
        } else {
            Ok((0, length))
        }
    }

    fn calculate_running_window(
        &self,
        _state: &mut AggregateWindowAccumulatorState,
        _accumulator: &mut Box<dyn Accumulator>,
        _value_slice: &Vec<ArrayRef>,
        _order_bys: &Vec<ArrayRef>,
        _is_end: bool,
        _window_sort_keys: &WindowSortKeys,
    ) -> Result<Option<ArrayRef>> {
        Err(DataFusionError::NotImplemented(
            "running window calculation is not implemented".to_string(),
        ))
    }
}

fn calculate_index_of_row<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
    range_columns: &[ArrayRef],
    sort_options: &[SortOptions],
    idx: usize,
    delta: Option<&ScalarValue>,
    window_sort_keys: &WindowSortKeys,
) -> Result<usize> {
    let current_row_values = range_columns
        .iter()
        .map(|col| ScalarValue::try_from_array(col, idx))
        .collect::<Result<Vec<ScalarValue>>>()?;
    let end_range = if let Some(delta) = delta {
        let is_descending: bool = sort_options
            .first()
            .ok_or_else(|| DataFusionError::Internal("Array is empty".to_string()))?
            .descending;
        let is_descending = if !window_sort_keys.is_empty() {
            if let Some(column_info) = window_sort_keys
                .first()
                .ok_or_else(|| DataFusionError::Internal("Array is empty".to_string()))?
                .column_info
            {
                !column_info.is_ascending
            } else {
                is_descending
            }
        } else {
            is_descending
        };

        current_row_values
            .iter()
            .map(|value| {
                if value.is_null() {
                    return Ok(value.clone());
                }
                if SEARCH_SIDE == is_descending {
                    // TODO: Handle positive overflows
                    value.add(delta)
                } else if value.is_unsigned() && value < delta {
                    // NOTE: This gets a polymorphic zero without having long coercion code for ScalarValue.
                    //       If we decide to implement a "default" construction mechanism for ScalarValue,
                    //       change the following statement to use that.
                    value.sub(value)
                } else {
                    // TODO: Handle negative overflows
                    value.sub(delta)
                }
            })
            .collect::<Result<Vec<ScalarValue>>>()?
    } else {
        current_row_values
    };
    let new_sort_options = if !window_sort_keys.is_empty() {
        sort_options
            .into_iter()
            .map(|elem| {
                let descending =
                    if let Some(column_info) = window_sort_keys[0].column_info {
                        !column_info.is_ascending
                    } else {
                        elem.descending
                    };
                SortOptions {
                    nulls_first: elem.nulls_first,
                    descending: descending,
                }
            })
            .collect::<Vec<SortOptions>>()
    } else {
        sort_options.to_vec()
    };
    // `BISECT_SIDE` true means bisect_left, false means bisect_right
    bisect::<BISECT_SIDE>(range_columns, &end_range, &new_sort_options)
}

fn get_reversed_window_frame(window_frame: &WindowFrame) -> Result<Arc<WindowFrame>> {
    let mut new_window_frame = window_frame.clone();
    match &window_frame.start_bound {
        WindowFrameBound::Preceding(elem) => {
            new_window_frame.end_bound = WindowFrameBound::Following(elem.clone())
        }
        WindowFrameBound::Following(elem) => {
            new_window_frame.end_bound = WindowFrameBound::Preceding(elem.clone())
        }
        WindowFrameBound::CurrentRow => {
            new_window_frame.end_bound = WindowFrameBound::CurrentRow
        }
    };
    match &window_frame.end_bound {
        WindowFrameBound::Preceding(elem) => {
            new_window_frame.start_bound = WindowFrameBound::Following(elem.clone())
        }
        WindowFrameBound::Following(elem) => {
            new_window_frame.start_bound = WindowFrameBound::Preceding(elem.clone())
        }
        WindowFrameBound::CurrentRow => {
            new_window_frame.start_bound = WindowFrameBound::CurrentRow
        }
    };
    Ok(Arc::new(new_window_frame))
}

#[derive(Debug, Clone)]
pub struct AggregateWindowAccumulatorState {
    pub last_range: (usize, usize),
    pub cur_range: (usize, usize),
    pub last_idx: usize,
    pub aggregate_state: Vec<AggregateState>,
}

impl AggregateWindowAccumulatorState {
    /// create a new aggregate window function expression
    pub fn new() -> Self {
        Self {
            last_range: (0, 0),
            cur_range: (0, 0),
            last_idx: 0,
            aggregate_state: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct WindowAccumulatorResult {
    pub partition_id: Vec<ScalarValue>,
    pub col: Option<ArrayRef>,
    pub num_rows: usize,
}
