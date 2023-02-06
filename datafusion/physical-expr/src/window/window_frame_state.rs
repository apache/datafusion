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

//! This module provides utilities for window frame index calculations depending on the window frame mode:
//! RANGE, ROWS, GROUPS.

use arrow::array::ArrayRef;
use arrow::compute::kernels::sort::SortOptions;
use datafusion_common::utils::{
    compare_rows, get_row_at_idx, search_in_slice, search_till_change,
};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// This object stores the window frame state for use in incremental calculations.
#[derive(Debug)]
pub enum WindowFrameContext<'a> {
    // ROWS-frames are inherently stateless:
    Rows(&'a Arc<WindowFrame>),
    // RANGE-frames will soon have a stateful implementation that is more efficient than a stateless one:
    Range {
        window_frame: &'a Arc<WindowFrame>,
        state: WindowFrameStateRange,
    },
    // GROUPS-frames have a stateful implementation that is more efficient than a stateless one:
    Groups {
        window_frame: &'a Arc<WindowFrame>,
        state: WindowFrameStateGroups,
    },
}

impl<'a> WindowFrameContext<'a> {
    /// Create a new default state for the given window frame.
    pub fn new(window_frame: &'a Arc<WindowFrame>) -> Self {
        match window_frame.units {
            WindowFrameUnits::Rows => WindowFrameContext::Rows(window_frame),
            WindowFrameUnits::Range => WindowFrameContext::Range {
                window_frame,
                state: WindowFrameStateRange::default(),
            },
            WindowFrameUnits::Groups => WindowFrameContext::Groups {
                window_frame,
                state: WindowFrameStateGroups::default(),
                // state: WindowFrameStateGroups::default(),
            },
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    pub fn calculate_range(
        &mut self,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        last_range: &Range<usize>,
    ) -> Result<Range<usize>> {
        match *self {
            WindowFrameContext::Rows(window_frame) => {
                Self::calculate_range_rows(window_frame, length, idx)
            }
            // sort_options is used in RANGE mode calculations because the ordering and the position of the nulls
            // have impact on the range calculations and comparison of the rows.
            WindowFrameContext::Range {
                window_frame,
                ref mut state,
            } => state.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
                last_range,
            ),
            // sort_options is not used in GROUPS mode calculations as the inequality of two rows is the indicator
            // of a group change, and the ordering and the position of the nulls do not have impact on inequality.
            WindowFrameContext::Groups {
                window_frame,
                ref mut state,
            } => state.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
                last_range,
            ),
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    fn calculate_range_rows(
        window_frame: &Arc<WindowFrame>,
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
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
            WindowFrameBound::CurrentRow => idx,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                return Err(DataFusionError::Internal(format!(
                    "Frame start cannot be UNBOUNDED FOLLOWING '{window_frame:?}'"
                )))
            }
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                min(idx + n as usize, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(DataFusionError::Internal("Rows should be Uint".to_string()))
            }
        };
        let end = match window_frame.end_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => {
                return Err(DataFusionError::Internal(format!(
                    "Frame end cannot be UNBOUNDED PRECEDING '{window_frame:?}'"
                )))
            }
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                if idx >= n as usize {
                    idx - n as usize + 1
                } else {
                    0
                }
            }
            WindowFrameBound::CurrentRow => idx + 1,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                min(idx + n as usize + 1, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return Err(DataFusionError::Internal("Rows should be Uint".to_string()))
            }
        };
        Ok(Range { start, end })
    }
}

/// This structure encapsulates all the state information we require as we
/// scan ranges of data while processing window frames. Currently we calculate
/// things from scratch every time, but we will make this incremental in the future.
#[derive(Debug, Default)]
pub struct WindowFrameStateRange {}

impl WindowFrameStateRange {
    /// This function calculates beginning/ending indices for the frame of the current row.
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        last_range: &Range<usize>,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => {
                if range_columns.is_empty() {
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
            WindowFrameBound::CurrentRow => {
                if range_columns.is_empty() {
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        idx: usize,
        delta: Option<&ScalarValue>,
        last_range: &Range<usize>,
        length: usize,
    ) -> Result<usize> {
        let current_row_values = get_row_at_idx(range_columns, idx)?;
        let end_range = if let Some(delta) = delta {
            let is_descending: bool = sort_options
                .first()
                .ok_or_else(|| DataFusionError::Internal("Array is empty".to_string()))?
                .descending;

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
        let search_start = if SIDE {
            last_range.start
        } else {
            last_range.end
        };
        let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
            let cmp = compare_rows(current, target, sort_options)?;
            Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
        };
        search_in_slice(range_columns, &end_range, compare_fn, search_start, length)
    }
}

// In GROUPS mode, rows with duplicate sorting values are grouped together.
// Therefore, there must be an ORDER BY clause in the window definition to use GROUPS mode.
// The syntax is as follows:
//     GROUPS frame_start [ frame_exclusion ]
//     GROUPS BETWEEN frame_start AND frame_end [ frame_exclusion ]
// The optional frame_exclusion specifier is not yet supported.
// The frame_start and frame_end parameters allow us to specify which rows the window
// frame starts and ends with. They accept the following values:
//    - UNBOUNDED PRECEDING: Start with the first row of the partition. Possible only in frame_start.
//    - offset PRECEDING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups before the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups before the current group.
//    - CURRENT ROW: When used in frame_start, it refers to the first row of the group containing
//                   the current row. When used in frame_end, it refers to the last row of the group
//                   containing the current row.
//    - offset FOLLOWING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups after the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups after the current group.
//    - UNBOUNDED FOLLOWING: End with the last row of the partition. Possible only in frame_end.

// This structure encapsulates all the state information we require as we
// scan groups of data while processing window frames.
#[derive(Debug, Default)]
pub struct WindowFrameStateGroups {
    group_start_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    // Keeps the groups index that row index belongs
    cur_group_idx: usize,
}

impl WindowFrameStateGroups {
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
        last_range: &Range<usize>,
    ) -> Result<Range<usize>> {
        if range_columns.is_empty() {
            return Err(DataFusionError::Execution(
                "GROUPS mode requires an ORDER BY clause".to_string(),
            ));
        }
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => {
                if range_columns.is_empty() {
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    sort_options,
                    idx,
                    Some(n),
                    last_range,
                    length,
                )?,
            WindowFrameBound::CurrentRow => {
                if range_columns.is_empty() {
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        None,
                        last_range,
                        length,
                    )?
                }
            }
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                        last_range,
                        length,
                    )?
                }
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively).
    fn calculate_index_of_row<const SIDE: bool, const PRECEDING: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        _sort_options: &[SortOptions],
        idx: usize,
        delta: Option<&ScalarValue>,
        last_range: &Range<usize>,
        length: usize,
    ) -> Result<usize> {
        let search_start = if SIDE {
            last_range.start
        } else {
            last_range.end
        };
        let delta = if let Some(delta) = delta {
            match delta {
                ScalarValue::UInt64(Some(val)) => Ok(*val as usize),
                _ => Err(DataFusionError::Execution("expects uint64".to_string())),
            }
        } else {
            Ok(0)
        }?;
        let mut change_idx = search_start;
        let last_group = self.group_start_indices.back();
        if let Some((_last_group, bound_idx)) = last_group {
            // Start searching from change point from last boundary
            change_idx = *bound_idx;
        }

        // Progress groups until idx is inside a group
        while idx > change_idx {
            let group_row = get_row_at_idx(range_columns, change_idx)?;
            change_idx = search_till_change(range_columns, change_idx, length)?;
            self.group_start_indices.push_back((group_row, change_idx));
        }

        // Update the group index `idx` belongs.
        while self.cur_group_idx < self.group_start_indices.len() {
            if idx >= self.group_start_indices[self.cur_group_idx].1 {
                self.cur_group_idx += 1;
            } else {
                break;
            }
        }
        let group_idx = if PRECEDING {
            if self.cur_group_idx > delta {
                self.cur_group_idx - delta
            } else {
                0
            }
        } else {
            self.cur_group_idx + delta
        };

        // Expand group_start_indices until it includes at least group_idx
        while self.group_start_indices.len() <= group_idx && change_idx < length {
            // println!("change idx: {:?}, idx: {:?}, self.cur_group_idx: {:?}", change_idx, idx, self.cur_group_idx);
            let group_row = get_row_at_idx(range_columns, change_idx)?;
            change_idx = search_till_change(range_columns, change_idx, length)?;
            self.group_start_indices.push_back((group_row, change_idx));
        }
        Ok(match (SIDE, PRECEDING) {
            (true, _) => {
                let group_idx = min(group_idx, self.group_start_indices.len());
                if group_idx > 0 {
                    self.group_start_indices[group_idx - 1].1
                } else {
                    0
                }
            }
            (false, true) => {
                if self.cur_group_idx >= delta {
                    let group_idx = self.cur_group_idx - delta;
                    self.group_start_indices[group_idx].1
                } else {
                    0
                }
            }
            (false, false) => {
                let group_idx = min(
                    self.cur_group_idx + delta,
                    self.group_start_indices.len() - 1,
                );
                self.group_start_indices[group_idx].1
            }
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use arrow::array::Float64Array;
//     use datafusion_common::ScalarValue;
//     use std::sync::Arc;
//
//     use crate::from_slice::FromSlice;
//
//     use super::*;
//
//     struct TestData {
//         arrays: Vec<ArrayRef>,
//         group_indices: [usize; 6],
//         num_groups: usize,
//         num_rows: usize,
//         next_group_indices: [usize; 5],
//     }
//
//     fn test_data() -> TestData {
//         let num_groups: usize = 5;
//         let num_rows: usize = 6;
//         let group_indices = [0, 1, 2, 2, 4, 5];
//         let next_group_indices = [1, 2, 4, 4, 5];
//
//         let arrays: Vec<ArrayRef> = vec![
//             Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 8., 9., 10.])),
//             Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 3., 4.0, 5.0])),
//             Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 8., 10., 11.0])),
//             Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 8., 5., 0.0])),
//         ];
//         TestData {
//             arrays,
//             group_indices,
//             num_groups,
//             num_rows,
//             next_group_indices,
//         }
//     }
//
//     #[test]
//     fn test_find_next_group_and_start_index() {
//         let test_data = test_data();
//         for (current_idx, next_idx) in test_data.next_group_indices.iter().enumerate() {
//             let current_row_values = test_data
//                 .arrays
//                 .iter()
//                 .map(|col| ScalarValue::try_from_array(col, current_idx))
//                 .collect::<Result<Vec<ScalarValue>>>()
//                 .unwrap();
//             let next_row_values = test_data
//                 .arrays
//                 .iter()
//                 .map(|col| ScalarValue::try_from_array(col, *next_idx))
//                 .collect::<Result<Vec<ScalarValue>>>()
//                 .unwrap();
//             let res = WindowFrameStateGroups::find_next_group_and_start_index(
//                 &test_data.arrays,
//                 &current_row_values,
//                 current_idx,
//             )
//             .unwrap();
//             assert_eq!(res, Some((next_row_values, *next_idx)));
//         }
//         let current_idx = test_data.num_rows - 1;
//         let current_row_values = test_data
//             .arrays
//             .iter()
//             .map(|col| ScalarValue::try_from_array(col, current_idx))
//             .collect::<Result<Vec<ScalarValue>>>()
//             .unwrap();
//         let res = WindowFrameStateGroups::find_next_group_and_start_index(
//             &test_data.arrays,
//             &current_row_values,
//             current_idx,
//         )
//         .unwrap();
//         assert_eq!(res, None);
//     }
//
//     #[test]
//     fn test_window_frame_groups_preceding_delta_greater_than_partition_size() {
//         const START: bool = true;
//         const END: bool = false;
//         const PRECEDING: bool = true;
//         const DELTA: u64 = 10;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<PRECEDING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 0);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         window_frame_groups
//             .extend_window_frame_if_necessary::<PRECEDING>(&test_data.arrays, DELTA)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 0);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         for idx in 0..test_data.num_rows {
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, 0);
//             let end = window_frame_groups
//                 .calculate_index_of_group::<END, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(end, 0);
//         }
//     }
//
//     #[test]
//     fn test_window_frame_groups_following_delta_greater_than_partition_size() {
//         const START: bool = true;
//         const END: bool = false;
//         const FOLLOWING: bool = false;
//         const DELTA: u64 = 10;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<FOLLOWING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
//         assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
//         assert!(window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         window_frame_groups
//             .extend_window_frame_if_necessary::<FOLLOWING>(&test_data.arrays, DELTA)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
//         assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
//         assert!(window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         for idx in 0..test_data.num_rows {
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, FOLLOWING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, test_data.num_rows);
//             let end = window_frame_groups
//                 .calculate_index_of_group::<END, FOLLOWING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(end, test_data.num_rows);
//         }
//     }
//
//     #[test]
//     fn test_window_frame_groups_preceding_and_following_delta_greater_than_partition_size(
//     ) {
//         const START: bool = true;
//         const END: bool = false;
//         const FOLLOWING: bool = false;
//         const PRECEDING: bool = true;
//         const DELTA: u64 = 10;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<PRECEDING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 0);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         window_frame_groups
//             .extend_window_frame_if_necessary::<FOLLOWING>(&test_data.arrays, DELTA)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(
//             window_frame_groups.window_frame_end_idx,
//             test_data.num_groups as u64
//         );
//         assert!(window_frame_groups.reached_end);
//         assert_eq!(
//             window_frame_groups.group_start_indices.len(),
//             test_data.num_groups
//         );
//
//         for idx in 0..test_data.num_rows {
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, 0);
//             let end = window_frame_groups
//                 .calculate_index_of_group::<END, FOLLOWING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(end, test_data.num_rows);
//         }
//     }
//
//     #[test]
//     fn test_window_frame_groups_preceding_and_following_1() {
//         const START: bool = true;
//         const END: bool = false;
//         const FOLLOWING: bool = false;
//         const PRECEDING: bool = true;
//         const DELTA: u64 = 1;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<PRECEDING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 0);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         window_frame_groups
//             .extend_window_frame_if_necessary::<FOLLOWING>(&test_data.arrays, DELTA)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 2 * DELTA + 1);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(
//             window_frame_groups.group_start_indices.len(),
//             2 * DELTA as usize + 1
//         );
//
//         for idx in 0..test_data.num_rows {
//             let start_idx = if idx < DELTA as usize {
//                 0
//             } else {
//                 test_data.group_indices[idx] - DELTA as usize
//             };
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, test_data.group_indices[start_idx]);
//             let mut end_idx = if idx >= test_data.num_groups {
//                 test_data.num_rows
//             } else {
//                 test_data.next_group_indices[idx]
//             };
//             for _ in 0..DELTA {
//                 end_idx = if end_idx >= test_data.num_groups {
//                     test_data.num_rows
//                 } else {
//                     test_data.next_group_indices[end_idx]
//                 };
//             }
//             let end = window_frame_groups
//                 .calculate_index_of_group::<END, FOLLOWING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(end, end_idx);
//         }
//     }
//
//     #[test]
//     fn test_window_frame_groups_preceding_1_and_unbounded_following() {
//         const START: bool = true;
//         const PRECEDING: bool = true;
//         const DELTA: u64 = 1;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<PRECEDING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 0);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 0);
//
//         for idx in 0..test_data.num_rows {
//             let start_idx = if idx < DELTA as usize {
//                 0
//             } else {
//                 test_data.group_indices[idx] - DELTA as usize
//             };
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, test_data.group_indices[start_idx]);
//         }
//     }
//
//     #[test]
//     fn test_window_frame_groups_current_and_unbounded_following() {
//         const START: bool = true;
//         const PRECEDING: bool = true;
//         const DELTA: u64 = 0;
//
//         let test_data = test_data();
//         let mut window_frame_groups = WindowFrameStateGroups::default();
//         window_frame_groups
//             .initialize::<PRECEDING>(DELTA, &test_data.arrays)
//             .unwrap();
//         assert_eq!(window_frame_groups.window_frame_start_idx, 0);
//         assert_eq!(window_frame_groups.window_frame_end_idx, 1);
//         assert!(!window_frame_groups.reached_end);
//         assert_eq!(window_frame_groups.group_start_indices.len(), 1);
//
//         for idx in 0..test_data.num_rows {
//             let start = window_frame_groups
//                 .calculate_index_of_group::<START, PRECEDING>(
//                     &test_data.arrays,
//                     idx,
//                     DELTA,
//                     test_data.num_rows,
//                 )
//                 .unwrap();
//             assert_eq!(start, test_data.group_indices[idx]);
//         }
//     }
// }
