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
use datafusion_common::bisect::{bisect, find_bisect_point};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use std::cmp::min;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;

pub trait WindowFrameStateImpl: Debug {
    /// We use start and end bounds to calculate current row's starting and ending range.
    fn calculate_range(
        &mut self,
        window_frame: &Option<Arc<WindowFrame>>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)>;
}

#[derive(Debug)]
pub enum WindowFrameState {
    Rows(WindowFrameStateRows),
    Range(WindowFrameStateRange),
    Groups(WindowFrameStateGroups),
    NoFrame(WindowFrameStateNoFrame),
}

impl WindowFrameStateImpl for WindowFrameState {
    fn calculate_range(
        &mut self,
        window_frame: &Option<Arc<WindowFrame>>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)> {
        match *self {
            WindowFrameState::Rows(ref mut frame) => frame.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
            ),
            WindowFrameState::Range(ref mut frame) => frame.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
            ),
            WindowFrameState::Groups(ref mut frame) => frame.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
            ),
            WindowFrameState::NoFrame(ref mut frame) => frame.calculate_range(
                window_frame,
                range_columns,
                sort_options,
                length,
                idx,
            ),
        }
    }
}

impl WindowFrameState {
    pub fn new(window_frame: &Option<Arc<WindowFrame>>) -> WindowFrameState {
        if let Some(window_frame) = window_frame {
            match window_frame.units {
                WindowFrameUnits::Rows => {
                    WindowFrameState::Rows(WindowFrameStateRows::default())
                }
                WindowFrameUnits::Range => {
                    WindowFrameState::Range(WindowFrameStateRange::default())
                }
                WindowFrameUnits::Groups => {
                    WindowFrameState::Groups(WindowFrameStateGroups::default())
                }
            }
        } else {
            WindowFrameState::NoFrame(WindowFrameStateNoFrame::default())
        }
    }
}

#[derive(Debug, Default)]
pub struct WindowFrameStateNoFrame {}

impl WindowFrameStateImpl for WindowFrameStateNoFrame {
    fn calculate_range(
        &mut self,
        _window_frame: &Option<Arc<WindowFrame>>,
        _range_columns: &[ArrayRef],
        _sort_options: &[SortOptions],
        length: usize,
        _idx: usize,
    ) -> Result<(usize, usize)> {
        Ok((0, length))
    }
}

#[derive(Debug, Default)]
pub struct WindowFrameStateRows {}

impl WindowFrameStateImpl for WindowFrameStateRows {
    fn calculate_range(
        &mut self,
        window_frame: &Option<Arc<WindowFrame>>,
        _range_columns: &[ArrayRef],
        _sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)> {
        if let Some(window_frame) = window_frame {
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
                        "Frame start cannot be UNBOUNDED FOLLOWING '{:?}'",
                        window_frame
                    )))
                }
                WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                    min(idx + n as usize, length)
                }
                // ERRONEOUS FRAMES
                WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
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
                WindowFrameBound::CurrentRow => idx + 1,
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
                WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                    min(idx + n as usize + 1, length)
                }
                // ERRONEOUS FRAMES
                WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                    return Err(DataFusionError::Internal(
                        "Rows should be Uint".to_string(),
                    ))
                }
            };
            Ok((start, end))
        } else {
            Ok((0, length))
        }
    }
}

/// This structure encapsulates all the state information we require as we
/// scan ranges of data while processing window frames.
#[derive(Debug, Default)]
pub struct WindowFrameStateRange {}

impl WindowFrameStateImpl for WindowFrameStateRange {
    fn calculate_range(
        &mut self,
        window_frame: &Option<Arc<WindowFrame>>,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)> {
        if let Some(window_frame) = window_frame {
            let start = match &window_frame.start_bound {
                WindowFrameBound::Preceding(n) => {
                    if n.is_null() {
                        // UNBOUNDED PRECEDING
                        0
                    } else {
                        self.calculate_index_of_row::<true, true>(
                            range_columns,
                            sort_options,
                            idx,
                            Some(n),
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
                        )?
                    }
                }
                WindowFrameBound::Following(n) => self
                    .calculate_index_of_row::<true, false>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
                    )?,
            };
            let end = match &window_frame.end_bound {
                WindowFrameBound::Preceding(n) => self
                    .calculate_index_of_row::<false, true>(
                        range_columns,
                        sort_options,
                        idx,
                        Some(n),
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
                        )?
                    }
                }
                WindowFrameBound::Following(n) => {
                    if n.is_null() {
                        // UNBOUNDED FOLLOWING
                        length
                    } else {
                        self.calculate_index_of_row::<false, false>(
                            range_columns,
                            sort_options,
                            idx,
                            Some(n),
                        )?
                    }
                }
            };
            Ok((start, end))
        } else {
            Ok((0, length))
        }
    }
}

impl WindowFrameStateRange {
    fn calculate_index_of_row<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        sort_options: &[SortOptions],
        idx: usize,
        delta: Option<&ScalarValue>,
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
        // `BISECT_SIDE` true means bisect_left, false means bisect_right
        bisect::<BISECT_SIDE>(range_columns, &end_range, sort_options)
    }
}

/// In GROUPS mode, rows with duplicate sorting values are grouped together.
/// Therefore, there must be an ORDER BY clause in the window definition to use GROUPS mode.
/// The syntax is as follows:
///     GROUPS frame_start [ frame_exclusion ]
///     GROUPS BETWEEN frame_start AND frame_end [ frame_exclusion ]
/// The optional frame_exclusion specifier is not yet supported.
/// The frame_start and frame_end parameters allow us to specify which rows the window
/// frame starts and ends with. They accept the following values:
///    - UNBOUNDED PRECEDING: Start with the first row of the partition. Possible only in frame_start.
///    - offset PRECEDING: When used in frame_start, it refers to the first row of the group
///                        that comes "offset" groups before the current group (i.e. the group
///                        containing the current row). When used in frame_end, it refers to the
///                        last row of the group that comes "offset" groups before the current group.
///    - CURRENT ROW: When used in frame_start, it refers to the first row of the group containing
///                   the current row. When used in frame_end, it refers to the last row of the group
///                   containing the current row.
///    - offset FOLLOWING: When used in frame_start, it refers to the first row of the group
///                        that comes "offset" groups after the current group (i.e. the group
///                        containing the current row). When used in frame_end, it refers to the
///                        last row of the group that comes "offset" groups after the current group.
///    - UNBOUNDED FOLLOWING: End with the last row of the partition. Possible only in frame_end.

/// This structure encapsulates all the state information we require as we
/// scan groups of data while processing window frames.

#[derive(Debug, Default)]
pub struct WindowFrameStateGroups {
    current_group_idx: u64,
    group_start_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    previous_row_values: Option<Vec<ScalarValue>>,
    reached_end: bool,
    window_frame_end_idx: u64,
    window_frame_start_idx: u64,
}

impl WindowFrameStateImpl for WindowFrameStateGroups {
    fn calculate_range(
        &mut self,
        window_frame: &Option<Arc<WindowFrame>>,
        range_columns: &[ArrayRef],
        _sort_options: &[SortOptions],
        length: usize,
        idx: usize,
    ) -> Result<(usize, usize)> {
        if range_columns.is_empty() {
            return Err(DataFusionError::Execution(
                "GROUPS mode requires an ORDER BY clause".to_string(),
            ));
        }
        if let Some(window_frame) = window_frame {
            let start = match window_frame.start_bound {
                // UNBOUNDED PRECEDING
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => 0,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => self
                    .calculate_index_of_group::<true, true>(
                        range_columns,
                        idx,
                        n,
                        length,
                    )?,
                WindowFrameBound::CurrentRow => self
                    .calculate_index_of_group::<true, true>(
                        range_columns,
                        idx,
                        0,
                        length,
                    )?,
                WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => self
                    .calculate_index_of_group::<true, false>(
                        range_columns,
                        idx,
                        n,
                        length,
                    )?,
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                    return Err(DataFusionError::Internal(format!(
                        "Frame start cannot be UNBOUNDED FOLLOWING '{:?}'",
                        window_frame
                    )))
                }
                // ERRONEOUS FRAMES
                WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                    return Err(DataFusionError::Internal(
                        "Groups should be Uint".to_string(),
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
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => self
                    .calculate_index_of_group::<false, true>(
                        range_columns,
                        idx,
                        n,
                        length,
                    )?,
                WindowFrameBound::CurrentRow => self
                    .calculate_index_of_group::<false, false>(
                        range_columns,
                        idx,
                        0,
                        length,
                    )?,
                WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => self
                    .calculate_index_of_group::<false, false>(
                        range_columns,
                        idx,
                        n,
                        length,
                    )?,
                // UNBOUNDED FOLLOWING
                WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
                // ERRONEOUS FRAMES
                WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                    return Err(DataFusionError::Internal(
                        "Groups should be Uint".to_string(),
                    ))
                }
            };
            Ok((start, end))
        } else {
            Ok((0, length))
        }
    }
}

impl WindowFrameStateGroups {
    /// This function is the main public interface of WindowFrameStateGroups. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `BISECT_SIDE`
    /// supplied as false and true, respectively).
    pub fn calculate_index_of_group<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        idx: usize,
        delta: u64,
        length: usize,
    ) -> Result<usize> {
        let current_row_values = range_columns
            .iter()
            .map(|col| ScalarValue::try_from_array(col, idx))
            .collect::<Result<Vec<ScalarValue>>>()?;

        if BISECT_SIDE {
            // When we call this function to get the window frame start index, it tries to initialize
            // the internal grouping state if this is not already done before. This initialization takes
            // place only when the window frame start index is greater than or equal to zero. In this
            // case, the current row is stored in group_start_indices, with row values as the group
            // identifier and row index as the start index of the group.
            if !self.initialized() {
                self.initialize::<SEARCH_SIDE>(delta, range_columns)?;
            }
        } else if !self.reached_end {
            // When we call this function to get the window frame end index, it extends the window
            // frame one by one until the current row's window frame end index is reached by finding
            // the next group.
            self.extend_window_frame_if_necessary::<SEARCH_SIDE>(range_columns, delta)?;
        }
        // We keep track of previous row values, so that a group change can be identified.
        // If there is a group change, the window frame is advanced and shifted by one group.
        let group_change = match &self.previous_row_values {
            None => false,
            Some(values) => &current_row_values != values,
        };
        if self.previous_row_values.is_none() || group_change {
            self.previous_row_values = Some(current_row_values);
        }
        if group_change {
            self.current_group_idx += 1;
            self.advance_one_group::<SEARCH_SIDE>(range_columns)?;
            self.shift_one_group::<SEARCH_SIDE>(delta);
        }
        Ok(if self.group_start_indices.is_empty() {
            if self.reached_end {
                length
            } else {
                0
            }
        } else if BISECT_SIDE {
            match self.group_start_indices.get(0) {
                Some(&(_, idx)) => idx,
                None => 0,
            }
        } else {
            match (self.reached_end, self.group_start_indices.back()) {
                (false, Some(&(_, idx))) => idx,
                _ => length,
            }
        })
    }

    fn extend_window_frame_if_necessary<const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        delta: u64,
    ) -> Result<()> {
        let current_window_frame_end_idx = if !SEARCH_SIDE {
            self.current_group_idx + delta + 1
        } else if self.current_group_idx >= delta {
            self.current_group_idx - delta + 1
        } else {
            0
        };
        if current_window_frame_end_idx == 0 {
            // the end index of the window frame is still before the first index
            return Ok(());
        }
        if self.group_start_indices.is_empty() {
            self.initialize_window_frame_start(range_columns)?;
        }
        while !self.reached_end
            && self.window_frame_end_idx <= current_window_frame_end_idx
        {
            self.advance_one_group::<SEARCH_SIDE>(range_columns)?;
        }
        Ok(())
    }

    fn initialize<const SEARCH_SIDE: bool>(
        &mut self,
        delta: u64,
        range_columns: &[ArrayRef],
    ) -> Result<()> {
        if !SEARCH_SIDE {
            self.window_frame_start_idx = self.current_group_idx + delta;
            self.initialize_window_frame_start(range_columns)
        } else if self.current_group_idx >= delta {
            self.window_frame_start_idx = self.current_group_idx - delta;
            self.initialize_window_frame_start(range_columns)
        } else {
            Ok(())
        }
    }

    fn initialize_window_frame_start(
        &mut self,
        range_columns: &[ArrayRef],
    ) -> Result<()> {
        let mut group_values = range_columns
            .iter()
            .map(|col| ScalarValue::try_from_array(col, 0))
            .collect::<Result<Vec<ScalarValue>>>()?;
        let mut start_idx: usize = 0;
        for _ in 0..self.window_frame_start_idx {
            let next_group_and_start_index =
                WindowFrameStateGroups::find_next_group_and_start_index(
                    range_columns,
                    &group_values,
                    start_idx,
                )?;
            if let Some(entry) = next_group_and_start_index {
                (group_values, start_idx) = entry;
            } else {
                // not enough groups to generate a window frame
                self.window_frame_end_idx = self.window_frame_start_idx;
                self.reached_end = true;
                return Ok(());
            }
        }
        self.group_start_indices
            .push_back((group_values, start_idx));
        self.window_frame_end_idx = self.window_frame_start_idx + 1;
        Ok(())
    }

    fn initialized(&self) -> bool {
        self.reached_end || !self.group_start_indices.is_empty()
    }

    /// This function advances the window frame by one group.
    fn advance_one_group<const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
    ) -> Result<()> {
        let last_group_values = self.group_start_indices.back();
        let last_group_values = if let Some(values) = last_group_values {
            values
        } else {
            return Ok(());
        };
        let next_group_and_start_index =
            WindowFrameStateGroups::find_next_group_and_start_index(
                range_columns,
                &last_group_values.0,
                last_group_values.1,
            )?;
        if let Some(entry) = next_group_and_start_index {
            self.group_start_indices.push_back(entry);
            self.window_frame_end_idx += 1;
        } else {
            // not enough groups to proceed
            self.reached_end = true;
        }
        Ok(())
    }

    /// This function drops the oldest group from the window frame.
    fn shift_one_group<const SEARCH_SIDE: bool>(&mut self, delta: u64) {
        let current_window_frame_start_idx = if !SEARCH_SIDE {
            self.current_group_idx + delta
        } else if self.current_group_idx >= delta {
            self.current_group_idx - delta
        } else {
            0
        };
        if current_window_frame_start_idx > self.window_frame_start_idx {
            self.group_start_indices.pop_front();
            self.window_frame_start_idx += 1;
        }
    }

    /// This function finds the next group and its start index for a given group and start index.
    /// It utilizes an exponentially growing step size to find the group boundary.
    // TODO: For small group sizes, proceeding one-by-one to find the group change can be more efficient.
    // Statistics about previous group sizes can be used to choose one-by-one vs. exponentially growing,
    // or even to set the base step_size when exponentially growing. We can also create a benchmark
    // implementation to get insights about the crossover point.
    fn find_next_group_and_start_index(
        range_columns: &[ArrayRef],
        current_row_values: &[ScalarValue],
        idx: usize,
    ) -> Result<Option<(Vec<ScalarValue>, usize)>> {
        let mut step_size: usize = 1;
        let data_size: usize = range_columns
            .get(0)
            .ok_or_else(|| {
                DataFusionError::Internal("Column array shouldn't be empty".to_string())
            })?
            .len();
        let mut low = idx;
        let mut high = idx + step_size;
        while high < data_size {
            let val = range_columns
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, high))
                .collect::<Result<Vec<ScalarValue>>>()?;
            if val == current_row_values {
                low = high;
                step_size *= 2;
                high += step_size;
            } else {
                break;
            }
        }
        low = find_bisect_point(
            range_columns,
            current_row_values,
            |current, to_compare| Ok(current == to_compare),
            low,
            min(high, data_size),
        )?;
        if low == data_size {
            return Ok(None);
        }
        let val = range_columns
            .iter()
            .map(|arr| ScalarValue::try_from_array(arr, low))
            .collect::<Result<Vec<ScalarValue>>>()?;
        Ok(Some((val, low)))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    use crate::from_slice::FromSlice;

    use super::*;

    #[test]
    fn test_find_next_group_and_start_index() {
        const NUM_ROWS: usize = 6;
        const NEXT_INDICES: [usize; 5] = [1, 2, 4, 4, 5];

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 8., 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 3., 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 8., 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 8., 5., 0.0])),
        ];
        for (current_idx, next_idx) in NEXT_INDICES.iter().enumerate() {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, current_idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let next_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, *next_idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let res = WindowFrameStateGroups::find_next_group_and_start_index(
                &arrays,
                &current_row_values,
                current_idx,
            )
            .unwrap();
            assert_eq!(res, Some((next_row_values, *next_idx)));
        }
        let current_idx = NUM_ROWS - 1;
        let current_row_values = arrays
            .iter()
            .map(|col| ScalarValue::try_from_array(col, current_idx))
            .collect::<Result<Vec<ScalarValue>>>()
            .unwrap();
        let res = WindowFrameStateGroups::find_next_group_and_start_index(
            &arrays,
            &current_row_values,
            current_idx,
        )
        .unwrap();
        assert_eq!(res, None);
    }

    #[test]
    fn test_window_frame_groups_preceding_huge_delta() {
        const START: bool = true;
        const END: bool = false;
        const PRECEDING: bool = true;
        const DELTA: u64 = 10;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<PRECEDING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let start = window_frame_groups
                .calculate_index_of_group::<START, PRECEDING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, 0);
            let end = window_frame_groups
                .calculate_index_of_group::<END, PRECEDING>(&arrays, idx, DELTA, NUM_ROWS)
                .unwrap();
            assert_eq!(end, 0);
        }
    }

    #[test]
    fn test_window_frame_groups_following_huge_delta() {
        const START: bool = true;
        const END: bool = false;
        const FOLLOWING: bool = false;
        const DELTA: u64 = 10;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<FOLLOWING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
        assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
        assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let start = window_frame_groups
                .calculate_index_of_group::<START, FOLLOWING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, NUM_ROWS);
            let end = window_frame_groups
                .calculate_index_of_group::<END, FOLLOWING>(&arrays, idx, DELTA, NUM_ROWS)
                .unwrap();
            assert_eq!(end, NUM_ROWS);
        }
    }

    #[test]
    fn test_window_frame_groups_preceding_and_following_huge_delta() {
        const START: bool = true;
        const END: bool = false;
        const FOLLOWING: bool = false;
        const PRECEDING: bool = true;
        const DELTA: u64 = 10;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, NUM_ROWS as u64);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), NUM_ROWS);

        for idx in 0..NUM_ROWS {
            let start = window_frame_groups
                .calculate_index_of_group::<START, PRECEDING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, 0);
            let end = window_frame_groups
                .calculate_index_of_group::<END, FOLLOWING>(&arrays, idx, DELTA, NUM_ROWS)
                .unwrap();
            assert_eq!(end, NUM_ROWS);
        }
    }

    #[test]
    fn test_window_frame_groups_preceding_and_following_1() {
        const START: bool = true;
        const END: bool = false;
        const FOLLOWING: bool = false;
        const PRECEDING: bool = true;
        const DELTA: u64 = 1;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 2 * DELTA + 1);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(
            window_frame_groups.group_start_indices.len(),
            2 * DELTA as usize + 1
        );

        for idx in 0..NUM_ROWS {
            let start_idx = if idx < DELTA as usize {
                0
            } else {
                idx - DELTA as usize
            };
            let start = window_frame_groups
                .calculate_index_of_group::<START, PRECEDING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, start_idx);
            let end_idx = if idx + 1 + DELTA as usize > NUM_ROWS {
                NUM_ROWS
            } else {
                idx + 1 + DELTA as usize
            };
            let end = window_frame_groups
                .calculate_index_of_group::<END, FOLLOWING>(&arrays, idx, DELTA, NUM_ROWS)
                .unwrap();
            assert_eq!(end, end_idx);
        }
    }

    #[test]
    fn test_window_frame_groups_preceding_1_and_unbounded_following() {
        const START: bool = true;
        const PRECEDING: bool = true;
        const DELTA: u64 = 1;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let start_idx = if idx < DELTA as usize {
                0
            } else {
                idx - DELTA as usize
            };
            let start = window_frame_groups
                .calculate_index_of_group::<START, PRECEDING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, start_idx);
        }
    }

    #[test]
    fn test_window_frame_groups_current_and_unbounded_following() {
        const START: bool = true;
        const PRECEDING: bool = true;
        const DELTA: u64 = 0;
        const NUM_ROWS: usize = 5;

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let mut window_frame_groups = WindowFrameStateGroups::default();
        window_frame_groups
            .initialize::<PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 1);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 1);

        for idx in 0..NUM_ROWS {
            let start = window_frame_groups
                .calculate_index_of_group::<START, PRECEDING>(
                    &arrays, idx, DELTA, NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, idx);
        }
    }
}
