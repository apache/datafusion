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

//! This module provides the utilities for the GROUPS window frame index calculations.

use arrow::array::ArrayRef;
use std::cmp;
use std::collections::VecDeque;

use datafusion_common::bisect::find_bisect_point;
use datafusion_common::{DataFusionError, Result, ScalarValue};

/// In the GROUPS mode, the rows with duplicate sorting values are grouped together.
/// Therefore, there must be an ORDER BY clause in the window definition to use GROUPS mode.
/// The syntax is as follows:
///     GROUPS frame_start [ frame_exclusion ]
///     GROUPS BETWEEN frame_start AND frame_end [ frame_exclusion ]
/// The frame_exclusion is not yet supported.
/// The frame_start and frame_end parameters allow us to specify which rows the window frame starts and ends with.
/// They accept the following values:
///    - UNBOUNDED PRECEDING: (possible only in frame_start) start with the first row of the partition
///    - offset PRECEDING: When used as frame_start, it means the first row of the group which is a given number of
///                        groups before the current group (the group containing the current row).
///                        When used as frame_end, it means the last row of the group which is a given number of groups
///                        before the current group.
///    - CURRENT ROW: When used as frame_start, it means the first row in a group containing the current row.
///                   When used as frame_end, it means the last row in a group containing the current row.
///    - offset FOLLOWING: When used as frame_start, it means the first row of the group which is a given number of
///                        groups after the current group (the group containing the current row).
///                        When used as frame_end, it means the last row of the group which is a given number of groups
///                        after the current group.
///    - UNBOUNDED FOLLOWING: (possible only as a frame_end) end with the last row of the partition
///
/// In the following implementation, 'process' is the only public interface of the WindowFrameGroups. It is called for
/// frame_start and frame_end of each row, consecutively.
/// The call for frame_start, first, tries to initialize the WindowFrameGroups handler if it has not already been
/// initialized. The initialization means the setting of the window frame start index. The window frame start index can
/// only be set, if the current row's window frame start is greater or equal to zero depending on the offset. If the
/// window frame start index is greater or equal to zero, it means the current row can be stored in the
/// WindowFrameGroups handler's window frame (group_start_indices), the row values as the group identifier and the row
/// index as the start index of the group. Then, it keeps track of the previous row values, so that a group change can
/// be identified. If there is a group change, the WindowFrameGroups handler's window frame is proceeded by one group.
/// The call for frame_end, first, calculates the current row's window frame end index from the offset. If the current
/// row's window frame end index is greater than zero, the WindowFrameGroups handler's window frame is extended
/// one-by-one until the current row's window frame end index is reached by finding the next group.
/// The call to 'process' for frame_start returns the index of the the WindowFrameGroups handler's window frame's first
/// entry, if there is a window frame, otherwise it returns 0.
/// The call to 'process' for frame_end returns the index of the the WindowFrameGroups handler's window frame's last
/// entry, if there is a window frame, otherwise it returns the last index of the partition (length).

#[derive(Debug, Default)]
pub struct WindowFrameGroups {
    current_group_idx: u64,
    group_start_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    previous_row_values: Option<Vec<ScalarValue>>,
    reached_end: bool,
    window_frame_end_idx: u64,
    window_frame_start_idx: u64,
}

impl WindowFrameGroups {
    fn extend_window_frame_if_necessary<
        const BISECT_SIDE: bool,
        const SEARCH_SIDE: bool,
    >(
        &mut self,
        item_columns: &[ArrayRef],
        delta: u64,
    ) -> Result<()> {
        if BISECT_SIDE || self.reached_end {
            return Ok(());
        }
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
        while !self.reached_end
            && current_window_frame_end_idx >= self.window_frame_end_idx
        {
            if self.group_start_indices.is_empty() {
                self.initialize_window_frame_start(item_columns)?;
                continue;
            }
            self.proceed_one_group::<BISECT_SIDE, SEARCH_SIDE>(item_columns, delta)?;
        }
        Ok(())
    }

    fn initialize<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        delta: u64,
        item_columns: &[ArrayRef],
    ) -> Result<()> {
        if BISECT_SIDE {
            if !SEARCH_SIDE {
                self.window_frame_start_idx = self.current_group_idx + delta;
                return self.initialize_window_frame_start(item_columns);
            } else if self.current_group_idx >= delta {
                self.window_frame_start_idx = self.current_group_idx - delta;
                return self.initialize_window_frame_start(item_columns);
            }
        }
        Ok(())
    }

    fn initialize_window_frame_start(&mut self, item_columns: &[ArrayRef]) -> Result<()> {
        let mut group_values = item_columns
            .iter()
            .map(|col| ScalarValue::try_from_array(col, 0))
            .collect::<Result<Vec<ScalarValue>>>()?;
        let mut start_idx: usize = 0;
        for _ in 0..self.window_frame_start_idx {
            let next_group_and_start_index =
                WindowFrameGroups::find_next_group_and_start_index(
                    item_columns,
                    &group_values,
                    start_idx,
                    None,
                )?;
            match next_group_and_start_index {
                Some((group, index)) => {
                    group_values = group;
                    start_idx = index;
                }
                // not enough groups to generate a window frame
                None => {
                    self.reached_end = true;
                    self.window_frame_end_idx = self.window_frame_start_idx;
                    return Ok(());
                }
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

    /// This function proceeds the window frame by one group.
    /// First, it extends the window frame by adding one next group as the last entry.
    /// Then, if this function is called due to a group change (with BISECT_SIDE true), it pops the front entry.
    /// If this function is called with BISECT_SIDE false, it means we are trying to extend the window frame to reach the
    /// window frame size, so no popping of the front entry.
    fn proceed_one_group<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        item_columns: &[ArrayRef],
        delta: u64,
    ) -> Result<()> {
        let last_group_values = self.group_start_indices.back();
        let last_group_values = if let Some(values) = last_group_values {
            values
        } else {
            return Ok(());
        };
        let next_group_and_start_index =
            WindowFrameGroups::find_next_group_and_start_index(
                item_columns,
                &last_group_values.0,
                last_group_values.1,
                None,
            )?;
        match next_group_and_start_index {
            Some((group, index)) => {
                self.group_start_indices.push_back((group, index));
                self.window_frame_end_idx += 1;
            }
            // not enough groups to proceed
            None => {
                self.reached_end = true;
            }
        }
        if BISECT_SIDE {
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
        Ok(())
    }

    pub fn process<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        current_row_values: &[ScalarValue],
        delta: u64,
        item_columns: &[ArrayRef],
        length: usize,
    ) -> Result<usize> {
        if !self.initialized() {
            self.initialize::<BISECT_SIDE, SEARCH_SIDE>(delta, item_columns)?;
        }
        self.extend_window_frame_if_necessary::<BISECT_SIDE, SEARCH_SIDE>(
            item_columns,
            delta,
        )?;
        let group_change = match &self.previous_row_values {
            None => false,
            Some(values) => current_row_values != values,
        };
        if self.previous_row_values.is_none() || group_change {
            self.previous_row_values = Some(current_row_values.to_vec());
        }
        if group_change {
            self.current_group_idx += 1;
            self.proceed_one_group::<BISECT_SIDE, SEARCH_SIDE>(item_columns, delta)?;
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

    /// This function finds the next group and its start index for a given group and start index.
    /// It uses the bisect function, thus it should work on a bounded frame with a start and an end point.
    /// To find the end point, a step_size is used with a default value of 100. After proceeding the end point,
    /// if the entry at the end point still has the same group identifier (row values), the start point is set as the
    /// end point, and the end point is proceeded one step size more. The end point is found when the entry at the end
    /// point has a different group identifier (different row values), or the end of the partition is reached.
    /// Then, the bisect function can be applied with low as the start point, and high as the end point.
    ///
    /// Improvement:
    /// For small group sizes, proceeding one-by-one to find the group change can be more efficient.
    /// A statistics about the group sizes can be utilized to decide upon which approach to use, or even set the
    /// step_size.
    /// We will also need a benchmark implementation to prove the efficiency.
    fn find_next_group_and_start_index(
        item_columns: &[ArrayRef],
        current_row_values: &[ScalarValue],
        idx: usize,
        step_size: Option<usize>,
    ) -> Result<Option<(Vec<ScalarValue>, usize)>> {
        let step_size = if let Some(n) = step_size { n } else { 100 };
        if step_size == 0 {
            return Err(DataFusionError::Internal(
                "Step size cannot be 0".to_string(),
            ));
        }
        let data_size: usize = item_columns
            .get(0)
            .ok_or_else(|| {
                DataFusionError::Internal("Column array shouldn't be empty".to_string())
            })?
            .len();
        let mut low: usize = idx;
        let mut high: usize = cmp::min(low + step_size, data_size);
        while high < data_size {
            let val = item_columns
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, high))
                .collect::<Result<Vec<ScalarValue>>>()?;
            if val == current_row_values {
                low = high;
                high = cmp::min(low + step_size, data_size);
            } else {
                break;
            }
        }
        low = find_bisect_point(
            item_columns,
            current_row_values,
            |current, to_compare| Ok(current == to_compare),
            low,
            high,
        )?;
        if low == data_size {
            return Ok(None);
        }
        let val = item_columns
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
        const STEP_SIZE: usize = 1;

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
            let res = WindowFrameGroups::find_next_group_and_start_index(
                &arrays,
                &current_row_values,
                current_idx,
                None,
            )
            .unwrap();
            assert_eq!(res, Some((next_row_values.clone(), *next_idx)));
            let res = WindowFrameGroups::find_next_group_and_start_index(
                &arrays,
                &current_row_values,
                current_idx,
                Some(STEP_SIZE),
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
        let res = WindowFrameGroups::find_next_group_and_start_index(
            &arrays,
            &current_row_values,
            current_idx,
            None,
        )
        .unwrap();
        assert_eq!(res, None);
        let res = WindowFrameGroups::find_next_group_and_start_index(
            &arrays,
            &current_row_values,
            current_idx,
            Some(STEP_SIZE),
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<END, PRECEDING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let start = window_frame_groups
                .process::<START, PRECEDING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, 0);
            let end = window_frame_groups
                .process::<END, PRECEDING>(&current_row_values, DELTA, &arrays, NUM_ROWS)
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, FOLLOWING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
        assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<END, FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, DELTA);
        assert_eq!(window_frame_groups.window_frame_end_idx, DELTA);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let start = window_frame_groups
                .process::<START, FOLLOWING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, NUM_ROWS);
            let end = window_frame_groups
                .process::<END, FOLLOWING>(&current_row_values, DELTA, &arrays, NUM_ROWS)
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<END, FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, NUM_ROWS as u64);
        assert!(window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), NUM_ROWS);

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let start = window_frame_groups
                .process::<START, PRECEDING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, 0);
            let end = window_frame_groups
                .process::<END, FOLLOWING>(&current_row_values, DELTA, &arrays, NUM_ROWS)
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        window_frame_groups
            .extend_window_frame_if_necessary::<END, FOLLOWING>(&arrays, DELTA)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 2 * DELTA + 1);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(
            window_frame_groups.group_start_indices.len(),
            2 * DELTA as usize + 1
        );

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let start_idx = if idx < DELTA as usize {
                0
            } else {
                idx - DELTA as usize
            };
            let start = window_frame_groups
                .process::<START, PRECEDING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, start_idx);
            let end_idx = if idx + 1 + DELTA as usize > NUM_ROWS {
                NUM_ROWS
            } else {
                idx + 1 + DELTA as usize
            };
            let end = window_frame_groups
                .process::<END, FOLLOWING>(&current_row_values, DELTA, &arrays, NUM_ROWS)
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 0);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 0);

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();
            let start_idx = if idx < DELTA as usize {
                0
            } else {
                idx - DELTA as usize
            };
            let start = window_frame_groups
                .process::<START, PRECEDING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
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

        let mut window_frame_groups = WindowFrameGroups::default();
        window_frame_groups
            .initialize::<START, PRECEDING>(DELTA, &arrays)
            .unwrap();
        assert_eq!(window_frame_groups.window_frame_start_idx, 0);
        assert_eq!(window_frame_groups.window_frame_end_idx, 1);
        assert!(!window_frame_groups.reached_end);
        assert_eq!(window_frame_groups.group_start_indices.len(), 1);

        for idx in 0..NUM_ROWS {
            let current_row_values = arrays
                .iter()
                .map(|col| ScalarValue::try_from_array(col, idx))
                .collect::<Result<Vec<ScalarValue>>>()
                .unwrap();

            let start = window_frame_groups
                .process::<START, PRECEDING>(
                    &current_row_values,
                    DELTA,
                    &arrays,
                    NUM_ROWS,
                )
                .unwrap();
            assert_eq!(start, idx);
        }
    }
}
