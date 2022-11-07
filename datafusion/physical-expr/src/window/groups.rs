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

//! This module provides utilities for GROUPS mode window frame index calculations.
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

use arrow::array::ArrayRef;
use std::cmp;
use std::collections::VecDeque;

use datafusion_common::bisect::find_bisect_point;
use datafusion_common::{DataFusionError, Result, ScalarValue};

/// This structure encapsulates all the state information we require as we
/// scan groups of data while processing window frames.
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
    fn extend_window_frame_if_necessary<const SEARCH_SIDE: bool>(
        &mut self,
        item_columns: &[ArrayRef],
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
            // TODO: Do we need to check for self.window_frame_end_idx <= current_window_frame_end_idx?
            self.initialize_window_frame_start(item_columns)?;
        }
        while !self.reached_end
            && self.window_frame_end_idx <= current_window_frame_end_idx
        {
            self.advance_one_group::<SEARCH_SIDE>(item_columns)?;
        }
        Ok(())
    }

    fn initialize<const SEARCH_SIDE: bool>(
        &mut self,
        delta: u64,
        item_columns: &[ArrayRef],
    ) -> Result<()> {
        if !SEARCH_SIDE {
            self.window_frame_start_idx = self.current_group_idx + delta;
            self.initialize_window_frame_start(item_columns)
        } else if self.current_group_idx >= delta {
            self.window_frame_start_idx = self.current_group_idx - delta;
            self.initialize_window_frame_start(item_columns)
        } else {
            Ok(())
        }
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
        item_columns: &[ArrayRef],
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

    /// This function is the main public interface of WindowFrameGroups. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `BISECT_SIDE`
    /// supplied as false and true, respectively).
    pub fn process<const BISECT_SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        current_row_values: &[ScalarValue],
        delta: u64,
        item_columns: &[ArrayRef],
        length: usize,
    ) -> Result<usize> {
        if BISECT_SIDE {
            // When we call this function to get the window frame start index, it tries to initialize
            // the internal grouping state if this is not already done before. This initialization takes
            // place only when the window frame start index is greater than or equal to zero. In this
            // case, the current row is stored in group_start_indices, with row values as the group
            // identifier and row index as the start index of the group.
            if !self.initialized() {
                self.initialize::<SEARCH_SIDE>(delta, item_columns)?;
            }
        } else if !self.reached_end {
            // When we call this function to get the window frame end index, it extends the window
            // frame one by one until the current row's window frame end index is reached by finding
            // the next group.
            self.extend_window_frame_if_necessary::<SEARCH_SIDE>(item_columns, delta)?;
        }
        // We keep track of previous row values, so that a group change can be identified.
        // If there is a group change, the window frame is advanced and shifted by one group.
        let group_change = match &self.previous_row_values {
            None => false,
            Some(values) => current_row_values != values,
        };
        if self.previous_row_values.is_none() || group_change {
            self.previous_row_values = Some(current_row_values.to_vec());
        }
        if group_change {
            self.current_group_idx += 1;
            self.advance_one_group::<SEARCH_SIDE>(item_columns)?;
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

    /// This function finds the next group and its start index for a given group and start index.
    /// It utilizes an exponentially growing step size to find the group boundary.
    // TODO: For small group sizes, proceeding one-by-one to find the group change can be more efficient.
    // Statistics about previous group sizes can be used to choose one-by-one vs. exponentially growing,
    // or even to set the base step_size when exponentially growing. We can also create a benchmark
    // implementation to get insights about the crossover point.
    fn find_next_group_and_start_index(
        item_columns: &[ArrayRef],
        current_row_values: &[ScalarValue],
        idx: usize,
    ) -> Result<Option<(Vec<ScalarValue>, usize)>> {
        let mut step_size: usize = 1;
        let data_size: usize = item_columns
            .get(0)
            .ok_or_else(|| {
                DataFusionError::Internal("Column array shouldn't be empty".to_string())
            })?
            .len();
        let mut low = idx;
        let mut high = idx + step_size;
        while high < data_size {
            let val = item_columns
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
            item_columns,
            current_row_values,
            |current, to_compare| Ok(current == to_compare),
            low,
            cmp::min(high, data_size),
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
            .initialize::<PRECEDING>(DELTA, &arrays)
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
            .initialize::<PRECEDING>(DELTA, &arrays)
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
