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

//! Structures used to hold window function state (for implementing WindowUDFs)

use std::{collections::VecDeque, ops::Range, sync::Arc};

use crate::{WindowFrame, WindowFrameBound, WindowFrameUnits};

use arrow::{
    array::ArrayRef,
    compute::{concat, concat_batches, SortOptions},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::{
    internal_err,
    utils::{compare_rows, get_row_at_idx, search_in_slice},
    DataFusionError, Result, ScalarValue,
};

/// Holds the state of evaluating a window function
#[derive(Debug)]
pub struct WindowAggState {
    /// The range that we calculate the window function
    pub window_frame_range: Range<usize>,
    pub window_frame_ctx: Option<WindowFrameContext>,
    /// The index of the last row that its result is calculated inside the partition record batch buffer.
    pub last_calculated_index: usize,
    /// The offset of the deleted row number
    pub offset_pruned_rows: usize,
    /// Stores the results calculated by window frame
    pub out_col: ArrayRef,
    /// Keeps track of how many rows should be generated to be in sync with input record_batch.
    // (For each row in the input record batch we need to generate a window result).
    pub n_row_result_missing: usize,
    /// Flag indicating whether we have received all data for this partition
    pub is_end: bool,
}

impl WindowAggState {
    pub fn prune_state(&mut self, n_prune: usize) {
        self.window_frame_range = Range {
            start: self.window_frame_range.start - n_prune,
            end: self.window_frame_range.end - n_prune,
        };
        self.last_calculated_index -= n_prune;
        self.offset_pruned_rows += n_prune;

        match self.window_frame_ctx.as_mut() {
            // Rows have no state do nothing
            Some(WindowFrameContext::Rows(_)) => {}
            Some(WindowFrameContext::Range { .. }) => {}
            Some(WindowFrameContext::Groups { state, .. }) => {
                let mut n_group_to_del = 0;
                for (_, end_idx) in &state.group_end_indices {
                    if n_prune < *end_idx {
                        break;
                    }
                    n_group_to_del += 1;
                }
                state.group_end_indices.drain(0..n_group_to_del);
                state
                    .group_end_indices
                    .iter_mut()
                    .for_each(|(_, start_idx)| *start_idx -= n_prune);
                state.current_group_idx -= n_group_to_del;
            }
            None => {}
        };
    }

    pub fn update(
        &mut self,
        out_col: &ArrayRef,
        partition_batch_state: &PartitionBatchState,
    ) -> Result<()> {
        self.last_calculated_index += out_col.len();
        self.out_col = concat(&[&self.out_col, &out_col])?;
        self.n_row_result_missing =
            partition_batch_state.record_batch.num_rows() - self.last_calculated_index;
        self.is_end = partition_batch_state.is_end;
        Ok(())
    }

    pub fn new(out_type: &DataType) -> Result<Self> {
        let empty_out_col = ScalarValue::try_from(out_type)?.to_array_of_size(0)?;
        Ok(Self {
            window_frame_range: Range { start: 0, end: 0 },
            window_frame_ctx: None,
            last_calculated_index: 0,
            offset_pruned_rows: 0,
            out_col: empty_out_col,
            n_row_result_missing: 0,
            is_end: false,
        })
    }
}

/// This object stores the window frame state for use in incremental calculations.
#[derive(Debug)]
pub enum WindowFrameContext {
    /// ROWS frames are inherently stateless.
    Rows(Arc<WindowFrame>),
    /// RANGE frames are stateful, they store indices specifying where the
    /// previous search left off. This amortizes the overall cost to O(n)
    /// where n denotes the row count.
    Range {
        window_frame: Arc<WindowFrame>,
        state: WindowFrameStateRange,
    },
    /// GROUPS frames are stateful, they store group boundaries and indices
    /// specifying where the previous search left off. This amortizes the
    /// overall cost to O(n) where n denotes the row count.
    Groups {
        window_frame: Arc<WindowFrame>,
        state: WindowFrameStateGroups,
    },
}

impl WindowFrameContext {
    /// Create a new state object for the given window frame.
    pub fn new(window_frame: Arc<WindowFrame>, sort_options: Vec<SortOptions>) -> Self {
        match window_frame.units {
            WindowFrameUnits::Rows => WindowFrameContext::Rows(window_frame),
            WindowFrameUnits::Range => WindowFrameContext::Range {
                window_frame,
                state: WindowFrameStateRange::new(sort_options),
            },
            WindowFrameUnits::Groups => WindowFrameContext::Groups {
                window_frame,
                state: WindowFrameStateGroups::default(),
            },
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    pub fn calculate_range(
        &mut self,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        match self {
            WindowFrameContext::Rows(window_frame) => {
                Self::calculate_range_rows(window_frame, length, idx)
            }
            // Sort options is used in RANGE mode calculations because the
            // ordering or position of NULLs impact range calculations and
            // comparison of rows.
            WindowFrameContext::Range {
                window_frame,
                ref mut state,
            } => state.calculate_range(
                window_frame,
                last_range,
                range_columns,
                length,
                idx,
            ),
            // Sort options is not used in GROUPS mode calculations as the
            // inequality of two rows indicates a group change, and ordering
            // or position of NULLs do not impact inequality.
            WindowFrameContext::Groups {
                window_frame,
                ref mut state,
            } => state.calculate_range(window_frame, range_columns, length, idx),
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
                return internal_err!(
                    "Frame start cannot be UNBOUNDED FOLLOWING '{window_frame:?}'"
                )
            }
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                std::cmp::min(idx + n as usize, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return internal_err!("Rows should be Uint")
            }
        };
        let end = match window_frame.end_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => {
                return internal_err!(
                    "Frame end cannot be UNBOUNDED PRECEDING '{window_frame:?}'"
                )
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
                std::cmp::min(idx + n as usize + 1, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_) | WindowFrameBound::Following(_) => {
                return internal_err!("Rows should be Uint")
            }
        };
        Ok(Range { start, end })
    }
}

/// State for each unique partition determined according to PARTITION BY column(s)
#[derive(Debug)]
pub struct PartitionBatchState {
    /// The record batch belonging to current partition
    pub record_batch: RecordBatch,
    /// The record batch that contains the most recent row at the input.
    /// Please note that this batch doesn't necessarily have the same partitioning
    /// with `record_batch`. Keeping track of this batch enables us to prune
    /// `record_batch` when cardinality of the partition is sparse.
    pub most_recent_row: Option<RecordBatch>,
    /// Flag indicating whether we have received all data for this partition
    pub is_end: bool,
    /// Number of rows emitted for each partition
    pub n_out_row: usize,
}

impl PartitionBatchState {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            record_batch: RecordBatch::new_empty(schema),
            most_recent_row: None,
            is_end: false,
            n_out_row: 0,
        }
    }

    pub fn extend(&mut self, batch: &RecordBatch) -> Result<()> {
        self.record_batch =
            concat_batches(&self.record_batch.schema(), [&self.record_batch, batch])?;
        Ok(())
    }

    pub fn set_most_recent_row(&mut self, batch: RecordBatch) {
        // It is enough for the batch to contain only a single row (the rest
        // are not necessary).
        self.most_recent_row = Some(batch);
    }
}

/// This structure encapsulates all the state information we require as we scan
/// ranges of data while processing RANGE frames.
/// Attribute `sort_options` stores the column ordering specified by the ORDER
/// BY clause. This information is used to calculate the range.
#[derive(Debug, Default)]
pub struct WindowFrameStateRange {
    sort_options: Vec<SortOptions>,
}

impl WindowFrameStateRange {
    /// Create a new object to store the search state.
    fn new(sort_options: Vec<SortOptions>) -> Self {
        Self { sort_options }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    // Argument `last_range` stores the resulting indices from the previous search. Since the indices only
    // advance forward, we start from `last_range` subsequently. Thus, the overall
    // time complexity of linear search amortizes to O(n) where n denotes the total
    // row count.
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        last_range: &Range<usize>,
        range_columns: &[ArrayRef],
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        last_range,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<true, true>(
                range_columns,
                last_range,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    last_range,
                    idx,
                    Some(n),
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    last_range,
                    idx,
                    Some(n),
                    length,
                )?,
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<false, false>(
                range_columns,
                last_range,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        last_range,
                        idx,
                        Some(n),
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
        last_range: &Range<usize>,
        idx: usize,
        delta: Option<&ScalarValue>,
        length: usize,
    ) -> Result<usize> {
        let current_row_values = get_row_at_idx(range_columns, idx)?;
        let end_range = if let Some(delta) = delta {
            let is_descending: bool = self
                .sort_options
                .first()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Sort options unexpectedly absent in a window frame".to_string(),
                    )
                })?
                .descending;

            current_row_values
                .iter()
                .map(|value| {
                    if value.is_null() {
                        return Ok(value.clone());
                    }
                    if SEARCH_SIDE == is_descending {
                        // TODO: Handle positive overflows.
                        value.add(delta)
                    } else if value.is_unsigned() && value < delta {
                        // NOTE: This gets a polymorphic zero without having long coercion code for ScalarValue.
                        //       If we decide to implement a "default" construction mechanism for ScalarValue,
                        //       change the following statement to use that.
                        value.sub(value)
                    } else {
                        // TODO: Handle negative overflows.
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
            let cmp = compare_rows(current, target, &self.sort_options)?;
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

/// This structure encapsulates all the state information we require as we
/// scan groups of data while processing window frames.
#[derive(Debug, Default)]
pub struct WindowFrameStateGroups {
    /// A tuple containing group values and the row index where the group ends.
    /// Example: [[1, 1], [1, 1], [2, 1], [2, 1], ...] would correspond to
    ///          [([1, 1], 2), ([2, 1], 4), ...].
    pub group_end_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    /// The group index to which the row index belongs.
    pub current_group_idx: usize,
}

impl WindowFrameStateGroups {
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<true, true>(
                range_columns,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<false, false>(
                range_columns,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively). Generic argument `SEARCH_SIDE` determines
    /// the sign of `delta` (where true/false represents negative/positive respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        idx: usize,
        delta: Option<&ScalarValue>,
        length: usize,
    ) -> Result<usize> {
        let delta = if let Some(delta) = delta {
            if let ScalarValue::UInt64(Some(value)) = delta {
                *value as usize
            } else {
                return internal_err!(
                    "Unexpectedly got a non-UInt64 value in a GROUPS mode window frame"
                );
            }
        } else {
            0
        };
        let mut group_start = 0;
        let last_group = self.group_end_indices.back_mut();
        if let Some((group_row, group_end)) = last_group {
            if *group_end < length {
                let new_group_row = get_row_at_idx(range_columns, *group_end)?;
                // If last/current group keys are the same, we extend the last group:
                if new_group_row.eq(group_row) {
                    // Update the end boundary of the group (search right boundary):
                    *group_end = search_in_slice(
                        range_columns,
                        group_row,
                        check_equality,
                        *group_end,
                        length,
                    )?;
                }
            }
            // Start searching from the last group boundary:
            group_start = *group_end;
        }

        // Advance groups until `idx` is inside a group:
        while idx >= group_start {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_end_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Update the group index `idx` belongs to:
        while self.current_group_idx < self.group_end_indices.len()
            && idx >= self.group_end_indices[self.current_group_idx].1
        {
            self.current_group_idx += 1;
        }

        // Find the group index of the frame boundary:
        let group_idx = if SEARCH_SIDE {
            if self.current_group_idx > delta {
                self.current_group_idx - delta
            } else {
                0
            }
        } else {
            self.current_group_idx + delta
        };

        // Extend `group_start_indices` until it includes at least `group_idx`:
        while self.group_end_indices.len() <= group_idx && group_start < length {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_end_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Calculate index of the group boundary:
        Ok(match (SIDE, SEARCH_SIDE) {
            // Window frame start:
            (true, _) => {
                let group_idx = std::cmp::min(group_idx, self.group_end_indices.len());
                if group_idx > 0 {
                    // Normally, start at the boundary of the previous group.
                    self.group_end_indices[group_idx - 1].1
                } else {
                    // If previous group is out of the table, start at zero.
                    0
                }
            }
            // Window frame end, PRECEDING n
            (false, true) => {
                if self.current_group_idx >= delta {
                    let group_idx = self.current_group_idx - delta;
                    self.group_end_indices[group_idx].1
                } else {
                    // Group is out of the table, therefore end at zero.
                    0
                }
            }
            // Window frame end, FOLLOWING n
            (false, false) => {
                let group_idx = std::cmp::min(
                    self.current_group_idx + delta,
                    self.group_end_indices.len() - 1,
                );
                self.group_end_indices[group_idx].1
            }
        })
    }
}

fn check_equality(current: &[ScalarValue], target: &[ScalarValue]) -> Result<bool> {
    Ok(current == target)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Float64Array;

    fn get_test_data() -> (Vec<ArrayRef>, Vec<SortOptions>) {
        let range_columns: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            5.0, 7.0, 8.0, 8.0, 9., 10., 10., 10., 11.,
        ]))];
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: false,
        }];

        (range_columns, sort_options)
    }

    fn assert_expected(
        expected_results: Vec<(Range<usize>, usize)>,
        window_frame: &Arc<WindowFrame>,
    ) -> Result<()> {
        let mut window_frame_groups = WindowFrameStateGroups::default();
        let (range_columns, _) = get_test_data();
        let n_row = range_columns[0].len();
        for (idx, (expected_range, expected_group_idx)) in
            expected_results.into_iter().enumerate()
        {
            let range = window_frame_groups.calculate_range(
                window_frame,
                &range_columns,
                n_row,
                idx,
            )?;
            assert_eq!(range, expected_range);
            assert_eq!(window_frame_groups.current_group_idx, expected_group_idx);
        }
        Ok(())
    }

    #[test]
    fn test_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
        ));
        let expected_results = vec![
            (Range { start: 0, end: 2 }, 0),
            (Range { start: 0, end: 4 }, 1),
            (Range { start: 1, end: 5 }, 2),
            (Range { start: 1, end: 5 }, 2),
            (Range { start: 2, end: 8 }, 3),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 4, end: 9 }, 4),
            (Range { start: 5, end: 9 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }

    #[test]
    fn test_window_frame_group_boundaries_both_following() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
        ));
        let expected_results = vec![
            (Range::<usize> { start: 1, end: 4 }, 0),
            (Range::<usize> { start: 2, end: 5 }, 1),
            (Range::<usize> { start: 4, end: 8 }, 2),
            (Range::<usize> { start: 4, end: 8 }, 2),
            (Range::<usize> { start: 5, end: 9 }, 3),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 8, end: 9 }, 4),
            (Range::<usize> { start: 9, end: 9 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }

    #[test]
    fn test_window_frame_group_boundaries_both_preceding() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        ));
        let expected_results = vec![
            (Range::<usize> { start: 0, end: 0 }, 0),
            (Range::<usize> { start: 0, end: 1 }, 1),
            (Range::<usize> { start: 0, end: 2 }, 2),
            (Range::<usize> { start: 0, end: 2 }, 2),
            (Range::<usize> { start: 1, end: 4 }, 3),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 2, end: 5 }, 4),
            (Range::<usize> { start: 4, end: 8 }, 5),
        ];
        assert_expected(expected_results, &window_frame)
    }
}
