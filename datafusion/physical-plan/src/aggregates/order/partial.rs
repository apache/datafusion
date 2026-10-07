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

use std::cmp::Ordering;
use std::mem::size_of;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow_ord::partition::partition;
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;

/// Tracks group completion when rows are contiguous for a subset of
/// the group keys.
///
/// Once those key values change, they will not appear again, so all groups
/// in the previous run are complete and can be emitted.
///
/// For example, given `SUM(amt) GROUP BY id, state` if the input is
/// sorted by `state`, when a new value of `state` is seen, all groups
/// with prior values of `state` can be emitted.
///
/// The state is tracked like this:
///
/// ```text
///                                            ┏━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━┓
///     ┌─────┐    ┌───────────────────┐ ┌─────┃        9        ┃ ┃ "MD"  ┃
///     │┌───┐│    │ ┌──────────────┐  │ │     ┗━━━━━━━━━━━━━━━━━┛ ┗━━━━━━━┛
///     ││ 0 ││    │ │  123, "MA"   │  │ │      current_run_start   group_key
///     │└───┘│    │ └──────────────┘  │ │
///     │ ... │    │    ...            │ │      current_run_start tracks the
///     │┌───┐│    │ ┌──────────────┐  │ │      smallest group index that had
///     ││ 8 ││    │ │  765, "MA"   │  │ │      the same group_key as current
///     │├───┤│    │ ├──────────────┤  │ │
///     ││ 9 ││    │ │  923, "MD"   │◀─┼─┘
///     │├───┤│    │ ├──────────────┤  │        ┏━━━━━━━━━━━━━━┓
///     ││10 ││    │ │  345, "MD"   │  │  ┌─────┃      11      ┃
///     │├───┤│    │ ├──────────────┤  │  │     ┗━━━━━━━━━━━━━━┛
///     ││11 ││    │ │  124, "MD"   │◀─┼──┘         current
///     │└───┘│    │ └──────────────┘  │
///     └─────┘    └───────────────────┘
///
///  group indices
/// (in group value  group_values               current tracks the most
///      order)                                    recent group index
/// ```
#[derive(Debug)]
pub struct GroupClusteringPartial {
    /// State machine
    state: State,

    /// The indexes of the group by columns whose values form contiguous runs.
    /// For example if grouping by `id, state` and contiguous on `state`
    /// this would be `[1]`.
    grouping_indices: Vec<usize>,
}

#[derive(Debug, Default, PartialEq)]
enum State {
    /// The state was temporarily taken. `Self::Taken` is left
    /// when state must be temporarily taken to satisfy the borrow
    /// checker. If an error happens before the state can be restored,
    /// the completion information is lost and execution can not
    /// proceed, but there is no undefined behavior.
    #[default]
    Taken,

    /// Seen no input yet
    Start,

    /// Data is in progress.
    InProgress {
        /// Smallest group index in the current run.
        current_run_start: usize,
        /// The key values of the current run.
        group_key: Vec<ScalarValue>,
        /// index of the current group for which values are being
        /// generated
        current: usize,
    },

    /// Seen end of input, all groups can be emitted
    Complete,
}

impl State {
    fn size(&self) -> usize {
        match self {
            State::Taken => 0,
            State::Start => 0,
            State::InProgress { group_key, .. } => group_key
                .iter()
                .map(|scalar_value| scalar_value.size())
                .sum(),
            State::Complete => 0,
        }
    }
}

impl GroupClusteringPartial {
    /// Creates a tracker for runs defined by the specified grouping columns.
    pub fn try_new(grouping_indices: Vec<usize>) -> Result<Self> {
        debug_assert!(!grouping_indices.is_empty());
        Ok(Self {
            state: State::Start,
            grouping_indices,
        })
    }

    /// Select the keys that define contiguous runs from the group values.
    ///
    /// For example, if `group_values` contains `A, B, C` but the input is
    /// contiguous on `(B, C)`, this returns the arrays for `B` and `C`.
    fn compute_group_keys(&mut self, group_values: &[ArrayRef]) -> Vec<ArrayRef> {
        // Take only the columns that define contiguous runs.
        self.grouping_indices
            .iter()
            .map(|&idx| Arc::clone(&group_values[idx]))
            .collect()
    }

    /// How many groups be emitted, or None if no data can be emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => None,
            State::InProgress {
                current_run_start, ..
            } => {
                // The current run is incomplete; only groups from earlier runs can be emitted.
                if *current_run_start == 0 {
                    None
                } else {
                    Some(EmitTo::First(*current_run_start))
                }
            }
            State::Complete => Some(EmitTo::All),
        }
    }

    /// remove the first n groups from the internal state, shifting
    /// all existing indexes down by `n`
    pub fn remove_groups(&mut self, n: usize) {
        match &mut self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => panic!("invalid state: start"),
            State::InProgress {
                current_run_start,
                current,
                group_key: _,
            } => {
                // shift indexes down by n
                assert!(*current >= n);
                *current -= n;
                assert!(*current_run_start >= n);
                *current_run_start -= n;
            }
            State::Complete => panic!("invalid state: complete"),
        }
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        self.state = match self.state {
            State::Taken => unreachable!("State previously taken"),
            _ => State::Complete,
        };
    }

    /// Starts tracking a new input segment with the same contiguous-key
    /// columns.
    pub fn reset(&mut self) {
        self.state = State::Start;
    }

    fn updated_group_key(
        current_run_start: usize,
        group_key: Option<Vec<ScalarValue>>,
        range_current_run_start: usize,
        range_group_key: Vec<ScalarValue>,
    ) -> Result<(usize, Vec<ScalarValue>)> {
        if let Some(group_key) = group_key {
            let sort_options = vec![SortOptions::new(false, false); group_key.len()];
            let ordering = compare_rows(&group_key, &range_group_key, &sort_options)?;
            if ordering == Ordering::Equal {
                return Ok((current_run_start, group_key));
            }
        }

        Ok((range_current_run_start, range_group_key))
    }

    /// Called when new groups are added in a batch. See documentation
    /// on [`super::GroupClustering::new_groups`]
    pub fn new_groups(
        &mut self,
        batch_group_values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert!(total_num_groups > 0);
        assert!(!batch_group_values.is_empty());

        let max_group_index = total_num_groups - 1;

        let (current_run_start, group_key) = match std::mem::take(&mut self.state) {
            State::Taken => unreachable!("State previously taken"),
            State::Start => (0, None),
            State::InProgress {
                current_run_start,
                group_key,
                ..
            } => (current_run_start, Some(group_key)),
            State::Complete => {
                panic!("Saw new group after the end of input");
            }
        };

        // Select the columns that define contiguous runs.
        let group_keys = self.compute_group_keys(batch_group_values);

        // Check if the key values indicate a boundary inside the batch.
        let ranges = partition(&group_keys)?.ranges();
        let last_range = ranges.last().unwrap();

        let range_current_run_start = group_indices[last_range.start];
        let range_group_key = get_row_at_idx(&group_keys, last_range.start)?;

        let (current_run_start, group_key) = if last_range.start == 0 {
            // There was no boundary in the batch. Compare with the previous group_key (if present)
            // to check if there was a boundary between the current batch and the previous one.
            Self::updated_group_key(
                current_run_start,
                group_key,
                range_current_run_start,
                range_group_key,
            )?
        } else {
            (range_current_run_start, range_group_key)
        };

        self.state = State::InProgress {
            current_run_start,
            current: max_group_index,
            group_key,
        };

        Ok(())
    }

    /// Return the size of memory allocated by this structure
    pub(crate) fn size(&self) -> usize {
        size_of::<Self>() + self.grouping_indices.allocated_size() + self.state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;

    #[rstest::rstest]
    #[case::sorted([1, 2, 3, 4])]
    #[case::clustered([3, 1, 4, 2])]
    fn test_group_clustering_partial(#[case] keys: [i32; 4]) -> Result<()> {
        let [first, second, third, fourth] = keys;
        // Contiguous on column a.
        let grouping_indices = vec![0];
        let mut group_clustering = GroupClusteringPartial::try_new(grouping_indices)?;

        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![first, second, third])),
            Arc::new(Int32Array::from(vec![2, 1, 3])),
        ];

        let group_indices = vec![0, 1, 2];
        let total_num_groups = 3;

        group_clustering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;

        assert_eq!(
            group_clustering.state,
            State::InProgress {
                current_run_start: 2,
                group_key: vec![ScalarValue::Int32(Some(third))],
                current: 2
            }
        );
        assert_eq!(group_clustering.emit_to(), Some(EmitTo::First(2)));

        // push without a boundary
        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![third, third, third])),
            Arc::new(Int32Array::from(vec![2, 1, 7])),
        ];
        let group_indices = vec![3, 4, 5];
        let total_num_groups = 6;

        group_clustering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;

        assert_eq!(
            group_clustering.state,
            State::InProgress {
                current_run_start: 2,
                group_key: vec![ScalarValue::Int32(Some(third))],
                current: 5
            }
        );
        assert_eq!(group_clustering.emit_to(), Some(EmitTo::First(2)));

        // push with only a boundary to previous batch
        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![fourth, fourth, fourth])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
        ];
        let group_indices = vec![6, 7, 8];
        let total_num_groups = 9;

        group_clustering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;
        assert_eq!(
            group_clustering.state,
            State::InProgress {
                current_run_start: 6,
                group_key: vec![ScalarValue::Int32(Some(fourth))],
                current: 8
            }
        );
        assert_eq!(group_clustering.emit_to(), Some(EmitTo::First(6)));

        Ok(())
    }
}
