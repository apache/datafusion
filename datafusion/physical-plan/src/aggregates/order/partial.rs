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

/// Tracks grouping state when the data is ordered by some subset of
/// the group keys.
///
/// Once the next *sort key* value is seen, never see groups with that
/// sort key again, so we can emit all groups with the previous sort
/// key and earlier.
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
///     ││ 0 ││    │ │  123, "MA"   │  │ │        current_sort      sort_key
///     │└───┘│    │ └──────────────┘  │ │
///     │ ... │    │    ...            │ │      current_sort tracks the
///     │┌───┐│    │ ┌──────────────┐  │ │      smallest group index that had
///     ││ 8 ││    │ │  765, "MA"   │  │ │      the same sort_key as current
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
pub struct GroupOrderingPartial {
    /// State machine
    state: State,

    /// The indexes of the group by columns that form the sort key.
    /// For example if grouping by `id, state` and ordered by `state`
    /// this would be `[1]`.
    order_indices: Vec<usize>,
}

#[derive(Debug, Default, PartialEq)]
enum State {
    /// The ordering was temporarily taken.  `Self::Taken` is left
    /// when state must be temporarily taken to satisfy the borrow
    /// checker. If an error happens before the state can be restored,
    /// the ordering information is lost and execution can not
    /// proceed, but there is no undefined behavior.
    #[default]
    Taken,

    /// Seen no input yet
    Start,

    /// Data is in progress.
    InProgress {
        /// Smallest group index with the sort_key
        current_sort: usize,
        /// The sort key of group_index `current_sort`
        sort_key: Vec<ScalarValue>,
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
            State::InProgress { sort_key, .. } => sort_key
                .iter()
                .map(|scalar_value| scalar_value.size())
                .sum(),
            State::Complete => 0,
        }
    }
}

impl GroupOrderingPartial {
    /// TODO: Remove unnecessary `input_schema` parameter.
    pub fn try_new(order_indices: Vec<usize>) -> Result<Self> {
        debug_assert!(!order_indices.is_empty());
        Ok(Self {
            state: State::Start,
            order_indices,
        })
    }

    /// Select sort keys from the group values
    ///
    /// For example, if group_values had `A, B, C` but the input was
    /// only sorted on `B` and `C` this should return rows for (`B`,
    /// `C`)
    fn compute_sort_keys(&mut self, group_values: &[ArrayRef]) -> Vec<ArrayRef> {
        // Take only the columns that are in the sort key
        self.order_indices
            .iter()
            .map(|&idx| Arc::clone(&group_values[idx]))
            .collect()
    }

    /// How many groups be emitted, or None if no data can be emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => None,
            State::InProgress { current_sort, .. } => {
                // Can not emit if we are still on the first row sort
                // row otherwise we can emit all groups that had earlier sort keys
                //
                if *current_sort == 0 {
                    None
                } else {
                    Some(EmitTo::First(*current_sort))
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
                current_sort,
                current,
                sort_key: _,
            } => {
                // shift indexes down by n
                assert!(*current >= n);
                *current -= n;
                assert!(*current_sort >= n);
                *current_sort -= n;
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

    fn updated_sort_key(
        current_sort: usize,
        sort_key: Option<Vec<ScalarValue>>,
        range_current_sort: usize,
        range_sort_key: Vec<ScalarValue>,
    ) -> Result<(usize, Vec<ScalarValue>)> {
        if let Some(sort_key) = sort_key {
            let sort_options = vec![SortOptions::new(false, false); sort_key.len()];
            let ordering = compare_rows(&sort_key, &range_sort_key, &sort_options)?;
            if ordering == Ordering::Equal {
                return Ok((current_sort, sort_key));
            }
        }

        Ok((range_current_sort, range_sort_key))
    }

    /// Called when new groups are added in a batch. See documentation
    /// on [`super::GroupOrdering::new_groups`]
    pub fn new_groups(
        &mut self,
        batch_group_values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert!(total_num_groups > 0);
        assert!(!batch_group_values.is_empty());

        let max_group_index = total_num_groups - 1;

        let (current_sort, sort_key) = match std::mem::take(&mut self.state) {
            State::Taken => unreachable!("State previously taken"),
            State::Start => (0, None),
            State::InProgress {
                current_sort,
                sort_key,
                ..
            } => (current_sort, Some(sort_key)),
            State::Complete => {
                panic!("Saw new group after the end of input");
            }
        };

        // Select the sort key columns
        let sort_keys = self.compute_sort_keys(batch_group_values);

        // Check if the sort keys indicate a boundary inside the batch
        let ranges = partition(&sort_keys)?.ranges();
        let last_range = ranges.last().unwrap();

        let range_current_sort = group_indices[last_range.start];
        let range_sort_key = get_row_at_idx(&sort_keys, last_range.start)?;

        let (current_sort, sort_key) = if last_range.start == 0 {
            // There was no boundary in the batch. Compare with the previous sort_key (if present)
            // to check if there was a boundary between the current batch and the previous one.
            Self::updated_sort_key(
                current_sort,
                sort_key,
                range_current_sort,
                range_sort_key,
            )?
        } else {
            (range_current_sort, range_sort_key)
        };

        self.state = State::InProgress {
            current_sort,
            current: max_group_index,
            sort_key,
        };

        Ok(())
    }

    /// Return the size of memory allocated by this structure
    pub(crate) fn size(&self) -> usize {
        size_of::<Self>() + self.order_indices.allocated_size() + self.state.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;

    #[test]
    fn test_group_ordering_partial() -> Result<()> {
        // Ordered on column a
        let order_indices = vec![0];
        let mut group_ordering = GroupOrderingPartial::try_new(order_indices)?;

        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![2, 1, 3])),
        ];

        let group_indices = vec![0, 1, 2];
        let total_num_groups = 3;

        group_ordering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;

        assert_eq!(
            group_ordering.state,
            State::InProgress {
                current_sort: 2,
                sort_key: vec![ScalarValue::Int32(Some(3))],
                current: 2
            }
        );

        // push without a boundary
        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![3, 3, 3])),
            Arc::new(Int32Array::from(vec![2, 1, 7])),
        ];
        let group_indices = vec![3, 4, 5];
        let total_num_groups = 6;

        group_ordering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;

        assert_eq!(
            group_ordering.state,
            State::InProgress {
                current_sort: 2,
                sort_key: vec![ScalarValue::Int32(Some(3))],
                current: 5
            }
        );

        // push with only a boundary to previous batch
        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![4, 4, 4])),
            Arc::new(Int32Array::from(vec![1, 1, 1])),
        ];
        let group_indices = vec![6, 7, 8];
        let total_num_groups = 9;

        group_ordering.new_groups(
            &batch_group_values,
            &group_indices,
            total_num_groups,
        )?;
        assert_eq!(
            group_ordering.state,
            State::InProgress {
                current_sort: 6,
                sort_key: vec![ScalarValue::Int32(Some(4))],
                current: 8
            }
        );

        Ok(())
    }
}
