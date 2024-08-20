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

use datafusion_expr::EmitTo;

/// Tracks grouping state when the data is ordered entirely by its
/// group keys
///
/// When the group values are sorted, as soon as we see group `n+1` we
/// know we will never see any rows for group `n again and thus they
/// can be emitted.
///
/// For example, given `SUM(amt) GROUP BY id` if the input is sorted
/// by `id` as soon as a new `id` value is seen all previous values
/// can be emitted.
///
/// The state is tracked like this:
///
/// ```text
///      ┌─────┐   ┌──────────────────┐
///      │┌───┐│   │ ┌──────────────┐ │         ┏━━━━━━━━━━━━━━┓
///      ││ 0 ││   │ │     123      │ │   ┌─────┃      13      ┃
///      │└───┘│   │ └──────────────┘ │   │     ┗━━━━━━━━━━━━━━┛
///      │ ... │   │    ...           │   │
///      │┌───┐│   │ ┌──────────────┐ │   │         current
///      ││12 ││   │ │     234      │ │   │
///      │├───┤│   │ ├──────────────┤ │   │
///      ││12 ││   │ │     234      │ │   │
///      │├───┤│   │ ├──────────────┤ │   │
///      ││13 ││   │ │     456      │◀┼───┘
///      │└───┘│   │ └──────────────┘ │
///      └─────┘   └──────────────────┘
///
///  group indices    group_values        current tracks the most
/// (in group value                          recent group index
///      order)
/// ```
///
/// In this diagram, the current group is `13`, and thus groups
/// `0..12` can be emitted. Note that `13` can not yet be emitted as
/// there may be more values in the next batch with the same group_id.
#[derive(Debug)]
pub struct GroupOrderingFull {
    state: State,
}

#[derive(Debug)]
enum State {
    /// Seen no input yet
    Start,

    /// Data is in progress. `current is the current group for which
    /// values are being generated. Can emit `current` - 1
    InProgress { current: usize },

    /// Seen end of input: all groups can be emitted
    Complete,
}

impl GroupOrderingFull {
    pub fn new() -> Self {
        Self {
            state: State::Start,
        }
    }

    // How many groups be emitted, or None if no data can be emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Start => None,
            State::InProgress { current, .. } => {
                if *current == 0 {
                    // Can not emit if still on the first row
                    None
                } else {
                    // otherwise emit all rows prior to the current group
                    Some(EmitTo::First(*current))
                }
            }
            State::Complete { .. } => Some(EmitTo::All),
        }
    }

    /// remove the first n groups from the internal state, shifting
    /// all existing indexes down by `n`
    pub fn remove_groups(&mut self, n: usize) {
        match &mut self.state {
            State::Start => panic!("invalid state: start"),
            State::InProgress { current } => {
                // shift down by n
                assert!(*current >= n);
                *current -= n;
            }
            State::Complete { .. } => panic!("invalid state: complete"),
        }
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        self.state = State::Complete;
    }

    /// Called when new groups are added in a batch. See documentation
    /// on [`super::GroupOrdering::new_groups`]
    pub fn new_groups(&mut self, total_num_groups: usize) {
        assert_ne!(total_num_groups, 0);

        // Update state
        let max_group_index = total_num_groups - 1;
        self.state = match self.state {
            State::Start => State::InProgress {
                current: max_group_index,
            },
            State::InProgress { current } => {
                // expect to see new group indexes when called again
                assert!(current <= max_group_index, "{current} <= {max_group_index}");
                State::InProgress {
                    current: max_group_index,
                }
            }
            State::Complete { .. } => {
                panic!("Saw new group after input was complete");
            }
        };
    }

    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

impl Default for GroupOrderingFull {
    fn default() -> Self {
        Self::new()
    }
}
