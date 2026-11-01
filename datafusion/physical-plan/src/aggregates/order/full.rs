// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::EmitTo;

/// Tracks group completion when rows are contiguous for the complete
/// grouping tuple.
///
/// When groups are contiguous, as soon as we see group `n+1` we
/// know we will never see any rows for group `n` again and thus they
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
pub struct GroupClusteringFull {
    state: State,
}

#[derive(Debug)]
enum State {
    /// Seen no input yet
    Start,

    /// Data is in progress. `current` is the current group for which
    /// values are being generated. Can emit `current` - 1
    InProgress { current: usize },

    /// Seen end of input: all groups can be emitted
    Complete,
}

impl GroupClusteringFull {
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
            State::Complete => Some(EmitTo::All),
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
            State::Complete => panic!("invalid state: complete"),
        }
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        self.state = State::Complete;
    }

    /// Starts tracking a new input segment with contiguous groups.
    pub fn reset(&mut self) {
        self.state = State::Start;
    }

    /// Called when new groups are added in a batch. See documentation
    /// on [`super::GroupClustering::new_groups`]
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
            State::Complete => {
                panic!("Saw new group after input was complete");
            }
        };
    }
}

impl Default for GroupClusteringFull {
    fn default() -> Self {
        Self::new()
    }
}
