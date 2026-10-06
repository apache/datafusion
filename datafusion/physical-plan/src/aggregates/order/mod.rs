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

use std::mem::size_of;

use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_expr::EmitTo;

mod full;
mod partial;

pub use full::GroupCompletionFull;
pub use partial::GroupCompletionPartial;

/// Describes how an aggregate can determine that groups are complete.
///
/// Input ordering is one way to establish a group-completion mode, but the
/// execution machinery only needs to know when it can safely emit completed
/// groups. This mode does not describe the sort order of the input or output.
///
/// For example, when grouping by `key`, both inputs have fully contiguous
/// groups within the input partition:
///
/// ```text
/// sorted:     A A B B C C
/// not sorted: C C A A B B
/// ```
///
/// In both cases, once the key changes, the previous key will not appear again,
/// so its group is complete and can be emitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupCompletionMode {
    /// No group can be known complete before the input ends.
    None,
    /// Rows with the same values at these grouping-expression indices form one
    /// contiguous range. When those values change, every group in the previous
    /// range is complete and can be emitted.
    ///
    /// For example, with `GROUP BY (a, b)`, `Partial(vec![0])` means all rows
    /// for each value of `a` are contiguous, while an `(a, b)` tuple may recur
    /// within that range.
    Partial(Vec<usize>),
    /// Rows with the same complete grouping tuple form one contiguous range.
    /// When the tuple changes, the previous group can be emitted.
    Full,
}

/// Tracks when groups in the hash table are complete and can be emitted.
#[derive(Debug)]
pub enum GroupCompletion {
    /// No group can be known complete before the input ends.
    None,
    /// Rows are contiguous for a subset of the grouping keys.
    /// When those key values change, all groups in the previous run
    /// are complete and can be emitted.
    Partial(GroupCompletionPartial),
    /// Rows are contiguous for the complete grouping tuple.
    /// When the tuple changes, the previous group can be emitted.
    Full(GroupCompletionFull),
}

impl GroupCompletion {
    /// Create a `GroupCompletion` for the specified group-completion mode.
    pub fn try_new(mode: &GroupCompletionMode) -> Result<Self> {
        match mode {
            GroupCompletionMode::None => Ok(GroupCompletion::None),
            GroupCompletionMode::Partial(grouping_indices) => {
                GroupCompletionPartial::try_new(grouping_indices.clone())
                    .map(GroupCompletion::Partial)
            }
            GroupCompletionMode::Full => {
                Ok(GroupCompletion::Full(GroupCompletionFull::new()))
            }
        }
    }

    /// Returns how many completed groups can be emitted, or `None` if no data
    /// can be emitted.
    pub fn emit_to(&self) -> Option<EmitTo> {
        match self {
            GroupCompletion::None => None,
            GroupCompletion::Partial(partial) => partial.emit_to(),
            GroupCompletion::Full(full) => full.emit_to(),
        }
    }

    /// Returns the emit strategy to use under memory pressure (OOM).
    ///
    /// Returns the strategy that must be used when emitting up to `n` groups
    /// while respecting the configured group-completion mode.
    ///
    /// Returns `None` if no data can be emitted.
    pub fn oom_emit_to(&self, n: usize) -> Option<EmitTo> {
        if n == 0 {
            return None;
        }

        match self {
            GroupCompletion::None => Some(EmitTo::First(n)),
            GroupCompletion::Partial(_) | GroupCompletion::Full(_) => {
                self.emit_to().map(|emit_to| match emit_to {
                    EmitTo::First(max) => EmitTo::First(n.min(max)),
                    EmitTo::All => EmitTo::First(n),
                })
            }
        }
    }

    /// Updates the state to indicate that the input is complete.
    pub fn input_done(&mut self) {
        match self {
            GroupCompletion::None => {}
            GroupCompletion::Partial(partial) => partial.input_done(),
            GroupCompletion::Full(full) => full.input_done(),
        }
    }

    /// Resets the completion state while preserving the configured mode.
    ///
    /// Clustered partial aggregation uses this after passing intermediate states
    /// downstream, and clustered final aggregation uses it after spilling a run.
    /// In both cases the hash table is empty and can start tracking the next
    /// input batch from a fresh completion state.
    pub fn reset(&mut self) {
        match self {
            GroupCompletion::None => {}
            GroupCompletion::Partial(partial) => partial.reset(),
            GroupCompletion::Full(full) => full.reset(),
        }
    }

    /// Removes the first `n` groups from the internal state, shifting all
    /// existing indexes down by `n`.
    pub fn remove_groups(&mut self, n: usize) {
        match self {
            GroupCompletion::None => {}
            GroupCompletion::Partial(partial) => partial.remove_groups(n),
            GroupCompletion::Full(full) => full.remove_groups(n),
        }
    }

    /// Called when new groups are added in a batch.
    ///
    /// * `batch_group_values`: group key values for each row in the batch
    ///
    /// * `group_indices`: indices for each row in the batch
    ///
    /// * `total_num_groups`: total number of groups (so max
    ///   group_index is total_num_groups - 1).
    pub fn new_groups(
        &mut self,
        batch_group_values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        match self {
            GroupCompletion::None => {}
            GroupCompletion::Partial(partial) => {
                partial.new_groups(
                    batch_group_values,
                    group_indices,
                    total_num_groups,
                )?;
            }
            GroupCompletion::Full(full) => {
                full.new_groups(total_num_groups);
            }
        }
        Ok(())
    }

    /// Returns the size of memory used by the completion state, in bytes.
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + match self {
                GroupCompletion::None => 0,
                GroupCompletion::Partial(partial) => partial.size(),
                GroupCompletion::Full(full) => full.size(),
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::Int32Array;

    #[test]
    fn test_oom_emit_to_none_completion() {
        let group_completion = GroupCompletion::None;

        assert_eq!(group_completion.oom_emit_to(0), None);
        assert_eq!(group_completion.oom_emit_to(5), Some(EmitTo::First(5)));
    }

    /// Creates a partial group-completion tracker with three groups.
    ///
    /// `group_key_values` controls whether a run boundary exists in the batch:
    /// distinct values such as `[1, 2, 3]` create boundaries, while repeated
    /// values such as `[1, 1, 1]` do not.
    fn partial_completion(group_key_values: Vec<i32>) -> Result<GroupCompletion> {
        let mut group_completion =
            GroupCompletion::Partial(GroupCompletionPartial::try_new(vec![0])?);

        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(group_key_values)),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ];
        let group_indices = vec![0, 1, 2];

        group_completion.new_groups(&batch_group_values, &group_indices, 3)?;

        Ok(group_completion)
    }

    #[test]
    fn test_oom_emit_to_partial_clamps_to_boundary() -> Result<()> {
        let group_completion = partial_completion(vec![1, 2, 3])?;

        // Can emit both `1` and `2` groups because we have seen `3`
        assert_eq!(group_completion.emit_to(), Some(EmitTo::First(2)));
        assert_eq!(group_completion.oom_emit_to(1), Some(EmitTo::First(1)));
        assert_eq!(group_completion.oom_emit_to(3), Some(EmitTo::First(2)));

        Ok(())
    }

    #[test]
    fn test_oom_emit_to_partial_without_boundary() -> Result<()> {
        let group_completion = partial_completion(vec![1, 1, 1])?;

        // Can't emit the last `1` group as it may have more values
        assert_eq!(group_completion.emit_to(), None);
        assert_eq!(group_completion.oom_emit_to(3), None);

        Ok(())
    }
}
