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

use crate::InputOrderMode;
pub use full::GroupOrderingFull;
pub use partial::GroupOrderingPartial;

/// Ordering information for each group in the hash table
#[derive(Debug)]
pub enum GroupOrdering {
    /// Groups are not ordered
    None,
    /// Groups are ordered by some pre-set of the group keys
    Partial(GroupOrderingPartial),
    /// Groups are entirely contiguous,
    Full(GroupOrderingFull),
}

impl GroupOrdering {
    /// Create a `GroupOrdering` for the specified ordering
    pub fn try_new(mode: &InputOrderMode) -> Result<Self> {
        match mode {
            InputOrderMode::Linear => Ok(GroupOrdering::None),
            InputOrderMode::PartiallySorted(order_indices) => {
                GroupOrderingPartial::try_new(order_indices.clone())
                    .map(GroupOrdering::Partial)
            }
            InputOrderMode::Sorted => Ok(GroupOrdering::Full(GroupOrderingFull::new())),
        }
    }

    /// Returns how many groups can be emitted while respecting the current
    /// ordering guarantees, or `None` if no data can be emitted.
    pub fn emit_to(&self) -> Option<EmitTo> {
        match self {
            GroupOrdering::None => None,
            GroupOrdering::Partial(partial) => partial.emit_to(),
            GroupOrdering::Full(full) => full.emit_to(),
        }
    }

    /// Returns the emit strategy to use under memory pressure (OOM).
    ///
    /// Returns the strategy that must be used when emitting up to `n` groups
    /// while respecting the current ordering guarantees.
    ///
    /// Returns `None` if no data can be emitted.
    pub fn oom_emit_to(&self, n: usize) -> Option<EmitTo> {
        if n == 0 {
            return None;
        }

        match self {
            GroupOrdering::None => Some(EmitTo::First(n)),
            GroupOrdering::Partial(_) | GroupOrdering::Full(_) => {
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
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => partial.input_done(),
            GroupOrdering::Full(full) => full.input_done(),
        }
    }

    /// Removes the first `n` groups from the internal state, shifting all
    /// existing indexes down by `n`.
    pub fn remove_groups(&mut self, n: usize) {
        match self {
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => partial.remove_groups(n),
            GroupOrdering::Full(full) => full.remove_groups(n),
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
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => {
                partial.new_groups(
                    batch_group_values,
                    group_indices,
                    total_num_groups,
                )?;
            }
            GroupOrdering::Full(full) => {
                full.new_groups(total_num_groups);
            }
        };
        Ok(())
    }

    /// Returns the size of memory used by the ordering state, in bytes.
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + match self {
                GroupOrdering::None => 0,
                GroupOrdering::Partial(partial) => partial.size(),
                GroupOrdering::Full(full) => full.size(),
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::Int32Array;

    #[test]
    fn test_oom_emit_to_none_ordering() {
        let group_ordering = GroupOrdering::None;

        assert_eq!(group_ordering.oom_emit_to(0), None);
        assert_eq!(group_ordering.oom_emit_to(5), Some(EmitTo::First(5)));
    }

    /// Creates a partially ordered grouping state with three groups.
    ///
    /// `sort_key_values` controls whether a sort boundary exists in the batch:
    /// distinct values such as `[1, 2, 3]` create boundaries, while repeated
    /// values such as `[1, 1, 1]` do not.
    fn partial_ordering(sort_key_values: Vec<i32>) -> Result<GroupOrdering> {
        let mut group_ordering =
            GroupOrdering::Partial(GroupOrderingPartial::try_new(vec![0])?);

        let batch_group_values: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(sort_key_values)),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ];
        let group_indices = vec![0, 1, 2];

        group_ordering.new_groups(&batch_group_values, &group_indices, 3)?;

        Ok(group_ordering)
    }

    #[test]
    fn test_oom_emit_to_partial_clamps_to_boundary() -> Result<()> {
        let group_ordering = partial_ordering(vec![1, 2, 3])?;

        // Can emit both `1` and `2` groups because we have seen `3`
        assert_eq!(group_ordering.emit_to(), Some(EmitTo::First(2)));
        assert_eq!(group_ordering.oom_emit_to(1), Some(EmitTo::First(1)));
        assert_eq!(group_ordering.oom_emit_to(3), Some(EmitTo::First(2)));

        Ok(())
    }

    #[test]
    fn test_oom_emit_to_partial_without_boundary() -> Result<()> {
        let group_ordering = partial_ordering(vec![1, 1, 1])?;

        // Can't emit the last `1` group as it may have more values
        assert_eq!(group_ordering.emit_to(), None);
        assert_eq!(group_ordering.oom_emit_to(3), None);

        Ok(())
    }
}
