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

    // How many groups be emitted, or None if no data can be emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match self {
            GroupOrdering::None => None,
            GroupOrdering::Partial(partial) => partial.emit_to(),
            GroupOrdering::Full(full) => full.emit_to(),
        }
    }

    /// Updates the state the input is done
    pub fn input_done(&mut self) {
        match self {
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => partial.input_done(),
            GroupOrdering::Full(full) => full.input_done(),
        }
    }

    /// remove the first n groups from the internal state, shifting
    /// all existing indexes down by `n`
    pub fn remove_groups(&mut self, n: usize) {
        match self {
            GroupOrdering::None => {}
            GroupOrdering::Partial(partial) => partial.remove_groups(n),
            GroupOrdering::Full(full) => full.remove_groups(n),
        }
    }

    /// Called when new groups are added in a batch
    ///
    /// * `total_num_groups`: total number of groups (so max
    ///   group_index is total_num_groups - 1).
    ///
    /// * `group_values`: group key values for *each row* in the batch
    ///
    /// * `group_indices`: indices for each row in the batch
    ///
    /// * `hashes`: hash values for each row in the batch
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

    /// Return the size of non-Arrow allocations used by the ordering state, in bytes.
    ///
    /// This includes Vec capacity, ScalarValue allocations, and other
    /// non-Arrow data structures. Arrow Buffer memory should be tracked
    /// separately via [`claim_buffers`].
    ///
    /// [`claim_buffers`]: GroupOrdering::claim_buffers
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + match self {
                GroupOrdering::None => 0,
                GroupOrdering::Partial(partial) => partial.size(),
                GroupOrdering::Full(full) => full.size(),
            }
    }

    /// Claim Arrow buffers with the memory pool for accurate tracking.
    ///
    /// This method should be called to register Arrow Buffer instances
    /// with the pool, enabling automatic deduplication of shared buffers.
    ///
    /// Current implementations do not store Arrow buffers, so this is a no-op.
    pub fn claim_buffers(&self, _pool: &dyn arrow_buffer::MemoryPool) {
        // No-op: Current implementations use Vec and ScalarValue, not Arrow buffers.
        // Memory is tracked via size() method.
    }
}
