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

//! Storage abstraction for aggregation intermediate result blocks.

use std::fmt::Debug;
use std::ops::{Index, IndexMut};

pub mod blocked;
pub mod flat;
pub mod vec_block_store;

pub use blocked::BlockedBlockStore;
pub use flat::FlatBlockStore;
pub use vec_block_store::VecBlockStore;

/// Storage abstraction for aggregation intermediate result blocks.
///
/// [`BlockStore`] lets flat and blocked group state share the same accumulation
/// flow while using different physical layouts. Implementations should keep
/// block lookup cheap because it is used by per-row accumulator update paths.
pub trait BlockStore<B: Block>:
    Debug + Index<usize, Output = B> + IndexMut<usize>
{
    /// Append or replace a block in the store.
    ///
    /// Blocked storage appends `block` to the accumulated block list. Flat
    /// storage replaces its single backing block with `block`.
    fn push_block(&mut self, block: B);

    /// Remove and return the next block to emit.
    ///
    /// Returns `None` when there are no active blocks left. Blocked storage
    /// pops blocks in insertion order. Flat storage returns its single backing
    /// block once, then becomes empty.
    fn pop_block(&mut self) -> Option<B>;

    /// Reserve block capacity for pushing one value.
    ///
    /// Implementations may allocate a new block only when the current storage
    /// cannot accept another value without growing. This does not change the
    /// logical number of values held by the store.
    fn reserve_blocks(&mut self);

    /// Ensure the store contains slots for `total_num_groups` values.
    ///
    /// Missing slots are appended and filled with `default_value`. Existing
    /// values are preserved, and implementations do not shrink when
    /// `total_num_groups` is less than or equal to the current logical length.
    fn resize(&mut self, total_num_groups: usize, default_value: B::T);

    /// Return the number of active blocks.
    ///
    /// This counts blocks visible to the current phase. During blocked
    /// emission, blocks already returned by [`Self::pop_block`] are excluded.
    fn num_blocks(&self) -> usize;

    /// Return the configured block size for blocked storage.
    ///
    /// Returns `None` for flat storage because it has no configured block size.
    fn block_size(&self) -> Option<usize>;

    /// Clear all active blocks and reset the store to accumulation mode.
    ///
    /// Any in-progress emission state is discarded. After `clear`, the store
    /// behaves like a newly constructed empty store with the same configuration.
    fn clear(&mut self);
}

/// The abstraction to represent one aggregation intermediate result block
/// in `blocked approach`, multiple blocks compose a [`BlockStore`].
///
/// Many types of aggregation intermediate result exist, and we define an interface
/// to abstract the necessary behaviors of various intermediate result types.
pub trait Block: Debug + Default {
    type T: Clone;

    /// Create a new empty block with the given capacity hint.
    ///
    /// `capacity` is `0` for flat storage (no hint) and the configured block
    /// size for blocked storage. Implementations should treat `0` the same as
    /// `Default::default()`.
    fn new(capacity: usize) -> Self;

    /// Fill the block with default value
    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T);

    /// Return the length of the block
    fn len(&self) -> usize;

    /// Return true if the block is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
