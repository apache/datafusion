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

use crate::aggregate::groups_accumulator::blocks::Block;

pub mod blocked;
pub mod flat;

pub use blocked::BlockedBlockStore;
pub use flat::FlatBlockStore;

/// Storage abstraction for aggregation intermediate result blocks.
///
/// [`BlockStore`] lets flat and blocked group state share the same accumulation
/// flow while using different physical layouts. Implementations should keep
/// block lookup cheap because it is used by per-row accumulator update paths.
pub trait BlockStore<B: Block>:
    Debug + Index<usize, Output = B> + IndexMut<usize>
{
    /// Create a new store.
    ///
    /// `block_size` is `None` for flat storage and `Some(_)` for blocked
    /// storage.
    fn new(block_size: Option<usize>) -> Self;

    /// Ensure the store can hold `total_num_groups` values.
    fn resize<F>(&mut self, total_num_groups: usize, new_block: F, default_value: B::T)
    where
        F: Fn(Option<usize>) -> B;

    /// Return the number of active blocks.
    fn num_blocks(&self) -> usize;

    /// Return true if there are no active blocks.
    fn is_empty(&self) -> bool;

    /// Clear all active blocks and release owned block storage.
    fn clear(&mut self);
}
