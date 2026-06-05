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

//! Aggregation intermediate results block abstraction

use std::fmt::Debug;

// Re-export `Blocks` from its new home for backward compatibility.
pub use crate::aggregate::groups_accumulator::block_store::blocked::BlockedBlockStore;

/// The abstraction to represent one aggregation intermediate result block
/// in `blocked approach`, multiple blocks compose a [`Blocks`]
///
/// Many types of aggregation intermediate result exist, and we define an interface
/// to abstract the necessary behaviors of various intermediate result types.
///
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
