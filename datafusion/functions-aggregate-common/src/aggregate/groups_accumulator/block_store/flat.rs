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

//! Flat [`BlockStore`] implementation backed by a single block.

use std::fmt::Debug;
use std::ops::{Index, IndexMut};

use datafusion_common::{Result, internal_err};
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::{
    Block, BlockStore, VecBlockStore,
};

/// A flat [`BlockStore`] implementation backed by a single block.
///
/// This newtype is intended for flat group state where `block_id` is always 0.
/// The physical layout is exactly one `B`, avoiding the extra `Vec<B>` lookup
/// needed by blocked storage.
#[derive(Debug)]
pub struct FlatBlockStore<B: Block>(B);

impl<B: Block> FlatBlockStore<B> {
    /// Create a new flat store with an empty default block.
    pub fn new() -> Self {
        Self(B::default())
    }

    /// Return the underlying block.
    pub fn inner(&self) -> &B {
        &self.0
    }

    /// Return the underlying block mutably.
    pub fn inner_mut(&mut self) -> &mut B {
        &mut self.0
    }

    /// Consume the store and return the underlying block.
    pub fn into_inner(self) -> B {
        self.0
    }
}

impl<B: Block> Default for FlatBlockStore<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Block> BlockStore<B> for FlatBlockStore<B> {
    fn allocate_block(&mut self) {}

    fn resize(&mut self, total_num_groups: usize, default_value: B::T) {
        if total_num_groups == 0 {
            return;
        }

        if self.0.is_empty() {
            self.0 = B::new(0);
        }

        let existing_len = self.0.len();
        if existing_len < total_num_groups {
            self.0
                .fill_default_value(total_num_groups - existing_len, default_value);
        }
    }

    fn num_blocks(&self) -> usize {
        usize::from(!self.is_empty())
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn clear(&mut self) {
        self.0 = B::default();
    }
}

impl<B: Block> Index<usize> for FlatBlockStore<B> {
    type Output = B;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        assert_eq!(index, 0, "flat block store only has block 0");
        &self.0
    }
}

impl<B: Block> IndexMut<usize> for FlatBlockStore<B> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert_eq!(index, 0, "flat block store only has block 0");
        &mut self.0
    }
}


impl<T: Clone + Debug> VecBlockStore<T> for FlatBlockStore<Vec<T>> {
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::All | EmitTo::First(_) => Ok(emit_to.take_needed(&mut self[0])),
            EmitTo::NextBlock => {
                internal_err!(
                    "flat value block store does not support emitting next block"
                )
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    type TestBlock = Vec<u32>;

    #[test]
    fn flat_block_store_resizes_single_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        assert!(store.is_empty());
        assert_eq!(store.num_blocks(), 0);

        store.resize(3, 42);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42]);

        store.resize(5, 7);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42, 42, 42, 7, 7]);

        store.clear();
        assert!(store.is_empty());
        assert_eq!(store.num_blocks(), 0);
    }

    #[test]
    fn flat_block_store_index_accesses_inner_block() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.resize(2, 42);

        store[0][1] = 7;
        assert_eq!(store[0], vec![42, 7]);
    }

    #[test]
    fn flat_block_store_allocate_block_is_noop() {
        let mut store = FlatBlockStore::<TestBlock>::new();
        store.allocate_block();
        assert_eq!(store.num_blocks(), 0);

        store.resize(1, 42);
        store.allocate_block();
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store[0], vec![42]);
    }
}
