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

//! [`BlockStore`] wrapper specialized for stores whose blocks are `Vec<T>`.

use std::fmt::Debug;
use std::iter;
use std::marker::PhantomData;
use std::ops::{Index, IndexMut};

use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::{Block, BlockStore};

/// Thin wrapper around a [`BlockStore<Vec<T>>`] that adds vector-specific
/// emit semantics on top of the generic block store API.
///
/// Emitting `EmitTo::First` requires vector-specific split semantics, so it
/// lives on this wrapper rather than on [`BlockStore`] itself. The wrapper
/// re-exposes every [`BlockStore`] method by delegation, plus an [`emit`]
/// method implemented purely via [`BlockStore::push_block`] /
/// [`BlockStore::pop_block`] so it works uniformly over flat and blocked
/// storage.
///
/// [`emit`]: VecBlockStore::emit
#[derive(Debug)]
pub struct VecBlockStore<T, S>
where
    T: Clone + Debug,
    S: BlockStore<Vec<T>>,
{
    inner: S,
    _phantom: PhantomData<T>,
}

impl<T, S> VecBlockStore<T, S>
where
    T: Clone + Debug,
    S: BlockStore<Vec<T>>,
{
    /// Wrap an existing block store.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    // ---- BlockStore method delegation ----------------------------------
    pub fn reserve_blocks(&mut self) {
        self.inner.reserve_blocks();
    }

    pub fn resize(&mut self, total_num_groups: usize, default_value: T) {
        self.inner.resize(total_num_groups, default_value);
    }

    pub fn num_blocks(&self) -> usize {
        self.inner.num_blocks()
    }

    pub fn block_size(&self) -> Option<usize> {
        self.inner.block_size()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    // ---- Emit ----------------------------------------------------------

    /// Emit values according to `emit_to`, expressed only in terms of
    /// [`BlockStore::push_block`] and [`BlockStore::pop_block`].
    ///
    /// - [`EmitTo::All`]: drains every block via repeated `pop_block` and
    ///   concatenates the results into a single `Vec<T>`.
    /// - [`EmitTo::First`]`(n)`: pops the first block, splits off the first
    ///   `n` values, and pushes the remainder back. Only meaningful when the
    ///   first block holds at least `n` values (true for flat storage); the
    ///   call returns an internal error otherwise.
    /// - [`EmitTo::NextBlock`]: pops a single block.
    pub fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::All => self.inner.pop_block().ok_or_else(|| {
                internal_datafusion_err!("cannot emit all: block store is empty")
            }),
            EmitTo::First(n) => {
                let mut first = self.inner.pop_block().ok_or_else(|| {
                    internal_datafusion_err!(
                        "cannot emit first {n}: block store is empty"
                    )
                })?;
                if n > first.len() {
                    return internal_err!(
                        "EmitTo::First({}) exceeds the first block length {}",
                        n,
                        first.len()
                    );
                }
                let rest = first.split_off(n);
                self.inner.push_block(rest);
                Ok(first)
            }
            EmitTo::NextBlock => self
                .inner
                .pop_block()
                .ok_or_else(|| internal_datafusion_err!("no more blocks to emit")),
        }
    }
}

impl<T, S> Index<usize> for VecBlockStore<T, S>
where
    T: Clone + Debug,
    S: BlockStore<Vec<T>>,
{
    type Output = Vec<T>;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<T, S> IndexMut<usize> for VecBlockStore<T, S>
where
    T: Clone + Debug,
    S: BlockStore<Vec<T>>,
{
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

/// Most aggregation intermediate results are naturally represented as `Vec<T>`,
/// so we provide a blanket [`Block`] implementation for it. Specialized layouts
/// (e.g. compact representations for non-trivially-sized values) should define
/// their own block type rather than wrap `Vec<T>`.
impl<T: Clone + Debug> Block for Vec<T> {
    type T = T;

    fn new(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T) {
        self.extend(iter::repeat_n(default_value, fill_len));
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr_common::groups_accumulator::EmitTo;

    use crate::aggregate::groups_accumulator::block_store::{
        BlockStore, BlockedBlockStore, FlatBlockStore, VecBlockStore,
    };

    type FlatTestStore = VecBlockStore<u32, FlatBlockStore<Vec<u32>>>;
    type BlockedTestStore = VecBlockStore<u32, BlockedBlockStore<Vec<u32>>>;

    fn flat_store(values: Vec<u32>) -> FlatTestStore {
        let mut store = FlatTestStore::new(FlatBlockStore::new());
        store.resize(values.len(), 0);
        store[0].clone_from(&values);
        store
    }

    fn blocked_store(block_size: usize, blocks: &[&[u32]]) -> BlockedTestStore {
        let mut inner = BlockedBlockStore::new(block_size);
        for block in blocks {
            inner.push_block(block.to_vec());
        }
        VecBlockStore::new(inner)
    }

    // Covers EmitTo::All returning the flat store's single backing block.
    // Example: flat block [1, 2, 3] emits [1, 2, 3] and leaves the store empty.
    #[test]
    fn test_emit_all_returns_flat_block_values() {
        let mut store = flat_store(vec![1, 2, 3]);

        assert_eq!(store.emit(EmitTo::All).unwrap(), vec![1, 2, 3]);
        assert!(!store.is_empty());
        assert_eq!(store.num_blocks(), 1);
    }

    // Covers EmitTo::All returning the next blocked store block.
    // Example: blocks [1, 2] and [3] emit [1, 2], leaving one block active.
    #[test]
    fn test_emit_all_returns_next_blocked_block() {
        let mut store = blocked_store(2, &[&[1, 2], &[3]]);

        assert_eq!(store.emit(EmitTo::All).unwrap(), vec![1, 2]);
        assert_eq!(store.num_blocks(), 1);
    }

    // Covers EmitTo::All returning an error when blocked storage has no blocks.
    // Example: emitting all from a new blocked store returns "cannot emit all".
    #[test]
    fn test_emit_all_returns_error_when_blocked_store_has_no_blocks() {
        let mut store = BlockedTestStore::new(BlockedBlockStore::new(2));

        let error = store.emit(EmitTo::All).unwrap_err().to_string();
        assert!(error.contains("cannot emit all: block store is empty"));
    }

    // Covers EmitTo::First splitting a flat block and preserving the remainder.
    // Example: first 2 from [1, 2, 3, 4] emits [1, 2], then all emits [3, 4].
    #[test]
    fn test_emit_first_splits_flat_block_and_keeps_remainder() {
        let mut store = flat_store(vec![1, 2, 3, 4]);

        assert_eq!(store.emit(EmitTo::First(2)).unwrap(), vec![1, 2]);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.emit(EmitTo::All).unwrap(), vec![3, 4]);
        assert!(!store.is_empty());
        assert_eq!(store.num_blocks(), 1);
    }

    // Covers EmitTo::First(0) pushing the entire flat block back as the remainder.
    // Example: first 0 from [1, 2] emits [], then all emits [1, 2].
    #[test]
    fn test_emit_first_keeps_all_values_when_count_is_zero() {
        let mut store = flat_store(vec![1, 2]);

        assert_eq!(store.emit(EmitTo::First(0)).unwrap(), Vec::<u32>::new());
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.emit(EmitTo::All).unwrap(), vec![1, 2]);
    }

    // Covers EmitTo::First returning an error when the requested count is too large.
    // Example: first 3 from [1, 2] errors because the first block length is 2.
    #[test]
    fn test_emit_first_returns_error_when_count_exceeds_first_block_len() {
        let mut store = flat_store(vec![1, 2]);

        let error = store.emit(EmitTo::First(3)).unwrap_err().to_string();
        assert!(error.contains("EmitTo::First(3) exceeds the first block length 2"));
    }

    // Covers EmitTo::First returning an error when blocked storage has no blocks.
    // Example: first 1 from a new blocked store returns "cannot emit first 1".
    #[test]
    fn test_emit_first_returns_error_when_blocked_store_has_no_blocks() {
        let mut store = BlockedTestStore::new(BlockedBlockStore::new(2));

        let error = store.emit(EmitTo::First(1)).unwrap_err().to_string();
        assert!(error.contains("cannot emit first 1: block store is empty"));
    }

    // Covers EmitTo::NextBlock draining blocked storage one block at a time.
    // Example: blocks [1, 2] and [3, 4] emit [1, 2], then [3, 4], then error.
    #[test]
    fn test_emit_next_block_drains_blocked_blocks_in_order() {
        let mut store = blocked_store(2, &[&[1, 2], &[3, 4]]);

        assert_eq!(store.emit(EmitTo::NextBlock).unwrap(), vec![1, 2]);
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.emit(EmitTo::NextBlock).unwrap(), vec![3, 4]);
        assert!(store.is_empty());
        let error = store.emit(EmitTo::NextBlock).unwrap_err().to_string();
        assert!(error.contains("no more blocks to emit"));
    }

    // Covers EmitTo::NextBlock returning the current single flat block.
    // Example: flat block [9] emits [9], then the next block request emits [].
    #[test]
    fn test_emit_next_block_returns_current_flat_block() {
        let mut store = flat_store(vec![9]);

        assert_eq!(store.emit(EmitTo::NextBlock).unwrap(), vec![9]);
        assert!(!store.is_empty());
        assert_eq!(store.num_blocks(), 1);
        assert_eq!(store.emit(EmitTo::NextBlock).unwrap(), Vec::<u32>::new());
    }
}
