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
    pub fn allocate_block(&mut self) {
        self.inner.allocate_block();
    }

    pub fn resize(&mut self, total_num_groups: usize, default_value: T) {
        self.inner.resize(total_num_groups, default_value);
    }

    pub fn num_blocks(&self) -> usize {
        self.inner.num_blocks()
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
