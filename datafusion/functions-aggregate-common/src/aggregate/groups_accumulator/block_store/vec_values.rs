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

//! Vector-backed value blocks for grouped accumulators.

use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::blocked::BlockedBlockStore;
use crate::aggregate::groups_accumulator::block_store::{BlockStore, FlatBlockStore};
use crate::aggregate::groups_accumulator::blocks::Block;

/// Newtype for values stored in grouped accumulators.
///
/// This avoids exposing raw `Vec<T>` as the storage block type while preserving
/// efficient access to the underlying vector in hot paths.
#[derive(Debug)]
pub struct VecValues<T: Clone + Debug>(Vec<T>);

impl<T: Clone + Debug> VecValues<T> {
    /// Create an empty values block with at least the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    /// Return the underlying vector mutably.
    pub fn as_mut_vec(&mut self) -> &mut Vec<T> {
        &mut self.0
    }

    /// Consume this wrapper and return the underlying vector.
    pub fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<T: Clone + Debug> Default for VecValues<T> {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl<T: Clone + Debug> Deref for VecValues<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Clone + Debug> DerefMut for VecValues<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Clone + Debug> Block for VecValues<T> {
    type T = T;

    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T) {
        self.0.fill_default_value(fill_len, default_value);
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

/// [`BlockStore`] extension for stores whose blocks are [`VecValues`].
///
/// Emitting `EmitTo::First` requires vector-specific split/take semantics, so it
/// lives on this extension trait rather than the generic [`BlockStore`] trait.
pub trait VecValuesBlockStore<T>: BlockStore<VecValues<T>>
where
    T: Clone + Debug,
{
    /// Emit values according to `emit_to`.
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>>;
}

impl<T: Clone + Debug> VecValuesBlockStore<T> for FlatBlockStore<VecValues<T>> {
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::All | EmitTo::First(_) => {
                Ok(emit_to.take_needed(self[0].as_mut_vec()))
            }
            EmitTo::NextBlock => {
                internal_err!(
                    "flat value block store does not support emitting next block"
                )
            }
        }
    }
}

impl<T: Clone + Debug> VecValuesBlockStore<T> for BlockedBlockStore<VecValues<T>> {
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::NextBlock => {
                let blk = self
                    .pop_block()
                    .ok_or_else(|| internal_datafusion_err!("no more blocks to emit"))?
                    .into_inner();
                Ok(blk)
            }
            EmitTo::All | EmitTo::First(_) => {
                internal_err!(
                    "blocks value block store does not support emitting all or first"
                )
            }
        }
    }
}
