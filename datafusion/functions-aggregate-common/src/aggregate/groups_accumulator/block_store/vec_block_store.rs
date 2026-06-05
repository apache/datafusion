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

//! [`BlockStore`] extension for stores whose blocks are `Vec<T>`.

use std::fmt::Debug;

use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::blocked::BlockedBlockStore;
use crate::aggregate::groups_accumulator::block_store::{BlockStore, FlatBlockStore};

/// [`BlockStore`] extension for stores whose blocks are `Vec<T>`.
///
/// Emitting `EmitTo::First` requires vector-specific split/take semantics, so it
/// lives on this extension trait rather than the generic [`BlockStore`] trait.
pub trait VecBlockStore<T>: BlockStore<Vec<T>>
where
    T: Clone + Debug,
{
    /// Emit values according to `emit_to`.
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>>;
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

impl<T: Clone + Debug> VecBlockStore<T> for BlockedBlockStore<Vec<T>> {
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<T>> {
        match emit_to {
            EmitTo::NextBlock => self
                .pop_block()
                .ok_or_else(|| internal_datafusion_err!("no more blocks to emit")),
            EmitTo::All | EmitTo::First(_) => {
                internal_err!(
                    "blocks value block store does not support emitting all or first"
                )
            }
        }
    }
}
