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
use std::iter;

use datafusion_common::Result;
use datafusion_expr_common::groups_accumulator::EmitTo;

use crate::aggregate::groups_accumulator::block_store::{Block, BlockStore};

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
