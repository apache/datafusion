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

use arrow::array::NullBufferBuilder;
use arrow::buffer::NullBuffer;

/// Builder for an (optional) null mask
///
/// Optimized for avoid creating the bitmask when all values are non-null
#[derive(Debug)]
pub(crate) struct MaybeNullBufferBuilder {
    /// Note this is an Arrow *VALIDITY* buffer (so it is false for nulls, true
    /// for non-nulls)
    nulls: NullBufferBuilder,
}

impl MaybeNullBufferBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            nulls: NullBufferBuilder::new(0),
        }
    }

    /// Return true if the row at index `row` is null
    pub fn is_null(&self, row: usize) -> bool {
        match self.nulls.as_slice() {
            // validity mask means a unset bit is NULL
            Some(_) => !self.nulls.is_valid(row),
            None => false,
        }
    }

    /// Set the nullness of the next row to `is_null`
    ///
    /// If `value` is true, the row is null.
    /// If `value` is false, the row is non null
    pub fn append(&mut self, is_null: bool) {
        self.nulls.append(!is_null)
    }

    pub fn append_n(&mut self, n: usize, is_null: bool) {
        if is_null {
            self.nulls.append_n_nulls(n);
        } else {
            self.nulls.append_n_non_nulls(n);
        }
    }

    /// return the number of heap allocated bytes used by this structure to store boolean values
    pub fn allocated_size(&self) -> usize {
        // NullBufferBuilder builder::allocated_size returns capacity in bits
        self.nulls.allocated_size() / 8
    }

    /// Return a NullBuffer representing the accumulated nulls so far
    pub fn build(mut self) -> Option<NullBuffer> {
        self.nulls.finish()
    }

    /// Returns a NullBuffer representing the first `n` rows accumulated so far
    /// shifting any remaining down by `n`
    pub fn take_n(&mut self, n: usize) -> Option<NullBuffer> {
        // Copy over the values at  n..len-1 values to the start of a
        // new builder and leave it in self
        //
        // TODO: it would be great to use something like `set_bits` from arrow here.
        let mut new_builder = NullBufferBuilder::new(self.nulls.len());
        for i in n..self.nulls.len() {
            new_builder.append(self.nulls.is_valid(i));
        }
        std::mem::swap(&mut new_builder, &mut self.nulls);

        // take only first n values from the original builder
        new_builder.truncate(n);
        new_builder.finish()
    }

    /// Returns true if this builder might have any nulls
    ///
    /// This is guaranteed to be true if there are nulls
    /// but may be true even if there are no nulls
    pub(crate) fn might_have_nulls(&self) -> bool {
        self.nulls.as_slice().is_some()
    }
}
