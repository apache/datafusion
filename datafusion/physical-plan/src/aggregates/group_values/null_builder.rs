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

use arrow_buffer::{BooleanBufferBuilder, NullBuffer};

/// Support building up an optional null mask
#[derive(Debug)]
pub enum MaybeNullBufferBuilder {
    ///  seen `row_count` rows but no nulls yet
    NoNulls { row_count: usize },
    /// have at least one null value
    ///
    /// Note this is an Arrow *VALIDITY* buffer (so it is false for nulls, true
    /// for non-nulls)
    Nulls(BooleanBufferBuilder),
}

impl MaybeNullBufferBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::NoNulls { row_count: 0 }
    }

    /// Returns true if this builder is tracking any nulls
    ///
    /// This will return true if at least one null has been set via XXX
    pub fn has_nulls(&self) -> bool {
        matches!(self, Self::Nulls { .. })
    }

    /// Return true if the row at index `row` is null
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::NoNulls { .. } => false,
            // validity mask means a unset bit is NULL
            Self::Nulls(nulls) => !nulls.get_bit(row),
        }
    }

    /// Set the nullness of the next row to `is_null`
    ///
    /// num_values is the current length of the rows being tracked
    ///
    /// If `value` is true, the row is null.
    /// If `value` is false, the row is non null
    pub fn append(&mut self, is_null: bool) {
        match self {
            Self::NoNulls { row_count } if !is_null => {
                *row_count += 1;
            }
            Self::NoNulls { row_count } => {
                // have seen no nulls so far, this is the  first null,
                // need to create the nulls buffer for all currently valid values
                // alloc 2x the need given we push a new but immediately
                let mut nulls = BooleanBufferBuilder::new(*row_count * 2);
                nulls.append_n(*row_count, true);
                nulls.append(false);
                *self = Self::Nulls(nulls);
            }
            Self::Nulls(nulls) => nulls.append(is_null),
        }
    }

    /// return the number of heap allocated bytes used by this structure to store boolean values
    pub fn allocated_size(&self) -> usize {
        match self {
            Self::NoNulls { .. } => 0,
            Self::Nulls(nulls) => nulls.capacity() / 8,
        }
    }

    /// Return a NullBuffer representing the accumulated nulls so far
    pub fn build(self) -> Option<NullBuffer> {
        match self {
            Self::NoNulls { .. } => None,
            Self::Nulls(mut nulls) => Some(NullBuffer::from(nulls.finish())),
        }
    }

    /// Returns a NullBuffer representing the first `n` rows accumulated so far
    /// shifting any remaining down by `n`
    pub fn take_n(&mut self, n: usize) -> Option<NullBuffer> {
        match self {
            Self::NoNulls { row_count } => {
                *row_count -= n;
                None
            }
            Self::Nulls(nulls) => {
                // Copy over the values at  n..len-1 values to the start of a
                // new builder and leave it in self
                //
                // TODO: it would be great to use something like `set_bits` from arrow here.
                let mut new_builder = BooleanBufferBuilder::new(nulls.len());
                for i in n..nulls.len() {
                    new_builder.append(nulls.get_bit(i));
                }
                std::mem::swap(&mut new_builder, nulls);

                // take only first n values from the original builder
                new_builder.truncate(n);
                Some(NullBuffer::from(new_builder.finish()))
            }
        }
    }
}
