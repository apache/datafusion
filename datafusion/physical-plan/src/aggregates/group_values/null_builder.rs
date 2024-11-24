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

use arrow_buffer::{
    bit_util, BooleanBuffer, BooleanBufferBuilder, MutableBuffer, NullBuffer,
};

/// Builder for an (optional) null mask
///
/// Optimized for avoid creating the bitmask when all values are non-null
#[derive(Debug)]
pub(crate) enum MaybeNullBufferBuilder {
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

    pub fn new_from_buffer(buffer: MutableBuffer, len: usize) -> Self {
        let builder = BooleanBufferBuilder::new_from_buffer(buffer, len);
        Self::Nulls(builder)
    }

    /// Return true if the row at index `row` is null
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::NoNulls { .. } => false,
            // validity mask means a unset bit is NULL
            Self::Nulls(builder) => !builder.get_bit(row),
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
            Self::NoNulls { row_count } if is_null => {
                // have seen no nulls so far, this is the  first null,
                // need to create the nulls buffer for all currently valid values
                // alloc 2x the need given we push a new but immediately
                let mut nulls = BooleanBufferBuilder::new(*row_count * 2);
                nulls.append_n(*row_count, true);
                nulls.append(false);
                *self = Self::Nulls(nulls);
            }
            Self::NoNulls { row_count } => {
                *row_count += 1;
            }
            Self::Nulls(builder) => builder.append(!is_null),
        }
    }

    pub fn append_n(&mut self, n: usize, is_null: bool) {
        match self {
            Self::NoNulls { row_count } if is_null => {
                // have seen no nulls so far, this is the  first null,
                // need to create the nulls buffer for all currently valid values
                // alloc 2x the need given we push a new but immediately
                let mut nulls = BooleanBufferBuilder::new(*row_count * 2);
                nulls.append_n(*row_count, true);
                nulls.append_n(n, false);
                *self = Self::Nulls(nulls);
            }
            Self::NoNulls { row_count } => {
                *row_count += n;
            }
            Self::Nulls(builder) => builder.append_n(n, !is_null),
        }
    }

    /// return the number of heap allocated bytes used by this structure to store boolean values
    pub fn allocated_size(&self) -> usize {
        match self {
            Self::NoNulls { .. } => 0,
            // BooleanBufferBuilder builder::capacity returns capacity in bits (not bytes)
            Self::Nulls(builder) => builder.capacity() / 8,
        }
    }

    /// Return a NullBuffer representing the accumulated nulls so far
    pub fn build(self) -> Option<NullBuffer> {
        match self {
            Self::NoNulls { .. } => None,
            Self::Nulls(mut builder) => Some(NullBuffer::from(builder.finish())),
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
            Self::Nulls(builder) => {
                // Copy over the values at  n..len-1 values to the start of a
                // new builder and leave it in self
                //
                // TODO: it would be great to use something like `set_bits` from arrow here.
                let mut new_builder = BooleanBufferBuilder::new(builder.len());
                for i in n..builder.len() {
                    new_builder.append(builder.get_bit(i));
                }
                std::mem::swap(&mut new_builder, builder);

                // take only first n values from the original builder
                new_builder.truncate(n);
                Some(NullBuffer::from(new_builder.finish()))
            }
        }
    }
}

pub fn build_nulls_with_buffer<I>(
    nulls_iter: I,
    nulls_len: usize,
    mut buffer: MutableBuffer,
) -> (Option<NullBuffer>, Option<MutableBuffer>)
where
    I: Iterator<Item = bool>,
{
    // Ensure the buffer big enough, and init to all `false`
    buffer.clear();
    let bytes_len = bit_util::ceil(nulls_len, 8);
    buffer.resize(bytes_len, 0);

    let nulls_slice = buffer.as_slice_mut();
    let mut has_nulls = false;
    nulls_iter.enumerate().for_each(|(idx, is_valid)| {
        if is_valid {
            bit_util::set_bit(nulls_slice, idx);
        } else {
            has_nulls = true;
        }
    });

    if has_nulls {
        let bool_buffer = BooleanBuffer::new(buffer.into(), 0, nulls_len);
        let null_buffer = NullBuffer::new(bool_buffer);

        (Some(null_buffer), None)
    } else {
        (None, Some(buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use arrow_buffer::{bit_util, MutableBuffer};
    use rand::{thread_rng, Rng};

    use crate::aggregates::group_values::null_builder::build_nulls_with_buffer;

    #[test]
    fn test_build_nulls_with_buffer() {
        // Situations will be covered:
        //   1. `all valid`
        //   2. `mixed`
        //   3. `all valid` + `reuse buffer`
        //   4. `mixed` + `reuse buffer`

        let mut rng = thread_rng();
        let len0 = 64;
        let len1 = 256;

        let all_valid0 = iter::repeat(true).take(len0).collect::<Vec<_>>();
        let mixed0 = (0..len0)
            .into_iter()
            .map(|_| {
                let rnd = rng.gen_range(0.0..=1.0);
                rnd < 0.5
            })
            .collect::<Vec<_>>();

        let all_valid1 = iter::repeat(true).take(len1).collect::<Vec<_>>();
        let mixed1 = (0..len1)
            .into_iter()
            .map(|_| {
                let rnd = rng.gen_range(0.0..=1.0);
                rnd < 0.5
            })
            .collect::<Vec<_>>();

        let buffer_all_valid = MutableBuffer::new(0);
        let buffer_mixed = MutableBuffer::new(0);

        // 1. `all valid`, should return `none null buffer` and `non-none reuse buffer`
        let (null_buffer_opt, reuse_buffer_opt) =
            build_nulls_with_buffer(all_valid0.iter().copied(), len0, buffer_all_valid);
        assert!(null_buffer_opt.is_none());
        assert!(reuse_buffer_opt.is_some());

        let buffer_all_valid = reuse_buffer_opt.unwrap();
        assert_eq!(buffer_all_valid.len(), bit_util::ceil(len0, 8));

        // 2. `mixed`, should return `non-none null buffer` and `none reuse buffer`
        let (null_buffer_opt, reuse_buffer_opt) =
            build_nulls_with_buffer(mixed0.iter().copied(), len0, buffer_mixed);
        assert!(null_buffer_opt.is_some());
        assert!(reuse_buffer_opt.is_none());

        let null_buffer = null_buffer_opt.unwrap();
        null_buffer
            .iter()
            .zip(mixed0.iter().copied())
            .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));
        let buffer_mixed = null_buffer
            .into_inner()
            .into_inner()
            .into_mutable()
            .unwrap();
        assert_eq!(buffer_mixed.len(), bit_util::ceil(len0, 8));

        // 3. `all valid` + `reuse buffer`, should return `none null buffer` and `non-none reuse buffer`
        let (null_buffer_opt, reuse_buffer_opt) =
            build_nulls_with_buffer(all_valid1.iter().copied(), len1, buffer_all_valid);
        assert!(null_buffer_opt.is_none());
        assert!(reuse_buffer_opt.is_some());

        let buffer_all_valid = reuse_buffer_opt.unwrap();
        assert_eq!(buffer_all_valid.len(), bit_util::ceil(len1, 8));

        // 4. `mixed` + `reuse buffer`, should return `non-none null buffer` and `none reuse buffer`
        let (null_buffer_opt, reuse_buffer_opt) =
            build_nulls_with_buffer(mixed1.iter().copied(), len1, buffer_mixed);
        assert!(null_buffer_opt.is_some());
        assert!(reuse_buffer_opt.is_none());

        let null_buffer = null_buffer_opt.unwrap();
        null_buffer
            .iter()
            .zip(mixed1.iter().copied())
            .for_each(|(lhs, rhs)| assert_eq!(lhs, rhs));
        let buffer_mixed = null_buffer
            .into_inner()
            .into_inner()
            .into_mutable()
            .unwrap();
        assert_eq!(buffer_mixed.len(), bit_util::ceil(len1, 8));
    }
}
