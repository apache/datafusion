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

//! Hash-based filter for byte array types (Utf8, Binary, and their variants)

use std::marker::PhantomData;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::ByteArrayType;
use datafusion_common::Result;
use datafusion_common::hash_utils::with_hashes;
use hashbrown::HashTable;

use super::result::{build_in_list_result, handle_dictionary};
use super::static_filter::StaticFilter;

// =============================================================================
// BYTE ACCESS TRAIT
// =============================================================================

/// Trait abstracting byte array access for GenericByteArray types.
pub(crate) trait ByteAccess: 'static {
    type Native: PartialEq + AsRef<[u8]> + ?Sized;

    /// Get a value from the array at the given index (unchecked for performance).
    ///
    /// # Safety
    /// `idx` must be in bounds for the array.
    unsafe fn get_unchecked(arr: &dyn Array, idx: usize) -> &Self::Native;

    /// Get the null buffer from the array, if any.
    fn nulls(arr: &dyn Array) -> Option<&NullBuffer>;
}

/// Marker type for GenericByteArray access (Utf8, LargeUtf8, Binary, LargeBinary).
pub(crate) struct ByteArrayAccess<T: ByteArrayType>(PhantomData<T>);

impl<T> ByteAccess for ByteArrayAccess<T>
where
    T: ByteArrayType + 'static,
    T::Native: PartialEq,
{
    type Native = T::Native;

    #[inline(always)]
    unsafe fn get_unchecked(arr: &dyn Array, idx: usize) -> &Self::Native {
        unsafe { arr.as_bytes::<T>().value_unchecked(idx) }
    }

    #[inline(always)]
    fn nulls(arr: &dyn Array) -> Option<&NullBuffer> {
        arr.as_bytes::<T>().nulls()
    }
}

// =============================================================================
// BYTE FILTER
// =============================================================================

/// Hash-based filter for byte array types (Utf8, Binary, and their variants).
///
/// Uses HashTable with batch hashing via `with_hashes` for SIMD-optimized
/// hash computation. Stores indices into the haystack array for O(1) lookup.
pub(crate) struct ByteFilter<A: ByteAccess> {
    /// The haystack array containing values to match against.
    in_array: ArrayRef,
    /// HashTable storing indices into `in_array` for O(1) lookup.
    table: HashTable<usize>,
    /// Random state for consistent hashing between haystack and needles.
    state: RandomState,
    _phantom: PhantomData<A>,
}

impl<A: ByteAccess> ByteFilter<A> {
    pub(crate) fn try_new(in_array: ArrayRef) -> Result<Self> {
        let state = RandomState::new();
        let mut table = HashTable::new();

        // Build haystack table using batch hashing
        with_hashes([in_array.as_ref()], &state, |hashes| {
            for i in 0..in_array.len() {
                if in_array.is_valid(i) {
                    let hash = hashes[i];

                    // Only insert if not already present (deduplication)
                    // SAFETY: i is in bounds and we checked validity
                    let val: &[u8] =
                        unsafe { A::get_unchecked(in_array.as_ref(), i) }.as_ref();
                    if table
                        .find(hash, |&idx| {
                            let stored: &[u8] =
                                unsafe { A::get_unchecked(in_array.as_ref(), idx) }
                                    .as_ref();
                            stored == val
                        })
                        .is_none()
                    {
                        table.insert_unique(hash, i, |&idx| hashes[idx]);
                    }
                }
            }
            Ok::<_, datafusion_common::DataFusionError>(())
        })?;

        Ok(Self {
            in_array,
            table,
            state,
            _phantom: PhantomData,
        })
    }
}

impl<A: ByteAccess> StaticFilter for ByteFilter<A> {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        let needle_nulls = A::nulls(v);
        let haystack_has_nulls = self.in_array.null_count() > 0;

        // Batch hash all needle values using SIMD-optimized hashing
        with_hashes([v], &self.state, |needle_hashes| {
            Ok(build_in_list_result(
                v.len(),
                needle_nulls,
                haystack_has_nulls,
                negated,
                #[inline(always)]
                |i| {
                    // SAFETY: i is in bounds from build_in_list_result iteration
                    let needle_val: &[u8] = unsafe { A::get_unchecked(v, i) }.as_ref();
                    let hash = needle_hashes[i];
                    // Look up using pre-computed hash, compare via index into haystack
                    self.table
                        .find(hash, |&idx| {
                            let haystack_val: &[u8] =
                                unsafe { A::get_unchecked(self.in_array.as_ref(), idx) }
                                    .as_ref();
                            haystack_val == needle_val
                        })
                        .is_some()
                },
            ))
        })
    }
}
