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

//! Optimized primitive type filters for InList expressions
//!
//! This module provides high-performance membership testing for Arrow primitive types.

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, downcast_array, downcast_dictionary_array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::take;
use arrow::datatypes::*;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::hash::{Hash, Hasher};

use super::result::{build_in_list_result, handle_dictionary};
use super::static_filter::StaticFilter;

// =============================================================================
// BITMAP FILTERS (O(1) lookup for u8/u16 via bit test)
// =============================================================================

/// Trait for bitmap storage (stack-allocated for u8, heap-allocated for u16).
pub(crate) trait BitmapStorage: Send + Sync {
    fn new_zeroed() -> Self;
    fn set_bit(&mut self, index: usize);
    fn get_bit(&self, index: usize) -> bool;
}

impl BitmapStorage for [u64; 4] {
    #[inline]
    fn new_zeroed() -> Self {
        [0u64; 4]
    }
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self[index / 64] |= 1u64 << (index % 64);
    }
    #[inline(always)]
    fn get_bit(&self, index: usize) -> bool {
        (self[index / 64] >> (index % 64)) & 1 != 0
    }
}

impl BitmapStorage for Box<[u64; 1024]> {
    #[inline]
    fn new_zeroed() -> Self {
        Box::new([0u64; 1024])
    }
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self[index / 64] |= 1u64 << (index % 64);
    }
    #[inline(always)]
    fn get_bit(&self, index: usize) -> bool {
        (self[index / 64] >> (index % 64)) & 1 != 0
    }
}

/// Configuration trait for bitmap filters.
pub(crate) trait BitmapFilterConfig: Send + Sync + 'static {
    type Native: ArrowNativeType + Copy + Send + Sync;
    type ArrowType: ArrowPrimitiveType<Native = Self::Native>;
    type Storage: BitmapStorage;

    fn to_index(v: Self::Native) -> usize;
}

/// Config for u8 bitmap (256 bits = 32 bytes, fits in cache line).
pub(crate) enum U8Config {}
impl BitmapFilterConfig for U8Config {
    type Native = u8;
    type ArrowType = UInt8Type;
    type Storage = [u64; 4];

    #[inline(always)]
    fn to_index(v: u8) -> usize {
        v as usize
    }
}

/// Config for u16 bitmap (65536 bits = 8 KB, fits in L1 cache).
pub(crate) enum U16Config {}
impl BitmapFilterConfig for U16Config {
    type Native = u16;
    type ArrowType = UInt16Type;
    type Storage = Box<[u64; 1024]>;

    #[inline(always)]
    fn to_index(v: u16) -> usize {
        v as usize
    }
}

/// Bitmap filter for O(1) set membership via single bit test.
///
/// For small integer types (u8/u16), bitmap lookup outperforms both branchless
/// and hashed approaches at all list sizes.
pub(crate) struct BitmapFilter<C: BitmapFilterConfig> {
    null_count: usize,
    bits: C::Storage,
}

impl<C: BitmapFilterConfig> BitmapFilter<C> {
    pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let prim_array =
            in_array.as_primitive_opt::<C::ArrowType>().ok_or_else(|| {
                exec_datafusion_err!("BitmapFilter: expected primitive array")
            })?;
        let mut bits = C::Storage::new_zeroed();
        for v in prim_array.iter().flatten() {
            bits.set_bit(C::to_index(v));
        }
        Ok(Self {
            null_count: prim_array.null_count(),
            bits,
        })
    }

    #[inline(always)]
    fn check(&self, needle: C::Native) -> bool {
        self.bits.get_bit(C::to_index(needle))
    }

    /// Check membership using a raw values slice (zero-copy path for type reinterpretation).
    #[inline]
    pub(crate) fn contains_slice(
        &self,
        values: &[C::Native],
        nulls: Option<&NullBuffer>,
        negated: bool,
    ) -> BooleanArray {
        build_in_list_result(values.len(), nulls, self.null_count > 0, negated, |i| {
            self.check(unsafe { *values.get_unchecked(i) })
        })
    }
}

impl<C: BitmapFilterConfig> StaticFilter for BitmapFilter<C> {
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<C::ArrowType>().ok_or_else(|| {
            exec_datafusion_err!("BitmapFilter: expected primitive array")
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            #[inline(always)]
            |i| self.check(unsafe { *input_values.get_unchecked(i) }),
        ))
    }
}

// =============================================================================
// LEGACY FILTERS (to be replaced by optimized ones in subsequent commits)
// =============================================================================

/// Wrapper for f32 that implements Hash and Eq using bit comparison.
#[derive(Clone, Copy)]
pub(crate) struct OrderedFloat32(pub(crate) f32);

impl Hash for OrderedFloat32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat32 {}

impl From<f32> for OrderedFloat32 {
    fn from(v: f32) -> Self {
        Self(v)
    }
}

/// Wrapper for f64 that implements Hash and Eq using bit comparison.
#[derive(Clone, Copy)]
pub(crate) struct OrderedFloat64(pub(crate) f64);

impl Hash for OrderedFloat64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat64 {}

impl From<f64> for OrderedFloat64 {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        pub(crate) struct $Name {
            null_count: usize,
            values: HashSet<<$ArrowType as ArrowPrimitiveType>::Native>,
        }

        impl $Name {
            pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(v);
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let haystack_has_nulls = self.null_count > 0;
                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&needle_values[i])
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&needle_values[i])
                    })
                };

                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => None,
                    (true, false) => needle_nulls.cloned(),
                    (false, true) => {
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        let needle_validity = needle_nulls
                            .map(|n| n.inner().clone())
                            .unwrap_or_else(|| {
                                BooleanBuffer::new_set(needle_values.len())
                            });
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

macro_rules! float_static_filter {
    ($Name:ident, $ArrowType:ty, $OrderedType:ty) => {
        pub(crate) struct $Name {
            null_count: usize,
            values: HashSet<$OrderedType>,
        }

        impl $Name {
            pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(<$OrderedType>::from(v));
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let haystack_has_nulls = self.null_count > 0;
                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                };

                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => None,
                    (true, false) => needle_nulls.cloned(),
                    (false, true) => {
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        let needle_validity = needle_nulls
                            .map(|n| n.inner().clone())
                            .unwrap_or_else(|| {
                                BooleanBuffer::new_set(needle_values.len())
                            });
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

float_static_filter!(Float32StaticFilter, Float32Type, OrderedFloat32);
float_static_filter!(Float64StaticFilter, Float64Type, OrderedFloat64);
