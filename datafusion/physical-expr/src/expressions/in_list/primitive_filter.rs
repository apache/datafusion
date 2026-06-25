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

//! Optimized primitive type filters for InList expressions.
//!
//! This module provides membership tests for Arrow primitive types.

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::*;
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::hash::{Hash, Hasher};

use super::result::build_in_list_result;
use super::static_filter::{StaticFilter, handle_dictionary};

/// Bitmap filter for O(1) set membership via single bit test.
///
/// `UInt8` has only 256 possible values, so the filter stores membership in a
/// 256-bit bitmap instead of using a hash table.
pub(super) struct UInt8BitmapFilter {
    null_count: usize,
    bits: [u64; 4],
}

impl UInt8BitmapFilter {
    pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
        let prim_array = in_array.as_primitive_opt::<UInt8Type>().ok_or_else(|| {
            exec_datafusion_err!("UInt8BitmapFilter: expected UInt8 array")
        })?;
        let mut bits = [0u64; 4];
        let mut set_bit = |v: u8| {
            let index = usize::from(v);
            bits[index / 64] |= 1u64 << (index % 64);
        };

        let values = prim_array.values();
        match prim_array.nulls() {
            None => {
                for &v in values {
                    set_bit(v);
                }
            }
            Some(nulls) => {
                for i in
                    BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                {
                    set_bit(values[i]);
                }
            }
        }
        Ok(Self {
            null_count: prim_array.null_count(),
            bits,
        })
    }

    #[inline(always)]
    fn check(&self, needle: u8) -> bool {
        let index = needle as usize;
        (self.bits[index / 64] >> (index % 64)) & 1 != 0
    }
}

impl StaticFilter for UInt8BitmapFilter {
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let v = v.as_primitive_opt::<UInt8Type>().ok_or_else(|| {
            exec_datafusion_err!("UInt8BitmapFilter: expected UInt8 array")
        })?;
        let input_values = v.values();
        Ok(build_in_list_result(
            v.len(),
            v.nulls(),
            self.null_count > 0,
            negated,
            #[inline(always)]
            |i| {
                // SAFETY: `build_in_list_result` invokes this closure for
                // indices in `0..v.len()`, which matches `input_values.len()`.
                let needle = unsafe { *input_values.get_unchecked(i) };
                self.check(needle)
            },
        ))
    }
}

/// Wrapper for f32 that implements Hash and Eq using bit comparison.
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat32(f32);

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
/// This treats NaN values as equal to each other when they have the same bit pattern.
#[derive(Clone, Copy)]
struct OrderedFloat64(f64);

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

// Macro to generate specialized StaticFilter implementations for primitive types
macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        primitive_static_filter!(
            $Name,
            $ArrowType,
            <$ArrowType as ArrowPrimitiveType>::Native,
            |v| v
        );
    };
    ($Name:ident, $ArrowType:ty, $SetValueType:ty, $to_set_value:expr) => {
        pub(super) struct $Name {
            null_count: usize,
            values: HashSet<$SetValueType>,
        }

        impl $Name {
            pub(super) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array =
                    in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                        exec_datafusion_err!(
                            "Failed to downcast an array to a '{}' array",
                            stringify!($ArrowType)
                        )
                    })?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(($to_set_value)(v));
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                handle_dictionary!(self, v, negated);

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

                // Truth table for `value [NOT] IN (set)` with SQL three-valued logic:
                // ("-" means the value doesn't affect the result)
                //
                // | needle_null | haystack_null | negated | in set? | result |
                // |-------------|---------------|---------|---------|--------|
                // | true        | -             | false   | -       | null   |
                // | true        | -             | true    | -       | null   |
                // | false       | true          | false   | yes     | true   |
                // | false       | true          | false   | no      | null   |
                // | false       | true          | true    | yes     | false  |
                // | false       | true          | true    | no      | null   |
                // | false       | false         | false   | yes     | true   |
                // | false       | false         | false   | no      | false  |
                // | false       | false         | true    | yes     | false  |
                // | false       | false         | true    | no      | true   |

                // Compute the "contains" result using collect_bool (fast batched approach)
                // This ignores nulls - we handle them separately
                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&($to_set_value)(needle_values[i]))
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&($to_set_value)(needle_values[i]))
                    })
                };

                // Compute the null mask
                // Output is null when:
                // 1. needle value is null, OR
                // 2. needle value is not in set AND haystack has nulls
                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => {
                        // No nulls anywhere
                        None
                    }
                    (true, false) => {
                        // Only needle has nulls - just use needle's null mask
                        needle_nulls.cloned()
                    }
                    (false, true) => {
                        // Only haystack has nulls - result is null when value not in set
                        // Valid (not null) when original "in set" is true
                        // For NOT IN: contains_buffer = !original, so validity = !contains_buffer
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        // Both have nulls - combine needle nulls with haystack-induced nulls
                        let needle_validity =
                            needle_nulls.map(|n| n.inner().clone()).unwrap_or_else(
                                || BooleanBuffer::new_set(needle_values.len()),
                            );

                        // Valid when original "in set" is true (see above)
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };

                        // Combined validity: valid only where both are valid
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

// Generate specialized filters for all integer primitive types
primitive_static_filter!(Int8StaticFilter, Int8Type);
primitive_static_filter!(Int16StaticFilter, Int16Type);
primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt16StaticFilter, UInt16Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

// Macro to generate specialized StaticFilter implementations for float types
// Floats require a wrapper type (OrderedFloat*) to implement Hash/Eq due to NaN semantics
macro_rules! float_static_filter {
    ($Name:ident, $ArrowType:ty, $OrderedType:ty) => {
        primitive_static_filter!($Name, $ArrowType, $OrderedType, <$OrderedType>::from);
    };
}

// Generate specialized filters for float types using ordered wrappers
float_static_filter!(Float32StaticFilter, Float32Type, OrderedFloat32);
float_static_filter!(Float64StaticFilter, Float64Type, OrderedFloat64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, Int8Array, UInt8Array};

    fn assert_contains(
        filter: &UInt8BitmapFilter,
        needles: &dyn Array,
        expected: Vec<Option<bool>>,
    ) -> Result<()> {
        assert_eq!(
            filter.contains(needles, false)?,
            BooleanArray::from(expected)
        );
        Ok(())
    }

    #[test]
    fn bitmap_filter_u8_handles_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = UInt8BitmapFilter::try_new(&haystack)?;
        let needles = UInt8Array::from(vec![Some(1), Some(2), None, Some(3)]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );

        Ok(())
    }

    #[test]
    fn bitmap_filter_u8_handles_dictionary_needles() -> Result<()> {
        let haystack: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]));
        let filter = UInt8BitmapFilter::try_new(&haystack)?;

        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Arc::new(UInt8Array::from(vec![Some(1), Some(2), Some(3)]));
        let needles = DictionaryArray::try_new(keys, values)?;

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])
    }
}
