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

//! Shared helpers for Spark-compatible hash functions.
//!
//! Generic helpers ([`hash_primitive_values`], [`hash_bytes_values`]) cover
//! the cases where a function-based abstraction is clean; less uniform paths
//! (booleans, decimals, list/map dispatch) remain as macros.
//!
//! Ported from Apache DataFusion Comet:
//! <https://github.com/apache/datafusion-comet/blob/main/native/spark-expr/src/hash_funcs/utils.rs>

use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::ArrowPrimitiveType;
use datafusion_common::Result;

/// Hash the values of a primitive array, calling `transform` to convert each
/// value into a byte slice before feeding it to `hash_method`.
#[inline]
pub(crate) fn hash_primitive_values<P, F, B, H>(
    array: &PrimitiveArray<P>,
    hashes: &mut [u64],
    transform: F,
    hash_method: H,
) where
    P: ArrowPrimitiveType,
    F: Fn(P::Native) -> B,
    B: AsRef<[u8]>,
    H: Fn(B, u64) -> u64,
{
    let values = array.values();
    if array.null_count() == 0 {
        // Fast path: no nulls, skip null checks
        for (i, hash) in hashes.iter_mut().enumerate() {
            *hash = hash_method(transform(values[i]), *hash);
        }
    } else {
        // Slow path: check each row for null
        for (i, hash) in hashes.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = hash_method(transform(values[i]), *hash);
            }
        }
    }
}

/// Hash an array by calling `value_at(array, i)` to produce a byte slice for
/// each non-null row. Used for byte-slice arrays (Utf8/Binary/FixedSizeBinary
/// and their Large/View variants) and for arrays whose values need a small
/// transform (e.g. booleans widened to `i32`, decimals to `i128` bytes).
#[inline]
pub(crate) fn hash_bytes_values<A, B, H>(
    array: &A,
    hashes: &mut [u64],
    value_at: impl Fn(&A, usize) -> B,
    hash_method: H,
) where
    A: Array,
    B: AsRef<[u8]>,
    H: Fn(B, u64) -> u64,
{
    if array.null_count() == 0 {
        for (i, hash) in hashes.iter_mut().enumerate() {
            *hash = hash_method(value_at(array, i), *hash);
        }
    } else {
        for (i, hash) in hashes.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = hash_method(value_at(array, i), *hash);
            }
        }
    }
}

/// Fallible variant of [`hash_bytes_values`]: `value_at` may return an error
/// (used by the small-decimal path, where values may not fit in `i64`).
#[inline]
pub(crate) fn try_hash_bytes_values<A, B, H>(
    array: &A,
    hashes: &mut [u64],
    value_at: impl Fn(&A, usize) -> Result<B>,
    hash_method: H,
) -> Result<()>
where
    A: Array,
    B: AsRef<[u8]>,
    H: Fn(B, u64) -> u64,
{
    if array.null_count() == 0 {
        for (i, hash) in hashes.iter_mut().enumerate() {
            *hash = hash_method(value_at(array, i)?, *hash);
        }
    } else {
        for (i, hash) in hashes.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = hash_method(value_at(array, i)?, *hash);
            }
        }
    }
    Ok(())
}

/// Downcast `$column` to `$array_type`, panicking with a useful error if the
/// type does not match.
#[macro_export]
#[doc(hidden)]
macro_rules! downcast_array {
    ($column:ident, $array_type:ident) => {
        $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            })
    };
}

/// Hash a byte-slice-accessor array (strings, binary, fixed-size binary, ...)
/// where `value(i)` returns the item directly.
///
/// Kept as a macro because `a.value(i)` returns a borrow whose lifetime
/// depends on the array, which Rust's closure inference does not propagate
/// through a `Fn(&A, usize) -> B` bound with a free type parameter `B`.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array {
    ($array_type:ident, $column:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        if array.null_count() == 0 {
            for i in 0..$hashes.len() {
                $hashes[i] = $hash_method(&array.value(i), $hashes[i]);
            }
        } else {
            for i in 0..$hashes.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method(&array.value(i), $hashes[i]);
                }
            }
        }
    };
}

/// Hash a `BooleanArray`, widening each value to `$hash_input_type` first.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array_boolean {
    ($array_type:ident, $column:ident, $hash_input_type:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        $crate::function::hash::utils::hash_bytes_values(
            array,
            $hashes,
            |a, i| $hash_input_type::from(a.value(i)).to_le_bytes(),
            $hash_method,
        );
    };
}

/// Hash a primitive array, widening each native value to `$ty` before hashing.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array_primitive {
    ($array_type:ident, $column:ident, $ty:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        $crate::function::hash::utils::hash_primitive_values(
            array,
            $hashes,
            |v| (v as $ty).to_le_bytes(),
            $hash_method,
        );
    };
}

/// Hash a floating-point primitive array, normalizing `-0.0` to `0.0` per Spark.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array_primitive_float {
    ($array_type:ident, $column:ident, $ty:ident, $ty2:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        $crate::function::hash::utils::hash_primitive_values(
            array,
            $hashes,
            |v| {
                // Spark uses 0 as hash for -0.0, see `Murmur3Hash` expression.
                if v == 0.0 && v.is_sign_negative() {
                    (0 as $ty2).to_le_bytes()
                } else {
                    (v as $ty).to_le_bytes()
                }
            },
            $hash_method,
        );
    };
}

/// Hash a small (precision <= 18) decimal array by reducing each value to `i64`.
/// Errors via `?` if a value does not fit in `i64`.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array_small_decimal {
    ($array_type:ident, $column:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        $crate::function::hash::utils::try_hash_bytes_values(
            array,
            $hashes,
            |a, i| {
                i64::try_from(a.value(i))
                    .map(|v| v.to_le_bytes())
                    .map_err(|e| DataFusionError::Execution(e.to_string()))
            },
            $hash_method,
        )?;
    };
}

/// Hash a wide decimal array using the raw 128-bit byte representation.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_array_decimal {
    ($array_type:ident, $column:ident, $hashes:ident, $hash_method:ident) => {
        let array = $crate::downcast_array!($column, $array_type);
        $crate::function::hash::utils::hash_bytes_values(
            array,
            $hashes,
            |a, i| a.value(i).to_le_bytes(),
            $hash_method,
        );
    };
}

/// Hash a list array with primitive elements by directly accessing the underlying buffer.
/// This avoids the overhead of slicing and recursive calls for common cases.
/// Supports both variable-length lists (with offsets) and fixed-size lists.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_list_primitive {
    // Variable-length list variant (List/LargeList)
    (offsets: $offsets:expr, $list_array:ident, $elem_array:ident, $hashes:ident, $hash_method:ident, $value_transform:expr) => {
        if $list_array.null_count() == 0 && $elem_array.null_count() == 0 {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                let start = $offsets[row_idx] as usize;
                let end = $offsets[row_idx + 1] as usize;
                for elem_idx in start..end {
                    let value = $elem_array.value(elem_idx);
                    *hash = $hash_method($value_transform(value), *hash);
                }
            }
        } else {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                if !$list_array.is_null(row_idx) {
                    let start = $offsets[row_idx] as usize;
                    let end = $offsets[row_idx + 1] as usize;
                    for elem_idx in start..end {
                        if !$elem_array.is_null(elem_idx) {
                            let value = $elem_array.value(elem_idx);
                            *hash = $hash_method($value_transform(value), *hash);
                        }
                    }
                }
            }
        }
    };
    // Fixed-size list variant
    (fixed_size: $list_size:expr, $list_array:ident, $elem_array:ident, $hashes:ident, $hash_method:ident, $value_transform:expr) => {
        if $list_array.null_count() == 0 && $elem_array.null_count() == 0 {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                let start = row_idx * $list_size;
                for elem_idx in 0..$list_size {
                    let value = $elem_array.value(start + elem_idx);
                    *hash = $hash_method($value_transform(value), *hash);
                }
            }
        } else {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                if !$list_array.is_null(row_idx) {
                    let start = row_idx * $list_size;
                    for elem_idx in 0..$list_size {
                        if !$elem_array.is_null(start + elem_idx) {
                            let value = $elem_array.value(start + elem_idx);
                            *hash = $hash_method($value_transform(value), *hash);
                        }
                    }
                }
            }
        }
    };
}

/// Hash a list array by recursively hashing each element.
/// For each row, we hash all elements in the list.
/// Spark hashes arrays by recursively hashing each element, where each
/// element's hash is computed using the previous element's hash as the seed.
/// This creates a chain: hash(elem_n, hash(elem_n-1, ... hash(elem_0, seed)...))
/// Dispatches hash operations for List/LargeList/FixedSizeList arrays with primitive element types.
/// This macro eliminates duplication by handling the type-to-array mapping for all supported primitives.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_list_with_primitive_elements {
    // Variant for List/LargeList with offsets
    (offsets: $list_array_type:ident, $list_array:ident, $values:ident, $offsets:ident, $field:expr, $hashes_buffer:ident, $hash_method:ident, $recursive_hash_method:ident, $fallback_offset_type:ty, $col:ident) => {
        match $field.data_type() {
            DataType::Int8 => {
                let elem_array = $values.as_any().downcast_ref::<Int8Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i8| (v as i32).to_le_bytes());
            }
            DataType::Int16 => {
                let elem_array = $values.as_any().downcast_ref::<Int16Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i16| (v as i32).to_le_bytes());
            }
            DataType::Int32 => {
                let elem_array = $values.as_any().downcast_ref::<Int32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Int64 => {
                let elem_array = $values.as_any().downcast_ref::<Int64Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Float32 => {
                let elem_array = $values.as_any().downcast_ref::<Float32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f32| if v == 0.0 && v.is_sign_negative() { (0_i32).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Float64 => {
                let elem_array = $values.as_any().downcast_ref::<Float64Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f64| if v == 0.0 && v.is_sign_negative() { (0_i64).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Boolean => {
                let elem_array = $values.as_any().downcast_ref::<BooleanArray>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: bool| (i32::from(v)).to_le_bytes());
            }
            DataType::Utf8 => {
                let elem_array = $values.as_any().downcast_ref::<StringArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = $offsets[row_idx] as usize;
                        let end = $offsets[row_idx + 1] as usize;
                        for elem_idx in start..end {
                            *hash = $hash_method(elem_array.value(elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = $offsets[row_idx] as usize;
                            let end = $offsets[row_idx + 1] as usize;
                            for elem_idx in start..end {
                                if !elem_array.is_null(elem_idx) {
                                    *hash = $hash_method(elem_array.value(elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Binary => {
                let elem_array = $values.as_any().downcast_ref::<BinaryArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = $offsets[row_idx] as usize;
                        let end = $offsets[row_idx + 1] as usize;
                        for elem_idx in start..end {
                            *hash = $hash_method(elem_array.value(elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = $offsets[row_idx] as usize;
                            let end = $offsets[row_idx + 1] as usize;
                            for elem_idx in start..end {
                                if !elem_array.is_null(elem_idx) {
                                    *hash = $hash_method(elem_array.value(elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Date32 => {
                let elem_array = $values.as_any().downcast_ref::<Date32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let elem_array = $values.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            _ => {
                // Fall back to recursive approach for complex element types
                $crate::hash_list_array!($list_array_type, $fallback_offset_type, $col, $hashes_buffer, $recursive_hash_method);
            }
        }
    };
    // Variant for FixedSizeList with fixed size
    (fixed_size: $list_array:ident, $values:ident, $list_size:ident, $field:expr, $hashes_buffer:ident, $hash_method:ident, $recursive_hash_method:ident) => {
        match $field.data_type() {
            DataType::Int8 => {
                let elem_array = $values.as_any().downcast_ref::<Int8Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i8| (v as i32).to_le_bytes());
            }
            DataType::Int16 => {
                let elem_array = $values.as_any().downcast_ref::<Int16Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i16| (v as i32).to_le_bytes());
            }
            DataType::Int32 => {
                let elem_array = $values.as_any().downcast_ref::<Int32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Int64 => {
                let elem_array = $values.as_any().downcast_ref::<Int64Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Float32 => {
                let elem_array = $values.as_any().downcast_ref::<Float32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f32| if v == 0.0 && v.is_sign_negative() { (0_i32).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Float64 => {
                let elem_array = $values.as_any().downcast_ref::<Float64Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f64| if v == 0.0 && v.is_sign_negative() { (0_i64).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Boolean => {
                let elem_array = $values.as_any().downcast_ref::<BooleanArray>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: bool| (i32::from(v)).to_le_bytes());
            }
            DataType::Utf8 => {
                let elem_array = $values.as_any().downcast_ref::<StringArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                if !elem_array.is_null(start + elem_idx) {
                                    *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Binary => {
                let elem_array = $values.as_any().downcast_ref::<BinaryArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                if !elem_array.is_null(start + elem_idx) {
                                    *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Date32 => {
                let elem_array = $values.as_any().downcast_ref::<Date32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let elem_array = $values.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            _ => {
                // Fall back to recursive approach for complex element types
                if $list_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            let elem_array = $values.slice(start + elem_idx, 1);
                            let mut single_hash = [*hash];
                            $recursive_hash_method(&[elem_array], &mut single_hash)?;
                            *hash = single_hash[0];
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                let elem_array = $values.slice(start + elem_idx, 1);
                                let mut single_hash = [*hash];
                                $recursive_hash_method(&[elem_array], &mut single_hash)?;
                                *hash = single_hash[0];
                            }
                        }
                    }
                }
            }
        }
    };
}

/// Hash a map array by hashing its key/value entries in order.
///
/// Specializes for common key/value type combinations so the hot path stays
/// monomorphic; falls back to recursive hashing for arbitrary nested types.
#[macro_export]
#[doc(hidden)]
macro_rules! hash_map_array {
    // Specialized variant: typed key and value arrays, byte transforms.
    ($map_array:ident, $key_array:ident, $value_array:ident, $offsets:ident, $hashes_buffer:ident, $hash_method:ident, $key_transform:expr, $value_transform:expr) => {
        if $map_array.null_count() == 0
            && $key_array.null_count() == 0
            && $value_array.null_count() == 0
        {
            for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                let start = $offsets[row_idx] as usize;
                let end = $offsets[row_idx + 1] as usize;
                for entry_idx in start..end {
                    *hash =
                        $hash_method($key_transform($key_array.value(entry_idx)), *hash);
                    *hash = $hash_method(
                        $value_transform($value_array.value(entry_idx)),
                        *hash,
                    );
                }
            }
        } else {
            for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                if !$map_array.is_null(row_idx) {
                    let start = $offsets[row_idx] as usize;
                    let end = $offsets[row_idx + 1] as usize;
                    for entry_idx in start..end {
                        if !$key_array.is_null(entry_idx) {
                            *hash = $hash_method(
                                $key_transform($key_array.value(entry_idx)),
                                *hash,
                            );
                        }
                        if !$value_array.is_null(entry_idx) {
                            *hash = $hash_method(
                                $value_transform($value_array.value(entry_idx)),
                                *hash,
                            );
                        }
                    }
                }
            }
        }
    };
    // Fallback variant: recursively hash each key/value entry.
    (recursive: $map_array:ident, $keys:ident, $values:ident, $offsets:ident, $hashes_buffer:ident, $recursive_hash_method:ident) => {
        if $map_array.null_count() == 0 {
            for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                let start = $offsets[row_idx] as usize;
                let end = $offsets[row_idx + 1] as usize;
                for entry_idx in start..end {
                    let key_array = $keys.slice(entry_idx, 1);
                    let mut single_hash = [*hash];
                    $recursive_hash_method(&[key_array], &mut single_hash)?;
                    *hash = single_hash[0];

                    let value_array = $values.slice(entry_idx, 1);
                    single_hash = [*hash];
                    $recursive_hash_method(&[value_array], &mut single_hash)?;
                    *hash = single_hash[0];
                }
            }
        } else {
            for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                if !$map_array.is_null(row_idx) {
                    let start = $offsets[row_idx] as usize;
                    let end = $offsets[row_idx + 1] as usize;
                    for entry_idx in start..end {
                        let key_array = $keys.slice(entry_idx, 1);
                        let mut single_hash = [*hash];
                        $recursive_hash_method(&[key_array], &mut single_hash)?;
                        *hash = single_hash[0];

                        let value_array = $values.slice(entry_idx, 1);
                        single_hash = [*hash];
                        $recursive_hash_method(&[value_array], &mut single_hash)?;
                        *hash = single_hash[0];
                    }
                }
            }
        }
    };
}

/// Dispatch over a `MapArray`'s key/value types, calling [`hash_map_array!`]
/// with the right specialization (or the recursive fallback).
#[macro_export]
#[doc(hidden)]
macro_rules! hash_map_with_typed_entries {
    ($col:ident, $field:expr, $hashes_buffer:ident, $hash_method:ident, $recursive_hash_method:ident) => {
        let map_array = $col.as_any().downcast_ref::<MapArray>().unwrap();
        let keys = map_array.keys();
        let values = map_array.values();
        let offsets = map_array.offsets();

        if let DataType::Struct(fields) = $field.data_type() {
            let key_type = &fields[0].data_type();
            let value_type = &fields[1].data_type();

            match (key_type, value_type) {
                (DataType::Utf8, DataType::Int32) => {
                    let key_array = keys.as_any().downcast_ref::<StringArray>().unwrap();
                    let value_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                    $crate::hash_map_array!(
                        map_array, key_array, value_array, offsets, $hashes_buffer,
                        $hash_method, |v| v, |v: i32| v.to_le_bytes()
                    );
                }
                (DataType::Int32, DataType::Utf8) => {
                    let key_array = keys.as_any().downcast_ref::<Int32Array>().unwrap();
                    let value_array = values.as_any().downcast_ref::<StringArray>().unwrap();
                    $crate::hash_map_array!(
                        map_array, key_array, value_array, offsets, $hashes_buffer,
                        $hash_method, |v: i32| v.to_le_bytes(), |v| v
                    );
                }
                (DataType::Utf8, DataType::Utf8) => {
                    let key_array = keys.as_any().downcast_ref::<StringArray>().unwrap();
                    let value_array = values.as_any().downcast_ref::<StringArray>().unwrap();
                    $crate::hash_map_array!(
                        map_array, key_array, value_array, offsets, $hashes_buffer,
                        $hash_method, |v| v, |v| v
                    );
                }
                (DataType::Int32, DataType::Int32) => {
                    let key_array = keys.as_any().downcast_ref::<Int32Array>().unwrap();
                    let value_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                    $crate::hash_map_array!(
                        map_array, key_array, value_array, offsets, $hashes_buffer,
                        $hash_method, |v: i32| v.to_le_bytes(), |v: i32| v.to_le_bytes()
                    );
                }
                _ => {
                    $crate::hash_map_array!(
                        recursive: map_array, keys, values, offsets,
                        $hashes_buffer, $recursive_hash_method
                    );
                }
            }
        } else {
            return Err(DataFusionError::Internal(format!(
                "Map field type must be a struct, got: {}",
                $field.data_type()
            )));
        }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! hash_list_array {
    ($array_type:ident, $offset_type:ty, $column: ident, $hashes: ident, $recursive_hash_method: ident) => {
        let list_array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });

        let values = list_array.values();
        let offsets = list_array.offsets();

        if list_array.null_count() == 0 {
            // Fast path: no nulls, skip null checks
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let len = end - start;
                // Hash each element in sequence, chaining the hash values
                for elem_idx in 0..len {
                    let elem_array = values.slice(start + elem_idx, 1);
                    let mut single_hash = [*hash];
                    $recursive_hash_method(&[elem_array], &mut single_hash)?;
                    *hash = single_hash[0];
                }
            }
        } else {
            // Slow path: array has nulls, check each row
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                if !list_array.is_null(row_idx) {
                    let start = offsets[row_idx] as usize;
                    let end = offsets[row_idx + 1] as usize;
                    let len = end - start;
                    // Hash each element in sequence, chaining the hash values
                    for elem_idx in 0..len {
                        let elem_array = values.slice(start + elem_idx, 1);
                        let mut single_hash = [*hash];
                        $recursive_hash_method(&[elem_array], &mut single_hash)?;
                        *hash = single_hash[0];
                    }
                }
            }
        }
    };
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
///
/// `hash_method` is the hash function to use.
/// `create_dictionary_hash_method` is the function to create hashes for dictionary arrays input.
/// `recursive_hash_method` is the function to call for recursive hashing of complex types.
#[macro_export]
#[doc(hidden)]
macro_rules! create_hashes_internal {
    ($arrays: ident, $hashes_buffer: ident, $hash_method: ident, $create_dictionary_hash_method: ident, $recursive_hash_method: ident) => {
        use arrow::array::{types::*, *};
        use arrow::datatypes::{DataType, TimeUnit};
        use datafusion_common::DataFusionError;

        for (i, col) in $arrays.iter().enumerate() {
            let first_col = i == 0;
            match col.data_type() {
                DataType::Boolean => {
                    $crate::hash_array_boolean!(
                        BooleanArray,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int8 => {
                    $crate::hash_array_primitive!(
                        Int8Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int16 => {
                    $crate::hash_array_primitive!(
                        Int16Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int32 => {
                    $crate::hash_array_primitive!(
                        Int32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int64 => {
                    $crate::hash_array_primitive!(
                        Int64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float32 => {
                    $crate::hash_array_primitive_float!(
                        Float32Array,
                        col,
                        f32,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float64 => {
                    $crate::hash_array_primitive_float!(
                        Float64Array,
                        col,
                        f64,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    $crate::hash_array_primitive!(
                        TimestampSecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMillisecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMicrosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampNanosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date32 => {
                    $crate::hash_array_primitive!(
                        Date32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date64 => {
                    $crate::hash_array_primitive!(
                        Date64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Utf8 => {
                    $crate::hash_array!(StringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeUtf8 => {
                    $crate::hash_array!(LargeStringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Utf8View => {
                    $crate::hash_array!(StringViewArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Binary => {
                    $crate::hash_array!(BinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeBinary => {
                    $crate::hash_array!(LargeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::BinaryView => {
                    $crate::hash_array!(BinaryViewArray, col, $hashes_buffer, $hash_method);
                }
                DataType::FixedSizeBinary(_) => {
                    $crate::hash_array!(FixedSizeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Null => {
                    // Nulls don't update the hash
                }
                // Apache Spark: if it's a small decimal, i.e. precision <= 18, turn it into long and hash it.
                // Else, turn it into bytes and hash it.
                DataType::Decimal128(precision, _) if *precision <= 18 => {
                    $crate::hash_array_small_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
                }
                DataType::Decimal128(_, _) => {
                    $crate::hash_array_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
                }
                DataType::Dictionary(index_type, _) => match **index_type {
                    DataType::Int8 => {
                        $create_dictionary_hash_method::<Int8Type>(col, $hashes_buffer, first_col)?;
                    }
                    DataType::Int16 => {
                        $create_dictionary_hash_method::<Int16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int32 => {
                        $create_dictionary_hash_method::<Int32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int64 => {
                        $create_dictionary_hash_method::<Int64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt8 => {
                        $create_dictionary_hash_method::<UInt8Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt16 => {
                        $create_dictionary_hash_method::<UInt16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt32 => {
                        $create_dictionary_hash_method::<UInt32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt64 => {
                        $create_dictionary_hash_method::<UInt64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unsupported dictionary type in hasher hashing: {}",
                            col.data_type(),
                        )))
                    }
                },
                DataType::List(field) => {
                    let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
                    let values = list_array.values();
                    let offsets = list_array.offsets();

                    $crate::hash_list_with_primitive_elements!(offsets: ListArray, list_array, values, offsets, field, $hashes_buffer, $hash_method, $recursive_hash_method, i32, col);
                }
                DataType::LargeList(field) => {
                    let list_array = col.as_any().downcast_ref::<LargeListArray>().unwrap();
                    let values = list_array.values();
                    let offsets = list_array.offsets();

                    $crate::hash_list_with_primitive_elements!(offsets: LargeListArray, list_array, values, offsets, field, $hashes_buffer, $hash_method, $recursive_hash_method, i64, col);
                }
                DataType::FixedSizeList(field, size) => {
                    let list_array = col.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    let values = list_array.values();
                    let list_size = *size as usize;

                    $crate::hash_list_with_primitive_elements!(fixed_size: list_array, values, list_size, field, $hashes_buffer, $hash_method, $recursive_hash_method);
                }
                DataType::Struct(_) => {
                    let struct_array = col.as_any().downcast_ref::<StructArray>().unwrap();
                    // Hash each field of the struct - Spark hashes all fields recursively
                    let columns: Vec<ArrayRef> = struct_array.columns().to_vec();
                    if !columns.is_empty() {
                        $recursive_hash_method(&columns, $hashes_buffer)?;
                    }
                }
                DataType::Map(field, _) => {
                    $crate::hash_map_with_typed_entries!(
                        col, field, $hashes_buffer, $hash_method, $recursive_hash_method
                    );
                }
                _ => {
                    // This is internal because we should have caught this before.
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type in hasher: {}",
                        col.data_type()
                    )));
                }
            }
        }
    };
}
