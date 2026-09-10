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

//! Provides `rank` function to assign a rank to each value in an array

use arrow::array::cast::AsArray;
use arrow::array::types::*;
use arrow::array::{
    Array, ArrowNativeTypeOp, BooleanArray, GenericByteArray, GenericByteViewArray,
    downcast_primitive_array,
};
use arrow::buffer::NullBuffer;
use arrow::error::{ArrowError};
use std::cmp::Ordering;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType};

/// Whether `arrow_ord::rank` can rank an array of given data type.
pub(crate) fn can_rank(data_type: &DataType) -> bool {
    data_type.is_primitive()
        || matches!(
            data_type,
            DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8View
                | DataType::BinaryView
        )
}

/// Assigns a rank to each value in `array` based on its position in the sorted order
///
/// Where values are equal, they will be assigned the highest of their ranks,
/// leaving gaps in the overall rank assignment
///
/// ```
/// # use arrow_array::StringArray;
/// # use arrow_ord::rank::rank;
/// let array = StringArray::from(vec![Some("foo"), None, Some("foo"), None, Some("bar")]);
/// let ranks = rank(&array, None).unwrap();
/// assert_eq!(ranks, &[5, 2, 5, 2, 3]);
/// ```
pub fn rank(array: &dyn Array, options: Option<SortOptions>) -> Result<Vec<u32>, ArrowError> {
    let options = options.unwrap_or_default();
    let ranks = downcast_primitive_array! {
        array => primitive_rank(array.values(), array.nulls(), options),
        DataType::Boolean => boolean_rank(array.as_boolean(), options),
        DataType::Utf8 => bytes_rank(array.as_bytes::<Utf8Type>(), options),
        DataType::LargeUtf8 => bytes_rank(array.as_bytes::<LargeUtf8Type>(), options),
        DataType::Binary => bytes_rank(array.as_bytes::<BinaryType>(), options),
        DataType::LargeBinary => bytes_rank(array.as_bytes::<LargeBinaryType>(), options),
        DataType::Utf8View => byte_view_rank(array.as_string_view(), options),
        DataType::BinaryView => byte_view_rank(array.as_binary_view(), options),
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
    };
    Ok(ranks)
}

#[inline(never)]
fn primitive_rank<T: ArrowNativeTypeOp>(
    values: &[T],
    nulls: Option<&NullBuffer>,
    options: SortOptions,
) -> Vec<u32> {
    let len: u32 = values.len().try_into().unwrap();
    let to_sort = match nulls.filter(|n| n.null_count() > 0) {
        Some(n) => n
            .valid_indices()
            .map(|idx| (values[idx], idx as u32))
            .collect(),
        None => values.iter().copied().zip(0..len).collect(),
    };
    rank_impl(values.len(), to_sort, options, T::compare, T::is_eq)
}

#[inline(never)]
fn bytes_rank<T: ByteArrayType>(array: &GenericByteArray<T>, options: SortOptions) -> Vec<u32> {
    let to_sort: Vec<(&[u8], u32)> = match array.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => n
            .valid_indices()
            .map(|idx| (array.value(idx).as_ref(), idx as u32))
            .collect(),
        None => (0..array.len())
            .map(|idx| (array.value(idx).as_ref(), idx as u32))
            .collect(),
    };
    rank_impl(array.len(), to_sort, options, Ord::cmp, PartialEq::eq)
}

#[inline(never)]
fn byte_view_rank<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    options: SortOptions,
) -> Vec<u32> {
    // An inline view already contains the complete value. Convert it once to
    // a key whose integer ordering matches the byte ordering, as is done by
    // `sort_byte_view`.
    if array.data_buffers().is_empty() {
        let to_sort: Vec<(u128, u32)> = match array.nulls().filter(|n| n.null_count() > 0) {
            Some(n) => n
                .valid_indices()
                .map(|idx| {
                    // SAFETY: `valid_indices` only yields indices in the array.
                    let raw = unsafe { *array.views().get_unchecked(idx) };
                    (GenericByteViewArray::<T>::inline_key_fast(raw), idx as u32)
                })
                .collect(),
            None => array
                .views()
                .iter()
                .enumerate()
                .map(|(idx, raw)| (GenericByteViewArray::<T>::inline_key_fast(*raw), idx as u32))
                .collect(),
        };
        return rank_impl(
            array.len(),
            to_sort,
            options,
            |a, b| a.cmp(&b),
            |a, b| a == b,
        );
    }

    if has_high_byte_view_key_collision_rate(array) {
        let to_sort: Vec<(&[u8], u32)> = match array.nulls().filter(|n| n.null_count() > 0) {
            Some(n) => n
                .valid_indices()
                .map(|idx| (array.value(idx).as_ref(), idx as u32))
                .collect(),
            None => (0..array.len())
                .map(|idx| (array.value(idx).as_ref(), idx as u32))
                .collect(),
        };
        return rank_impl(array.len(), to_sort, options, Ord::cmp, PartialEq::eq);
    }

    // Cache a wider prefix than the 4 bytes stored in a non-inline view. This
    // pays for the backing-buffer access once per value instead of once per
    // comparison, and only resolves the complete value when two keys collide.
    let to_sort: Vec<(u128, u32)> = match array.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => n
            .valid_indices()
            .map(|idx| {
                // SAFETY: `valid_indices` only yields indices in the array.
                let value: &[u8] = unsafe { array.value_unchecked(idx).as_ref() };
                (byte_view_key(value), idx as u32)
            })
            .collect(),
        None => (0..array.len())
            .map(|idx| {
                // SAFETY: `idx` is in `0..array.len()`.
                let value: &[u8] = unsafe { array.value_unchecked(idx).as_ref() };
                (byte_view_key(value), idx as u32)
            })
            .collect(),
    };
    rank_impl_by(
        array.len(),
        to_sort,
        options,
        |a, b| compare_view_key(array, a, b),
        |a, b| equal_view_key(array, a, b),
    )
}

// A 16-byte prefix fits in one `u128` and is wider than the 4-byte view prefix.
// Shorter keys collide more often; longer keys need another representation.
const BYTE_VIEW_KEY_LEN: usize = 16;

// Four valid values per window keeps sampling cheap while allowing an early
// all-collision decision. More samples improve confidence but add buffer reads.
const BYTE_VIEW_KEY_SAMPLES_PER_WINDOW: usize = 4;

// Capacity for the two windows; it must be at least
// `2 * BYTE_VIEW_KEY_SAMPLES_PER_WINDOW`. Increasing it alone has no effect;
// decreasing it without changing the sampling count can overflow the array.
const BYTE_VIEW_KEY_SAMPLE_SIZE: usize = 8;

// Bound entries inspected when nulls are present. A higher limit finds valid
// samples more reliably but costs reads; a lower limit is cheaper but less
// informative for null-heavy arrays.
const BYTE_VIEW_KEY_MAX_PROBES_PER_WINDOW: usize = 32;

// `colliding_keys * 3 >= sample_len` means roughly one third of sampled keys
// are duplicates. Lower values fall back earlier; higher values risk keeping
// the key path for collision-heavy inputs.
const BYTE_VIEW_KEY_FALLBACK_COLLISION_RATIO: usize = 3;

/// Estimates whether cached byte-view keys collide often enough to make the
/// key-based ranking path unattractive.
/// Caching a wider key usually avoids repeated backing-buffer reads for long
/// views. If sampled keys collide frequently, resolving full values plus the
/// extra key comparison can be slower than comparing slices directly, so the
/// caller falls back to that path. The bounded two-window sample keeps this
/// check inexpensive.
fn has_high_byte_view_key_collision_rate<T: ByteViewType>(array: &GenericByteViewArray<T>) -> bool {
    if array.len() < 2 {
        return false;
    }

    let mut keys = [0_u128; BYTE_VIEW_KEY_SAMPLE_SIZE];
    let mut sample_len = 0;
    let midpoint = array.len() / 2;

    // Probe small local windows in both halves. Keeping each probe local avoids
    // turning collision detection itself into scattered backing-buffer reads.
    for (start, end) in [(0, midpoint), (midpoint, array.len())] {
        let probe_end = end.min(start.saturating_add(BYTE_VIEW_KEY_MAX_PROBES_PER_WINDOW));
        let window_start = sample_len;
        let mut window_samples = 0;

        for idx in start..probe_end {
            if array.is_null(idx) {
                continue;
            }

            // SAFETY: `idx` is within a window bounded by `array.len()`.
            let value: &[u8] = unsafe { array.value_unchecked(idx).as_ref() };
            keys[sample_len] = byte_view_key(value);
            sample_len += 1;
            window_samples += 1;
            if window_samples == BYTE_VIEW_KEY_SAMPLES_PER_WINDOW {
                break;
            }
        }

        // Four equal keys already contribute three collisions. Even if every
        // sample in the other window is distinct, that satisfies the final
        // one-third threshold, so avoid touching the second backing-buffer
        // window in the common all-collision case.
        if window_samples == BYTE_VIEW_KEY_SAMPLES_PER_WINDOW
            && keys[window_start..sample_len]
                .windows(2)
                .all(|w| w[0] == w[1])
        {
            return true;
        }
    }

    if sample_len < 2 {
        return false;
    }

    let keys = &mut keys[..sample_len];
    keys.sort_unstable();
    let unique_keys = 1 + keys.windows(2).filter(|w| w[0] != w[1]).count();

    // If at least roughly one third of sampled keys collide, comparing the
    // wider key before every full-value comparison is likely more expensive
    // than sorting slices directly.
    let colliding_keys = sample_len - unique_keys;
    colliding_keys * BYTE_VIEW_KEY_FALLBACK_COLLISION_RATIO >= sample_len
}

#[inline(always)]
fn byte_view_key(value: &[u8]) -> u128 {
    let mut key = [0_u8; BYTE_VIEW_KEY_LEN];
    let key_len = value.len().min(key.len());
    key[..key_len].copy_from_slice(&value[..key_len]);

    // Big-endian conversion makes integer comparison equivalent to comparing
    // these bytes lexicographically. Equal keys fall back to the full values,
    // covering values that differ after 16 bytes and prefixes containing zero.
    u128::from_be_bytes(key)
}

#[inline(always)]
fn compare_view_key<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    a: &(u128, u32),
    b: &(u128, u32),
) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => {
            // SAFETY: both indices were produced from this array above.
            let full_a: &[u8] = unsafe { array.value_unchecked(a.1 as usize).as_ref() };
            let full_b: &[u8] = unsafe { array.value_unchecked(b.1 as usize).as_ref() };
            full_a.cmp(full_b)
        }
        ordering => ordering,
    }
}

#[inline(always)]
fn equal_view_key<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    a: &(u128, u32),
    b: &(u128, u32),
) -> bool {
    if a.0 != b.0 {
        return false;
    }

    // SAFETY: both indices were produced from this array above.
    let full_a: &[u8] = unsafe { array.value_unchecked(a.1 as usize).as_ref() };
    let full_b: &[u8] = unsafe { array.value_unchecked(b.1 as usize).as_ref() };
    full_a == full_b
}

fn rank_impl_by<T, C, E>(
    len: usize,
    mut valid: Vec<(T, u32)>,
    options: SortOptions,
    compare: C,
    eq: E,
) -> Vec<u32>
where
    C: Fn(&(T, u32), &(T, u32)) -> Ordering,
    E: Fn(&(T, u32), &(T, u32)) -> bool,
{
    // Same ranking and null handling as `rank_impl`, but callbacks receive
    // tuple references because key collisions use the index to read the full
    // value. `rank_impl` passes copied values directly through `Fn(T, T)`.
    // We can use an unstable sort as we combine equal values later
    valid.sort_unstable_by(compare);
    if options.descending {
        valid.reverse();
    }

    let (mut valid_rank, null_rank) = match options.nulls_first {
        true => (len as u32, (len - valid.len()) as u32),
        false => (valid.len() as u32, len as u32),
    };

    let mut out: Vec<_> = vec![null_rank; len];
    if let Some(v) = valid.last() {
        out[v.1 as usize] = valid_rank;
    }

    let mut count = 1; // Number of values in rank
    for w in valid.windows(2).rev() {
        match eq(&w[0], &w[1]) {
            true => {
                count += 1;
                out[w[0].1 as usize] = valid_rank;
            }
            false => {
                valid_rank -= count;
                count = 1;
                out[w[0].1 as usize] = valid_rank
            }
        }
    }

    out
}

fn rank_impl<T, C, E>(
    len: usize,
    mut valid: Vec<(T, u32)>,
    options: SortOptions,
    compare: C,
    eq: E,
) -> Vec<u32>
where
    T: Copy,
    C: Fn(T, T) -> Ordering,
    E: Fn(T, T) -> bool,
{
    // We can use an unstable sort as we combine equal values later
    valid.sort_unstable_by(|a, b| compare(a.0, b.0));
    if options.descending {
        valid.reverse();
    }

    let (mut valid_rank, null_rank) = match options.nulls_first {
        true => (len as u32, (len - valid.len()) as u32),
        false => (valid.len() as u32, len as u32),
    };

    let mut out: Vec<_> = vec![null_rank; len];
    if let Some(v) = valid.last() {
        out[v.1 as usize] = valid_rank;
    }

    let mut count = 1; // Number of values in rank
    for w in valid.windows(2).rev() {
        match eq(w[0].0, w[1].0) {
            true => {
                count += 1;
                out[w[0].1 as usize] = valid_rank;
            }
            false => {
                valid_rank -= count;
                count = 1;
                out[w[0].1 as usize] = valid_rank
            }
        }
    }

    out
}

/// Return the index for the rank when ranking boolean array
///
/// The index is calculated as follows:
/// if is_null is true, the index is 2
/// if is_null is false and the value is true, the index is 1
/// otherwise, the index is 0
///
/// false is 0 and true is 1 because these are the value when cast to number
#[inline]
fn get_boolean_rank_index(value: bool, is_null: bool) -> usize {
    let is_null_num = is_null as usize;
    (is_null_num << 1) | (value as usize & !is_null_num)
}

#[inline(never)]
fn boolean_rank(array: &BooleanArray, options: SortOptions) -> Vec<u32> {
    let null_count = array.null_count() as u32;
    let true_count = array.true_count() as u32;
    let false_count = array.len() as u32 - null_count - true_count;

    // Rank values for [false, true, null] in that order
    //
    // The value for a rank is last value rank + own value count
    // this means that if we have the following order: `false`, `true` and then `null`
    // the ranks will be:
    // - false: false_count
    // - true: false_count + true_count
    // - null: false_count + true_count + null_count
    //
    // If we have the following order: `null`, `false` and then `true`
    // the ranks will be:
    // - false: null_count + false_count
    // - true: null_count + false_count + true_count
    // - null: null_count
    //
    // You will notice that the last rank is always the total length of the array but we don't use it for readability on how the rank is calculated
    let ranks_index: [u32; 3] = match (options.descending, options.nulls_first) {
        // The order is null, true, false
        (true, true) => [
            null_count + true_count + false_count,
            null_count + true_count,
            null_count,
        ],
        // The order is true, false, null
        (true, false) => [
            true_count + false_count,
            true_count,
            true_count + false_count + null_count,
        ],
        // The order is null, false, true
        (false, true) => [
            null_count + false_count,
            null_count + false_count + true_count,
            null_count,
        ],
        // The order is false, true, null
        (false, false) => [
            false_count,
            false_count + true_count,
            false_count + true_count + null_count,
        ],
    };

    match array.nulls().filter(|n| n.null_count() > 0) {
        Some(n) => array
            .values()
            .iter()
            .zip(n.iter())
            .map(|(value, is_valid)| ranks_index[get_boolean_rank_index(value, !is_valid)])
            .collect::<Vec<u32>>(),
        None => array
            .values()
            .iter()
            .map(|value| ranks_index[value as usize])
            .collect::<Vec<u32>>(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    fn assert_same_rank(left: &dyn Array, right: &dyn Array) {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let options = SortOptions {
                    descending,
                    nulls_first,
                };
                assert_eq!(
                    rank(left, Some(options)).unwrap(),
                    rank(right, Some(options)).unwrap()
                );
            }
        }
    }

    #[test]
    fn test_primitive() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = Int32Array::from(vec![Some(1), Some(1), None, Some(3), Some(3), Some(4)]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[3, 3, 1, 5, 5, 6]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[6, 6, 1, 4, 4, 2]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[2, 2, 6, 4, 4, 5]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[5, 5, 6, 3, 3, 1]);

        // Test with non-zero null values
        let nulls = NullBuffer::from(vec![true, true, false, true, false, false]);
        let a = Int32Array::new(vec![1, 4, 3, 4, 5, 5].into(), Some(nulls));
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[4, 6, 3, 6, 3, 3]);
    }

    #[test]
    fn test_get_boolean_rank_index() {
        assert_eq!(get_boolean_rank_index(true, true), 2);
        assert_eq!(get_boolean_rank_index(false, true), 2);
        assert_eq!(get_boolean_rank_index(true, false), 1);
        assert_eq!(get_boolean_rank_index(false, false), 0);
    }

    #[test]
    fn test_nullable_booleans() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = BooleanArray::from(vec![Some(true), Some(true), None, Some(false), Some(false)]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 5, 1, 3, 3]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[3, 3, 1, 5, 5]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[4, 4, 5, 2, 2]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[2, 2, 5, 4, 4]);

        // Test with non-zero null values
        let nulls = NullBuffer::from(vec![true, true, false, true, true]);
        let a = BooleanArray::new(vec![true, true, true, false, false].into(), Some(nulls));
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 5, 1, 3, 3]);
    }

    #[test]
    fn test_booleans() {
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };

        let nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let nulls_last_descending = SortOptions {
            descending: true,
            nulls_first: false,
        };

        let a = BooleanArray::from(vec![true, false, false, false, true]);
        let res = rank(&a, None).unwrap();
        assert_eq!(res, &[5, 3, 3, 3, 5]);

        let res = rank(&a, Some(descending)).unwrap();
        assert_eq!(res, &[2, 5, 5, 5, 2]);

        let res = rank(&a, Some(nulls_last)).unwrap();
        assert_eq!(res, &[5, 3, 3, 3, 5]);

        let res = rank(&a, Some(nulls_last_descending)).unwrap();
        assert_eq!(res, &[2, 5, 5, 5, 2]);
    }

    #[test]
    fn test_bytes() {
        let v = vec!["foo", "fo", "bar", "bar"];
        let values = StringArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[4, 3, 2, 2]);

        let values = LargeStringArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[4, 3, 2, 2]);

        let values = StringViewArray::from(v);
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[4, 3, 2, 2]);

        let v: Vec<&[u8]> = vec![&[1, 2], &[0], &[1, 2, 3], &[1, 2]];
        let values = LargeBinaryArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 1, 4, 3]);

        let values = BinaryArray::from(v.clone());
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 1, 4, 3]);

        let values = BinaryViewArray::from_iter_values(v);
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 1, 4, 3]);
    }

    #[test]
    fn test_inline_byte_views() {
        let string_values = vec![
            Some(""),
            Some("short"),
            None,
            Some("0123456789qa"), // exactly the 12-byte inline limit
            Some("short"),
        ];
        let string_view = StringViewArray::from(string_values.clone());
        let string = StringArray::from(string_values);

        assert!(string_view.data_buffers().is_empty());
        assert_same_rank(&string_view, &string);

        let binary_values: Vec<Option<&[u8]>> = vec![
            Some(b""),
            Some(b"short"),
            None,
            Some(b"0123456789qa"),
            Some(b"short"),
        ];
        let binary_view = BinaryViewArray::from_iter(binary_values.clone());
        let binary = BinaryArray::from_opt_vec(binary_values);

        assert!(binary_view.data_buffers().is_empty());
        assert_same_rank(&binary_view, &binary);
    }

    #[test]
    fn test_string_view_with_nulls() {
        let values = StringViewArray::from(vec![
            Some("a string longer than twelve bytes"),
            Some("bar"),
            None,
            Some("a string longer than twelve bytes"),
        ]);
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 4, 1, 3]);
    }

    #[test]
    fn test_binary_view_with_nulls() {
        let long_value = b"a binary value longer than twelve bytes".as_ref();
        let values = BinaryViewArray::from_iter([
            Some(long_value),
            Some(b"bar".as_ref()),
            None,
            Some(long_value),
        ]);
        let res = rank(&values, None).unwrap();
        assert_eq!(res, &[3, 4, 1, 3]);
    }

    #[test]
    fn test_string_view_key_collisions() {
        let values = vec![
            Some("abcdefghijklmnop"),
            Some("abcdefghijklmnopA"),
            Some("abcdefghijklmnopB"),
            Some("abcdefghijklmnopA"),
            Some("abcdefghijklmno"),
            Some("short"),
            None,
        ];
        let expected = StringArray::from(values.clone());
        let actual = StringViewArray::from(values);

        assert_same_rank(&actual, &expected);
    }

    #[test]
    fn test_binary_view_key_collisions() {
        let zeroes_16 = [0_u8; 16];
        let zeroes_17 = [0_u8; 17];
        let mut zeroes_then_one = [0_u8; 17];
        zeroes_then_one[16] = 1;
        let mut zeroes_then_two = [0_u8; 17];
        zeroes_then_two[16] = 2;

        let values: Vec<Option<&[u8]>> = vec![
            Some(b""),
            Some(b"\0"),
            Some(&zeroes_16),
            Some(&zeroes_17),
            Some(&zeroes_then_one),
            Some(&zeroes_then_two),
            Some(&zeroes_then_one),
            None,
        ];
        let expected = BinaryArray::from_opt_vec(values.clone());
        let actual = BinaryViewArray::from_iter(values);

        assert_same_rank(&actual, &expected);
    }

    #[test]
    fn test_byte_view_high_key_collision_detection() {
        const SIZE: u32 = 64;

        let same_key: StringViewArray = (0..SIZE)
            .map(|i| {
                let suffix = i.wrapping_mul(2_654_435_761);
                Some(format!("abcdefghijklmnop{suffix:08x}"))
            })
            .collect();

        assert_eq!(
            byte_view_key(same_key.value(0).as_bytes()),
            byte_view_key(same_key.value(1).as_bytes())
        );
        assert!(has_high_byte_view_key_collision_rate(&same_key));

        let clustered: StringViewArray = (0..SIZE)
            .map(|i| {
                let suffix = i.wrapping_mul(2_654_435_761);
                let value = if i < SIZE / 2 {
                    format!("{suffix:016x}abcdefgh")
                } else {
                    format!("abcdefghijklmnop{suffix:08x}")
                };
                Some(value)
            })
            .collect();
        assert!(has_high_byte_view_key_collision_rate(&clustered));

        let with_nulls: StringViewArray = (0..SIZE)
            .map(|i| {
                (i % 2 == 0).then(|| {
                    let suffix = i.wrapping_mul(2_654_435_761);
                    format!("abcdefghijklmnop{suffix:08x}")
                })
            })
            .collect();
        assert!(has_high_byte_view_key_collision_rate(&with_nulls));

        let distinct: StringViewArray = (0..SIZE)
            .map(|i| {
                let suffix = i.wrapping_mul(2_654_435_761);
                Some(format!("{suffix:016x}abcdefgh"))
            })
            .collect();
        assert!(!has_high_byte_view_key_collision_rate(&distinct));
    }
}
