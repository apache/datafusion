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

//! Common utilities for implementing unicode functions

use crate::strings::{MAX_INLINE_LEN, sub_view};
use arrow::array::{
    Array, ArrayRef, GenericStringArray, Int64Array, OffsetSizeTrait, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBuffer, ScalarBuffer};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::cast::{
    as_generic_string_array, as_int64_array, as_string_view_array,
};
use datafusion_common::exec_err;
use datafusion_expr::ColumnarValue;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

/// If `cv` is a non-null scalar string, return its value.
pub(crate) fn try_as_scalar_str(cv: &ColumnarValue) -> Option<&str> {
    match cv {
        ColumnarValue::Scalar(s) => s.try_as_str().flatten(),
        _ => None,
    }
}

/// If `cv` is a non-null scalar Int64, return its value.
pub(crate) fn try_as_scalar_i64(cv: &ColumnarValue) -> Option<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::Int64(v)) => *v,
        _ => None,
    }
}

/// Estimates data capacity for `pad` based on `length_array` with row length.
/// For ASCII, one row is at most `target_len` bytes.
/// For UTF8, it could be larger
pub(crate) fn pad_data_capacity(length_array: &Int64Array) -> usize {
    length_array
        .iter()
        .flatten()
        .fold(0, |acc, len| acc.saturating_add(len as usize))
}

/// A trait for `left` and `right` byte slicing operations
pub(crate) trait LeftRightSlicer {
    fn slice(string: &str, n: i64) -> Range<usize>;
}

pub(crate) struct LeftSlicer {}

impl LeftRightSlicer for LeftSlicer {
    fn slice(string: &str, n: i64) -> Range<usize> {
        0..left_right_byte_length(string, n)
    }
}

pub(crate) struct RightSlicer {}

impl LeftRightSlicer for RightSlicer {
    fn slice(string: &str, n: i64) -> Range<usize> {
        if n == 0 {
            // Return nothing for `n=0`
            0..0
        } else if n == i64::MIN {
            // Special case for i64::MIN overflow
            0..0
        } else {
            left_right_byte_length(string, -n)..string.len()
        }
    }
}

const HIGH_BITS: u64 = 0x8080_8080_8080_8080;
const LOW_BITS: u64 = 0x0101_0101_0101_0101;

/// Whether `byte` starts a codepoint, i.e. is not a continuation byte
/// `0b10xx_xxxx`.
#[inline(always)]
fn is_char_start(byte: u8) -> bool {
    (byte as i8) >= -64
}

/// Number of codepoints starting in `block`. The sum fits in a `u8`, which
/// lets LLVM keep it in byte lanes and vectorize it even for baseline x86-64.
#[inline(always)]
fn count_char_starts(block: &[u8; 64]) -> usize {
    block
        .iter()
        .fold(0u8, |count, &byte| count + is_char_start(byte) as u8) as usize
}

/// Byte `k` of the result holds the number of codepoints starting in bytes
/// `0..=k` of `word`, so the top byte holds the total.
#[inline(always)]
fn char_start_prefix_counts(word: [u8; 8]) -> u64 {
    let word = u64::from_le_bytes(word);
    // Bit 7 of each byte is set unless the byte is `0b10xx_xxxx`
    let starts = !(word & !(word << 1)) & HIGH_BITS;
    (starts >> 7).wrapping_mul(LOW_BITS)
}

/// Index of the byte where the `n`-th (0-based) codepoint of a word starts,
/// given its [`char_start_prefix_counts`]. `n` must be below the total.
#[inline(always)]
fn select_char_start(prefix_counts: u64, n: usize) -> usize {
    // Bit 7 of `count + 0x7f - n` is set exactly for the bytes whose count
    // exceeds `n`. Counts are at most 8, so no byte carries into the next.
    let above = (prefix_counts + (0x7f - n as u64) * LOW_BITS) & HIGH_BITS;
    above.trailing_zeros() as usize / 8
}

/// Returns the byte offset of the `n`th codepoint in `string`,
/// or `string.len()` if the string has fewer than `n` codepoints.
#[inline(never)]
pub(crate) fn byte_offset_of_char(string: &str, mut n: usize) -> usize {
    let bytes = string.as_bytes();
    let mut start = 0;
    // A codepoint is at most 4 bytes, so for `n < 16` the answer is within the
    // first 64 bytes and counting whole blocks does not pay off.
    if n >= 16 {
        for block in bytes.as_chunks::<64>().0 {
            let count = count_char_starts(block);
            if n < count {
                break;
            }
            n -= count;
            start += 64;
        }
    }
    let (words, tail) = bytes[start..].as_chunks::<8>();
    for &word in words {
        let prefix_counts = char_start_prefix_counts(word);
        let count = (prefix_counts >> 56) as usize;
        if n < count {
            return start + select_char_start(prefix_counts, n);
        }
        n -= count;
        start += 8;
    }
    tail.iter()
        .enumerate()
        .filter(|&(_, &byte)| is_char_start(byte))
        .nth(n)
        .map_or(string.len(), |(i, _)| start + i)
}

/// Returns the byte offset where the last `n` codepoints of `string` start,
/// or 0 if the string has fewer than `n` codepoints.
#[inline(never)]
pub(crate) fn byte_offset_of_char_from_end(string: &str, mut n: usize) -> usize {
    if n == 0 {
        return string.len();
    }
    let bytes = string.as_bytes();
    let mut end = bytes.len();
    // Mirror of `byte_offset_of_char`, with `n` counting from 1: for
    // `n <= 16` the answer is within the last 64 bytes.
    if n > 16 {
        for block in bytes.as_rchunks::<64>().1.iter().rev() {
            let count = count_char_starts(block);
            if n <= count {
                break;
            }
            n -= count;
            end -= 64;
        }
    }
    let (head, words) = bytes[..end].as_rchunks::<8>();
    for &word in words.iter().rev() {
        let prefix_counts = char_start_prefix_counts(word);
        let count = (prefix_counts >> 56) as usize;
        if n <= count {
            return end - 8 + select_char_start(prefix_counts, count - n);
        }
        n -= count;
        end -= 8;
    }
    head.iter()
        .enumerate()
        .rev()
        .filter(|&(_, &byte)| is_char_start(byte))
        .nth(n - 1)
        .map_or(0, |(i, _)| i)
}

/// If `string` has more than `n` codepoints, returns the byte offset of
/// the `n`-th codepoint boundary. Otherwise returns the total codepoint count.
#[inline]
pub(crate) fn char_count_or_boundary(string: &str, n: usize) -> StringCharLen {
    let mut count = 0;
    for (byte_idx, _) in string.char_indices() {
        if count == n {
            return StringCharLen::ByteOffset(byte_idx);
        }
        count += 1;
    }
    StringCharLen::CharCount(count)
}

/// Result of [`char_count_or_boundary`].
pub(crate) enum StringCharLen {
    /// The string has more than `n` codepoints; contains the byte offset
    /// at the `n`-th codepoint boundary.
    ByteOffset(usize),
    /// The string has `n` or fewer codepoints; contains the exact count.
    CharCount(usize),
}

/// Calculate the byte length of the substring of `n` chars from string `string`
// Neither this nor the scans it calls are inlined: inlining the scans here
// makes every call save registers, which costs the ASCII paths about 5%,
// and inlining this into the per-row loops costs more than the call.
#[inline(never)]
fn left_right_byte_length(string: &str, n: i64) -> usize {
    let abs = n.unsigned_abs().min(usize::MAX as u64) as usize;
    let bytes = string.as_bytes();
    // ASCII bytes are never part of a multi-byte UTF-8 sequence, so if the
    // `abs` bytes at the relevant end of the string are ASCII, they are exactly
    // the `abs` characters at that end. Checking only those bytes is cheaper
    // than either a `char_indices()` scan or checking the whole string.
    match n.cmp(&0) {
        Ordering::Equal => 0,
        // Byte offset of the `abs`-th character from the end.
        Ordering::Less => {
            let start = bytes.len().saturating_sub(abs);
            if bytes[start..].is_ascii() {
                start
            } else {
                byte_offset_of_char_from_end(string, abs)
            }
        }
        // Byte offset of the `abs`-th character from the start.
        Ordering::Greater => {
            let end = abs.min(bytes.len());
            if bytes[..end].is_ascii() {
                end
            } else {
                byte_offset_of_char(string, abs)
            }
        }
    }
}

/// General implementation for `left` and `right` functions
pub(crate) fn general_left_right<F: LeftRightSlicer>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let n_array = as_int64_array(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = as_generic_string_array::<i32>(&args[0])?;
            general_left_right_array::<i32, F>(string_array, n_array)
        }
        DataType::LargeUtf8 => {
            let string_array = as_generic_string_array::<i64>(&args[0])?;
            general_left_right_array::<i64, F>(string_array, n_array)
        }
        DataType::Utf8View => {
            let string_view_array = as_string_view_array(&args[0])?;
            general_left_right_view::<F>(string_view_array, n_array)
        }
        _ => exec_err!("Not supported"),
    }
}

/// `left`/`right` for Utf8/LargeUtf8 input.
fn general_left_right_array<T: OffsetSizeTrait, F: LeftRightSlicer>(
    string_array: &GenericStringArray<T>,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let result = string_array
        .iter()
        .zip(n_array.iter())
        .map(|(string, n)| match (string, n) {
            (Some(string), Some(n)) => Some(&string[F::slice(string, n)]),
            _ => None,
        })
        .collect::<GenericStringArray<T>>();
    Ok(Arc::new(result) as ArrayRef)
}

/// `general_left_right` for StringViewArray input.
fn general_left_right_view<F: LeftRightSlicer>(
    string_view_array: &StringViewArray,
    n_array: &Int64Array,
) -> Result<ArrayRef> {
    let views = string_view_array.views();
    let new_nulls = NullBuffer::union(string_view_array.nulls(), n_array.nulls());
    let len = n_array.len();
    let mut has_out_of_line = false;

    let new_views = (0..len)
        .map(|idx| {
            if new_nulls.as_ref().is_some_and(|n| n.is_null(idx)) {
                return 0;
            }

            // SAFETY: we just checked validity above
            let string: &str = unsafe { string_view_array.value_unchecked(idx) };
            let n = n_array.value(idx);

            let range = F::slice(string, n);
            if range.len() > MAX_INLINE_LEN {
                has_out_of_line = true;
            }
            sub_view(views[idx], string.as_bytes(), range)
        })
        .collect::<Vec<u128>>();

    let views = ScalarBuffer::from(new_views);
    let data_buffers = if has_out_of_line {
        Arc::clone(string_view_array.data_buffers())
    } else {
        Arc::from([])
    };

    // SAFETY:
    // - Each view is produced by `sub_view` from the input view and a range
    //   within the input string, as returned by `F::slice`
    // - `F::slice` returns ranges that start and end on char boundaries (see
    //   `left_right_byte_length`), so every result is valid UTF-8
    // - Out-of-line views reuse the original buffer index and adjusted offset
    unsafe {
        let array = StringViewArray::new_unchecked(views, data_buffers, new_nulls);
        Ok(Arc::new(array) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Strings mixing 1 to 4 byte codepoints, with lengths crossing the 8 and
    /// 64 byte boundaries of the word and block loops.
    fn mixed_strings() -> Vec<String> {
        let chars = ['a', 'é', 'ж', '€', '数', '𝄞', '🔥', ' '];
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut strings = vec![];
        for len in 0..200 {
            let mut string = String::new();
            for _ in 0..len {
                // xorshift64
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                string.push(chars[(state % chars.len() as u64) as usize]);
            }
            strings.push(string);
        }
        // Runs of a single width, so whole words and blocks repeat one pattern
        for c in chars {
            strings.push(c.to_string().repeat(100));
        }
        strings
    }

    #[test]
    fn byte_offset_of_char_matches_char_indices() {
        for string in mixed_strings() {
            let char_count = string.chars().count();
            for n in 0..=char_count + 2 {
                let expected = string
                    .char_indices()
                    .nth(n)
                    .map_or(string.len(), |(i, _)| i);
                assert_eq!(byte_offset_of_char(&string, n), expected, "{string:?} {n}");

                let expected = match n {
                    0 => string.len(),
                    _ => string.char_indices().nth_back(n - 1).map_or(0, |(i, _)| i),
                };
                assert_eq!(
                    byte_offset_of_char_from_end(&string, n),
                    expected,
                    "{string:?} {n}"
                );
            }
        }
    }

    #[test]
    fn left_right_byte_length_extreme_n() {
        let string = "aжb€c🔥";
        assert_eq!(left_right_byte_length(string, i64::MAX), string.len());
        assert_eq!(left_right_byte_length(string, i64::MIN), 0);
        assert_eq!(left_right_byte_length(string, -2), "aжb€".len());
        assert_eq!(left_right_byte_length(string, 2), "aж".len());
    }
}
