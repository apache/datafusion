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

//! Hex encoding of bytes and integers.
//!
//! [`encode_bytes`] and [`encode_bytes_into`] encode a byte slice into an
//! owned `String` or an appended `Vec<u8>`, respectively; [`encode_bytes_to_slice`]
//! writes into a caller-provided, pre-sized buffer. [`encode_u64`] encodes an
//! integer, trimming leading zeros. All four take a [`HexCase`] to choose
//! between lowercase and uppercase digits.

/// Case of the emitted hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HexCase {
    /// Digits `0123456789abcdef`.
    Lower,
    /// Digits `0123456789ABCDEF`.
    Upper,
}

const LOWER_DIGITS: &[u8; 16] = b"0123456789abcdef";
const UPPER_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

/// Maps a full byte to its two hex digits, so encoding advances a whole byte
/// per iteration instead of a nibble.
const LOOKUP_LOWER: [[u8; 2]; 256] = build_lookup(LOWER_DIGITS);
const LOOKUP_UPPER: [[u8; 2]; 256] = build_lookup(UPPER_DIGITS);

const fn build_lookup(digits: &[u8; 16]) -> [[u8; 2]; 256] {
    let mut table = [[0u8; 2]; 256];
    let mut i = 0;
    while i < 256 {
        table[i][0] = digits[i >> 4];
        table[i][1] = digits[i & 0xF];
        i += 1;
    }
    table
}

impl HexCase {
    #[inline]
    const fn lookup(self) -> &'static [[u8; 2]; 256] {
        match self {
            HexCase::Lower => &LOOKUP_LOWER,
            HexCase::Upper => &LOOKUP_UPPER,
        }
    }

    #[inline]
    const fn digits(self) -> &'static [u8; 16] {
        match self {
            HexCase::Lower => LOWER_DIGITS,
            HexCase::Upper => UPPER_DIGITS,
        }
    }
}

/// Appends the hex encoding of `bytes` to `out`.
///
/// Allocates only through `out`'s own growth. Callers that must bound or guard
/// that growth should reserve capacity in `out` before calling.
#[inline(always)]
pub fn encode_bytes_into(bytes: &[u8], case: HexCase, out: &mut Vec<u8>) {
    let lookup = case.lookup();
    for &byte in bytes {
        out.extend_from_slice(&lookup[byte as usize]);
    }
}

/// Writes the hex encoding of `bytes` into `out`.
///
/// `out` must be exactly `2 * bytes.len()` bytes long. This is for callers
/// that already own a pre-sized buffer (for example a slice of a larger,
/// pre-allocated output array) and want to write directly into it rather
/// than appending to a `Vec`.
///
/// The length requirement is only checked in debug builds, via
/// `debug_assert_eq!`. In release builds a mismatched `out` length is not
/// an error: only `min(bytes.len(), out.len() / 2)` bytes are encoded, so
/// an `out` that is too short silently drops the remaining input, and an
/// `out` that is too long is left with unwritten, stale bytes at the end.
/// Callers are responsible for sizing `out` correctly.
///
/// # Example
///
/// ```
/// use datafusion_common::utils::hex::{HexCase, encode_bytes_to_slice};
///
/// let mut out = [0u8; 8];
/// encode_bytes_to_slice(&[0xde, 0xad, 0xbe, 0xef], HexCase::Lower, &mut out);
/// assert_eq!(&out, b"deadbeef");
/// ```
#[inline(always)]
pub fn encode_bytes_to_slice(bytes: &[u8], case: HexCase, out: &mut [u8]) {
    debug_assert_eq!(out.len(), bytes.len() * 2);
    let lookup = case.lookup();
    for (&b, chunk) in bytes.iter().zip(out.chunks_exact_mut(2)) {
        chunk.copy_from_slice(&lookup[b as usize]);
    }
}

/// Returns the hex encoding of `bytes` as an owned `String`.
///
/// # Example
///
/// ```
/// use datafusion_common::utils::hex::{HexCase, encode_bytes};
///
/// assert_eq!(encode_bytes(&[0xde, 0xad, 0xbe, 0xef], HexCase::Lower), "deadbeef");
/// assert_eq!(encode_bytes(&[0xde, 0xad, 0xbe, 0xef], HexCase::Upper), "DEADBEEF");
/// ```
#[inline]
pub fn encode_bytes(bytes: &[u8], case: HexCase) -> String {
    let mut out = Vec::with_capacity(bytes.len() * 2);
    encode_bytes_into(bytes, case, &mut out);
    // SAFETY: `out` holds only ASCII hex digits, which are valid UTF-8.
    unsafe { String::from_utf8_unchecked(out) }
}

/// Writes `v` as hex into `buf` and returns the written subslice.
///
/// Digits are written right-aligned with leading zeros trimmed, so the result
/// borrows the tail of `buf`. Zero encodes as `"0"`.
///
/// Signed values should be cast with `as u64`, which yields the two's
/// complement representation that both `to_hex` and Spark's `hex` produce for
/// negative input.
///
/// # Example
///
/// The caller owns the buffer and can reuse it across calls; each call
/// returns a fresh subslice of it, borrowed for as long as `buf` is:
///
/// ```
/// use datafusion_common::utils::hex::{HexCase, encode_u64};
///
/// let mut buf = [0u8; 16];
/// assert_eq!(encode_u64(0xAB, HexCase::Lower, &mut buf), b"ab");
/// assert_eq!(encode_u64(0, HexCase::Lower, &mut buf), b"0");
/// ```
#[inline(always)]
pub fn encode_u64(v: u64, case: HexCase, buf: &mut [u8; 16]) -> &[u8] {
    let start = write_digits(v, case, buf);
    &buf[start..]
}

/// Writes the digits of `v` right-aligned in `buf`, returning the index of the
/// first digit.
///
/// Split out from [`encode_u64`] so the mutable borrow of `buf` ends before the
/// returned slice reborrows it. The `v == 0` case returns early; if that early
/// return instead reborrowed `buf` as `&buf[..]` inline in [`encode_u64`], the
/// borrow checker would extend that reborrow over the rest of the function
/// body, conflicting with the mutable writes on the non-zero path below.
#[inline(always)]
fn write_digits(v: u64, case: HexCase, buf: &mut [u8; 16]) -> usize {
    if v == 0 {
        buf[15] = b'0';
        return 15;
    }

    // Consume two nibbles (one full byte) per iteration.
    let lookup = case.lookup();
    let mut pos = 16;
    let mut rest = v;
    while rest >= 0x10 {
        pos -= 2;
        let pair = lookup[(rest & 0xFF) as usize];
        buf[pos] = pair[0];
        buf[pos + 1] = pair[1];
        rest >>= 8;
    }
    if rest > 0 {
        // A single high nibble (0x1..=0xF) remains.
        pos -= 1;
        buf[pos] = case.digits()[rest as usize];
    }

    pos
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_u64(v: u64, case: HexCase) -> String {
        let mut buf = [0u8; 16];
        String::from_utf8(encode_u64(v, case, &mut buf).to_vec()).unwrap()
    }

    #[test]
    fn encode_u64_zero() {
        assert_eq!(hex_u64(0, HexCase::Lower), "0");
        assert_eq!(hex_u64(0, HexCase::Upper), "0");
    }

    #[test]
    fn encode_u64_single_nibble() {
        for v in 1..=0xFu64 {
            assert_eq!(hex_u64(v, HexCase::Lower), format!("{v:x}"));
            assert_eq!(hex_u64(v, HexCase::Upper), format!("{v:X}"));
        }
    }

    #[test]
    fn encode_u64_digit_count_boundaries() {
        // Straddle each odd/even digit-count boundary: the two-nibbles-per
        // iteration loop plus the trailing single-nibble fixup.
        for v in [
            0x10u64,
            0xFF,
            0x100,
            0xFFF,
            0x1000,
            0xFFFFF,
            0xFFFF_FFFF,
            0x1_0000_0000,
        ] {
            assert_eq!(hex_u64(v, HexCase::Lower), format!("{v:x}"));
            assert_eq!(hex_u64(v, HexCase::Upper), format!("{v:X}"));
        }
    }

    #[test]
    fn encode_u64_max() {
        assert_eq!(hex_u64(u64::MAX, HexCase::Lower), "ffffffffffffffff");
        assert_eq!(hex_u64(u64::MAX, HexCase::Upper), "FFFFFFFFFFFFFFFF");
    }

    #[test]
    fn encode_u64_signed_is_twos_complement() {
        // Callers cast signed values with `as u64`; this is the behaviour both
        // `to_hex` and Spark `hex` rely on for negative input.
        assert_eq!(hex_u64(-1i64 as u64, HexCase::Lower), "ffffffffffffffff");
        assert_eq!(hex_u64(i64::MIN as u64, HexCase::Upper), "8000000000000000");
    }

    #[test]
    fn encode_bytes_empty() {
        assert_eq!(encode_bytes(&[], HexCase::Lower), "");
        assert_eq!(encode_bytes(&[], HexCase::Upper), "");
    }

    #[test]
    fn encode_bytes_examples() {
        assert_eq!(encode_bytes(&[0x00], HexCase::Lower), "00");
        assert_eq!(encode_bytes(&[0xAB], HexCase::Lower), "ab");
        assert_eq!(encode_bytes(&[0xAB], HexCase::Upper), "AB");
        assert_eq!(
            encode_bytes(&[0xde, 0xad, 0xbe, 0xef], HexCase::Lower),
            "deadbeef"
        );
        assert_eq!(
            encode_bytes(&[0xde, 0xad, 0xbe, 0xef], HexCase::Upper),
            "DEADBEEF"
        );
    }

    #[test]
    fn encode_bytes_covers_every_byte_value() {
        let bytes: Vec<u8> = (0..=255u8).collect();

        let expected: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(encode_bytes(&bytes, HexCase::Lower), expected);

        let expected: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
        assert_eq!(encode_bytes(&bytes, HexCase::Upper), expected);
    }

    #[test]
    fn encode_bytes_into_appends_without_clearing() {
        let mut out = b"prefix-".to_vec();
        encode_bytes_into(&[0x01, 0x02], HexCase::Lower, &mut out);
        assert_eq!(out, b"prefix-0102");
    }

    #[test]
    fn encode_u64_reused_buffer_leaks_no_stale_digits() {
        let mut buf = [0u8; 16];
        assert_eq!(
            encode_u64(u64::MAX, HexCase::Lower, &mut buf),
            b"ffffffffffffffff"
        );
        assert_eq!(encode_u64(0, HexCase::Lower, &mut buf), b"0");
        assert_eq!(encode_u64(0xAB, HexCase::Lower, &mut buf), b"ab");
    }

    #[test]
    fn encode_bytes_to_slice_empty() {
        let mut out: [u8; 0] = [];
        encode_bytes_to_slice(&[], HexCase::Lower, &mut out);
        assert_eq!(out, [] as [u8; 0]);
    }

    #[test]
    fn encode_bytes_to_slice_examples() {
        let mut out = [0u8; 8];
        encode_bytes_to_slice(&[0xde, 0xad, 0xbe, 0xef], HexCase::Lower, &mut out);
        assert_eq!(&out, b"deadbeef");

        let mut out = [0u8; 8];
        encode_bytes_to_slice(&[0xde, 0xad, 0xbe, 0xef], HexCase::Upper, &mut out);
        assert_eq!(&out, b"DEADBEEF");
    }

    #[test]
    fn encode_bytes_to_slice_agrees_with_encode_bytes() {
        let bytes: Vec<u8> = (0..=255u8).collect();
        for case in [HexCase::Lower, HexCase::Upper] {
            let mut out = vec![0u8; bytes.len() * 2];
            encode_bytes_to_slice(&bytes, case, &mut out);
            assert_eq!(String::from_utf8(out).unwrap(), encode_bytes(&bytes, case));
        }
    }
}
