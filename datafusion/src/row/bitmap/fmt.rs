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

use std::fmt::Write;

use super::is_set;

/// Formats `bytes` taking into account an offset and length of the form
pub fn fmt(
    bytes: &[u8],
    offset: usize,
    length: usize,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    assert!(offset < 8);

    f.write_char('[')?;
    let mut remaining = length;
    if remaining == 0 {
        f.write_char(']')?;
        return Ok(());
    }

    let first = bytes[0];
    let bytes = &bytes[1..];
    let empty_before = 8usize.saturating_sub(remaining + offset);
    f.write_str("0b")?;
    for _ in 0..empty_before {
        f.write_char('_')?;
    }
    let until = std::cmp::min(8, offset + remaining);
    for i in offset..until {
        if is_set(first, offset + until - 1 - i) {
            f.write_char('1')?;
        } else {
            f.write_char('0')?;
        }
    }
    for _ in 0..offset {
        f.write_char('_')?;
    }
    remaining -= until - offset;

    if remaining == 0 {
        f.write_char(']')?;
        return Ok(());
    }

    let number_of_bytes = remaining / 8;
    for byte in &bytes[..number_of_bytes] {
        f.write_str(", ")?;
        f.write_fmt(format_args!("{:#010b}", byte))?;
    }
    remaining -= number_of_bytes * 8;
    if remaining == 0 {
        f.write_char(']')?;
        return Ok(());
    }

    let last = bytes[std::cmp::min((length + offset + 7) / 8, bytes.len() - 1)];
    let remaining = (length + offset) % 8;
    f.write_str(", ")?;
    f.write_str("0b")?;
    for _ in 0..(8 - remaining) {
        f.write_char('_')?;
    }
    for i in 0..remaining {
        if is_set(last, remaining - 1 - i) {
            f.write_char('1')?;
        } else {
            f.write_char('0')?;
        }
    }
    f.write_char(']')
}

#[cfg(test)]
mod tests {
    use super::*;

    struct A<'a>(&'a [u8], usize, usize);
    impl<'a> std::fmt::Debug for A<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            fmt(self.0, self.1, self.2, f)
        }
    }

    #[test]
    fn test_debug() -> std::fmt::Result {
        assert_eq!(format!("{:?}", A(&[1], 0, 0)), "[]");
        assert_eq!(format!("{:?}", A(&[0b11000001], 0, 8)), "[0b11000001]");
        assert_eq!(
            format!("{:?}", A(&[0b11000001, 1], 0, 9)),
            "[0b11000001, 0b_______1]"
        );
        assert_eq!(format!("{:?}", A(&[1], 0, 2)), "[0b______01]");
        assert_eq!(format!("{:?}", A(&[1], 1, 2)), "[0b_____00_]");
        assert_eq!(format!("{:?}", A(&[1], 2, 2)), "[0b____00__]");
        assert_eq!(format!("{:?}", A(&[1], 3, 2)), "[0b___00___]");
        assert_eq!(format!("{:?}", A(&[1], 4, 2)), "[0b__00____]");
        assert_eq!(format!("{:?}", A(&[1], 5, 2)), "[0b_00_____]");
        assert_eq!(format!("{:?}", A(&[1], 6, 2)), "[0b00______]");
        assert_eq!(
            format!("{:?}", A(&[0b11000001, 1], 1, 9)),
            "[0b1100000_, 0b______01]"
        );
        // extra bytes are ignored
        assert_eq!(
            format!("{:?}", A(&[0b11000001, 1, 1, 1], 1, 9)),
            "[0b1100000_, 0b______01]"
        );
        assert_eq!(
            format!("{:?}", A(&[0b11000001, 1, 1], 2, 16)),
            "[0b110000__, 0b00000001, 0b______01]"
        );
        Ok(())
    }
}
