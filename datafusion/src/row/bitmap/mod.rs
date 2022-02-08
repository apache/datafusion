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

//! General utilities for null bit section handling
//!
//! Note: this is a tailored version based on [arrow2 bitmap utils](https://github.com/jorgecarleitao/arrow2/tree/main/src/bitmap/utils)

mod fmt;

pub use fmt::fmt;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const UNSET_BIT_MASK: [u8; 8] = [
    255 - 1,
    255 - 2,
    255 - 4,
    255 - 8,
    255 - 16,
    255 - 32,
    255 - 64,
    255 - 128,
];
const ALL_VALID_MASK: [u8; 8] = [1, 3, 7, 15, 31, 63, 127, 255];

/// Returns whether bit at position `i` in `byte` is set or not
#[inline]
pub fn is_set(byte: u8, i: usize) -> bool {
    (byte & BIT_MASK[i]) != 0
}

/// Sets bit at position `i` in `byte`
#[inline]
pub fn set(byte: u8, i: usize, value: bool) -> u8 {
    if value {
        byte | BIT_MASK[i]
    } else {
        byte & UNSET_BIT_MASK[i]
    }
}

/// Sets bit at position `i` in `data`
#[inline]
pub fn set_bit(data: &mut [u8], i: usize, value: bool) {
    data[i / 8] = set(data[i / 8], i % 8, value);
}

/// Returns whether bit at position `i` in `data` is set or not.
///
/// # Safety
/// `i >= data.len() * 8` results in undefined behavior
#[inline]
pub unsafe fn get_bit_unchecked(data: &[u8], i: usize) -> bool {
    (*data.as_ptr().add(i >> 3) & BIT_MASK[i & 7]) != 0
}

/// Returns the number of bytes required to hold `bits` bits.
#[inline]
pub fn bytes_for(bits: usize) -> usize {
    bits.saturating_add(7) / 8
}

/// Returns if all fields are valid
pub fn all_valid(data: &[u8], n: usize) -> bool {
    for item in data.iter().take(n / 8) {
        if *item != ALL_VALID_MASK[7] {
            return false;
        }
    }
    if n % 8 == 0 {
        true
    } else {
        data[n / 8] == ALL_VALID_MASK[n % 8 - 1]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn test_validity(bs: &[bool]) {
        let mut data = vec![0; bytes_for(bs.len())];
        for (i, b) in bs.iter().enumerate() {
            set_bit(&mut data, i, *b);
        }
        let expected = bs.iter().all(|f| *f);
        assert_eq!(all_valid(&data, bs.len()), expected);
    }

    #[test]
    fn test_all_valid() {
        let sizes = [4, 8, 12, 16, 19, 23, 32, 44];
        for i in sizes {
            {
                // contains false
                let input = {
                    let mut rng = rand::thread_rng();
                    let mut input: Vec<bool> = vec![false; i];
                    rng.fill(&mut input[..]);
                    input
                };
                test_validity(&input);
            }

            {
                // all true
                let input = vec![true; i];
                test_validity(&input);
            }
        }
    }
}
