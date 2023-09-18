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

//! Row format validity utilities

use arrow::util::bit_util::get_bit_raw;
use std::fmt::Write;

const ALL_VALID_MASK: [u8; 8] = [1, 3, 7, 15, 31, 63, 127, 255];

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

/// Show null bit for each field in a tuple, 1 for valid and 0 for null.
/// For a tuple with nine total fields, valid at field 0, 6, 7, 8 shows as `[10000011, 1]`.
pub struct NullBitsFormatter<'a> {
    null_bits: &'a [u8],
    field_count: usize,
}

impl<'a> NullBitsFormatter<'a> {
    /// new
    pub fn new(null_bits: &'a [u8], field_count: usize) -> Self {
        Self {
            null_bits,
            field_count,
        }
    }
}

impl<'a> std::fmt::Debug for NullBitsFormatter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut is_first = true;
        let data = self.null_bits;
        for i in 0..self.field_count {
            if is_first {
                f.write_char('[')?;
                is_first = false;
            } else if i % 8 == 0 {
                f.write_str(", ")?;
            }
            if unsafe { get_bit_raw(data.as_ptr(), i) } {
                f.write_char('1')?;
            } else {
                f.write_char('0')?;
            }
        }
        f.write_char(']')?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::bit_util::{ceil, set_bit_raw, unset_bit_raw};
    use rand::Rng;

    fn test_validity(bs: &[bool]) {
        let n = bs.len();
        let mut data = vec![0; ceil(n, 8)];
        for (i, b) in bs.iter().enumerate() {
            if *b {
                let data_argument = &mut data;
                unsafe {
                    set_bit_raw(data_argument.as_mut_ptr(), i);
                };
            } else {
                let data_argument = &mut data;
                unsafe {
                    unset_bit_raw(data_argument.as_mut_ptr(), i);
                };
            }
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

    #[test]
    fn test_formatter() -> std::fmt::Result {
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001], 8)),
            "[10000011]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1], 9)),
            "[10000011, 1]"
        );
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 2)), "[10]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 3)), "[100]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 4)), "[1000]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 5)), "[10000]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 6)), "[100000]");
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[1], 7)),
            "[1000000]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[1], 8)),
            "[10000000]"
        );
        // extra bytes are ignored
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1, 1, 1], 9)),
            "[10000011, 1]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1, 1], 16)),
            "[10000011, 10000000]"
        );
        Ok(())
    }
}
