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

use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, UInt32Array};
use rand::rngs::StdRng;
use rand::Rng;

/// Randomly generate string arrays
pub struct StringArrayGenerator {
    //// The maximum length of the strings
    pub max_len: usize,
    /// the total number of strings in the output
    pub num_strings: usize,
    /// The number of distinct strings in the columns
    pub num_distinct_strings: usize,
    /// The percentage of nulls in the columns
    pub null_pct: f64,
    /// Random number generator
    pub rng: StdRng,
}

impl StringArrayGenerator {
    /// Creates a StringArray or LargeStringArray with random strings according
    /// to the parameters of the BatchGenerator
    pub fn gen_data<O: OffsetSizeTrait>(&mut self) -> ArrayRef {
        // table of strings from which to draw
        let distinct_strings: GenericStringArray<O> = (0..self.num_distinct_strings)
            .map(|_| Some(random_string(&mut self.rng, self.max_len)))
            .collect();

        // pick num_strings randomly from the distinct string table
        let indicies: UInt32Array = (0..self.num_strings)
            .map(|_| {
                if self.rng.gen::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_strings > 1 {
                    let range = 1..(self.num_distinct_strings as u32);
                    Some(self.rng.gen_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;
        arrow::compute::take(&distinct_strings, &indicies, options).unwrap()
    }
}

/// Return a string of random characters of length 1..=max_len
fn random_string(rng: &mut StdRng, max_len: usize) -> String {
    // pick characters at random (not just ascii)
    match max_len {
        0 => "".to_string(),
        1 => String::from(rng.gen::<char>()),
        _ => {
            let len = rng.gen_range(1..=max_len);
            rng.sample_iter::<char, _>(rand::distributions::Standard)
                .take(len)
                .map(char::from)
                .collect::<String>()
        }
    }
}
