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

use arrow::array::{
    ArrayRef, BinaryViewArray, GenericBinaryArray, OffsetSizeTrait, UInt32Array,
};
use arrow::compute;
use rand::Rng;
use rand::rngs::StdRng;

/// Randomly generate binary arrays
pub struct BinaryArrayGenerator {
    /// The maximum length of each binary element
    pub max_len: usize,
    /// The total number of binaries in the output
    pub num_binaries: usize,
    /// The number of distinct binary values in the columns
    pub num_distinct_binaries: usize,
    /// The percentage of nulls in the columns
    pub null_pct: f64,
    /// Random number generator
    pub rng: StdRng,
}

impl BinaryArrayGenerator {
    /// Creates a BinaryArray or LargeBinaryArray with random binary data.
    pub fn gen_data<O: OffsetSizeTrait>(&mut self) -> ArrayRef {
        let distinct_binaries: GenericBinaryArray<O> = (0..self.num_distinct_binaries)
            .map(|_| Some(random_binary(&mut self.rng, self.max_len)))
            .collect();

        // Pick num_binaries randomly from the distinct binary table
        let indices: UInt32Array = (0..self.num_binaries)
            .map(|_| {
                if self.rng.random::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_binaries > 1 {
                    let range = 0..(self.num_distinct_binaries as u32);
                    Some(self.rng.random_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        compute::take(&distinct_binaries, &indices, None).unwrap()
    }

    /// Creates a BinaryViewArray with random binary data.
    pub fn gen_binary_view(&mut self) -> ArrayRef {
        let distinct_binary_views: BinaryViewArray = (0..self.num_distinct_binaries)
            .map(|_| Some(random_binary(&mut self.rng, self.max_len)))
            .collect();

        let indices: UInt32Array = (0..self.num_binaries)
            .map(|_| {
                if self.rng.random::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_binaries > 1 {
                    let range = 0..(self.num_distinct_binaries as u32);
                    Some(self.rng.random_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        compute::take(&distinct_binary_views, &indices, None).unwrap()
    }
}

/// Return a binary vector of random bytes of length 1..=max_len
fn random_binary(rng: &mut StdRng, max_len: usize) -> Vec<u8> {
    if max_len == 0 {
        Vec::new()
    } else {
        let len = rng.random_range(1..=max_len);
        (0..len).map(|_| rng.random()).collect()
    }
}
