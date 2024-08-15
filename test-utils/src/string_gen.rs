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

use crate::stagger_batch;
use arrow::array::{ArrayRef, GenericStringArray, OffsetSizeTrait, UInt32Array};
use arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};

/// Randomly generate strings
pub struct StringBatchGenerator {
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

impl StringBatchGenerator {
    /// Make batches of random strings with a random length columns "a" and "b".
    ///
    /// * "a" is a StringArray
    /// * "b" is a LargeStringArray
    pub fn make_input_batches(&mut self) -> Vec<RecordBatch> {
        // use a random number generator to pick a random sized output
        let batch = RecordBatch::try_from_iter(vec![
            ("a", self.gen_data::<i32>()),
            ("b", self.gen_data::<i64>()),
        ])
        .unwrap();
        stagger_batch(batch)
    }

    /// Return a column sorted array of random strings, sorted by a
    ///
    /// if large is false, the array is a StringArray
    /// if large is true, the array is a LargeStringArray
    pub fn make_sorted_input_batches(&mut self, large: bool) -> Vec<RecordBatch> {
        let array = if large {
            self.gen_data::<i32>()
        } else {
            self.gen_data::<i64>()
        };

        let array = arrow::compute::sort(&array, None).unwrap();

        let batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
        stagger_batch(batch)
    }

    /// Creates a StringArray or LargeStringArray with random strings according
    /// to the parameters of the BatchGenerator
    fn gen_data<O: OffsetSizeTrait>(&mut self) -> ArrayRef {
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

    /// Return an set of `BatchGenerator`s that cover a range of interesting
    /// cases
    pub fn interesting_cases() -> Vec<Self> {
        let mut cases = vec![];
        let mut rng = thread_rng();
        for null_pct in [0.0, 0.01, 0.1, 0.5] {
            for _ in 0..100 {
                // max length of generated strings
                let max_len = rng.gen_range(1..50);
                let num_strings = rng.gen_range(1..100);
                let num_distinct_strings = if num_strings > 1 {
                    rng.gen_range(1..num_strings)
                } else {
                    num_strings
                };
                cases.push(StringBatchGenerator {
                    max_len,
                    num_strings,
                    num_distinct_strings,
                    null_pct,
                    rng: StdRng::from_seed(rng.gen()),
                })
            }
        }
        cases
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
