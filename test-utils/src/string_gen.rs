use crate::array_gen::StringArrayGenerator;
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
use arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{thread_rng, Rng, SeedableRng};

/// Randomly generate strings
pub struct StringBatchGenerator(StringArrayGenerator);

impl StringBatchGenerator {
    /// Make batches of random strings with a random length columns "a" and "b".
    ///
    /// * "a" is a StringArray
    /// * "b" is a LargeStringArray
    pub fn make_input_batches(&mut self) -> Vec<RecordBatch> {
        // use a random number generator to pick a random sized output
        let batch = RecordBatch::try_from_iter(vec![
            ("a", self.0.gen_data::<i32>()),
            ("b", self.0.gen_data::<i64>()),
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
            self.0.gen_data::<i32>()
        } else {
            self.0.gen_data::<i64>()
        };

        let array = arrow::compute::sort(&array, None).unwrap();

        let batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
        stagger_batch(batch)
    }

    /// Return an set of `BatchGenerator`s that cover a range of interesting
    /// cases
    pub fn interesting_cases() -> Vec<Self> {
        let mut cases = vec![];
        let mut rng = thread_rng();
        for null_pct in [0.0, 0.01, 0.1, 0.5] {
            for _ in 0..10 {
                // max length of generated strings
                let max_len = rng.gen_range(1..50);
                let num_strings = rng.gen_range(1..100);
                let num_distinct_strings = if num_strings > 1 {
                    rng.gen_range(1..num_strings)
                } else {
                    num_strings
                };
                cases.push(StringBatchGenerator(StringArrayGenerator {
                    max_len,
                    num_strings,
                    num_distinct_strings,
                    null_pct,
                    rng: StdRng::from_seed(rng.gen()),
                }))
            }
        }
        cases
    }
}
