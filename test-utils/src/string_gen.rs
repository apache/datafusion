// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::array_gen::StringArrayGenerator;
use crate::stagger_batch;
use arrow::record_batch::RecordBatch;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, rng};

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

    /// Return a set of `BatchGenerator`s that cover a range of interesting
    /// cases
    pub fn interesting_cases() -> Vec<Self> {
        let mut cases = vec![];
        let mut rng = rng();
        for null_pct in [0.0, 0.01, 0.1, 0.5] {
            for _ in 0..10 {
                // max length of generated strings
                let max_len = rng.random_range(1..50);
                let num_strings = rng.random_range(1..100);
                let num_distinct_strings = if num_strings > 1 {
                    rng.random_range(1..num_strings)
                } else {
                    num_strings
                };
                cases.push(StringBatchGenerator(StringArrayGenerator {
                    max_len,
                    num_strings,
                    num_distinct_strings,
                    null_pct,
                    rng: StdRng::from_seed(rng.random()),
                }))
            }
        }
        cases
    }
}
