// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{
    ArrayRef, BinaryViewArray, FixedSizeBinaryArray, GenericBinaryArray, OffsetSizeTrait,
    UInt32Array,
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

        let indices = self.gen_indices();
        compute::take(&distinct_binaries, &indices, None).unwrap()
    }

    /// Creates a FixedSizeBinaryArray with random binary data.
    pub fn gen_fixed_size_binary(&mut self) -> ArrayRef {
        let width = self.max_len;
        let distinct_binaries = FixedSizeBinaryArray::try_from_iter(
            (0..self.num_distinct_binaries)
                .map(|_| (0..width).map(|_| self.rng.random()).collect::<Vec<u8>>()),
        )
        .unwrap();

        let indices = self.gen_indices();
        compute::take(&distinct_binaries, &indices, None).unwrap()
    }

    /// Creates a BinaryViewArray with random binary data.
    pub fn gen_binary_view(&mut self) -> ArrayRef {
        let distinct_binary_views: BinaryViewArray = (0..self.num_distinct_binaries)
            .map(|_| Some(random_binary(&mut self.rng, self.max_len)))
            .collect();

        let indices = self.gen_indices();
        compute::take(&distinct_binary_views, &indices, None).unwrap()
    }

    /// Generates nullable indices into the distinct binary values.
    fn gen_indices(&mut self) -> UInt32Array {
        (0..self.num_binaries)
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
            .collect()
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
