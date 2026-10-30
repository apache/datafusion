// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{ArrayRef, BooleanArray, BooleanBuilder, UInt32Array};
use arrow::compute::take;
use rand::Rng;
use rand::rngs::StdRng;

/// Randomly generate boolean arrays
pub struct BooleanArrayGenerator {
    pub num_booleans: usize,
    pub num_distinct_booleans: usize,
    pub null_pct: f64,
    pub rng: StdRng,
}

impl BooleanArrayGenerator {
    /// Generate BooleanArray with bit-packed values
    pub fn gen_data<D>(&mut self) -> ArrayRef {
        // Table of booleans from which to draw (distinct means 1 or 2)
        let distinct_booleans: BooleanArray = match self.num_distinct_booleans {
            1 => {
                let value = self.rng.random::<bool>();
                let mut builder = BooleanBuilder::with_capacity(1);
                builder.append_value(value);
                builder.finish()
            }
            2 => {
                let mut builder = BooleanBuilder::with_capacity(2);
                builder.append_value(true);
                builder.append_value(false);
                builder.finish()
            }
            _ => unreachable!(),
        };

        // Generate indices to select from the distinct booleans
        let indices: UInt32Array = (0..self.num_booleans)
            .map(|_| {
                if self.rng.random::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_booleans > 1 {
                    Some(self.rng.random_range(0..self.num_distinct_booleans as u32))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;

        take(&distinct_booleans, &indices, options).unwrap()
    }
}
