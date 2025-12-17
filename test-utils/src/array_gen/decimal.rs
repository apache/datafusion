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

use arrow::array::{ArrayRef, PrimitiveArray, PrimitiveBuilder, UInt32Array};
use arrow::datatypes::DecimalType;
use rand::Rng;
use rand::rngs::StdRng;

use super::random_data::RandomNativeData;

/// Randomly generate decimal arrays
pub struct DecimalArrayGenerator {
    /// The precision of the decimal type
    pub precision: u8,
    /// The scale of the decimal type
    pub scale: i8,
    /// The total number of decimals in the output
    pub num_decimals: usize,
    /// The number of distinct decimals in the columns
    pub num_distinct_decimals: usize,
    /// The percentage of nulls in the columns
    pub null_pct: f64,
    /// Random number generator
    pub rng: StdRng,
}

impl DecimalArrayGenerator {
    /// Create a Decimal128Array / Decimal256Array with random values.
    pub fn gen_data<D>(&mut self) -> ArrayRef
    where
        D: DecimalType + RandomNativeData,
    {
        // table of decimals from which to draw
        let distinct_decimals: PrimitiveArray<D> = {
            let mut decimal_builder =
                PrimitiveBuilder::<D>::with_capacity(self.num_distinct_decimals);
            for _ in 0..self.num_distinct_decimals {
                decimal_builder
                    .append_option(Some(D::generate_random_native_data(&mut self.rng)));
            }

            decimal_builder
                .finish()
                .with_precision_and_scale(self.precision, self.scale)
                .unwrap()
        };

        // pick num_decimals randomly from the distinct decimal table
        let indices: UInt32Array = (0..self.num_decimals)
            .map(|_| {
                if self.rng.random::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_decimals > 1 {
                    let range = 1..(self.num_distinct_decimals as u32);
                    Some(self.rng.random_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;
        arrow::compute::take(&distinct_decimals, &indices, options).unwrap()
    }
}
