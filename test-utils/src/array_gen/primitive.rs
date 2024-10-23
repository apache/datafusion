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

use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, UInt32Array};
use arrow::datatypes::DataType;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::rngs::StdRng;
use rand::Rng;

/// Randomly generate primitive array
pub struct PrimitiveArrayGenerator {
    /// the total number of strings in the output
    pub num_primitives: usize,
    /// The number of distinct strings in the columns
    pub num_distinct_primitives: usize,
    /// The percentage of nulls in the columns
    pub null_pct: f64,
    /// Random number generator
    pub rng: StdRng,
}

// TODO: support generating more primitive arrays
impl PrimitiveArrayGenerator {
    pub fn gen_data<A>(&mut self) -> ArrayRef
    where
        A: ArrowPrimitiveType,
        Standard: Distribution<<A as ArrowPrimitiveType>::Native>,
    {
        // table of primitives from which to draw
        let distinct_primitives: PrimitiveArray<A> = (0..self.num_distinct_primitives)
            .map(|_| {
                Some(match A::DATA_TYPE {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32 => self.rng.gen::<A::Native>(),

                    DataType::Date64 => {
                        // TODO: constrain this range to valid dates if necessary
                        let date_value = self.rng.gen_range(i64::MIN..=i64::MAX);
                        let millis_per_day: i64 = 86_400_000;
                        let adjusted_value = date_value - (date_value % millis_per_day);
                        // SAFETY: here we can convert i64 to A::Native safely since we determine that
                        // the type A::Native is i64
                        unsafe {
                            std::ptr::read(
                                &adjusted_value as *const i64 as *const A::Native,
                            )
                        }
                    }

                    _ => {
                        let arrow_type = A::DATA_TYPE;
                        panic!("Unsupported arrow data type: {arrow_type}")
                    }
                })
            })
            .collect();

        // pick num_primitves randomly from the distinct string table
        let indicies: UInt32Array = (0..self.num_primitives)
            .map(|_| {
                if self.rng.gen::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_primitives > 1 {
                    let range = 1..(self.num_distinct_primitives as u32);
                    Some(self.rng.gen_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;
        arrow::compute::take(&distinct_primitives, &indicies, options).unwrap()
    }
}
