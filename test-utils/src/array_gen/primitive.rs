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

use arrow::array::{ArrayRef, PrimitiveArray, UInt32Array};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
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

macro_rules! impl_gen_data {
    ($NATIVE_TYPE:ty, $ARROW_TYPE:ident) => {
        paste::paste! {
            pub fn [< gen_data_ $NATIVE_TYPE >](&mut self) -> ArrayRef {
                    // table of strings from which to draw
                    let distinct_primitives: PrimitiveArray<$ARROW_TYPE> = (0..self.num_distinct_primitives)
                    .map(|_| Some(self.rng.gen::<$NATIVE_TYPE>()))
                    .collect();

                // pick num_strings randomly from the distinct string table
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
    };
}

// TODO: support generating more primitive arrays
impl PrimitiveArrayGenerator {
    impl_gen_data!(i8, Int8Type);
    impl_gen_data!(i16, Int16Type);
    impl_gen_data!(i32, Int32Type);
    impl_gen_data!(i64, Int64Type);
    impl_gen_data!(u8, UInt8Type);
    impl_gen_data!(u16, UInt16Type);
    impl_gen_data!(u32, UInt32Type);
    impl_gen_data!(u64, UInt64Type);
    impl_gen_data!(f32, Float32Type);
    impl_gen_data!(f64, Float64Type);
}
