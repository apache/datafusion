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

/// Trait for converting type safely from a native type T impl this trait.
pub trait FromNative: std::fmt::Debug + Send + Sync + Copy + Default {
    /// Convert native type from i64.
    fn from_i64(_: i64) -> Option<Self> {
        None
    }
}

macro_rules! native_type {
    ($t: ty $(, $from:ident)*) => {
        impl FromNative for $t {
            $(
                #[inline]
                fn $from(v: $t) -> Option<Self> {
                    Some(v)
                }
            )*
        }
    };
}

native_type!(i8);
native_type!(i16);
native_type!(i32);
native_type!(i64, from_i64);
native_type!(u8);
native_type!(u16);
native_type!(u32);
native_type!(u64);
native_type!(f32);
native_type!(f64);

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
        A::Native: FromNative,
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
                        let millis_per_day = 86_400_000;
                        let adjusted_value = date_value - (date_value % millis_per_day);
                        A::Native::from_i64(adjusted_value).unwrap()
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
