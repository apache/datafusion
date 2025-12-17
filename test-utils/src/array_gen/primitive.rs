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
use chrono_tz::{TZ_VARIANTS, Tz};
use rand::prelude::IndexedRandom;
use rand::{Rng, rng, rngs::StdRng};
use std::sync::Arc;

use super::random_data::RandomNativeData;

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
        A: ArrowPrimitiveType + RandomNativeData,
    {
        let data_type = match A::DATA_TYPE {
            DataType::Timestamp(unit, _) => {
                let timezone = Self::generate_timezone();
                DataType::Timestamp(unit, timezone)
            }
            other => other,
        };

        // table of primitives from which to draw
        let distinct_primitives: PrimitiveArray<A> = match data_type {
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
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Interval(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Timestamp(_, _) => (0..self.num_distinct_primitives)
                .map(|_| Some(A::generate_random_native_data(&mut self.rng)))
                .collect(),
            _ => {
                let arrow_type = A::DATA_TYPE;
                panic!("Unsupported arrow data type: {arrow_type}")
            }
        };

        // pick num_primitives randomly from the distinct string table
        let indices: UInt32Array = (0..self.num_primitives)
            .map(|_| {
                if self.rng.random::<f64>() < self.null_pct {
                    None
                } else if self.num_distinct_primitives > 1 {
                    let range = 1..(self.num_distinct_primitives as u32);
                    Some(self.rng.random_range(range))
                } else {
                    Some(0)
                }
            })
            .collect();

        let options = None;
        arrow::compute::take(&distinct_primitives, &indices, options).unwrap()
    }

    // Generates a random timezone or returns `None`.
    ///
    /// Returns:
    /// - `Some(Arc<String>)` containing the timezone name.
    /// - `None` if no timezone is selected.
    fn generate_timezone() -> Option<Arc<str>> {
        let mut rng = rng();

        // Allows for timezones + None
        let mut timezone_options: Vec<Option<&Tz>> = vec![None];
        timezone_options.extend(TZ_VARIANTS.iter().map(Some));

        let selected_option = timezone_options.choose(&mut rng).cloned().flatten(); // random timezone/None

        selected_option.map(|tz| Arc::from(tz.name()))
    }
}
