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

use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{ArrayRef, PrimitiveArray};

use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::HashSet;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::Hashable;

/// Specialized implementation of `AVG DISTINCT` for Float64 values, handling the
/// special case for NaN values and floating-point equality.
#[derive(Debug)]
pub struct Float64DistinctAvgAccumulator {
    values: HashSet<Hashable<f64>, RandomState>,
}

impl Float64DistinctAvgAccumulator {
    pub fn new() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl Default for Float64DistinctAvgAccumulator {
    fn default() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl Accumulator for Float64DistinctAvgAccumulator {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let arr = Arc::new(
            PrimitiveArray::<arrow::datatypes::Float64Type>::from_iter_values(
                self.values.iter().map(|v| v.0),
            ),
        );
        Ok(vec![SingleRowListArrayBuilder::new(arr).build_list_scalar()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<arrow::datatypes::Float64Type>(&values[0])?;
        arr.iter().for_each(|value| {
            if let Some(value) = value {
                self.values.insert(Hashable(value));
            }
        });

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "avg_distinct states must be single array");

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<arrow::datatypes::Float64Type>(&list)?;
                self.values
                    .extend(list.values().iter().map(|v| Hashable(*v)));
            };
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Float64(None));
        }

        let sum: f64 = self.values.iter().map(|v| v.0).sum();
        let count = self.values.len() as f64;
        let avg = sum / count;

        Ok(ScalarValue::Float64(Some(avg)))
    }

    fn size(&self) -> usize {
        let num_elements = self.values.len();
        let fixed_size = size_of_val(self) + size_of_val(&self.values);

        estimate_memory_size::<f64>(num_elements, fixed_size).unwrap()
    }
}
