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

//! Defines the accumulator for `SUM DISTINCT` for primitive numeric types

use std::collections::HashSet;
use std::fmt::Debug;
use std::mem::{size_of, size_of_val};

use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::ArrowPrimitiveType;
use arrow::array::AsArray;
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::DataType;

use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::Hashable;

/// Accumulator for computing SUM(DISTINCT expr)
pub struct DistinctSumAccumulator<T: ArrowPrimitiveType> {
    values: HashSet<Hashable<T::Native>, RandomState>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> Debug for DistinctSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctSumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowPrimitiveType> DistinctSumAccumulator<T> {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            values: HashSet::default(),
            data_type: data_type.clone(),
        }
    }

    pub fn distinct_count(&self) -> usize {
        self.values.len()
    }
}

impl<T: ArrowPrimitiveType> Accumulator for DistinctSumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let distinct_values = self
                .values
                .iter()
                .map(|value| {
                    ScalarValue::new_primitive::<T>(Some(value.0), &self.data_type)
                })
                .collect::<Result<Vec<_>>>()?;

            vec![ScalarValue::List(ScalarValue::new_list_nullable(
                &distinct_values,
                &self.data_type,
            ))]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(Hashable(array.value(idx)));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(Hashable(*x));
            }),
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for x in states[0].as_list::<i32>().iter().flatten() {
            self.update_batch(&[x])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc.add_wrapping(distinct_value.0)
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<T::Native>()
    }
}
