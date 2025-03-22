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

//! Specialized implementation of `AVG DISTINCT` for "Native" arrays such as
//! [`Int64Array`] and [`Float64Array`]
//!
//! [`Int64Array`]: arrow::array::Int64Array
//! [`Float64Array`]: arrow::array::Float64Array
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{ArrayRef, ArrowNativeTypeOp, PrimitiveArray};
use arrow::datatypes::{ArrowNativeType, DecimalType};

use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::HashSet;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::{DecimalAverager, Hashable};

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

/// Generic implementation of `AVG DISTINCT` for Decimal types.
/// Handles both Decimal128Type and Decimal256Type.
#[derive(Debug)]
pub struct DecimalDistinctAvgAccumulator<T: DecimalType + Debug> {
    values: HashSet<Hashable<T::Native>, RandomState>,
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + Debug> DecimalDistinctAvgAccumulator<T> {
    pub fn with_decimal_params(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Self {
        Self {
            values: HashSet::default(),
            sum_scale,
            target_precision,
            target_scale,
        }
    }
}

impl<T: DecimalType + Debug> Accumulator for DecimalDistinctAvgAccumulator<T> {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let arr = Arc::new(
            PrimitiveArray::<T>::from_iter_values(self.values.iter().map(|v| v.0))
                .with_data_type(T::TYPE_CONSTRUCTOR(T::MAX_PRECISION, self.sum_scale)),
        );
        Ok(vec![SingleRowListArrayBuilder::new(arr).build_list_scalar()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;
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
                let list = as_primitive_array::<T>(&list)?;
                self.values
                    .extend(list.values().iter().map(|v| Hashable(*v)));
            };
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.values.is_empty() {
            return ScalarValue::new_primitive::<T>(
                None,
                &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
            );
        }

        let sum = self
            .values
            .iter()
            .fold(T::Native::default(), |acc, v| acc.add_wrapping(v.0));
        let count = T::Native::from_usize(self.values.len()).unwrap();

        let decimal_averager = DecimalAverager::<T>::try_new(
            self.sum_scale,
            self.target_precision,
            self.target_scale,
        )?;

        let avg = decimal_averager.avg(sum, count)?;

        ScalarValue::new_primitive::<T>(
            Some(avg),
            &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
        )
    }

    fn size(&self) -> usize {
        let num_elements = self.values.len();
        let fixed_size = size_of_val(self) + size_of_val(&self.values);

        estimate_memory_size::<T::Native>(num_elements, fixed_size).unwrap_or(0)
    }
}
