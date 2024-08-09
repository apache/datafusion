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

//! Specialized implementation of `COUNT DISTINCT` for "Native" arrays such as
//! [`Int64Array`] and [`Float64Array`]
//!
//! [`Int64Array`]: arrow::array::Int64Array
//! [`Float64Array`]: arrow::array::Float64Array
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::types::ArrowPrimitiveType;
use arrow::array::ArrayRef;
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;

use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::array_into_list_array_nullable;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::Hashable;

#[derive(Debug)]
pub struct PrimitiveDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    values: HashSet<T::Native, RandomState>,
    data_type: DataType,
}

impl<T> PrimitiveDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            values: HashSet::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> Accumulator for PrimitiveDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash,
{
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let arr = Arc::new(
            PrimitiveArray::<T>::from_iter_values(self.values.iter().cloned())
                .with_data_type(self.data_type.clone()),
        );
        let list = Arc::new(array_into_list_array_nullable(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;
        arr.iter().for_each(|value| {
            if let Some(value) = value {
                self.values.insert(value);
            }
        });

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<T>(&list)?;
                self.values.extend(list.values())
            };
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        let num_elements = self.values.len();
        let fixed_size =
            std::mem::size_of_val(self) + std::mem::size_of_val(&self.values);

        estimate_memory_size::<T::Native>(num_elements, fixed_size).unwrap()
    }
}

#[derive(Debug)]
pub struct FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    values: HashSet<Hashable<T::Native>, RandomState>,
}

impl<T> FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    pub fn new() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T> Default for FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Accumulator for FloatDistinctCountAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
{
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let arr = Arc::new(PrimitiveArray::<T>::from_iter_values(
            self.values.iter().map(|v| v.0),
        )) as ArrayRef;
        let list = Arc::new(array_into_list_array_nullable(arr));
        Ok(vec![ScalarValue::List(list)])
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
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

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
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        let num_elements = self.values.len();
        let fixed_size =
            std::mem::size_of_val(self) + std::mem::size_of_val(&self.values);

        estimate_memory_size::<T::Native>(num_elements, fixed_size).unwrap()
    }
}
