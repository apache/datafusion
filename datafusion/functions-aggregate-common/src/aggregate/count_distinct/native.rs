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
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::PrimitiveArray;
use arrow::array::types::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::RandomState;

use datafusion_common::ScalarValue;
use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::utils::memory::estimate_memory_size;
use datafusion_expr_common::accumulator::Accumulator;

use crate::utils::GenericDistinctBuffer;

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
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    #[inline(never)]
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
        let fixed_size = size_of_val(self) + size_of_val(&self.values);

        estimate_memory_size::<T::Native>(num_elements, fixed_size).unwrap()
    }
}

#[derive(Debug)]
pub struct FloatDistinctCountAccumulator<T: ArrowPrimitiveType> {
    values: GenericDistinctBuffer<T>,
}

impl<T: ArrowPrimitiveType> FloatDistinctCountAccumulator<T> {
    pub fn new() -> Self {
        Self {
            values: GenericDistinctBuffer::new(T::DATA_TYPE),
        }
    }
}

impl<T: ArrowPrimitiveType> Default for FloatDistinctCountAccumulator<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType + Debug> Accumulator for FloatDistinctCountAccumulator<T> {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        self.values.state()
    }

    #[inline(never)]
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.values.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        self.values.merge_batch(states)
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.values.len() as i64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.size()
    }
}

/// Optimized COUNT DISTINCT accumulator for u8 using a bool array.
/// Uses 256 bytes to track all possible u8 values.
#[derive(Debug)]
pub struct BoolArray256DistinctCountAccumulator {
    seen: [bool; 256],
}

impl BoolArray256DistinctCountAccumulator {
    pub fn new() -> Self {
        Self { seen: [false; 256] }
    }

    #[inline]
    fn count(&self) -> i64 {
        self.seen.iter().filter(|&&b| b).count() as i64
    }
}

impl Default for BoolArray256DistinctCountAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for BoolArray256DistinctCountAccumulator {
    #[inline(never)]
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<arrow::datatypes::UInt8Type>(&values[0])?;
        for value in arr.iter().flatten() {
            self.seen[value as usize] = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<arrow::datatypes::UInt8Type>(&list)?;
                for value in list.values().iter() {
                    self.seen[*value as usize] = true;
                }
            };
            Ok(())
        })
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let values: Vec<u8> = self
            .seen
            .iter()
            .enumerate()
            .filter_map(|(idx, &seen)| if seen { Some(idx as u8) } else { None })
            .collect();

        let arr = Arc::new(
            PrimitiveArray::<arrow::datatypes::UInt8Type>::from_iter_values(values),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count())))
    }

    fn size(&self) -> usize {
        size_of_val(self) + 256
    }
}

/// Optimized COUNT DISTINCT accumulator for i8 using a bool array.
/// Uses 256 bytes to track all possible i8 values (mapped to 0..255).
#[derive(Debug)]
pub struct BoolArray256DistinctCountAccumulatorI8 {
    seen: [bool; 256],
}

impl BoolArray256DistinctCountAccumulatorI8 {
    pub fn new() -> Self {
        Self { seen: [false; 256] }
    }

    #[inline]
    fn count(&self) -> i64 {
        self.seen.iter().filter(|&&b| b).count() as i64
    }
}

impl Default for BoolArray256DistinctCountAccumulatorI8 {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for BoolArray256DistinctCountAccumulatorI8 {
    #[inline(never)]
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<arrow::datatypes::Int8Type>(&values[0])?;
        for value in arr.iter().flatten() {
            self.seen[value as u8 as usize] = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<arrow::datatypes::Int8Type>(&list)?;
                for value in list.values().iter() {
                    self.seen[*value as u8 as usize] = true;
                }
            };
            Ok(())
        })
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let values: Vec<i8> = self
            .seen
            .iter()
            .enumerate()
            .filter_map(
                |(idx, &seen)| {
                    if seen { Some(idx as u8 as i8) } else { None }
                },
            )
            .collect();

        let arr = Arc::new(
            PrimitiveArray::<arrow::datatypes::Int8Type>::from_iter_values(values),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count())))
    }

    fn size(&self) -> usize {
        size_of_val(self) + 256
    }
}

/// Optimized COUNT DISTINCT accumulator for u16 using a 65536-bit bitmap.
/// Uses 8KB (1024 x u64) to track all possible u16 values.
#[derive(Debug)]
pub struct Bitmap65536DistinctCountAccumulator {
    bitmap: Box<[u64; 1024]>,
}

impl Bitmap65536DistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            bitmap: Box::new([0; 1024]),
        }
    }

    #[inline]
    fn set_bit(&mut self, value: u16) {
        let word = (value / 64) as usize;
        let bit = value % 64;
        self.bitmap[word] |= 1u64 << bit;
    }

    #[inline]
    fn count(&self) -> i64 {
        self.bitmap.iter().map(|w| w.count_ones() as i64).sum()
    }
}

impl Default for Bitmap65536DistinctCountAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Bitmap65536DistinctCountAccumulator {
    #[inline(never)]
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<arrow::datatypes::UInt16Type>(&values[0])?;
        for value in arr.iter().flatten() {
            self.set_bit(value);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<arrow::datatypes::UInt16Type>(&list)?;
                for value in list.values().iter() {
                    self.set_bit(*value);
                }
            };
            Ok(())
        })
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        for (word_idx, &word) in self.bitmap.iter().enumerate() {
            if word != 0 {
                for bit in 0..64 {
                    if (word & (1u64 << bit)) != 0 {
                        values.push((word_idx as u16) * 64 + bit);
                    }
                }
            }
        }

        let arr = Arc::new(
            PrimitiveArray::<arrow::datatypes::UInt16Type>::from_iter_values(values),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count())))
    }

    fn size(&self) -> usize {
        size_of_val(self) + 8192
    }
}

/// Optimized COUNT DISTINCT accumulator for i16 using a 65536-bit bitmap.
/// Uses 8KB (1024 x u64) to track all possible i16 values (mapped to 0..65535).
#[derive(Debug)]
pub struct Bitmap65536DistinctCountAccumulatorI16 {
    bitmap: Box<[u64; 1024]>,
}

impl Bitmap65536DistinctCountAccumulatorI16 {
    pub fn new() -> Self {
        Self {
            bitmap: Box::new([0; 1024]),
        }
    }

    #[inline]
    fn set_bit(&mut self, value: i16) {
        let idx = value as u16;
        let word = (idx / 64) as usize;
        let bit = idx % 64;
        self.bitmap[word] |= 1u64 << bit;
    }

    #[inline]
    fn count(&self) -> i64 {
        self.bitmap.iter().map(|w| w.count_ones() as i64).sum()
    }
}

impl Default for Bitmap65536DistinctCountAccumulatorI16 {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for Bitmap65536DistinctCountAccumulatorI16 {
    #[inline(never)]
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<arrow::datatypes::Int16Type>(&values[0])?;
        for value in arr.iter().flatten() {
            self.set_bit(value);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                let list = as_primitive_array::<arrow::datatypes::Int16Type>(&list)?;
                for value in list.values().iter() {
                    self.set_bit(*value);
                }
            };
            Ok(())
        })
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let mut values = Vec::new();
        for (word_idx, &word) in self.bitmap.iter().enumerate() {
            if word != 0 {
                for bit in 0..64 {
                    if (word & (1u64 << bit)) != 0 {
                        values.push(((word_idx as u16) * 64 + bit) as i16);
                    }
                }
            }
        }

        let arr = Arc::new(
            PrimitiveArray::<arrow::datatypes::Int16Type>::from_iter_values(values),
        );
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count())))
    }

    fn size(&self) -> usize {
        size_of_val(self) + 8192
    }
}
