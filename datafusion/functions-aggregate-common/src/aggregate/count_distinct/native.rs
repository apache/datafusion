// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Specialized implementation of `COUNT DISTINCT` for "Native" arrays such as
//! [`Int64Array`] and [`Float64Array`]
//!
//! [`Int64Array`]: arrow::array::Int64Array
//! [`Float64Array`]: arrow::array::Float64Array
use std::fmt::Debug;
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::PrimitiveArray;
use arrow::array::types::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::RandomState;

use datafusion_common::cast::{as_boolean_array, as_list_array, as_primitive_array};
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_common::{HashSet, ScalarValue};
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
            PrimitiveArray::<T>::from_iter_values(self.values.iter().copied())
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
        if arr.null_count() == 0 {
            // Fast path: no nulls, so skip the per-element validity check and
            // insert directly from the values buffer (mirrors `merge_batch`).
            self.values.extend(arr.values().iter().copied());
        } else {
            arr.iter().flatten().for_each(|value| {
                self.values.insert(value);
            });
        }

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
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.allocation_size()
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
        size_of_val(self) - size_of_val(&self.values) + self.values.size()
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
            }
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
            }
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
            }
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
            }
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

/// Optimized COUNT DISTINCT accumulator for `Boolean` using two flags.
///
/// Tracks whether `false` and `true` have been observed; nulls are skipped.
/// Result is always 0, 1, or 2.
#[derive(Debug)]
pub struct BooleanDistinctCountAccumulator {
    has_seen_false: bool,
    has_seen_true: bool,
}

impl BooleanDistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            has_seen_false: false,
            has_seen_true: false,
        }
    }

    #[inline]
    fn seen_both(&self) -> bool {
        self.has_seen_false && self.has_seen_true
    }

    #[inline]
    fn count(&self) -> i64 {
        (self.has_seen_false as u8 + self.has_seen_true as u8) as i64
    }

    /// Update flags from a `BooleanArray`, short-circuiting per-flag once set.
    #[inline]
    fn observe(&mut self, arr: &BooleanArray) {
        if !self.has_seen_false && arr.has_false() {
            self.has_seen_false = true;
        }
        if !self.has_seen_true && arr.has_true() {
            self.has_seen_true = true;
        }
    }
}

impl Default for BooleanDistinctCountAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for BooleanDistinctCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() || self.seen_both() {
            return Ok(());
        }

        let arr = as_boolean_array(&values[0])?;
        self.observe(arr);
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() || self.seen_both() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if self.seen_both() {
                return Ok(());
            }
            if let Some(list) = maybe_list {
                self.observe(as_boolean_array(&list)?);
            }
            Ok(())
        })
    }

    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let mut values: Vec<bool> = Vec::with_capacity(2);
        if self.has_seen_false {
            values.push(false);
        }
        if self.has_seen_true {
            values.push(true);
        }

        let arr = Arc::new(BooleanArray::from(values));
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count())))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, Decimal256Array, Int32Array, Int64Array};
    use arrow::datatypes::{Decimal128Type, Decimal256Type, Int32Type, Int64Type, i256};

    fn check_matching_buffer_sizes<T>(data_type: DataType)
    where
        T: ArrowPrimitiveType + Send + Debug,
        T::Native: Eq + Hash,
    {
        for capacity in [0, 1, 3, 4, 7, 8, 14, 15, 128, 1024, 8192] {
            let mut primitive = PrimitiveDistinctCountAccumulator::<T>::new(&data_type);
            let mut generic = GenericDistinctBuffer::<T>::new(data_type.clone());
            primitive.values.reserve(capacity);
            generic.values.reserve(capacity);
            assert_eq!(primitive.values.capacity(), generic.values.capacity());
            assert_eq!(
                primitive.size() - size_of_val(&primitive),
                generic.size() - size_of_val(&generic),
                "{} with reserve({capacity})",
                std::any::type_name::<T>()
            );
        }
    }

    #[test]
    fn primitive_and_generic_sizes_agree_for_matching_capacity() {
        check_matching_buffer_sizes::<Int32Type>(DataType::Int32);
        check_matching_buffer_sizes::<Int64Type>(DataType::Int64);
        check_matching_buffer_sizes::<Decimal128Type>(DataType::Decimal128(38, 0));
        check_matching_buffer_sizes::<Decimal256Type>(DataType::Decimal256(76, 0));
    }

    fn check_update_and_merge<T>(input: ArrayRef)
    where
        T: ArrowPrimitiveType + Send + Debug,
        T::Native: Eq + Hash,
    {
        let mut direct = PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
        direct.update_batch(std::slice::from_ref(&input)).unwrap();
        assert_eq!(direct.evaluate().unwrap(), ScalarValue::Int64(Some(4)));

        let mut merged = PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
        // Keep enough spare capacity to merge overlapping states without growing.
        merged.values.reserve(input.len() * 4);
        let reserved_size = merged.size();
        merged.update_batch(&[input.slice(0, 3)]).unwrap();
        let mut partial = PrimitiveDistinctCountAccumulator::<T>::new(input.data_type());
        partial
            .update_batch(&[input.slice(3, input.len() - 3)])
            .unwrap();
        let state = partial
            .state()
            .unwrap()
            .iter()
            .map(|value| value.to_array().unwrap())
            .collect::<Vec<_>>();
        merged.merge_batch(&state).unwrap();
        merged.merge_batch(&state).unwrap();
        assert_eq!(merged.evaluate().unwrap(), direct.evaluate().unwrap());
        assert_eq!(merged.size(), reserved_size);
        merged.values.clear();
        assert_eq!(merged.evaluate().unwrap(), ScalarValue::Int64(Some(0)));
        assert_eq!(merged.size(), reserved_size);
    }

    #[test]
    fn update_and_merge_preserve_distinct_primitive_values() {
        check_update_and_merge::<Int32Type>(Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            Some(i32::MAX),
            Some(0),
            Some(-1),
            Some(i32::MAX),
            None,
        ])));
        check_update_and_merge::<Int64Type>(Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            Some(0),
            Some(-1),
            Some(i64::MAX),
            None,
        ])));
        let wide = 10_i128.pow(35);
        check_update_and_merge::<Decimal128Type>(Arc::new(
            Decimal128Array::from(vec![
                Some(wide),
                Some(-wide),
                Some(0),
                Some(-1),
                Some(wide),
                None,
            ])
            .with_precision_and_scale(38, 0)
            .unwrap(),
        ));
        let wide: i256 = i256::from_i128(1) << 200_u32;
        check_update_and_merge::<Decimal256Type>(Arc::new(
            Decimal256Array::from(vec![
                Some(wide),
                Some(-wide),
                Some(i256::ZERO),
                Some(i256::MINUS_ONE),
                Some(wide),
                None,
            ])
            .with_precision_and_scale(76, 0)
            .unwrap(),
        ));
    }

    #[test]
    fn empty_size_does_not_include_a_hash_table_allocation() {
        let acc = PrimitiveDistinctCountAccumulator::<Int64Type>::new(&DataType::Int64);
        assert_eq!(acc.size(), size_of_val(&acc));
    }

    #[test]
    fn size_accounts_for_reserved_capacity() {
        let mut acc =
            PrimitiveDistinctCountAccumulator::<Int64Type>::new(&DataType::Int64);
        let initial = acc.size();
        acc.values.reserve(10_000);
        let reserved = acc.size();
        assert!(reserved >= initial + acc.values.capacity() * size_of::<i64>());

        // Changing the number of values without reallocating must not change
        // the reported allocation, including when the reserved table is empty.
        acc.values.extend(0..128);
        assert_eq!(acc.size(), reserved);
        acc.values.clear();
        assert_eq!(acc.size(), reserved);
    }

    #[test]
    fn update_batch_null_free_fast_path_agrees_with_general_path() {
        // The null-free fast path must produce the same distinct set as the
        // general (validity-checking) path.
        let dense: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 2, 1]));
        let sparse: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(2),
            Some(1),
        ]));

        let mut dense_acc =
            PrimitiveDistinctCountAccumulator::<Int64Type>::new(&DataType::Int64);
        dense_acc
            .update_batch(std::slice::from_ref(&dense))
            .unwrap();

        let mut sparse_acc =
            PrimitiveDistinctCountAccumulator::<Int64Type>::new(&DataType::Int64);
        sparse_acc
            .update_batch(std::slice::from_ref(&sparse))
            .unwrap();

        // Both should count the 3 distinct non-null values {1, 2, 3}.
        assert_eq!(dense_acc.evaluate().unwrap(), ScalarValue::Int64(Some(3)));
        assert_eq!(sparse_acc.evaluate().unwrap(), ScalarValue::Int64(Some(3)));
    }
}
