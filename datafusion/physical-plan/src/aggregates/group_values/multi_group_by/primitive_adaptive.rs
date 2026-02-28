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

//! An adaptive [`GroupColumn`] for primitive types that switches between
//! flat (u32-indexed) and native storage at runtime based on observed data
//! range, without requiring column statistics.
//!
//! The flat mode uses a **fixed base** established on the first batch.
//! All u32 indices are `value.index_from(base)` truncated to u32.
//! Since the base never changes, **no shifting of existing indices is
//! ever required**. Migration to native storage happens only when a
//! value's `index_from(base)` would exceed `u32::MAX`.
//!
//! For types where `size_of::<T::Native>() <= 4`, the full type range
//! always fits in u32, so migration never occurs.
//!
//! [`GroupColumn`]: super::GroupColumn

use crate::aggregates::group_values::multi_group_by::{
    GroupColumn, Nulls, nulls_equal_to,
};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use crate::aggregates::group_values::single_group_by::primitive_flat::FlatIndex;
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, cast::AsArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use itertools::izip;
use std::iter;
use std::sync::Arc;

/// A [`GroupColumn`] for primitive types that adaptively chooses between
/// flat (u32-indexed) and native value storage at runtime.
///
/// Benefits of flat mode:
/// - Memory savings for large types (i64: 8→4 bytes per group)
/// - Uniform u32 comparisons in vectorized hot paths
///
/// # Template parameters
///
/// `T`: the Arrow primitive type
/// `NULLABLE`: if the data can contain any nulls
pub(crate) struct PrimitiveGroupValueBuilderAdaptive<
    T: ArrowPrimitiveType,
    const NULLABLE: bool,
> where
    T::Native: FlatIndex + Ord,
{
    data_type: DataType,
    state: AdaptiveColumnState<T>,
}

enum AdaptiveColumnState<T: ArrowPrimitiveType>
where
    T::Native: FlatIndex + Ord,
{
    /// Haven't seen data yet; will decide on first append.
    Initial,
    /// Using u32 indices `(value - base) as u32` for compact storage.
    /// The base is fixed at initialization and never changes.
    Flat {
        group_indices: Vec<u32>,
        /// Fixed base established on first non-null data.
        base: T::Native,
        nulls: MaybeNullBufferBuilder,
    },
    /// Using native values directly (terminal state).
    Native {
        group_values: Vec<T::Native>,
        nulls: MaybeNullBufferBuilder,
    },
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool>
    PrimitiveGroupValueBuilderAdaptive<T, NULLABLE>
where
    T::Native: FlatIndex + Ord,
{
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            state: AdaptiveColumnState::Initial,
        }
    }

    /// Initialize state from the first non-null value seen.
    /// Uses that value as the fixed base for all future flat indexing.
    fn initialize(&mut self, array: &ArrayRef, rows: &[usize]) {
        let arr = array.as_primitive::<T>();
        // Find first non-null value to use as base
        let base = rows.iter().find_map(|&row| {
            if !NULLABLE || !arr.is_null(row) {
                Some(arr.value(row))
            } else {
                None
            }
        });
        match base {
            Some(base) => {
                self.state = AdaptiveColumnState::Flat {
                    group_indices: Vec::new(),
                    base,
                    nulls: MaybeNullBufferBuilder::new(),
                };
            }
            None => {
                // All nulls: start flat with default base
                self.state = AdaptiveColumnState::Flat {
                    group_indices: Vec::new(),
                    base: T::Native::default(),
                    nulls: MaybeNullBufferBuilder::new(),
                };
            }
        }
    }

    /// Migrate from flat to native storage.
    fn migrate_to_native(&mut self) {
        if let AdaptiveColumnState::Flat {
            group_indices,
            base,
            nulls,
        } = &mut self.state
        {
            let values: Vec<T::Native> = group_indices
                .iter()
                .map(|&idx| T::Native::from_index(idx as usize, *base))
                .collect();
            self.state = AdaptiveColumnState::Native {
                group_values: values,
                nulls: std::mem::replace(nulls, MaybeNullBufferBuilder::new()),
            };
        }
    }

    /// Check if a value's index from base fits in u32.
    /// For types where `size_of::<T::Native>() <= 4`, this always returns true.
    #[inline]
    fn fits_in_u32(value: T::Native, base: T::Native) -> bool {
        if size_of::<T::Native>() <= 4 {
            true
        } else {
            value.index_from(base) <= u32::MAX as usize
        }
    }

    #[inline]
    fn value_to_u32(value: T::Native, base: T::Native) -> u32 {
        value.index_from(base) as u32
    }

    // ========================================================================
    // Flat-mode helpers
    // ========================================================================

    fn flat_equal_to(
        group_indices: &[u32],
        base: T::Native,
        nulls: &MaybeNullBufferBuilder,
        lhs_row: usize,
        array: &ArrayRef,
        rhs_row: usize,
    ) -> bool {
        if NULLABLE {
            let exist_null = nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
        }

        let value = array.as_primitive::<T>().value(rhs_row);
        // If value doesn't fit in u32 from this base, it can't match
        if !Self::fits_in_u32(value, base) {
            return false;
        }
        group_indices[lhs_row] == Self::value_to_u32(value, base)
    }

    fn flat_vectorized_equal_to_non_nullable(
        group_indices: &[u32],
        base: T::Native,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array_values = array.as_primitive::<T>().values();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            let result = {
                let left = if cfg!(debug_assertions) {
                    group_indices[lhs_row]
                } else {
                    unsafe { *group_indices.get_unchecked(lhs_row) }
                };
                let right = if cfg!(debug_assertions) {
                    Self::value_to_u32(array_values[rhs_row], base)
                } else {
                    Self::value_to_u32(
                        unsafe { *array_values.get_unchecked(rhs_row) },
                        base,
                    )
                };

                left == right
            };

            *equal_to_result = result && *equal_to_result;
        }
    }

    fn flat_vectorized_equal_nullable(
        group_indices: &[u32],
        base: T::Native,
        nulls: &MaybeNullBufferBuilder,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array = array.as_primitive::<T>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            if !*equal_to_result {
                continue;
            }

            let exist_null = nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                *equal_to_result = result;
                continue;
            }

            let value = array.value(rhs_row);
            *equal_to_result = Self::fits_in_u32(value, base)
                && (group_indices[lhs_row] == Self::value_to_u32(value, base));
        }
    }

    // ========================================================================
    // Native-mode helpers
    // ========================================================================

    fn native_equal_to(
        group_values: &[T::Native],
        nulls: &MaybeNullBufferBuilder,
        lhs_row: usize,
        array: &ArrayRef,
        rhs_row: usize,
    ) -> bool {
        if NULLABLE {
            let exist_null = nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
        }

        group_values[lhs_row] == array.as_primitive::<T>().value(rhs_row)
    }

    fn native_vectorized_equal_to_non_nullable(
        group_values: &[T::Native],
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array_values = array.as_primitive::<T>().values();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            let result = {
                let left = if cfg!(debug_assertions) {
                    group_values[lhs_row]
                } else {
                    unsafe { *group_values.get_unchecked(lhs_row) }
                };
                let right = if cfg!(debug_assertions) {
                    array_values[rhs_row]
                } else {
                    unsafe { *array_values.get_unchecked(rhs_row) }
                };

                left == right
            };

            *equal_to_result = result && *equal_to_result;
        }
    }

    fn native_vectorized_equal_nullable(
        group_values: &[T::Native],
        nulls: &MaybeNullBufferBuilder,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array = array.as_primitive::<T>();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            if !*equal_to_result {
                continue;
            }

            let exist_null = nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                *equal_to_result = result;
                continue;
            }

            *equal_to_result = group_values[lhs_row] == array.value(rhs_row);
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> GroupColumn
    for PrimitiveGroupValueBuilderAdaptive<T, NULLABLE>
where
    T::Native: FlatIndex + Ord,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        match &self.state {
            AdaptiveColumnState::Initial => {
                unreachable!("equal_to called before any values stored")
            }
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                Self::flat_equal_to(group_indices, *base, nulls, lhs_row, array, rhs_row)
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => Self::native_equal_to(group_values, nulls, lhs_row, array, rhs_row),
        }
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        if matches!(self.state, AdaptiveColumnState::Initial) {
            self.initialize(array, &[row]);
        }

        match &mut self.state {
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                if NULLABLE && array.is_null(row) {
                    nulls.append(true);
                    group_indices.push(0);
                } else {
                    let value = array.as_primitive::<T>().value(row);
                    if !Self::fits_in_u32(value, *base) {
                        // Value doesn't fit — migrate and retry
                        self.migrate_to_native();
                        return self.append_val(array, row);
                    }
                    if NULLABLE {
                        nulls.append(false);
                    }
                    group_indices.push(Self::value_to_u32(value, *base));
                }
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => {
                if NULLABLE {
                    if array.is_null(row) {
                        nulls.append(true);
                        group_values.push(T::Native::default());
                    } else {
                        nulls.append(false);
                        group_values.push(array.as_primitive::<T>().value(row));
                    }
                } else {
                    group_values.push(array.as_primitive::<T>().value(row));
                }
            }
            AdaptiveColumnState::Initial => unreachable!(),
        }
        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        match &self.state {
            AdaptiveColumnState::Initial => {
                unreachable!("vectorized_equal_to called before any values stored")
            }
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                if !NULLABLE || (array.null_count() == 0 && !nulls.might_have_nulls()) {
                    Self::flat_vectorized_equal_to_non_nullable(
                        group_indices,
                        *base,
                        lhs_rows,
                        array,
                        rhs_rows,
                        equal_to_results,
                    );
                } else {
                    Self::flat_vectorized_equal_nullable(
                        group_indices,
                        *base,
                        nulls,
                        lhs_rows,
                        array,
                        rhs_rows,
                        equal_to_results,
                    );
                }
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => {
                if !NULLABLE || (array.null_count() == 0 && !nulls.might_have_nulls()) {
                    Self::native_vectorized_equal_to_non_nullable(
                        group_values,
                        lhs_rows,
                        array,
                        rhs_rows,
                        equal_to_results,
                    );
                } else {
                    Self::native_vectorized_equal_nullable(
                        group_values,
                        nulls,
                        lhs_rows,
                        array,
                        rhs_rows,
                        equal_to_results,
                    );
                }
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        if matches!(self.state, AdaptiveColumnState::Initial) {
            self.initialize(array, rows);
        }

        // Check if any appended value requires migration (only for >32-bit types)
        if size_of::<T::Native>() > 4 {
            if let AdaptiveColumnState::Flat { base, .. } = &self.state {
                let base = *base;
                let arr = array.as_primitive::<T>();
                let needs_migrate = if arr.null_count() == 0 {
                    rows.iter()
                        .any(|&row| !Self::fits_in_u32(arr.value(row), base))
                } else {
                    rows.iter().any(|&row| {
                        !arr.is_null(row) && !Self::fits_in_u32(arr.value(row), base)
                    })
                };
                if needs_migrate {
                    self.migrate_to_native();
                }
            }
        }

        match &mut self.state {
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                let arr = array.as_primitive::<T>();
                let base_val = *base;
                let null_count = array.null_count();
                let num_rows = array.len();
                let nulls_kind = if null_count == 0 {
                    Nulls::None
                } else if null_count == num_rows {
                    Nulls::All
                } else {
                    Nulls::Some
                };

                match (NULLABLE, nulls_kind) {
                    (true, Nulls::Some) => {
                        for &row in rows {
                            if array.is_null(row) {
                                nulls.append(true);
                                group_indices.push(0);
                            } else {
                                nulls.append(false);
                                group_indices
                                    .push(Self::value_to_u32(arr.value(row), base_val));
                            }
                        }
                    }
                    (true, Nulls::None) => {
                        nulls.append_n(rows.len(), false);
                        for &row in rows {
                            group_indices
                                .push(Self::value_to_u32(arr.value(row), base_val));
                        }
                    }
                    (true, Nulls::All) => {
                        nulls.append_n(rows.len(), true);
                        group_indices.extend(iter::repeat_n(0u32, rows.len()));
                    }
                    (false, _) => {
                        for &row in rows {
                            group_indices
                                .push(Self::value_to_u32(arr.value(row), base_val));
                        }
                    }
                }
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => {
                let arr = array.as_primitive::<T>();
                let null_count = array.null_count();
                let num_rows = array.len();
                let nulls_kind = if null_count == 0 {
                    Nulls::None
                } else if null_count == num_rows {
                    Nulls::All
                } else {
                    Nulls::Some
                };

                match (NULLABLE, nulls_kind) {
                    (true, Nulls::Some) => {
                        for &row in rows {
                            if array.is_null(row) {
                                nulls.append(true);
                                group_values.push(T::Native::default());
                            } else {
                                nulls.append(false);
                                group_values.push(arr.value(row));
                            }
                        }
                    }
                    (true, Nulls::None) => {
                        nulls.append_n(rows.len(), false);
                        for &row in rows {
                            group_values.push(arr.value(row));
                        }
                    }
                    (true, Nulls::All) => {
                        nulls.append_n(rows.len(), true);
                        group_values
                            .extend(iter::repeat_n(T::Native::default(), rows.len()));
                    }
                    (false, _) => {
                        for &row in rows {
                            group_values.push(arr.value(row));
                        }
                    }
                }
            }
            AdaptiveColumnState::Initial => unreachable!(),
        }

        Ok(())
    }

    fn len(&self) -> usize {
        match &self.state {
            AdaptiveColumnState::Initial => 0,
            AdaptiveColumnState::Flat { group_indices, .. } => group_indices.len(),
            AdaptiveColumnState::Native { group_values, .. } => group_values.len(),
        }
    }

    fn size(&self) -> usize {
        match &self.state {
            AdaptiveColumnState::Initial => 0,
            AdaptiveColumnState::Flat {
                group_indices,
                nulls,
                ..
            } => group_indices.allocated_size() + nulls.allocated_size(),
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => group_values.allocated_size() + nulls.allocated_size(),
        }
    }

    fn build(self: Box<Self>) -> ArrayRef {
        match self.state {
            AdaptiveColumnState::Initial => {
                let empty = PrimitiveArray::<T>::builder(0)
                    .finish()
                    .with_data_type(self.data_type);
                Arc::new(empty)
            }
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                let nulls = nulls.build();
                if !NULLABLE {
                    assert!(nulls.is_none(), "unexpected nulls in non nullable input");
                }
                let values: Vec<T::Native> = group_indices
                    .iter()
                    .map(|&idx| T::Native::from_index(idx as usize, base))
                    .collect();
                let arr = PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls);
                Arc::new(arr.with_data_type(self.data_type))
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => {
                let nulls = nulls.build();
                if !NULLABLE {
                    assert!(nulls.is_none(), "unexpected nulls in non nullable input");
                }
                let arr =
                    PrimitiveArray::<T>::new(ScalarBuffer::from(group_values), nulls);
                Arc::new(arr.with_data_type(self.data_type))
            }
        }
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        match &mut self.state {
            AdaptiveColumnState::Initial => {
                let empty = PrimitiveArray::<T>::builder(0)
                    .finish()
                    .with_data_type(self.data_type.clone());
                Arc::new(empty)
            }
            AdaptiveColumnState::Flat {
                group_indices,
                base,
                nulls,
            } => {
                let first_n: Vec<T::Native> = group_indices[..n]
                    .iter()
                    .map(|&idx| T::Native::from_index(idx as usize, *base))
                    .collect();
                group_indices.drain(0..n);
                let first_n_nulls = if NULLABLE { nulls.take_n(n) } else { None };
                Arc::new(
                    PrimitiveArray::<T>::new(ScalarBuffer::from(first_n), first_n_nulls)
                        .with_data_type(self.data_type.clone()),
                )
            }
            AdaptiveColumnState::Native {
                group_values,
                nulls,
            } => {
                let first_n = group_values.drain(0..n).collect::<Vec<_>>();
                let first_n_nulls = if NULLABLE { nulls.take_n(n) } else { None };
                Arc::new(
                    PrimitiveArray::<T>::new(ScalarBuffer::from(first_n), first_n_nulls)
                        .with_data_type(self.data_type.clone()),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{Int32Type, Int64Type};

    #[test]
    fn test_adaptive_starts_flat() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);
        let array = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));
        assert_eq!(builder.len(), 3);

        assert!(builder.equal_to(0, &array, 0)); // 10 == 10
        assert!(!builder.equal_to(0, &array, 1)); // 10 != 20
        assert!(builder.equal_to(2, &array, 2)); // 30 == 30
    }

    #[test]
    fn test_adaptive_starts_native_for_huge_range() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int64Type, false>::new(DataType::Int64);
        // First value sets base=MIN, second value MAX is > u32::MAX away
        let array = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1]).unwrap();
        // base=i64::MIN, i64::MAX - i64::MIN > u32::MAX → migrates
        assert!(matches!(builder.state, AdaptiveColumnState::Native { .. }));
    }

    #[test]
    fn test_adaptive_no_shift_needed() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);

        // First batch: base=100
        let arr1 = Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef;
        builder.vectorized_append(&arr1, &[0, 1]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        // Second batch: value 50 is below base but still works (wrapping sub)
        let arr2 = Arc::new(Int32Array::from(vec![50])) as ArrayRef;
        builder.vectorized_append(&arr2, &[0]).unwrap();
        // Still flat — no migration needed for i32 (always fits in u32)
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        // All values still compare correctly
        assert!(builder.equal_to(0, &arr1, 0)); // 100 == 100
        assert!(builder.equal_to(1, &arr1, 1)); // 200 == 200
        assert!(builder.equal_to(2, &arr2, 0)); // 50 == 50

        // Negative values also work (wrapping arithmetic)
        let neg = Arc::new(Int32Array::from(vec![-1000])) as ArrayRef;
        builder.vectorized_append(&neg, &[0]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));
        assert!(builder.equal_to(3, &neg, 0)); // -1000 == -1000
    }

    #[test]
    fn test_adaptive_i32_full_range() {
        // i32 should NEVER migrate since the full range fits in u32
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);

        let arr = Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])) as ArrayRef;
        builder.vectorized_append(&arr, &[0, 1]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        assert!(builder.equal_to(0, &arr, 0));
        assert!(builder.equal_to(1, &arr, 1));
        assert!(!builder.equal_to(0, &arr, 1));
    }

    #[test]
    fn test_adaptive_i64_migrates_on_wide_range() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int64Type, false>::new(DataType::Int64);

        let arr1 = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        builder.vectorized_append(&arr1, &[0, 1, 2]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        // Value far from base triggers migration
        let arr2 =
            Arc::new(Int64Array::from(vec![1, (u32::MAX as i64) + 100])) as ArrayRef;
        builder.vectorized_append(&arr2, &[1]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Native { .. }));

        // Old values preserved
        assert!(builder.equal_to(0, &arr1, 0)); // 1 == 1
        assert!(builder.equal_to(1, &arr1, 1)); // 2 == 2
        assert!(builder.equal_to(2, &arr1, 2)); // 3 == 3
        // New value
        let check = Arc::new(Int64Array::from(vec![(u32::MAX as i64) + 100])) as ArrayRef;
        assert!(builder.equal_to(3, &check, 0));
    }

    #[test]
    fn test_adaptive_i64_stays_flat_for_small_range() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int64Type, false>::new(DataType::Int64);

        // All values within u32 range of each other
        let arr =
            Arc::new(Int64Array::from(vec![1_000_000, 2_000_000, 3_000_000])) as ArrayRef;
        builder.vectorized_append(&arr, &[0, 1, 2]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        assert!(builder.equal_to(0, &arr, 0));
        assert!(builder.equal_to(1, &arr, 1));
        assert!(builder.equal_to(2, &arr, 2));
    }

    #[test]
    fn test_adaptive_nullable() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, true>::new(DataType::Int32);
        let array =
            Arc::new(Int32Array::from(vec![Some(10), None, Some(10), None])) as ArrayRef;

        builder.append_val(&array, 0).unwrap(); // group 0 = 10
        builder.append_val(&array, 1).unwrap(); // group 1 = NULL

        assert!(builder.equal_to(0, &array, 0)); // 10 == 10
        assert!(!builder.equal_to(0, &array, 1)); // 10 != NULL
        assert!(builder.equal_to(0, &array, 2)); // 10 == 10
        assert!(builder.equal_to(1, &array, 1)); // NULL == NULL
        assert!(builder.equal_to(1, &array, 3)); // NULL == NULL
        assert!(!builder.equal_to(1, &array, 0)); // NULL != 10
    }

    #[test]
    fn test_adaptive_build() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int64Type, false>::new(DataType::Int64);
        let array = Arc::new(Int64Array::from(vec![100, 150, 200])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        let result = Box::new(builder).build();
        let values = result.as_primitive::<Int64Type>();
        assert_eq!(values.values().as_ref(), &[100i64, 150, 200]);
    }

    #[test]
    fn test_adaptive_take_n() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);
        let array = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2, 3]).unwrap();
        assert_eq!(builder.len(), 4);

        let first_two = builder.take_n(2);
        let values = first_two.as_primitive::<Int32Type>();
        assert_eq!(values.values().as_ref(), &[10, 20]);
        assert_eq!(builder.len(), 2);

        // Remaining values still correct
        let input = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;
        assert!(builder.equal_to(0, &input, 0)); // 30 == 30
        assert!(builder.equal_to(1, &input, 1)); // 40 == 40
    }

    #[test]
    fn test_adaptive_vectorized_equal_to() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);
        let array = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();

        let mut results = vec![true; 3];
        builder.vectorized_equal_to(&[0, 1, 2], &array, &[0, 1, 2], &mut results);
        assert_eq!(results, vec![true, true, true]);

        let other = Arc::new(Int32Array::from(vec![10, 99, 30])) as ArrayRef;
        let mut results = vec![true; 3];
        builder.vectorized_equal_to(&[0, 1, 2], &other, &[0, 1, 2], &mut results);
        assert_eq!(results, vec![true, false, true]);
    }

    #[test]
    fn test_adaptive_all_nulls_initial() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, true>::new(DataType::Int32);

        let all_nulls = Arc::new(Int32Array::from(vec![None, None])) as ArrayRef;
        builder.vectorized_append(&all_nulls, &[0, 1]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));

        // Subsequent non-null values
        let with_values = Arc::new(Int32Array::from(vec![Some(5)])) as ArrayRef;
        builder.vectorized_append(&with_values, &[0]).unwrap();
        assert!(matches!(builder.state, AdaptiveColumnState::Flat { .. }));
        assert_eq!(builder.len(), 3);
    }

    #[test]
    fn test_adaptive_build_with_values_below_base() {
        let mut builder =
            PrimitiveGroupValueBuilderAdaptive::<Int32Type, false>::new(DataType::Int32);

        // base = 100
        let arr1 = Arc::new(Int32Array::from(vec![100, 150, 200])) as ArrayRef;
        builder.vectorized_append(&arr1, &[0, 1, 2]).unwrap();

        // value below base — no shift needed with fixed base
        let arr2 = Arc::new(Int32Array::from(vec![50])) as ArrayRef;
        builder.vectorized_append(&arr2, &[0]).unwrap();

        let result = Box::new(builder).build();
        let values = result.as_primitive::<Int32Type>();
        assert_eq!(values.values().as_ref(), &[100, 150, 200, 50]);
    }
}
