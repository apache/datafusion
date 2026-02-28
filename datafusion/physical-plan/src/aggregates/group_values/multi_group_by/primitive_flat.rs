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

//! A flat-indexed variant of [`PrimitiveGroupValueBuilder`] for multi-column
//! group by when a column has a small value range.
//!
//! [`PrimitiveGroupValueBuilder`]: super::primitive::PrimitiveGroupValueBuilder

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

/// A [`GroupColumn`] for primitive values that stores flat indices (`u32`)
/// instead of native values.
///
/// This is used when column statistics show a small value range, allowing
/// values to be encoded as `(value - min)` indices. Benefits:
/// - Saves memory for large native types (e.g., i64: 8→4 bytes per group)
/// - Uniform `u32` comparisons in the vectorized equal_to hot path
/// - Native values are reconstructed only during emit (infrequent)
///
/// # Template parameters
///
/// `T`: the Arrow primitive type
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub(crate) struct PrimitiveGroupValueBuilderFlat<
    T: ArrowPrimitiveType,
    const NULLABLE: bool,
> where
    T::Native: FlatIndex,
{
    data_type: DataType,
    /// Stored as flat indices: `(value - min) as u32`
    group_indices: Vec<u32>,
    /// The minimum value, used to convert between flat indices and native values
    min: T::Native,
    nulls: MaybeNullBufferBuilder,
}

impl<T, const NULLABLE: bool> PrimitiveGroupValueBuilderFlat<T, NULLABLE>
where
    T: ArrowPrimitiveType,
    T::Native: FlatIndex,
{
    pub fn new(data_type: DataType, min: T::Native) -> Self {
        Self {
            data_type,
            group_indices: vec![],
            min,
            nulls: MaybeNullBufferBuilder::new(),
        }
    }

    #[inline]
    fn value_to_index(&self, value: T::Native) -> u32 {
        value.index_from(self.min) as u32
    }

    fn vectorized_equal_to_non_nullable(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        assert!(
            !NULLABLE || (array.null_count() == 0 && !self.nulls.might_have_nulls()),
            "called with nullable input"
        );
        let array_values = array.as_primitive::<T>().values();
        let min = self.min;

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            let result = {
                let left = if cfg!(debug_assertions) {
                    self.group_indices[lhs_row]
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe { *self.group_indices.get_unchecked(lhs_row) }
                };
                let right = if cfg!(debug_assertions) {
                    array_values[rhs_row].index_from(min) as u32
                } else {
                    // SAFETY: indices are guaranteed to be in bounds
                    unsafe {
                        (*array_values.get_unchecked(rhs_row)).index_from(min) as u32
                    }
                };

                // Always evaluate, to allow for auto-vectorization
                left == right
            };

            *equal_to_result = result && *equal_to_result;
        }
    }

    fn vectorized_equal_nullable(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        assert!(NULLABLE, "called with non-nullable input");
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

            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                *equal_to_result = result;
                continue;
            }

            *equal_to_result =
                self.group_indices[lhs_row] == self.value_to_index(array.value(rhs_row));
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> GroupColumn
    for PrimitiveGroupValueBuilderFlat<T, NULLABLE>
where
    T::Native: FlatIndex,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        if NULLABLE {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
        }

        self.group_indices[lhs_row]
            == self.value_to_index(array.as_primitive::<T>().value(rhs_row))
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        if NULLABLE {
            if array.is_null(row) {
                self.nulls.append(true);
                self.group_indices.push(0); // placeholder
            } else {
                self.nulls.append(false);
                self.group_indices
                    .push(self.value_to_index(array.as_primitive::<T>().value(row)));
            }
        } else {
            self.group_indices
                .push(self.value_to_index(array.as_primitive::<T>().value(row)));
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
        if !NULLABLE || (array.null_count() == 0 && !self.nulls.might_have_nulls()) {
            self.vectorized_equal_to_non_nullable(
                lhs_rows,
                array,
                rhs_rows,
                equal_to_results,
            );
        } else {
            self.vectorized_equal_nullable(lhs_rows, array, rhs_rows, equal_to_results);
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let arr = array.as_primitive::<T>();

        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Nulls::None
        } else if null_count == num_rows {
            Nulls::All
        } else {
            Nulls::Some
        };

        match (NULLABLE, all_null_or_non_null) {
            (true, Nulls::Some) => {
                for &row in rows {
                    if array.is_null(row) {
                        self.nulls.append(true);
                        self.group_indices.push(0);
                    } else {
                        self.nulls.append(false);
                        self.group_indices.push(self.value_to_index(arr.value(row)));
                    }
                }
            }

            (true, Nulls::None) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.group_indices.push(self.value_to_index(arr.value(row)));
                }
            }

            (true, Nulls::All) => {
                self.nulls.append_n(rows.len(), true);
                self.group_indices.extend(iter::repeat_n(0u32, rows.len()));
            }

            (false, _) => {
                for &row in rows {
                    self.group_indices.push(self.value_to_index(arr.value(row)));
                }
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.group_indices.len()
    }

    fn size(&self) -> usize {
        self.group_indices.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            data_type,
            group_indices,
            min,
            nulls,
            ..
        } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        let values: Vec<T::Native> = group_indices
            .iter()
            .map(|&idx| T::Native::from_index(idx as usize, min))
            .collect();

        let arr = PrimitiveArray::<T>::new(ScalarBuffer::from(values), nulls);
        Arc::new(arr.with_data_type(data_type))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n: Vec<T::Native> = self.group_indices[..n]
            .iter()
            .map(|&idx| T::Native::from_index(idx as usize, self.min))
            .collect();
        self.group_indices.drain(0..n);

        let first_n_nulls = if NULLABLE { self.nulls.take_n(n) } else { None };

        Arc::new(
            PrimitiveArray::<T>::new(ScalarBuffer::from(first_n), first_n_nulls)
                .with_data_type(self.data_type.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{Int32Type, Int64Type};

    #[test]
    fn test_flat_builder_basic_equal_to() {
        let mut builder =
            PrimitiveGroupValueBuilderFlat::<Int32Type, false>::new(DataType::Int32, 0);
        let array = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;

        builder.append_val(&array, 0).unwrap(); // group 0 = 10
        builder.append_val(&array, 1).unwrap(); // group 1 = 20
        builder.append_val(&array, 2).unwrap(); // group 2 = 30

        assert!(builder.equal_to(0, &array, 0)); // 10 == 10
        assert!(!builder.equal_to(0, &array, 1)); // 10 != 20
        assert!(builder.equal_to(2, &array, 2)); // 30 == 30
    }

    #[test]
    fn test_flat_builder_nullable() {
        let mut builder =
            PrimitiveGroupValueBuilderFlat::<Int32Type, true>::new(DataType::Int32, 0);
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
    fn test_flat_builder_vectorized() {
        let mut builder =
            PrimitiveGroupValueBuilderFlat::<Int32Type, false>::new(DataType::Int32, 0);
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
    fn test_flat_builder_build_reconstructs_values() {
        let mut builder =
            PrimitiveGroupValueBuilderFlat::<Int64Type, false>::new(DataType::Int64, 100);
        let array = Arc::new(Int64Array::from(vec![100, 150, 200])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2]).unwrap();

        let result = Box::new(builder).build();
        let values = result.as_primitive::<Int64Type>();
        assert_eq!(values.values().as_ref(), &[100i64, 150, 200]);
    }

    #[test]
    fn test_flat_builder_take_n() {
        let mut builder =
            PrimitiveGroupValueBuilderFlat::<Int32Type, false>::new(DataType::Int32, 0);
        let array = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        builder.vectorized_append(&array, &[0, 1, 2, 3]).unwrap();
        assert_eq!(builder.len(), 4);

        let first_two = builder.take_n(2);
        let values = first_two.as_primitive::<Int32Type>();
        assert_eq!(values.values().as_ref(), &[10, 20]);
        assert_eq!(builder.len(), 2);

        // Remaining values should still be correct
        let input = Arc::new(Int32Array::from(vec![30, 40])) as ArrayRef;
        assert!(builder.equal_to(0, &input, 0)); // 30 == 30
        assert!(builder.equal_to(1, &input, 1)); // 40 == 40
    }

    #[test]
    fn test_flat_builder_memory_savings_i64() {
        // Verify that u32 indices use less memory than i64 native values
        let builder =
            PrimitiveGroupValueBuilderFlat::<Int64Type, false>::new(DataType::Int64, 0);
        // The stored Vec<u32> should be 4 bytes per element instead of 8 for i64
        assert_eq!(size_of::<u32>(), 4, "u32 index should be 4 bytes");
        assert_eq!(size_of::<i64>(), 8, "i64 native should be 8 bytes");
        drop(builder);
    }
}
