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

use crate::aggregates::group_values::multi_group_by::{nulls_equal_to, GroupColumn};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow::array::{ArrowNativeTypeOp, BooleanBufferBuilder};
use arrow::array::{cast::AsArray, Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use itertools::izip;
use std::iter;
use std::sync::Arc;
use arrow::util::bit_iterator::BitIterator;
use arrow::util::bit_util;

/// An implementation of [`GroupColumn`] for primitive values
///
/// Optimized to skip null buffer construction if the input is known to be non nullable
///
/// # Template parameters
///
/// `T`: the native Rust type that stores the data
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub struct PrimitiveGroupValueBuilder<T: ArrowPrimitiveType, const NULLABLE: bool> {
    data_type: DataType,
    group_values: Vec<T::Native>,
    nulls: MaybeNullBufferBuilder,
}

impl<T, const NULLABLE: bool> PrimitiveGroupValueBuilder<T, NULLABLE>
where
    T: ArrowPrimitiveType,
{
    /// Create a new `PrimitiveGroupValueBuilder`
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            group_values: vec![],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }

    pub fn vectorized_equal_to_non_nullable(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        assert!(!NULLABLE || (array.null_count() == 0 && !self.nulls.has_nulls()), "called with nullable input");
        let array = array.as_primitive::<T>();

        let (call_index, lhs_rows_leftover, rhs_rows_leftover) = run_on_auto_vectorization_simd_tuple::<usize, 8, _>(
            lhs_rows,
            rhs_rows,
            &mut |call_index, lhs_rows_idxs: &[usize; 8], rhs_rows_idxs: &[usize; 8]| {
                let bitmask = gather_and_compare_u8(&self.group_values, lhs_rows_idxs, array.values(), rhs_rows_idxs);

                for i in call_index..call_index + 8 {
                    equal_to_results[i] = equal_to_results[i] && (bitmask & (1 << (i - call_index)) != 0);
                }
            }
        );

        assert!(lhs_rows_leftover.len() < 8, "must have less than 8 left over");
        if !lhs_rows_leftover.is_empty() {
            let bitmask: u8 = lhs_rows_leftover.into_iter()
              .map(|&idx| unsafe { *self.group_values.get_unchecked(idx) })
              .zip(
                  rhs_rows_leftover
                    .into_iter()
                    .map(|&idx| unsafe { *array.values().get_unchecked(idx) })
              )
              .enumerate()
              .fold(
                  0,
                  |acc, (index, (l, r))| acc | (l.is_eq(r) as u8) << index
              );

            for i in call_index..call_index + lhs_rows_leftover.len() {
                equal_to_results[i] = equal_to_results[i] && (bitmask & (1 << (i - call_index)) != 0);
            }
        }
    }

    pub fn vectorized_equal_nullable(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut [bool],
    ) {
        let array = array.as_primitive::<T>();

        let (call_index, lhs_rows_leftover, rhs_rows_leftover) = run_on_auto_vectorization_simd_tuple::<usize, 8, _>(
            lhs_rows,
            rhs_rows,
            &mut |call_index, lhs_rows_idxs: &[usize; 8], rhs_rows_idxs: &[usize; 8]| {
                assert_eq!(lhs_rows_idxs.len(), 8, "must have 8");

                let equal_bitmask = gather_and_compare_u8(&self.group_values, lhs_rows_idxs, array.values(), rhs_rows_idxs);
                let block_equal_to_results = compare_with_nullability(
                    equal_bitmask,
                    get_validity_from_null_buffer_builder(
                        &self.nulls, lhs_rows_idxs.into_iter()
                    ),
                    get_validity_from_array(
                        &array, rhs_rows_idxs.into_iter()
                    )
                );

                for i in call_index..call_index + 8 {
                    equal_to_results[i] = equal_to_results[i] && (block_equal_to_results & (1 << (i - call_index)) != 0);
                }
            }
        );

        assert!(lhs_rows_leftover.len() < 8, "must have less than 8 left over");
        if !lhs_rows_leftover.is_empty() {
            let equal_bitmask: u8 = lhs_rows_leftover.into_iter()
              .map(|&idx| unsafe { *self.group_values.get_unchecked(idx) })
              .zip(
                  rhs_rows_leftover
                    .into_iter()
                    .map(|&idx| unsafe { *array.values().get_unchecked(idx) })
              )
              .enumerate()
              .fold(
                  0,
                  |acc, (index, (l, r))| acc | (l.is_eq(r) as u8) << index
              );

            let block_equal_to_results = compare_with_nullability(
                equal_bitmask,

                get_validity_from_null_buffer_builder(
                    &self.nulls, lhs_rows_leftover.into_iter()
                ),
                get_validity_from_array(
                    &array, rhs_rows_leftover.into_iter()
                )
            );

            for i in call_index..call_index + lhs_rows_leftover.len() {
                equal_to_results[i] = equal_to_results[i] && (block_equal_to_results & (1 << (i - call_index)) != 0);
            }
        }
    }
}

impl<T: ArrowPrimitiveType, const NULLABLE: bool> GroupColumn
    for PrimitiveGroupValueBuilder<T, NULLABLE>
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // Perf: skip null check (by short circuit) if input is not nullable
        if NULLABLE {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
            // Otherwise, we need to check their values
        }

        self.group_values[lhs_row] == array.as_primitive::<T>().value(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        // Perf: skip null check if input can't have nulls
        if NULLABLE {
            if array.is_null(row) {
                self.nulls.append(true);
                self.group_values.push(T::default_value());
            } else {
                self.nulls.append(false);
                self.group_values.push(array.as_primitive::<T>().value(row));
            }
        } else {
            self.group_values.push(array.as_primitive::<T>().value(row));
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
        if(!NULLABLE || (array.null_count() == 0 && !self.nulls.has_nulls())) {
            self.vectorized_equal_to_non_nullable(
                lhs_rows,
                array,
                rhs_rows,
                equal_to_results,
            );

            return;
        } else {
            self.vectorized_equal_nullable(
                lhs_rows,
                array,
                rhs_rows,
                equal_to_results,
            );
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let arr = array.as_primitive::<T>();

        let null_count = array.null_count();
        let num_rows = array.len();
        let all_null_or_non_null = if null_count == 0 {
            Some(true)
        } else if null_count == num_rows {
            Some(false)
        } else {
            None
        };

        match (NULLABLE, all_null_or_non_null) {
            (true, None) => {
                for &row in rows {
                    if array.is_null(row) {
                        self.nulls.append(true);
                        self.group_values.push(T::default_value());
                    } else {
                        self.nulls.append(false);
                        self.group_values.push(arr.value(row));
                    }
                }
            }

            (true, Some(true)) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.group_values.push(arr.value(row));
                }
            }

            (true, Some(false)) => {
                self.nulls.append_n(rows.len(), true);
                self.group_values
                    .extend(iter::repeat_n(T::default_value(), rows.len()));
            }

            (false, _) => {
                for &row in rows {
                    self.group_values.push(arr.value(row));
                }
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.group_values.len()
    }

    fn size(&self) -> usize {
        self.group_values.allocated_size() + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            data_type,
            group_values,
            nulls,
        } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        let arr = PrimitiveArray::<T>::new(ScalarBuffer::from(group_values), nulls);
        // Set timezone information for timestamp
        Arc::new(arr.with_data_type(data_type))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n = self.group_values.drain(0..n).collect::<Vec<_>>();

        let first_n_nulls = if NULLABLE { self.nulls.take_n(n) } else { None };

        Arc::new(
            PrimitiveArray::<T>::new(ScalarBuffer::from(first_n), first_n_nulls)
                .with_data_type(self.data_type.clone()),
        )
    }
}


pub fn compare_to_bitmask<T: ArrowNativeTypeOp>(a: [T; 64], b: [T; 64]) -> u64 {
    let mut bitmask = 0;
    for (index, (l, r)) in a.into_iter().zip(b.into_iter()).enumerate() {
        bitmask |= (l.is_eq(r) as u64) << index;
    }

    bitmask
}

pub fn gather_and_compare<const N: usize, T: ArrowNativeTypeOp>(a_array: &[T], a_idx: &[usize; 64], b_array: &[T], b_idx: &[usize; 64]) -> u64 {
    let a_idx_values = a_idx.map(|idx| unsafe { *a_array.get_unchecked(idx) });
    let b_idx_values = b_idx.map(|idx| unsafe { *b_array.get_unchecked(idx) });

    let bitmask = compare_to_bitmask(a_idx_values, b_idx_values);

    return bitmask;
}


pub fn compare_to_bitmask_u8<T: ArrowNativeTypeOp>(a: [T; 8], b: [T; 8]) -> u8 {
    let mut bitmask = 0;
    for (index, (l, r)) in a.into_iter().zip(b.into_iter()).enumerate() {
        bitmask |= (l.is_eq(r) as u8) << index;
    }

    bitmask
}

pub fn gather_and_compare_u8<T: ArrowNativeTypeOp>(a_array: &[T], a_idx: &[usize; 8], b_array: &[T], b_idx: &[usize; 8]) -> u8 {
    let a_idx_values = a_idx.map(|idx| unsafe { *a_array.get_unchecked(idx) });
    let b_idx_values = b_idx.map(|idx| unsafe { *b_array.get_unchecked(idx) });

    let bitmask = compare_to_bitmask_u8(a_idx_values, b_idx_values);

    return bitmask;
}


pub fn compare_to_bitmask_u8_bit(a: [bool; 8], b: [bool; 8]) -> u8 {
    let mut bitmask = 0;
    for (index, (l, r)) in a.into_iter().zip(b.into_iter()).enumerate() {
        bitmask |= ((l == r) as u8) << index;
    }

    bitmask
}

pub fn gather_and_compare_u8_nulls(a_array: &MaybeNullBufferBuilder, a_idx: &[usize; 8], b_array: &impl Array, b_idx: &[usize; 8]) -> u8 {
    let a_idx_values = a_idx.map(|idx| a_array.is_null(idx));
    let b_idx_values = b_idx.map(|idx| b_array.is_null(idx));

    let bitmask = compare_to_bitmask_u8_bit(a_idx_values, b_idx_values);

    return bitmask;
}

fn get_validity_from_null_buffer_builder<'a>(a_array: &MaybeNullBufferBuilder, a_idx: impl ExactSizeIterator<Item=&'a usize> + 'a) -> u8 {
    assert!(a_idx.len() <= 8, "only support up to 8 elements");
    if !a_array.has_nulls() {
        return 0xFF;
    }

    let mut bitmask = 0;
    for (index, &idx_in_array) in a_idx.into_iter().enumerate() {
        bitmask |= ((!a_array.is_null(idx_in_array)) as u8) << index;
    }

    bitmask
}

fn get_validity_from_array<'a>(a_array: &impl Array, a_idx: impl ExactSizeIterator<Item=&'a usize> + 'a) -> u8 {
    assert!(a_idx.len() <= 8, "only support up to 8 elements");
    
    if a_array.null_count() == 0 {
        return 0xFF;
    }

    let nulls = a_array.nulls().expect("must have nulls if null count > 0");

    let mut bitmask = 0;
    for (index, &idx_in_array) in a_idx.enumerate() {
        bitmask |= (nulls.is_valid(idx_in_array) as u8) << index;
    }

    bitmask
}

/// Given `a` and `b` arrays, and their nullability bitmasks `a_valid` and `b_valid` (set bits indicate non-null),
/// and the equality bitmask `equals` (set bits indicate equality),
/// return a combined bitmask where:
/// - If both `a` and `b` are null, the corresponding bit is set (1).
/// - If both `a` and `b` are non-null, the corresponding bit is set if they are equal (from `equals`).
/// - If one is null and the other is non-null, the corresponding bit is unset (0).
fn compare_with_nullability(equals: u8, a_valid: u8, b_valid: u8) -> u8 {
    // Both null: bit is set (1)
    let both_null = !a_valid & !b_valid;

    // Both valid: use the equals bit
    let both_valid = a_valid & b_valid;
    let both_valid_result = both_valid & equals;

    // Combine: both_null OR (both_valid AND equals)
    both_null | both_valid_result
}

fn run_on_auto_vectorization_simd_tuple<'a, T, const N: usize, SimdFn: FnMut(usize, &[T; N], &[T; N])>(slice_a: &'a [T], slice_b: &'a [T], run_on_simd: &mut SimdFn) -> (usize, &'a [T], &'a [T]) {
    assert_eq!(slice_a.len(), slice_b.len());
    let (simd_chunks_a, remainder_a) = slice_a.as_chunks::<N>();
    let (simd_chunks_b, remainder_b) = slice_b.as_chunks::<N>();
    let simd_chunks = simd_chunks_a.into_iter().zip(simd_chunks_b.into_iter());
    let mut i = 0;
    for (chunk_a, chunk_b) in simd_chunks {
        run_on_simd(i, chunk_a, chunk_b);
        i += 1;
    }

    (i, remainder_a, remainder_b)
}

trait ChunksExt {
    type Item: Sized;
    fn as_chunks<const N: usize>(&self) -> (&[[Self::Item; N]], &[Self::Item]);
}

impl<T: Sized> ChunksExt for [T] {
    type Item = T;
    fn as_chunks<const N: usize>(&self) -> (&[[T; N]], &[T]) {
        assert!(N != 0, "chunk size must be non-zero");
        let len_rounded_down = self.len() / N * N;
        // SAFETY: The rounded-down value is always the same or smaller than the
        // original length, and thus must be in-bounds of the slice.
        let (multiple_of_n, remainder) = unsafe { self.split_at_unchecked(len_rounded_down) };
        // SAFETY: We already panicked for zero, and ensured by construction
        // that the length of the subslice is a multiple of N.
        let array_slice = unsafe { multiple_of_n.as_chunks_unchecked() };
        (array_slice, remainder)
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
    use arrow::array::{ArrayRef, Int64Array, NullBufferBuilder};
    use arrow::datatypes::{DataType, Int64Type};

    use super::GroupColumn;

    #[test]
    fn test_nullable_primitive_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, true>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_nullable_primitive_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, true>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_nullable_primitive_equal_to_internal(append, equal_to);
    }

    fn test_nullable_primitive_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut PrimitiveGroupValueBuilder<Int64Type, true>, &ArrayRef, &[usize]),
        E: FnMut(
            &PrimitiveGroupValueBuilder<Int64Type, true>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - exist null, input not null
        //   - exist null, input null; values not equal
        //   - exist null, input null; values equal
        //   - exist not null, input null
        //   - exist not null, input not null; values not equal
        //   - exist not null, input not null; values equal

        // Define PrimitiveGroupValueBuilder
        let mut builder =
            PrimitiveGroupValueBuilder::<Int64Type, true>::new(DataType::Int64);
        let builder_array = Arc::new(Int64Array::from(vec![
            None,
            None,
            None,
            Some(1),
            Some(2),
            Some(3),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array
        let (_, values, _nulls) =
            Int64Array::from(vec![Some(1), Some(2), None, None, Some(1), Some(3)])
                .into_parts();

        // explicitly build a null buffer where one of the null values also happens to match
        let mut nulls = NullBufferBuilder::new(6);
        nulls.append_non_null();
        nulls.append_null(); // this sets Some(2) to null above
        nulls.append_null();
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_non_null();
        let input_array = Arc::new(Int64Array::new(values, nulls.finish())) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5],
            &input_array,
            &[0, 1, 2, 3, 4, 5],
            &mut equal_to_results,
        );

        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(!equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(equal_to_results[5]);
    }

    #[test]
    fn test_not_nullable_primitive_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_not_nullable_primitive_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_not_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Int64Type, false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to = |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            builder.vectorized_equal_to(
                lhs_rows,
                input_array,
                rhs_rows,
                equal_to_results,
            );
        };

        test_not_nullable_primitive_equal_to_internal(append, equal_to);
    }

    fn test_not_nullable_primitive_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut PrimitiveGroupValueBuilder<Int64Type, false>, &ArrayRef, &[usize]),
        E: FnMut(
            &PrimitiveGroupValueBuilder<Int64Type, false>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut Vec<bool>,
        ),
    {
        // Will cover such cases:
        //   - values equal
        //   - values not equal

        // Define PrimitiveGroupValueBuilder
        let mut builder =
            PrimitiveGroupValueBuilder::<Int64Type, false>::new(DataType::Int64);
        let builder_array =
            Arc::new(Int64Array::from(vec![Some(0), Some(1)])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1]);

        // Define input array
        let input_array = Arc::new(Int64Array::from(vec![Some(0), Some(2)])) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1],
            &input_array,
            &[0, 1],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(!equal_to_results[1]);
    }

    #[test]
    fn test_nullable_primitive_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder =
            PrimitiveGroupValueBuilder::<Int64Type, true>::new(DataType::Int64);

        // All nulls input array
        let all_nulls_input_array = Arc::new(Int64Array::from(vec![
            Option::<i64>::None,
            None,
            None,
            None,
            None,
        ])) as _;
        builder
            .vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4])
            .unwrap();

        let mut equal_to_results = vec![true; all_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);

        // All not nulls input array
        let all_not_nulls_input_array = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])) as _;
        builder
            .vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4])
            .unwrap();

        let mut equal_to_results = vec![true; all_not_nulls_input_array.len()];
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }
}
