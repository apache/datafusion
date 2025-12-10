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

use crate::aggregates::group_values::multi_group_by::helper::{
    combine_nullability_and_value_equal_bit_packed_u64, compare_fixed_nulls_to_packed,
    compare_fixed_raw_nulls_to_packed, compare_nulls_to_packed, CollectBool,
};
use crate::aggregates::group_values::multi_group_by::{
    nulls_equal_to, FixedBitPackedMutableBuffer, GroupColumn, Nulls,
};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow::array::{cast::AsArray, Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::array::{ArrowNativeTypeOp, BooleanArray};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use arrow::util::bit_util::{apply_bitwise_binary_op, apply_bitwise_unary_op};
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use itertools::izip;
use std::sync::Arc;
use std::{iter, u64};

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

    fn get_fixed_bit_packed_u64_for_eq_values(
        &self,
        length: usize,
        lhs_rows: &[usize; 64],
        rhs_rows: &[usize; 64],
        array_values: &ScalarBuffer<T::Native>,
    ) -> u64 {
        u64::collect_bool::<
            // Using rest true as we don't wanna change bits beyond num_rows
            true,
            _,
        >(length, |bit_idx| {
            let (lhs_row, rhs_row) = if cfg!(debug_assertions) {
                (lhs_rows[bit_idx], rhs_rows[bit_idx])
            } else {
                // SAFETY: indices are guaranteed to be in bounds
                unsafe {
                    (
                        *lhs_rows.get_unchecked(bit_idx),
                        *rhs_rows.get_unchecked(bit_idx),
                    )
                }
            };

            // Getting unchecked not only for bound checks but because the bound checks are
            // what prevents auto-vectorization
            let left = if cfg!(debug_assertions) {
                self.group_values[lhs_row]
            } else {
                // SAFETY: indices are guaranteed to be in bounds
                unsafe { *self.group_values.get_unchecked(lhs_row) }
            };
            let right = if cfg!(debug_assertions) {
                array_values[rhs_row]
            } else {
                // SAFETY: indices are guaranteed to be in bounds
                unsafe { *array_values.get_unchecked(rhs_row) }
            };

            // Always evaluate, to allow for auto-vectorization
            left.is_eq(right)
        })
    }

    // TODO - extract this function for other datatype impl
    pub fn inner_vectorized_equal<const CHECK_NULLABILITY: bool>(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut FixedBitPackedMutableBuffer,
    ) {
        if !CHECK_NULLABILITY {
            assert!(
                (array.null_count() == 0 && !self.nulls.might_have_nulls()),
                "CHECK_NULLABILITY is false for nullable called with nullable input"
            );
        }

        let array = array.as_primitive::<T>();
        let array_values = array.values();

        assert_eq!(lhs_rows.len(), rhs_rows.len());
        assert_eq!(lhs_rows.len(), equal_to_results.len());

        // TODO - skip to the first true bit in equal_to_results to avoid unnecessary work
        //        in iterating over unnecessary bits oe even get a slice of starting from first true bit to the last true bit

        // TODO - do not assume for byte aligned, added here just for POC
        let mut index = 0;
        let num_rows = lhs_rows.len();

        let self_nulls_slice = if CHECK_NULLABILITY {
            self.nulls.maybe_as_slice()
        } else {
            None
        };
        let array_nulls = if CHECK_NULLABILITY {
            array
                .nulls()
                .map(|nulls| (nulls.offset(), nulls.inner().values()))
        } else {
            None
        };

        let mut scrach_left_64: [usize; 64] = [0; 64];
        let mut scrach_right_64: [usize; 64] = [0; 64];
        apply_bitwise_unary_op(
            equal_to_results.0.as_slice_mut(),
            0,
            lhs_rows.len(),
            |eq| {
                // If already false, skip 64 items
                if eq == 0 {
                    index += 64;
                    return 0;
                }

                let length = num_rows - index;

                // Creating an array of size 64 to allow for optimization when building u64 bit packed from this
                let (lhs_rows_fixed, rhs_rows_fixed) = if length >= 64 {
                    (
                        lhs_rows[index..index + 64].try_into().unwrap(),
                        rhs_rows[index..index + 64].try_into().unwrap(),
                    )
                } else {
                    scrach_left_64[..length].copy_from_slice(&lhs_rows[index..]);
                    scrach_right_64[..length].copy_from_slice(&rhs_rows[index..]);

                    (&scrach_left_64, &scrach_right_64)
                };

                let (nullability_eq, both_valid) = if CHECK_NULLABILITY {
                    // TODO - rest here should be
                    compare_fixed_raw_nulls_to_packed(
                        length,
                        lhs_rows_fixed,
                        self_nulls_slice,
                        rhs_rows_fixed,
                        array_nulls,
                    )
                } else {
                    (
                        // nullability equal
                        u64::MAX,
                        // both valid
                        u64::MAX,
                    )
                };
                
                // If given `nullability_eq` and `both_valid` we can have all the data we need: 
                // eq | nullability_eq | both_valid | result
                // T  | T              | T          | F
                // T  | T              | F          | T
                // T  | F              | T          | <impossible>
                // T  | F              | F          | T
                // F  | T              | T          | T
                // F  | T              | F          | T
                // F  | F              | T          | <impossible>
                // F  | F              | F          | T
                if !(eq & nullability_eq & both_valid) == u64::MAX {
                    // eq | nullability_eq | both_valid | result
                    // T  | T              | T          | <impossible>
                    // T  | T              | F          | T
                    // T  | F              | T          | <impossible>
                    // T  | F              | F          | F
                    // F  | T              | T          | F
                    // F  | T              | F          | F
                    // F  | F              | T          | <impossible>
                    // F  | F              | F          | F
                    index += 64;
                    return eq & nullability_eq & !both_valid;
                }

                // TODO - we can maybe get only from the first set bit until the last set bit
                // and then update those gaps with false
                // TODO - make sure not to override bits after `length`
                let values_eq = self.get_fixed_bit_packed_u64_for_eq_values(
                    length,
                    lhs_rows_fixed,
                    rhs_rows_fixed,
                    array_values,
                );

                let result = if CHECK_NULLABILITY {
                    combine_nullability_and_value_equal_bit_packed_u64(
                        both_valid,
                        nullability_eq,
                        values_eq,
                    )
                } else {
                    values_eq
                };

                index += 64;
                eq & result
            },
        );
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

        self.group_values[lhs_row].is_eq(array.as_primitive::<T>().value(rhs_row))
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
        equal_to_results: &mut FixedBitPackedMutableBuffer,
    ) {
        if !NULLABLE || (array.null_count() == 0 && !self.nulls.might_have_nulls()) {
            self.inner_vectorized_equal::<false>(
                lhs_rows,
                array,
                rhs_rows,
                equal_to_results,
            );
        } else {
            self.inner_vectorized_equal::<true>(
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
                        self.group_values.push(T::default_value());
                    } else {
                        self.nulls.append(false);
                        self.group_values.push(arr.value(row));
                    }
                }
            }

            (true, Nulls::None) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.group_values.push(arr.value(row));
                }
            }

            (true, Nulls::All) => {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::GroupColumn;
    use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
    use crate::aggregates::group_values::multi_group_by::FixedBitPackedMutableBuffer;
    use arrow::array::{ArrayRef, Float32Array, Int64Array, NullBufferBuilder};
    use arrow::datatypes::{DataType, Float32Type, Int64Type};
    use itertools::Itertools;

    #[test]
    fn test_nullable_primitive_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Float32Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to =
            |builder: &PrimitiveGroupValueBuilder<Float32Type, true>,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut FixedBitPackedMutableBuffer| {
                let iter = lhs_rows.iter().zip(rhs_rows.iter());
                for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                    equal_to_results
                        .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
                }
            };

        test_nullable_primitive_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut PrimitiveGroupValueBuilder<Float32Type, true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to =
            |builder: &PrimitiveGroupValueBuilder<Float32Type, true>,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut FixedBitPackedMutableBuffer| {
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
        A: FnMut(&mut PrimitiveGroupValueBuilder<Float32Type, true>, &ArrayRef, &[usize]),
        E: FnMut(
            &PrimitiveGroupValueBuilder<Float32Type, true>,
            &[usize],
            &ArrayRef,
            &[usize],
            &mut FixedBitPackedMutableBuffer,
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
            PrimitiveGroupValueBuilder::<Float32Type, true>::new(DataType::Float32);
        let builder_array = Arc::new(Float32Array::from(vec![
            None,
            None,
            None,
            Some(1.0),
            Some(2.0),
            Some(f32::NAN),
            Some(3.0),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5, 6]);

        // Define input array
        let (_, values, _nulls) = Float32Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            Some(1.0),
            None,
            Some(f32::NAN),
            None,
        ])
        .into_parts();

        // explicitly build a null buffer where one of the null values also happens to match
        let mut nulls = NullBufferBuilder::new(6);
        nulls.append_non_null();
        nulls.append_null(); // this sets Some(2) to null above
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_null();
        let input_array = Arc::new(Float32Array::new(values, nulls.finish())) as ArrayRef;

        // Check
        let mut equal_to_results = FixedBitPackedMutableBuffer::new_set(builder.len());

        equal_to(
            &builder,
            &[0, 1, 2, 3, 4, 5, 6],
            &input_array,
            &[0, 1, 2, 3, 4, 5, 6],
            &mut equal_to_results,
        );

        let equal_to_results: Vec<bool> = equal_to_results.into();
        assert!(!equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(!equal_to_results[4]);
        assert!(equal_to_results[5]);
        assert!(!equal_to_results[6]);
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

        let equal_to =
            |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut FixedBitPackedMutableBuffer| {
                let iter = lhs_rows.iter().zip(rhs_rows.iter());
                for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                    equal_to_results
                        .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
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

        let equal_to =
            |builder: &PrimitiveGroupValueBuilder<Int64Type, false>,
             lhs_rows: &[usize],
             input_array: &ArrayRef,
             rhs_rows: &[usize],
             equal_to_results: &mut FixedBitPackedMutableBuffer| {
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
            &mut FixedBitPackedMutableBuffer,
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
        let mut equal_to_results = FixedBitPackedMutableBuffer::new_set(builder.len());
        equal_to(
            &builder,
            &[0, 1],
            &input_array,
            &[0, 1],
            &mut equal_to_results,
        );

        let equal_to_results: Vec<bool> = equal_to_results.into();
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

        let mut equal_to_results =
            FixedBitPackedMutableBuffer::new_set(all_nulls_input_array.len());
        builder.vectorized_equal_to(
            &[0, 1, 2, 3, 4],
            &all_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        let equal_to_results: Vec<bool> = equal_to_results.into();
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

        let mut equal_to_results = FixedBitPackedMutableBuffer::new_set(all_not_nulls_input_array.len());
        builder.vectorized_equal_to(
            &[5, 6, 7, 8, 9],
            &all_not_nulls_input_array,
            &[0, 1, 2, 3, 4],
            &mut equal_to_results,
        );

        let equal_to_results: Vec<bool> = equal_to_results.into();

        assert!(equal_to_results[0]);
        assert!(equal_to_results[1]);
        assert!(equal_to_results[2]);
        assert!(equal_to_results[3]);
        assert!(equal_to_results[4]);
    }
}
