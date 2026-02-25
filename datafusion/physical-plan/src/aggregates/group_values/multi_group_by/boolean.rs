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

use std::sync::Arc;

use crate::aggregates::group_values::multi_group_by::Nulls;
use crate::aggregates::group_values::multi_group_by::{GroupColumn, nulls_equal_to};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;
use arrow::array::{Array as _, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder};
use datafusion_common::Result;
use itertools::izip;

/// An implementation of [`GroupColumn`] for booleans
///
/// Optimized to skip null buffer construction if the input is known to be non nullable
///
/// # Template parameters
///
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub struct BooleanGroupValueBuilder<const NULLABLE: bool> {
    buffer: BooleanBufferBuilder,
    nulls: MaybeNullBufferBuilder,
}

impl<const NULLABLE: bool> BooleanGroupValueBuilder<NULLABLE> {
    /// Create a new `BooleanGroupValueBuilder`
    pub fn new() -> Self {
        Self {
            buffer: BooleanBufferBuilder::new(0),
            nulls: MaybeNullBufferBuilder::new(),
        }
    }
}

impl<const NULLABLE: bool> GroupColumn for BooleanGroupValueBuilder<NULLABLE> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        if NULLABLE {
            let exist_null = self.nulls.is_null(lhs_row);
            let input_null = array.is_null(rhs_row);
            if let Some(result) = nulls_equal_to(exist_null, input_null) {
                return result;
            }
        }

        self.buffer.get_bit(lhs_row) == array.as_boolean().value(rhs_row)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        if NULLABLE {
            if array.is_null(row) {
                self.nulls.append(true);
                self.buffer.append(bool::default());
            } else {
                self.nulls.append(false);
                self.buffer.append(array.as_boolean().value(row));
            }
        } else {
            self.buffer.append(array.as_boolean().value(row));
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
        let array = array.as_boolean();

        let iter = izip!(
            lhs_rows.iter(),
            rhs_rows.iter(),
            equal_to_results.iter_mut(),
        );

        for (&lhs_row, &rhs_row, equal_to_result) in iter {
            // Has found not equal to in previous column, don't need to check
            if !*equal_to_result {
                continue;
            }

            if NULLABLE {
                let exist_null = self.nulls.is_null(lhs_row);
                let input_null = array.is_null(rhs_row);
                if let Some(result) = nulls_equal_to(exist_null, input_null) {
                    *equal_to_result = result;
                    continue;
                }
            }

            *equal_to_result = self.buffer.get_bit(lhs_row) == array.value(rhs_row);
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        let arr = array.as_boolean();

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
                        self.buffer.append(bool::default());
                    } else {
                        self.nulls.append(false);
                        self.buffer.append(arr.value(row));
                    }
                }
            }

            (true, Nulls::None) => {
                self.nulls.append_n(rows.len(), false);
                for &row in rows {
                    self.buffer.append(arr.value(row));
                }
            }

            (true, Nulls::All) => {
                self.nulls.append_n(rows.len(), true);
                self.buffer.append_n(rows.len(), bool::default());
            }

            (false, _) => {
                for &row in rows {
                    self.buffer.append(arr.value(row));
                }
            }
        }

        Ok(())
    }

    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn size(&self) -> usize {
        self.buffer.capacity() / 8 + self.nulls.allocated_size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self { mut buffer, nulls } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        let arr = BooleanArray::new(buffer.finish(), nulls);

        Arc::new(arr)
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n_nulls = if NULLABLE { self.nulls.take_n(n) } else { None };

        let mut new_builder = BooleanBufferBuilder::new(self.buffer.len());
        new_builder.append_packed_range(n..self.buffer.len(), self.buffer.as_slice());
        std::mem::swap(&mut new_builder, &mut self.buffer);

        // take only first n values from the original builder
        new_builder.truncate(n);

        Arc::new(BooleanArray::new(new_builder.finish(), first_n_nulls))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::NullBufferBuilder;

    use super::*;

    #[test]
    fn test_nullable_boolean_equal_to() {
        let append = |builder: &mut BooleanGroupValueBuilder<true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to = |builder: &BooleanGroupValueBuilder<true>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_nullable_boolean_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut BooleanGroupValueBuilder<true>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to = |builder: &BooleanGroupValueBuilder<true>,
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

        test_nullable_boolean_equal_to_internal(append, equal_to);
    }

    fn test_nullable_boolean_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut BooleanGroupValueBuilder<true>, &ArrayRef, &[usize]),
        E: FnMut(
            &BooleanGroupValueBuilder<true>,
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
        let mut builder = BooleanGroupValueBuilder::<true>::new();
        let builder_array = Arc::new(BooleanArray::from(vec![
            None,
            None,
            None,
            Some(true),
            Some(false),
            Some(true),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);

        // Define input array
        let (values, _nulls) = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None,
            Some(true),
            Some(true),
        ])
        .into_parts();

        // explicitly build a null buffer where one of the null values also happens to match
        let mut nulls = NullBufferBuilder::new(6);
        nulls.append_non_null();
        nulls.append_null(); // this sets Some(false) to null above
        nulls.append_null();
        nulls.append_null();
        nulls.append_non_null();
        nulls.append_non_null();
        let input_array = Arc::new(BooleanArray::new(values, nulls.finish())) as ArrayRef;

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
        let append = |builder: &mut BooleanGroupValueBuilder<false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            for &index in append_rows {
                builder.append_val(builder_array, index).unwrap();
            }
        };

        let equal_to = |builder: &BooleanGroupValueBuilder<false>,
                        lhs_rows: &[usize],
                        input_array: &ArrayRef,
                        rhs_rows: &[usize],
                        equal_to_results: &mut Vec<bool>| {
            let iter = lhs_rows.iter().zip(rhs_rows.iter());
            for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
                equal_to_results[idx] = builder.equal_to(lhs_row, input_array, rhs_row);
            }
        };

        test_not_nullable_boolean_equal_to_internal(append, equal_to);
    }

    #[test]
    fn test_not_nullable_primitive_vectorized_equal_to() {
        let append = |builder: &mut BooleanGroupValueBuilder<false>,
                      builder_array: &ArrayRef,
                      append_rows: &[usize]| {
            builder
                .vectorized_append(builder_array, append_rows)
                .unwrap();
        };

        let equal_to = |builder: &BooleanGroupValueBuilder<false>,
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

        test_not_nullable_boolean_equal_to_internal(append, equal_to);
    }

    fn test_not_nullable_boolean_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
    where
        A: FnMut(&mut BooleanGroupValueBuilder<false>, &ArrayRef, &[usize]),
        E: FnMut(
            &BooleanGroupValueBuilder<false>,
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
        let mut builder = BooleanGroupValueBuilder::<false>::new();
        let builder_array = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
        ])) as ArrayRef;
        append(&mut builder, &builder_array, &[0, 1, 2, 3]);

        // Define input array
        let input_array = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(true),
            Some(true),
        ])) as ArrayRef;

        // Check
        let mut equal_to_results = vec![true; builder.len()];
        equal_to(
            &builder,
            &[0, 1, 2, 3],
            &input_array,
            &[0, 1, 2, 3],
            &mut equal_to_results,
        );

        assert!(equal_to_results[0]);
        assert!(!equal_to_results[1]);
        assert!(!equal_to_results[2]);
        assert!(equal_to_results[3]);
    }

    #[test]
    fn test_nullable_boolean_vectorized_operation_special_case() {
        // Test the special `all nulls` or `not nulls` input array case
        // for vectorized append and equal to

        let mut builder = BooleanGroupValueBuilder::<true>::new();

        // All nulls input array
        let all_nulls_input_array =
            Arc::new(BooleanArray::from(vec![None, None, None, None, None])) as _;
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
        let all_not_nulls_input_array = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
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
