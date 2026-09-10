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

use crate::aggregates_blocked::group_values::multi_group_by::Nulls;
use crate::aggregates_blocked::group_values::multi_group_by::{BlockedGroupColumn, nulls_equal_to};
use crate::aggregates_blocked::group_values::null_builder::NullBufferBuilderExt;
use arrow::array::{
    Array as _, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, NullBufferBuilder,
};
use datafusion_common::Result;
use datafusion_expr_common::blocked_helpers::{BlockedBooleanBuilder, BlockedNullsBuilder};
use datafusion_expr_common::groups_accumulator::{BlockedGroupSelection, BlocksIndex};

/// An implementation of [`GroupColumn`] for booleans
///
/// Optimized to skip null buffer construction if the input is known to be non nullable
///
/// # Template parameters
///
/// `NULLABLE`: if the data can contain any nulls
#[derive(Debug)]
pub struct BooleanGroupValueBuilder<const IS_FIXED_BLOCK: bool, const NULLABLE: bool> {
    buffer: BlockedBooleanBuilder<IS_FIXED_BLOCK>,
    nulls: BlockedNullsBuilder<IS_FIXED_BLOCK>,
}

impl<const IS_FIXED_BLOCK: bool, const NULLABLE: bool> BooleanGroupValueBuilder<IS_FIXED_BLOCK, NULLABLE> {
    /// Create a new `BooleanGroupValueBuilder`
    pub fn new(block_size: usize) -> Self {
        Self {
            buffer: BlockedBooleanBuilder::new(block_size),
            nulls: BlockedNullsBuilder::new(block_size),
        }
    }
}

impl<const IS_FIXED_BLOCK: bool, const NULLABLE: bool> BlockedGroupColumn<IS_FIXED_BLOCK> for BooleanGroupValueBuilder<IS_FIXED_BLOCK, NULLABLE> {
    fn batch_size(&self) -> usize {
        self.buffer.block_size()
    }

    fn equal_to(&self, lhs_row: BlocksIndex, array: &ArrayRef, rhs_row: usize) -> bool {
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
                self.nulls.push_null();
                self.buffer.append(bool::default());
            } else {
                self.nulls.push_non_null();
                self.buffer.append(array.as_boolean().value(row));
            }
        } else {
            self.buffer.append(array.as_boolean().value(row));
        }

        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[BlocksIndex],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        let array = array.as_boolean();

        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            if !equal_to_results.get_bit(idx) {
                continue;
            }

            if NULLABLE {
                let exist_null = self.nulls.is_null(lhs_row);
                let input_null = array.is_null(rhs_row);
                if let Some(result) = nulls_equal_to(exist_null, input_null) {
                    if !result {
                        equal_to_results.set_bit(idx, false);
                    }
                    continue;
                }
            }

            if self.buffer.get_bit(lhs_row) != array.value(rhs_row) {
                equal_to_results.set_bit(idx, false);
            }
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
                        self.nulls.push_null();
                        self.buffer.append(bool::default());
                    } else {
                        self.nulls.push_non_null();
                        self.buffer.append(arr.value(row));
                    }
                }
            }

            (true, Nulls::None) => {
                self.nulls.push_n_non_nulls(rows.len());
                for &row in rows {
                    self.buffer.append(arr.value(row));
                }
            }

            (true, Nulls::All) => {
                self.nulls.push_n_nulls(rows.len());
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
        self.buffer.allocated_size() + self.nulls.allocated_size()
    }

    fn values_preserving(&self, selection: BlockedGroupSelection<'_>) -> Result<ArrayRef> {
        selection.validate_num_groups(self.buffer.len())?;
        let mut values = BooleanBufferBuilder::new(selection.len());
        for index in selection.iter() {
            values.append(self.buffer.get_bit(index));
        }
        let nulls = if NULLABLE {
            self.nulls.build_preserving(selection)?
        } else {
            None
        };
        Ok(Arc::new(BooleanArray::new(values.finish(), nulls)))
    }

    fn take_all(self: Box<Self>) -> Vec<ArrayRef> {
        let Self { mut buffer, mut nulls } = *self;

        if NULLABLE {
            let buffers = buffer.take_all();
            let nulls = nulls.take_all();

            buffers.into_iter().zip(nulls.into_iter()).map(|(buffer, nulls)| {
                Arc::new(BooleanArray::new(buffer, nulls)) as ArrayRef
            }).collect()
        } else {
            assert_eq!(nulls.len(), 0);
            let buffers = buffer.take_all();

            buffers.into_iter().map(|buffer| {
                Arc::new(BooleanArray::new(buffer, None)) as ArrayRef
            }).collect()
        }
    }

    fn take_next_block(&mut self) -> Option<ArrayRef> {
        let values = self.buffer.take_block();
        let nulls = if NULLABLE { self.nulls.take_block() } else { values.as_ref().map(|_| None) };

        match (values, nulls) {
            (Some(values), Some(nulls)) => {
                Some(Arc::new(BooleanArray::new(values, nulls)))
            }
            (None, None) => None,
            (v, n) => unreachable!("either both nulls and buffer should be none or neither, values is some: {}, nulls is some {}", v.is_some(), n.is_some())
        }
    }

    fn take_n(&mut self, n: usize,
              // adjusted_block_size_iter: Option<Box<dyn ClonableIter<Item=usize>>>,
    ) -> ArrayRef {
        let first_n_nulls = if NULLABLE { self.nulls.take_n(
            n,
            // adjusted_block_size_iter.clone()
            None::<std::iter::Empty<_>>,
        ) } else { None };
        let first_n_values = self.buffer.take_n(
            n,
            // adjusted_block_size_iter
            None::<std::iter::Empty<_>>,
        );

        Arc::new(BooleanArray::new(first_n_values, first_n_nulls))
    }

    fn start_new_block(&mut self) {
        self.buffer.start_new_block();
        if NULLABLE {
            self.nulls.start_new_block();
        }

    }
}
//
// #[cfg(test)]
// mod tests {
//     use arrow::array::{BooleanBufferBuilder, NullBufferBuilder};
//
//     use super::*;
//
//     fn make_true_buffer(n: usize) -> BooleanBufferBuilder {
//         let mut buf = BooleanBufferBuilder::new(n);
//         buf.append_n(n, true);
//         buf
//     }
//
//     fn to_vec(buf: &BooleanBufferBuilder) -> Vec<bool> {
//         (0..buf.len()).map(|i| buf.get_bit(i)).collect()
//     }
//
//     #[test]
//     fn test_nullable_boolean_equal_to() {
//         let append = |builder: &mut BooleanGroupValueBuilder<true>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             for &index in append_rows {
//                 builder.append_val(builder_array, index).unwrap();
//             }
//         };
//
//         let equal_to =
//             |builder: &BooleanGroupValueBuilder<true>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 let iter = lhs_rows.iter().zip(rhs_rows.iter());
//                 for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
//                     equal_to_results
//                         .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
//                 }
//             };
//
//         test_nullable_boolean_equal_to_internal(append, equal_to);
//     }
//
//     #[test]
//     fn test_nullable_primitive_vectorized_equal_to() {
//         let append = |builder: &mut BooleanGroupValueBuilder<true>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             builder
//                 .vectorized_append(builder_array, append_rows)
//                 .unwrap();
//         };
//
//         let equal_to =
//             |builder: &BooleanGroupValueBuilder<true>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 builder.vectorized_equal_to(
//                     lhs_rows,
//                     input_array,
//                     rhs_rows,
//                     equal_to_results,
//                 );
//             };
//
//         test_nullable_boolean_equal_to_internal(append, equal_to);
//     }
//
//     fn test_nullable_boolean_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
//     where
//         A: FnMut(&mut BooleanGroupValueBuilder<true>, &ArrayRef, &[usize]),
//         E: FnMut(
//             &BooleanGroupValueBuilder<true>,
//             &[usize],
//             &ArrayRef,
//             &[usize],
//             &mut BooleanBufferBuilder,
//         ),
//     {
//         // Will cover such cases:
//         //   - exist null, input not null
//         //   - exist null, input null; values not equal
//         //   - exist null, input null; values equal
//         //   - exist not null, input null
//         //   - exist not null, input not null; values not equal
//         //   - exist not null, input not null; values equal
//
//         // Define BooleanGroupValueBuilder
//         let mut builder = BooleanGroupValueBuilder::<true>::new();
//         let builder_array = Arc::new(BooleanArray::from(vec![
//             None,
//             None,
//             None,
//             Some(true),
//             Some(false),
//             Some(true),
//         ])) as ArrayRef;
//         append(&mut builder, &builder_array, &[0, 1, 2, 3, 4, 5]);
//
//         // Define input array
//         let (values, _nulls) = BooleanArray::from(vec![
//             Some(true),
//             Some(false),
//             None,
//             None,
//             Some(true),
//             Some(true),
//         ])
//         .into_parts();
//
//         // explicitly build a null buffer where one of the null values also happens to match
//         let mut nulls = NullBufferBuilder::new(6);
//         nulls.append_non_null();
//         nulls.append_null();
//         nulls.append_null();
//         nulls.append_null();
//         nulls.append_non_null();
//         nulls.append_non_null();
//         let input_array = Arc::new(BooleanArray::new(values, nulls.finish())) as ArrayRef;
//
//         // Check
//         let mut equal_to_results = make_true_buffer(builder.len());
//         equal_to(
//             &builder,
//             &[0, 1, 2, 3, 4, 5],
//             &input_array,
//             &[0, 1, 2, 3, 4, 5],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(!results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(!results[3]);
//         assert!(!results[4]);
//         assert!(results[5]);
//     }
//
//     #[test]
//     fn test_not_nullable_primitive_equal_to() {
//         let append = |builder: &mut BooleanGroupValueBuilder<false>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             for &index in append_rows {
//                 builder.append_val(builder_array, index).unwrap();
//             }
//         };
//
//         let equal_to =
//             |builder: &BooleanGroupValueBuilder<false>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 let iter = lhs_rows.iter().zip(rhs_rows.iter());
//                 for (idx, (&lhs_row, &rhs_row)) in iter.enumerate() {
//                     equal_to_results
//                         .set_bit(idx, builder.equal_to(lhs_row, input_array, rhs_row));
//                 }
//             };
//
//         test_not_nullable_boolean_equal_to_internal(append, equal_to);
//     }
//
//     #[test]
//     fn test_not_nullable_primitive_vectorized_equal_to() {
//         let append = |builder: &mut BooleanGroupValueBuilder<false>,
//                       builder_array: &ArrayRef,
//                       append_rows: &[usize]| {
//             builder
//                 .vectorized_append(builder_array, append_rows)
//                 .unwrap();
//         };
//
//         let equal_to =
//             |builder: &BooleanGroupValueBuilder<false>,
//              lhs_rows: &[usize],
//              input_array: &ArrayRef,
//              rhs_rows: &[usize],
//              equal_to_results: &mut BooleanBufferBuilder| {
//                 builder.vectorized_equal_to(
//                     lhs_rows,
//                     input_array,
//                     rhs_rows,
//                     equal_to_results,
//                 );
//             };
//
//         test_not_nullable_boolean_equal_to_internal(append, equal_to);
//     }
//
//     fn test_not_nullable_boolean_equal_to_internal<A, E>(mut append: A, mut equal_to: E)
//     where
//         A: FnMut(&mut BooleanGroupValueBuilder<false>, &ArrayRef, &[usize]),
//         E: FnMut(
//             &BooleanGroupValueBuilder<false>,
//             &[usize],
//             &ArrayRef,
//             &[usize],
//             &mut BooleanBufferBuilder,
//         ),
//     {
//         // Will cover such cases:
//         //   - values equal
//         //   - values not equal
//
//         // Define BooleanGroupValueBuilder
//         let mut builder = BooleanGroupValueBuilder::<false>::new();
//         let builder_array = Arc::new(BooleanArray::from(vec![
//             Some(false),
//             Some(true),
//             Some(false),
//             Some(true),
//         ])) as ArrayRef;
//         append(&mut builder, &builder_array, &[0, 1, 2, 3]);
//
//         // Define input array
//         let input_array = Arc::new(BooleanArray::from(vec![
//             Some(false),
//             Some(false),
//             Some(true),
//             Some(true),
//         ])) as ArrayRef;
//
//         // Check
//         let mut equal_to_results = make_true_buffer(builder.len());
//         equal_to(
//             &builder,
//             &[0, 1, 2, 3],
//             &input_array,
//             &[0, 1, 2, 3],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(results[0]);
//         assert!(!results[1]);
//         assert!(!results[2]);
//         assert!(results[3]);
//     }
//
//     #[test]
//     fn test_nullable_boolean_vectorized_operation_special_case() {
//         // Test the special `all nulls` or `not nulls` input array case
//         // for vectorized append and equal to
//
//         let mut builder = BooleanGroupValueBuilder::<true>::new();
//
//         // All nulls input array
//         let all_nulls_input_array =
//             Arc::new(BooleanArray::from(vec![None, None, None, None, None])) as _;
//         builder
//             .vectorized_append(&all_nulls_input_array, &[0, 1, 2, 3, 4])
//             .unwrap();
//
//         let mut equal_to_results = make_true_buffer(all_nulls_input_array.len());
//         builder.vectorized_equal_to(
//             &[0, 1, 2, 3, 4],
//             &all_nulls_input_array,
//             &[0, 1, 2, 3, 4],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(results[3]);
//         assert!(results[4]);
//
//         // All not nulls input array
//         let all_not_nulls_input_array = Arc::new(BooleanArray::from(vec![
//             Some(false),
//             Some(true),
//             Some(false),
//             Some(true),
//             Some(true),
//         ])) as _;
//         builder
//             .vectorized_append(&all_not_nulls_input_array, &[0, 1, 2, 3, 4])
//             .unwrap();
//
//         let mut equal_to_results = make_true_buffer(all_not_nulls_input_array.len());
//         builder.vectorized_equal_to(
//             &[5, 6, 7, 8, 9],
//             &all_not_nulls_input_array,
//             &[0, 1, 2, 3, 4],
//             &mut equal_to_results,
//         );
//         let results = to_vec(&equal_to_results);
//
//         assert!(results[0]);
//         assert!(results[1]);
//         assert!(results[2]);
//         assert!(results[3]);
//         assert!(results[4]);
//     }
// }
