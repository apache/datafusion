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

//! Dedicated implementation of `GroupsAccumulator` for `array_agg`

use std::iter::repeat_n;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, GenericListArray, OffsetSizeTrait};
use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels;
use arrow::datatypes::{ArrowNativeType, Field, FieldRef};
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

#[derive(Clone)]
pub struct AggGroupAccumulator<T: OffsetSizeTrait + Clone> {
    _virtual: PhantomData<T>,

    inner_field: FieldRef,
    // invoke of  update_batch and [1,2,3] [4,5,6]
    stacked_batches: Vec<ArrayRef>,
    // address items of each group within the stacked_batches
    // this is maintained to perform kernel::interleave
    stacked_group_indices: Vec<(
        /*group_number*/ usize,
        /*array_number*/ usize,
        /*offset_in_array*/ usize,
    )>,
    stacked_batches_size: usize,
    indice_sorted: bool,
    // max group seen so far, 1 based offset
    // after the call to `evaluate` this needs to be offseted by the number of
    // group consumed
    // zero means there is no state accumulated
    max_seen_group: usize,
    groups_consumed: usize,
}

impl<T: OffsetSizeTrait + Clone> AggGroupAccumulator<T> {
    pub fn new(inner_field: FieldRef) -> Self {
        Self {
            _virtual: PhantomData,
            inner_field,
            stacked_batches: vec![],
            stacked_batches_size: 0,
            stacked_group_indices: vec![],
            indice_sorted: false,
            max_seen_group: 0,
            groups_consumed: 0,
        }
    }
    fn consume_stacked_batches(
        &mut self,
        emit_to: EmitTo,
    ) -> Result<GenericListArray<T>> {
        // this is inclusive, zero-based
        let stop_at_group = match emit_to {
            EmitTo::All => self.max_seen_group - self.groups_consumed - 1,
            EmitTo::First(groups_taken) => groups_taken - 1,
        };
        // this can still happen, if all the groups have not been consumed
        // but internally they are filtered out (by filter option)
        // because stacked_group_indices only contains valid entry
        if self.stacked_group_indices.is_empty() {
            let offsets = OffsetBuffer::from_lengths(vec![0; stop_at_group + 1]);
            return Ok(GenericListArray::<T>::new(
                self.inner_field.clone(),
                offsets,
                new_empty_array(self.inner_field.data_type()),
                None,
            ));
        }

        // in the case of continuous calls to function `evaluate` happen,
        // (without any interleaving calls to `merge_batch` or `update_batch`)
        // the first call will basically sort everything beforehand
        // so the second one does not need to
        if !self.indice_sorted {
            self.indice_sorted = true;
            self.stacked_group_indices.sort_by_key(|a| {
                // TODO: array_agg with distinct and custom order can be implemented here
                a.0
            });
        }

        let mut current_group = 0;

        let mut group_windows = Vec::<T>::with_capacity(stop_at_group + 1);
        group_windows.push(T::zero());
        let mut split_offset = None;
        self.groups_consumed += stop_at_group + 1;

        // TODO: init with a good cap if possible via some stats during accumulation phase
        let mut interleave_offsets = vec![];
        for (offset, (group_index, array_number, offset_in_array)) in
            self.stacked_group_indices.iter().enumerate()
        {
            // stop consuming from this offset
            if *group_index > stop_at_group {
                split_offset = Some(offset);
                break;
            }
            if *group_index > current_group {
                // there can be interleaving empty group indices
                // i.e if the indices are like [0 1 1 4]
                // then the group_windows should be [0 1 3 3 3 4]
                group_windows.push(T::usize_as(offset));
                for _ in current_group + 1..*group_index {
                    group_windows.push(T::usize_as(offset));
                }
                current_group = *group_index;
            }
            interleave_offsets.push((*array_number, *offset_in_array));
        }

        if let Some(split_offset) = split_offset {
            let mut tail_part = self.stacked_group_indices.split_off(split_offset);
            mem::swap(&mut self.stacked_group_indices, &mut tail_part);
            for item in self.stacked_group_indices.iter_mut() {
                // shift down the number of group being taken
                item.0 -= stop_at_group + 1
            }
            // i.e given this offset arrays
            // [1 1 1 2 7]
            // and if stop_at_group = 5 the loop will break
            // at current_group = 2
            // we have to backfill the group_windows with
            // 5 items
            // [0 3 4 4 4 4]
            for _ in current_group..=stop_at_group {
                group_windows.push(T::usize_as(split_offset));
            }
        } else {
            let end_offset = T::usize_as(self.stacked_group_indices.len());
            for _ in current_group..=stop_at_group {
                group_windows.push(end_offset);
            }

            if self.stacked_group_indices.len() > 11 {
                println!("debug first items {:?}", &self.stacked_group_indices[..10]);
                println!("debug first group windows {:?}", &group_windows[..10]);
                println!(
                    "debug last items {:?}",
                    &self.stacked_group_indices[self.stacked_group_indices.len() - 10..]
                );
                println!(
                    "debug group windows {:?}",
                    &group_windows[group_windows.len() - 10..]
                );
                println!("group windows len {}", group_windows.len());
            }
            mem::take(&mut self.stacked_group_indices);
        };

        let stacked_batches = self
            .stacked_batches
            .iter()
            .map(|a| a.as_ref())
            .collect::<Vec<_>>();

        let offsets_buffer = OffsetBuffer::new(group_windows.into());

        // group indices like [1,1,1,2,2,2]
        // backend_array like [a,b,c,d,e,f]
        // offsets is like: [0,3,6]
        // then result should be [a,b,c], [d,e,f]

        // backend_array is a flatten list of individual values before aggregation
        let backend_array =
            kernels::interleave::interleave(&stacked_batches, &interleave_offsets)?;
        let dt = backend_array.data_type();
        let field = Arc::new(Field::new_list_field(dt.clone(), true));

        let arr = GenericListArray::<T>::new(field, offsets_buffer, backend_array, None);
        // Only when this happen, we know that the stacked_batches are no longer needed
        if self.stacked_group_indices.is_empty() {
            mem::take(&mut self.stacked_batches);
            self.stacked_batches_size = 0;
        }
        Ok(arr)
    }
}

impl<T: OffsetSizeTrait + Clone> GroupsAccumulator for AggGroupAccumulator<T> {
    // given the stacked_batch as:
    // - batch1 [1,4,5,6,7]
    // - batch2 [5,1,1,1,1]

    // and group_indices as
    // indices g1: [(0,0), (1,1), (1,2) ...]
    // indices g2: []
    // indices g3: []
    // indices g4: [(0,1)]
    // each tuple represents (batch_index, and offset within the batch index)
    // for example
    // - (0,0) means the 0th item inside batch1, which is `1`
    // - (1,1) means the 1th item inside batch2, which is `1`
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let singular_col = values
            .first()
            .ok_or(internal_datafusion_err!("invalid agg input"))?;

        self.stacked_batches.push(Arc::clone(singular_col));
        self.stacked_batches_size += singular_col.get_array_memory_size();
        let batch_index = self.stacked_batches.len() - 1;
        if let Some(filter) = opt_filter {
            for (array_offset, (group_index, filter_value)) in
                group_indices.iter().zip(filter.iter()).enumerate()
            {
                if let Some(true) = filter_value {
                    self.stacked_group_indices.push((
                        *group_index,
                        batch_index,
                        array_offset,
                    ));
                }
            }
        } else {
            for (array_offset, group_index) in group_indices.iter().enumerate() {
                self.stacked_group_indices.push((
                    *group_index,
                    batch_index,
                    array_offset,
                ));
            }
        }
        self.indice_sorted = false;
        self.max_seen_group = total_num_groups;
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emit_to_str = match emit_to {
            EmitTo::All => "all".to_string(),
            EmitTo::First(n) => format!("first {n}"),
        };
        let arr = self.consume_stacked_batches(emit_to)?;
        println!("finalize {} {}", arr.len(), emit_to_str);
        Ok(Arc::new(arr) as ArrayRef)
    }

    // filtered_null_mask(opt_filter, &values);
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let emit_to_str = match emit_to {
            EmitTo::All => "all".to_string(),
            EmitTo::First(n) => format!("first {n}"),
        };
        let arr = self.consume_stacked_batches(emit_to)?;
        println!(
            "evaluate state {} {} {} {}",
            arr.len(),
            emit_to_str,
            self.max_seen_group,
            self.groups_consumed
        );
        Ok(vec![Arc::new(arr) as ArrayRef])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        // for merge_batch which happens at final stage
        // opt_filter will always be none
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let singular_col = values
            .first()
            .ok_or(internal_datafusion_err!("invalid agg input"))?;
        let list_arr = singular_col.as_list::<i32>();
        let new_array_number = self.stacked_batches.len();
        // TODO: the backed_arr contains redundant data
        // make sure that flatten_group_index has the same length with backed_arr
        let flatten_group_index =
            group_indices
                .iter()
                .enumerate()
                .flat_map(|(row, group_index)| {
                    let end = list_arr.value_offsets()[row + 1].as_usize();
                    assert!(!list_arr.is_null(row));
                    let start = list_arr.value_offsets()[row].as_usize();
                    (start..end).map(|offset| (*group_index, new_array_number, offset))
                });
        self.stacked_group_indices.extend(flatten_group_index);

        let backed_arr = list_arr.values();
        self.stacked_batches.push(Arc::clone(backed_arr));
        self.stacked_batches_size += backed_arr.get_array_memory_size();
        self.indice_sorted = false;
        self.max_seen_group = total_num_groups;
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.stacked_group_indices.capacity() * size_of::<(usize, usize, usize)>()
            + self.stacked_batches.capacity() * size_of::<Vec<ArrayRef>>()
            + self.stacked_batches_size
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        assert!(opt_filter.is_none());
        assert!(values.len() == 1);
        let col_array = values
            .first()
            .ok_or(internal_datafusion_err!("invalid state for array agg"))?;

        let num_rows = col_array.len();
        // If there are no rows, return empty arrays
        if num_rows == 0 {
            return Ok(vec![new_empty_array(col_array.data_type())]);
        }
        let dt = col_array.data_type();

        let offsets = OffsetBuffer::from_lengths(repeat_n(1, num_rows));
        let field = Arc::new(Field::new_list_field(dt.clone(), true));

        let arr = GenericListArray::<i32>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::clone(col_array),
            None,
        );
        Ok(vec![Arc::new(arr) as Arc<dyn Array>])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, AsArray, BooleanArray, GenericListArray, StringArray},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field},
    };
    use datafusion_common::Result;
    use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

    use crate::aggregate::array_agg::AggGroupAccumulator;

    fn build_list_arr(
        values: Vec<Option<&str>>,
        offsets: Vec<i32>,
        nulls: Option<Vec<bool>>,
    ) -> Arc<dyn Array> {
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let backed_arr = Arc::new(StringArray::from_iter(values)) as Arc<dyn Array>;

        let arr = GenericListArray::<i32>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            backed_arr,
            nulls.map(NullBuffer::from_iter),
        );
        Arc::new(arr) as Arc<dyn Array>
    }

    #[test]
    fn test_agg_group_accumulator() -> Result<()> {
        // backed_arr: ["a","b","c", null, "d"]
        // partial_state: ["b","c"],[null], ["d"]]
        // group_indices: [2,1,1]
        // total num_group = 3
        let partial_state = build_list_arr(
            vec![Some("a"), Some("b"), Some("c"), None, Some("d")],
            vec![1, 3, 4, 5],
            Some(vec![true, true, true]),
        );
        let group_indices = vec![2, 1, 1];

        let mut acc = AggGroupAccumulator::<i32>::new(Arc::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        acc.merge_batch(&[Arc::clone(&partial_state)], &group_indices, None, 3)?;
        assert_eq!(
            &vec![
                (2, 0, 1), // b
                (2, 0, 2), // c
                (1, 0, 3), // null
                (1, 0, 4), // d
            ],
            &acc.stacked_group_indices
        );
        let backed_arr = partial_state.as_list::<i32>().values();
        assert_eq!(vec![Arc::clone(backed_arr)], acc.stacked_batches);

        // backed_arr: ["a","b","c", null, "d"]
        // group_indices: [2,4,1,1,1]
        // filter_opt as [true,true,false,true,false] meaning group 1 and 3 will result
        // into empty array
        let opt_filter = Some(BooleanArray::from_iter(vec![
            Some(true),
            Some(true),
            None,
            Some(true),
            None,
        ]));
        let group_indices = vec![2, 5, 1, 1, 1];
        acc.update_batch(
            &[Arc::clone(backed_arr)],
            &group_indices,
            opt_filter.as_ref(),
            6,
        )?;
        assert_eq!(6, acc.max_seen_group);
        assert_eq!(
            &vec![
                // from the prev merge_batch call
                (2, 0, 1), // b
                (2, 0, 2), // c
                (1, 0, 3), // null
                (1, 0, 4), // d
                // from the update_batch call
                (2, 1, 0), // a
                (5, 1, 1), // b
                // (1, 1,  2) c but filtered out
                (1, 1, 3), // null
                           // (1, 1, 4) // d but filterd out
            ],
            &acc.stacked_group_indices
        );
        assert_eq!(
            vec![Arc::clone(backed_arr), Arc::clone(backed_arr),],
            acc.stacked_batches
        );
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::All)?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0 is empty
                    // group1
                    None,
                    Some("d"),
                    None,
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                    // group3,group4 is empty
                    // group5
                    Some("b"),
                ],
                vec![0, 0, 3, 6, 6, 6, 7],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(6, acc2.groups_consumed);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(1))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                ],
                vec![0, 0],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(6, acc2.max_seen_group);
            assert_eq!(1, acc2.groups_consumed);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(2))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0 is empty
                    // group1
                    None,
                    Some("d"),
                    None,
                ],
                vec![0, 0, 3],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(6, acc2.max_seen_group);
            assert_eq!(2, acc2.groups_consumed);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(3))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0 is empty
                    // group1
                    None,
                    Some("d"),
                    None,
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                ],
                vec![0, 0, 3, 6],
                None,
            );


            assert_eq!(6, acc2.max_seen_group);
            assert_eq!(3, acc2.groups_consumed);
            assert_eq!(vec![expected_final_state], final_state);

            assert_eq!(
                &vec![
                    (2, 1, 1), // shift downward from (5,1,1) representing b
                ],
                &acc2.stacked_group_indices
            );
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(4))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0 is empty
                    // group1
                    None,
                    Some("d"),
                    None,
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                    // group3 is empty
                ],
                vec![0, 0, 3, 6, 6],
                None,
            );

            assert_eq!(6, acc2.max_seen_group);
            assert_eq!(4, acc2.groups_consumed);
            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(
                &vec![
                    (1, 1, 1), // shift downward from (5,1,1) representing b
                ],
                &acc2.stacked_group_indices
            );
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(5))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0 is empty
                    // group1
                    None,
                    Some("d"),
                    None,
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                    // group3,group4 is empty
                ],
                vec![0, 0, 3, 6, 6, 6],
                None,
            );


            assert_eq!(6, acc2.max_seen_group);
            assert_eq!(5, acc2.groups_consumed);
            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(
                &vec![
                    (0, 1, 1), // shift downward from (5,1,1) representing b
                ],
                &acc2.stacked_group_indices
            );
        }
        Ok(())
    }
}
