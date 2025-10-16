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
use std::mem;
use std::sync::Arc;

use arrow::array::{new_empty_array, Array, GenericListArray};
use arrow::array::{ArrayRef, AsArray, BooleanArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::kernels;
use arrow::datatypes::{ArrowNativeType, Field};
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};

#[derive(Default, Clone)]
pub struct AggGroupAccumulator {
    // [1,2,3] [4,5,6]
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
    max_group: usize,
}

impl AggGroupAccumulator {
    pub fn new() -> Self {
        Self {
            stacked_batches: vec![],
            stacked_batches_size: 0,
            stacked_group_indices: vec![],
            indice_sorted: false,
            max_group: 0,
        }
    }
    fn consume_stacked_batches(
        &mut self,
        emit_to: EmitTo,
    ) -> Result<GenericListArray<i32>> {
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

        let mut current_group = self.stacked_group_indices[0].0;

        // this is inclusive, zero-based
        let stop_at_group = match emit_to {
            EmitTo::All => self.max_group - 1,
            EmitTo::First(groups_taken) => groups_taken - 1,
        };
        let mut group_windows =
            Vec::<i32>::with_capacity(self.max_group.min(stop_at_group) + 1);
        group_windows.push(0);
        let mut split_offset = None;

        // TODO: init with a good cap if possible via some stats during accumulation phase
        let mut interleave_offsets = vec![];
        for (offset, (group_index, array_number, offset_in_array)) in
            self.stacked_group_indices.iter().enumerate()
        {
            if *group_index > stop_at_group {
                split_offset = Some(offset);
                break;
            }
            if *group_index > current_group {
                current_group = *group_index;
                group_windows.push(offset as i32);
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

            group_windows.push(split_offset as i32);
        } else {
            group_windows.push(self.stacked_group_indices.len() as i32);
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
        // offsets should be: [0,3,6]
        // then result should be [a,b,c], [d,e,f]

        // backend_array is a flatten list of individual values before aggregation
        let backend_array =
            kernels::interleave::interleave(&stacked_batches, &interleave_offsets)?;
        let dt = backend_array.data_type();
        let field = Arc::new(Field::new_list_field(dt.clone(), true));

        let arr =
            GenericListArray::<i32>::new(field, offsets_buffer, backend_array, None);
        // Only when this happen, we know that the stacked_batches are no longer needed
        if self.stacked_group_indices.is_empty() {
            mem::take(&mut self.stacked_batches);
            self.stacked_batches_size = 0;
        }
        Ok(arr)
    }
}

impl GroupsAccumulator for AggGroupAccumulator {
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
        self.max_group = self.max_group.max(total_num_groups);
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let arr = self.consume_stacked_batches(emit_to)?;
        Ok(Arc::new(arr) as ArrayRef)
    }

    // filtered_null_mask(opt_filter, &values);
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
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
                    let start = list_arr.value_offsets()[row].as_usize();
                    (start..end).map(|offset| (*group_index, new_array_number, offset))
                });
        self.stacked_group_indices.extend(flatten_group_index);

        let backed_arr = list_arr.values();
        self.stacked_batches.push(Arc::clone(backed_arr));
        self.stacked_batches_size += backed_arr.get_array_memory_size();
        self.indice_sorted = false;
        self.max_group = self.max_group.max(total_num_groups);
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
        array::{Array, AsArray, BooleanArray, GenericListArray, ListArray, StringArray},
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
        let nulls = match nulls {
            Some(nulls) => Some(NullBuffer::from_iter(nulls.into_iter())),
            None => None,
        };
        let arr = GenericListArray::<i32>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            backed_arr,
            nulls,
        );
        return Arc::new(arr) as Arc<dyn Array>;
    }

    #[test]
    fn test_agg_group_accumulator() -> Result<()> {
        let mut acc = AggGroupAccumulator::new();
        // backed_arr: ["a","b","c", null, "d"]
        // partial_state: ["b","c"],[null], ["d"]]
        // group_indices: [2,0,1]
        // total num_group = 3
        let partial_state = build_list_arr(
            vec![Some("a"), Some("b"), Some("c"), None, Some("d")],
            vec![1, 3, 4, 5],
            Some(vec![true, true, true]),
        );
        let group_indices = vec![2, 0, 1];

        acc.merge_batch(&[Arc::clone(&partial_state)], &group_indices, None, 3);
        assert_eq!(
            &vec![
                (2, 0, 1), // b
                (2, 0, 2), // c
                (0, 0, 3), // null
                (1, 0, 4), // d
            ],
            &acc.stacked_group_indices
        );
        let backed_arr = partial_state.as_list::<i32>().values();
        assert_eq!(vec![Arc::clone(backed_arr)], acc.stacked_batches);

        // backed_arr: ["a","b","c", null, "d"]
        // group_indices: [2,4,0,0,0]
        // filter_opt as [true,true,false,true,false]
        let opt_filter = Some(BooleanArray::from_iter(vec![
            Some(true),
            Some(true),
            None,
            Some(true),
            None,
        ]));
        let group_indices = vec![2, 4, 0, 0, 0];
        acc.update_batch(
            &[Arc::clone(backed_arr)],
            &group_indices,
            opt_filter.as_ref(),
            5,
        );

        assert_eq!(
            &vec![
                // from the prev merge_batch call
                (2, 0, 1), // b
                (2, 0, 2), // c
                (0, 0, 3), // null
                (1, 0, 4), // d
                // from the update_batch call
                (2, 1, 0), // a
                (4, 1, 1), // b
                (0, 1, 3), // null
            ],
            &acc.stacked_group_indices
        );
        assert_eq!(
            vec![Arc::clone(&backed_arr), Arc::clone(&backed_arr),],
            acc.stacked_batches
        );
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::All)?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                    None,
                    None,
                    // group1
                    Some("d"),
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                    // group3 not exist
                    // group4
                    Some("b"),
                ],
                vec![0, 2, 3, 6, 7],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(1))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                    None, None,
                ],
                vec![0, 2],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(2))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                    None,
                    None,
                    // group1
                    Some("d"),
                ],
                vec![0, 2, 3],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(3))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                    None,
                    None,
                    // group1
                    Some("d"),
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                ],
                vec![0, 2, 3, 6],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);

            assert_eq!(
                &vec![
                    (1, 1, 1), // shift downward from (4,1,1) representing b
                ],
                &acc2.stacked_group_indices
            );
        }
        {
            let mut acc2 = acc.clone();
            let final_state = acc2.state(EmitTo::First(4))?;

            let expected_final_state = build_list_arr(
                vec![
                    // group0
                    None,
                    None,
                    // group1
                    Some("d"),
                    // group2
                    Some("b"),
                    Some("c"),
                    Some("a"),
                ],
                vec![0, 2, 3, 6],
                None,
            );

            assert_eq!(vec![expected_final_state], final_state);
            assert_eq!(
                &vec![
                    (0, 1, 1), // shift downward from (4,1,1) representing b
                ],
                &acc2.stacked_group_indices
            );
        }
        Ok(())
    }
}
