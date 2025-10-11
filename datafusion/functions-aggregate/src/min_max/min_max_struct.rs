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

use std::{cmp::Ordering, mem::size_of, sync::Arc};

use arrow::{
    array::{
        Array, ArrayData, ArrayRef, AsArray, BooleanArray, MutableArrayData, StructArray,
    },
    datatypes::DataType,
};
use datafusion_common::{
    internal_err,
    scalar::{copy_array_data, partial_cmp_struct},
    Result,
};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::apply_filter_as_nulls;

/// Accumulator for MIN/MAX operations on Struct data types.
///
/// This accumulator tracks the minimum or maximum struct value encountered
/// during aggregation, depending on the `is_min` flag.
///
/// The comparison is done based on the struct fields in order.
pub(crate) struct MinMaxStructAccumulator {
    /// Inner data storage.
    inner: MinMaxStructState,
    /// if true, is `MIN` otherwise is `MAX`
    is_min: bool,
}

impl MinMaxStructAccumulator {
    pub fn new_min(data_type: DataType) -> Self {
        Self {
            inner: MinMaxStructState::new(data_type),
            is_min: true,
        }
    }

    pub fn new_max(data_type: DataType) -> Self {
        Self {
            inner: MinMaxStructState::new(data_type),
            is_min: false,
        }
    }
}

impl GroupsAccumulator for MinMaxStructAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let array = &values[0];
        assert_eq!(array.len(), group_indices.len());
        assert_eq!(array.data_type(), &self.inner.data_type);
        // apply filter if needed
        let array = apply_filter_as_nulls(array, opt_filter)?;

        fn struct_min(a: &StructArray, b: &StructArray) -> bool {
            matches!(partial_cmp_struct(a, b), Some(Ordering::Less))
        }

        fn struct_max(a: &StructArray, b: &StructArray) -> bool {
            matches!(partial_cmp_struct(a, b), Some(Ordering::Greater))
        }

        if self.is_min {
            self.inner.update_batch(
                array.as_struct(),
                group_indices,
                total_num_groups,
                struct_min,
            )
        } else {
            self.inner.update_batch(
                array.as_struct(),
                group_indices,
                total_num_groups,
                struct_max,
            )
        }
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let (_, min_maxes) = self.inner.emit_to(emit_to);
        let fields = match &self.inner.data_type {
            DataType::Struct(fields) => fields,
            _ => return internal_err!("Data type is not a struct"),
        };
        let null_array = StructArray::new_null(fields.clone(), 1);
        let min_maxes_data: Vec<ArrayData> = min_maxes
            .iter()
            .map(|v| match v {
                Some(v) => v.to_data(),
                None => null_array.to_data(),
            })
            .collect();
        let min_maxes_refs: Vec<&ArrayData> = min_maxes_data.iter().collect();
        let mut copy = MutableArrayData::new(min_maxes_refs, true, min_maxes_data.len());

        for (i, item) in min_maxes_data.iter().enumerate() {
            copy.extend(i, 0, item.len());
        }
        let result = copy.freeze();
        assert_eq!(&self.inner.data_type, result.data_type());
        Ok(Arc::new(StructArray::from(result)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // min/max are their own states (no transition needed)
        self.evaluate(emit_to).map(|arr| vec![arr])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // min/max are their own states (no transition needed)
        self.update_batch(values, group_indices, opt_filter, total_num_groups)
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // Min/max do not change the values as they are their own states
        // apply the filter by combining with the null mask, if any
        let output = apply_filter_as_nulls(&values[0], opt_filter)?;
        Ok(vec![output])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}

#[derive(Debug)]
struct MinMaxStructState {
    /// The minimum/maximum value for each group
    min_max: Vec<Option<StructArray>>,
    /// The data type of the array
    data_type: DataType,
    /// The total bytes of the string data (for pre-allocating the final array,
    /// and tracking memory usage)
    total_data_bytes: usize,
    /// Tracks the groups that were updated in the current batch so that we only
    /// touch entries that actually changed. This avoids clearing dense scratch
    /// structures across batches.
    scratch_touched_groups: Vec<usize>,
    /// Dense scratch entries reused across batches. Each entry stores the epoch
    /// of the last batch that touched it together with the location of the
    /// candidate value for the group.
    scratch_entries: Vec<ScratchEntry>,
    /// Epoch identifying the current batch. When the epoch wraps we reset the
    /// scratch entries eagerly to maintain correctness.
    scratch_epoch: u64,
    /// Reusable buffer storing candidate values taken from the current batch.
    scratch_batch_inputs: Vec<StructArray>,
}

#[derive(Debug, Default, Clone)]
struct ScratchEntry {
    epoch: u64,
    location: ScratchLocation,
}

#[derive(Debug, Clone, Default)]
enum ScratchLocation {
    /// No value from the current batch has been observed for the group yet.
    #[default]
    Untouched,
    /// The group should keep the previously materialised min/max value.
    Existing,
    /// The min/max candidate for the group resides in the current batch at the
    /// provided index within `scratch_batch_inputs`.
    Batch(usize),
}

/// Implement the MinMaxStructState with a comparison function
/// for comparing structs
impl MinMaxStructState {
    /// Create a new MinMaxStructState
    ///
    /// # Arguments:
    /// * `data_type`: The data type of the arrays that will be passed to this accumulator
    fn new(data_type: DataType) -> Self {
        Self {
            min_max: vec![],
            data_type,
            total_data_bytes: 0,
            scratch_touched_groups: vec![],
            scratch_entries: vec![],
            scratch_epoch: 0,
            scratch_batch_inputs: vec![],
        }
    }

    /// Set the specified group to the given value, updating memory usage appropriately
    fn set_value(&mut self, group_index: usize, new_val: &StructArray) {
        let new_val = StructArray::from(copy_array_data(&new_val.to_data()));
        match self.min_max[group_index].as_mut() {
            None => {
                self.total_data_bytes += new_val.get_array_memory_size();
                self.min_max[group_index] = Some(new_val);
            }
            Some(existing_val) => {
                // Copy data over to avoid re-allocating
                self.total_data_bytes -= existing_val.get_array_memory_size();
                self.total_data_bytes += new_val.get_array_memory_size();
                *existing_val = new_val;
            }
        }
    }

    /// Updates the min/max values for the given string values
    ///
    /// `cmp` is the  comparison function to use, called like `cmp(new_val, existing_val)`
    /// returns true if the `new_val` should replace `existing_val`
    fn update_batch<F>(
        &mut self,
        array: &StructArray,
        group_indices: &[usize],
        total_num_groups: usize,
        mut cmp: F,
    ) -> Result<()>
    where
        F: FnMut(&StructArray, &StructArray) -> bool + Send + Sync,
    {
        self.min_max.resize(total_num_groups, None);

        if self.scratch_entries.len() < total_num_groups {
            self.scratch_entries
                .resize_with(total_num_groups, ScratchEntry::default);
        }

        self.scratch_epoch = self.scratch_epoch.wrapping_add(1);
        if self.scratch_epoch == 0 {
            for entry in &mut self.scratch_entries {
                entry.epoch = 0;
                entry.location = ScratchLocation::Untouched;
            }
            self.scratch_epoch = 1;
        }

        let mut touched_groups = std::mem::take(&mut self.scratch_touched_groups);
        touched_groups.clear();
        let mut batch_inputs = std::mem::take(&mut self.scratch_batch_inputs);
        batch_inputs.clear();

        for (index, group_index) in (0..array.len()).zip(group_indices.iter()) {
            let group_index = *group_index;
            if array.is_null(index) {
                continue;
            }

            if group_index >= total_num_groups {
                return internal_err!(
                    "group index {group_index} out of bounds for {total_num_groups} groups"
                );
            }

            let new_val = array.slice(index, 1);

            let entry = &mut self.scratch_entries[group_index];
            if entry.epoch != self.scratch_epoch {
                entry.epoch = self.scratch_epoch;
                entry.location = ScratchLocation::Untouched;
                touched_groups.push(group_index);
            }

            let existing_val = match &entry.location {
                ScratchLocation::Untouched => {
                    if let Some(existing_val) = self.min_max[group_index].as_ref() {
                        entry.location = ScratchLocation::Existing;
                        existing_val
                    } else {
                        let batch_index = batch_inputs.len();
                        batch_inputs.push(new_val);
                        entry.location = ScratchLocation::Batch(batch_index);
                        continue;
                    }
                }
                ScratchLocation::Existing => self.min_max[group_index]
                    .as_ref()
                    .expect("existing value must be present"),
                ScratchLocation::Batch(existing_index) => &batch_inputs[*existing_index],
            };

            if cmp(&new_val, existing_val) {
                let batch_index = batch_inputs.len();
                batch_inputs.push(new_val);
                entry.location = ScratchLocation::Batch(batch_index);
            }
        }

        for &group_index in &touched_groups {
            if let ScratchLocation::Batch(batch_index) =
                &self.scratch_entries[group_index].location
            {
                let value = &batch_inputs[*batch_index];
                self.set_value(group_index, value);
            }
        }

        batch_inputs.clear();
        self.scratch_batch_inputs = batch_inputs;
        touched_groups.clear();
        self.scratch_touched_groups = touched_groups;
        Ok(())
    }

    /// Emits the specified min_max values
    ///
    /// Returns (data_capacity, min_maxes), updating the current value of total_data_bytes
    ///
    /// - `data_capacity`: the total length of all strings and their contents,
    /// - `min_maxes`: the actual min/max values for each group
    fn emit_to(&mut self, emit_to: EmitTo) -> (usize, Vec<Option<StructArray>>) {
        match emit_to {
            EmitTo::All => {
                (
                    std::mem::take(&mut self.total_data_bytes), // reset total bytes and min_max
                    std::mem::take(&mut self.min_max),
                )
            }
            EmitTo::First(n) => {
                let first_min_maxes: Vec<_> = self.min_max.drain(..n).collect();
                let first_data_capacity: usize = first_min_maxes
                    .iter()
                    .map(|opt| opt.as_ref().map(|s| s.len()).unwrap_or(0))
                    .sum();
                self.total_data_bytes -= first_data_capacity;
                (first_data_capacity, first_min_maxes)
            }
        }
    }

    fn size(&self) -> usize {
        self.total_data_bytes
            + self.min_max.len() * size_of::<Option<StructArray>>()
            + self.scratch_entries.capacity() * size_of::<ScratchEntry>()
            + self.scratch_touched_groups.capacity() * size_of::<usize>()
            + self.scratch_batch_inputs.capacity() * size_of::<StructArray>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Int32Type};
    use std::sync::Arc;

    fn create_test_struct_array(
        int_values: Vec<Option<i32>>,
        str_values: Vec<Option<&str>>,
    ) -> StructArray {
        let int_array = Int32Array::from(int_values);
        let str_array = StringArray::from(str_values);

        let fields = vec![
            Field::new("int_field", DataType::Int32, true),
            Field::new("str_field", DataType::Utf8, true),
        ];

        StructArray::new(
            Fields::from(fields),
            vec![
                Arc::new(int_array) as ArrayRef,
                Arc::new(str_array) as ArrayRef,
            ],
            None,
        )
    }

    fn create_nested_struct_array(
        int_values: Vec<Option<i32>>,
        str_values: Vec<Option<&str>>,
    ) -> StructArray {
        let inner_struct = create_test_struct_array(int_values, str_values);

        let fields = vec![Field::new("inner", inner_struct.data_type().clone(), true)];

        StructArray::new(
            Fields::from(fields),
            vec![Arc::new(inner_struct) as ArrayRef],
            None,
        )
    }

    #[test]
    fn test_min_max_simple_struct() {
        let array = create_test_struct_array(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let mut min_accumulator =
            MinMaxStructAccumulator::new_min(array.data_type().clone());
        let mut max_accumulator =
            MinMaxStructAccumulator::new_max(array.data_type().clone());
        let values = vec![Arc::new(array) as ArrayRef];
        let group_indices = vec![0, 0, 0];

        min_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        max_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        let min_result = min_accumulator.evaluate(EmitTo::All).unwrap();
        let max_result = max_accumulator.evaluate(EmitTo::All).unwrap();
        let min_result = min_result.as_struct();
        let max_result = max_result.as_struct();

        assert_eq!(min_result.len(), 1);
        let int_array = min_result.column(0).as_primitive::<Int32Type>();
        let str_array = min_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(str_array.value(0), "a");

        assert_eq!(max_result.len(), 1);
        let int_array = max_result.column(0).as_primitive::<Int32Type>();
        let str_array = max_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 3);
        assert_eq!(str_array.value(0), "c");
    }

    #[test]
    fn test_min_max_nested_struct() {
        let array = create_nested_struct_array(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
        );

        let mut min_accumulator =
            MinMaxStructAccumulator::new_min(array.data_type().clone());
        let mut max_accumulator =
            MinMaxStructAccumulator::new_max(array.data_type().clone());
        let values = vec![Arc::new(array) as ArrayRef];
        let group_indices = vec![0, 0, 0];

        min_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        max_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        let min_result = min_accumulator.evaluate(EmitTo::All).unwrap();
        let max_result = max_accumulator.evaluate(EmitTo::All).unwrap();
        let min_result = min_result.as_struct();
        let max_result = max_result.as_struct();

        assert_eq!(min_result.len(), 1);
        let inner = min_result.column(0).as_struct();
        let int_array = inner.column(0).as_primitive::<Int32Type>();
        let str_array = inner.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(str_array.value(0), "a");

        assert_eq!(max_result.len(), 1);
        let inner = max_result.column(0).as_struct();
        let int_array = inner.column(0).as_primitive::<Int32Type>();
        let str_array = inner.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 3);
        assert_eq!(str_array.value(0), "c");
    }

    #[test]
    fn test_min_max_with_nulls() {
        let array = create_test_struct_array(
            vec![Some(1), None, Some(3)],
            vec![Some("a"), None, Some("c")],
        );

        let mut min_accumulator =
            MinMaxStructAccumulator::new_min(array.data_type().clone());
        let mut max_accumulator =
            MinMaxStructAccumulator::new_max(array.data_type().clone());
        let values = vec![Arc::new(array) as ArrayRef];
        let group_indices = vec![0, 0, 0];

        min_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        max_accumulator
            .update_batch(&values, &group_indices, None, 1)
            .unwrap();
        let min_result = min_accumulator.evaluate(EmitTo::All).unwrap();
        let max_result = max_accumulator.evaluate(EmitTo::All).unwrap();
        let min_result = min_result.as_struct();
        let max_result = max_result.as_struct();

        assert_eq!(min_result.len(), 1);
        let int_array = min_result.column(0).as_primitive::<Int32Type>();
        let str_array = min_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(str_array.value(0), "a");

        assert_eq!(max_result.len(), 1);
        let int_array = max_result.column(0).as_primitive::<Int32Type>();
        let str_array = max_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 3);
        assert_eq!(str_array.value(0), "c");
    }

    #[test]
    fn test_min_max_multiple_groups() {
        let array = create_test_struct_array(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );

        let mut min_accumulator =
            MinMaxStructAccumulator::new_min(array.data_type().clone());
        let mut max_accumulator =
            MinMaxStructAccumulator::new_max(array.data_type().clone());
        let values = vec![Arc::new(array) as ArrayRef];
        let group_indices = vec![0, 1, 0, 1];

        min_accumulator
            .update_batch(&values, &group_indices, None, 2)
            .unwrap();
        max_accumulator
            .update_batch(&values, &group_indices, None, 2)
            .unwrap();
        let min_result = min_accumulator.evaluate(EmitTo::All).unwrap();
        let max_result = max_accumulator.evaluate(EmitTo::All).unwrap();
        let min_result = min_result.as_struct();
        let max_result = max_result.as_struct();

        assert_eq!(min_result.len(), 2);
        let int_array = min_result.column(0).as_primitive::<Int32Type>();
        let str_array = min_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(str_array.value(0), "a");
        assert_eq!(int_array.value(1), 2);
        assert_eq!(str_array.value(1), "b");

        assert_eq!(max_result.len(), 2);
        let int_array = max_result.column(0).as_primitive::<Int32Type>();
        let str_array = max_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 3);
        assert_eq!(str_array.value(0), "c");
        assert_eq!(int_array.value(1), 4);
        assert_eq!(str_array.value(1), "d");
    }

    #[test]
    fn test_min_max_with_filter() {
        let array = create_test_struct_array(
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );

        // Create a filter that only keeps even numbers
        let filter = BooleanArray::from(vec![false, true, false, true]);

        let mut min_accumulator =
            MinMaxStructAccumulator::new_min(array.data_type().clone());
        let mut max_accumulator =
            MinMaxStructAccumulator::new_max(array.data_type().clone());
        let values = vec![Arc::new(array) as ArrayRef];
        let group_indices = vec![0, 0, 0, 0];

        min_accumulator
            .update_batch(&values, &group_indices, Some(&filter), 1)
            .unwrap();
        max_accumulator
            .update_batch(&values, &group_indices, Some(&filter), 1)
            .unwrap();
        let min_result = min_accumulator.evaluate(EmitTo::All).unwrap();
        let max_result = max_accumulator.evaluate(EmitTo::All).unwrap();
        let min_result = min_result.as_struct();
        let max_result = max_result.as_struct();

        assert_eq!(min_result.len(), 1);
        let int_array = min_result.column(0).as_primitive::<Int32Type>();
        let str_array = min_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 2);
        assert_eq!(str_array.value(0), "b");

        assert_eq!(max_result.len(), 1);
        let int_array = max_result.column(0).as_primitive::<Int32Type>();
        let str_array = max_result.column(1).as_string::<i32>();
        assert_eq!(int_array.value(0), 4);
        assert_eq!(str_array.value(0), "d");
    }

    #[test]
    fn test_min_max_sparse_multi_batch() {
        let batch_len = 128;
        let total_groups = 1024;
        let group_indices: Vec<usize> = (0..batch_len).map(|i| i * 8).collect();

        let batch_one = create_test_struct_array(
            (0..batch_len).map(|i| Some(1000_i32 - i as i32)).collect(),
            vec![Some("batch_one"); batch_len],
        );

        let mut accumulator =
            MinMaxStructAccumulator::new_min(batch_one.data_type().clone());
        let values_one = vec![Arc::new(batch_one) as ArrayRef];

        accumulator
            .update_batch(&values_one, &group_indices, None, total_groups)
            .unwrap();

        let batch_two = create_test_struct_array(
            (0..batch_len).map(|i| Some(-(i as i32))).collect(),
            vec![Some("batch_two"); batch_len],
        );
        let values_two = vec![Arc::new(batch_two) as ArrayRef];

        accumulator
            .update_batch(&values_two, &group_indices, None, total_groups)
            .unwrap();

        let result = accumulator.evaluate(EmitTo::All).unwrap();
        let result = result.as_struct();

        let int_array = result.column(0).as_primitive::<Int32Type>();

        for (i, group_index) in group_indices.iter().copied().enumerate() {
            assert!(result.is_valid(group_index));
            assert_eq!(int_array.value(group_index), -(i as i32));
        }

        assert!(result.is_null(total_groups - 1));
    }
}
