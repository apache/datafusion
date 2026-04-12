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

use crate::aggregates::group_values::GroupValues;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, DictionaryArray, Int8Builder, Int16Builder, Int32Builder, Int64Builder, LargeStringBuilder, Scalar, StringArray, StringBuilder, StringViewBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder
};
use std::mem;
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::EmitTo;
use std::collections::HashMap;
use std::marker::PhantomData;
use arrow::datatypes::ArrowPrimitiveType;
pub struct GroupValuesDictionary<K:ArrowDictionaryKeyType + Send> {
    /*
    We know that every single &[ArrayRef] that is passed in is a dictionary array

    self.inter() will be called across record batches, this means that
    we cannot rely on a trivial approach where we just store the dictionary mapping as it is



    Possible soluitions:
    1A. store a hashmap that last across .intern() calls
        | cast cols:&[ArrayRef] to generic Dictionary array, check if weve already stored its values (unqiue values) before
        | if we have check the current mapping internally and update the groups array with the initial mapping for this value
        | if it does not exist already (hashmap.size) is the group_id for this element
    1B. how do we retrive the dictionary encoded array this function expects?
        | NOTE: emit returns one value per group not one value per row. The group values are the distinct values in the order they were first seen — not the full expanded key array [one per group index]
        | keep a value_order array that stores unique elements the first time their seen, this maintains order for self.emit()
        | the return type of the array self.emit() returns is based on the value type of the dictionary, may be smart to have an internal Group values that handels that logic
        |

    Possible optmizations (Ignore for now)
    2A. dont rely directly in a hashmap we could hash all of the values at once and then as we iterate the keys array refer to them as the values are assumed to be smaller than the keys
        | at the start of self.intern hash every value in the dictionary
        | iterate through the keys section of dict_array
            | for each key check its corresponding value and if it exist


    */
    // stores the order new unique elements are seen for self.emit()
    seen_elements: Vec<ScalarValue>,  //  Box<dyn Builder> doesnt provide the flexibility of building partion arrays that wed need to support emit::First(N)
    value_dt : DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen. stored as -> <unique_value:initial_group_id>
    unique_dict_value_mapping: HashMap<ScalarValue, usize>,
}

impl<K:ArrowDictionaryKeyType + Send> GroupValuesDictionary<K> {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            seen_elements: Vec::new(),
            unique_dict_value_mapping: HashMap::new(),
            value_dt: data_type.clone(),
            _phantom: PhantomData
        }
    }
    
}

impl<K:ArrowDictionaryKeyType + Send> GroupValues for GroupValuesDictionary<K>  {
    // not really sure how to return the size of strings and binary values so this is a best effort approach
    fn size(&self) -> usize {
        let arr_size = element_size(&self.value_dt) * self.unique_dict_value_mapping.len();
        let dict_size = self.unique_dict_value_mapping.len() * size_of::<(ScalarValue, usize)>() + 100 /* rough estimate for hashmap overhead */; // rough estimate for hashmap overhead
        arr_size + dict_size
    }
    fn len(&self) -> usize {
        self.unique_dict_value_mapping.len()
    }
    fn is_empty(&self) -> bool {
        self.unique_dict_value_mapping.is_empty()
    }
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        if cols.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "GroupValuesDictionary only supports single column group by".to_string(),
            ));
        }
        let array = cols[0].clone();
        groups.clear(); // zero out buffer
        let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        // grab the keys and values array
        let values = dict_array.values();
        let key_array = dict_array.keys();

        // for each of the values check if its already been stored in the hashtable
            // A. if it has grab the corresponding initail group integer assigned to it
            // B. if it has not its group integer is self.seen_elements.len - 1 and then store this mapping
        for i in 0..key_array.len() {
            if key_array.is_null(i){
                // Null case -> skip!
                continue;
            }
            let key = key_array.value(i).as_usize();
            let scalar_value = ScalarValue::try_from_array(values, key)?;
            let group_id = if let Some(group_id) = self.unique_dict_value_mapping.get(&scalar_value) {
                *group_id
            } else {
                let new_group_id = self.seen_elements.len();
                self.seen_elements.push(scalar_value.clone());
                self.unique_dict_value_mapping.insert(scalar_value, new_group_id);
                new_group_id
            };
            groups.push(group_id);

        }
        
        Ok(())
    }
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
       let columns: Vec<ScalarValue> = match emit_to {
        EmitTo::All => {
            self.unique_dict_value_mapping.clear();
            mem::take(&mut self.seen_elements)
        },
        EmitTo::First(n) => {
            // drain first n elements, keeping the rest
            let first_n = self.seen_elements.drain(..n).collect();
            // shift all remaining group indices down by n
            self.unique_dict_value_mapping.retain(|_, group_idx| {
                match group_idx.checked_sub(n) {
                    Some(new_idx) => {
                        *group_idx = new_idx;
                        true
                    }
                    // this group was in the first n, remove it
                    None => false,
                }
            });
            first_n
        }
    };

    // convert Vec<ScalarValue> into an ArrayRef
    let array = ScalarValue::iter_to_array(columns.into_iter())?;
    Ok(vec![array])
       
    }
    fn clear_shrink(&mut self, num_rows: usize) {
        self.seen_elements.clear();
        self.seen_elements.shrink_to(num_rows);
        self.unique_dict_value_mapping.clear();
        self.unique_dict_value_mapping.shrink_to(num_rows);
    }
}
fn element_size(dt: &DataType) -> usize {
    match dt{
        DataType::Utf8 | DataType::LargeUtf8 => 20, // rough estimate for average string size
        DataType::Binary | DataType::LargeBinary => 20, // rough estimate for average binary size
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 => 4,
        DataType::Int64 | DataType::UInt64 => 8,
        _ => 0, // default case for unsupported types
    }
}

#[cfg(test)]
mod group_values_trait_test {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray, UInt8Array};
    use std::sync::Arc;

    fn create_dict_array(keys: Vec<u8>, values: Vec<&str>) -> ArrayRef {
        let values = StringArray::from(values);
        let keys = UInt8Array::from(keys);
        Arc::new(
            DictionaryArray::<arrow::datatypes::UInt8Type>::try_new(
                keys,
                Arc::new(values),
            )
            .unwrap(),
        )
    }
    /*
    cargo test --package datafusion-physical-plan --lib -- aggregates::group_values::single_group_by::dictionary::group_values_trait_test::test_group_values_dictionary --exact --nocapture --include-ignored
    
    fn run_groupvalue_test_suite() -> Result<()> {
        let tests: Vec<(&str, fn(&mut dyn GroupValues))> = vec![
            ("test_single_group_all_same_values", basic_functionality::test_single_group_all_same_values),
            ("test_multiple_groups", basic_functionality::test_multiple_groups),
            ("test_all_different_values", basic_functionality::test_all_different_values),
            ("test_empty_batch", edge_cases::test_empty_batch),
            ("test_single_row", edge_cases::test_single_row),
            ("test_repeated_pattern", edge_cases::test_repeated_pattern),
            ("test_multiple_columns_passed", multi_column::test_multiple_columns_passed),
            ("test_consecutive_batches_then_emit", consecutive_batches::test_consecutive_batches_then_emit),
            ("test_three_consecutive_batches_with_partial_emit", consecutive_batches::test_three_consecutive_batches_with_partial_emit),
            ("test_size_grows_after_intern", state_management::test_size_grows_after_intern),
            ("test_complex_emit_flow_with_multiple_internS", state_management::test_complex_emit_flow_with_multiple_internS),
            ("test_clear_shrink_resets_state", state_management::test_clear_shrink_resets_state),
            ("test_clear_shrink_with_zero", state_management::test_clear_shrink_with_zero),
            ("test_emit_all_clears_state", state_management::test_emit_all_clears_state),
            ("test_emit_first_n", state_management::test_emit_first_n),
            ("test_group_assignment_order", data_correctness::test_group_assignment_order),
            ("test_groups_vector_correctness_first_appearance", data_correctness::test_groups_vector_correctness_first_appearance),
            ("test_groups_vector_sequential_assignment", data_correctness::test_groups_vector_sequential_assignment),
            ("test_emit_partial_preserves_state", data_correctness::test_emit_partial_preserves_state),
            ("test_emit_restores_intern_ability", data_correctness::test_emit_restores_intern_ability),
        ];
        for (name, test_function) in tests {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            println!("Running test: {name}");
            test_function(&mut group_values);
        }

        Ok(())
    }
    */

    mod basic_functionality {
        use super::*;

        pub fn test_single_group_all_same_values(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array = create_dict_array(vec![0, 0, 0], vec!["red"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(groups_vector.len(), 3);
            assert_eq!(group_values_trait_obj.len(), 1);
            assert!(!group_values_trait_obj.is_empty());
        }
        #[test]
        fn run_test_single_group_all_same_values() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_single_group_all_same_values(&mut group_values);
        }

        pub fn test_multiple_groups(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 0, 2, 1], vec!["red", "blue", "green"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            println!("groups_vector after intern: {:#?}", groups_vector);
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector.len(), 5);
        }

        #[test]
        fn run_test_multiple_groups() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_multiple_groups(&mut group_values);
        }

        pub fn test_all_different_values(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 3, 4],
                vec!["apple", "banana", "cherry", "date", "elderberry"],
            );

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 5);
            assert_eq!(groups_vector.len(), 5);
        }

        #[test]
        fn run_test_all_different_values() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_all_different_values(&mut group_values);
        }
    }

    mod edge_cases {
        use super::*;

        pub fn test_empty_batch(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(vec![], vec!["red"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 0);
            assert_eq!(groups_vector.len(), 0);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_empty_batch() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_empty_batch(&mut group_values);
        }

        pub fn test_single_row(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(vec![0], vec!["apple"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 1);
            assert_eq!(groups_vector.len(), 1);
            assert_eq!(groups_vector[0], 0);
        }

        #[test]
        fn run_test_single_row() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_single_row(&mut group_values);
        }

        pub fn test_repeated_pattern(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 2, 0, 1, 2, 0, 1, 2], vec!["a", "b", "c"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector.len(), 9);
        }

        #[test]
        fn run_test_repeated_pattern() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_repeated_pattern(&mut group_values);
        }
    }

    mod multi_column {
        use super::*;

        pub fn test_multiple_columns_passed(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array1 = create_dict_array(vec![0, 1, 0], vec!["red", "blue"]);

            let dict_array2 = create_dict_array(vec![0, 0, 1], vec!["x", "y"]);

            let mut groups_vector = Vec::new();
            let result = group_values_trait_obj
                .intern(&[dict_array1, dict_array2], &mut groups_vector);
            assert!(
                result.is_err(),
                "Should error when multiple columns are passed (only single column supported)"
            );
        }

        #[test]
        fn run_test_multiple_columns_passed() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_multiple_columns_passed(&mut group_values);
        }
    }

    mod consecutive_batches {
        use super::*;

        pub fn test_consecutive_batches_then_emit(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1 = create_dict_array(vec![0, 1, 0], vec!["red", "blue"]);

            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            assert_eq!(groups_vector1.len(), 3);

            let batch2 = create_dict_array(vec![0, 1, 2], vec!["green", "red", "blue"]);

            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector2.len(), 3);

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_consecutive_batches_then_emit() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_consecutive_batches_then_emit(&mut group_values);
        }

        pub fn test_three_consecutive_batches_with_partial_emit(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1 = create_dict_array(vec![0, 1], vec!["a", "b"]);
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            let batch2 = create_dict_array(vec![0, 1, 2], vec!["a", "b", "c"]);
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);

            let batch3 = create_dict_array(vec![0, 1,0,1,1,1,1,1,1,0,1,1,0,1,2,1,2], vec!["c", "d","e"]);
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj
                .intern(&[batch3], &mut groups_vector3)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 5);

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            assert!(group_values_trait_obj.is_empty());
            result.iter().for_each(|array| {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                let values: Vec<String> = (0..string_array.len())
                    .map(|i| string_array.value(i).to_string())
                    .collect();
                let unexpected_values: Vec<&String> = values.iter().filter(|v| **v != "a" && **v != "b" && **v != "c" && **v != "d" && **v != "e").collect();
                assert!(
                    unexpected_values.is_empty(),
                    "Emitted unexpected values: {:#?}",
                    unexpected_values
                );
            });
        }

        #[test]
        fn run_test_three_consecutive_batches_with_partial_emit() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_three_consecutive_batches_with_partial_emit(&mut group_values);
        }
    }

    mod state_management {
        use super::*;

        fn test_initial_state_is_empty(group_values_trait_obj: &dyn GroupValues) {
            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_initial_state_is_empty() {
            let group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_initial_state_is_empty(&group_values);
        }

        pub fn test_size_grows_after_intern(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let initial_size = group_values_trait_obj.size();

            let dict_array1 =
                create_dict_array(vec![0, 1, 0, 1, 2], vec!["red", "blue", "green"]);

            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array1], &mut groups_vector1)
                .unwrap();

            let size_after_first_intern = group_values_trait_obj.size();
            assert!(
                size_after_first_intern > initial_size,
                "Size should grow after first intern"
            );

            let dict_array2 = create_dict_array(
                vec![0, 1, 2, 3, 4],
                vec!["yellow", "orange", "purple", "pink", "brown"],
            );

            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array2], &mut groups_vector2)
                .unwrap();

            let size_after_second_intern = group_values_trait_obj.size();
            assert!(
                size_after_second_intern > size_after_first_intern,
                "Size should grow after second intern with new items"
            );

            let dict_array3 =
                create_dict_array(vec![0, 1, 2], vec!["red", "blue", "green"]);

            let mut groups_vector3 = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array3], &mut groups_vector3)
                .unwrap();

            let size_after_third_intern = group_values_trait_obj.size();
            assert_eq!(
                size_after_third_intern, size_after_second_intern,
                "Size should not grow when interning previously seen values"
            );

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after emit all"
            );
        }

        #[test]
        fn run_test_size_grows_after_intern() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_size_grows_after_intern(&mut group_values);
        }

        pub fn test_clear_shrink_resets_state(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array = create_dict_array(vec![0, 1, 0], vec!["red", "blue"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            group_values_trait_obj.clear_shrink(100);
            assert_eq!(group_values_trait_obj.len(), 0);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_clear_shrink_resets_state() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_clear_shrink_resets_state(&mut group_values);
        }

        pub fn test_clear_shrink_with_zero(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 2, 1, 0], vec!["red", "blue", "green"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            group_values_trait_obj.clear_shrink(0);
            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_clear_shrink_with_zero() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_clear_shrink_with_zero(&mut group_values);
        }

        pub fn test_emit_all_clears_state(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(vec![0, 1, 0], vec!["red", "blue"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();

            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_emit_all_clears_state() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_emit_all_clears_state(&mut group_values);
        }

        pub fn test_emit_first_n(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 2], vec!["apple", "banana", "cherry"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);

            let _result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            let _result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_emit_first_n() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_emit_first_n(&mut group_values);
        }

        pub fn test_complex_emit_flow_with_multiple_internS(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1 = create_dict_array(vec![0, 1, 2, 3], vec!["a", "b", "c", "d"]);
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);

            let _result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                2,
                "After emitting 2, should have 2 left"
            );

            let batch2 = create_dict_array(vec![0, 1, 4], vec!["a", "b", "e"]);
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                3,
                "After second intern, should have 3 groups"
            );

            let _result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                2,
                "After emitting 1 more, should have 2 left"
            );

            let batch3 = create_dict_array(vec![2, 5, 6], vec!["a", "f", "g"]);
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj
                .intern(&[batch3], &mut groups_vector3)
                .unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                4,
                "After third intern, should have 4 groups"
            );

            let _result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(
                group_values_trait_obj.is_empty(),
                "After emitting all, should be empty"
            );
            assert_eq!(group_values_trait_obj.len(), 0);
        }
    }

    mod data_correctness {
        use super::*;

        pub fn test_group_assignment_order(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 0, 2, 1], vec!["red", "blue", "green"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(groups_vector.len(), 5);
            assert_eq!(groups_vector[0], groups_vector[2]);
            assert_eq!(groups_vector[1], groups_vector[4]);
        }

        #[test]
        fn run_test_group_assignment_order() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_group_assignment_order(&mut group_values);
        }

        pub fn test_groups_vector_correctness_first_appearance(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array =
                create_dict_array(vec![0, 1, 2, 0, 1, 2], vec!["x", "y", "z"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(groups_vector.len(), 6);
            let group_x = groups_vector[0];
            let group_y = groups_vector[1];
            let group_z = groups_vector[2];

            assert_eq!(
                groups_vector[3], group_x,
                "Fourth row should match first row group"
            );
            assert_eq!(
                groups_vector[4], group_y,
                "Fifth row should match second row group"
            );
            assert_eq!(
                groups_vector[5], group_z,
                "Sixth row should match third row group"
            );
        }

        #[test]
        fn run_test_groups_vector_correctness_first_appearance() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_groups_vector_correctness_first_appearance(&mut group_values);
        }

        pub fn test_groups_vector_sequential_assignment(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array =
                create_dict_array(vec![2, 0, 1], vec!["first", "second", "third"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(groups_vector.len(), 3);
            assert_eq!(
                group_values_trait_obj.len(),
                3,
                "Should have exactly 3 unique groups"
            );
            let all_different = groups_vector[0] != groups_vector[1]
                && groups_vector[1] != groups_vector[2]
                && groups_vector[0] != groups_vector[2];
            assert!(
                all_different,
                "All rows should have different group assignments"
            );
        }

        #[test]
        fn run_test_groups_vector_sequential_assignment() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_groups_vector_sequential_assignment(&mut group_values);
        }

        pub fn test_emit_partial_preserves_state(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let dict_array =
                create_dict_array(vec![0, 1, 2, 3], vec!["a", "b", "c", "d"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);

            let emitted = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_eq!(emitted.len(), 1);
            assert_eq!(
                group_values_trait_obj.len(),
                2,
                "Should have 2 groups remaining after partial emit"
            );

            let emitted_remaining = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(emitted_remaining.len(), 1);
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after final emit"
            );
        }

        #[test]
        fn run_test_emit_partial_preserves_state() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_emit_partial_preserves_state(&mut group_values);
        }

        pub fn test_emit_restores_intern_ability(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1 = create_dict_array(vec![0, 1], vec!["alpha", "beta"]);

            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(group_values_trait_obj.is_empty());

            let batch2 =
                create_dict_array(vec![0, 1, 2], vec!["gamma", "delta", "epsilon"]);

            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                3,
                "Should be able to intern new groups after emit"
            );

            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after second emit"
            );
        }

        #[test]
        fn run_test_emit_restores_intern_ability() {
            let mut group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(&DataType::Utf8);
            test_emit_restores_intern_ability(&mut group_values);
        }
    }
}
