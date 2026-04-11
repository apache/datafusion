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

use datafusion_common::Result;
use crate::aggregates::group_values::GroupValues;
use arrow::array::ArrayRef;
use datafusion_expr::EmitTo;
pub struct GroupValuesDictionary {}


impl GroupValuesDictionary {
    pub fn new() -> Self {
        Self {}
    }
   
}

impl GroupValues for GroupValuesDictionary {
    fn size(&self) -> usize {
        0
    }
    fn len(&self) -> usize {
        0
    }
    fn is_empty(&self) -> bool {
        true
    }
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        Ok(())
    }
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![])
    }
    fn clear_shrink(&mut self, num_rows: usize) {
        
    }
}

#[cfg(test)]
mod group_values_trait_test {
    use super::*;
    use arrow::array::{DictionaryArray, StringArray, UInt8Array};
    use std::sync::Arc;

    fn create_dict_array(
        keys: Vec<u8>,
        values: Vec<&str>,
    ) -> ArrayRef {
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
    #[test]
    fn test_group_values_dictionary() {
        let mut group_values = GroupValuesDictionary::new();
        run_groupvalue_test_suite(&mut group_values).unwrap();
    }

    fn run_groupvalue_test_suite(group_values_trait_obj: &mut dyn GroupValues) -> Result<()> {
        let tests: Vec<fn(&mut dyn GroupValues)> = vec![
            basic_functionality::test_single_group_all_same_values,
            basic_functionality::test_multiple_groups,
            basic_functionality::test_all_different_values,
            edge_cases::test_empty_batch,
            edge_cases::test_single_row,
            edge_cases::test_repeated_pattern,
            multi_column::test_multiple_columns_passed,
            consecutive_batches::test_consecutive_batches_then_emit,
            consecutive_batches::test_three_consecutive_batches_with_partial_emit,
            state_management::test_size_grows_after_intern,
            state_management::test_clear_shrink_resets_state,
            state_management::test_clear_shrink_with_zero,
            state_management::test_emit_all_clears_state,
            state_management::test_emit_first_n,
            state_management::test_complex_emit_flow_with_multiple_internS,
            data_correctness::test_group_assignment_order,
            data_correctness::test_groups_vector_correctness_first_appearance,
            data_correctness::test_groups_vector_sequential_assignment,
            data_correctness::test_emit_partial_preserves_state,
            data_correctness::test_emit_restores_intern_ability,
        ];
        for test_functions in tests {
            test_functions(group_values_trait_obj);
        }
        
        Ok(())
    }

    mod basic_functionality {
        use super::*;

        pub fn test_single_group_all_same_values(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 0, 0],
                vec!["red"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(groups_vector.len(), 3);
            assert_eq!(group_values_trait_obj.len(), 1);
            assert!(!group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_single_group_all_same_values() {
            let mut group_values = GroupValuesDictionary::new();
            test_single_group_all_same_values(&mut group_values);
        }

        pub fn test_multiple_groups(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 0, 2, 1],
                vec!["red", "blue", "green"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector.len(), 5);
        }

        #[test]
        fn run_test_multiple_groups() {
            let mut group_values = GroupValuesDictionary::new();
            test_multiple_groups(&mut group_values);
        }

        pub fn test_all_different_values(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 3, 4],
                vec!["apple", "banana", "cherry", "date", "elderberry"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 5);
            assert_eq!(groups_vector.len(), 5);
        }

        #[test]
        fn run_test_all_different_values() {
            let mut group_values = GroupValuesDictionary::new();
            test_all_different_values(&mut group_values);
        }
    }

    mod edge_cases {
        use super::*;

        pub fn test_empty_batch(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![],
                vec!["red"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 0);
            assert_eq!(groups_vector.len(), 0);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_empty_batch() {
            let mut group_values = GroupValuesDictionary::new();
            test_empty_batch(&mut group_values);
        }

        pub fn test_single_row(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0],
                vec!["apple"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 1);
            assert_eq!(groups_vector.len(), 1);
            assert_eq!(groups_vector[0], 0);
        }

        #[test]
        fn run_test_single_row() {
            let mut group_values = GroupValuesDictionary::new();
            test_single_row(&mut group_values);
        }

        pub fn test_repeated_pattern(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 0, 1, 2, 0, 1, 2],
                vec!["a", "b", "c"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector.len(), 9);
        }

        #[test]
        fn run_test_repeated_pattern() {
            let mut group_values = GroupValuesDictionary::new();
            test_repeated_pattern(&mut group_values);
        }
    }

    mod multi_column {
        use super::*;

        pub fn test_multiple_columns_passed(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array1 = create_dict_array(
                vec![0, 1, 0],
                vec!["red", "blue"],
            );
            
            let dict_array2 = create_dict_array(
                vec![0, 0, 1],
                vec!["x", "y"],
            );
            
            let mut groups_vector = Vec::new();
            let result = group_values_trait_obj.intern(&[dict_array1, dict_array2], &mut groups_vector);
            assert!(result.is_err(), "Should error when multiple columns are passed (only single column supported)");
        }

        #[test]
        fn run_test_multiple_columns_passed() {
            let mut group_values = GroupValuesDictionary::new();
            test_multiple_columns_passed(&mut group_values);
        }
    }

    mod consecutive_batches {
        use super::*;

        pub fn test_consecutive_batches_then_emit(group_values_trait_obj: &mut dyn GroupValues) {
            let batch1 = create_dict_array(
                vec![0, 1, 0],
                vec!["red", "blue"],
            );
            
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj.intern(&[batch1], &mut groups_vector1).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            assert_eq!(groups_vector1.len(), 3);
            
            let batch2 = create_dict_array(
                vec![0, 1, 2],
                vec!["green", "red", "blue"],
            );
            
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj.intern(&[batch2], &mut groups_vector2).unwrap();
            
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector2.len(), 3);
            
            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_consecutive_batches_then_emit() {
            let mut group_values = GroupValuesDictionary::new();
            test_consecutive_batches_then_emit(&mut group_values);
        }

        pub fn test_three_consecutive_batches_with_partial_emit(group_values_trait_obj: &mut dyn GroupValues) {
            let batch1 = create_dict_array(
                vec![0, 1],
                vec!["a", "b"],
            );
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj.intern(&[batch1], &mut groups_vector1).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            
            let batch2 = create_dict_array(
                vec![0, 1, 2],
                vec!["a", "b", "c"],
            );
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj.intern(&[batch2], &mut groups_vector2).unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);
            
            let batch3 = create_dict_array(
                vec![2, 3],
                vec!["c", "d"],
            );
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj.intern(&[batch3], &mut groups_vector3).unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);
            
            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_three_consecutive_batches_with_partial_emit() {
            let mut group_values = GroupValuesDictionary::new();
            test_three_consecutive_batches_with_partial_emit(&mut group_values);
        }
    }

    mod state_management {
        use super::*;

        fn test_initial_state_is_empty(group_values_trait_obj: &dyn GroupValues) {
            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
            assert_eq!(group_values_trait_obj.size(), 0);
        }

        #[test]
        fn run_test_initial_state_is_empty() {
            let group_values = GroupValuesDictionary::new();
            test_initial_state_is_empty(&group_values);
        }

        pub fn test_size_grows_after_intern(group_values_trait_obj: &mut dyn GroupValues) {
            let initial_size = group_values_trait_obj.size();
            
            let dict_array1 = create_dict_array(
                vec![0, 1, 0, 1, 2],
                vec!["red", "blue", "green"],
            );
            
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj.intern(&[dict_array1], &mut groups_vector1).unwrap();
            
            let size_after_first_intern = group_values_trait_obj.size();
            assert!(size_after_first_intern > initial_size, "Size should grow after first intern");
            
            let dict_array2 = create_dict_array(
                vec![0, 1, 2, 3, 4],
                vec!["yellow", "orange", "purple", "pink", "brown"],
            );
            
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj.intern(&[dict_array2], &mut groups_vector2).unwrap();
            
            let size_after_second_intern = group_values_trait_obj.size();
            assert!(size_after_second_intern > size_after_first_intern, "Size should grow after second intern with new items");
            
            let dict_array3 = create_dict_array(
                vec![0, 1, 2],
                vec!["red", "blue", "green"],
            );
            
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj.intern(&[dict_array3], &mut groups_vector3).unwrap();
            
            let size_after_third_intern = group_values_trait_obj.size();
            assert_eq!(size_after_third_intern, size_after_second_intern, "Size should not grow when interning previously seen values");
            
            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(result.len(), 1);
            assert!(group_values_trait_obj.is_empty(), "Should be empty after emit all");
        }

        #[test]
        fn run_test_size_grows_after_intern() {
            let mut group_values = GroupValuesDictionary::new();
            test_size_grows_after_intern(&mut group_values);
        }

        pub fn test_clear_shrink_resets_state(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 0],
                vec!["red", "blue"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            
            group_values_trait_obj.clear_shrink(100);
            assert_eq!(group_values_trait_obj.len(), 0);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_clear_shrink_resets_state() {
            let mut group_values = GroupValuesDictionary::new();
            test_clear_shrink_resets_state(&mut group_values);
        }

        pub fn test_clear_shrink_with_zero(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 1, 0],
                vec!["red", "blue", "green"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            group_values_trait_obj.clear_shrink(0);
            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_clear_shrink_with_zero() {
            let mut group_values = GroupValuesDictionary::new();
            test_clear_shrink_with_zero(&mut group_values);
        }

        pub fn test_emit_all_clears_state(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 0],
                vec!["red", "blue"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            
            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();
            
            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_emit_all_clears_state() {
            let mut group_values = GroupValuesDictionary::new();
            test_emit_all_clears_state(&mut group_values);
        }

        pub fn test_emit_first_n(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2],
                vec!["apple", "banana", "cherry"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);
            
            let _result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            
            let _result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_emit_first_n() {
            let mut group_values = GroupValuesDictionary::new();
            test_emit_first_n(&mut group_values);
        }

        pub fn test_complex_emit_flow_with_multiple_internS(group_values_trait_obj: &mut dyn GroupValues) {
            let batch1 = create_dict_array(
                vec![0, 1, 2, 3],
                vec!["a", "b", "c", "d"],
            );
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj.intern(&[batch1], &mut groups_vector1).unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);
            
            let _result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2, "After emitting 2, should have 2 left");
            
            let batch2 = create_dict_array(
                vec![0, 1, 4],
                vec!["a", "b", "e"],
            );
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj.intern(&[batch2], &mut groups_vector2).unwrap();
            assert_eq!(group_values_trait_obj.len(), 3, "After second intern, should have 3 groups");
            
            let _result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2, "After emitting 1 more, should have 2 left");
            
            let batch3 = create_dict_array(
                vec![2, 5, 6],
                vec!["a", "f", "g"],
            );
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj.intern(&[batch3], &mut groups_vector3).unwrap();
            assert_eq!(group_values_trait_obj.len(), 4, "After third intern, should have 4 groups");
            
            let _result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(group_values_trait_obj.is_empty(), "After emitting all, should be empty");
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_complex_emit_flow_with_multiple_internS() {
            let mut group_values = GroupValuesDictionary::new();
            test_complex_emit_flow_with_multiple_internS(&mut group_values);
        }
    }

    mod data_correctness {
        use super::*;

        pub fn test_group_assignment_order(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 0, 2, 1],
                vec!["red", "blue", "green"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(groups_vector.len(), 5);
            assert_eq!(groups_vector[0], groups_vector[2]);
            assert_eq!(groups_vector[1], groups_vector[4]);
        }

        #[test]
        fn run_test_group_assignment_order() {
            let mut group_values = GroupValuesDictionary::new();
            test_group_assignment_order(&mut group_values);
        }

        pub fn test_groups_vector_correctness_first_appearance(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 0, 1, 2],
                vec!["x", "y", "z"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(groups_vector.len(), 6);
            let group_x = groups_vector[0];
            let group_y = groups_vector[1];
            let group_z = groups_vector[2];
            
            assert_eq!(groups_vector[3], group_x, "Fourth row should match first row group");
            assert_eq!(groups_vector[4], group_y, "Fifth row should match second row group");
            assert_eq!(groups_vector[5], group_z, "Sixth row should match third row group");
        }

        #[test]
        fn run_test_groups_vector_correctness_first_appearance() {
            let mut group_values = GroupValuesDictionary::new();
            test_groups_vector_correctness_first_appearance(&mut group_values);
        }

        pub fn test_groups_vector_sequential_assignment(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![2, 0, 1],
                vec!["first", "second", "third"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            
            assert_eq!(groups_vector.len(), 3);
            assert_eq!(group_values_trait_obj.len(), 3, "Should have exactly 3 unique groups");
            let all_different = groups_vector[0] != groups_vector[1] && groups_vector[1] != groups_vector[2] && groups_vector[0] != groups_vector[2];
            assert!(all_different, "All rows should have different group assignments");
        }

        #[test]
        fn run_test_groups_vector_sequential_assignment() {
            let mut group_values = GroupValuesDictionary::new();
            test_groups_vector_sequential_assignment(&mut group_values);
        }

        pub fn test_emit_partial_preserves_state(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(
                vec![0, 1, 2, 3],
                vec!["a", "b", "c", "d"],
            );
            
            let mut groups_vector = Vec::new();
            group_values_trait_obj.intern(&[dict_array], &mut groups_vector).unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);
            
            let emitted = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_eq!(emitted.len(), 1);
            assert_eq!(group_values_trait_obj.len(), 2, "Should have 2 groups remaining after partial emit");
            
            let emitted_remaining = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_eq!(emitted_remaining.len(), 1);
            assert!(group_values_trait_obj.is_empty(), "Should be empty after final emit");
        }

        #[test]
        fn run_test_emit_partial_preserves_state() {
            let mut group_values = GroupValuesDictionary::new();
            test_emit_partial_preserves_state(&mut group_values);
        }

        pub fn test_emit_restores_intern_ability(group_values_trait_obj: &mut dyn GroupValues) {
            let batch1 = create_dict_array(
                vec![0, 1],
                vec!["alpha", "beta"],
            );
            
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj.intern(&[batch1], &mut groups_vector1).unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);
            
            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(group_values_trait_obj.is_empty());
            
            let batch2 = create_dict_array(
                vec![0, 1, 2],
                vec!["gamma", "delta", "epsilon"],
            );
            
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj.intern(&[batch2], &mut groups_vector2).unwrap();
            assert_eq!(group_values_trait_obj.len(), 3, "Should be able to intern new groups after emit");
            
            let _ = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert!(group_values_trait_obj.is_empty(), "Should be empty after second emit");
        }

        #[test]
        fn run_test_emit_restores_intern_ability() {
            let mut group_values = GroupValuesDictionary::new();
            test_emit_restores_intern_ability(&mut group_values);
        }
    }
}