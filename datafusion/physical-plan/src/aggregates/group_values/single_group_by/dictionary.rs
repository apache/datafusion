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
use crate::hash_utils::RandomState;
use arrow::array::{Array, ArrayRef, DictionaryArray, StringArray};
use arrow::array::{Datum, PrimitiveBuilder};
use arrow::array::{Int32Array, PrimitiveArray};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowNativeType, DataType};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::EmitTo;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use std::{mem, usize};

pub struct GroupValuesDictionary<K: ArrowDictionaryKeyType + Send> {
    /*
    We know that every single &[ArrayRef] that is passed in is a dictionary array

    self.inter() will be called across record batches, this means that
    we cannot rely on a trivial approach where we just store the dictionary mapping as it is



    Possible soluitions:
    1A. store a hashmap that last across .intern() calls
        | cast cols:&[ArrayRef] to generic Dictionary array, check if weve already stored its values (unique values) before
        | if we have check the current mapping internally and update the groups array with the initial mapping for this value
        | if it does not exist already (hashmap.size) is the group_id for this element
    1B. how do we retrieve the dictionary encoded array this function expects?
        | NOTE: emit returns one value per group not one value per row. The group values are the distinct values in the order they were first seen — not the full expanded key array [one per group index]
        | keep a value_order array that stores unique elements the first time their seen, this maintains order for self.emit()
        | the return type of the array self.emit() returns is based on the value type of the dictionary, may be smart to have an internal Group values that handles that logic
        |

    Possible optimizations (Ignore for now)
    2A. dont rely directly in a hashmap we could hash all of the values at once and then as we iterate the keys array refer to them as the values are assumed to be smaller than the keys
        | at the start of self.intern hash every value in the dictionary
        | iterate through the keys section of dict_array
            | for each key check its corresponding value and if it exist


    */
    // stores the order new unique elements are seen for self.emit()
    seen_elements: Vec<Vec<u8>>, //  Box<dyn Builder> doesnt provide the flexibility of building partition arrays that wed need to support emit::First(N)
    value_dt: DataType,
    _phantom: PhantomData<K>,
    // keeps track of which values weve already seen. stored as -> <unique_value_hash:(initial_group_id, raw_bytes)>
    unique_dict_value_mapping: HashMap<u64, Vec<(usize, Vec<u8>)>>,
    random_state: RandomState,
}

impl<K: ArrowDictionaryKeyType + Send> GroupValuesDictionary<K> {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            seen_elements: Vec::new(),
            unique_dict_value_mapping: HashMap::new(),
            value_dt: data_type.clone(),
            _phantom: PhantomData,
            random_state: RandomState::default(),
        }
    }
    fn compute_value_hashes(&mut self, values: &ArrayRef) -> Result<Vec<u64>> {
        let mut hashes = vec![0u64; values.len()];
        create_hashes(&[values.clone()], &self.random_state, &mut hashes)?;
        Ok(hashes)
    }
    fn keys_to_usize(keys: &PrimitiveArray<K>) -> Vec<Option<usize>> {
        keys.iter()
            .map(|k| k.map(|v| v.to_usize().unwrap()))
            .collect()
    }

    // TODO: This may be a good spot to optimize since, even in the best case where the row exist in the cache, we are still having to compute its byte reprsention
    // although this is how data is stored internally in arrow it may be worth looking into this
    fn get_raw_bytes<'a>(values: &'a ArrayRef, index: usize) -> &'a [u8] {
        match values.data_type() {
            DataType::Utf8 => values
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray")
                .value(index)
                .as_bytes(),
            // TODO: add branches for LargeUtf8, Binary, LargeBinary, primitives etc
            other => unimplemented!("get_raw_bytes not implemented for {other:?}"),
        }
    }
    // space efficient minimal representation of null values for dictionary value types that require raw byte comparisons, this allows us to avoid special casing nulls in the hash map and just treat them as a normal value with a specific raw byte representation
    fn sentinel_repr(dt: &DataType) -> Vec<u8> {
        match dt {
            DataType::Utf8 => [0xFF, 0xFF, 0xFF, 0xFF].to_vec(),
            // this cant appear in valid utf8 so no risk of collision with real values, we can use this as the raw byte representation for nulls to simplify logic and avoid special casing nulls in the hash map
            _ => unimplemented!("sentinel_repr not implemented for binary types"),
        }
    }
}

impl<K: ArrowDictionaryKeyType + Send> GroupValues for GroupValuesDictionary<K> {
    // not really sure how to return the size of strings and binary values so this is a best effort approach
    fn size(&self) -> usize {
        let arr_size =
            element_size(&self.value_dt) * self.unique_dict_value_mapping.len();
        let dict_size = self.unique_dict_value_mapping.len() * size_of::<(ScalarValue, usize)>() + 100 /* rough estimate for hashmap overhead */;
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
        let array = Arc::clone(&cols[0]);
        groups.clear(); // zero out buffer
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<K>>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "GroupValuesDictionary expected DictionaryArray but got {:?}",
                    array.data_type()
                ))
            })?;

        // pre-allocate space for seen_elements using occupancy
        // occupancy count gives us the number of truly distinct non-null values in this batch
        let occupied = dict_array.occupancy().count_set_bits();
        self.seen_elements.reserve(occupied);

        let values = dict_array.values();
        let key_array = dict_array.keys();

        // compute hashes for all values in the values array upfront
        // value_hashes[i] corresponds to values[i]
        let value_hashes = self.compute_value_hashes(values)?;

        // convert key array to Vec<usize> for cheap indexed access
        // avoids repeated .value(i).to_usize() calls in the hot loop
        let keys_as_usize = Self::keys_to_usize(key_array);

        // for each of the values check if its already been stored in the hashtable
        // 1. if it has grab the corresponding initial group integer assigned to it
        // 2. if it has not its group integer is self.seen_elements.len - 1 and then store this mapping
        for i in 0..key_array.len() {
            let hash = match keys_as_usize[i] {
                None => (usize::MAX - 1) as u64,
                Some(key) => value_hashes[key],
            };

            let group_id =
                if let Some(entries) = self.unique_dict_value_mapping.get(&hash) {
                    if hash == (usize::MAX - 1) as u64 {
                        // null case - all nulls map to same group, just grab first entry
                        entries[0].0
                    } else {
                        // non-null case - find matching entry by raw byte comparison
                        let raw = Self::get_raw_bytes(values, keys_as_usize[i].unwrap());
                        if let Some((group_id, _)) = entries
                            .iter()
                            .find(|(_, stored_bytes)| raw == stored_bytes.as_slice())
                        {
                            *group_id
                        } else {
                            // hash collision - new unique value that hashed to the same hash as a previous value, assign new group id and store in mapping
                            let new_group_id = self.seen_elements.len();
                            let raw_bytes = raw.to_vec();
                            self.seen_elements.push(raw_bytes.clone());
                            self.unique_dict_value_mapping
                                .get_mut(&hash)
                                .unwrap()
                                .push((new_group_id, raw_bytes));
                            new_group_id
                        }
                    }
                } else {
                    // completely new hash - new group
                    let new_group_id = self.seen_elements.len();
                    let raw_bytes = match keys_as_usize[i] {
                        None => Self::sentinel_repr(&values.data_type()),
                        Some(key) => Self::get_raw_bytes(values, key).to_vec(),
                    };
                    self.seen_elements.push(raw_bytes.clone());
                    self.unique_dict_value_mapping
                        .insert(hash, vec![(new_group_id, raw_bytes)]);
                    new_group_id
                };
            groups.push(group_id);
        }
        Ok(())
    }
    // This needs to return a dictionary encoded array
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Err(datafusion_common::DataFusionError::Execution(
            "".to_string(),
        ))
        /*let columns: Vec<ScalarValue> = match emit_to {
            EmitTo::All => {
                self.unique_dict_value_mapping.clear();
                mem::take(&mut self.seen_elements)
            }
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
        let n = columns.len();

        // keys are just 0..n since each group maps to exactly one distinct value
        let mut keys_builder = PrimitiveBuilder::<K>::with_capacity(n);
        for i in 0..n {
            keys_builder.append_value(K::Native::usize_as(i));
        }
        let keys = keys_builder.finish();
        // values are the distinct scalars in order
        let values = ScalarValue::iter_to_array(columns.into_iter())?;

        let dict_array = DictionaryArray::<K>::try_new(keys, values)?;
        Ok(vec![Arc::new(dict_array)])*/
    }
    fn clear_shrink(&mut self, num_rows: usize) {
        self.seen_elements.clear();
        self.seen_elements.shrink_to(num_rows);
        self.unique_dict_value_mapping.clear();
        self.unique_dict_value_mapping.shrink_to(num_rows);
    }
}
fn element_size(dt: &DataType) -> usize {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => 20, // rough estimate for average string size
        DataType::Binary | DataType::LargeBinary => 40, // rough estimate for average binary size
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

    // Helper function to validate that emitted arrays are DictionaryArrays with the correct type
    fn assert_emitted_is_dict_array(result: &[ArrayRef]) {
        assert_eq!(result.len(), 1, "Expected exactly one array in emit result");
        let array = &result[0];

        match array.data_type() {
            DataType::Dictionary(key_type, value_type) => {
                // Verify it's the expected key type (UInt8 in our tests)
                match key_type.as_ref() {
                    DataType::UInt8 => {}
                    other => panic!("Expected UInt8 key type, got {other:?}"),
                }

                // Verify it's the expected value type (Utf8 in our tests)
                match value_type.as_ref() {
                    DataType::Utf8 => {}
                    other => panic!("Expected Utf8 value type, got {other:?}"),
                }
            }
            other => panic!("Expected DictionaryArray, got {other:?}"),
        }

        // Now verify we can actually downcast to the expected types
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
            .expect("Failed to downcast to DictionaryArray<UInt8Type>");

        let _values = dict_array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Dictionary values should be StringArray");
    }

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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_single_group_all_same_values(&mut group_values);
        }

        pub fn test_multiple_groups(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array =
                create_dict_array(vec![0, 1, 0, 2, 1], vec!["red", "blue", "green"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector.len(), 5);
        }

        #[test]
        fn run_test_multiple_groups() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_multiple_groups(&mut group_values);
        }

        pub fn test_multiple_groups_with_nulls(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let keys = UInt8Array::from(vec![Some(0), None, Some(1), None, Some(0)]);
            let values = StringArray::from(vec!["red", "blue"]);
            let dict_array = Arc::new(
                DictionaryArray::<arrow::datatypes::UInt8Type>::try_new(
                    keys,
                    Arc::new(values),
                )
                .unwrap(),
            ) as ArrayRef;

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            assert_eq!(groups_vector.len(), 5);
            assert_eq!(group_values_trait_obj.len(), 3);
            assert_eq!(groups_vector[1], groups_vector[3]);
            assert_eq!(groups_vector[0], groups_vector[4]);
            assert_ne!(groups_vector[0], groups_vector[1]);
            assert_ne!(groups_vector[2], groups_vector[1]);
        }

        #[test]
        fn run_test_multiple_groups_with_nulls() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_multiple_groups_with_nulls(&mut group_values);
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_repeated_pattern(&mut group_values);
        }

        pub fn test_null_heavy_mixed_values(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let keys = UInt8Array::from(vec![
                None,
                None,
                Some(0u8),
                None,
                Some(1u8),
                None,
                Some(0u8),
                Some(1u8),
                None,
                Some(2u8),
                None,
            ]);
            let values = StringArray::from(vec!["red", "blue", "green"]);
            let dict_array = Arc::new(
                DictionaryArray::<arrow::datatypes::UInt8Type>::try_new(
                    keys,
                    Arc::new(values),
                )
                .unwrap(),
            ) as ArrayRef;

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();

            // groups are: null + red + blue + green
            assert_eq!(group_values_trait_obj.len(), 4);
            assert_eq!(groups_vector.len(), 11);

            // all null rows should map to one group
            let null_group = groups_vector[0];
            assert_eq!(groups_vector[1], null_group);
            assert_eq!(groups_vector[3], null_group);
            assert_eq!(groups_vector[5], null_group);
            assert_eq!(groups_vector[8], null_group);
            assert_eq!(groups_vector[10], null_group);

            // repeated non-null values should map consistently
            assert_eq!(groups_vector[2], groups_vector[6]); // red
            assert_eq!(groups_vector[4], groups_vector[7]); // blue

            // null and non-null groups should remain distinct
            assert_ne!(groups_vector[2], null_group);
            assert_ne!(groups_vector[4], null_group);
            assert_ne!(groups_vector[9], null_group);
        }

        #[test]
        fn run_test_null_heavy_mixed_values() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_null_heavy_mixed_values(&mut group_values);
        }

        pub fn test_null_group_stable_across_batches_with_reordered_dict(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1_keys = UInt8Array::from(vec![None, Some(0u8), None, Some(1u8)]);
            let batch1_values = StringArray::from(vec!["a", "b"]);
            let batch1 = Arc::new(
                DictionaryArray::<arrow::datatypes::UInt8Type>::try_new(
                    batch1_keys,
                    Arc::new(batch1_values),
                )
                .unwrap(),
            ) as ArrayRef;

            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 3); // null + a + b
            let null_group = groups_vector1[0];
            let a_group = groups_vector1[1];
            let b_group = groups_vector1[3];
            assert_eq!(groups_vector1[2], null_group);

            // Same logical values, but dictionary value ordering changed: ["a", "c", "b"]
            let batch2_keys =
                UInt8Array::from(vec![Some(0u8), None, Some(2u8), None, Some(1u8)]);
            let batch2_values = StringArray::from(vec!["a", "c", "b"]);
            let batch2 = Arc::new(
                DictionaryArray::<arrow::datatypes::UInt8Type>::try_new(
                    batch2_keys,
                    Arc::new(batch2_values),
                )
                .unwrap(),
            ) as ArrayRef;

            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();

            assert_eq!(group_values_trait_obj.len(), 4); // adds only new value "c"
            assert_eq!(groups_vector2[0], a_group); // "a" should reuse prior group
            assert_eq!(groups_vector2[1], null_group);
            assert_eq!(groups_vector2[3], null_group);
            assert_eq!(groups_vector2[2], b_group); // "b" should reuse prior group
            assert_ne!(groups_vector2[4], null_group); // "c" is not null
            assert_ne!(groups_vector2[4], a_group);
            assert_ne!(groups_vector2[4], b_group);
        }

        #[test]
        fn run_test_null_group_stable_across_batches_with_reordered_dict() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_null_group_stable_across_batches_with_reordered_dict(&mut group_values);
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            assert_emitted_is_dict_array(&result);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_consecutive_batches_then_emit() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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

            let batch3 = create_dict_array(
                vec![0, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 2, 1, 2],
                vec!["c", "d", "e"],
            );
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj
                .intern(&[batch3], &mut groups_vector3)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 5);

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&result);
            assert!(group_values_trait_obj.is_empty());
            result.iter().for_each(|array| {
                let dict_array = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<arrow::datatypes::UInt8Type>>()
                    .unwrap();
                let values = dict_array.values();
                let string_array = values.as_any().downcast_ref::<StringArray>().unwrap();
                let value_strings: Vec<String> = (0..string_array.len())
                    .map(|i| string_array.value(i).to_string())
                    .collect();
                let unexpected_values: Vec<&String> = value_strings
                    .iter()
                    .filter(|v| {
                        **v != "a" && **v != "b" && **v != "c" && **v != "d" && **v != "e"
                    })
                    .collect();
                assert!(
                    unexpected_values.is_empty(),
                    "Emitted unexpected values: {unexpected_values:#?}"
                );
            });
        }

        #[test]
        fn run_test_three_consecutive_batches_with_partial_emit() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let group_values = GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                &DataType::Utf8,
            );
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
            assert_emitted_is_dict_array(&result);
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after emit all"
            );
        }

        #[test]
        fn run_test_size_grows_after_intern() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_clear_shrink_with_zero(&mut group_values);
        }

        pub fn test_emit_all_clears_state(group_values_trait_obj: &mut dyn GroupValues) {
            let dict_array = create_dict_array(vec![0, 1, 0], vec!["red", "blue"]);

            let mut groups_vector = Vec::new();
            group_values_trait_obj
                .intern(&[dict_array], &mut groups_vector)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 2);

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&result);

            assert!(group_values_trait_obj.is_empty());
            assert_eq!(group_values_trait_obj.len(), 0);
        }

        #[test]
        fn run_test_emit_all_clears_state() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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

            let result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_emitted_is_dict_array(&result);
            assert_eq!(group_values_trait_obj.len(), 2);

            let result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_emitted_is_dict_array(&result);
            assert!(group_values_trait_obj.is_empty());
        }

        #[test]
        fn run_test_emit_first_n() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_emit_first_n(&mut group_values);
        }

        pub fn test_complex_emit_flow_with_multiple_intern(
            group_values_trait_obj: &mut dyn GroupValues,
        ) {
            let batch1 = create_dict_array(vec![0, 1, 2, 3], vec!["a", "b", "c", "d"]);
            let mut groups_vector1 = Vec::new();
            group_values_trait_obj
                .intern(&[batch1], &mut groups_vector1)
                .unwrap();
            assert_eq!(group_values_trait_obj.len(), 4);

            let result = group_values_trait_obj.emit(EmitTo::First(2)).unwrap();
            assert_emitted_is_dict_array(&result);
            assert_eq!(
                group_values_trait_obj.len(),
                2,
                "After emitting 2, should have 2 left (c, d)"
            );

            let batch2 = create_dict_array(vec![0, 1, 2], vec!["a", "b", "e"]);
            let mut groups_vector2 = Vec::new();
            group_values_trait_obj
                .intern(&[batch2], &mut groups_vector2)
                .unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                5,
                "After second intern: 2 remaining (c,d) + 3 new from batch2 (a,b,e) = 5 groups"
            );

            let result = group_values_trait_obj.emit(EmitTo::First(1)).unwrap();
            assert_emitted_is_dict_array(&result);
            assert_eq!(
                group_values_trait_obj.len(),
                4,
                "After emitting 1 more (c), should have 4 left (d,a,b,e)"
            );

            let batch3 = create_dict_array(vec![0, 1, 2], vec!["a", "f", "g"]);
            let mut groups_vector3 = Vec::new();
            group_values_trait_obj
                .intern(&[batch3], &mut groups_vector3)
                .unwrap();
            assert_eq!(
                group_values_trait_obj.len(),
                6,
                "After third intern: 4 remaining (d,a,b,e) + 2 new from batch3 (f,g) = 6 groups (a already exists)"
            );

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&result);
            assert!(
                group_values_trait_obj.is_empty(),
                "After emitting all, should be empty"
            );
            assert_eq!(group_values_trait_obj.len(), 0);
        }
        #[test]
        fn run_test_complex_emit_flow_with_multiple_intern() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_complex_emit_flow_with_multiple_intern(&mut group_values);
        }
    }

    mod data_correctness {
        use super::*;
        use arrow::array::Int32Array;

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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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
            assert_emitted_is_dict_array(&emitted);
            assert_eq!(
                group_values_trait_obj.len(),
                2,
                "Should have 2 groups remaining after partial emit"
            );

            let emitted_remaining = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&emitted_remaining);
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after final emit"
            );
        }

        #[test]
        fn run_test_emit_partial_preserves_state() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
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

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&result);
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

            let result = group_values_trait_obj.emit(EmitTo::All).unwrap();
            assert_emitted_is_dict_array(&result);
            assert!(
                group_values_trait_obj.is_empty(),
                "Should be empty after second emit"
            );
        }

        #[test]
        fn run_test_emit_restores_intern_ability() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::UInt8Type>::new(
                    &DataType::Utf8,
                );
            test_emit_restores_intern_ability(&mut group_values);
        }
        fn test_null_keys_form_single_group(
            group_values: &mut dyn GroupValues,
        ) -> Result<()> {
            // keys: [0, null, 1, null, 0]
            // values: ["a", "b"]
            // null keys should all map to the same group
            let keys = Int32Array::from(vec![Some(0), None, Some(1), None, Some(0)]);
            let values = StringArray::from(vec!["a", "b"]);
            let dict = Arc::new(DictionaryArray::new(keys, Arc::new(values))) as ArrayRef;

            let mut groups = Vec::new();
            group_values.intern(&[dict], &mut groups)?;

            // should have 3 groups: "a", "b", null
            assert_eq!(group_values.len(), 3);
            // null rows (index 1 and 3) should map to same group
            assert_eq!(groups[1], groups[3]);
            // non null rows should map to correct groups
            assert_eq!(groups[0], groups[4]); // both "a"
            assert_ne!(groups[0], groups[2]); // "a" != "b"
            Ok(())
        }
        #[test]
        fn run_test_null_keys_form_single_group() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::Int32Type>::new(
                    &DataType::Utf8,
                );
            test_null_keys_form_single_group(&mut group_values).unwrap();
        }

        fn test_null_values_in_dictionary_form_single_group(
            group_values: &mut dyn GroupValues,
        ) -> Result<()> {
            // keys: [0, 1, 2, 1, 0]
            // values: ["a", null, "b"]
            // keys pointing to null value should all map to same group
            let keys = Int32Array::from(vec![0, 1, 2, 1, 0]);
            let values = StringArray::from(vec![Some("a"), None, Some("b")]);
            let dict = Arc::new(DictionaryArray::new(keys, Arc::new(values))) as ArrayRef;

            let mut groups = Vec::new();
            group_values.intern(&[dict], &mut groups)?;

            // should have 3 groups: "a", null, "b"
            assert_eq!(group_values.len(), 3);
            // rows pointing to null value (index 1 and 3) should map to same group
            assert_eq!(groups[1], groups[3]);
            // non null rows should map correctly
            assert_eq!(groups[0], groups[4]); // both "a"
            assert_ne!(groups[0], groups[2]); // "a" != "b"
            Ok(())
        }
        #[test]
        fn run_test_null_values_in_dictionary_form_single_group() {
            let mut group_values =
                GroupValuesDictionary::<arrow::datatypes::Int32Type>::new(
                    &DataType::Utf8,
                );
            test_null_values_in_dictionary_form_single_group(&mut group_values).unwrap();
        }
    }
}
