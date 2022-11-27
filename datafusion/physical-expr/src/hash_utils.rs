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

//! Functionality used both on logical and physical plans

use ahash::RandomState;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::{downcast_dictionary_array, downcast_primitive_array};
use arrow_buffer::i256;
use datafusion_common::{
    cast::{
        as_boolean_array, as_generic_binary_array, as_primitive_array, as_string_array,
    },
    DataFusionError, Result,
};
use std::sync::Arc;

// Combines two hashes into one hash
#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(random_state: &RandomState, hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(random_state.hash_one(1), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = random_state.hash_one(1);
        })
    }
}

pub(crate) trait HashValue {
    fn hash_one(&self, state: &RandomState) -> u64;
}

impl<'a, T: HashValue + ?Sized> HashValue for &'a T {
    fn hash_one(&self, state: &RandomState) -> u64 {
        T::hash_one(self, state)
    }
}

macro_rules! hash_value {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }
        })+
    };
}
hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64);
hash_value!(bool, str, [u8]);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));

fn hash_array<T>(
    array: T,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    if array.null_count() == 0 {
        if multi_col {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = combine_hashes(array.value(i).hash_one(random_state), *hash);
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = array.value(i).hash_one(random_state);
            }
        }
    } else if multi_col {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = combine_hashes(array.value(i).hash_one(random_state), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = array.value(i).hash_one(random_state);
            }
        }
    }
}

/// Hash the values in a dictionary array
fn hash_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) -> Result<()> {
    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let values = Arc::clone(array.values());
    let mut dict_hashes = vec![0; values.len()];
    create_hashes(&[values], random_state, &mut dict_hashes)?;

    // combine hash for each index in values
    if multi_col {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = combine_hashes(dict_hashes[key.as_usize()], *hash)
            } // no update for Null, consistent with other hashes
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = dict_hashes[key.as_usize()]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Test version of `create_hashes` that produces the same value for
/// all hashes (to test collisions)
///
/// See comments on `hashes_buffer` for more details
#[cfg(feature = "force_hash_collisions")]
pub fn create_hashes<'a>(
    _arrays: &[ArrayRef],
    _random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    Ok(hashes_buffer)
}

/// Test version of `create_row_hashes` that produces the same value for
/// all hashes (to test collisions)
///
/// See comments on `hashes_buffer` for more details
#[cfg(feature = "force_hash_collisions")]
pub fn create_row_hashes<'a>(
    _rows: &[Vec<u8>],
    _random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    Ok(hashes_buffer)
}

/// Creates hash values for every row, based on their raw bytes.
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_row_hashes<'a>(
    rows: &[Vec<u8>],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    for hash in hashes_buffer.iter_mut() {
        *hash = 0
    }
    for (i, hash) in hashes_buffer.iter_mut().enumerate() {
        *hash = random_state.hash_one(&rows[i]);
    }
    Ok(hashes_buffer)
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[cfg(not(feature = "force_hash_collisions"))]
pub fn create_hashes<'a>(
    arrays: &[ArrayRef],
    random_state: &RandomState,
    hashes_buffer: &'a mut Vec<u64>,
) -> Result<&'a mut Vec<u64>> {
    // combine hashes with `combine_hashes` if we have more than 1 column

    let multi_col = arrays.len() > 1;

    for col in arrays {
        let array = col.as_ref();
        downcast_primitive_array! {
            array => hash_array(array, random_state, hashes_buffer, multi_col),
            DataType::Null => hash_null(random_state, hashes_buffer, multi_col),
            DataType::Boolean => hash_array(as_boolean_array(array)?, random_state, hashes_buffer, multi_col),
            DataType::Utf8 => hash_array(as_string_array(array)?, random_state, hashes_buffer, multi_col),
            DataType::LargeUtf8 => hash_array(as_largestring_array(array), random_state, hashes_buffer, multi_col),
            DataType::Binary => hash_array(as_generic_binary_array::<i32>(array)?, random_state, hashes_buffer, multi_col),
            DataType::LargeBinary => hash_array(as_generic_binary_array::<i64>(array)?, random_state, hashes_buffer, multi_col),
            DataType::FixedSizeBinary(_) => {
                let array: &FixedSizeBinaryArray = array.as_any().downcast_ref().unwrap();
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Decimal128(_, _) => {
                let array = as_primitive_array::<Decimal128Type>(array)?;
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Decimal256(_, _) => {
                let array = as_primitive_array::<Decimal256Type>(array)?;
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                array => hash_dictionary(array, random_state, hashes_buffer, multi_col)?,
                _ => unreachable!()
            }
            _ => {
                // This is internal because we should have caught this before.
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {}",
                    col.data_type()
                )));
            }
        }
    }
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    use crate::from_slice::FromSlice;
    use arrow::{array::*, datatypes::*};
    use std::sync::Arc;

    use super::*;

    #[test]
    fn create_hashes_for_decimal_array() -> Result<()> {
        let array = vec![1, 2, 3, 4]
            .into_iter()
            .map(Some)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(20, 3)
            .unwrap();
        let array_ref = Arc::new(array);
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; array_ref.len()];
        let hashes = create_hashes(&[array_ref], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4);
        Ok(())
    }

    #[test]
    fn create_hashes_for_float_arrays() -> Result<()> {
        let f32_arr = Arc::new(Float32Array::from_slice([0.12, 0.5, 1f32, 444.7]));
        let f64_arr = Arc::new(Float64Array::from_slice([0.12, 0.5, 1f64, 444.7]));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; f32_arr.len()];
        let hashes = create_hashes(&[f32_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        let hashes = create_hashes(&[f64_arr], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 4,);

        Ok(())
    }

    #[test]
    fn create_hashes_binary() -> Result<()> {
        let byte_array = Arc::new(BinaryArray::from_vec(vec![
            &[4, 3, 2],
            &[4, 3, 2],
            &[1, 2, 3],
        ]));

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; byte_array.len()];
        let hashes = create_hashes(&[byte_array], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 3,);

        Ok(())
    }

    #[test]
    fn create_hashes_fixed_size_binary() -> Result<()> {
        let input_arg = vec![vec![1, 2], vec![5, 6], vec![5, 6]];
        let fixed_size_binary_array =
            Arc::new(FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap());

        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let hashes_buff = &mut vec![0; fixed_size_binary_array.len()];
        let hashes =
            create_hashes(&[fixed_size_binary_array], &random_state, hashes_buff)?;
        assert_eq!(hashes.len(), 3,);

        Ok(())
    }

    #[test]
    // Tests actual values of hashes, which are different if forcing collisions
    #[cfg(not(feature = "force_hash_collisions"))]
    fn create_hashes_for_dict_arrays() {
        let strings = vec![Some("foo"), None, Some("bar"), Some("foo"), None];

        let string_array = Arc::new(strings.iter().cloned().collect::<StringArray>());
        let dict_array = Arc::new(
            strings
                .iter()
                .cloned()
                .collect::<DictionaryArray<Int8Type>>(),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let mut string_hashes = vec![0; strings.len()];
        create_hashes(&[string_array], &random_state, &mut string_hashes).unwrap();

        let mut dict_hashes = vec![0; strings.len()];
        create_hashes(&[dict_array], &random_state, &mut dict_hashes).unwrap();

        // Null values result in a zero hash,
        for (val, hash) in strings.iter().zip(string_hashes.iter()) {
            match val {
                Some(_) => assert_ne!(*hash, 0),
                None => assert_eq!(*hash, 0),
            }
        }

        // same logical values should hash to the same hash value
        assert_eq!(string_hashes, dict_hashes);

        // Same values should map to same hash values
        assert_eq!(strings[1], strings[4]);
        assert_eq!(dict_hashes[1], dict_hashes[4]);
        assert_eq!(strings[0], strings[3]);
        assert_eq!(dict_hashes[0], dict_hashes[3]);

        // different strings should map to different hash values
        assert_ne!(strings[0], strings[2]);
        assert_ne!(dict_hashes[0], dict_hashes[2]);
    }

    #[test]
    // Tests actual values of hashes, which are different if forcing collisions
    #[cfg(not(feature = "force_hash_collisions"))]
    fn create_multi_column_hash_for_dict_arrays() {
        let strings1 = vec![Some("foo"), None, Some("bar")];
        let strings2 = vec![Some("blarg"), Some("blah"), None];

        let string_array = Arc::new(strings1.iter().cloned().collect::<StringArray>());
        let dict_array = Arc::new(
            strings2
                .iter()
                .cloned()
                .collect::<DictionaryArray<Int32Type>>(),
        );

        let random_state = RandomState::with_seeds(0, 0, 0, 0);

        let mut one_col_hashes = vec![0; strings1.len()];
        create_hashes(&[dict_array.clone()], &random_state, &mut one_col_hashes).unwrap();

        let mut two_col_hashes = vec![0; strings1.len()];
        create_hashes(
            &[dict_array, string_array],
            &random_state,
            &mut two_col_hashes,
        )
        .unwrap();

        assert_eq!(one_col_hashes.len(), 3);
        assert_eq!(two_col_hashes.len(), 3);

        assert_ne!(one_col_hashes, two_col_hashes);
    }
}
