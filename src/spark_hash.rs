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

//! This includes utilities for hashing and murmur3 hashing.

use arrow::{
    compute::take,
    datatypes::{ArrowNativeTypeOp, UInt16Type, UInt32Type, UInt64Type, UInt8Type},
};
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::*,
        datatypes::{
            ArrowDictionaryKeyType, ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type,
            Int8Type, TimeUnit,
        },
    },
    error::{DataFusionError, Result},
};

use crate::xxhash64::spark_compatible_xxhash64;

/// Spark-compatible murmur3 hash function
#[inline]
pub fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
    #[inline]
    fn mix_k1(mut k1: i32) -> i32 {
        k1 = k1.mul_wrapping(0xcc9e2d51u32 as i32);
        k1 = k1.rotate_left(15);
        k1 = k1.mul_wrapping(0x1b873593u32 as i32);
        k1
    }

    #[inline]
    fn mix_h1(mut h1: i32, k1: i32) -> i32 {
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.mul_wrapping(5).add_wrapping(0xe6546b64u32 as i32);
        h1
    }

    #[inline]
    fn fmix(mut h1: i32, len: i32) -> i32 {
        h1 ^= len;
        h1 ^= (h1 as u32 >> 16) as i32;
        h1 = h1.mul_wrapping(0x85ebca6bu32 as i32);
        h1 ^= (h1 as u32 >> 13) as i32;
        h1 = h1.mul_wrapping(0xc2b2ae35u32 as i32);
        h1 ^= (h1 as u32 >> 16) as i32;
        h1
    }

    #[inline]
    unsafe fn hash_bytes_by_int(data: &[u8], seed: u32) -> i32 {
        // safety: data length must be aligned to 4 bytes
        let mut h1 = seed as i32;
        for i in (0..data.len()).step_by(4) {
            let ints = data.as_ptr().add(i) as *const i32;
            let mut half_word = ints.read_unaligned();
            if cfg!(target_endian = "big") {
                half_word = half_word.reverse_bits();
            }
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        h1
    }
    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // safety:
    // avoid boundary checking in performance critical codes.
    // all operations are guaranteed to be safe
    // data is &[u8] so we do not need to check for proper alignment
    unsafe {
        let mut h1 = if len_aligned > 0 {
            hash_bytes_by_int(&data[0..len_aligned], seed)
        } else {
            seed as i32
        };

        for i in len_aligned..len {
            let half_word = *data.get_unchecked(i) as i8 as i32;
            h1 = mix_h1(h1, mix_k1(half_word));
        }
        fmix(h1, len as i32) as u32
    }
}

macro_rules! hash_array {
    ($array_type: ident, $column: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = $hash_method(&array.value(i), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = $hash_method(&array.value(i), *hash);
                }
            }
        }
    };
}

macro_rules! hash_array_boolean {
    ($array_type: ident, $column: ident, $hash_input_type: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = $hash_method($hash_input_type::from(array.value(i)).to_le_bytes(), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        $hash_method($hash_input_type::from(array.value(i)).to_le_bytes(), *hash);
                }
            }
        }
    };
}

macro_rules! hash_array_primitive {
    ($array_type: ident, $column: ident, $ty: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                *hash = $hash_method((*value as $ty).to_le_bytes(), *hash);
            }
        } else {
            for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                if !array.is_null(i) {
                    *hash = $hash_method((*value as $ty).to_le_bytes(), *hash);
                }
            }
        }
    };
}

macro_rules! hash_array_primitive_float {
    ($array_type: ident, $column: ident, $ty: ident, $ty2: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();
        let values = array.values();

        if array.null_count() == 0 {
            for (hash, value) in $hashes.iter_mut().zip(values.iter()) {
                // Spark uses 0 as hash for -0.0, see `Murmur3Hash` expression.
                if *value == 0.0 && value.is_sign_negative() {
                    *hash = $hash_method((0 as $ty2).to_le_bytes(), *hash);
                } else {
                    *hash = $hash_method((*value as $ty).to_le_bytes(), *hash);
                }
            }
        } else {
            for (i, (hash, value)) in $hashes.iter_mut().zip(values.iter()).enumerate() {
                if !array.is_null(i) {
                    // Spark uses 0 as hash for -0.0, see `Murmur3Hash` expression.
                    if *value == 0.0 && value.is_sign_negative() {
                        *hash = $hash_method((0 as $ty2).to_le_bytes(), *hash);
                    } else {
                        *hash = $hash_method((*value as $ty).to_le_bytes(), *hash);
                    }
                }
            }
        }
    };
}

macro_rules! hash_array_decimal {
    ($array_type:ident, $column: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column.as_any().downcast_ref::<$array_type>().unwrap();

        if array.null_count() == 0 {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                *hash = $hash_method(array.value(i).to_le_bytes(), *hash);
            }
        } else {
            for (i, hash) in $hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = $hash_method(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
    };
}

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u32],
    first_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !first_col {
        // unpack the dictionary array as each row may have a different hash input
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        create_murmur3_hashes(&[unpacked], hashes_buffer)?;
    } else {
        // For the first column, hash each dictionary value once, and then use
        // that computed hash for each key value to avoid a potentially
        // expensive redundant hashing for large dictionary elements (e.g. strings)
        let dict_values = Arc::clone(dict_array.values());
        // same initial seed as Spark
        let mut dict_hashes = vec![42; dict_values.len()];
        create_murmur3_hashes(&[dict_values], &mut dict_hashes)?;
        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    ))
                })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

// Hash the values in a dictionary array using xxhash64
fn create_xxhash64_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u64],
    first_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !first_col {
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        create_xxhash64_hashes(&[unpacked], hashes_buffer)?;
    } else {
        // Hash each dictionary value once, and then use that computed
        // hash for each key value to avoid a potentially expensive
        // redundant hashing for large dictionary elements (e.g. strings)
        let dict_values = Arc::clone(dict_array.values());
        // same initial seed as Spark
        let mut dict_hashes = vec![42u64; dict_values.len()];
        create_xxhash64_hashes(&[dict_values], &mut dict_hashes)?;

        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    ))
                })?;
                *hash = dict_hashes[idx]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
///
/// `hash_method` is the hash function to use.
/// `create_dictionary_hash_method` is the function to create hashes for dictionary arrays input.
macro_rules! create_hashes_internal {
    ($arrays: ident, $hashes_buffer: ident, $hash_method: ident, $create_dictionary_hash_method: ident) => {
        for (i, col) in $arrays.iter().enumerate() {
            let first_col = i == 0;
            match col.data_type() {
                DataType::Boolean => {
                    hash_array_boolean!(BooleanArray, col, i32, $hashes_buffer, $hash_method);
                }
                DataType::Int8 => {
                    hash_array_primitive!(Int8Array, col, i32, $hashes_buffer, $hash_method);
                }
                DataType::Int16 => {
                    hash_array_primitive!(Int16Array, col, i32, $hashes_buffer, $hash_method);
                }
                DataType::Int32 => {
                    hash_array_primitive!(Int32Array, col, i32, $hashes_buffer, $hash_method);
                }
                DataType::Int64 => {
                    hash_array_primitive!(Int64Array, col, i64, $hashes_buffer, $hash_method);
                }
                DataType::Float32 => {
                    hash_array_primitive_float!(
                        Float32Array,
                        col,
                        f32,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float64 => {
                    hash_array_primitive_float!(
                        Float64Array,
                        col,
                        f64,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    hash_array_primitive!(
                        TimestampSecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    hash_array_primitive!(
                        TimestampMillisecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    hash_array_primitive!(
                        TimestampMicrosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    hash_array_primitive!(
                        TimestampNanosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date32 => {
                    hash_array_primitive!(Date32Array, col, i32, $hashes_buffer, $hash_method);
                }
                DataType::Date64 => {
                    hash_array_primitive!(Date64Array, col, i64, $hashes_buffer, $hash_method);
                }
                DataType::Utf8 => {
                    hash_array!(StringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeUtf8 => {
                    hash_array!(LargeStringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Binary => {
                    hash_array!(BinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeBinary => {
                    hash_array!(LargeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::FixedSizeBinary(_) => {
                    hash_array!(FixedSizeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Decimal128(_, _) => {
                    hash_array_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
                }
                DataType::Dictionary(index_type, _) => match **index_type {
                    DataType::Int8 => {
                        $create_dictionary_hash_method::<Int8Type>(col, $hashes_buffer, first_col)?;
                    }
                    DataType::Int16 => {
                        $create_dictionary_hash_method::<Int16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int32 => {
                        $create_dictionary_hash_method::<Int32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int64 => {
                        $create_dictionary_hash_method::<Int64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt8 => {
                        $create_dictionary_hash_method::<UInt8Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt16 => {
                        $create_dictionary_hash_method::<UInt16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt32 => {
                        $create_dictionary_hash_method::<UInt32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt64 => {
                        $create_dictionary_hash_method::<UInt64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unsupported dictionary type in hasher hashing: {}",
                            col.data_type(),
                        )))
                    }
                },
                _ => {
                    // This is internal because we should have caught this before.
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type in hasher: {}",
                        col.data_type()
                    )));
                }
            }
        }
    };
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_murmur3_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut [u32],
) -> Result<&'a mut [u32]> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        spark_compatible_murmur3_hash,
        create_hashes_dictionary
    );
    Ok(hashes_buffer)
}

/// Creates xxhash64 hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_xxhash64_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut [u64],
) -> Result<&'a mut [u64]> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        spark_compatible_xxhash64,
        create_xxhash64_hashes_dictionary
    );
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float32Array, Float64Array};
    use std::sync::Arc;

    use super::{create_murmur3_hashes, create_xxhash64_hashes};
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};

    macro_rules! test_hashes_internal {
        ($hash_method: ident, $input: expr, $initial_seeds: expr, $expected: expr) => {
            let i = $input;
            let mut hashes = $initial_seeds.clone();
            $hash_method(&[i], &mut hashes).unwrap();
            assert_eq!(hashes, $expected);
        };
    }

    macro_rules! test_hashes_with_nulls {
        ($method: ident, $t: ty, $values: ident, $expected: ident, $seed_type: ty) => {
            // copied before inserting nulls
            let mut input_with_nulls = $values.clone();
            let mut expected_with_nulls = $expected.clone();
            // test before inserting nulls
            let len = $values.len();
            let initial_seeds = vec![42 as $seed_type; len];
            let i = Arc::new(<$t>::from($values)) as ArrayRef;
            test_hashes_internal!($method, i, initial_seeds, $expected);

            // test with nulls
            let median = len / 2;
            input_with_nulls.insert(0, None);
            input_with_nulls.insert(median, None);
            expected_with_nulls.insert(0, 42 as $seed_type);
            expected_with_nulls.insert(median, 42 as $seed_type);
            let len_with_nulls = len + 2;
            let initial_seeds_with_nulls = vec![42 as $seed_type; len_with_nulls];
            let nullable_input = Arc::new(<$t>::from(input_with_nulls)) as ArrayRef;
            test_hashes_internal!(
                $method,
                nullable_input,
                initial_seeds_with_nulls,
                expected_with_nulls
            );
        };
    }

    fn test_murmur3_hash<I: Clone, T: arrow_array::Array + From<Vec<Option<I>>> + 'static>(
        values: Vec<Option<I>>,
        expected: Vec<u32>,
    ) {
        test_hashes_with_nulls!(create_murmur3_hashes, T, values, expected, u32);
    }

    fn test_xxhash64_hash<I: Clone, T: arrow_array::Array + From<Vec<Option<I>>> + 'static>(
        values: Vec<Option<I>>,
        expected: Vec<u64>,
    ) {
        test_hashes_with_nulls!(create_xxhash64_hashes, T, values, expected, u64);
    }

    #[test]
    fn test_i8() {
        test_murmur3_hash::<i8, Int8Array>(
            vec![Some(1), Some(0), Some(-1), Some(i8::MAX), Some(i8::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x43b4d8ed, 0x422a1365],
        );
        test_xxhash64_hash::<i8, Int8Array>(
            vec![Some(1), Some(0), Some(-1), Some(i8::MAX), Some(i8::MIN)],
            vec![
                0xa309b38455455929,
                0x3229fbc4681e48f3,
                0x1bfdda8861c06e45,
                0x77cc15d9f9f2cdc2,
                0x39bc22b9e94d81d0,
            ],
        );
    }

    #[test]
    fn test_i32() {
        test_murmur3_hash::<i32, Int32Array>(
            vec![Some(1), Some(0), Some(-1), Some(i32::MAX), Some(i32::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x07fb67e7, 0x2b1f0fc6],
        );
        test_xxhash64_hash::<i32, Int32Array>(
            vec![Some(1), Some(0), Some(-1), Some(i32::MAX), Some(i32::MIN)],
            vec![
                0xa309b38455455929,
                0x3229fbc4681e48f3,
                0x1bfdda8861c06e45,
                0x14f0ac009c21721c,
                0x1cc7cb8d034769cd,
            ],
        );
    }

    #[test]
    fn test_i64() {
        test_murmur3_hash::<i64, Int64Array>(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
            vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb],
        );
        test_xxhash64_hash::<i64, Int64Array>(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
            vec![
                0x9ed50fd59358d232,
                0xb71b47ebda15746c,
                0x358ae035bfb46fd2,
                0xd2f1c616ae7eb306,
                0x88608019c494c1f4,
            ],
        );
    }

    #[test]
    fn test_f32() {
        test_murmur3_hash::<f32, Float32Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0xe434cc39, 0x379fae8f, 0x379fae8f, 0xdc0da8eb, 0xcbdc340f, 0xc0361c86,
            ],
        );
        test_xxhash64_hash::<f32, Float32Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0x9b92689757fcdbd,
                0x3229fbc4681e48f3,
                0x3229fbc4681e48f3,
                0xa2becc0e61bb3823,
                0x8f20ab82d4f3687f,
                0xdce4982d97f7ac4,
            ],
        )
    }

    #[test]
    fn test_f64() {
        test_murmur3_hash::<f64, Float64Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0xe4876492, 0x9c67b85d, 0x9c67b85d, 0x13d81357, 0xb87e1595, 0xa0eef9f9,
            ],
        );

        test_xxhash64_hash::<f64, Float64Array>(
            vec![
                Some(1.0),
                Some(0.0),
                Some(-0.0),
                Some(-1.0),
                Some(99999999999.99999999999),
                Some(-99999999999.99999999999),
            ],
            vec![
                0xe1fd6e07fee8ad53,
                0xb71b47ebda15746c,
                0xb71b47ebda15746c,
                0x8cdde022746f8f1f,
                0x793c5c88d313eac7,
                0xc5e60e7b75d9b232,
            ],
        )
    }

    #[test]
    fn test_str() {
        let input = [
            "hello", "bar", "", "😁", "天地", "a", "ab", "abc", "abcd", "abcde",
        ]
        .iter()
        .map(|s| Some(s.to_string()))
        .collect::<Vec<Option<String>>>();
        let expected: Vec<u32> = vec![
            3286402344, 2486176763, 142593372, 885025535, 2395000894, 1485273170, 0xfa37157b,
            1322437556, 0xe860e5cc, 814637928,
        ];

        test_murmur3_hash::<String, StringArray>(input.clone(), expected);
        test_xxhash64_hash::<String, StringArray>(
            input,
            vec![
                0xc3629e6318d53932,
                0xe7097b6a54378d8a,
                0x98b1582b0977e704,
                0xa80d9d5a6a523bd5,
                0xfcba5f61ac666c61,
                0x88e4fe59adf7b0cc,
                0x259dd873209a3fe3,
                0x13c1d910702770e6,
                0xa17b5eb5dc364dff,
                0xf241303e4a90f299,
            ],
        )
    }
}
