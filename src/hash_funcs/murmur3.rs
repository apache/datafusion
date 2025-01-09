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

use crate::create_hashes_internal;
use arrow::compute::take;
use arrow_array::types::ArrowDictionaryKeyType;
use arrow_array::{Array, ArrayRef, ArrowNativeTypeOp, DictionaryArray, Int32Array};
use arrow_buffer::ArrowNativeType;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Spark compatible murmur3 hash (just `hash` in Spark) in vectorized execution fashion
pub fn spark_murmur3_hash(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u32> = vec![0_u32; num_rows];
            hashes.fill(*seed as u32);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_murmur3_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    hashes[0] as i32,
                ))))
            } else {
                let hashes: Vec<i32> = hashes.into_iter().map(|x| x as i32).collect();
                Ok(ColumnarValue::Array(Arc::new(Int32Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function murmur3_hash must be an Int32 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

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

/// Hash the values in a dictionary array
fn create_hashes_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes_buffer: &mut [u32],
    first_col: bool,
) -> datafusion_common::Result<()> {
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

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
pub fn create_murmur3_hashes<'a>(
    arrays: &[ArrayRef],
    hashes_buffer: &'a mut [u32],
) -> datafusion_common::Result<&'a mut [u32]> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        spark_compatible_murmur3_hash,
        create_hashes_dictionary
    );
    Ok(hashes_buffer)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float32Array, Float64Array};
    use std::sync::Arc;

    use crate::murmur3::create_murmur3_hashes;
    use crate::test_hashes_with_nulls;
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};

    fn test_murmur3_hash<I: Clone, T: arrow_array::Array + From<Vec<Option<I>>> + 'static>(
        values: Vec<Option<I>>,
        expected: Vec<u32>,
    ) {
        test_hashes_with_nulls!(create_murmur3_hashes, T, values, expected, u32);
    }

    #[test]
    fn test_i8() {
        test_murmur3_hash::<i8, Int8Array>(
            vec![Some(1), Some(0), Some(-1), Some(i8::MAX), Some(i8::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x43b4d8ed, 0x422a1365],
        );
    }

    #[test]
    fn test_i32() {
        test_murmur3_hash::<i32, Int32Array>(
            vec![Some(1), Some(0), Some(-1), Some(i32::MAX), Some(i32::MIN)],
            vec![0xdea578e3, 0x379fae8f, 0xa0590e3d, 0x07fb67e7, 0x2b1f0fc6],
        );
    }

    #[test]
    fn test_i64() {
        test_murmur3_hash::<i64, Int64Array>(
            vec![Some(1), Some(0), Some(-1), Some(i64::MAX), Some(i64::MIN)],
            vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb],
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
    }
}
