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

use arrow::compute::take;
use twox_hash::XxHash64;

use datafusion::{
    arrow::{
        array::*,
        datatypes::{ArrowDictionaryKeyType, ArrowNativeType},
    },
    common::{internal_err, ScalarValue},
    error::{DataFusionError, Result},
};

use crate::create_hashes_internal;
use arrow_array::{Array, ArrayRef, Int64Array};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// Spark compatible xxhash64 in vectorized execution fashion
pub fn spark_xxhash64(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    let length = args.len();
    let seed = &args[length - 1];
    match seed {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))) => {
            // iterate over the arguments to find out the length of the array
            let num_rows = args[0..args.len() - 1]
                .iter()
                .find_map(|arg| match arg {
                    ColumnarValue::Array(array) => Some(array.len()),
                    ColumnarValue::Scalar(_) => None,
                })
                .unwrap_or(1);
            let mut hashes: Vec<u64> = vec![0_u64; num_rows];
            hashes.fill(*seed as u64);
            let arrays = args[0..args.len() - 1]
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(array) => Arc::clone(array),
                    ColumnarValue::Scalar(scalar) => {
                        scalar.clone().to_array_of_size(num_rows).unwrap()
                    }
                })
                .collect::<Vec<ArrayRef>>();
            create_xxhash64_hashes(&arrays, &mut hashes)?;
            if num_rows == 1 {
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    hashes[0] as i64,
                ))))
            } else {
                let hashes: Vec<i64> = hashes.into_iter().map(|x| x as i64).collect();
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(hashes))))
            }
        }
        _ => {
            internal_err!(
                "The seed of function xxhash64 must be an Int64 scalar value, but got: {:?}.",
                seed
            )
        }
    }
}

#[inline]
fn spark_compatible_xxhash64<T: AsRef<[u8]>>(data: T, seed: u64) -> u64 {
    XxHash64::oneshot(seed, data.as_ref())
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

/// Creates xxhash64 hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
fn create_xxhash64_hashes<'a>(
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

    use super::create_xxhash64_hashes;
    use crate::test_hashes_with_nulls;
    use datafusion::arrow::array::{ArrayRef, Int32Array, Int64Array, Int8Array, StringArray};

    fn test_xxhash64_hash<I: Clone, T: arrow_array::Array + From<Vec<Option<I>>> + 'static>(
        values: Vec<Option<I>>,
        expected: Vec<u64>,
    ) {
        test_hashes_with_nulls!(create_xxhash64_hashes, T, values, expected, u64);
    }

    #[test]
    fn test_i8() {
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
