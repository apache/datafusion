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

// This is a Spark-compatible MurmurHash3 implementation.
// The algorithm is based on Austin Appleby's original MurmurHash3:
//   https://github.com/aappleby/smhasher/blob/0ff96f7835817a27d0487325b6c16033e2992eb5/src/MurmurHash3.cpp
// Spark's implementation is derived from Guava's Murmur3_32HashFunction:
//   https://github.com/google/guava/blob/master/guava/src/com/google/common/hash/Murmur3_32HashFunction.java

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, DictionaryArray, Int32Array,
    types::ArrowDictionaryKeyType,
};
use arrow::compute::take;
use arrow::datatypes::{ArrowNativeType, DataType};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::utils::create_hashes_internal;

const DEFAULT_SEED: i32 = 42;

/// Spark-compatible murmur3 hash function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#hash>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMurmur3Hash {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkMurmur3Hash {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMurmur3Hash {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec!["hash".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkMurmur3Hash {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "murmur3_hash"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("murmur3_hash requires at least one argument");
        }

        let num_rows = args.number_rows;

        // Initialize hashes with seed
        let mut hashes: Vec<u32> = vec![DEFAULT_SEED as u32; num_rows];

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        // Hash each column
        for (i, col) in arrays.iter().enumerate() {
            hash_column_murmur3(col, &mut hashes, i == 0)?;
        }

        // Convert to Int32
        let result: Vec<i32> = hashes.into_iter().map(|h| h as i32).collect();
        let result_array = Int32Array::from(result);

        if num_rows == 1 {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(
                result_array.value(0),
            ))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
    }
}

/// Spark-compatible murmur3 hash algorithm
#[inline]
pub fn spark_compatible_murmur3_hash<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
    #[inline]
    fn mix_k1(mut k1: i32) -> i32 {
        k1 = k1.mul_wrapping(0xcc9e2d51u32 as i32);
        k1 = k1.rotate_left(15);
        k1.mul_wrapping(0x1b873593u32 as i32)
    }

    #[inline]
    fn mix_h1(mut h1: i32, k1: i32) -> i32 {
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1.mul_wrapping(5).add_wrapping(0xe6546b64u32 as i32)
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
        // SAFETY: caller guarantees data length is aligned to 4 bytes
        unsafe {
            let mut h1 = seed as i32;
            for i in (0..data.len()).step_by(4) {
                let ints = data.as_ptr().add(i) as *const i32;
                let half_word = ints.read_unaligned();
                h1 = mix_h1(h1, mix_k1(half_word));
            }
            h1
        }
    }

    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // SAFETY:
    // Avoid boundary checking in performance critical code.
    // All operations are guaranteed to be safe.
    // data is &[u8] so we do not need to check for proper alignment.
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
fn hash_column_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes: &mut [u32],
    first_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !first_col {
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        hash_column_murmur3(&unpacked, hashes, false)?;
    } else {
        let dict_values = Arc::clone(dict_array.values());
        let mut dict_hashes = vec![DEFAULT_SEED as u32; dict_values.len()];
        hash_column_murmur3(&dict_values, &mut dict_hashes, true)?;
        for (hash, key) in hashes.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                let idx = key.to_usize().ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(format!(
                        "Can not convert key value {:?} to usize in dictionary of type {:?}",
                        key,
                        dict_array.data_type()
                    ))
                })?;
                *hash = dict_hashes[idx]
            }
            // No update for Null keys, consistent with other types
        }
    }
    Ok(())
}

/// Create hashes for a batch of arrays (used for recursive hashing of complex types).
fn create_murmur3_hashes(arrays: &[ArrayRef], hashes: &mut [u32]) -> Result<()> {
    for (i, col) in arrays.iter().enumerate() {
        hash_column_murmur3(col, hashes, i == 0)?;
    }
    Ok(())
}

fn hash_column_murmur3(
    col: &ArrayRef,
    hashes: &mut [u32],
    first_col: bool,
) -> Result<()> {
    // Handle Dictionary types separately (turbofish syntax not supported in macros)
    if let DataType::Dictionary(key_type, _) = col.data_type() {
        return match key_type.as_ref() {
            DataType::Int8 => hash_column_dictionary::<arrow::datatypes::Int8Type>(
                col, hashes, first_col,
            ),
            DataType::Int16 => hash_column_dictionary::<arrow::datatypes::Int16Type>(
                col, hashes, first_col,
            ),
            DataType::Int32 => hash_column_dictionary::<arrow::datatypes::Int32Type>(
                col, hashes, first_col,
            ),
            DataType::Int64 => hash_column_dictionary::<arrow::datatypes::Int64Type>(
                col, hashes, first_col,
            ),
            DataType::UInt8 => hash_column_dictionary::<arrow::datatypes::UInt8Type>(
                col, hashes, first_col,
            ),
            DataType::UInt16 => hash_column_dictionary::<arrow::datatypes::UInt16Type>(
                col, hashes, first_col,
            ),
            DataType::UInt32 => hash_column_dictionary::<arrow::datatypes::UInt32Type>(
                col, hashes, first_col,
            ),
            DataType::UInt64 => hash_column_dictionary::<arrow::datatypes::UInt64Type>(
                col, hashes, first_col,
            ),
            dt => {
                internal_err!("Unsupported dictionary key type for murmur3_hash: {dt}")
            }
        };
    }

    create_hashes_internal!(
        col,
        hashes,
        spark_compatible_murmur3_hash,
        create_murmur3_hashes,
        "murmur3_hash"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, StringArray};

    #[test]
    fn test_murmur3_i32() {
        let seed = 42u32;
        assert_eq!(
            spark_compatible_murmur3_hash(1i32.to_le_bytes(), seed),
            0xdea578e3
        );
        assert_eq!(
            spark_compatible_murmur3_hash(0i32.to_le_bytes(), seed),
            0x379fae8f
        );
        assert_eq!(
            spark_compatible_murmur3_hash((-1i32).to_le_bytes(), seed),
            0xa0590e3d
        );
    }

    #[test]
    fn test_murmur3_i32_boundary() {
        let seed = 42u32;
        let h = spark_compatible_murmur3_hash(i32::MAX.to_le_bytes(), seed);
        assert_ne!(h, seed);
        let h = spark_compatible_murmur3_hash(i32::MIN.to_le_bytes(), seed);
        assert_ne!(h, seed);
    }

    #[test]
    fn test_murmur3_i8() {
        let seed = 42u32;
        assert_eq!(
            spark_compatible_murmur3_hash((1i8 as i32).to_le_bytes(), seed),
            spark_compatible_murmur3_hash(1i32.to_le_bytes(), seed),
        );
    }

    #[test]
    fn test_murmur3_i64() {
        let seed = 42u32;
        assert_eq!(
            spark_compatible_murmur3_hash(1i64.to_le_bytes(), seed),
            0x99f0149d
        );
        assert_eq!(
            spark_compatible_murmur3_hash(0i64.to_le_bytes(), seed),
            0x9c67b85d
        );
        assert_eq!(
            spark_compatible_murmur3_hash((-1i64).to_le_bytes(), seed),
            0xc8008529
        );
    }

    #[test]
    fn test_murmur3_i64_boundary() {
        let seed = 42u32;
        let h = spark_compatible_murmur3_hash(i64::MAX.to_le_bytes(), seed);
        assert_ne!(h, seed);
        let h = spark_compatible_murmur3_hash(i64::MIN.to_le_bytes(), seed);
        assert_ne!(h, seed);
    }

    #[test]
    fn test_murmur3_f32() {
        let seed = 42u32;
        let neg_zero = -0.0f32;
        let bytes = if neg_zero == 0.0 && neg_zero.is_sign_negative() {
            0i32.to_le_bytes()
        } else {
            neg_zero.to_le_bytes()
        };
        assert_eq!(
            spark_compatible_murmur3_hash(0i32.to_le_bytes(), seed),
            spark_compatible_murmur3_hash(bytes, seed),
        );
    }

    #[test]
    fn test_murmur3_f64() {
        let seed = 42u32;
        let neg_zero = -0.0f64;
        let bytes = if neg_zero == 0.0 && neg_zero.is_sign_negative() {
            0i64.to_le_bytes()
        } else {
            neg_zero.to_le_bytes()
        };
        assert_eq!(
            spark_compatible_murmur3_hash(0i64.to_le_bytes(), seed),
            spark_compatible_murmur3_hash(bytes, seed),
        );
    }

    #[test]
    fn test_murmur3_string() {
        let seed = 42u32;
        assert_eq!(spark_compatible_murmur3_hash("hello", seed), 3286402344);
        assert_eq!(spark_compatible_murmur3_hash("", seed), 142593372);
        assert_eq!(spark_compatible_murmur3_hash("abc", seed), 1322437556);
    }

    #[test]
    fn test_murmur3_string_emoji_cjk() {
        let seed = 42u32;
        let h1 = spark_compatible_murmur3_hash("😁", seed);
        assert_ne!(h1, seed);
        let h2 = spark_compatible_murmur3_hash("天地", seed);
        assert_ne!(h2, seed);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_murmur3_dictionary_string() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc", "hello", "world"]
                .into_iter()
                .collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u32; 5];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(hashes[0], spark_compatible_murmur3_hash("hello", 42));
        assert_eq!(hashes[1], spark_compatible_murmur3_hash("world", 42));
        assert_eq!(hashes[2], spark_compatible_murmur3_hash("abc", 42));
        assert_eq!(hashes[3], hashes[0]);
        assert_eq!(hashes[4], hashes[1]);
    }

    #[test]
    fn test_murmur3_dictionary_int() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let keys = Int32Array::from(vec![0, 1, 2, 0, 1]);
        let values = Int32Array::from(vec![100, 200, 300]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u32; 5];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(
            hashes[0],
            spark_compatible_murmur3_hash(100i32.to_le_bytes(), 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_murmur3_hash(200i32.to_le_bytes(), 42)
        );
        assert_eq!(
            hashes[2],
            spark_compatible_murmur3_hash(300i32.to_le_bytes(), 42)
        );
        assert_eq!(hashes[3], hashes[0]);
        assert_eq!(hashes[4], hashes[1]);
    }

    #[test]
    fn test_murmur3_dictionary_with_nulls() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(0), None]);
        let values = StringArray::from(vec!["hello", "world"]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u32; 5];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(hashes[0], spark_compatible_murmur3_hash("hello", 42));
        assert_eq!(hashes[2], spark_compatible_murmur3_hash("world", 42));
        assert_eq!(hashes[3], spark_compatible_murmur3_hash("hello", 42));
        assert_eq!(hashes[1], DEFAULT_SEED as u32);
        assert_eq!(hashes[4], DEFAULT_SEED as u32);
    }

    #[test]
    fn test_murmur3_dictionary_non_first_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc"].into_iter().collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![123u32, 456u32, 789u32];
        hash_column_murmur3(&array_ref, &mut hashes, false).unwrap();

        assert_eq!(hashes[0], spark_compatible_murmur3_hash("hello", 123));
        assert_eq!(hashes[1], spark_compatible_murmur3_hash("world", 456));
        assert_eq!(hashes[2], spark_compatible_murmur3_hash("abc", 789));
    }

    #[test]
    fn test_murmur3_fixed_size_binary() {
        let array = FixedSizeBinaryArray::from(vec![
            Some(&[0x01, 0x02, 0x03, 0x04][..]),
            Some(&[0x05, 0x06, 0x07, 0x08][..]),
            None,
            Some(&[0x00, 0x00, 0x00, 0x00][..]),
        ]);
        let array_ref: ArrayRef = Arc::new(array);

        let mut hashes = vec![DEFAULT_SEED as u32; 4];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(
            hashes[0],
            spark_compatible_murmur3_hash([0x01, 0x02, 0x03, 0x04], 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_murmur3_hash([0x05, 0x06, 0x07, 0x08], 42)
        );
        assert_eq!(hashes[2], DEFAULT_SEED as u32);
        assert_eq!(
            hashes[3],
            spark_compatible_murmur3_hash([0x00, 0x00, 0x00, 0x00], 42)
        );
    }

    #[test]
    fn test_murmur3_struct() {
        use arrow::array::StructArray;
        use arrow::datatypes::Field;

        let int_array = Int32Array::from(vec![1, 2, 3]);
        let str_array = StringArray::from(vec!["a", "b", "c"]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(int_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, false)),
                Arc::new(str_array) as ArrayRef,
            ),
        ]);
        let array_ref: ArrayRef = Arc::new(struct_array);

        let mut hashes = vec![DEFAULT_SEED as u32; 3];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED as u32);
        }
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[1], hashes[2]);
    }

    #[test]
    fn test_murmur3_list() {
        use arrow::array::ListArray;
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Field;

        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 3, 6].into());
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        let array_ref: ArrayRef = Arc::new(list_array);

        let mut hashes = vec![DEFAULT_SEED as u32; 3];
        hash_column_murmur3(&array_ref, &mut hashes, true).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED as u32);
        }
        assert_ne!(hashes[0], hashes[1]);
    }
}
