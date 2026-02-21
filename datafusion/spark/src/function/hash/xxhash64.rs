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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int64Array, types::ArrowDictionaryKeyType,
};
use arrow::compute::take;
use arrow::datatypes::{ArrowNativeType, DataType};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use twox_hash::XxHash64;

use super::utils::create_hashes_internal;

const DEFAULT_SEED: i64 = 42;

/// Spark-compatible xxhash64 function.
/// <https://spark.apache.org/docs/latest/api/sql/index.html#xxhash64>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkXxhash64 {
    signature: Signature,
}

impl Default for SparkXxhash64 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkXxhash64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkXxhash64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("xxhash64 requires at least one argument");
        }

        let num_rows = args.number_rows;

        // Initialize hashes with seed
        let mut hashes: Vec<u64> = vec![DEFAULT_SEED as u64; num_rows];

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        // Hash each column
        for (i, col) in arrays.iter().enumerate() {
            hash_column_xxhash64(col, &mut hashes, i == 0)?;
        }

        // Convert to Int64
        let result: Vec<i64> = hashes.into_iter().map(|h| h as i64).collect();
        let result_array = Int64Array::from(result);

        if num_rows == 1 {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                result_array.value(0),
            ))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
    }
}

#[inline]
fn spark_compatible_xxhash64<T: AsRef<[u8]>>(data: T, seed: u64) -> u64 {
    XxHash64::oneshot(seed, data.as_ref())
}

/// Hash the values in a dictionary array
fn hash_column_dictionary<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    hashes: &mut [u64],
    first_col: bool,
) -> Result<()> {
    let dict_array = array.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
    if !first_col {
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        hash_column_xxhash64(&unpacked, hashes, false)?;
    } else {
        let dict_values = Arc::clone(dict_array.values());
        let mut dict_hashes = vec![DEFAULT_SEED as u64; dict_values.len()];
        hash_column_xxhash64(&dict_values, &mut dict_hashes, true)?;
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
fn create_xxhash64_hashes(arrays: &[ArrayRef], hashes: &mut [u64]) -> Result<()> {
    for (i, col) in arrays.iter().enumerate() {
        hash_column_xxhash64(col, hashes, i == 0)?;
    }
    Ok(())
}

fn hash_column_xxhash64(
    col: &ArrayRef,
    hashes: &mut [u64],
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
                internal_err!("Unsupported dictionary key type for xxhash64: {dt}")
            }
        };
    }

    create_hashes_internal!(
        col,
        hashes,
        spark_compatible_xxhash64,
        create_xxhash64_hashes,
        "xxhash64"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Int32Array, StringArray};

    #[test]
    fn test_xxhash64_i32() {
        let seed = 42u64;
        assert_eq!(
            spark_compatible_xxhash64(1i32.to_le_bytes(), seed),
            0xa309b38455455929
        );
        assert_eq!(
            spark_compatible_xxhash64(0i32.to_le_bytes(), seed),
            0x3229fbc4681e48f3
        );
        assert_eq!(
            spark_compatible_xxhash64((-1i32).to_le_bytes(), seed),
            0x1bfdda8861c06e45
        );
    }

    #[test]
    fn test_xxhash64_i32_boundary() {
        let seed = 42u64;
        let h = spark_compatible_xxhash64(i32::MAX.to_le_bytes(), seed);
        assert_ne!(h, seed);
        let h = spark_compatible_xxhash64(i32::MIN.to_le_bytes(), seed);
        assert_ne!(h, seed);
    }

    #[test]
    fn test_xxhash64_i8() {
        let seed = 42u64;
        // i8 is widened to i32 before hashing
        assert_eq!(
            spark_compatible_xxhash64((1i8 as i32).to_le_bytes(), seed),
            spark_compatible_xxhash64(1i32.to_le_bytes(), seed),
        );
    }

    #[test]
    fn test_xxhash64_i64() {
        let seed = 42u64;
        assert_eq!(
            spark_compatible_xxhash64(1i64.to_le_bytes(), seed),
            0x9ed50fd59358d232
        );
        assert_eq!(
            spark_compatible_xxhash64(0i64.to_le_bytes(), seed),
            0xb71b47ebda15746c
        );
        assert_eq!(
            spark_compatible_xxhash64((-1i64).to_le_bytes(), seed),
            0x358ae035bfb46fd2
        );
    }

    #[test]
    fn test_xxhash64_i64_boundary() {
        let seed = 42u64;
        let h = spark_compatible_xxhash64(i64::MAX.to_le_bytes(), seed);
        assert_ne!(h, seed);
        let h = spark_compatible_xxhash64(i64::MIN.to_le_bytes(), seed);
        assert_ne!(h, seed);
    }

    #[test]
    fn test_xxhash64_f32() {
        let seed = 42u64;
        let neg_zero = -0.0f32;
        let pos_zero = 0.0f32;
        assert_eq!(
            spark_compatible_xxhash64(0i32.to_le_bytes(), seed),
            spark_compatible_xxhash64(pos_zero.to_le_bytes(), seed),
        );
        // -0.0 normalized: hash 0i32 bytes
        let bytes = if neg_zero == 0.0 && neg_zero.is_sign_negative() {
            0i32.to_le_bytes()
        } else {
            neg_zero.to_le_bytes()
        };
        assert_eq!(
            spark_compatible_xxhash64(0i32.to_le_bytes(), seed),
            spark_compatible_xxhash64(bytes, seed),
        );
    }

    #[test]
    fn test_xxhash64_f64() {
        let seed = 42u64;
        let neg_zero = -0.0f64;
        let bytes = if neg_zero == 0.0 && neg_zero.is_sign_negative() {
            0i64.to_le_bytes()
        } else {
            neg_zero.to_le_bytes()
        };
        assert_eq!(
            spark_compatible_xxhash64(0i64.to_le_bytes(), seed),
            spark_compatible_xxhash64(bytes, seed),
        );
    }

    #[test]
    fn test_xxhash64_string() {
        let seed = 42u64;
        assert_eq!(spark_compatible_xxhash64("hello", seed), 0xc3629e6318d53932);
        assert_eq!(spark_compatible_xxhash64("", seed), 0x98b1582b0977e704);
        assert_eq!(spark_compatible_xxhash64("abc", seed), 0x13c1d910702770e6);
    }

    #[test]
    fn test_xxhash64_string_emoji_cjk() {
        let seed = 42u64;
        let h1 = spark_compatible_xxhash64("😁", seed);
        assert_ne!(h1, seed);
        let h2 = spark_compatible_xxhash64("天地", seed);
        assert_ne!(h2, seed);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_xxhash64_dictionary_string() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc", "hello", "world"]
                .into_iter()
                .collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[1], spark_compatible_xxhash64("world", 42));
        assert_eq!(hashes[2], spark_compatible_xxhash64("abc", 42));
        assert_eq!(hashes[3], hashes[0]);
        assert_eq!(hashes[4], hashes[1]);
    }

    #[test]
    fn test_xxhash64_dictionary_int() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let keys = Int32Array::from(vec![0, 1, 2, 0, 1]);
        let values = Int32Array::from(vec![100, 200, 300]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(
            hashes[0],
            spark_compatible_xxhash64(100i32.to_le_bytes(), 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_xxhash64(200i32.to_le_bytes(), 42)
        );
        assert_eq!(
            hashes[2],
            spark_compatible_xxhash64(300i32.to_le_bytes(), 42)
        );
        assert_eq!(hashes[3], hashes[0]);
        assert_eq!(hashes[4], hashes[1]);
    }

    #[test]
    fn test_xxhash64_dictionary_with_nulls() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(0), None]);
        let values = StringArray::from(vec!["hello", "world"]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[2], spark_compatible_xxhash64("world", 42));
        assert_eq!(hashes[3], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[1], DEFAULT_SEED as u64);
        assert_eq!(hashes[4], DEFAULT_SEED as u64);
    }

    #[test]
    fn test_xxhash64_dictionary_non_first_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc"].into_iter().collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![123u64, 456u64, 789u64];
        hash_column_xxhash64(&array_ref, &mut hashes, false).unwrap();

        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 123));
        assert_eq!(hashes[1], spark_compatible_xxhash64("world", 456));
        assert_eq!(hashes[2], spark_compatible_xxhash64("abc", 789));
    }

    #[test]
    fn test_xxhash64_fixed_size_binary() {
        let array = FixedSizeBinaryArray::from(vec![
            Some(&[0x01, 0x02, 0x03, 0x04][..]),
            Some(&[0x05, 0x06, 0x07, 0x08][..]),
            None,
            Some(&[0x00, 0x00, 0x00, 0x00][..]),
        ]);
        let array_ref: ArrayRef = Arc::new(array);

        let mut hashes = vec![DEFAULT_SEED as u64; 4];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        assert_eq!(
            hashes[0],
            spark_compatible_xxhash64([0x01, 0x02, 0x03, 0x04], 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_xxhash64([0x05, 0x06, 0x07, 0x08], 42)
        );
        assert_eq!(hashes[2], DEFAULT_SEED as u64);
        assert_eq!(
            hashes[3],
            spark_compatible_xxhash64([0x00, 0x00, 0x00, 0x00], 42)
        );
    }

    #[test]
    fn test_xxhash64_struct() {
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

        let mut hashes = vec![DEFAULT_SEED as u64; 3];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED as u64);
        }
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[1], hashes[2]);
    }

    #[test]
    fn test_xxhash64_list() {
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

        let mut hashes = vec![DEFAULT_SEED as u64; 3];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED as u64);
        }
        assert_ne!(hashes[0], hashes[1]);
    }
}
