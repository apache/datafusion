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
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, types::ArrowDictionaryKeyType,
};
use arrow::compute::take;
use arrow::datatypes::{ArrowNativeType, DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use twox_hash::XxHash64;

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

        // Determine number of rows from the first array argument
        let num_rows = args
            .args
            .iter()
            .find_map(|arg| match arg {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(1);

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
        // Unpack the dictionary array as each row may have a different hash input
        let unpacked = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        hash_column_xxhash64(&unpacked, hashes, false)?;
    } else {
        // For the first column, hash each dictionary value once, and then use
        // that computed hash for each key value to avoid a potentially
        // expensive redundant hashing for large dictionary elements (e.g. strings)
        let dict_values = Arc::clone(dict_array.values());
        // Same initial seed as Spark
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

fn hash_column_xxhash64(
    col: &ArrayRef,
    hashes: &mut [u64],
    first_col: bool,
) -> Result<()> {
    match col.data_type() {
        DataType::Boolean => {
            let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = i32::from(array.value(i));
                    *hash = spark_compatible_xxhash64(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int8 => {
            let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i) as i32;
                    *hash = spark_compatible_xxhash64(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int16 => {
            let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i) as i32;
                    *hash = spark_compatible_xxhash64(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int32 => {
            let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int64 => {
            let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Float32 => {
            let array = col.as_any().downcast_ref::<Float32Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i);
                    // Spark uses 0 as hash for -0.0
                    let bytes = if val == 0.0 && val.is_sign_negative() {
                        0i32.to_le_bytes()
                    } else {
                        val.to_le_bytes()
                    };
                    *hash = spark_compatible_xxhash64(bytes, *hash);
                }
            }
        }
        DataType::Float64 => {
            let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i);
                    // Spark uses 0 as hash for -0.0
                    let bytes = if val == 0.0 && val.is_sign_negative() {
                        0i64.to_le_bytes()
                    } else {
                        val.to_le_bytes()
                    };
                    *hash = spark_compatible_xxhash64(bytes, *hash);
                }
            }
        }
        DataType::Date32 => {
            let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Date64 => {
            let array = col.as_any().downcast_ref::<Date64Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Utf8 => {
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::LargeUtf8 => {
            let array = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::Utf8View => {
            let array = col.as_string_view();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::Binary => {
            let array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::LargeBinary => {
            let array = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::BinaryView => {
            let array = col.as_binary_view();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::FixedSizeBinary(_) => {
            let array = col.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_xxhash64(array.value(i), *hash);
                }
            }
        }
        DataType::Decimal128(precision, _) if *precision <= 18 => {
            let array = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    // For small decimals, hash as i64
                    let val = array.value(i) as i64;
                    *hash = spark_compatible_xxhash64(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let array = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash =
                        spark_compatible_xxhash64(array.value(i).to_le_bytes(), *hash);
                }
            }
        }
        DataType::Null => {
            // Nulls don't update the hash
        }
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => hash_column_dictionary::<arrow::datatypes::Int8Type>(
                col, hashes, first_col,
            )?,
            DataType::Int16 => hash_column_dictionary::<arrow::datatypes::Int16Type>(
                col, hashes, first_col,
            )?,
            DataType::Int32 => hash_column_dictionary::<arrow::datatypes::Int32Type>(
                col, hashes, first_col,
            )?,
            DataType::Int64 => hash_column_dictionary::<arrow::datatypes::Int64Type>(
                col, hashes, first_col,
            )?,
            DataType::UInt8 => hash_column_dictionary::<arrow::datatypes::UInt8Type>(
                col, hashes, first_col,
            )?,
            DataType::UInt16 => hash_column_dictionary::<arrow::datatypes::UInt16Type>(
                col, hashes, first_col,
            )?,
            DataType::UInt32 => hash_column_dictionary::<arrow::datatypes::UInt32Type>(
                col, hashes, first_col,
            )?,
            DataType::UInt64 => hash_column_dictionary::<arrow::datatypes::UInt64Type>(
                col, hashes, first_col,
            )?,
            dt => {
                return internal_err!(
                    "Unsupported dictionary key type for xxhash64: {dt}"
                );
            }
        },
        dt => {
            return internal_err!("Unsupported data type for xxhash64: {dt}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_xxhash64_string() {
        let seed = 42u64;
        assert_eq!(spark_compatible_xxhash64("hello", seed), 0xc3629e6318d53932);
        assert_eq!(spark_compatible_xxhash64("", seed), 0x98b1582b0977e704);
        assert_eq!(spark_compatible_xxhash64("abc", seed), 0x13c1d910702770e6);
    }

    #[test]
    fn test_xxhash64_dictionary_string() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Create a dictionary array with string values
        // Keys: [0, 1, 2, 0, 1] -> ["hello", "world", "abc", "hello", "world"]
        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc", "hello", "world"]
                .into_iter()
                .collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        // Verify hashes match the expected values for strings
        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[1], spark_compatible_xxhash64("world", 42));
        assert_eq!(hashes[2], spark_compatible_xxhash64("abc", 42));
        // Repeated values should have the same hash
        assert_eq!(hashes[3], hashes[0]); // "hello" again
        assert_eq!(hashes[4], hashes[1]); // "world" again
    }

    #[test]
    fn test_xxhash64_dictionary_int() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Create a dictionary array with int values
        let keys = Int32Array::from(vec![0, 1, 2, 0, 1]);
        let values = Int32Array::from(vec![100, 200, 300]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        // Verify hashes match the expected values for i32
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
        // Repeated values should have the same hash
        assert_eq!(hashes[3], hashes[0]);
        assert_eq!(hashes[4], hashes[1]);
    }

    #[test]
    fn test_xxhash64_dictionary_with_nulls() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Create a dictionary array with null keys
        let keys = Int32Array::from(vec![Some(0), None, Some(1), Some(0), None]);
        let values = StringArray::from(vec!["hello", "world"]);
        let dict_array =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![DEFAULT_SEED as u64; 5];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        // Non-null keys should have correct hashes
        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[2], spark_compatible_xxhash64("world", 42));
        assert_eq!(hashes[3], spark_compatible_xxhash64("hello", 42));
        // Null keys should keep the initial seed value (unchanged)
        assert_eq!(hashes[1], DEFAULT_SEED as u64);
        assert_eq!(hashes[4], DEFAULT_SEED as u64);
    }

    #[test]
    fn test_xxhash64_dictionary_non_first_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Test dictionary as non-first column (uses unpacking via take)
        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc"].into_iter().collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        // Start with non-seed hash values (simulating previous column hashing)
        let mut hashes = vec![123u64, 456u64, 789u64];
        hash_column_xxhash64(&array_ref, &mut hashes, false).unwrap();

        // The hashes should be updated from the previous values
        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 123));
        assert_eq!(hashes[1], spark_compatible_xxhash64("world", 456));
        assert_eq!(hashes[2], spark_compatible_xxhash64("abc", 789));
    }

    #[test]
    fn test_xxhash64_fixed_size_binary() {
        // Create a FixedSizeBinary array with 4-byte values
        let array = FixedSizeBinaryArray::from(vec![
            Some(&[0x01, 0x02, 0x03, 0x04][..]),
            Some(&[0x05, 0x06, 0x07, 0x08][..]),
            None,
            Some(&[0x00, 0x00, 0x00, 0x00][..]),
        ]);
        let array_ref: ArrayRef = Arc::new(array);

        let mut hashes = vec![DEFAULT_SEED as u64; 4];
        hash_column_xxhash64(&array_ref, &mut hashes, true).unwrap();

        // Verify hashes match expected values
        assert_eq!(
            hashes[0],
            spark_compatible_xxhash64(&[0x01, 0x02, 0x03, 0x04], 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_xxhash64(&[0x05, 0x06, 0x07, 0x08], 42)
        );
        // Null value should keep the seed
        assert_eq!(hashes[2], DEFAULT_SEED as u64);
        assert_eq!(
            hashes[3],
            spark_compatible_xxhash64(&[0x00, 0x00, 0x00, 0x00], 42)
        );
    }
}
