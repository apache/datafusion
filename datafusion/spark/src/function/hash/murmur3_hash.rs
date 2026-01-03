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
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, BinaryArray, BooleanArray, Date32Array,
    Date64Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

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
        let mut hashes: Vec<u32> = vec![DEFAULT_SEED as u32; num_rows];

        // Convert all arguments to arrays
        let arrays: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => Arc::clone(array),
                ColumnarValue::Scalar(scalar) => scalar
                    .to_array_of_size(num_rows)
                    .expect("Failed to convert scalar to array"),
            })
            .collect();

        // Hash each column
        for col in &arrays {
            hash_column_murmur3(col, &mut hashes)?;
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
                let mut half_word = ints.read_unaligned();
                if cfg!(target_endian = "big") {
                    half_word = half_word.reverse_bits();
                }
                h1 = mix_h1(h1, mix_k1(half_word));
            }
            h1
        }
    }

    let data = data.as_ref();
    let len = data.len();
    let len_aligned = len - len % 4;

    // SAFETY: all operations are guaranteed to be safe
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

fn hash_column_murmur3(col: &ArrayRef, hashes: &mut [u32]) -> Result<()> {
    match col.data_type() {
        DataType::Boolean => {
            let array = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = i32::from(array.value(i));
                    *hash = spark_compatible_murmur3_hash(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int8 => {
            let array = col.as_any().downcast_ref::<Int8Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i) as i32;
                    *hash = spark_compatible_murmur3_hash(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int16 => {
            let array = col.as_any().downcast_ref::<Int16Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    let val = array.value(i) as i32;
                    *hash = spark_compatible_murmur3_hash(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Int32 => {
            let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
        DataType::Int64 => {
            let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
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
                    *hash = spark_compatible_murmur3_hash(bytes, *hash);
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
                    *hash = spark_compatible_murmur3_hash(bytes, *hash);
                }
            }
        }
        DataType::Date32 => {
            let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
        DataType::Date64 => {
            let array = col.as_any().downcast_ref::<Date64Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
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
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
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
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
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
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
        DataType::Utf8 => {
            let array = col.as_any().downcast_ref::<StringArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::LargeUtf8 => {
            let array = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::Utf8View => {
            let array = col.as_string_view();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::Binary => {
            let array = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::LargeBinary => {
            let array = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::BinaryView => {
            let array = col.as_binary_view();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(array.value(i), *hash);
                }
            }
        }
        DataType::Decimal128(precision, _) if *precision <= 18 => {
            let array = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    // For small decimals, hash as i64
                    let val = array.value(i) as i64;
                    *hash = spark_compatible_murmur3_hash(val.to_le_bytes(), *hash);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let array = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !array.is_null(i) {
                    *hash = spark_compatible_murmur3_hash(
                        array.value(i).to_le_bytes(),
                        *hash,
                    );
                }
            }
        }
        DataType::Null => {
            // Nulls don't update the hash
        }
        dt => {
            return internal_err!("Unsupported data type for murmur3_hash: {dt}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_murmur3_string() {
        let seed = 42u32;
        assert_eq!(spark_compatible_murmur3_hash("hello", seed), 3286402344);
        assert_eq!(spark_compatible_murmur3_hash("", seed), 142593372);
        assert_eq!(spark_compatible_murmur3_hash("abc", seed), 1322437556);
    }
}
