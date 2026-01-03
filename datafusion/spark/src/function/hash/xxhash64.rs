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
    Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
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
            hash_column_xxhash64(col, &mut hashes)?;
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

fn hash_column_xxhash64(col: &ArrayRef, hashes: &mut [u64]) -> Result<()> {
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
            let array = col
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
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
        assert_eq!(
            spark_compatible_xxhash64("hello", seed),
            0xc3629e6318d53932
        );
        assert_eq!(spark_compatible_xxhash64("", seed), 0x98b1582b0977e704);
        assert_eq!(
            spark_compatible_xxhash64("abc", seed),
            0x13c1d910702770e6
        );
    }
}
