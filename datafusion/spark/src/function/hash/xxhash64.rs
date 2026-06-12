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

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int64Array, types::ArrowDictionaryKeyType,
};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::compute::take;
use arrow::datatypes::{ArrowNativeType, DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use twox_hash::XxHash64;

use crate::create_hashes_internal;

const DEFAULT_SEED: u64 = 42;

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
    fn name(&self) -> &str {
        "xxhash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        // Spark's HashExpression overrides nullable to false: NULL inputs are
        // skipped and the seed is used, so the result is never null.
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let mut hashes: Vec<u64> = vec![DEFAULT_SEED; num_rows];

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        create_xxhash64_hashes(&arrays, &mut hashes)?;

        if num_rows == 1 {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                hashes[0] as i64,
            ))))
        } else {
            // Reinterpret Vec<u64> as ScalarBuffer<i64> without copying — both
            // types have identical layout, and `as i64` is a bitcast.
            let buffer = ScalarBuffer::<i64>::from(Buffer::from_vec(hashes));
            Ok(ColumnarValue::Array(Arc::new(Int64Array::new(
                buffer, None,
            ))))
        }
    }
}

#[inline]
fn spark_compatible_xxhash64<T: AsRef<[u8]>>(data: T, seed: u64) -> u64 {
    XxHash64::oneshot(seed, data.as_ref())
}

/// Hash the values in a dictionary array using xxhash64.
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
        // Hash each dictionary value once, then look up by key. This avoids
        // redundant hashing of large dictionary entries (e.g. long strings).
        let dict_values = Arc::clone(dict_array.values());
        let mut dict_hashes = vec![DEFAULT_SEED; dict_values.len()];
        create_xxhash64_hashes(&[dict_values], &mut dict_hashes)?;

        for (hash, key) in hashes_buffer.iter_mut().zip(dict_array.keys().iter()) {
            if let Some(key) = key {
                *hash = dict_hashes[key.as_usize()]
            }
            // No update for Null keys, consistent with other types.
        }
    }
    Ok(())
}

/// Create xxhash64 hash values for every row, based on the values in the columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately and seeded with the
/// initial hash value (Spark uses `42`).
fn create_xxhash64_hashes(arrays: &[ArrayRef], hashes_buffer: &mut [u64]) -> Result<()> {
    create_hashes_internal!(
        arrays,
        hashes_buffer,
        spark_compatible_xxhash64,
        create_xxhash64_hashes_dictionary,
        create_xxhash64_hashes
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryArray, Int32Array, StringArray};

    #[test]
    fn test_xxhash64_nullability() -> Result<()> {
        let func = SparkXxhash64::new();

        // Spark's xxhash64 is never null (NULL args are skipped, seed is returned),
        // so the output field is non-nullable regardless of input nullability.
        let nullable: FieldRef = Arc::new(Field::new("a", DataType::Int32, true));
        let non_nullable: FieldRef = Arc::new(Field::new("b", DataType::Int32, false));

        let out = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&nullable), Arc::clone(&non_nullable)],
            scalar_arguments: &[None, None],
        })?;
        assert!(!out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Int64);

        Ok(())
    }

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

    /// Spark normalizes `-0.0` to `0.0` before hashing, so both produce
    /// the same hash. Exercise the dispatch through `create_xxhash64_hashes`
    /// to cover the `hash_array_primitive_float!` normalization path.
    #[test]
    fn test_xxhash64_negative_zero_f32() {
        use arrow::array::Float32Array;
        let array: ArrayRef = Arc::new(Float32Array::from(vec![0.0f32, -0.0f32]));
        let mut hashes = vec![DEFAULT_SEED; 2];
        create_xxhash64_hashes(&[array], &mut hashes).unwrap();
        assert_eq!(hashes[0], hashes[1]);
        assert_eq!(hashes[0], spark_compatible_xxhash64(0i32.to_le_bytes(), 42));
    }

    #[test]
    fn test_xxhash64_negative_zero_f64() {
        use arrow::array::Float64Array;
        let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0f64, -0.0f64]));
        let mut hashes = vec![DEFAULT_SEED; 2];
        create_xxhash64_hashes(&[array], &mut hashes).unwrap();
        assert_eq!(hashes[0], hashes[1]);
        assert_eq!(hashes[0], spark_compatible_xxhash64(0i64.to_le_bytes(), 42));
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

        let mut hashes = vec![DEFAULT_SEED; 5];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

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

        let mut hashes = vec![DEFAULT_SEED; 5];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

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

        let mut hashes = vec![DEFAULT_SEED; 5];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[2], spark_compatible_xxhash64("world", 42));
        assert_eq!(hashes[3], spark_compatible_xxhash64("hello", 42));
        assert_eq!(hashes[1], DEFAULT_SEED);
        assert_eq!(hashes[4], DEFAULT_SEED);
    }

    #[test]
    fn test_xxhash64_dictionary_non_first_column() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let dict_array: DictionaryArray<Int32Type> =
            vec!["hello", "world", "abc"].into_iter().collect();
        let array_ref: ArrayRef = Arc::new(dict_array);

        let mut hashes = vec![123u64, 456u64, 789u64];
        create_xxhash64_hashes_dictionary::<Int32Type>(&array_ref, &mut hashes, false)
            .unwrap();

        assert_eq!(hashes[0], spark_compatible_xxhash64("hello", 123));
        assert_eq!(hashes[1], spark_compatible_xxhash64("world", 456));
        assert_eq!(hashes[2], spark_compatible_xxhash64("abc", 789));
    }

    #[test]
    fn test_xxhash64_fixed_size_binary() {
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(&[0x01, 0x02, 0x03, 0x04][..]),
                Some(&[0x05, 0x06, 0x07, 0x08][..]),
                None,
                Some(&[0x00, 0x00, 0x00, 0x00][..]),
            ]
            .into_iter(),
            4,
        )
        .unwrap();
        let array_ref: ArrayRef = Arc::new(array);

        let mut hashes = vec![DEFAULT_SEED; 4];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

        assert_eq!(
            hashes[0],
            spark_compatible_xxhash64([0x01, 0x02, 0x03, 0x04], 42)
        );
        assert_eq!(
            hashes[1],
            spark_compatible_xxhash64([0x05, 0x06, 0x07, 0x08], 42)
        );
        assert_eq!(hashes[2], DEFAULT_SEED);
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

        let mut hashes = vec![DEFAULT_SEED; 3];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED);
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

        let mut hashes = vec![DEFAULT_SEED; 3];
        create_xxhash64_hashes(&[array_ref], &mut hashes).unwrap();

        for hash in &hashes {
            assert_ne!(*hash, DEFAULT_SEED);
        }
        assert_ne!(hashes[0], hashes[1]);
    }
}
