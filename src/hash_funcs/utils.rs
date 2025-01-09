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

#[macro_export]
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

#[macro_export]
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

#[macro_export]
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

#[macro_export]
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

#[macro_export]
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

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
///
/// `hash_method` is the hash function to use.
/// `create_dictionary_hash_method` is the function to create hashes for dictionary arrays input.
#[macro_export]
macro_rules! create_hashes_internal {
    ($arrays: ident, $hashes_buffer: ident, $hash_method: ident, $create_dictionary_hash_method: ident) => {
        use arrow::datatypes::{DataType, TimeUnit};
        use arrow_array::{types::*, *};

        for (i, col) in $arrays.iter().enumerate() {
            let first_col = i == 0;
            match col.data_type() {
                DataType::Boolean => {
                    $crate::hash_array_boolean!(
                        BooleanArray,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int8 => {
                    $crate::hash_array_primitive!(
                        Int8Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int16 => {
                    $crate::hash_array_primitive!(
                        Int16Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int32 => {
                    $crate::hash_array_primitive!(
                        Int32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int64 => {
                    $crate::hash_array_primitive!(
                        Int64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float32 => {
                    $crate::hash_array_primitive_float!(
                        Float32Array,
                        col,
                        f32,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float64 => {
                    $crate::hash_array_primitive_float!(
                        Float64Array,
                        col,
                        f64,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    $crate::hash_array_primitive!(
                        TimestampSecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMillisecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMicrosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampNanosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date32 => {
                    $crate::hash_array_primitive!(
                        Date32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date64 => {
                    $crate::hash_array_primitive!(
                        Date64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Utf8 => {
                    $crate::hash_array!(StringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeUtf8 => {
                    $crate::hash_array!(LargeStringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Binary => {
                    $crate::hash_array!(BinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeBinary => {
                    $crate::hash_array!(LargeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::FixedSizeBinary(_) => {
                    $crate::hash_array!(FixedSizeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Decimal128(_, _) => {
                    $crate::hash_array_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
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

pub(crate) mod test_utils {

    #[macro_export]
    macro_rules! test_hashes_internal {
        ($hash_method: ident, $input: expr, $initial_seeds: expr, $expected: expr) => {
            let i = $input;
            let mut hashes = $initial_seeds.clone();
            $hash_method(&[i], &mut hashes).unwrap();
            assert_eq!(hashes, $expected);
        };
    }

    #[macro_export]
    macro_rules! test_hashes_with_nulls {
        ($method: ident, $t: ty, $values: ident, $expected: ident, $seed_type: ty) => {
            // copied before inserting nulls
            let mut input_with_nulls = $values.clone();
            let mut expected_with_nulls = $expected.clone();
            // test before inserting nulls
            let len = $values.len();
            let initial_seeds = vec![42 as $seed_type; len];
            let i = Arc::new(<$t>::from($values)) as ArrayRef;
            $crate::test_hashes_internal!($method, i, initial_seeds, $expected);

            // test with nulls
            let median = len / 2;
            input_with_nulls.insert(0, None);
            input_with_nulls.insert(median, None);
            expected_with_nulls.insert(0, 42 as $seed_type);
            expected_with_nulls.insert(median, 42 as $seed_type);
            let len_with_nulls = len + 2;
            let initial_seeds_with_nulls = vec![42 as $seed_type; len_with_nulls];
            let nullable_input = Arc::new(<$t>::from(input_with_nulls)) as ArrayRef;
            $crate::test_hashes_internal!(
                $method,
                nullable_input,
                initial_seeds_with_nulls,
                expected_with_nulls
            );
        };
    }
}
