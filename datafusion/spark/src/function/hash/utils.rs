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

//! Shared macros for Spark-compatible hash functions (xxhash64 and murmur3).
//! Ported from Apache DataFusion Comet:
//! <https://github.com/apache/datafusion-comet/blob/main/native/spark-expr/src/hash_funcs/utils.rs>

/// Main type dispatcher macro covering all Arrow types including complex types
/// (List, LargeList, FixedSizeList, Struct, Map).
///
/// Dictionary types are NOT handled by this macro — they must be handled by the
/// caller before invoking this macro, since dictionary dispatch requires
/// turbofish syntax that doesn't work inside macros.
///
/// Parameters:
/// - `$col` - the column `&ArrayRef`
/// - `$hashes` - `&mut [HashType]` buffer
/// - `$hash_fn` - the core hash function `(bytes, seed) -> hash`
/// - `$create_hashes_fn` - recursive hashing function for complex types
/// - `$func_name` - function name string for error messages
macro_rules! create_hashes_internal {
    ($col:ident, $hashes:ident, $hash_fn:path, $create_hashes_fn:path, $func_name:expr) => {{
        use arrow::array::{
            Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array,
            Date64Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
            Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
            LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        use arrow::datatypes::TimeUnit;

        match $col.data_type() {
            DataType::Boolean => {
                let array = $col.as_any().downcast_ref::<BooleanArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(i32::from(array.value(i)).to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash =
                                $hash_fn(i32::from(array.value(i)).to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Int8 => {
                let array = $col.as_any().downcast_ref::<Int8Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn((*val as i32).to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn((values[i] as i32).to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Int16 => {
                let array = $col.as_any().downcast_ref::<Int16Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn((*val as i32).to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn((values[i] as i32).to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Int32 => {
                let array = $col.as_any().downcast_ref::<Int32Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Int64 => {
                let array = $col.as_any().downcast_ref::<Int64Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Float32 => {
                let array = $col.as_any().downcast_ref::<Float32Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        let bytes = if *val == 0.0 && val.is_sign_negative() {
                            0i32.to_le_bytes()
                        } else {
                            val.to_le_bytes()
                        };
                        *hash = $hash_fn(bytes, *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            let val = values[i];
                            let bytes = if val == 0.0 && val.is_sign_negative() {
                                0i32.to_le_bytes()
                            } else {
                                val.to_le_bytes()
                            };
                            *hash = $hash_fn(bytes, *hash);
                        }
                    }
                }
            }
            DataType::Float64 => {
                let array = $col.as_any().downcast_ref::<Float64Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        let bytes = if *val == 0.0 && val.is_sign_negative() {
                            0i64.to_le_bytes()
                        } else {
                            val.to_le_bytes()
                        };
                        *hash = $hash_fn(bytes, *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            let val = values[i];
                            let bytes = if val == 0.0 && val.is_sign_negative() {
                                0i64.to_le_bytes()
                            } else {
                                val.to_le_bytes()
                            };
                            *hash = $hash_fn(bytes, *hash);
                        }
                    }
                }
            }
            DataType::Date32 => {
                let array = $col.as_any().downcast_ref::<Date32Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Date64 => {
                let array = $col.as_any().downcast_ref::<Date64Array>().unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let array = $col
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = $col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = $col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = $col
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let values = array.values();
                if array.null_count() == 0 {
                    for (hash, val) in $hashes.iter_mut().zip(values.iter()) {
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(values[i].to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Utf8 => {
                let array = $col.as_any().downcast_ref::<StringArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::LargeUtf8 => {
                let array = $col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::Utf8View => {
                let array = $col.as_string_view();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::Binary => {
                let array = $col.as_any().downcast_ref::<BinaryArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::LargeBinary => {
                let array = $col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::BinaryView => {
                let array = $col.as_binary_view();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::FixedSizeBinary(_) => {
                let array = $col
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i), *hash);
                        }
                    }
                }
            }
            DataType::Decimal128(precision, _) if *precision <= 18 => {
                let array = $col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        let val = i64::try_from(array.value(i)).map_err(|e| {
                            datafusion_common::DataFusionError::Internal(format!(
                                "Failed to convert Decimal128 to i64: {e}"
                            ))
                        })?;
                        *hash = $hash_fn(val.to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            let val = i64::try_from(array.value(i)).map_err(|e| {
                                datafusion_common::DataFusionError::Internal(format!(
                                    "Failed to convert Decimal128 to i64: {e}"
                                ))
                            })?;
                            *hash = $hash_fn(val.to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let array = $col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                if array.null_count() == 0 {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        *hash = $hash_fn(array.value(i).to_le_bytes(), *hash);
                    }
                } else {
                    for (i, hash) in $hashes.iter_mut().enumerate() {
                        if !array.is_null(i) {
                            *hash = $hash_fn(array.value(i).to_le_bytes(), *hash);
                        }
                    }
                }
            }
            DataType::Null => {
                // Nulls don't update the hash
            }
            DataType::List(_) => {
                let list_array = $col
                    .as_any()
                    .downcast_ref::<arrow::array::ListArray>()
                    .unwrap();
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !list_array.is_null(i) {
                        let elem_array = list_array.value(i);
                        let num_elems = elem_array.len();
                        if num_elems > 0 {
                            let mut elem_hashes = vec![*hash; num_elems];
                            $create_hashes_fn(&[elem_array], &mut elem_hashes)?;
                            for eh in &elem_hashes {
                                *hash = *eh;
                            }
                        }
                    }
                }
            }
            DataType::LargeList(_) => {
                let list_array = $col
                    .as_any()
                    .downcast_ref::<arrow::array::LargeListArray>()
                    .unwrap();
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !list_array.is_null(i) {
                        let elem_array = list_array.value(i);
                        let num_elems = elem_array.len();
                        if num_elems > 0 {
                            let mut elem_hashes = vec![*hash; num_elems];
                            $create_hashes_fn(&[elem_array], &mut elem_hashes)?;
                            for eh in &elem_hashes {
                                *hash = *eh;
                            }
                        }
                    }
                }
            }
            DataType::FixedSizeList(_, _) => {
                let list_array = $col
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeListArray>()
                    .unwrap();
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !list_array.is_null(i) {
                        let elem_array = list_array.value(i);
                        let num_elems = elem_array.len();
                        if num_elems > 0 {
                            let mut elem_hashes = vec![*hash; num_elems];
                            $create_hashes_fn(&[elem_array], &mut elem_hashes)?;
                            for eh in &elem_hashes {
                                *hash = *eh;
                            }
                        }
                    }
                }
            }
            DataType::Struct(_) => {
                let struct_array = $col
                    .as_any()
                    .downcast_ref::<arrow::array::StructArray>()
                    .unwrap();
                let columns: Vec<ArrayRef> = struct_array.columns().to_vec();
                $create_hashes_fn(&columns, $hashes)?;
            }
            DataType::Map(_, _) => {
                let map_array = $col
                    .as_any()
                    .downcast_ref::<arrow::array::MapArray>()
                    .unwrap();
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    if !map_array.is_null(i) {
                        let entry_struct = map_array.value(i);
                        let num_entries = entry_struct.len();
                        if num_entries > 0 {
                            let struct_array = entry_struct
                                .as_any()
                                .downcast_ref::<arrow::array::StructArray>()
                                .unwrap();
                            let columns: Vec<ArrayRef> = struct_array.columns().to_vec();
                            let mut entry_hashes = vec![*hash; num_entries];
                            $create_hashes_fn(&columns, &mut entry_hashes)?;
                            for eh in &entry_hashes {
                                *hash = *eh;
                            }
                        }
                    }
                }
            }
            dt => {
                return internal_err!("Unsupported data type for {}: {dt}", $func_name);
            }
        }
    }};
}

pub(crate) use create_hashes_internal;
