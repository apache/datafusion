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

//! This module provides DataFusion specific casting functions
//! that provide error handling. They are intended to "never fail"
//! but provide an error message rather than a panic, as the corresponding
//! kernels in arrow-rs such as `as_boolean_array` do.

use crate::{downcast_value, DataFusionError, Result};
use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array,
        Float64Array, GenericBinaryArray, GenericListArray, GenericStringArray,
        Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
        IntervalYearMonthArray, LargeListArray, ListArray, MapArray, NullArray,
        OffsetSizeTrait, PrimitiveArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt32Array, UInt64Array, UInt8Array, UnionArray,
    },
    datatypes::{ArrowDictionaryKeyType, ArrowPrimitiveType},
};
use arrow_array::Decimal256Array;

// Downcast ArrayRef to Date32Array
pub fn as_date32_array(array: &dyn Array) -> Result<&Date32Array> {
    Ok(downcast_value!(array, Date32Array))
}

// Downcast ArrayRef to StructArray
pub fn as_struct_array(array: &dyn Array) -> Result<&StructArray> {
    Ok(downcast_value!(array, StructArray))
}

// Downcast ArrayRef to UInt8Array
pub fn as_uint8_array(array: &dyn Array) -> Result<&UInt8Array> {
    Ok(downcast_value!(array, UInt8Array))
}

// Downcast ArrayRef to Int32Array
pub fn as_int32_array(array: &dyn Array) -> Result<&Int32Array> {
    Ok(downcast_value!(array, Int32Array))
}

// Downcast ArrayRef to Int64Array
pub fn as_int64_array(array: &dyn Array) -> Result<&Int64Array> {
    Ok(downcast_value!(array, Int64Array))
}

// Downcast ArrayRef to Decimal128Array
pub fn as_decimal128_array(array: &dyn Array) -> Result<&Decimal128Array> {
    Ok(downcast_value!(array, Decimal128Array))
}

// Downcast ArrayRef to Decimal256Array
pub fn as_decimal256_array(array: &dyn Array) -> Result<&Decimal256Array> {
    Ok(downcast_value!(array, Decimal256Array))
}

// Downcast ArrayRef to Float32Array
pub fn as_float32_array(array: &dyn Array) -> Result<&Float32Array> {
    Ok(downcast_value!(array, Float32Array))
}

// Downcast ArrayRef to Float64Array
pub fn as_float64_array(array: &dyn Array) -> Result<&Float64Array> {
    Ok(downcast_value!(array, Float64Array))
}

// Downcast ArrayRef to StringArray
pub fn as_string_array(array: &dyn Array) -> Result<&StringArray> {
    Ok(downcast_value!(array, StringArray))
}

// Downcast ArrayRef to UInt32Array
pub fn as_uint32_array(array: &dyn Array) -> Result<&UInt32Array> {
    Ok(downcast_value!(array, UInt32Array))
}

// Downcast ArrayRef to UInt64Array
pub fn as_uint64_array(array: &dyn Array) -> Result<&UInt64Array> {
    Ok(downcast_value!(array, UInt64Array))
}

// Downcast ArrayRef to BooleanArray
pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray> {
    Ok(downcast_value!(array, BooleanArray))
}

// Downcast ArrayRef to ListArray
pub fn as_list_array(array: &dyn Array) -> Result<&ListArray> {
    Ok(downcast_value!(array, ListArray))
}

// Downcast ArrayRef to DictionaryArray
pub fn as_dictionary_array<T: ArrowDictionaryKeyType>(
    array: &dyn Array,
) -> Result<&DictionaryArray<T>> {
    Ok(downcast_value!(array, DictionaryArray, T))
}

// Downcast ArrayRef to GenericBinaryArray
pub fn as_generic_binary_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericBinaryArray<T>> {
    Ok(downcast_value!(array, GenericBinaryArray, T))
}

// Downcast ArrayRef to GenericListArray
pub fn as_generic_list_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericListArray<T>> {
    Ok(downcast_value!(array, GenericListArray, T))
}

// Downcast ArrayRef to LargeListArray
pub fn as_large_list_array(array: &dyn Array) -> Result<&LargeListArray> {
    Ok(downcast_value!(array, LargeListArray))
}

// Downcast ArrayRef to PrimitiveArray
pub fn as_primitive_array<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> Result<&PrimitiveArray<T>> {
    Ok(downcast_value!(array, PrimitiveArray, T))
}

// Downcast ArrayRef to MapArray
pub fn as_map_array(array: &dyn Array) -> Result<&MapArray> {
    Ok(downcast_value!(array, MapArray))
}

// Downcast ArrayRef to NullArray
pub fn as_null_array(array: &dyn Array) -> Result<&NullArray> {
    Ok(downcast_value!(array, NullArray))
}

// Downcast ArrayRef to NullArray
pub fn as_union_array(array: &dyn Array) -> Result<&UnionArray> {
    Ok(downcast_value!(array, UnionArray))
}

// Downcast ArrayRef to TimestampNanosecondArray
pub fn as_timestamp_nanosecond_array(
    array: &dyn Array,
) -> Result<&TimestampNanosecondArray> {
    Ok(downcast_value!(array, TimestampNanosecondArray))
}

// Downcast ArrayRef to TimestampMillisecondArray
pub fn as_timestamp_millisecond_array(
    array: &dyn Array,
) -> Result<&TimestampMillisecondArray> {
    Ok(downcast_value!(array, TimestampMillisecondArray))
}

// Downcast ArrayRef to TimestampMicrosecondArray
pub fn as_timestamp_microsecond_array(
    array: &dyn Array,
) -> Result<&TimestampMicrosecondArray> {
    Ok(downcast_value!(array, TimestampMicrosecondArray))
}

// Downcast ArrayRef to TimestampSecondArray
pub fn as_timestamp_second_array(array: &dyn Array) -> Result<&TimestampSecondArray> {
    Ok(downcast_value!(array, TimestampSecondArray))
}

// Downcast ArrayRef to IntervalYearMonthArray
pub fn as_interval_ym_array(array: &dyn Array) -> Result<&IntervalYearMonthArray> {
    Ok(downcast_value!(array, IntervalYearMonthArray))
}

// Downcast ArrayRef to IntervalDayTimeArray
pub fn as_interval_dt_array(array: &dyn Array) -> Result<&IntervalDayTimeArray> {
    Ok(downcast_value!(array, IntervalDayTimeArray))
}

// Downcast ArrayRef to IntervalMonthDayNanoArray
pub fn as_interval_mdn_array(array: &dyn Array) -> Result<&IntervalMonthDayNanoArray> {
    Ok(downcast_value!(array, IntervalMonthDayNanoArray))
}

// Downcast ArrayRef to BinaryArray
pub fn as_binary_array(array: &dyn Array) -> Result<&BinaryArray> {
    Ok(downcast_value!(array, BinaryArray))
}

// Downcast ArrayRef to FixedSizeListArray
pub fn as_fixed_size_list_array(array: &dyn Array) -> Result<&FixedSizeListArray> {
    Ok(downcast_value!(array, FixedSizeListArray))
}

// Downcast ArrayRef to FixedSizeListArray
pub fn as_fixed_size_binary_array(array: &dyn Array) -> Result<&FixedSizeBinaryArray> {
    Ok(downcast_value!(array, FixedSizeBinaryArray))
}

// Downcast ArrayRef to Date64Array
pub fn as_date64_array(array: &dyn Array) -> Result<&Date64Array> {
    Ok(downcast_value!(array, Date64Array))
}

// Downcast ArrayRef to GenericBinaryArray
pub fn as_generic_string_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericStringArray<T>> {
    Ok(downcast_value!(array, GenericStringArray, T))
}
