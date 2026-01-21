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

use crate::{Result, downcast_value};
use arrow::array::{
    BinaryViewArray, Decimal32Array, Decimal64Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, Float16Array,
    Int8Array, Int16Array, LargeBinaryArray, LargeListViewArray, LargeStringArray,
    ListViewArray, StringViewArray, UInt16Array,
};
use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
        Decimal256Array, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray,
        Float32Array, Float64Array, GenericBinaryArray, GenericListArray,
        GenericStringArray, Int32Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeListArray, ListArray,
        MapArray, NullArray, OffsetSizeTrait, PrimitiveArray, StringArray, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt32Array,
        UInt64Array, UnionArray,
    },
    datatypes::{ArrowDictionaryKeyType, ArrowPrimitiveType},
};

// Downcast Array to Date32Array
pub fn as_date32_array(array: &dyn Array) -> Result<&Date32Array> {
    Ok(downcast_value!(array, Date32Array))
}

// Downcast Array to Date64Array
pub fn as_date64_array(array: &dyn Array) -> Result<&Date64Array> {
    Ok(downcast_value!(array, Date64Array))
}

// Downcast Array to StructArray
pub fn as_struct_array(array: &dyn Array) -> Result<&StructArray> {
    Ok(downcast_value!(array, StructArray))
}

// Downcast Array to Int8Array
pub fn as_int8_array(array: &dyn Array) -> Result<&Int8Array> {
    Ok(downcast_value!(array, Int8Array))
}

// Downcast Array to UInt8Array
pub fn as_uint8_array(array: &dyn Array) -> Result<&UInt8Array> {
    Ok(downcast_value!(array, UInt8Array))
}

// Downcast Array to Int16Array
pub fn as_int16_array(array: &dyn Array) -> Result<&Int16Array> {
    Ok(downcast_value!(array, Int16Array))
}

// Downcast Array to UInt16Array
pub fn as_uint16_array(array: &dyn Array) -> Result<&UInt16Array> {
    Ok(downcast_value!(array, UInt16Array))
}

// Downcast Array to Int32Array
pub fn as_int32_array(array: &dyn Array) -> Result<&Int32Array> {
    Ok(downcast_value!(array, Int32Array))
}

// Downcast Array to UInt32Array
pub fn as_uint32_array(array: &dyn Array) -> Result<&UInt32Array> {
    Ok(downcast_value!(array, UInt32Array))
}

// Downcast Array to Int64Array
pub fn as_int64_array(array: &dyn Array) -> Result<&Int64Array> {
    Ok(downcast_value!(array, Int64Array))
}

// Downcast Array to UInt64Array
pub fn as_uint64_array(array: &dyn Array) -> Result<&UInt64Array> {
    Ok(downcast_value!(array, UInt64Array))
}

// Downcast Array to Decimal32Array
pub fn as_decimal32_array(array: &dyn Array) -> Result<&Decimal32Array> {
    Ok(downcast_value!(array, Decimal32Array))
}

// Downcast Array to Decimal64Array
pub fn as_decimal64_array(array: &dyn Array) -> Result<&Decimal64Array> {
    Ok(downcast_value!(array, Decimal64Array))
}

// Downcast Array to Decimal128Array
pub fn as_decimal128_array(array: &dyn Array) -> Result<&Decimal128Array> {
    Ok(downcast_value!(array, Decimal128Array))
}

// Downcast Array to Decimal256Array
pub fn as_decimal256_array(array: &dyn Array) -> Result<&Decimal256Array> {
    Ok(downcast_value!(array, Decimal256Array))
}

// Downcast Array to Float16Array
pub fn as_float16_array(array: &dyn Array) -> Result<&Float16Array> {
    Ok(downcast_value!(array, Float16Array))
}

// Downcast Array to Float32Array
pub fn as_float32_array(array: &dyn Array) -> Result<&Float32Array> {
    Ok(downcast_value!(array, Float32Array))
}

// Downcast Array to Float64Array
pub fn as_float64_array(array: &dyn Array) -> Result<&Float64Array> {
    Ok(downcast_value!(array, Float64Array))
}

// Downcast Array to StringArray
pub fn as_string_array(array: &dyn Array) -> Result<&StringArray> {
    Ok(downcast_value!(array, StringArray))
}

// Downcast Array to StringViewArray
pub fn as_string_view_array(array: &dyn Array) -> Result<&StringViewArray> {
    Ok(downcast_value!(array, StringViewArray))
}

// Downcast Array to LargeStringArray
pub fn as_large_string_array(array: &dyn Array) -> Result<&LargeStringArray> {
    Ok(downcast_value!(array, LargeStringArray))
}

// Downcast Array to BooleanArray
pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray> {
    Ok(downcast_value!(array, BooleanArray))
}

// Downcast Array to ListArray
pub fn as_list_array(array: &dyn Array) -> Result<&ListArray> {
    Ok(downcast_value!(array, ListArray))
}

// Downcast Array to DictionaryArray
pub fn as_dictionary_array<T: ArrowDictionaryKeyType>(
    array: &dyn Array,
) -> Result<&DictionaryArray<T>> {
    Ok(downcast_value!(array, DictionaryArray, T))
}

// Downcast Array to GenericBinaryArray
pub fn as_generic_binary_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericBinaryArray<T>> {
    Ok(downcast_value!(array, GenericBinaryArray, T))
}

// Downcast Array to GenericListArray
pub fn as_generic_list_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericListArray<T>> {
    Ok(downcast_value!(array, GenericListArray, T))
}

// Downcast Array to LargeListArray
pub fn as_large_list_array(array: &dyn Array) -> Result<&LargeListArray> {
    Ok(downcast_value!(array, LargeListArray))
}

// Downcast Array to PrimitiveArray
pub fn as_primitive_array<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> Result<&PrimitiveArray<T>> {
    Ok(downcast_value!(array, PrimitiveArray, T))
}

// Downcast Array to MapArray
pub fn as_map_array(array: &dyn Array) -> Result<&MapArray> {
    Ok(downcast_value!(array, MapArray))
}

// Downcast Array to NullArray
pub fn as_null_array(array: &dyn Array) -> Result<&NullArray> {
    Ok(downcast_value!(array, NullArray))
}

// Downcast Array to NullArray
pub fn as_union_array(array: &dyn Array) -> Result<&UnionArray> {
    Ok(downcast_value!(array, UnionArray))
}

// Downcast Array to Time32SecondArray
pub fn as_time32_second_array(array: &dyn Array) -> Result<&Time32SecondArray> {
    Ok(downcast_value!(array, Time32SecondArray))
}

// Downcast Array to Time32MillisecondArray
pub fn as_time32_millisecond_array(array: &dyn Array) -> Result<&Time32MillisecondArray> {
    Ok(downcast_value!(array, Time32MillisecondArray))
}

// Downcast Array to Time64MicrosecondArray
pub fn as_time64_microsecond_array(array: &dyn Array) -> Result<&Time64MicrosecondArray> {
    Ok(downcast_value!(array, Time64MicrosecondArray))
}

// Downcast Array to Time64NanosecondArray
pub fn as_time64_nanosecond_array(array: &dyn Array) -> Result<&Time64NanosecondArray> {
    Ok(downcast_value!(array, Time64NanosecondArray))
}

// Downcast Array to TimestampNanosecondArray
pub fn as_timestamp_nanosecond_array(
    array: &dyn Array,
) -> Result<&TimestampNanosecondArray> {
    Ok(downcast_value!(array, TimestampNanosecondArray))
}

// Downcast Array to TimestampMillisecondArray
pub fn as_timestamp_millisecond_array(
    array: &dyn Array,
) -> Result<&TimestampMillisecondArray> {
    Ok(downcast_value!(array, TimestampMillisecondArray))
}

// Downcast Array to TimestampMicrosecondArray
pub fn as_timestamp_microsecond_array(
    array: &dyn Array,
) -> Result<&TimestampMicrosecondArray> {
    Ok(downcast_value!(array, TimestampMicrosecondArray))
}

// Downcast Array to TimestampSecondArray
pub fn as_timestamp_second_array(array: &dyn Array) -> Result<&TimestampSecondArray> {
    Ok(downcast_value!(array, TimestampSecondArray))
}

// Downcast Array to IntervalYearMonthArray
pub fn as_interval_ym_array(array: &dyn Array) -> Result<&IntervalYearMonthArray> {
    Ok(downcast_value!(array, IntervalYearMonthArray))
}

// Downcast Array to IntervalDayTimeArray
pub fn as_interval_dt_array(array: &dyn Array) -> Result<&IntervalDayTimeArray> {
    Ok(downcast_value!(array, IntervalDayTimeArray))
}

// Downcast Array to IntervalMonthDayNanoArray
pub fn as_interval_mdn_array(array: &dyn Array) -> Result<&IntervalMonthDayNanoArray> {
    Ok(downcast_value!(array, IntervalMonthDayNanoArray))
}

// Downcast Array to DurationSecondArray
pub fn as_duration_second_array(array: &dyn Array) -> Result<&DurationSecondArray> {
    Ok(downcast_value!(array, DurationSecondArray))
}

// Downcast Array to DurationMillisecondArray
pub fn as_duration_millisecond_array(
    array: &dyn Array,
) -> Result<&DurationMillisecondArray> {
    Ok(downcast_value!(array, DurationMillisecondArray))
}

// Downcast Array to DurationMicrosecondArray
pub fn as_duration_microsecond_array(
    array: &dyn Array,
) -> Result<&DurationMicrosecondArray> {
    Ok(downcast_value!(array, DurationMicrosecondArray))
}

// Downcast Array to DurationNanosecondArray
pub fn as_duration_nanosecond_array(
    array: &dyn Array,
) -> Result<&DurationNanosecondArray> {
    Ok(downcast_value!(array, DurationNanosecondArray))
}

// Downcast Array to BinaryArray
pub fn as_binary_array(array: &dyn Array) -> Result<&BinaryArray> {
    Ok(downcast_value!(array, BinaryArray))
}

// Downcast Array to BinaryViewArray
pub fn as_binary_view_array(array: &dyn Array) -> Result<&BinaryViewArray> {
    Ok(downcast_value!(array, BinaryViewArray))
}

// Downcast Array to LargeBinaryArray
pub fn as_large_binary_array(array: &dyn Array) -> Result<&LargeBinaryArray> {
    Ok(downcast_value!(array, LargeBinaryArray))
}

// Downcast Array to FixedSizeListArray
pub fn as_fixed_size_list_array(array: &dyn Array) -> Result<&FixedSizeListArray> {
    Ok(downcast_value!(array, FixedSizeListArray))
}

// Downcast Array to FixedSizeBinaryArray
pub fn as_fixed_size_binary_array(array: &dyn Array) -> Result<&FixedSizeBinaryArray> {
    Ok(downcast_value!(array, FixedSizeBinaryArray))
}

// Downcast Array to GenericBinaryArray
pub fn as_generic_string_array<T: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<&GenericStringArray<T>> {
    Ok(downcast_value!(array, GenericStringArray, T))
}

// Downcast Array to ListViewArray
pub fn as_list_view_array(array: &dyn Array) -> Result<&ListViewArray> {
    Ok(downcast_value!(array, ListViewArray))
}

// Downcast Array to LargeListViewArray
pub fn as_large_list_view_array(array: &dyn Array) -> Result<&LargeListViewArray> {
    Ok(downcast_value!(array, LargeListViewArray))
}
