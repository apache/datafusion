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

use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, ArrowPrimitiveType, BinaryViewArray,
    BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, Float32Array,
    Float64Array, GenericBinaryArray, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
    LargeListArray, LargeListViewArray, LargeStringArray, ListArray, ListViewArray,
    MapArray, PrimitiveArray, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array, UnionArray,
};
use arrow::datatypes::{
    i256, ArrowDictionaryKeyType, DataType, Int16Type, Int32Type, Int64Type, Int8Type,
    IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use bigdecimal::num_traits::{Float, ToBytes};
use datafusion_common::{exec_err, DataFusionError, Result};
use std::sync::Arc;

fn hash_impl<T, H: Copy>(
    arr: ArrayIter<impl ArrayAccessor<Item = T>>,
    seed: &mut [H],
    func: impl Fn(H, T) -> H,
) -> Result<()> {
    let len = arr.len();
    if len != seed.len() {
        return exec_err!("Array length mismatch: {} != {}", len, seed.len());
    }
    for (hash, elem) in seed.iter_mut().zip(arr) {
        if let Some(elem) = elem {
            *hash = func(*hash, elem);
        }
    }
    Ok(())
}

fn try_hash_impl<T, H: Copy>(
    arr: ArrayIter<impl ArrayAccessor<Item = T>>,
    seed: &mut [H],
    func: impl Fn(H, T) -> Result<H>,
) -> Result<()> {
    let len = arr.len();
    if len != seed.len() {
        return exec_err!("Array length mismatch: {} != {}", len, seed.len());
    }
    for (hash, elem) in seed.iter_mut().zip(arr) {
        if let Some(elem) = elem {
            *hash = func(*hash, elem)?;
        }
    }
    Ok(())
}

pub trait SparkHasher<H: Copy + std::fmt::Debug> {
    fn oneshot(seed: H, data: &[u8]) -> H;

    fn hash_boolean(arr: &BooleanArray, seed: &mut [H]) -> Result<()> {
        let true_bytes = 1_i32.to_le_bytes();
        let false_bytes = 0_i32.to_le_bytes();
        hash_impl(arr.iter(), seed, |seed, v| {
            if v {
                Self::oneshot(seed, &true_bytes)
            } else {
                Self::oneshot(seed, &false_bytes)
            }
        })?;
        Ok(())
    }

    fn hash_primitive<T, U>(arr: &PrimitiveArray<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrowPrimitiveType,
        U: ToBytes + From<T::Native>,
    {
        hash_impl(arr.iter(), seed, |seed, v| {
            let bytes = U::from(v).to_le_bytes();
            Self::oneshot(seed, bytes.as_ref())
        })?;
        Ok(())
    }

    fn try_hash_primitive<T, U>(arr: &PrimitiveArray<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrowPrimitiveType,
        U: ToBytes + TryFrom<T::Native>,
        <U as TryFrom<T::Native>>::Error: std::fmt::Display,
    {
        try_hash_impl(arr.iter(), seed, |seed, v| {
            let uv =
                U::try_from(v).map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let bytes = uv.to_le_bytes();
            Ok(Self::oneshot(seed, bytes.as_ref()))
        })?;
        Ok(())
    }

    fn hash_primitive_float<T, U>(arr: &PrimitiveArray<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrowPrimitiveType,
        T::Native: ToBytes + Float,
        U: ToBytes + Default,
    {
        let neg_zero = T::Native::neg_zero();
        let neg_zero_bytes = U::to_le_bytes(&U::default());

        hash_impl(arr.iter(), seed, |seed, v| {
            if v == neg_zero {
                Self::oneshot(seed, neg_zero_bytes.as_ref())
            } else {
                let bytes = v.to_le_bytes();
                Self::oneshot(seed, bytes.as_ref())
            }
        })?;
        Ok(())
    }

    fn hash_bytes<T>(iter: ArrayIter<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrayAccessor,
        T::Item: AsRef<[u8]>,
    {
        hash_impl(iter, seed, |seed, v| Self::oneshot(seed, v.as_ref()))?;
        Ok(())
    }

    fn hash_list<T>(iter: ArrayIter<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrayAccessor<Item = ArrayRef>,
    {
        try_hash_impl(iter, seed, |seed, v| {
            let len = v.len();
            let mut result = [seed; 1];
            for i in 0..len {
                let slice = v.slice(i, 1);
                Self::hash(&slice, &mut result)?;
            }
            Ok(result[0])
        })?;
        Ok(())
    }

    fn hash_map<T>(iter: ArrayIter<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrayAccessor<Item = StructArray>,
    {
        try_hash_impl(iter, seed, |seed, v| {
            let len = v.len();
            let mut result = [seed; 1];
            for i in 0..len {
                let slice = v.slice(i, 1);
                Self::hash(slice.column(0), &mut result)?;
                Self::hash(slice.column(1), &mut result)?;
            }
            Ok(result[0])
        })?;
        Ok(())
    }

    fn hash_struct(arr: &StructArray, seed: &mut [H]) -> Result<()> {
        Self::hash_arrays(arr.columns(), seed)
    }

    fn hash_arrays(arr: &[ArrayRef], seed: &mut [H]) -> Result<()> {
        for arr in arr {
            Self::hash(arr, seed)?;
        }
        Ok(())
    }

    fn hash_dictionary<T>(arr: &DictionaryArray<T>, seed: &mut [H]) -> Result<()>
    where
        T: ArrowDictionaryKeyType,
    {
        let len = arr.len();
        if len != seed.len() {
            return exec_err!("Array length mismatch: {} != {}", len, seed.len());
        }

        for (i, seed) in seed.iter_mut().enumerate().take(arr.len()) {
            if arr.is_null(i) {
                continue;
            }
            let mut result = [*seed; 1];

            let value = arr.values().slice(i, 1);
            let key: Arc<dyn Array> = Arc::new(arr.keys().slice(i, 1));

            for j in 0..key.len() {
                let value = value.slice(j, 1);
                let key = key.slice(j, 1);
                Self::hash(&key, &mut result)?;
                Self::hash(&value, &mut result)?;
            }
            *seed = result[0];
        }
        Ok(())
    }

    fn hash_union(arr: &UnionArray, seed: &mut [H]) -> Result<()> {
        let len = arr.len();
        if len != seed.len() {
            return exec_err!("Array length mismatch: {} != {}", len, seed.len());
        }

        for (i, seed) in seed.iter_mut().enumerate().take(len) {
            let value = arr.value(i);
            let mut result = [*seed; 1];
            for i in 0..value.len() {
                let elem = value.slice(i, 1);
                Self::hash(&elem, &mut result)?;
            }
            *seed = result[0];
        }
        Ok(())
    }

    /// Computes the xxHash64 hash of the given data
    fn hash(arr: &ArrayRef, seed: &mut [H]) -> Result<()> {
        match arr.data_type() {
            DataType::Null => {
                // do nothing
            }
            DataType::Boolean => {
                let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                Self::hash_boolean(arr, seed)?;
            }
            DataType::Int8 => {
                let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::UInt8 => {
                let arr = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Int16 => {
                let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::UInt16 => {
                let arr = arr.as_any().downcast_ref::<UInt16Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Int32 => {
                let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::UInt32 => {
                let arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
                Self::hash_primitive::<_, u32>(arr, seed)?;
            }
            DataType::Int64 => {
                let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::UInt64 => {
                let arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                Self::hash_primitive::<_, u64>(arr, seed)?;
            }
            DataType::Float32 => {
                let arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
                Self::hash_primitive_float::<_, i32>(arr, seed)?;
            }
            DataType::Float64 => {
                let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                Self::hash_primitive_float::<_, i64>(arr, seed)?;
            }
            DataType::Decimal128(precision, _) if *precision <= 18 => {
                let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                Self::try_hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Decimal128(_, _) => {
                let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                Self::hash_primitive::<_, i128>(arr, seed)?;
            }
            DataType::Decimal256(_, _) => {
                let arr = arr.as_any().downcast_ref::<Decimal256Array>().unwrap();
                Self::hash_primitive::<_, I256>(arr, seed)?;
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let arr = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Date32 => {
                let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Date64 => {
                let arr = arr.as_any().downcast_ref::<Date64Array>().unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Utf8 => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::LargeUtf8 => {
                let arr = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::Binary => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i32>>()
                    .unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::LargeBinary => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<GenericBinaryArray<i64>>()
                    .unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::FixedSizeBinary(_) => {
                let arr = arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::Utf8View => {
                let arr = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::BinaryView => {
                let arr = arr.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                Self::hash_bytes(arr.iter(), seed)?;
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let arr = arr.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
                Self::hash_primitive::<_, _IntervalDayTime>(arr, seed)?;
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap();
                Self::hash_primitive::<_, _IntervalMonthDayNano>(arr, seed)?;
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<IntervalYearMonthArray>()
                    .unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Duration(TimeUnit::Second) => {
                let arr = arr.as_any().downcast_ref::<DurationSecondArray>().unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Time32(TimeUnit::Second) => {
                let arr = arr.as_any().downcast_ref::<Time32SecondArray>().unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i32>(arr, seed)?;
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap();
                Self::hash_primitive::<_, i64>(arr, seed)?;
            }
            DataType::List(_) => {
                let arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                Self::hash_list(arr.iter(), seed)?;
            }
            DataType::ListView(_) => {
                let arr = arr.as_any().downcast_ref::<ListViewArray>().unwrap();
                Self::hash_list(arr.iter(), seed)?;
            }
            DataType::LargeList(_) => {
                let arr = arr.as_any().downcast_ref::<LargeListArray>().unwrap();
                Self::hash_list(arr.iter(), seed)?;
            }
            DataType::LargeListView(_) => {
                let arr = arr.as_any().downcast_ref::<LargeListViewArray>().unwrap();
                Self::hash_list(arr.iter(), seed)?;
            }
            DataType::Map(_, _) => {
                let arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
                Self::hash_map(arr.iter(), seed)?;
            }
            DataType::Struct(_) => {
                let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
                Self::hash_struct(arr, seed)?;
            }
            DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                DataType::Int8 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int8Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::UInt8 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt8Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::Int16 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int16Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::UInt16 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt16Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::Int32 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::UInt32 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt32Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::Int64 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int64Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::UInt64 => {
                    let arr = arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<UInt64Type>>()
                        .unwrap();
                    Self::hash_dictionary(arr, seed)?;
                }
                DataType::Union(_, _) => {
                    let arr = arr.as_any().downcast_ref::<UnionArray>().unwrap();
                    Self::hash_union(arr, seed)?;
                }
                _ => {
                    return exec_err!("Unsupported key type: {}", key_type);
                }
            },
            _ => {
                return exec_err!("Unsupported data type: {}", arr.data_type());
            }
        };
        Ok(())
    }
}

struct I256(i256);

impl From<i256> for I256 {
    fn from(value: i256) -> Self {
        I256(value)
    }
}

impl ToBytes for I256 {
    type Bytes = [u8; 32];

    fn to_le_bytes(&self) -> Self::Bytes {
        self.0.to_le_bytes()
    }

    fn to_be_bytes(&self) -> Self::Bytes {
        self.0.to_be_bytes()
    }
}

struct _IntervalDayTime(IntervalDayTime);

impl From<IntervalDayTime> for _IntervalDayTime {
    fn from(value: IntervalDayTime) -> Self {
        _IntervalDayTime(value)
    }
}

impl ToBytes for _IntervalDayTime {
    type Bytes = [u8; 8];

    fn to_le_bytes(&self) -> Self::Bytes {
        let days = self.0.days.to_le_bytes();
        let milliseconds = self.0.milliseconds.to_le_bytes();
        let mut bytes = [0; 8];
        bytes[..4].copy_from_slice(&days);
        bytes[4..].copy_from_slice(&milliseconds);
        bytes
    }

    fn to_be_bytes(&self) -> Self::Bytes {
        let days = self.0.days.to_be_bytes();
        let milliseconds = self.0.milliseconds.to_be_bytes();
        let mut bytes = [0; 8];
        bytes[..4].copy_from_slice(&days);
        bytes[4..].copy_from_slice(&milliseconds);
        bytes
    }
}

struct _IntervalMonthDayNano(IntervalMonthDayNano);

impl From<IntervalMonthDayNano> for _IntervalMonthDayNano {
    fn from(value: IntervalMonthDayNano) -> Self {
        _IntervalMonthDayNano(value)
    }
}

impl ToBytes for _IntervalMonthDayNano {
    type Bytes = [u8; 16];

    fn to_le_bytes(&self) -> Self::Bytes {
        let months = self.0.months.to_le_bytes();
        let days = self.0.days.to_le_bytes();
        let nanoseconds = self.0.nanoseconds.to_le_bytes();
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&months);
        bytes[4..8].copy_from_slice(&days);
        bytes[8..].copy_from_slice(&nanoseconds);
        bytes
    }

    fn to_be_bytes(&self) -> Self::Bytes {
        let months = self.0.months.to_be_bytes();
        let days = self.0.days.to_be_bytes();
        let nanoseconds = self.0.nanoseconds.to_be_bytes();
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&months);
        bytes[4..8].copy_from_slice(&days);
        bytes[8..].copy_from_slice(&nanoseconds);
        bytes
    }
}
