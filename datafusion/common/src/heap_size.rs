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

use crate::stats::Precision;
use crate::{ColumnStatistics, ScalarValue, Statistics};
use arrow::array::{
    Array, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
};
use arrow::datatypes::{
    DataType, Field, Fields, IntervalDayTime, IntervalMonthDayNano, IntervalUnit,
    TimeUnit, UnionFields, UnionMode, i256,
};
use chrono::{DateTime, Utc};
use half::f16;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// This is a temporary solution until <https://github.com/apache/datafusion/pull/19599> and
/// <https://github.com/apache/arrow-rs/pull/9138> are resolved.
/// Trait for calculating the size of various containers
pub trait DFHeapSize {
    /// Return the size of any bytes allocated on the heap by this object,
    /// including heap memory in those structures
    ///
    /// Note that the size of the type itself is not included in the result --
    /// instead, that size is added by the caller (e.g. container).
    fn heap_size(&self) -> usize;
}

impl DFHeapSize for Statistics {
    fn heap_size(&self) -> usize {
        self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self
                .column_statistics
                .iter()
                .map(|s| s.heap_size())
                .sum::<usize>()
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd + DFHeapSize> DFHeapSize
    for Precision<T>
{
    fn heap_size(&self) -> usize {
        self.get_value().map_or_else(|| 0, |v| v.heap_size())
    }
}

impl DFHeapSize for ColumnStatistics {
    fn heap_size(&self) -> usize {
        self.null_count.heap_size()
            + self.max_value.heap_size()
            + self.min_value.heap_size()
            + self.sum_value.heap_size()
            + self.distinct_count.heap_size()
            + self.byte_size.heap_size()
    }
}

impl DFHeapSize for ScalarValue {
    fn heap_size(&self) -> usize {
        use crate::scalar::ScalarValue::*;
        match self {
            Null => 0,
            Boolean(b) => b.heap_size(),
            Float16(f) => f.heap_size(),
            Float32(f) => f.heap_size(),
            Float64(f) => f.heap_size(),
            Decimal32(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
            Decimal64(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
            Decimal128(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
            Decimal256(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
            Int8(i) => i.heap_size(),
            Int16(i) => i.heap_size(),
            Int32(i) => i.heap_size(),
            Int64(i) => i.heap_size(),
            UInt8(u) => u.heap_size(),
            UInt16(u) => u.heap_size(),
            UInt32(u) => u.heap_size(),
            UInt64(u) => u.heap_size(),
            Utf8(u) => u.heap_size(),
            Utf8View(u) => u.heap_size(),
            LargeUtf8(l) => l.heap_size(),
            Binary(b) => b.heap_size(),
            BinaryView(b) => b.heap_size(),
            FixedSizeBinary(a, b) => a.heap_size() + b.heap_size(),
            LargeBinary(l) => l.heap_size(),
            FixedSizeList(f) => f.heap_size(),
            List(l) => l.heap_size(),
            LargeList(l) => l.heap_size(),
            Struct(s) => s.heap_size(),
            Map(m) => m.heap_size(),
            Date32(d) => d.heap_size(),
            Date64(d) => d.heap_size(),
            Time32Second(t) => t.heap_size(),
            Time32Millisecond(t) => t.heap_size(),
            Time64Microsecond(t) => t.heap_size(),
            Time64Nanosecond(t) => t.heap_size(),
            TimestampSecond(a, b) => a.heap_size() + b.heap_size(),
            TimestampMillisecond(a, b) => a.heap_size() + b.heap_size(),
            TimestampMicrosecond(a, b) => a.heap_size() + b.heap_size(),
            TimestampNanosecond(a, b) => a.heap_size() + b.heap_size(),
            IntervalYearMonth(i) => i.heap_size(),
            IntervalDayTime(i) => i.heap_size(),
            IntervalMonthDayNano(i) => i.heap_size(),
            DurationSecond(d) => d.heap_size(),
            DurationMillisecond(d) => d.heap_size(),
            DurationMicrosecond(d) => d.heap_size(),
            DurationNanosecond(d) => d.heap_size(),
            Union(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
            Dictionary(a, b) => a.heap_size() + b.heap_size(),
            RunEndEncoded(a, b, c) => a.heap_size() + b.heap_size() + c.heap_size(),
        }
    }
}

impl DFHeapSize for DataType {
    fn heap_size(&self) -> usize {
        use DataType::*;
        match self {
            Null => 0,
            Boolean => 0,
            Int8 => 0,
            Int16 => 0,
            Int32 => 0,
            Int64 => 0,
            UInt8 => 0,
            UInt16 => 0,
            UInt32 => 0,
            UInt64 => 0,
            Float16 => 0,
            Float32 => 0,
            Float64 => 0,
            Timestamp(t, s) => t.heap_size() + s.heap_size(),
            Date32 => 0,
            Date64 => 0,
            Time32(t) => t.heap_size(),
            Time64(t) => t.heap_size(),
            Duration(t) => t.heap_size(),
            Interval(i) => i.heap_size(),
            Binary => 0,
            FixedSizeBinary(i) => i.heap_size(),
            LargeBinary => 0,
            BinaryView => 0,
            Utf8 => 0,
            LargeUtf8 => 0,
            Utf8View => 0,
            List(v) => v.heap_size(),
            ListView(v) => v.heap_size(),
            FixedSizeList(f, i) => f.heap_size() + i.heap_size(),
            LargeList(l) => l.heap_size(),
            LargeListView(l) => l.heap_size(),
            Struct(s) => s.heap_size(),
            Union(u, m) => u.heap_size() + m.heap_size(),
            Dictionary(a, b) => a.heap_size() + b.heap_size(),
            Decimal32(u8, i8) => u8.heap_size() + i8.heap_size(),
            Decimal64(u8, i8) => u8.heap_size() + i8.heap_size(),
            Decimal128(u8, i8) => u8.heap_size() + i8.heap_size(),
            Decimal256(u8, i8) => u8.heap_size() + i8.heap_size(),
            Map(m, b) => m.heap_size() + b.heap_size(),
            RunEndEncoded(a, b) => a.heap_size() + b.heap_size(),
        }
    }
}

impl<T: DFHeapSize> DFHeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        let item_size = size_of::<T>();
        // account for the contents of the Vec
        (self.capacity() * item_size) +
            // add any heap allocations by contents
            self.iter().map(|t| t.heap_size()).sum::<usize>()
    }
}

impl<K: DFHeapSize, V: DFHeapSize> DFHeapSize for HashMap<K, V> {
    fn heap_size(&self) -> usize {
        let capacity = self.capacity();
        if capacity == 0 {
            return 0;
        }

        // HashMap doesn't provide a way to get its heap size, so this is an approximation based on
        // the behavior of hashbrown::HashMap as at version 0.16.0, and may become inaccurate
        // if the implementation changes.
        let key_val_size = size_of::<(K, V)>();
        // Overhead for the control tags group, which may be smaller depending on architecture
        let group_size = 16;
        // 1 byte of metadata stored per bucket.
        let metadata_size = 1;

        // Compute the number of buckets for the capacity. Based on hashbrown's capacity_to_buckets
        let buckets = if capacity < 15 {
            let min_cap = match key_val_size {
                0..=1 => 14,
                2..=3 => 7,
                _ => 3,
            };
            let cap = min_cap.max(capacity);
            if cap < 4 {
                4
            } else if cap < 8 {
                8
            } else {
                16
            }
        } else {
            (capacity.saturating_mul(8) / 7).next_power_of_two()
        };

        group_size
            + (buckets * (key_val_size + metadata_size))
            + self.keys().map(|k| k.heap_size()).sum::<usize>()
            + self.values().map(|v| v.heap_size()).sum::<usize>()
    }
}

impl<T: DFHeapSize> DFHeapSize for Arc<T> {
    fn heap_size(&self) -> usize {
        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * size_of::<usize>() + size_of::<T>() + self.as_ref().heap_size()
    }
}

impl DFHeapSize for Arc<dyn DFHeapSize> {
    fn heap_size(&self) -> usize {
        2 * size_of::<usize>() + size_of_val(self.as_ref()) + self.as_ref().heap_size()
    }
}

impl DFHeapSize for Fields {
    fn heap_size(&self) -> usize {
        self.into_iter().map(|f| f.heap_size()).sum::<usize>()
    }
}

impl DFHeapSize for StructArray {
    fn heap_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

impl DFHeapSize for LargeListArray {
    fn heap_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

impl DFHeapSize for ListArray {
    fn heap_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

impl DFHeapSize for FixedSizeListArray {
    fn heap_size(&self) -> usize {
        self.get_array_memory_size()
    }
}
impl DFHeapSize for MapArray {
    fn heap_size(&self) -> usize {
        self.get_array_memory_size()
    }
}

impl DFHeapSize for Arc<str> {
    fn heap_size(&self) -> usize {
        2 * size_of::<usize>() + self.as_ref().heap_size()
    }
}

impl<T: DFHeapSize> DFHeapSize for Box<T> {
    fn heap_size(&self) -> usize {
        size_of::<T>() + self.as_ref().heap_size()
    }
}

impl<T: DFHeapSize> DFHeapSize for Option<T> {
    fn heap_size(&self) -> usize {
        self.as_ref().map(|inner| inner.heap_size()).unwrap_or(0)
    }
}

impl<A, B> DFHeapSize for (A, B)
where
    A: DFHeapSize,
    B: DFHeapSize,
{
    fn heap_size(&self) -> usize {
        self.0.heap_size() + self.1.heap_size()
    }
}

impl DFHeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

impl DFHeapSize for str {
    fn heap_size(&self) -> usize {
        self.len()
    }
}

impl DFHeapSize for UnionFields {
    fn heap_size(&self) -> usize {
        self.iter().map(|f| f.0.heap_size() + f.1.heap_size()).sum()
    }
}

impl DFHeapSize for UnionMode {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for TimeUnit {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for IntervalUnit {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for Field {
    fn heap_size(&self) -> usize {
        self.name().heap_size()
            + self.data_type().heap_size()
            + self.is_nullable().heap_size()
            + self.dict_is_ordered().heap_size()
            + self.metadata().heap_size()
    }
}

impl DFHeapSize for IntervalMonthDayNano {
    fn heap_size(&self) -> usize {
        self.days.heap_size() + self.months.heap_size() + self.nanoseconds.heap_size()
    }
}

impl DFHeapSize for IntervalDayTime {
    fn heap_size(&self) -> usize {
        self.days.heap_size() + self.milliseconds.heap_size()
    }
}

impl DFHeapSize for DateTime<Utc> {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for bool {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl DFHeapSize for u8 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for u16 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for u32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for u64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for i8 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for i16 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for i32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl DFHeapSize for i64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for i128 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for i256 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for f16 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for f32 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
impl DFHeapSize for f64 {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

impl DFHeapSize for usize {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}
