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

//! Estimating the heap-allocated memory owned by a value.
//!
//! The [`DFHeapSize`] trait reports the number of bytes a value owns on the
//! heap, **excluding** the stack size of the value itself.
//!
//! Implementations need to use [`DFHeapSizeCtx`] that is pushed through every
//! nested call. The context records which allocations have already been measured
//! so they are only counted once.
//!
//! # Example
//!
//! ```
//! use datafusion_common::heap_size::{DFHeapSize, DFHeapSizeCtx};
//! use std::sync::Arc;
//!
//! let shared: Arc<String> = Arc::new("hello".to_string());
//! let alias = Arc::clone(&shared);
//!
//! let mut ctx = DFHeapSizeCtx::default();
//! // The shared allocation is counted once even when reached twice.
//! let total = shared.heap_size(&mut ctx) + alias.heap_size(&mut ctx);
//! assert_eq!(total, shared.heap_size(&mut DFHeapSizeCtx::default()));
//! ```

use crate::stats::Precision;
use crate::{ColumnStatistics, ScalarValue, Statistics, TableReference};
use arrow::array::{
    Array, FixedSizeListArray, LargeListArray, LargeListViewArray, ListArray,
    ListViewArray, MapArray, StructArray,
};
use arrow::datatypes::{
    DataType, Field, Fields, IntervalDayTime, IntervalMonthDayNano, IntervalUnit,
    TimeUnit, UnionFields, UnionMode, i256,
};
use chrono::{DateTime, Utc};
use half::f16;
use hashbrown::HashSet;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// Trait for computing how many bytes a value has allocated on the heap.
///
/// Implementations need to use [`DFHeapSizeCtx`] that is pushed through every
/// nested call. The context records which allocations have already been measured
/// so they are only counted once.
///
pub trait DFHeapSize {
    /// Return the number of bytes this value has allocated on the heap,
    /// including heap memory owned transitively by nested values.
    ///
    /// Note that the size of the type itself is not included in the result --
    /// instead, that size is added by the caller (e.g. container).
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize;
}

#[derive(Default)]
pub struct DFHeapSizeCtx {
    seen: HashSet<usize>,
}

impl DFHeapSize for Statistics {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.num_rows.heap_size(ctx)
            + self.total_byte_size.heap_size(ctx)
            + self.column_statistics.heap_size(ctx)
    }
}

impl DFHeapSize for TableReference {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        match self {
            TableReference::Bare { table } => table.heap_size(ctx),
            TableReference::Partial { schema, table } => {
                schema.heap_size(ctx) + table.heap_size(ctx)
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => catalog.heap_size(ctx) + schema.heap_size(ctx) + table.heap_size(ctx),
        }
    }
}

impl<T: Debug + Clone + PartialEq + Eq + PartialOrd + DFHeapSize> DFHeapSize
    for Precision<T>
{
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.get_value().map_or_else(|| 0, |v| v.heap_size(ctx))
    }
}

impl DFHeapSize for ColumnStatistics {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.null_count.heap_size(ctx)
            + self.max_value.heap_size(ctx)
            + self.min_value.heap_size(ctx)
            + self.sum_value.heap_size(ctx)
            + self.distinct_count.heap_size(ctx)
            + self.byte_size.heap_size(ctx)
    }
}

impl DFHeapSize for ScalarValue {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        use crate::scalar::ScalarValue::*;
        match self {
            Null => 0,
            Boolean(b) => b.heap_size(ctx),
            Float16(f) => f.heap_size(ctx),
            Float32(f) => f.heap_size(ctx),
            Float64(f) => f.heap_size(ctx),
            Decimal32(a, b, c) => a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx),
            Decimal64(a, b, c) => a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx),
            Decimal128(a, b, c) => a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx),
            Decimal256(a, b, c) => a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx),
            Int8(i) => i.heap_size(ctx),
            Int16(i) => i.heap_size(ctx),
            Int32(i) => i.heap_size(ctx),
            Int64(i) => i.heap_size(ctx),
            UInt8(u) => u.heap_size(ctx),
            UInt16(u) => u.heap_size(ctx),
            UInt32(u) => u.heap_size(ctx),
            UInt64(u) => u.heap_size(ctx),
            Utf8(u) => u.heap_size(ctx),
            Utf8View(u) => u.heap_size(ctx),
            LargeUtf8(l) => l.heap_size(ctx),
            Binary(b) => b.heap_size(ctx),
            BinaryView(b) => b.heap_size(ctx),
            FixedSizeBinary(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            LargeBinary(l) => l.heap_size(ctx),
            FixedSizeList(f) => f.heap_size(ctx),
            List(l) => l.heap_size(ctx),
            LargeList(l) => l.heap_size(ctx),
            Struct(s) => s.heap_size(ctx),
            Map(m) => m.heap_size(ctx),
            Date32(d) => d.heap_size(ctx),
            Date64(d) => d.heap_size(ctx),
            Time32Second(t) => t.heap_size(ctx),
            Time32Millisecond(t) => t.heap_size(ctx),
            Time64Microsecond(t) => t.heap_size(ctx),
            Time64Nanosecond(t) => t.heap_size(ctx),
            TimestampSecond(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            TimestampMillisecond(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            TimestampMicrosecond(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            TimestampNanosecond(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            IntervalYearMonth(i) => i.heap_size(ctx),
            IntervalDayTime(i) => i.heap_size(ctx),
            IntervalMonthDayNano(i) => i.heap_size(ctx),
            DurationSecond(d) => d.heap_size(ctx),
            DurationMillisecond(d) => d.heap_size(ctx),
            DurationMicrosecond(d) => d.heap_size(ctx),
            DurationNanosecond(d) => d.heap_size(ctx),
            Union(a, b, c) => a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx),
            Dictionary(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            RunEndEncoded(a, b, c) => {
                a.heap_size(ctx) + b.heap_size(ctx) + c.heap_size(ctx)
            }
            ListView(a) => a.heap_size(ctx),
            LargeListView(a) => a.heap_size(ctx),
        }
    }
}

impl DFHeapSize for DataType {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
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
            Timestamp(t, s) => t.heap_size(ctx) + s.heap_size(ctx),
            Date32 => 0,
            Date64 => 0,
            Time32(t) => t.heap_size(ctx),
            Time64(t) => t.heap_size(ctx),
            Duration(t) => t.heap_size(ctx),
            Interval(i) => i.heap_size(ctx),
            Binary => 0,
            FixedSizeBinary(i) => i.heap_size(ctx),
            LargeBinary => 0,
            BinaryView => 0,
            Utf8 => 0,
            LargeUtf8 => 0,
            Utf8View => 0,
            List(v) => v.heap_size(ctx),
            ListView(v) => v.heap_size(ctx),
            FixedSizeList(f, i) => f.heap_size(ctx) + i.heap_size(ctx),
            LargeList(l) => l.heap_size(ctx),
            LargeListView(l) => l.heap_size(ctx),
            Struct(s) => s.heap_size(ctx),
            Union(u, m) => u.heap_size(ctx) + m.heap_size(ctx),
            Dictionary(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
            Decimal32(p, s) => p.heap_size(ctx) + s.heap_size(ctx),
            Decimal64(p, s) => p.heap_size(ctx) + s.heap_size(ctx),
            Decimal128(p, s) => p.heap_size(ctx) + s.heap_size(ctx),
            Decimal256(p, s) => p.heap_size(ctx) + s.heap_size(ctx),
            Map(m, b) => m.heap_size(ctx) + b.heap_size(ctx),
            RunEndEncoded(a, b) => a.heap_size(ctx) + b.heap_size(ctx),
        }
    }
}

impl<T: DFHeapSize> DFHeapSize for Vec<T> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        let item_size = size_of::<T>();
        // account for the contents of the Vec
        (self.capacity() * item_size) +
            // add any heap allocations by contents
            self.iter().map(|t| t.heap_size(ctx)).sum::<usize>()
    }
}

impl<K: DFHeapSize, V: DFHeapSize> DFHeapSize for HashMap<K, V> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
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
            + self.keys().map(|k| k.heap_size(ctx)).sum::<usize>()
            + self.values().map(|v| v.heap_size(ctx)).sum::<usize>()
    }
}

impl<T: DFHeapSize> DFHeapSize for Arc<T> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        let ptr = Arc::as_ptr(self) as usize;

        if !ctx.seen.insert(ptr) {
            return 0;
        }

        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * size_of::<usize>() + size_of::<T>() + self.as_ref().heap_size(ctx)
    }
}

impl DFHeapSize for Arc<str> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        let ptr = Arc::as_ptr(self) as *const i32 as usize;

        if !ctx.seen.insert(ptr) {
            return 0;
        }

        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * size_of::<usize>() + self.as_ref().heap_size(ctx)
    }
}

impl DFHeapSize for Arc<dyn DFHeapSize> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        let ptr = Arc::as_ptr(self) as *const i32 as usize;

        if !ctx.seen.insert(ptr) {
            return 0;
        }

        // Arc stores weak and strong counts on the heap alongside an instance of T
        2 * size_of::<usize>() + size_of_val(self.as_ref()) + self.as_ref().heap_size(ctx)
    }
}

impl DFHeapSize for Fields {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.into_iter().map(|f| f.heap_size(ctx)).sum::<usize>()
    }
}

impl<T: DFHeapSize> DFHeapSize for Box<T> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        size_of::<T>() + self.as_ref().heap_size(ctx)
    }
}

impl<T: DFHeapSize> DFHeapSize for Option<T> {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.as_ref().map(|inner| inner.heap_size(ctx)).unwrap_or(0)
    }
}

impl<A, B> DFHeapSize for (A, B)
where
    A: DFHeapSize,
    B: DFHeapSize,
{
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.0.heap_size(ctx) + self.1.heap_size(ctx)
    }
}

impl DFHeapSize for String {
    fn heap_size(&self, _: &mut DFHeapSizeCtx) -> usize {
        self.capacity()
    }
}

impl DFHeapSize for str {
    fn heap_size(&self, _: &mut DFHeapSizeCtx) -> usize {
        // Internal accounting helper for owners like Arc<str>
        self.len()
    }
}

impl DFHeapSize for UnionFields {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.iter()
            .map(|f| f.0.heap_size(ctx) + f.1.heap_size(ctx))
            .sum()
    }
}

impl DFHeapSize for Field {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.name().heap_size(ctx)
            + self.data_type().heap_size(ctx)
            + self.is_nullable().heap_size(ctx)
            + self.dict_is_ordered().heap_size(ctx)
            + self.metadata().heap_size(ctx)
    }
}

impl DFHeapSize for IntervalMonthDayNano {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.days.heap_size(ctx)
            + self.months.heap_size(ctx)
            + self.nanoseconds.heap_size(ctx)
    }
}

impl DFHeapSize for IntervalDayTime {
    fn heap_size(&self, ctx: &mut DFHeapSizeCtx) -> usize {
        self.days.heap_size(ctx) + self.milliseconds.heap_size(ctx)
    }
}

/// Implement [`DFHeapSize`] for types that own no heap allocations.
macro_rules! impl_zero_heap_size {
    ($($t:ty),+ $(,)?) => {
        $(
            impl DFHeapSize for $t {
                fn heap_size(&self, _: &mut DFHeapSizeCtx) -> usize {
                    0 // no heap allocations
                }
            }
        )+
    };
}

impl_zero_heap_size!(
    bool,
    u8,
    u16,
    u32,
    u64,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    i256,
    f16,
    f32,
    f64,
    UnionMode,
    TimeUnit,
    IntervalUnit,
    DateTime<Utc>,
);

/// Implement [`DFHeapSize`] for Arrow arrays types.
macro_rules! impl_array_heap_size {
    ($($t:ty),+ $(,)?) => {
        $(
            impl DFHeapSize for $t {
                fn heap_size(&self, _: &mut DFHeapSizeCtx) -> usize {
                    self.get_array_memory_size()
                }
            }
        )+
    };
}

impl_array_heap_size!(
    StructArray,
    LargeListArray,
    LargeListViewArray,
    ListArray,
    ListViewArray,
    FixedSizeListArray,
    MapArray,
);

#[cfg(test)]
mod tests {
    use super::*;

    fn size<T: DFHeapSize + ?Sized>(v: &T) -> usize {
        v.heap_size(&mut DFHeapSizeCtx::default())
    }

    #[test]
    fn test_heap_size_arc_avoid_double_accounting() {
        let a1 = Arc::new(vec![1, 2, 3]);
        let mut ctx = DFHeapSizeCtx::default();
        let heap_size = a1.heap_size(&mut ctx);

        let a2 = Arc::clone(&a1);
        let a3 = Arc::clone(&a1);
        let a4 = Arc::clone(&a3);

        let mut ctx = DFHeapSizeCtx::default();
        let heap_size_with_clones = a1.heap_size(&mut ctx)
            + a2.heap_size(&mut ctx)
            + a3.heap_size(&mut ctx)
            + a4.heap_size(&mut ctx);

        assert_eq!(heap_size, heap_size_with_clones);
    }

    #[test]
    fn test_heap_size_arc_str_avoid_double_accounting() {
        let a1: Arc<str> = Arc::from("Hello");
        let mut ctx = DFHeapSizeCtx::default();
        let heap_size = a1.heap_size(&mut ctx);

        let a2 = Arc::clone(&a1);
        let a3 = Arc::clone(&a1);
        let a4 = Arc::clone(&a3);

        let mut ctx = DFHeapSizeCtx::default();
        let heap_size_with_clones = a1.heap_size(&mut ctx)
            + a2.heap_size(&mut ctx)
            + a3.heap_size(&mut ctx)
            + a4.heap_size(&mut ctx);

        assert_eq!(heap_size, heap_size_with_clones);
    }

    #[test]
    fn test_arc_dyn() {
        let a1: Arc<dyn DFHeapSize> = Arc::new(String::from("hello"));
        let baseline = size(&a1);

        let a2 = Arc::clone(&a1);
        let mut ctx = DFHeapSizeCtx::default();
        let with_clones = a1.heap_size(&mut ctx) + a2.heap_size(&mut ctx);
        assert_eq!(baseline, with_clones);
    }

    #[test]
    fn test_primitives() {
        assert_eq!(size(&true), 0);
        assert_eq!(size(&0u8), 0);
        assert_eq!(size(&0u16), 0);
        assert_eq!(size(&0u32), 0);
        assert_eq!(size(&0u64), 0);
        assert_eq!(size(&0usize), 0);
        assert_eq!(size(&0i8), 0);
        assert_eq!(size(&0i16), 0);
        assert_eq!(size(&0i32), 0);
        assert_eq!(size(&0i64), 0);
        assert_eq!(size(&0i128), 0);
        assert_eq!(size(&i256::ZERO), 0);
        assert_eq!(size(&0f32), 0);
        assert_eq!(size(&0f64), 0);
        assert_eq!(size(&f16::from_f32(0.0)), 0);
    }

    #[test]
    fn test_heap_size_union_mode() {
        assert_eq!(size(&UnionMode::Sparse), 0);
        assert_eq!(size(&UnionMode::Dense), 0);
    }

    #[test]
    fn test_heap_size_time_units() {
        assert_eq!(size(&TimeUnit::Second), 0);
        assert_eq!(size(&IntervalUnit::YearMonth), 0);
        assert_eq!(size(&DateTime::<Utc>::UNIX_EPOCH), 0);
        assert_eq!(size(&Utc::now()), 0);
    }

    #[test]
    fn test_string() {
        let mut s = String::with_capacity(32);
        s.push_str("hello");
        assert_eq!(size(&s), 32);

        let empty = String::new();
        assert_eq!(size(&empty), 0);
    }

    #[test]
    fn test_owned_str() {
        let a: Arc<str> = Arc::from("Hello");
        assert!(size(&a) > 0);
    }

    #[test]
    fn test_option() {
        let some: Option<String> = Some(String::from("hi"));
        assert_eq!(size(&some), some.as_ref().unwrap().capacity());

        let none: Option<String> = None;
        assert_eq!(size(&none), 0);
    }

    #[test]
    fn test_vec() {
        let v: Vec<i32> = vec![1, 2, 3];
        assert!(size(&v) > 0);

        let strings = vec![String::from("ab"), String::from("cdef")];
        assert!(size(&strings) > 0);

        let empty: Vec<i32> = Vec::new();
        assert_eq!(size(&empty), 0);
    }

    #[test]
    fn test_box() {
        let b: Box<i32> = Box::new(42);
        assert!(size(&b) > 0);

        let b: Box<String> = Box::new(String::from("hello"));
        assert!(size(&b) > 0);
    }

    #[test]
    fn test_tuple() {
        let zero = (1i32, 2i64);
        assert_eq!(size(&zero), 0);

        let t = (String::from("hello"), String::from("world"));
        assert!(size(&t) > 0);
    }

    #[test]
    fn test_hashmap() {
        let m: HashMap<i32, i32> = HashMap::new();
        assert_eq!(size(&m), 0);

        let mut m: HashMap<String, String> = HashMap::new();
        m.insert("key".into(), "value".into());

        assert!(size(&m) > 0);
    }

    #[test]
    fn test_precision() {
        let exact: Precision<usize> = Precision::Exact(42);
        assert_eq!(size(&exact), 0);

        let inexact: Precision<usize> = Precision::Inexact(99);
        assert_eq!(size(&inexact), 0);

        let absent: Precision<usize> = Precision::Absent;
        assert_eq!(size(&absent), 0);
    }

    #[test]
    fn test_scalar_values() {
        assert_eq!(size(&ScalarValue::Null), 0);
        assert_eq!(size(&ScalarValue::Int32(Some(42))), 0);
        assert_eq!(size(&ScalarValue::Boolean(Some(true))), 0);
        assert_eq!(size(&ScalarValue::Float64(None)), 0);

        let sv = ScalarValue::Utf8(Some(String::from("hello")));
        assert_eq!(size(&sv), "hello".len());

        let sv = ScalarValue::Utf8(None);
        assert_eq!(size(&sv), 0);
    }

    #[test]
    fn test_data_type_primitives() {
        assert_eq!(size(&DataType::Int32), 0);
        assert_eq!(size(&DataType::Utf8), 0);
        assert_eq!(size(&DataType::Boolean), 0);
        assert_eq!(size(&DataType::Null), 0);
    }

    #[test]
    fn test_data_type_with_field() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert!(size(&list) > 0);
    }

    #[test]
    fn test_table_references() {
        let tr = TableReference::bare("users");
        // Arc<str> overhead (two usize counts) plus the bytes of "users".
        assert!(size(&tr) > 0);
        let tr = TableReference::full("cat", "schema", "users");
        assert!(size(&tr) > 0);
    }

    #[test]
    fn test_column_statistics() {
        let mut col = ColumnStatistics::new_unknown();
        col.max_value = Precision::Exact(ScalarValue::Utf8(Some("hello".into())));
        col.min_value = Precision::Exact(ScalarValue::Utf8(Some("ab".into())));
        assert_eq!(size(&col), "hello".len() + "ab".len());

        let mut col = ColumnStatistics::new_unknown();
        col.max_value = Precision::Exact(ScalarValue::Utf8(Some("hello".into())));
        let stats = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![col],
        };
        assert!(size(&stats) > 0);
    }

    #[test]
    fn test_field() {
        let field = Field::new("temperature", DataType::Float64, true);
        assert!(size(&field) > 0);
    }

    #[test]
    fn test_list_array() {
        use arrow::array::types::Int32Type;

        let array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4)]),
        ]);
        assert_eq!(size(&array), array.get_array_memory_size());
        assert!(size(&array) > 0);

        let large =
            LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                Some(1),
                Some(2),
            ])]);
        assert_eq!(size(&large), large.get_array_memory_size());
        assert!(size(&large) > 0);
    }

    #[test]
    fn test_struct_array() {
        use arrow::array::Int32Array;

        let array = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
        )]);
        assert_eq!(size(&array), array.get_array_memory_size());
        assert!(size(&array) > 0);
    }

    #[test]
    fn test_fixed_size_list_array() {
        use arrow::array::Int32Array;

        let values = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let array = FixedSizeListArray::new(field, 2, values, None);
        assert_eq!(size(&array), array.get_array_memory_size());
        assert!(size(&array) > 0);
    }

    #[test]
    fn test_map_array() {
        use arrow::array::{Int32Builder, MapBuilder, StringBuilder};

        let mut builder =
            MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("key");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        let array = builder.finish();
        assert_eq!(size(&array), array.get_array_memory_size());
        assert!(size(&array) > 0);
    }
}
