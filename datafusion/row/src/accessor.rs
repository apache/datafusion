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

//! [`RowAccessor`] provides a Read/Write/Modify access for row with all fixed-sized fields:

use crate::layout::RowLayout;
use crate::validity::NullBitsFormatter;
use crate::{fn_get_idx, fn_get_idx_opt, fn_set_idx};
use arrow::array::{Array, ArrowPrimitiveType, BooleanArray, PrimitiveArray};
use arrow::datatypes::{DataType, Schema};
use arrow::util::bit_util::{get_bit_raw, set_bit_raw};
use datafusion_common::ScalarValue;
use std::fmt::Debug;
use std::ops::{BitAnd, BitOr, BitXor};
use std::sync::Arc;

//TODO: DRY with reader and writer

/// Provides read/write/modify access to a tuple stored in Row format
/// at `data[base_offset..]`
///
/// ```text
/// Set / Update data
///     in [u8]
///      ─ ─ ─ ─ ─ ─ ─ ┐         Read data out as native
///     │                         types or ScalarValues
///                    │
///     │  ┌───────────────────────┐
///        │                       │
///     └ ▶│         [u8]          │─ ─ ─ ─ ─ ─ ─ ─▶
///        │                       │
///        └───────────────────────┘
/// ```
pub struct RowAccessor<'a> {
    /// Layout on how to read each field
    layout: Arc<RowLayout>,
    /// Raw bytes slice where the tuple stores
    data: &'a mut [u8],
    /// Start position for the current tuple in the raw bytes slice.
    base_offset: usize,
}

impl<'a> std::fmt::Debug for RowAccessor<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.null_free() {
            write!(f, "null_free")
        } else {
            let null_bits = self.null_bits();
            write!(
                f,
                "{:?}",
                NullBitsFormatter::new(null_bits, self.layout.field_count)
            )
        }
    }
}

#[macro_export]
macro_rules! fn_add_idx {
    ($NATIVE: ident) => {
        paste::item! {
            /// add field at `idx` with `value`
            #[inline(always)]
            pub fn [<add_ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                if self.is_valid_at(idx) {
                    self.[<set_ $NATIVE>](idx, value + self.[<get_ $NATIVE>](idx));
                } else {
                    self.set_non_null_at(idx);
                    self.[<set_ $NATIVE>](idx, value);
                }
            }
        }
    };
}

macro_rules! fn_max_min_idx {
    ($NATIVE: ident, $OP: ident) => {
        paste::item! {
            /// check max then update
            #[inline(always)]
            pub fn [<$OP _ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                if self.is_valid_at(idx) {
                    let v = value.$OP(self.[<get_ $NATIVE>](idx));
                    self.[<set_ $NATIVE>](idx, v);
                } else {
                    self.set_non_null_at(idx);
                    self.[<set_ $NATIVE>](idx, value);
                }
            }
        }
    };
}

macro_rules! fn_bit_and_or_xor_idx {
    ($NATIVE: ident, $OP: ident) => {
        paste::item! {
            /// check bit_and then update
            #[inline(always)]
            pub fn [<$OP _ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                if self.is_valid_at(idx) {
                    let v = value.$OP(self.[<get_ $NATIVE>](idx));
                    self.[<set_ $NATIVE>](idx, v);
                } else {
                    self.set_non_null_at(idx);
                    self.[<set_ $NATIVE>](idx, value);
                }
            }
        }
    };
}

macro_rules! fn_get_idx_scalar {
    ($NATIVE: ident, $SCALAR:ident) => {
        paste::item! {
            #[inline(always)]
            pub fn [<get_ $NATIVE _scalar>](&self, idx: usize) -> ScalarValue {
                if self.is_valid_at(idx) {
                    ScalarValue::$SCALAR(Some(self.[<get_ $NATIVE>](idx)))
                } else {
                    ScalarValue::$SCALAR(None)
                }
            }
        }
    };
}

impl<'a> RowAccessor<'a> {
    /// new
    pub fn new(schema: &Schema) -> Self {
        Self {
            layout: Arc::new(RowLayout::new(schema)),
            data: &mut [],
            base_offset: 0,
        }
    }

    pub fn new_from_layout(layout: Arc<RowLayout>) -> Self {
        Self {
            layout,
            data: &mut [],
            base_offset: 0,
        }
    }

    /// Update this row to point to position `offset` in `base`
    pub fn point_to(&mut self, offset: usize, data: &'a mut [u8]) {
        self.base_offset = offset;
        self.data = data;
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.layout.field_count);
    }

    #[inline(always)]
    fn field_offsets(&self) -> &[usize] {
        &self.layout.field_offsets
    }

    #[inline(always)]
    fn null_free(&self) -> bool {
        self.layout.null_free
    }

    #[inline(always)]
    fn null_bits(&self) -> &[u8] {
        if self.null_free() {
            &[]
        } else {
            let start = self.base_offset;
            &self.data[start..start + self.layout.null_width]
        }
    }

    fn is_valid_at(&self, idx: usize) -> bool {
        unsafe { get_bit_raw(self.null_bits().as_ptr(), idx) }
    }

    // ------------------------------
    // ----- Fixed Sized getters ----
    // ------------------------------

    fn get_bool(&self, idx: usize) -> bool {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        let value = &self.data[self.base_offset + offset..];
        value[0] != 0
    }

    fn get_u8(&self, idx: usize) -> u8 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        self.data[self.base_offset + offset]
    }

    fn_get_idx!(u16, 2);
    fn_get_idx!(u32, 4);
    fn_get_idx!(u64, 8);
    fn_get_idx!(i8, 1);
    fn_get_idx!(i16, 2);
    fn_get_idx!(i32, 4);
    fn_get_idx!(i64, 8);
    fn_get_idx!(f32, 4);
    fn_get_idx!(f64, 8);
    fn_get_idx!(i128, 16);

    fn_get_idx_opt!(bool);
    fn_get_idx_opt!(u8);
    fn_get_idx_opt!(u16);
    fn_get_idx_opt!(u32);
    fn_get_idx_opt!(u64);
    fn_get_idx_opt!(i8);
    fn_get_idx_opt!(i16);
    fn_get_idx_opt!(i32);
    fn_get_idx_opt!(i64);
    fn_get_idx_opt!(f32);
    fn_get_idx_opt!(f64);
    fn_get_idx_opt!(i128);

    fn_get_idx_scalar!(bool, Boolean);
    fn_get_idx_scalar!(u8, UInt8);
    fn_get_idx_scalar!(u16, UInt16);
    fn_get_idx_scalar!(u32, UInt32);
    fn_get_idx_scalar!(u64, UInt64);
    fn_get_idx_scalar!(i8, Int8);
    fn_get_idx_scalar!(i16, Int16);
    fn_get_idx_scalar!(i32, Int32);
    fn_get_idx_scalar!(i64, Int64);
    fn_get_idx_scalar!(f32, Float32);
    fn_get_idx_scalar!(f64, Float64);

    fn get_decimal128_scalar(&self, idx: usize, p: u8, s: i8) -> ScalarValue {
        if self.is_valid_at(idx) {
            ScalarValue::Decimal128(Some(self.get_i128(idx)), p, s)
        } else {
            ScalarValue::Decimal128(None, p, s)
        }
    }

    pub fn get_as_scalar(&self, dt: &DataType, index: usize) -> ScalarValue {
        match dt {
            DataType::Boolean => self.get_bool_scalar(index),
            DataType::Int8 => self.get_i8_scalar(index),
            DataType::Int16 => self.get_i16_scalar(index),
            DataType::Int32 => self.get_i32_scalar(index),
            DataType::Int64 => self.get_i64_scalar(index),
            DataType::UInt8 => self.get_u8_scalar(index),
            DataType::UInt16 => self.get_u16_scalar(index),
            DataType::UInt32 => self.get_u32_scalar(index),
            DataType::UInt64 => self.get_u64_scalar(index),
            DataType::Float32 => self.get_f32_scalar(index),
            DataType::Float64 => self.get_f64_scalar(index),
            DataType::Decimal128(p, s) => self.get_decimal128_scalar(index, *p, *s),
            _ => unreachable!(),
        }
    }

    // ------------------------------
    // ----- Fixed Sized setters ----
    // ------------------------------

    pub(crate) fn set_non_null_at(&mut self, idx: usize) {
        assert!(
            !self.null_free(),
            "Unexpected call to set_non_null_at on null-free row writer"
        );
        let null_bits = &mut self.data[0..self.layout.null_width];
        unsafe {
            set_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

    fn set_bool(&mut self, idx: usize, value: bool) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        self.data[offset] = u8::from(value);
    }

    fn set_u8(&mut self, idx: usize, value: u8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        self.data[offset] = value;
    }

    fn_set_idx!(u16, 2);
    fn_set_idx!(u32, 4);
    fn_set_idx!(u64, 8);
    fn_set_idx!(i16, 2);
    fn_set_idx!(i32, 4);
    fn_set_idx!(i64, 8);
    fn_set_idx!(f32, 4);
    fn_set_idx!(f64, 8);
    fn_set_idx!(i128, 16);

    fn set_i8(&mut self, idx: usize, value: i8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        self.data[offset] = value.to_le_bytes()[0];
    }

    // ------------------------------
    // ---- Fixed sized updaters ----
    // ------------------------------

    fn_add_idx!(u8);
    fn_add_idx!(u16);
    fn_add_idx!(u32);
    fn_add_idx!(u64);
    fn_add_idx!(i8);
    fn_add_idx!(i16);
    fn_add_idx!(i32);
    fn_add_idx!(i64);
    fn_add_idx!(f32);
    fn_add_idx!(f64);
    fn_add_idx!(i128);

    fn_max_min_idx!(bool, max);
    fn_max_min_idx!(u8, max);
    fn_max_min_idx!(u16, max);
    fn_max_min_idx!(u32, max);
    fn_max_min_idx!(u64, max);
    fn_max_min_idx!(i8, max);
    fn_max_min_idx!(i16, max);
    fn_max_min_idx!(i32, max);
    fn_max_min_idx!(i64, max);
    fn_max_min_idx!(f32, max);
    fn_max_min_idx!(f64, max);
    fn_max_min_idx!(i128, max);

    fn_max_min_idx!(bool, min);
    fn_max_min_idx!(u8, min);
    fn_max_min_idx!(u16, min);
    fn_max_min_idx!(u32, min);
    fn_max_min_idx!(u64, min);
    fn_max_min_idx!(i8, min);
    fn_max_min_idx!(i16, min);
    fn_max_min_idx!(i32, min);
    fn_max_min_idx!(i64, min);
    fn_max_min_idx!(f32, min);
    fn_max_min_idx!(f64, min);
    fn_max_min_idx!(i128, min);

    fn_bit_and_or_xor_idx!(bool, bitand);
    fn_bit_and_or_xor_idx!(u8, bitand);
    fn_bit_and_or_xor_idx!(u16, bitand);
    fn_bit_and_or_xor_idx!(u32, bitand);
    fn_bit_and_or_xor_idx!(u64, bitand);
    fn_bit_and_or_xor_idx!(i8, bitand);
    fn_bit_and_or_xor_idx!(i16, bitand);
    fn_bit_and_or_xor_idx!(i32, bitand);
    fn_bit_and_or_xor_idx!(i64, bitand);

    fn_bit_and_or_xor_idx!(bool, bitor);
    fn_bit_and_or_xor_idx!(u8, bitor);
    fn_bit_and_or_xor_idx!(u16, bitor);
    fn_bit_and_or_xor_idx!(u32, bitor);
    fn_bit_and_or_xor_idx!(u64, bitor);
    fn_bit_and_or_xor_idx!(i8, bitor);
    fn_bit_and_or_xor_idx!(i16, bitor);
    fn_bit_and_or_xor_idx!(i32, bitor);
    fn_bit_and_or_xor_idx!(i64, bitor);

    fn_bit_and_or_xor_idx!(u8, bitxor);
    fn_bit_and_or_xor_idx!(u16, bitxor);
    fn_bit_and_or_xor_idx!(u32, bitxor);
    fn_bit_and_or_xor_idx!(u64, bitxor);
    fn_bit_and_or_xor_idx!(i8, bitxor);
    fn_bit_and_or_xor_idx!(i16, bitxor);
    fn_bit_and_or_xor_idx!(i32, bitxor);
    fn_bit_and_or_xor_idx!(i64, bitxor);
}

pub trait RowAccumulatorNativeType: Debug + Send + Sync {
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor);

    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor);

    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor);

    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor);

    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor);

    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor);
}

impl RowAccumulatorNativeType for bool {
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        //TODO add test
        row.set_bool(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_bool(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_bool(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_bool(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_bool(row_idx, self)
    }

    fn bit_xor_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }
}

impl RowAccumulatorNativeType for u8 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_u8(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_u8(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_u8(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_u8(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_u8(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_u8(row_idx, self)
    }
}

impl RowAccumulatorNativeType for u16 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_u16(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_u16(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_u16(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_u16(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_u16(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_u16(row_idx, self)
    }
}

impl RowAccumulatorNativeType for u32 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_u32(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_u32(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_u32(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_u32(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_u32(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_u32(row_idx, self)
    }
}

impl RowAccumulatorNativeType for u64 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_u64(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_u64(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_u64(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_u64(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_u64(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_u64(row_idx, self)
    }
}

impl RowAccumulatorNativeType for i8 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_i8(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_i8(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_i8(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_i8(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_i8(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_i8(row_idx, self)
    }
}

impl RowAccumulatorNativeType for i16 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_i16(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_i16(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_i16(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_i16(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_i16(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_i16(row_idx, self)
    }
}
impl RowAccumulatorNativeType for i32 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_i32(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_i32(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_i32(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_i32(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_i32(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_i32(row_idx, self)
    }
}

impl RowAccumulatorNativeType for i64 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_i64(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_i64(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_i64(row_idx, self)
    }

    #[inline(always)]
    fn bit_and_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitand_i64(row_idx, self)
    }

    #[inline(always)]
    fn bit_or_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitor_i64(row_idx, self)
    }

    #[inline(always)]
    fn bit_xor_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.bitxor_i64(row_idx, self)
    }
}

impl RowAccumulatorNativeType for f32 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_f32(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_f32(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_f32(row_idx, self)
    }

    fn bit_and_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_or_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_xor_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }
}

impl RowAccumulatorNativeType for f64 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_f64(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_f64(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_f64(row_idx, self)
    }

    fn bit_and_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_or_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_xor_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }
}

impl RowAccumulatorNativeType for i128 {
    #[inline(always)]
    fn add_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.add_i128(row_idx, self)
    }

    #[inline(always)]
    fn min_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.min_i128(row_idx, self)
    }

    #[inline(always)]
    fn max_to_row(self, row_idx: usize, row: &mut RowAccessor) {
        row.max_i128(row_idx, self)
    }

    fn bit_and_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_or_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }

    fn bit_xor_to_row(self, _row_idx: usize, _row: &mut RowAccessor) {
        unimplemented!()
    }
}

pub trait ArrowArrayReader: Array {
    type Item: RowAccumulatorNativeType;

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if the value is outside the bounds of the array
    fn value_at(&self, index: usize) -> Self::Item;

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_at_unchecked(&self, index: usize) -> Self::Item;
}

impl<'a> ArrowArrayReader for &'a BooleanArray {
    type Item = bool;

    #[inline]
    fn value_at(&self, index: usize) -> Self::Item {
        BooleanArray::value(self, index)
    }

    #[inline]
    unsafe fn value_at_unchecked(&self, index: usize) -> Self::Item {
        BooleanArray::value_unchecked(self, index)
    }
}

impl<'a, T: ArrowPrimitiveType> ArrowArrayReader for &'a PrimitiveArray<T>
where
    <T as ArrowPrimitiveType>::Native: RowAccumulatorNativeType,
{
    type Item = T::Native;

    #[inline]
    fn value_at(&self, index: usize) -> Self::Item {
        PrimitiveArray::value(self, index)
    }

    #[inline]
    unsafe fn value_at_unchecked(&self, index: usize) -> Self::Item {
        PrimitiveArray::value_unchecked(self, index)
    }
}
