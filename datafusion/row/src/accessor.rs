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

//! Setter/Getter for row with all fixed-sized fields.

use crate::layout::{RowLayout, RowType};
use crate::validity::{all_valid, NullBitsFormatter};
use crate::{fn_get_idx, fn_get_idx_opt, fn_set_idx, get_idx, set_idx};
use arrow::datatypes::Schema;
use arrow::util::bit_util::{get_bit_raw, set_bit_raw, unset_bit_raw};

//TODO: DRY with reader and writer

/// Read the tuple `data[base_offset..]` we are currently pointing to
pub struct RowAccessor<'a> {
    /// Layout on how to read each field
    layout: RowLayout,
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

impl<'a> RowAccessor<'a> {
    /// new
    pub fn new(schema: &Schema, row_type: RowType) -> Self {
        Self {
            layout: RowLayout::new(schema, row_type),
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

    #[inline(always)]
    fn all_valid(&self) -> bool {
        if self.null_free() {
            true
        } else {
            let null_bits = self.null_bits();
            all_valid(null_bits, self.layout.field_count)
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

    fn get_date32(&self, idx: usize) -> i32 {
        get_idx!(i32, self, idx, 4)
    }

    fn get_date64(&self, idx: usize) -> i64 {
        get_idx!(i64, self, idx, 8)
    }

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

    fn get_date32_opt(&self, idx: usize) -> Option<i32> {
        if self.is_valid_at(idx) {
            Some(self.get_date32(idx))
        } else {
            None
        }
    }

    fn get_date64_opt(&self, idx: usize) -> Option<i64> {
        if self.is_valid_at(idx) {
            Some(self.get_date64(idx))
        } else {
            None
        }
    }

    // ------------------------------
    // ----- Fixed Sized setters ----
    // ------------------------------

    pub(crate) fn set_null_at(&mut self, idx: usize) {
        assert!(
            !self.null_free(),
            "Unexpected call to set_null_at on null-free row writer"
        );
        let null_bits = &mut self.data[0..self.layout.null_width];
        unsafe {
            unset_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

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
        self.data[offset] = if value { 1 } else { 0 };
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

    fn set_i8(&mut self, idx: usize, value: i8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets()[idx];
        self.data[offset] = value.to_le_bytes()[0];
    }

    fn set_date32(&mut self, idx: usize, value: i32) {
        set_idx!(4, self, idx, value)
    }

    fn set_date64(&mut self, idx: usize, value: i64) {
        set_idx!(8, self, idx, value)
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
}
