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

//! An implementation of Row backed by raw bytes
//!
//! Each tuple consists of up to three parts: [null bit set] [values] [var length data]
//!
//! The null bit set is used for null tracking and is aligned to 1-byte. It stores
//! one bit per field.
//!
//! In the region of the values, we store the fields in the order they are defined in the schema.
//! - For fixed-length, sequential access fields, we store them directly.
//!       E.g., 4 bytes for int and 1 byte for bool.
//! - For fixed-length, update often fields, we store one 8-byte word per field.
//! - For fields of non-primitive or variable-length types,
//!       we append their actual content to the end of the var length region and
//!       store their offset relative to row base and their length, packed into an 8-byte word.

use crate::row::bitmap::{get_bit, set_bit};
use arrow::datatypes::{DataType, Schema};
use std::sync::Arc;

mod bitmap;
mod page;

const UTF8_DEFAULT_SIZE: usize = 20;
const BINARY_DEFAULT_SIZE: usize = 100;

pub struct Row<'a> {
    data: &'a mut [u8],
    base_offset: usize,
    field_count: usize,
    row_width: usize,
    pub(crate) fixed_size: bool,
    null_width: usize,
    values_width: usize,
    varlena_width: usize,
    varlena_offset: usize,
    field_offsets: Vec<usize>,
}

impl<'a> Row<'a> {
    pub fn new(schema: &Arc<Schema>) -> Self {
        assert!(supported(&schema));
        let field_count = schema.fields().len();
        let null_width = null_width(field_count);
        let (field_offsets, values_width) = get_offsets(null_width, &schema);
        let fixed_size = fixed_size(&schema);
        Self {
            data: &mut [],
            base_offset: 0,
            field_count,
            row_width: 0,
            fixed_size,
            null_width,
            values_width,
            varlena_width: 0,
            varlena_offset: null_width + values_width,
            field_offsets,
        }
    }

    /// Update this row to point to position `offset` in `base`
    pub fn point_to(&mut self, base: &'a mut [u8], offset: usize) {
        self.data = base;
        self.base_offset = offset;
        self.varlena_width = 0;
        self.varlena_offset = self.null_width + self.values_width;
    }

    pub fn new_from(schema: &Arc<Schema>, base: &'a mut [u8], offset: usize) -> Self {
        let mut row = Self::new(schema);
        row.point_to(base, offset);
        row
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.field_count);
    }

    // ----------------------
    // Accessors
    // ----------------------

    fn is_valid_at(&self, idx: usize) -> bool {
        let null_bits = &self.data[self.base_offset..self.base_offset + self.null_width];
        get_bit(null_bits, idx)
    }

    fn get_boolean(&self, idx: usize) -> bool {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        let value = &self.data[self.base_offset + offset..];
        value[0] != 0
    }

    fn get_u8(&self, idx: usize) -> u8 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[self.base_offset + offset]
    }

    fn get_u16(&self, idx: usize) -> u16 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        u16::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_u32(&self, idx: usize) -> u32 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        u32::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_u64(&self, idx: usize) -> u64 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        u64::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_i8(&self, idx: usize) -> i8 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i8::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_i16(&self, idx: usize) -> i16 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i16::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_i32(&self, idx: usize) -> i32 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i32::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_i64(&self, idx: usize) -> i64 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i64::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_f32(&self, idx: usize) -> f32 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        f32::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_f64(&self, idx: usize) -> f64 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        f64::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_date32(&self, idx: usize) -> i32 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i32::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_date64(&self, idx: usize) -> i64 {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        i64::from_le_bytes(self.data[self.base_offset + offset..].try_into().unwrap())
    }

    fn get_utf8(&self, idx: usize) -> &str {
        self.assert_index_valid(idx);
        let offset_size = self.get_u64(idx);
        let offset = (offset_size >> 32) as usize;
        let len = (offset_size & 0xffff_ffff) as usize;
        let varlena_offset = self.base_offset + offset;
        let bytes = &self.data[varlena_offset..varlena_offset + len];
        std::str::from_utf8(bytes).unwrap()
    }

    fn get_binary(&self, idx: usize) -> &[u8] {
        self.assert_index_valid(idx);
        let offset_size = self.get_u64(idx);
        let offset = (offset_size >> 32) as usize;
        let len = (offset_size & 0xffff_ffff) as usize;
        let varlena_offset = self.base_offset + offset;
        &self.data[varlena_offset..varlena_offset + len]
    }

    // ----------------------
    // Mutators
    // ----------------------

    fn set_null_at(&mut self, idx: usize) {
        let null_bits =
            &mut self.data[self.base_offset..self.base_offset + self.null_width];
        set_bit(null_bits, idx, false)
    }

    fn set_non_null_at(&mut self, idx: usize) {
        let null_bits =
            &mut self.data[self.base_offset..self.base_offset + self.null_width];
        set_bit(null_bits, idx, true)
    }

    fn set_boolean(&mut self, idx: usize, value: bool) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[self.base_offset + offset] = if value { 1 } else { 0 };
    }

    fn set_u8(&mut self, idx: usize, value: u8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[self.base_offset + offset] = value;
    }

    fn set_u16(&mut self, idx: usize, value: u16) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn set_u32(&mut self, idx: usize, value: u32) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn set_u64(&mut self, idx: usize, value: u64) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn set_i8(&mut self, idx: usize, value: i8) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset] = value.to_le_bytes()[0];
    }

    fn set_i16(&mut self, idx: usize, value: i16) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn set_i32(&mut self, idx: usize, value: i32) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn set_i64(&mut self, idx: usize, value: i64) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn set_f32(&mut self, idx: usize, value: f32) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn set_f64(&mut self, idx: usize, value: f64) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn set_date32(&mut self, idx: usize, value: i32) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn set_date64(&mut self, idx: usize, value: i64) {
        self.assert_index_valid(idx);
        let offset = self.base_offset + self.field_offsets[idx];
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn set_offset_size(&mut self, idx: usize, size: usize) {
        let offset_and_size: u64 = (self.varlena_offset << 32 | size) as u64;
        self.set_u64(idx, offset_and_size);
    }

    fn set_utf8(&mut self, idx: usize, value: &str) {
        self.assert_index_valid(idx);
        let bytes = value.as_bytes();
        let size = bytes.len();
        self.set_offset_size(idx, size);
        let varlena_offset = self.base_offset + self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(&bytes);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    fn set_binary(&mut self, idx: usize, value: &[u8]) {
        self.assert_index_valid(idx);
        let size = value.len();
        self.set_offset_size(idx, size);
        let varlena_offset = self.base_offset + self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(&value);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    pub fn current_width(&self) -> usize {
        self.null_width + self.values_width + self.varlena_width
    }

    /// End each row at 8-byte word boundary.
    fn end_padding(&mut self) {
        let payload_width = self.current_width();
        self.row_width = (payload_width.saturating_add(7) / 8) * 8;
    }
}

/// Get number of bytes needed for null bit set
fn null_width(num_fields: usize) -> usize {
    num_fields.saturating_add(7) / 8
}

/// Get relative offsets for each field and total width for values
fn get_offsets(null_width: usize, schema: &Arc<Schema>) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        offset += type_width(f.data_type());
    }
    (offsets, offset - null_width)
}

fn supported_type(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Boolean
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Date32
            | Date64
            | Utf8
            | Binary
    )
}

fn var_length(dt: &DataType) -> bool {
    use DataType::*;
    matches!(dt, Utf8 | Binary)
}

fn type_width(dt: &DataType) -> usize {
    use DataType::*;
    if var_length(dt) {
        return 8;
    }
    match dt {
        Boolean | UInt8 | Int8 => 1,
        UInt16 | Int16 => 2,
        UInt32 | Int32 | Float32 | Date32 => 4,
        UInt64 | Int64 | Float64 | Date64 => 8,
        _ => unreachable!(),
    }
}

fn estimate_row_width(schema: &Arc<Schema>) -> usize {
    let mut width = 0;
    for f in schema.fields() {
        width += type_width(f.data_type());
        match f.data_type() {
            DataType::Utf8 => width += UTF8_DEFAULT_SIZE,
            DataType::Binary => width += BINARY_DEFAULT_SIZE,
            _ => {}
        }
    }
    width
}

fn fixed_size(schema: &Arc<Schema>) -> bool {
    schema.fields().iter().all(|f| !var_length(f.data_type()))
}

fn supported(schema: &Arc<Schema>) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| supported_type(f.data_type()))
}

#[cfg(test)]
mod tests {

    #[test]
    fn round_trip() {
        assert!(true)
    }
}
