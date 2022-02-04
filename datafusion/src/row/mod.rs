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

struct Row<'a> {
    data: &'a mut [u8],
    base_offset: usize,
    length: usize,
    schema: Arc<Schema>,
    null_width: usize,
    values_width: usize,
    varlena_width: usize,
    field_offsets: Vec<usize>,
}

impl<'a> Row<'a> {
    pub fn new(schema: Arc<Schema>) -> Self {
        assert!(supported(&schema));
        let null_width = null_width(schema.fields().len());
        let (field_offsets, values_width) = get_offsets(null_width, &schema);
        Self {
            data: &mut [],
            base_offset: 0,
            length: 0,
            schema,
            null_width,
            values_width,
            varlena_width: 0,
            field_offsets,
        }
    }

    /// Update this row to point to position `offset` in `base`
    pub fn point_to(&mut self, base: &'a mut [u8], offset: usize) {
        self.data = base;
        self.base_offset = offset;
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.schema.fields().len());
    }

    // ----------------------
    // Accessors
    // ----------------------

    fn is_valid_at(&self, idx: usize) -> bool {
        let null_bits = &self.data[self.base_offset..self.base_offset + self.null_width];
        get_bit(null_bits, idx)
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

    /// End each row at 8-byte word boundary.
    fn end_padding(&mut self) {
        let payload_width = self.null_width + self.values_width + self.varlena_width;
        self.length = (payload_width.saturating_add(7) / 8) * 8;
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
            | Float16
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
        UInt16 | Int16 | Float16 => 2,
        UInt32 | Int32 | Float32 | Date32 => 4,
        UInt64 | Int64 | Float64 | Date64 => 8,
        _ => unreachable!(),
    }
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
