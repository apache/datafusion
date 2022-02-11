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

//! Reusable row writer backed by Vec<u8> to stitch attributes together

use crate::row::{estimate_row_width, fixed_size, get_offsets, supported};
use arrow::array::Array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util::{ceil, round_upto_power_of_2, set_bit_raw, unset_bit_raw};
use std::cmp::max;
use std::sync::Arc;

/// Append batch from `row_idx` to `output` buffer start from `offset`
/// # Panics
///
/// This function will panic if the output buffer doesn't have enough space to hold all the rows
pub fn write_batch_unchecked(
    output: &mut [u8],
    offset: usize,
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Schema>,
) -> Vec<usize> {
    let mut writer = RowWriter::new(&schema);
    let mut current_offset = offset;
    let mut offsets = vec![];
    for cur_row in row_idx..batch.num_rows() {
        offsets.push(current_offset);
        let row_width = write_row(&mut writer, cur_row, batch);
        output[current_offset..current_offset + row_width]
            .copy_from_slice(writer.get_row());
        current_offset += row_width;
        writer.reset()
    }
    offsets
}

macro_rules! set_idx {
    ($WIDTH: literal, $SELF: ident, $IDX: ident, $VALUE: ident) => {{
        $SELF.assert_index_valid($IDX);
        let offset = $SELF.field_offsets[$IDX];
        $SELF.data[offset..offset + $WIDTH].copy_from_slice(&$VALUE.to_le_bytes());
    }};
}

macro_rules! fn_set_idx {
    ($NATIVE: ident, $WIDTH: literal) => {
        paste::item! {
            fn [<set_ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                self.assert_index_valid(idx);
                let offset = self.field_offsets[idx];
                self.data[offset..offset + $WIDTH].copy_from_slice(&value.to_le_bytes());
            }
        }
    };
}

/// Reusable row writer backed by Vec<u8>
pub struct RowWriter {
    /// buffer for the current tuple been written.
    data: Vec<u8>,
    /// Total number of fields for each tuple.
    field_count: usize,
    /// Length in bytes for the current tuple, 8-bytes word aligned.
    row_width: usize,
    /// The number of bytes used to store null bits for each field.
    null_width: usize,
    /// Length in bytes for `values` part of the current tuple.
    values_width: usize,
    /// Length in bytes for `variable length data` part of the current tuple.
    varlena_width: usize,
    /// Current offset for the next variable length field to write to.
    varlena_offset: usize,
    /// Starting offset for each fields in the raw bytes.
    /// For fixed length fields, it's where the actual data stores.
    /// For variable length fields, it's a pack of (offset << 32 | length) if we use u64.
    field_offsets: Vec<usize>,
}

impl RowWriter {
    /// new
    pub fn new(schema: &Arc<Schema>) -> Self {
        assert!(supported(schema));
        let field_count = schema.fields().len();
        let null_width = ceil(field_count, 8);
        let (field_offsets, values_width) = get_offsets(null_width, schema);
        let mut init_capacity = estimate_row_width(null_width, schema);
        if !fixed_size(schema) {
            // double the capacity to avoid repeated resize
            init_capacity *= 2;
        }
        Self {
            data: vec![0; init_capacity],
            field_count,
            row_width: 0,
            null_width,
            values_width,
            varlena_width: 0,
            varlena_offset: null_width + values_width,
            field_offsets,
        }
    }

    /// Reset the row writer state for new tuple
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.row_width = 0;
        self.varlena_width = 0;
        self.varlena_offset = self.null_width + self.values_width;
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.field_count);
    }

    fn set_null_at(&mut self, idx: usize) {
        let null_bits = &mut self.data[0..self.null_width];
        unsafe {
            unset_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

    fn set_non_null_at(&mut self, idx: usize) {
        let null_bits = &mut self.data[0..self.null_width];
        unsafe {
            set_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

    fn set_bool(&mut self, idx: usize, value: bool) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[offset] = if value { 1 } else { 0 };
    }

    fn set_u8(&mut self, idx: usize, value: u8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
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
        let offset = self.field_offsets[idx];
        self.data[offset] = value.to_le_bytes()[0];
    }

    fn set_date32(&mut self, idx: usize, value: i32) {
        set_idx!(4, self, idx, value)
    }

    fn set_date64(&mut self, idx: usize, value: i64) {
        set_idx!(8, self, idx, value)
    }

    fn set_offset_size(&mut self, idx: usize, size: u32) {
        let offset_and_size: u64 = (self.varlena_offset as u64) << 32 | (size as u64);
        self.set_u64(idx, offset_and_size);
    }

    fn set_utf8(&mut self, idx: usize, value: &str) {
        self.assert_index_valid(idx);
        let bytes = value.as_bytes();
        let size = bytes.len();
        self.set_offset_size(idx, size as u32);
        let varlena_offset = self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(bytes);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    fn set_binary(&mut self, idx: usize, value: &[u8]) {
        self.assert_index_valid(idx);
        let size = value.len();
        self.set_offset_size(idx, size as u32);
        let varlena_offset = self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(value);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    fn current_width(&self) -> usize {
        self.null_width + self.values_width + self.varlena_width
    }

    /// End each row at 8-byte word boundary.
    fn end_padding(&mut self) {
        let payload_width = self.current_width();
        self.row_width = round_upto_power_of_2(payload_width, 8);
        if self.data.capacity() < self.row_width {
            self.data.resize(self.row_width, 0);
        }
    }

    fn get_row(&self) -> &[u8] {
        &self.data[0..self.row_width]
    }
}

/// Stitch attributes of tuple in `batch` at `row_idx` and returns the tuple width
fn write_row(row: &mut RowWriter, row_idx: usize, batch: &RecordBatch) -> usize {
    // Get the row from the batch denoted by row_idx
    for ((i, f), col) in batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .zip(batch.columns().iter())
    {
        if !col.is_null(row_idx) {
            row.set_non_null_at(i);
            write_field(i, row_idx, col, f.data_type(), row);
        } else {
            row.set_null_at(i);
        }
    }

    row.end_padding();
    row.row_width
}

fn write_field(
    col_idx: usize,
    row_idx: usize,
    col: &Arc<dyn Array>,
    dt: &DataType,
    row: &mut RowWriter,
) {
    // TODO: JIT compile this
    use arrow::array::*;
    use DataType::*;
    match dt {
        Boolean => {
            let c = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            row.set_bool(col_idx, c.value(row_idx));
        }
        UInt8 => {
            let c = col.as_any().downcast_ref::<UInt8Array>().unwrap();
            row.set_u8(col_idx, c.value(row_idx));
        }
        UInt16 => {
            let c = col.as_any().downcast_ref::<UInt16Array>().unwrap();
            row.set_u16(col_idx, c.value(row_idx));
        }
        UInt32 => {
            let c = col.as_any().downcast_ref::<UInt32Array>().unwrap();
            row.set_u32(col_idx, c.value(row_idx));
        }
        UInt64 => {
            let c = col.as_any().downcast_ref::<UInt64Array>().unwrap();
            row.set_u64(col_idx, c.value(row_idx));
        }
        Int8 => {
            let c = col.as_any().downcast_ref::<Int8Array>().unwrap();
            row.set_i8(col_idx, c.value(row_idx));
        }
        Int16 => {
            let c = col.as_any().downcast_ref::<Int16Array>().unwrap();
            row.set_i16(col_idx, c.value(row_idx));
        }
        Int32 => {
            let c = col.as_any().downcast_ref::<Int32Array>().unwrap();
            row.set_i32(col_idx, c.value(row_idx));
        }
        Int64 => {
            let c = col.as_any().downcast_ref::<Int64Array>().unwrap();
            row.set_i64(col_idx, c.value(row_idx));
        }
        Float32 => {
            let c = col.as_any().downcast_ref::<Float32Array>().unwrap();
            row.set_f32(col_idx, c.value(row_idx));
        }
        Float64 => {
            let c = col.as_any().downcast_ref::<Float64Array>().unwrap();
            row.set_f64(col_idx, c.value(row_idx));
        }
        Date32 => {
            let c = col.as_any().downcast_ref::<Date32Array>().unwrap();
            row.set_date32(col_idx, c.value(row_idx));
        }
        Date64 => {
            let c = col.as_any().downcast_ref::<Date64Array>().unwrap();
            row.set_date64(col_idx, c.value(row_idx));
        }
        Utf8 => {
            let c = col.as_any().downcast_ref::<StringArray>().unwrap();
            let s = c.value(row_idx);
            let new_width = row.current_width() + s.as_bytes().len();
            if new_width > row.data.capacity() {
                // double the capacity to avoid repeated resize
                row.data.resize(max(row.data.capacity() * 2, new_width), 0);
            }
            row.set_utf8(col_idx, s);
        }
        Binary => {
            let c = col.as_any().downcast_ref::<BinaryArray>().unwrap();
            let binary = c.value(row_idx);
            let new_width = row.current_width() + binary.len();
            if new_width > row.data.capacity() {
                // double the capacity to avoid repeated resize
                row.data.resize(max(row.data.capacity() * 2, new_width), 0);
            }
            row.set_binary(col_idx, binary);
        }
        _ => unimplemented!(),
    }
}
