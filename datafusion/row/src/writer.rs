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

//! [`RowWriter`]  writes [`RecordBatch`]es to Vec<u8> to stitch attributes together

use crate::layout::{estimate_row_width, RowLayout, RowType};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util::{round_upto_power_of_2, set_bit_raw, unset_bit_raw};
use datafusion_common::Result;
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
    row_type: RowType,
) -> Vec<usize> {
    let mut writer = RowWriter::new(&schema, row_type);
    let mut current_offset = offset;
    let mut offsets = vec![];
    let columns = batch.columns();
    for cur_row in row_idx..batch.num_rows() {
        offsets.push(current_offset);
        let row_width = write_row(&mut writer, cur_row, &schema, columns);
        output[current_offset..current_offset + row_width]
            .copy_from_slice(writer.get_row());
        current_offset += row_width;
        writer.reset()
    }
    offsets
}

/// bench interpreted version write
#[inline(never)]
pub fn bench_write_batch(
    batches: &[Vec<RecordBatch>],
    schema: Arc<Schema>,
    row_type: RowType,
) -> Result<Vec<usize>> {
    let mut writer = RowWriter::new(&schema, row_type);
    let mut lengths = vec![];

    for batch in batches.iter().flatten() {
        let columns = batch.columns();
        for cur_row in 0..batch.num_rows() {
            let row_width = write_row(&mut writer, cur_row, &schema, columns);
            lengths.push(row_width);
            writer.reset()
        }
    }

    Ok(lengths)
}

#[macro_export]
macro_rules! set_idx {
    ($WIDTH: literal, $SELF: ident, $IDX: ident, $VALUE: ident) => {{
        $SELF.assert_index_valid($IDX);
        let offset = $SELF.field_offsets()[$IDX];
        $SELF.data[offset..offset + $WIDTH].copy_from_slice(&$VALUE.to_le_bytes());
    }};
}

#[macro_export]
macro_rules! fn_set_idx {
    ($NATIVE: ident, $WIDTH: literal) => {
        paste::item! {
            fn [<set_ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                self.assert_index_valid(idx);
                let offset = self.field_offsets()[idx];
                self.data[offset..offset + $WIDTH].copy_from_slice(&value.to_le_bytes());
            }
        }
    };
}

/// Reusable row writer backed by Vec<u8>
///
/// ```text
///                             ┌ ─ ─ ─ ─ ─ ─ ─ ─
///                                 RowWriter    │
/// ┌───────────────────────┐   │  [RowFormat]
/// │                       │                    │
/// │                       │   │(copy from Array
/// │                       │        to [u8])    │          ┌───────────────────────┐
/// │      RecordBatch      │   └ ─ ─ ─ ─ ─ ─ ─ ─           │       RowFormat       │
/// │                       │──────────────────────────────▶│        Vec<u8>        │
/// │   (... N Rows ...)    │                               │                       │
/// │                       │                               └───────────────────────┘
/// │                       │
/// │                       │
/// └───────────────────────┘
/// ```
pub struct RowWriter {
    /// Layout on how to write each field
    layout: RowLayout,
    /// buffer for the current tuple been written.
    data: Vec<u8>,
    /// Length in bytes for the current tuple, 8-bytes word aligned.
    pub(crate) row_width: usize,
    /// Length in bytes for `variable length data` part of the current tuple.
    varlena_width: usize,
    /// Current offset for the next variable length field to write to.
    varlena_offset: usize,
}

impl RowWriter {
    /// new
    pub fn new(schema: &Schema, row_type: RowType) -> Self {
        let layout = RowLayout::new(schema, row_type);
        let init_capacity = estimate_row_width(schema, &layout);
        let varlena_offset = layout.fixed_part_width();
        Self {
            layout,
            data: vec![0; init_capacity],
            row_width: 0,
            varlena_width: 0,
            varlena_offset,
        }
    }

    /// Reset the row writer state for new tuple
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.row_width = 0;
        self.varlena_width = 0;
        self.varlena_offset = self.layout.fixed_part_width();
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
        self.layout.fixed_part_width() + self.varlena_width
    }

    /// End each row at 8-byte word boundary.
    pub(crate) fn end_padding(&mut self) {
        let payload_width = self.current_width();
        self.row_width = round_upto_power_of_2(payload_width, 8);
        if self.data.len() < self.row_width {
            self.data.resize(self.row_width, 0);
        }
    }

    /// Get raw bytes
    pub fn get_row(&self) -> &[u8] {
        &self.data[0..self.row_width]
    }
}

/// Stitch attributes of tuple in `batch` at `row_idx` and returns the tuple width
pub fn write_row(
    row_writer: &mut RowWriter,
    row_idx: usize,
    schema: &Schema,
    columns: &[ArrayRef],
) -> usize {
    // Get the row from the batch denoted by row_idx
    if row_writer.null_free() {
        for ((i, f), col) in schema.fields().iter().enumerate().zip(columns.iter()) {
            write_field(i, row_idx, col, f.data_type(), row_writer);
        }
    } else {
        for ((i, f), col) in schema.fields().iter().enumerate().zip(columns.iter()) {
            if !col.is_null(row_idx) {
                row_writer.set_non_null_at(i);
                write_field(i, row_idx, col, f.data_type(), row_writer);
            } else {
                row_writer.set_null_at(i);
            }
        }
    }

    row_writer.end_padding();
    row_writer.row_width
}

macro_rules! fn_write_field {
    ($NATIVE: ident, $ARRAY: ident) => {
        paste::item! {
            pub(crate) fn [<write_field_ $NATIVE>](to: &mut RowWriter, from: &Arc<dyn Array>, col_idx: usize, row_idx: usize) {
                let from = from
                    .as_any()
                    .downcast_ref::<$ARRAY>()
                    .unwrap();
                to.[<set_ $NATIVE>](col_idx, from.value(row_idx));
            }
        }
    };
}

fn_write_field!(bool, BooleanArray);
fn_write_field!(u8, UInt8Array);
fn_write_field!(u16, UInt16Array);
fn_write_field!(u32, UInt32Array);
fn_write_field!(u64, UInt64Array);
fn_write_field!(i8, Int8Array);
fn_write_field!(i16, Int16Array);
fn_write_field!(i32, Int32Array);
fn_write_field!(i64, Int64Array);
fn_write_field!(f32, Float32Array);
fn_write_field!(f64, Float64Array);

pub(crate) fn write_field_date32(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<Date32Array>().unwrap();
    to.set_date32(col_idx, from.value(row_idx));
}

pub(crate) fn write_field_date64(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<Date64Array>().unwrap();
    to.set_date64(col_idx, from.value(row_idx));
}

pub(crate) fn write_field_utf8(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<StringArray>().unwrap();
    let s = from.value(row_idx);
    let new_width = to.current_width() + s.as_bytes().len();
    if new_width > to.data.len() {
        to.data.resize(max(to.data.capacity(), new_width), 0);
    }
    to.set_utf8(col_idx, s);
}

pub(crate) fn write_field_binary(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<BinaryArray>().unwrap();
    let s = from.value(row_idx);
    let new_width = to.current_width() + s.len();
    if new_width > to.data.len() {
        to.data.resize(max(to.data.capacity(), new_width), 0);
    }
    to.set_binary(col_idx, s);
}

fn write_field(
    col_idx: usize,
    row_idx: usize,
    col: &Arc<dyn Array>,
    dt: &DataType,
    row: &mut RowWriter,
) {
    use DataType::*;
    match dt {
        Boolean => write_field_bool(row, col, col_idx, row_idx),
        UInt8 => write_field_u8(row, col, col_idx, row_idx),
        UInt16 => write_field_u16(row, col, col_idx, row_idx),
        UInt32 => write_field_u32(row, col, col_idx, row_idx),
        UInt64 => write_field_u64(row, col, col_idx, row_idx),
        Int8 => write_field_i8(row, col, col_idx, row_idx),
        Int16 => write_field_i16(row, col, col_idx, row_idx),
        Int32 => write_field_i32(row, col, col_idx, row_idx),
        Int64 => write_field_i64(row, col, col_idx, row_idx),
        Float32 => write_field_f32(row, col, col_idx, row_idx),
        Float64 => write_field_f64(row, col, col_idx, row_idx),
        Date32 => write_field_date32(row, col, col_idx, row_idx),
        Date64 => write_field_date64(row, col, col_idx, row_idx),
        Utf8 => write_field_utf8(row, col, col_idx, row_idx),
        Binary => write_field_binary(row, col, col_idx, row_idx),
        _ => unimplemented!(),
    }
}
