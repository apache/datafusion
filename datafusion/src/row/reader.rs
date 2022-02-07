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

//! Accessing row from raw bytes

use crate::error::{DataFusionError, Result};
use crate::row::bitmap::get_bit;
use crate::row::{get_offsets, null_width, supported};
use arrow::array::{make_builder, Array, ArrayBuilder};
use arrow::datatypes::{DataType, Schema};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Read `data` of raw-bytes rows starting at `offsets` out to a record batch
pub fn read_as_batch(
    data: &mut [u8],
    schema: Arc<Schema>,
    offsets: Vec<usize>,
) -> Result<RecordBatch> {
    let row_num = offsets.len();
    let mut output = MutableRecordBatch::new(row_num, schema.clone());
    let mut row = RowReader::new(&schema, data);

    for i in 0..row_num {
        row.point_to(offsets[i]);
        read_row(&row, &mut output, &schema)?
    }

    output.output().map_err(DataFusionError::ArrowError)
}

struct RowReader<'a> {
    data: &'a [u8],
    base_offset: usize,
    field_count: usize,
    field_offsets: Vec<usize>,
}

impl<'a> RowReader<'a> {
    fn new(schema: &Arc<Schema>, data: &'a [u8]) -> Self {
        assert!(supported(&schema));
        let field_count = schema.fields().len();
        let null_width = null_width(field_count);
        let (field_offsets, _) = get_offsets(null_width, &schema);
        Self {
            data,
            base_offset: 0,
            field_count,
            field_offsets,
        }
    }

    /// Update this row to point to position `offset` in `base`
    fn point_to(&mut self, offset: usize) {
        self.base_offset = offset;
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.field_count);
    }

    // ----------------------
    // Accessors
    // ----------------------

    fn is_valid_at(&self, idx: usize) -> bool {
        get_bit(&self.data, idx)
    }

    fn get_bool(&self, idx: usize) -> bool {
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

    fn get_bool_opt(&self, idx: usize) -> Option<bool> {
        if self.is_valid_at(idx) {
            Some(self.get_bool(idx))
        } else {
            None
        }
    }

    fn get_u8_opt(&self, idx: usize) -> Option<u8> {
        if self.is_valid_at(idx) {
            Some(self.get_u8(idx))
        } else {
            None
        }
    }

    fn get_u16_opt(&self, idx: usize) -> Option<u16> {
        if self.is_valid_at(idx) {
            Some(self.get_u16(idx))
        } else {
            None
        }
    }

    fn get_u32_opt(&self, idx: usize) -> Option<u32> {
        if self.is_valid_at(idx) {
            Some(self.get_u32(idx))
        } else {
            None
        }
    }

    fn get_u64_opt(&self, idx: usize) -> Option<u64> {
        if self.is_valid_at(idx) {
            Some(self.get_u64(idx))
        } else {
            None
        }
    }

    fn get_i8_opt(&self, idx: usize) -> Option<i8> {
        if self.is_valid_at(idx) {
            Some(self.get_i8(idx))
        } else {
            None
        }
    }

    fn get_i16_opt(&self, idx: usize) -> Option<i16> {
        if self.is_valid_at(idx) {
            Some(self.get_i16(idx))
        } else {
            None
        }
    }

    fn get_i32_opt(&self, idx: usize) -> Option<i32> {
        if self.is_valid_at(idx) {
            Some(self.get_i32(idx))
        } else {
            None
        }
    }

    fn get_i64_opt(&self, idx: usize) -> Option<i64> {
        if self.is_valid_at(idx) {
            Some(self.get_i64(idx))
        } else {
            None
        }
    }

    fn get_f32_opt(&self, idx: usize) -> Option<f32> {
        if self.is_valid_at(idx) {
            Some(self.get_f32(idx))
        } else {
            None
        }
    }

    fn get_f64_opt(&self, idx: usize) -> Option<f64> {
        if self.is_valid_at(idx) {
            Some(self.get_f64(idx))
        } else {
            None
        }
    }

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

    fn get_utf8_opt(&self, idx: usize) -> Option<&str> {
        if self.is_valid_at(idx) {
            Some(self.get_utf8(idx))
        } else {
            None
        }
    }

    fn get_binary_opt(&self, idx: usize) -> Option<&[u8]> {
        if self.is_valid_at(idx) {
            Some(self.get_binary(idx))
        } else {
            None
        }
    }
}

fn read_row(
    row: &RowReader,
    batch: &mut MutableRecordBatch,
    schema: &Arc<Schema>,
) -> Result<()> {
    for ((col_idx, to), field) in batch
        .arrays
        .iter_mut()
        .enumerate()
        .zip(schema.fields().iter())
    {
        read_field(to, field.data_type(), col_idx, row)?
    }
    Ok(())
}

fn read_field(
    to: &mut Box<dyn ArrayBuilder>,
    dt: &DataType,
    col_idx: usize,
    row: &RowReader,
) -> Result<()> {
    use arrow::array::*;
    use DataType::*;
    match dt {
        Boolean => {
            let to = to.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
            to.append_option(row.get_bool_opt(col_idx))?;
        }
        UInt8 => {
            let to = to.as_any_mut().downcast_mut::<UInt8Builder>().unwrap();
            to.append_option(row.get_u8_opt(col_idx))?;
        }
        UInt16 => {
            let to = to.as_any_mut().downcast_mut::<UInt16Builder>().unwrap();
            to.append_option(row.get_u16_opt(col_idx))?;
        }
        UInt32 => {
            let to = to.as_any_mut().downcast_mut::<UInt32Builder>().unwrap();
            to.append_option(row.get_u32_opt(col_idx))?;
        }
        UInt64 => {
            let to = to.as_any_mut().downcast_mut::<UInt64Builder>().unwrap();
            to.append_option(row.get_u64_opt(col_idx))?;
        }
        Int8 => {
            let to = to.as_any_mut().downcast_mut::<Int8Builder>().unwrap();
            to.append_option(row.get_i8_opt(col_idx))?;
        }
        Int16 => {
            let to = to.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            to.append_option(row.get_i16_opt(col_idx))?;
        }
        Int32 => {
            let to = to.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            to.append_option(row.get_i32_opt(col_idx))?;
        }
        Int64 => {
            let to = to.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            to.append_option(row.get_i64_opt(col_idx))?;
        }
        Float32 => {
            let to = to.as_any_mut().downcast_mut::<Float32Builder>().unwrap();
            to.append_option(row.get_f32_opt(col_idx))?;
        }
        Float64 => {
            let to = to.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
            to.append_option(row.get_f64_opt(col_idx))?;
        }
        Date32 => {
            let to = to.as_any_mut().downcast_mut::<Date32Builder>().unwrap();
            to.append_option(row.get_date32_opt(col_idx))?;
        }
        Date64 => {
            let to = to.as_any_mut().downcast_mut::<Date64Builder>().unwrap();
            to.append_option(row.get_date64_opt(col_idx))?;
        }
        Utf8 => {
            let to = to.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            to.append_option(row.get_utf8_opt(col_idx))?;
        }
        Binary => {
            let to = to.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
            if row.is_valid_at(col_idx) {
                to.append_value(row.get_binary(col_idx))?;
            } else {
                to.append_null()?;
            }
        }
        _ => unimplemented!(),
    }
    Ok(())
}

struct MutableRecordBatch {
    arrays: Vec<Box<dyn ArrayBuilder>>,
    schema: Arc<Schema>,
}

impl MutableRecordBatch {
    fn new(target_batch_size: usize, schema: Arc<Schema>) -> Self {
        let arrays = new_arrays(&schema, target_batch_size);
        Self { arrays, schema }
    }

    fn output(&mut self) -> ArrowResult<RecordBatch> {
        let result = make_batch(self.schema.clone(), self.arrays.drain(..).collect());
        result
    }
}

fn new_arrays(schema: &Arc<Schema>, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            make_builder(dt, batch_size)
        })
        .collect::<Vec<_>>()
}

fn make_batch(
    schema: Arc<Schema>,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}
