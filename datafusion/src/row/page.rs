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

//! Page holds multiple row-wise tuples

use crate::row::{supported, Row, BINARY_DEFAULT_SIZE, UTF8_DEFAULT_SIZE};
use arrow::array::Array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use super::estimate_row_width;

struct Page {
    data: Vec<u8>,
    capacity: usize,
    available: usize,
    /// The start offsets of each row in the page.
    /// The last offset equals the current size of the page.
    rows: Vec<usize>,
    schema: Arc<Schema>,
}

impl Page {
    fn new(capacity: usize, schema: Arc<Schema>) -> Self {
        assert!(supported(&schema));
        Self {
            data: vec![0; capacity],
            capacity,
            available: 0,
            rows: vec![0],
            schema,
        }
    }

    /// Append batch from `row_idx` to Page and returns (is_page_full, next_row_to_write)
    fn write_batch(&mut self, batch: &RecordBatch, row_idx: usize) -> (bool, usize) {
        let mut row = Row::new_from(&self.schema, &mut self.data, self.available);
        let estimate_row_width = estimate_row_width(&self.schema);

        if row.fixed_size {
            for cur_row in row_idx..batch.num_rows() {
                if !self.has_space(estimate_row_width) {
                    return (true, cur_row);
                }
                self.write_row_unchecked(&mut row, cur_row, batch);
                row.point_to(&mut self.data, self.available);
            }
        } else {
            for cur_row in row_idx..batch.num_rows() {
                if !self.has_space(estimate_row_width) {
                    return (true, cur_row);
                }
                let success = self.write_row(&mut row, cur_row, batch);
                if !success {
                    return (true, cur_row);
                }
                row.point_to(&mut self.data, self.available);
            }
        }
        (false, batch.num_rows())
    }

    fn write_row(&mut self, row: &mut Row, row_idx: usize, batch: &RecordBatch) -> bool {
        // Get the row from the batch denoted by row_idx
        for ((i, f), col) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .zip(batch.columns().iter())
        {
            if !c.is_null(row_idx) {
                if !self.write_field(i, row_idx, col, f.data_type(), row) {
                    return false;
                }
                row.set_non_null_at(i);
            } else {
                row.set_null_at(i);
            }
        }

        row.end_padding();
        self.available += row.row_width;
        self.rows.push(self.available);
        true
    }

    fn write_row_unchecked(
        &mut self,
        row: &mut Row,
        row_idx: usize,
        batch: &RecordBatch,
    ) {
        // Get the row from the batch denoted by row_idx
        for ((i, f), col) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .zip(batch.columns().iter())
        {
            if !c.is_null(row_idx) {
                row.set_non_null_at(i);
                self.write_field(i, row_idx, col, f.data_type(), row);
            } else {
                row.set_null_at(i);
            }
        }

        row.end_padding();
        self.available += row.row_width;
        self.rows.push(self.available);
    }

    fn write_field(
        &mut self,
        col_idx: usize,
        row_idx: usize,
        col: &Arc<dyn Array>,
        dt: &DataTypem,
        row: &mut Row,
    ) -> bool {
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
                let str = c.value(row_idx);
                let len = str.as_bytes().len();
                if len > UTF8_DEFAULT_SIZE && self.has_space(len + row.current_width()) {
                    return false;
                }
                row.set_utf8(col_idx, str);
            }
            Binary => {
                let c = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                let binary = c.value(row_idx);
                let len = binary.len();
                if len > BINARY_DEFAULT_SIZE && self.has_space(len + row.current_width())
                {
                    return false;
                }
                row.set_binary(col_idx, binary);
            }
            _ => unimplemented!(),
        }
        true
    }

    fn get_row(&self, row_id: usize) -> &[u8] {
        let row_offset = self.rows[row_id];
        &self.data[row_offset..row_offset + self.rows[row_id + 1] - row_offset]
    }

    /// has enough space for a row of the given size
    fn has_space(&self, row_size: usize) -> bool {
        self.available + row_size <= self.capacity
    }
}
