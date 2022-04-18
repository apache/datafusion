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

//! Various row layout for different use case

use crate::row::{row_supported, schema_null_free, var_length};
use arrow::datatypes::{DataType, Schema};
use arrow::util::bit_util::{ceil, round_upto_power_of_2};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

const UTF8_DEFAULT_SIZE: usize = 20;
const BINARY_DEFAULT_SIZE: usize = 100;

#[derive(Copy, Clone, Debug)]
/// Type of a RowLayout
pub enum RowType {
    /// This type of layout will store each field with minimum bytes for space efficiency.
    /// Its typical use case represents a sorting payload that accesses all row fields as a unit.
    Compact,
    /// This type of layout will store one 8-byte word per field for CPU-friendly,
    /// It is mainly used to represent the rows with frequently updated content,
    /// for example, grouping state for hash aggregation.
    WordAligned,
    // RawComparable,
}

/// Reveals how the fields of a record are stored in the raw-bytes format
pub(crate) struct RowLayout {
    /// Type of the layout
    type_: RowType,
    /// If a row is null free according to its schema
    pub(crate) null_free: bool,
    /// The number of bytes used to store null bits for each field.
    pub(crate) null_width: usize,
    /// Length in bytes for `values` part of the current tuple.
    pub(crate) values_width: usize,
    /// Total number of fields for each tuple.
    pub(crate) field_count: usize,
    /// Starting offset for each fields in the raw bytes.
    pub(crate) field_offsets: Vec<usize>,
}

impl RowLayout {
    pub(crate) fn new(schema: &Arc<Schema>, type_: RowType) -> Self {
        assert!(row_supported(schema));
        let null_free = schema_null_free(schema);
        let field_count = schema.fields().len();
        let null_width = if null_free { 0 } else { ceil(field_count, 8) };
        let (field_offsets, values_width) = match type_ {
            RowType::Compact => compact_offsets(null_width, schema),
            RowType::WordAligned => word_aligned_offsets(null_width, schema),
        };
        Self {
            type_,
            null_free,
            null_width,
            values_width,
            field_count,
            field_offsets,
        }
    }

    #[inline(always)]
    pub(crate) fn init_varlena_offset(&self) -> usize {
        self.null_width + self.values_width
    }
}

impl Debug for RowLayout {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowLayout")
            .field("type", &self.type_)
            .field("null_width", &self.null_width)
            .field("values_width", &self.values_width)
            .field("field_count", &self.field_count)
            .field("offsets", &self.field_offsets)
            .finish()
    }
}

/// Get relative offsets for each field and total width for values
fn compact_offsets(null_width: usize, schema: &Arc<Schema>) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        offset += compact_type_width(f.data_type());
    }
    (offsets, offset - null_width)
}

fn compact_type_width(dt: &DataType) -> usize {
    use DataType::*;
    if var_length(dt) {
        return std::mem::size_of::<u64>();
    }
    match dt {
        Boolean | UInt8 | Int8 => 1,
        UInt16 | Int16 => 2,
        UInt32 | Int32 | Float32 | Date32 => 4,
        UInt64 | Int64 | Float64 | Date64 => 8,
        _ => unreachable!(),
    }
}

fn word_aligned_offsets(null_width: usize, schema: &Arc<Schema>) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for _ in schema.fields() {
        offsets.push(offset);
        offset += 8; // a 8-bytes word for each field
    }
    (offsets, offset - null_width)
}

/// Estimate row width based on schema
pub fn estimate_row_width(schema: &Arc<Schema>) -> usize {
    let null_free = schema_null_free(schema);
    let field_count = schema.fields().len();
    let mut width = if null_free { 0 } else { ceil(field_count, 8) };
    for f in schema.fields() {
        width += compact_type_width(f.data_type());
        match f.data_type() {
            DataType::Utf8 => width += UTF8_DEFAULT_SIZE,
            DataType::Binary => width += BINARY_DEFAULT_SIZE,
            _ => {}
        }
    }
    round_upto_power_of_2(width, 8)
}
