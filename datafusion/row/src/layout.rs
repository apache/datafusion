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

use crate::schema_null_free;
use arrow::datatypes::{DataType, Schema};
use arrow::util::bit_util::{ceil, round_upto_power_of_2};

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
#[derive(Debug, Clone)]
pub struct RowLayout {
    /// Type of the layout
    row_type: RowType,
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
    /// new
    pub fn new(schema: &Schema, row_type: RowType) -> Self {
        assert!(
            row_supported(schema, row_type),
            "{:?}Row with {:?} not supported yet.",
            row_type,
            schema,
        );
        let null_free = schema_null_free(schema);
        let field_count = schema.fields().len();
        let null_width = if null_free {
            0
        } else {
            match row_type {
                RowType::Compact => ceil(field_count, 8),
                RowType::WordAligned => round_upto_power_of_2(ceil(field_count, 8), 8),
            }
        };
        let (field_offsets, values_width) = match row_type {
            RowType::Compact => compact_offsets(null_width, schema),
            RowType::WordAligned => word_aligned_offsets(null_width, schema),
        };
        Self {
            row_type,
            null_free,
            null_width,
            values_width,
            field_count,
            field_offsets,
        }
    }

    /// Get fixed part width for this layout
    #[inline(always)]
    pub fn fixed_part_width(&self) -> usize {
        self.null_width + self.values_width
    }
}

/// Get relative offsets for each field and total width for values
fn compact_offsets(null_width: usize, schema: &Schema) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        offset += compact_type_width(f.data_type());
    }
    (offsets, offset - null_width)
}

fn var_length(dt: &DataType) -> bool {
    use DataType::*;
    matches!(dt, Utf8 | Binary)
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

fn word_aligned_offsets(null_width: usize, schema: &Schema) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        assert!(!matches!(f.data_type(), DataType::Decimal(_, _)));
        // All of the current support types can fit into one single 8-bytes word.
        // When we decide to support Decimal type in the future, its width would be
        // of two 8-bytes words and should adapt the width calculation below.
        offset += 8;
    }
    (offsets, offset - null_width)
}

/// Estimate row width based on schema
pub(crate) fn estimate_row_width(schema: &Schema, layout: &RowLayout) -> usize {
    let mut width = layout.fixed_part_width();
    if matches!(layout.row_type, RowType::WordAligned) {
        return width;
    }
    for f in schema.fields() {
        match f.data_type() {
            DataType::Utf8 => width += UTF8_DEFAULT_SIZE,
            DataType::Binary => width += BINARY_DEFAULT_SIZE,
            _ => {}
        }
    }
    round_upto_power_of_2(width, 8)
}

/// Tell if we can create raw-bytes based rows since we currently
/// has limited data type supports in the row format
pub fn row_supported(schema: &Schema, row_type: RowType) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| supported_type(f.data_type(), row_type))
}

fn supported_type(dt: &DataType, row_type: RowType) -> bool {
    use DataType::*;

    match row_type {
        RowType::Compact => {
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
        // only fixed length types are supported for fast in-place update.
        RowType::WordAligned => {
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
            )
        }
    }
}
