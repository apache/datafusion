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

//! Various row layouts for different use case

use crate::schema_null_free;
use arrow::datatypes::{DataType, Schema};
use arrow::util::bit_util::{ceil, round_upto_power_of_2};

/// Row layout stores one or multiple 8-byte word(s) per field for CPU-friendly
/// and efficient processing.
///
/// It is mainly used to represent the rows with frequently updated content,
/// for example, grouping state for hash aggregation.
///
/// Each tuple consists of two parts: "`null bit set`" and "`values`".
///
/// For null-free tuples, the null bit set can be omitted.
///
/// The null bit set, when present, is aligned to 8 bytes. It stores one bit per field.
///
/// In the region of the values, we store the fields in the order they are defined in the schema.
/// Each field is stored in one or multiple 8-byte words.
///
/// ```plaintext
/// ┌─────────────────┬─────────────────────┐
/// │Validity Bitmask │      Fields         │
/// │ (8-byte aligned)│   (8-byte words)    │
/// └─────────────────┴─────────────────────┘
/// ```
///
///  For example, given the schema (Int8, Float32, Int64) with a null-free tuple
///
///  Encoding the tuple (1, 3.14, 42)
///
///  Requires 24 bytes (3 fields * 8 bytes each):
///
/// ```plaintext
/// ┌──────────────────────┬──────────────────────┬──────────────────────┐
/// │         0x01         │      0x4048F5C3      │      0x0000002A      │
/// └──────────────────────┴──────────────────────┴──────────────────────┘
/// 0                      8                      16                     24
/// ```
///
///  If the schema allows null values and the tuple is (1, NULL, 42)
///
///  Encoding the tuple requires 32 bytes (1 * 8 bytes for the null bit set + 3 fields * 8 bytes each):
///
/// ```plaintext
/// ┌──────────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┐
/// │       0b00000101         │         0x01         │      0x00000000      │      0x0000002A      │
/// │ (7 bytes padding after)  │                      │                      │                      │
/// └──────────────────────────┴──────────────────────┴──────────────────────┴──────────────────────┘
/// 0                          8                      16                     24                     32
/// ```
#[derive(Debug, Clone)]
pub struct RowLayout {
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
    pub fn new(schema: &Schema) -> Self {
        assert!(
            row_supported(schema),
            "Row with {schema:?} not supported yet.",
        );
        let null_free = schema_null_free(schema);
        let field_count = schema.fields().len();
        let null_width = if null_free {
            0
        } else {
            round_upto_power_of_2(ceil(field_count, 8), 8)
        };
        let (field_offsets, values_width) = word_aligned_offsets(null_width, schema);
        Self {
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

fn word_aligned_offsets(null_width: usize, schema: &Schema) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        assert!(!matches!(f.data_type(), DataType::Decimal256(_, _)));
        // All of the current support types can fit into one single 8-bytes word except for Decimal128.
        // For Decimal128, its width is of two 8-bytes words.
        match f.data_type() {
            DataType::Decimal128(_, _) => offset += 16,
            _ => offset += 8,
        }
    }
    (offsets, offset - null_width)
}

/// Return true of data in `schema` can be converted to raw-bytes
/// based rows.
///
/// Note all schemas can be supported in the row format
pub fn row_supported(schema: &Schema) -> bool {
    schema.fields().iter().all(|f| {
        let dt = f.data_type();
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
                | Decimal128(_, _)
        )
    })
}
