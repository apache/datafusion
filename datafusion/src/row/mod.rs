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

use arrow::datatypes::{DataType, Schema};
use std::sync::Arc;

mod bitmap;
mod reader;
mod writer;

const UTF8_DEFAULT_SIZE: usize = 20;
const BINARY_DEFAULT_SIZE: usize = 100;

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

fn estimate_row_width(null_width: usize, schema: &Arc<Schema>) -> usize {
    let mut width = null_width;
    for f in schema.fields() {
        width += type_width(f.data_type());
        match f.data_type() {
            DataType::Utf8 => width += UTF8_DEFAULT_SIZE,
            DataType::Binary => width += BINARY_DEFAULT_SIZE,
            _ => {}
        }
    }
    (width.saturating_add(7) / 8) * 8
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
    use super::*;
    use crate::error::Result;
    use crate::row::reader::read_as_batch;
    use crate::row::writer::write_batch_unchecked;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use DataType::*;

    macro_rules! fn_test_single_type {
        ($ARRAY: ident, $TYPE: expr, $VEC: expr) => {
            paste::item! {
                #[test]
                #[allow(non_snake_case)]
                fn [<test_single_ $TYPE>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, false)]));
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let row_offsets =
                        { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
                    let output_batch = { read_as_batch(&mut vector, schema, row_offsets)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }
            }
        };
    }

    fn_test_single_type!(
        BooleanArray,
        Boolean,
        vec![Some(true), Some(false), None, Some(true), None]
    );

    fn_test_single_type!(
        Int8Array,
        Int8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int16Array,
        Int16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int32Array,
        Int32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int64Array,
        Int64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt8Array,
        UInt8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt16Array,
        UInt16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt32Array,
        UInt32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt64Array,
        UInt64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Float32Array,
        Float32,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Float64Array,
        Float64,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Date32Array,
        Date32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Date64Array,
        Date64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        StringArray,
        Utf8,
        vec![Some("hello"), Some("world"), None, Some(""), Some("")]
    );
}
