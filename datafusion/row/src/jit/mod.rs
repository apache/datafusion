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

//! Just-In-Time(JIT) version for row reader and writers

pub mod reader;
pub mod writer;

#[macro_export]
/// register external functions to the assembler
macro_rules! reg_fn {
    ($ASS:ident, $FN: path, $PARAM: expr, $RET: expr) => {
        $ASS.register_extern_fn(fn_name($FN), $FN as *const u8, $PARAM, $RET)?;
    };
}

fn fn_name<T>(f: T) -> &'static str {
    fn type_name_of<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }
    let name = type_name_of(f);

    // Find and cut the rest of the path
    match &name.rfind(':') {
        Some(pos) => &name[pos + 1..name.len()],
        None => name,
    }
}

#[cfg(test)]
mod tests {
    use crate::jit::reader::read_as_batch_jit;
    use crate::jit::writer::write_batch_unchecked_jit;
    use crate::layout::RowType::{Compact, WordAligned};
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;
    use datafusion_jit::api::Assembler;
    use std::sync::Arc;
    use DataType::*;

    macro_rules! fn_test_single_type {
        ($ARRAY: ident, $TYPE: expr, $VEC: expr, $ROWTYPE: expr) => {
            paste::item! {
                #[test]
                #[allow(non_snake_case)]
                fn [<test_ $ROWTYPE _single_ $TYPE _jit>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, true)]));
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let assembler = Assembler::default();
                    let row_offsets =
                        { write_batch_unchecked_jit(&mut vector, 0, &batch, 0, schema.clone(), &assembler, $ROWTYPE)? };
                    let output_batch = { read_as_batch_jit(&vector, schema, &row_offsets, &assembler, $ROWTYPE)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }

                #[test]
                #[allow(non_snake_case)]
                fn [<test_ $ROWTYPE _single_ $TYPE _jit_null_free>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, false)]));
                    let v = $VEC.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
                    let a = $ARRAY::from(v);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let assembler = Assembler::default();
                    let row_offsets =
                        { write_batch_unchecked_jit(&mut vector, 0, &batch, 0, schema.clone(), &assembler, $ROWTYPE)? };
                    let output_batch = { read_as_batch_jit(&vector, schema, &row_offsets, &assembler, $ROWTYPE)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }
            }
        };
    }

    fn_test_single_type!(
        BooleanArray,
        Boolean,
        vec![Some(true), Some(false), None, Some(true), None],
        Compact
    );

    fn_test_single_type!(
        BooleanArray,
        Boolean,
        vec![Some(true), Some(false), None, Some(true), None],
        WordAligned
    );

    fn_test_single_type!(
        Int8Array,
        Int8,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Int8Array,
        Int8,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        Int16Array,
        Int16,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Int16Array,
        Int16,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        Int32Array,
        Int32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Int32Array,
        Int32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        Int64Array,
        Int64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Int64Array,
        Int64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        UInt8Array,
        UInt8,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        UInt8Array,
        UInt8,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        UInt16Array,
        UInt16,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        UInt16Array,
        UInt16,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        UInt32Array,
        UInt32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        UInt32Array,
        UInt32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        UInt64Array,
        UInt64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        UInt64Array,
        UInt64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        Float32Array,
        Float32,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)],
        Compact
    );

    fn_test_single_type!(
        Float32Array,
        Float32,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)],
        WordAligned
    );

    fn_test_single_type!(
        Float64Array,
        Float64,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)],
        Compact
    );

    fn_test_single_type!(
        Float64Array,
        Float64,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)],
        WordAligned
    );

    fn_test_single_type!(
        Date32Array,
        Date32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Date32Array,
        Date32,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        Date64Array,
        Date64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        Compact
    );

    fn_test_single_type!(
        Date64Array,
        Date64,
        vec![Some(5), Some(7), None, Some(0), Some(111)],
        WordAligned
    );

    fn_test_single_type!(
        StringArray,
        Utf8,
        vec![Some("hello"), Some("world"), None, Some(""), Some("")],
        Compact
    );

    #[test]
    fn test_single_binary_jit() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, true)]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let a = BinaryArray::from_opt_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let assembler = Assembler::default();
        let row_offsets = {
            write_batch_unchecked_jit(
                &mut vector,
                0,
                &batch,
                0,
                schema.clone(),
                &assembler,
                Compact,
            )?
        };
        let output_batch =
            { read_as_batch_jit(&vector, schema, &row_offsets, &assembler, Compact)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    fn test_single_binary_jit_null_free() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, false)]));
        let values: Vec<&[u8]> = vec![b"one", b"two", b"", b"three"];
        let a = BinaryArray::from_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let assembler = Assembler::default();
        let row_offsets = {
            write_batch_unchecked_jit(
                &mut vector,
                0,
                &batch,
                0,
                schema.clone(),
                &assembler,
                Compact,
            )?
        };
        let output_batch =
            { read_as_batch_jit(&vector, schema, &row_offsets, &assembler, Compact)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }
}
