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

//! Accessing row from raw bytes with JIT

use crate::jit::fn_name;
use crate::layout::RowType;
use crate::reader::RowReader;
use crate::reader::*;
use crate::reg_fn;
use crate::MutableRecordBatch;
use arrow::array::ArrayBuilder;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_jit::api::Assembler;
use datafusion_jit::api::GeneratedFunction;
use datafusion_jit::ast::{I64, PTR};
use std::sync::Arc;

/// Read `data` of raw-bytes rows starting at `offsets` out to a record batch

pub fn read_as_batch_jit(
    data: &[u8],
    schema: Arc<Schema>,
    offsets: &[usize],
    assembler: &Assembler,
    row_type: RowType,
) -> Result<RecordBatch> {
    let row_num = offsets.len();
    let mut output = MutableRecordBatch::new(row_num, schema.clone());
    let mut row = RowReader::new(&schema, row_type);
    register_read_functions(assembler)?;
    let gen_func = gen_read_row(&schema, assembler)?;
    let mut jit = assembler.create_jit();
    let code_ptr = jit.compile(gen_func)?;
    let code_fn = unsafe {
        std::mem::transmute::<_, fn(&RowReader, &mut MutableRecordBatch)>(code_ptr)
    };

    for offset in offsets.iter().take(row_num) {
        row.point_to(*offset, data);
        code_fn(&row, &mut output);
    }

    output.output().map_err(DataFusionError::ArrowError)
}

fn get_array_mut(
    batch: &mut MutableRecordBatch,
    col_idx: usize,
) -> &mut Box<dyn ArrayBuilder> {
    let arrays: &mut [Box<dyn ArrayBuilder>] = batch.arrays.as_mut();
    &mut arrays[col_idx]
}

fn register_read_functions(asm: &Assembler) -> Result<()> {
    let reader_param = vec![PTR, I64, PTR];
    reg_fn!(asm, get_array_mut, vec![PTR, I64], Some(PTR));
    reg_fn!(asm, read_field_bool, reader_param.clone(), None);
    reg_fn!(asm, read_field_u8, reader_param.clone(), None);
    reg_fn!(asm, read_field_u16, reader_param.clone(), None);
    reg_fn!(asm, read_field_u32, reader_param.clone(), None);
    reg_fn!(asm, read_field_u64, reader_param.clone(), None);
    reg_fn!(asm, read_field_i8, reader_param.clone(), None);
    reg_fn!(asm, read_field_i16, reader_param.clone(), None);
    reg_fn!(asm, read_field_i32, reader_param.clone(), None);
    reg_fn!(asm, read_field_i64, reader_param.clone(), None);
    reg_fn!(asm, read_field_f32, reader_param.clone(), None);
    reg_fn!(asm, read_field_f64, reader_param.clone(), None);
    reg_fn!(asm, read_field_date32, reader_param.clone(), None);
    reg_fn!(asm, read_field_date64, reader_param.clone(), None);
    reg_fn!(asm, read_field_utf8, reader_param.clone(), None);
    reg_fn!(asm, read_field_binary, reader_param.clone(), None);
    reg_fn!(asm, read_field_bool_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_u8_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_u16_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_u32_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_u64_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_i8_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_i16_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_i32_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_i64_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_f32_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_f64_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_date32_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_date64_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_utf8_null_free, reader_param.clone(), None);
    reg_fn!(asm, read_field_binary_null_free, reader_param, None);
    Ok(())
}

fn gen_read_row(schema: &Schema, assembler: &Assembler) -> Result<GeneratedFunction> {
    use DataType::*;
    let mut builder = assembler
        .new_func_builder("read_row")
        .param("row", PTR)
        .param("batch", PTR);
    let mut b = builder.enter_block();
    for (i, f) in schema.fields().iter().enumerate() {
        let dt = f.data_type();
        let arr = format!("a{}", i);
        b.declare_as(
            &arr,
            b.call("get_array_mut", vec![b.id("batch")?, b.lit_i(i as i64)])?,
        )?;
        let params = vec![b.id(&arr)?, b.lit_i(i as i64), b.id("row")?];
        if f.is_nullable() {
            match dt {
                Boolean => b.call_stmt("read_field_bool", params)?,
                UInt8 => b.call_stmt("read_field_u8", params)?,
                UInt16 => b.call_stmt("read_field_u16", params)?,
                UInt32 => b.call_stmt("read_field_u32", params)?,
                UInt64 => b.call_stmt("read_field_u64", params)?,
                Int8 => b.call_stmt("read_field_i8", params)?,
                Int16 => b.call_stmt("read_field_i16", params)?,
                Int32 => b.call_stmt("read_field_i32", params)?,
                Int64 => b.call_stmt("read_field_i64", params)?,
                Float32 => b.call_stmt("read_field_f32", params)?,
                Float64 => b.call_stmt("read_field_f64", params)?,
                Date32 => b.call_stmt("read_field_date32", params)?,
                Date64 => b.call_stmt("read_field_date64", params)?,
                Utf8 => b.call_stmt("read_field_utf8", params)?,
                Binary => b.call_stmt("read_field_binary", params)?,
                _ => unimplemented!(),
            }
        } else {
            match dt {
                Boolean => b.call_stmt("read_field_bool_null_free", params)?,
                UInt8 => b.call_stmt("read_field_u8_null_free", params)?,
                UInt16 => b.call_stmt("read_field_u16_null_free", params)?,
                UInt32 => b.call_stmt("read_field_u32_null_free", params)?,
                UInt64 => b.call_stmt("read_field_u64_null_free", params)?,
                Int8 => b.call_stmt("read_field_i8_null_free", params)?,
                Int16 => b.call_stmt("read_field_i16_null_free", params)?,
                Int32 => b.call_stmt("read_field_i32_null_free", params)?,
                Int64 => b.call_stmt("read_field_i64_null_free", params)?,
                Float32 => b.call_stmt("read_field_f32_null_free", params)?,
                Float64 => b.call_stmt("read_field_f64_null_free", params)?,
                Date32 => b.call_stmt("read_field_date32_null_free", params)?,
                Date64 => b.call_stmt("read_field_date64_null_free", params)?,
                Utf8 => b.call_stmt("read_field_utf8_null_free", params)?,
                Binary => b.call_stmt("read_field_binary_null_free", params)?,
                _ => unimplemented!(),
            }
        }
    }
    Ok(b.build())
}
