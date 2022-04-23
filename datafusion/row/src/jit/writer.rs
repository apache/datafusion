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

//! Reusable JIT version of row writer backed by Vec<u8> to stitch attributes together

use crate::jit::fn_name;
use crate::layout::RowType;
use crate::reg_fn;
use crate::schema_null_free;
use crate::writer::RowWriter;
use crate::writer::*;
use arrow::array::Array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_jit::api::CodeBlock;
use datafusion_jit::api::{Assembler, GeneratedFunction};
use datafusion_jit::ast::Expr;
use datafusion_jit::ast::{BOOL, I64, PTR};
use std::sync::Arc;

/// Append batch from `row_idx` to `output` buffer start from `offset`
/// # Panics
///
/// This function will panic if the output buffer doesn't have enough space to hold all the rows
pub fn write_batch_unchecked_jit(
    output: &mut [u8],
    offset: usize,
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Schema>,
    assembler: &Assembler,
    row_type: RowType,
) -> Result<Vec<usize>> {
    let mut writer = RowWriter::new(&schema, row_type);
    let mut current_offset = offset;
    let mut offsets = vec![];
    register_write_functions(assembler)?;
    let gen_func = gen_write_row(&schema, assembler)?;
    let mut jit = assembler.create_jit();
    let code_ptr = jit.compile(gen_func)?;

    let code_fn = unsafe {
        std::mem::transmute::<_, fn(&mut RowWriter, usize, &RecordBatch)>(code_ptr)
    };

    for cur_row in row_idx..batch.num_rows() {
        offsets.push(current_offset);
        code_fn(&mut writer, cur_row, batch);
        writer.end_padding();
        let row_width = writer.row_width;
        output[current_offset..current_offset + row_width]
            .copy_from_slice(writer.get_row());
        current_offset += row_width;
        writer.reset()
    }
    Ok(offsets)
}

/// bench jit version write
#[inline(never)]
pub fn bench_write_batch_jit(
    batches: &[Vec<RecordBatch>],
    schema: Arc<Schema>,
    row_type: RowType,
) -> Result<Vec<usize>> {
    let assembler = Assembler::default();
    let mut writer = RowWriter::new(&schema, row_type);
    let mut lengths = vec![];
    register_write_functions(&assembler)?;
    let gen_func = gen_write_row(&schema, &assembler)?;
    let mut jit = assembler.create_jit();
    let code_ptr = jit.compile(gen_func)?;
    let code_fn = unsafe {
        std::mem::transmute::<_, fn(&mut RowWriter, usize, &RecordBatch)>(code_ptr)
    };

    for batch in batches.iter().flatten() {
        for cur_row in 0..batch.num_rows() {
            code_fn(&mut writer, cur_row, batch);
            writer.end_padding();
            lengths.push(writer.row_width);
            writer.reset()
        }
    }
    Ok(lengths)
}

// we could remove this function wrapper once we find a way to call the trait method directly.
fn is_null(col: &Arc<dyn Array>, row_idx: usize) -> bool {
    col.is_null(row_idx)
}

fn register_write_functions(asm: &Assembler) -> Result<()> {
    let reader_param = vec![PTR, I64, PTR];
    reg_fn!(asm, RecordBatch::column, vec![PTR, I64], Some(PTR));
    reg_fn!(asm, RowWriter::set_null_at, vec![PTR, I64], None);
    reg_fn!(asm, RowWriter::set_non_null_at, vec![PTR, I64], None);
    reg_fn!(asm, is_null, vec![PTR, I64], Some(BOOL));
    reg_fn!(asm, write_field_bool, reader_param.clone(), None);
    reg_fn!(asm, write_field_u8, reader_param.clone(), None);
    reg_fn!(asm, write_field_u16, reader_param.clone(), None);
    reg_fn!(asm, write_field_u32, reader_param.clone(), None);
    reg_fn!(asm, write_field_u64, reader_param.clone(), None);
    reg_fn!(asm, write_field_i8, reader_param.clone(), None);
    reg_fn!(asm, write_field_i16, reader_param.clone(), None);
    reg_fn!(asm, write_field_i32, reader_param.clone(), None);
    reg_fn!(asm, write_field_i64, reader_param.clone(), None);
    reg_fn!(asm, write_field_f32, reader_param.clone(), None);
    reg_fn!(asm, write_field_f64, reader_param.clone(), None);
    reg_fn!(asm, write_field_date32, reader_param.clone(), None);
    reg_fn!(asm, write_field_date64, reader_param.clone(), None);
    reg_fn!(asm, write_field_utf8, reader_param.clone(), None);
    reg_fn!(asm, write_field_binary, reader_param, None);
    Ok(())
}

fn gen_write_row(schema: &Schema, assembler: &Assembler) -> Result<GeneratedFunction> {
    let mut builder = assembler
        .new_func_builder("write_row")
        .param("row", PTR)
        .param("row_idx", I64)
        .param("batch", PTR);
    let null_free = schema_null_free(schema);
    let mut b = builder.enter_block();
    for (i, f) in schema.fields().iter().enumerate() {
        let dt = f.data_type();
        let arr = format!("a{}", i);
        b.declare_as(
            &arr,
            b.call("column", vec![b.id("batch")?, b.lit_i(i as i64)])?,
        )?;
        if f.is_nullable() {
            b.if_block(
                |c| c.call("is_null", vec![c.id(&arr)?, c.id("row_idx")?]),
                |t| {
                    t.call_stmt("set_null_at", vec![t.id("row")?, t.lit_i(i as i64)])?;
                    Ok(())
                },
                |e| {
                    e.call_stmt(
                        "set_non_null_at",
                        vec![e.id("row")?, e.lit_i(i as i64)],
                    )?;
                    let params = vec![
                        e.id("row")?,
                        e.id(&arr)?,
                        e.lit_i(i as i64),
                        e.id("row_idx")?,
                    ];
                    write_typed_field_stmt(dt, e, params)?;
                    Ok(())
                },
            )?;
        } else {
            if !null_free {
                b.call_stmt("set_non_null_at", vec![b.id("row")?, b.lit_i(i as i64)])?;
            }
            let params = vec![
                b.id("row")?,
                b.id(&arr)?,
                b.lit_i(i as i64),
                b.id("row_idx")?,
            ];
            write_typed_field_stmt(dt, &mut b, params)?;
        }
    }
    Ok(b.build())
}

fn write_typed_field_stmt<'a>(
    dt: &DataType,
    b: &mut CodeBlock<'a>,
    params: Vec<Expr>,
) -> Result<()> {
    use DataType::*;
    match dt {
        Boolean => b.call_stmt("write_field_bool", params)?,
        UInt8 => b.call_stmt("write_field_u8", params)?,
        UInt16 => b.call_stmt("write_field_u16", params)?,
        UInt32 => b.call_stmt("write_field_u32", params)?,
        UInt64 => b.call_stmt("write_field_u64", params)?,
        Int8 => b.call_stmt("write_field_i8", params)?,
        Int16 => b.call_stmt("write_field_i16", params)?,
        Int32 => b.call_stmt("write_field_i32", params)?,
        Int64 => b.call_stmt("write_field_i64", params)?,
        Float32 => b.call_stmt("write_field_f32", params)?,
        Float64 => b.call_stmt("write_field_f64", params)?,
        Date32 => b.call_stmt("write_field_date32", params)?,
        Date64 => b.call_stmt("write_field_date64", params)?,
        Utf8 => b.call_stmt("write_field_utf8", params)?,
        Binary => b.call_stmt("write_field_binary", params)?,
        _ => unimplemented!(),
    }
    Ok(())
}
