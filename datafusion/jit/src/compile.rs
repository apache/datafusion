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

//! Compile DataFusion Expr to JIT'd function.

use std::mem;
use std::sync::Arc;

use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::{Float64Type, Int64Type};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::logical_plan::Projection;
use datafusion_expr::{Expr, LogicalPlan};

use crate::api::{Assembler, CodeBlock, FunctionBuilder};
use crate::ast::{JITType, I32};
use crate::{
    api::GeneratedFunction,
    ast::{Expr as JITExpr, I64, PTR_SIZE},
};

// Alias pointer type.
// The raw pointer `R64` or `R32` is not compatible with integers.
const PTR_TYPE: JITType = if PTR_SIZE == 8 { I64 } else { I32 };

type JITFunction = fn(*const i64, *const i64, i64) -> ();

/// Wrap JIT Expr to array compute function.
pub fn build_calc_fn(
    assembler: &Assembler,
    jit_expr: JITExpr,
    inputs: Vec<(String, JITType)>,
    ret_type: JITType,
) -> Result<GeneratedFunction> {
    let mut builder = assembler.new_func_builder("calc_fn");
    // Declare in-param.
    // Each input takes one position, following by a pointer to place result,
    // and the last is the length of inputs/output arrays.
    for (name, _) in &inputs {
        builder = builder.param(format!("{}_array", name), PTR_TYPE);
    }
    let builder = builder.param("result", ret_type).param("len", I64);

    // Start build function body.
    // It's loop that calculates the result one by one.
    let mut fn_body = builder.enter_block();
    fn_body.declare_as("index", fn_body.lit_i(0))?;
    fn_body.while_block(
        |cond| cond.lt(cond.id("index")?, cond.id("len")?),
        |w| {
            w.declare_as("offset", w.mul(w.id("index")?, w.lit_i(PTR_SIZE as i64))?)?;
            for (name, ty) in &inputs {
                w.declare_as(
                    format!("{}_ptr", name),
                    w.add(w.id(format!("{}_array", name))?, w.id("offset")?)?,
                )?;
                w.declare_as(name, w.load(w.id(format!("{}_ptr", name))?, *ty)?)?;
            }
            w.declare_as("res_ptr", w.add(w.id("result")?, w.id("offset")?)?)?;
            w.declare_as("res", jit_expr.clone())?;
            w.store(w.id("res")?, w.id("res_ptr")?)?;

            w.assign("index", w.add(w.id("index")?, w.lit_i(1))?)?;
            Ok(())
        },
    )?;

    let gen_func = fn_body.build();
    Ok(gen_func)
}

#[derive(Default)]
pub struct JITContext {
    assembler: Assembler,
    fn_state: Option<FnState>,
}

impl JITContext {
    pub fn compile_logical_plan(
        &mut self,
        logical_plan: LogicalPlan,
    ) -> Result<JITExecutionPlan> {
        match logical_plan {
            LogicalPlan::Projection(projection) => {
                let Projection {
                    expr: exprs,
                    input,
                    schema: output_schema,
                    alias: _alias,
                } = projection;

                let gen_fn = self.generate_fn(
                    exprs,
                    input.schema().clone(),
                    output_schema.clone(),
                )?;

                let fn_ptr =
                    unsafe { mem::transmute::<_, JITFunction>(self.compile(gen_fn)?) };
                let exec_plan = JITExecutionPlan {
                    fn_ptr,
                    output_schema,
                };
                Ok(exec_plan)
            }
            plan => Err(DataFusionError::NotImplemented(format!(
                "Compiling {:?} is not yet supported",
                plan
            ))),
        }
    }

    /// Generate function IR for a group of exprs
    fn generate_fn(
        &mut self,
        exprs: Vec<Expr>,
        input_schema: DFSchemaRef,
        output_schema: DFSchemaRef,
    ) -> Result<GeneratedFunction> {
        let input_fields = collect_jit_type(&input_schema)?;
        let output_fields = collect_jit_type(&output_schema)?;

        let jit_exprs = exprs
            .iter()
            .map(|expr| JITExpr::try_from((expr.clone(), input_schema.clone())))
            .collect::<Result<Vec<_>>>()?;

        self.start_fn(input_fields, output_fields)?;
        for (index, expr) in jit_exprs.into_iter().enumerate() {
            self.build_expr_block(expr, index)?;
        }
        let gen_fn = self.finish_fn()?;

        Ok(gen_fn)
    }
}

pub struct FnState {
    fn_body: CodeBlock,
    in_field: Vec<(String, JITType)>,
    out_field: Vec<(String, JITType)>,
}

impl FnState {
    fn new(
        builder: FunctionBuilder,
        in_field: Vec<(String, JITType)>,
        out_field: Vec<(String, JITType)>,
    ) -> Self {
        Self {
            fn_body: builder.enter_block(),
            in_field,
            out_field,
        }
    }
}

impl JITContext {
    /// The generated function will have signature like
    /// ```rust, ignore
    /// fn calc_fn(
    ///     input_arrays: *const *const Array,
    ///     output_arrays: *const *const Array,
    ///     length: i64, // row count
    /// );
    /// ```
    fn start_fn(
        &mut self,
        in_field: Vec<(String, JITType)>,
        out_field: Vec<(String, JITType)>,
    ) -> Result<()> {
        if self.fn_state.is_some() {
            return Err(DataFusionError::Internal(
                "Function construction already started".to_string(),
            ));
        }

        let mut builder = self.assembler.new_func_builder("calc_fn");

        // declare fn sig
        builder = builder
            .param("input_arrays", PTR_TYPE)
            .param("output_arrays", PTR_TYPE)
            .param("length", I64);
        self.fn_state = Some(FnState::new(builder, in_field, out_field));

        Ok(())
    }

    /// Build one loop for an expr. The index is the position of this expr
    /// and is used to offset result column.
    ///
    /// The generated loop block contains three parts:
    /// - retrieve data from pointer (`*const *const Array`)
    /// - evaluate expr
    /// - store result to output array
    fn build_expr_block(&mut self, expr: JITExpr, index: usize) -> Result<()> {
        // retrieve fn state
        let fn_state = match self.fn_state.as_mut() {
            Some(builder) => builder,
            None => {
                return Err(DataFusionError::Internal(
                    "Function construction is not started".to_string(),
                ));
            }
        };

        let (res_name, _) = fn_state.out_field[index].clone();

        // build one calculation loop for expression.
        let calc_block = &mut fn_state.fn_body;
        calc_block.declare_as("index", calc_block.lit_i(0))?;
        calc_block.while_block(
            |cond| cond.lt(cond.id("index")?, cond.id("length")?),
            |w| {
                // declare index variable
                w.declare_as("offset", w.mul(w.id("index")?, w.lit_i(PTR_SIZE as i64))?)?;

                // transform input pointers
                w.declare_as("array_index", w.lit_i(0))?;
                w.declare_as("array_offset", w.lit_i(0))?;
                w.declare_as("array_ptr", w.lit_i(0))?;
                for (name, ty) in &fn_state.in_field {
                    w.assign(
                        "array_offset",
                        w.mul(w.id("array_index")?, w.lit_i(PTR_SIZE as i64))?,
                    )?;
                    w.assign(
                        "array_ptr",
                        w.add(w.id("input_arrays")?, w.id("array_offset")?)?,
                    )?;
                    w.assign("array_ptr", w.load(w.id("array_ptr")?, PTR_TYPE)?)?;
                    w.declare_as(
                        format!("{}_ptr", name),
                        w.add(w.id("array_ptr")?, w.id("offset")?)?,
                    )?;
                    w.declare_as(name, w.load(w.id(format!("{}_ptr", name))?, *ty)?)?;
                    w.assign("array_index", w.add(w.id("array_index")?, w.lit_i(1))?)?;
                }

                // declare result pointer
                let res_array_offset = index * PTR_SIZE;
                w.declare_as(
                    format!("{}_array", res_name),
                    w.add(w.id("output_arrays")?, w.lit_i(res_array_offset as i64))?,
                )?;
                w.assign(
                    format!("{}_array", res_name),
                    w.load(w.id(format!("{}_array", res_name))?, PTR_TYPE)?,
                )?;
                w.declare_as(
                    format!("{}_ptr", res_name),
                    w.add(w.id(format!("{}_array", res_name))?, w.id("offset")?)?,
                )?;

                // evaluate expr
                w.declare_as(&res_name, expr.clone())?;

                // store result
                w.store(w.id(&res_name)?, w.id(format!("{}_ptr", res_name))?)?;

                // step index
                w.assign("index", w.add(w.id("index")?, w.lit_i(1))?)?;
                Ok(())
            },
        )?;

        Ok(())
    }

    fn finish_fn(&mut self) -> Result<GeneratedFunction> {
        let fn_state = match self.fn_state.as_mut() {
            Some(builder) => builder,
            None => {
                return Err(DataFusionError::Internal(
                    "Function construction is not started".to_string(),
                ));
            }
        };
        let gen_fn = fn_state.fn_body.build();
        self.fn_state = None;
        Ok(gen_fn)
    }

    fn compile(&self, gen_fn: GeneratedFunction) -> Result<*const u8> {
        let mut jit = self.assembler.create_jit();
        jit.compile(gen_fn)
    }
}

/// A JIT compiled execution plan.
///
/// Currently only a minimal [execute] interface is implemented.
pub struct JITExecutionPlan {
    fn_ptr: JITFunction,
    output_schema: DFSchemaRef,
}

impl JITExecutionPlan {
    /// Execute on input batch.
    ///
    /// Partitioning and task context are not supported yet.
    pub fn execute(
        &self,
        // partition: usize,
        input: RecordBatch,
        // context: Arc<TaskContext>,
    ) -> Result<RecordBatch> {
        self.check_input(&input)?;

        let input_pointers = self.convert_input_record_batch(&input)?;
        let length = input.num_rows();
        let output_pointers = self.alloc_output_array(&self.output_schema, length)?;

        // involve function
        (self.fn_ptr)(
            input_pointers.as_ptr() as _,
            output_pointers.as_ptr() as _,
            length as i64,
        );

        let result = self.convert_output_record_batch(
            output_pointers,
            &self.output_schema,
            length,
        )?;

        Ok(result)
    }

    fn check_input(&self, input: &RecordBatch) -> Result<()> {
        for column in input.columns() {
            if column.null_count() != 0 {
                return Err(DataFusionError::NotImplemented(
                    "Computing on nullable array not yet supported".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Make a pointer vector of input [RecordBatch]. Currently only support primitive arrays.
    fn convert_input_record_batch(&self, batch: &RecordBatch) -> Result<Vec<*const ()>> {
        let mut result = Vec::with_capacity(batch.num_columns());

        for array in batch.columns() {
            let values_ptr = match array.data_type() {
                arrow::datatypes::DataType::Int64 => array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap()
                    .values()
                    .as_ptr()
                    as *const (),
                arrow::datatypes::DataType::Float64 => array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap()
                    .values()
                    .as_ptr()
                    as *const (),

                arrow::datatypes::DataType::Null
                | arrow::datatypes::DataType::Boolean
                | arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32
                | arrow::datatypes::DataType::UInt64
                | arrow::datatypes::DataType::Float16
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Timestamp(_, _)
                | arrow::datatypes::DataType::Date32
                | arrow::datatypes::DataType::Date64
                | arrow::datatypes::DataType::Time32(_)
                | arrow::datatypes::DataType::Time64(_)
                | arrow::datatypes::DataType::Duration(_)
                | arrow::datatypes::DataType::Interval(_)
                | arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::FixedSizeBinary(_)
                | arrow::datatypes::DataType::LargeBinary
                | arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::List(_)
                | arrow::datatypes::DataType::FixedSizeList(_, _)
                | arrow::datatypes::DataType::LargeList(_)
                | arrow::datatypes::DataType::Struct(_)
                | arrow::datatypes::DataType::Union(_, _, _)
                | arrow::datatypes::DataType::Dictionary(_, _)
                | arrow::datatypes::DataType::Decimal(_, _)
                | arrow::datatypes::DataType::Map(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {}",
                        array.data_type()
                    )));
                }
            };
            result.push(values_ptr)
        }

        Ok(result)
    }

    /// Preallocate output arrays.
    ///
    /// This method will allocate n vectors (number of schema's fields), with `length`
    /// elements each. And return their pointers (of data part). The allocated memory
    /// will be "leak" via [Vec::from_raw_parts]. And needs to be retrieved via
    /// [convert_output_record_batch].
    fn alloc_output_array(
        &self,
        schema: &DFSchemaRef,
        length: usize,
    ) -> Result<Vec<*const ()>> {
        let mut pointers = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let buffer_pointer = match field.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let buf = Vec::<i64>::with_capacity(length);
                    let ptr = buf.as_ptr() as _;
                    mem::forget(buf);
                    ptr
                }
                arrow::datatypes::DataType::UInt64 => {
                    let buf = Vec::<u64>::with_capacity(length);
                    let ptr = buf.as_ptr() as _;
                    mem::forget(buf);
                    ptr
                }
                arrow::datatypes::DataType::Float64 => {
                    let buf = Vec::<f64>::with_capacity(length);
                    let ptr = buf.as_ptr() as _;
                    mem::forget(buf);
                    ptr
                }
                arrow::datatypes::DataType::Null
                | arrow::datatypes::DataType::Boolean
                | arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32
                | arrow::datatypes::DataType::Float16
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Timestamp(_, _)
                | arrow::datatypes::DataType::Date32
                | arrow::datatypes::DataType::Date64
                | arrow::datatypes::DataType::Time32(_)
                | arrow::datatypes::DataType::Time64(_)
                | arrow::datatypes::DataType::Duration(_)
                | arrow::datatypes::DataType::Interval(_)
                | arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::FixedSizeBinary(_)
                | arrow::datatypes::DataType::LargeBinary
                | arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::List(_)
                | arrow::datatypes::DataType::FixedSizeList(_, _)
                | arrow::datatypes::DataType::LargeList(_)
                | arrow::datatypes::DataType::Struct(_)
                | arrow::datatypes::DataType::Union(_, _, _)
                | arrow::datatypes::DataType::Dictionary(_, _)
                | arrow::datatypes::DataType::Decimal(_, _)
                | arrow::datatypes::DataType::Map(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {}",
                        field.data_type()
                    )));
                }
            };
            pointers.push(buffer_pointer);
        }

        Ok(pointers)
    }

    /// Read and deallocate result arrays from `pointers`, which is allocated by
    /// [alloc_output_array] and has guaranteed memories. The content would be default
    /// value if it's not set by calculation program.
    fn convert_output_record_batch(
        &self,
        pointers: Vec<*const ()>,
        schema: &DFSchemaRef,
        length: usize,
    ) -> Result<RecordBatch> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

        for (pointer, field) in pointers.into_iter().zip(schema.fields().iter()) {
            let array: Arc<dyn Array> = match field.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let vec = unsafe {
                        Vec::<i64>::from_raw_parts(pointer as _, length, length)
                    };
                    Arc::new(PrimitiveArray::from_iter(vec.into_iter()))
                }
                arrow::datatypes::DataType::UInt64 => {
                    let vec = unsafe {
                        Vec::<u64>::from_raw_parts(pointer as _, length, length)
                    };
                    Arc::new(PrimitiveArray::from_iter(vec.into_iter()))
                }
                arrow::datatypes::DataType::Float64 => {
                    let vec = unsafe {
                        Vec::<f64>::from_raw_parts(pointer as _, length, length)
                    };
                    Arc::new(PrimitiveArray::from_iter(vec.into_iter()))
                }
                arrow::datatypes::DataType::Null
                | arrow::datatypes::DataType::Boolean
                | arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32
                | arrow::datatypes::DataType::Float16
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Timestamp(_, _)
                | arrow::datatypes::DataType::Date32
                | arrow::datatypes::DataType::Date64
                | arrow::datatypes::DataType::Time32(_)
                | arrow::datatypes::DataType::Time64(_)
                | arrow::datatypes::DataType::Duration(_)
                | arrow::datatypes::DataType::Interval(_)
                | arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::FixedSizeBinary(_)
                | arrow::datatypes::DataType::LargeBinary
                | arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::List(_)
                | arrow::datatypes::DataType::FixedSizeList(_, _)
                | arrow::datatypes::DataType::LargeList(_)
                | arrow::datatypes::DataType::Struct(_)
                | arrow::datatypes::DataType::Union(_, _, _)
                | arrow::datatypes::DataType::Dictionary(_, _)
                | arrow::datatypes::DataType::Decimal(_, _)
                | arrow::datatypes::DataType::Map(_, _) => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type {}",
                        field.data_type()
                    )));
                }
            };
            arrays.push(array);
        }

        let schema: DFSchema = (schema.as_ref()).clone();
        let batch = RecordBatch::try_new(schema.try_into().unwrap(), arrays)?;

        Ok(batch)
    }
}

fn collect_jit_type(schema: &DFSchema) -> Result<Vec<(String, JITType)>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Ok((
                field.qualified_name(),
                JITType::try_from(field.data_type())?,
            ))
        })
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::Array,
        compute::kernels::arithmetic,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_common::{DFField, DFSchema};
    use datafusion_expr::{logical_plan::table_scan, Expr as DFExpr};

    use crate::ast::BinaryExpr;

    use super::*;

    fn run_df_expr(
        df_expr: DFExpr,
        schema: Arc<DFSchema>,
        lhs: PrimitiveArray<Int64Type>,
        rhs: PrimitiveArray<Int64Type>,
    ) -> Result<PrimitiveArray<Int64Type>> {
        if lhs.null_count() != 0 || rhs.null_count() != 0 {
            return Err(DataFusionError::NotImplemented(
                "Computing on nullable array not yet supported".to_string(),
            ));
        }
        if lhs.len() != rhs.len() {
            return Err(DataFusionError::NotImplemented(
                "Computing on different length arrays not yet supported".to_string(),
            ));
        }

        // translate DF Expr to JIT Expr
        let input_fields = schema
            .fields()
            .iter()
            .map(|field| {
                Ok((
                    field.qualified_name(),
                    JITType::try_from(field.data_type())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let jit_expr: JITExpr = (df_expr, schema).try_into()?;

        // allocate memory for calc result
        let len = lhs.len();
        let result = vec![0i64; len];

        // compile and run JIT code
        let assembler = Assembler::default();
        let gen_func = build_calc_fn(&assembler, jit_expr, input_fields, I64)?;
        let mut jit = assembler.create_jit();
        let code_ptr = jit.compile(gen_func)?;
        let code_fn = unsafe {
            mem::transmute::<_, fn(*const i64, *const i64, *const i64, i64) -> ()>(
                code_ptr,
            )
        };
        code_fn(
            lhs.values().as_ptr(),
            rhs.values().as_ptr(),
            result.as_ptr(),
            len as i64,
        );

        let result_array = PrimitiveArray::<Int64Type>::from_iter(result);
        Ok(result_array)
    }

    #[test]
    fn array_add() {
        let array_a: PrimitiveArray<Int64Type> =
            PrimitiveArray::from_iter_values((0..10).map(|x| x + 1));
        let array_b: PrimitiveArray<Int64Type> =
            PrimitiveArray::from_iter_values((10..20).map(|x| x + 1));
        let expected = arithmetic::add(&array_a, &array_b).unwrap();

        let df_expr = datafusion_expr::col("a") + datafusion_expr::col("b");
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("table1"), "a", DataType::Int64, false),
                    DFField::new(Some("table1"), "b", DataType::Int64, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );

        let result = run_df_expr(df_expr, schema, array_a, array_b).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn calc_fn_builder() {
        let expr = JITExpr::Binary(BinaryExpr::Add(
            Box::new(JITExpr::Identifier("table1.a".to_string(), I64)),
            Box::new(JITExpr::Identifier("table1.b".to_string(), I64)),
        ));
        let fields = vec![("table1.a".to_string(), I64), ("table1.b".to_string(), I64)];

        let expected = r#"fn calc_fn_0(table1.a_array: i64, table1.b_array: i64, result: i64, len: i64) -> () {
    let index: i64;
    index = 0;
    while index < len {
        let offset: i64;
        offset = index * 8;
        let table1.a_ptr: i64;
        table1.a_ptr = table1.a_array + offset;
        let table1.a: i64;
        table1.a = *(table1.a_ptr);
        let table1.b_ptr: i64;
        table1.b_ptr = table1.b_array + offset;
        let table1.b: i64;
        table1.b = *(table1.b_ptr);
        let res_ptr: i64;
        res_ptr = result + offset;
        let res: i64;
        res = table1.a + table1.b;
        *(res_ptr) = res
        index = index + 1;
    }
}"#;

        let assembler = Assembler::default();
        let gen_func = build_calc_fn(&assembler, expr, fields, I64).unwrap();
        assert_eq!(format!("{}", &gen_func), expected);
    }

    #[test]
    fn generate_compute_function() {
        let input_schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "a", DataType::Int64, false),
                    DFField::new(None, "b", DataType::Int64, false),
                    DFField::new(None, "c", DataType::Float64, false),
                    DFField::new(None, "d", DataType::Float64, false),
                    DFField::new(None, "e", DataType::Int64, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );
        let output_schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("result"), "a", DataType::Int64, false),
                    DFField::new(Some("result"), "b", DataType::Float64, false),
                    DFField::new(Some("result"), "c", DataType::Int64, false),
                    DFField::new(Some("result"), "d", DataType::Int64, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );
        let exprs = vec![
            datafusion_expr::col("a") + datafusion_expr::col("b"),
            datafusion_expr::col("c") + datafusion_expr::col("d"),
            datafusion_expr::col("e") - datafusion_expr::col("a"),
            datafusion_expr::col("e") * datafusion_expr::col("b"),
        ];

        // The generated code contains four loops (each expr has one).
        // The loop will computes result rows one by one. It will first retrieve
        // every value in the input columns (in this example we have five columns).
        // Then evaluate the expr and store its result.
        //
        // The generated code has lots of unnecessary lines. Mainly the data retrieve
        // parts. It will dereference every column every time regardless whether it is need.
        let expected = r#"fn calc_fn_0(input_arrays: i64, output_arrays: i64, length: i64) -> () {
    let index: i64;
    index = 0;
    while index < length {
        let offset: i64;
        offset = index * 8;
        let array_index: i64;
        array_index = 0;
        let array_offset: i64;
        array_offset = 0;
        let array_ptr: i64;
        array_ptr = 0;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let a_ptr: i64;
        a_ptr = array_ptr + offset;
        let a: i64;
        a = *(a_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let b_ptr: i64;
        b_ptr = array_ptr + offset;
        let b: i64;
        b = *(b_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let c_ptr: i64;
        c_ptr = array_ptr + offset;
        let c: f64;
        c = *(c_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let d_ptr: i64;
        d_ptr = array_ptr + offset;
        let d: f64;
        d = *(d_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let e_ptr: i64;
        e_ptr = array_ptr + offset;
        let e: i64;
        e = *(e_ptr);
        array_index = array_index + 1;
        let result.a_array: i64;
        result.a_array = output_arrays + 0;
        result.a_array = *(result.a_array);
        let result.a_ptr: i64;
        result.a_ptr = result.a_array + offset;
        let result.a: i64;
        result.a = a + b;
        *(result.a_ptr) = result.a
        index = index + 1;
    }
    let index: i64;
    index = 0;
    while index < length {
        let offset: i64;
        offset = index * 8;
        let array_index: i64;
        array_index = 0;
        let array_offset: i64;
        array_offset = 0;
        let array_ptr: i64;
        array_ptr = 0;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let a_ptr: i64;
        a_ptr = array_ptr + offset;
        let a: i64;
        a = *(a_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let b_ptr: i64;
        b_ptr = array_ptr + offset;
        let b: i64;
        b = *(b_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let c_ptr: i64;
        c_ptr = array_ptr + offset;
        let c: f64;
        c = *(c_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let d_ptr: i64;
        d_ptr = array_ptr + offset;
        let d: f64;
        d = *(d_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let e_ptr: i64;
        e_ptr = array_ptr + offset;
        let e: i64;
        e = *(e_ptr);
        array_index = array_index + 1;
        let result.b_array: i64;
        result.b_array = output_arrays + 8;
        result.b_array = *(result.b_array);
        let result.b_ptr: i64;
        result.b_ptr = result.b_array + offset;
        let result.b: f64;
        result.b = c + d;
        *(result.b_ptr) = result.b
        index = index + 1;
    }
    let index: i64;
    index = 0;
    while index < length {
        let offset: i64;
        offset = index * 8;
        let array_index: i64;
        array_index = 0;
        let array_offset: i64;
        array_offset = 0;
        let array_ptr: i64;
        array_ptr = 0;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let a_ptr: i64;
        a_ptr = array_ptr + offset;
        let a: i64;
        a = *(a_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let b_ptr: i64;
        b_ptr = array_ptr + offset;
        let b: i64;
        b = *(b_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let c_ptr: i64;
        c_ptr = array_ptr + offset;
        let c: f64;
        c = *(c_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let d_ptr: i64;
        d_ptr = array_ptr + offset;
        let d: f64;
        d = *(d_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let e_ptr: i64;
        e_ptr = array_ptr + offset;
        let e: i64;
        e = *(e_ptr);
        array_index = array_index + 1;
        let result.c_array: i64;
        result.c_array = output_arrays + 16;
        result.c_array = *(result.c_array);
        let result.c_ptr: i64;
        result.c_ptr = result.c_array + offset;
        let result.c: i64;
        result.c = e - a;
        *(result.c_ptr) = result.c
        index = index + 1;
    }
    let index: i64;
    index = 0;
    while index < length {
        let offset: i64;
        offset = index * 8;
        let array_index: i64;
        array_index = 0;
        let array_offset: i64;
        array_offset = 0;
        let array_ptr: i64;
        array_ptr = 0;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let a_ptr: i64;
        a_ptr = array_ptr + offset;
        let a: i64;
        a = *(a_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let b_ptr: i64;
        b_ptr = array_ptr + offset;
        let b: i64;
        b = *(b_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let c_ptr: i64;
        c_ptr = array_ptr + offset;
        let c: f64;
        c = *(c_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let d_ptr: i64;
        d_ptr = array_ptr + offset;
        let d: f64;
        d = *(d_ptr);
        array_index = array_index + 1;
        array_offset = array_index * 8;
        array_ptr = input_arrays + array_offset;
        array_ptr = *(array_ptr);
        let e_ptr: i64;
        e_ptr = array_ptr + offset;
        let e: i64;
        e = *(e_ptr);
        array_index = array_index + 1;
        let result.d_array: i64;
        result.d_array = output_arrays + 24;
        result.d_array = *(result.d_array);
        let result.d_ptr: i64;
        result.d_ptr = result.d_array + offset;
        let result.d: i64;
        result.d = e * b;
        *(result.d_ptr) = result.d
        index = index + 1;
    }
}"#;

        // compile logical plan into JIT execution plan
        let mut jit_ctx = JITContext::default();
        let gen_fn = jit_ctx
            .generate_fn(exprs, input_schema, output_schema)
            .unwrap();
        assert_eq!(format!("{}", &gen_fn), expected);
    }

    #[test]
    fn exec_logical_project_plan() {
        // prepare data
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
            Field::new("e", DataType::Int64, false),
        ]));
        let output_schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(Some("result"), "a", DataType::Int64, false),
                    DFField::new(Some("result"), "b", DataType::Float64, false),
                    DFField::new(Some("result"), "c", DataType::Int64, false),
                    DFField::new(Some("result"), "d", DataType::Int64, false),
                ],
                HashMap::new(),
            )
            .unwrap(),
        );
        let input_batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(0..10)) as _,
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(10..20)) as _,
                Arc::new(PrimitiveArray::<Float64Type>::from_iter_values(
                    (20..30).map(|x| x as f64),
                )) as _,
                Arc::new(PrimitiveArray::<Float64Type>::from_iter_values(
                    (30..40).map(|x| x as f64),
                )) as _,
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(40..50)) as _,
            ],
        )
        .unwrap();
        let exprs = vec![
            datafusion_expr::col("a") + datafusion_expr::col("b"),
            datafusion_expr::col("c") + datafusion_expr::col("d"),
            datafusion_expr::col("e") - datafusion_expr::col("a"),
            datafusion_expr::col("e") * datafusion_expr::col("b"),
        ];
        let expected_batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(
                    (0..10).map(|x| x * 2 + 10),
                )) as _,
                Arc::new(PrimitiveArray::<Float64Type>::from_iter_values(
                    (0..10).map(|x| x as f64 * 2.0 + 50.0),
                )) as _,
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(
                    (0..10).map(|_| 40),
                )) as _,
                Arc::new(PrimitiveArray::<Int64Type>::from_iter_values(
                    (0..10).map(|x| (x + 10) * (x + 40)),
                )) as _,
            ],
        )
        .unwrap();

        // prepare logical plan
        let table_scan = Arc::new(
            table_scan(None, &input_schema, None)
                .unwrap()
                .build()
                .unwrap(),
        );
        let projection_plan = LogicalPlan::Projection(Projection {
            expr: exprs,
            input: table_scan,
            schema: output_schema,
            alias: None,
        });

        // compile logical plan into JIT execution plan
        let mut jit_ctx = JITContext::default();
        let jit_exec_plan = jit_ctx.compile_logical_plan(projection_plan).unwrap();

        // execute
        let output_batch = jit_exec_plan.execute(input_batch).unwrap();
        assert_eq!(output_batch, expected_batch)
    }
}
