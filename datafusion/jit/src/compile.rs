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

use datafusion_common::Result;

use crate::api::Assembler;
use crate::ast::{JITType, I32};
use crate::{
    api::GeneratedFunction,
    ast::{Expr as JITExpr, I64, PTR_SIZE},
};

/// Wrap JIT Expr to array compute function.
pub fn build_calc_fn(
    assembler: &Assembler,
    jit_expr: JITExpr,
    inputs: Vec<(String, JITType)>,
    ret_type: JITType,
) -> Result<GeneratedFunction> {
    // Alias pointer type.
    // The raw pointer `R64` or `R32` is not compatible with integers.
    const PTR_TYPE: JITType = if PTR_SIZE == 8 { I64 } else { I32 };

    let mut builder = assembler.new_func_builder("calc_fn");
    // Declare in-param.
    // Each input takes one position, following by a pointer to place result,
    // and the last is the length of inputs/output arrays.
    for (name, _) in &inputs {
        builder = builder.param(format!("{}_array", name), PTR_TYPE);
    }
    let mut builder = builder.param("result", ret_type).param("len", I64);

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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, PrimitiveArray},
        datatypes::{DataType, Int64Type},
    };
    use datafusion_common::{DFField, DFSchema, DataFusionError};
    use datafusion_expr::Expr as DFExpr;

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
            core::mem::transmute::<_, fn(*const i64, *const i64, *const i64, i64) -> ()>(
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
        let expected =
            arrow::compute::kernels::arithmetic::add(&array_a, &array_b).unwrap();

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
}
