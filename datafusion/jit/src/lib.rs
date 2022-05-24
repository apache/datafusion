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

//! Just-In-Time compilation to accelerate DataFusion physical plan execution.

pub mod api;
pub mod ast;
pub mod compile;
pub mod jit;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::api::{Assembler, GeneratedFunction};
    use crate::ast::{BinaryExpr, Expr, Literal, TypedLit, I64};
    use crate::jit::JIT;
    use arrow::datatypes::DataType;
    use datafusion_common::{DFField, DFSchema, Result};
    use datafusion_expr::{col, lit};

    #[test]
    fn iterative_fib() -> Result<()> {
        let expected = r#"fn iterative_fib_0(n: i64) -> r: i64 {
    if n == 0 {
        r = 0;
    } else {
        n = n - 1;
        let a: i64;
        a = 0;
        r = 1;
        while n != 0 {
            let t: i64;
            t = r;
            r = r + a;
            a = t;
            n = n - 1;
        }
    }
}"#;
        let assembler = Assembler::default();
        let mut builder = assembler
            .new_func_builder("iterative_fib")
            .param("n", I64)
            .ret("r", I64);
        let mut fn_body = builder.enter_block();

        fn_body.if_block(
            |cond| cond.eq(cond.id("n")?, cond.lit_i(0)),
            |t| {
                t.assign("r", t.lit_i(0))?;
                Ok(())
            },
            |e| {
                e.assign("n", e.sub(e.id("n")?, e.lit_i(1))?)?;
                e.declare_as("a", e.lit_i(0))?;
                e.assign("r", e.lit_i(1))?;
                e.while_block(
                    |cond| cond.ne(cond.id("n")?, cond.lit_i(0)),
                    |w| {
                        w.declare_as("t", w.id("r")?)?;
                        w.assign("r", w.add(w.id("r")?, w.id("a")?)?)?;
                        w.assign("a", w.id("t")?)?;
                        w.assign("n", w.sub(w.id("n")?, w.lit_i(1))?)?;
                        Ok(())
                    },
                )?;
                Ok(())
            },
        )?;

        let gen_func = fn_body.build();
        assert_eq!(format!("{}", &gen_func), expected);
        let mut jit = assembler.create_jit();
        assert_eq!(55, run_iterative_fib_code(&mut jit, gen_func, 10)?);
        Ok(())
    }

    #[test]
    fn from_datafusion_expression() -> Result<()> {
        let df_expr = lit(1.0f32) + lit(2.0f32);
        let schema = Arc::new(DFSchema::empty());
        let jit_expr: crate::ast::Expr = (df_expr, schema).try_into()?;

        assert_eq!(
            jit_expr,
            Expr::Binary(BinaryExpr::Add(
                Box::new(Expr::Literal(Literal::Typed(TypedLit::Float(1.0)))),
                Box::new(Expr::Literal(Literal::Typed(TypedLit::Float(2.0))))
            )),
        );

        Ok(())
    }

    #[test]
    fn from_datafusion_expression_schema() -> Result<()> {
        let df_expr = col("a") + lit(1i64);
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![DFField::new(Some("table1"), "a", DataType::Int64, false)],
            HashMap::new(),
        )?);
        let jit_expr: crate::ast::Expr = (df_expr, schema).try_into()?;

        assert_eq!(
            jit_expr,
            Expr::Binary(BinaryExpr::Add(
                Box::new(Expr::Identifier("table1.a".to_string(), I64)),
                Box::new(Expr::Literal(Literal::Typed(TypedLit::Int(1))))
            )),
        );

        Ok(())
    }

    unsafe fn run_code<I, O>(
        jit: &mut JIT,
        code: GeneratedFunction,
        input: I,
    ) -> Result<O> {
        // Pass the string to the JIT, and it returns a raw pointer to machine code.
        let code_ptr = jit.compile(code)?;
        // Cast the raw pointer to a typed function pointer. This is unsafe, because
        // this is the critical point where you have to trust that the generated code
        // is safe to be called.
        let code_fn = core::mem::transmute::<_, fn(I) -> O>(code_ptr);
        // And now we can call it!
        Ok(code_fn(input))
    }

    fn run_iterative_fib_code(
        jit: &mut JIT,
        code: GeneratedFunction,
        input: isize,
    ) -> Result<isize> {
        unsafe { run_code(jit, code, input) }
    }
}
