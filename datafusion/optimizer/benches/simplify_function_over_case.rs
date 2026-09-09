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

//! Microbenchmarks for a scalar function applied to a CASE with literal
//! branches: plan-time simplifier cost over N branches, and per-batch
//! evaluation cost of the (2-branch) expression after the standard
//! simplification pipeline.

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{
    Case, ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, col, lit,
};
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_physical_expr::create_physical_expr;
use std::hint::black_box;
use std::sync::Arc;

/// A deliberately expensive immutable `&str -> i64` function standing in for
/// real parse-heavy work (timestamp/format parsing, regex compilation).
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExpensiveParse {
    signature: Signature,
}

impl ExpensiveParse {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

fn expensive_parse_str(s: &str) -> i64 {
    let mut acc: i64 = 0;
    for _round in 0..100 {
        for b in s.bytes() {
            acc = acc.wrapping_mul(31).wrapping_add(i64::from(b));
        }
    }
    acc
}

impl ScalarUDFImpl for ExpensiveParse {
    fn name(&self) -> &str {
        "expensive_parse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(v.as_deref().map(expensive_parse_str)),
            )),
            ColumnarValue::Array(array) => {
                let strings = arrow::array::cast::as_string_array(array);
                let out: arrow::array::Int64Array =
                    strings.iter().map(|v| v.map(expensive_parse_str)).collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            _ => datafusion_common::exec_err!("expensive_parse: unsupported argument"),
        }
    }
}

/// `f(CASE WHEN flag THEN 'b0' WHEN ... ELSE 'e' END)` with `branches` WHEN arms.
fn function_over_case(branches: usize) -> Expr {
    let case = Expr::Case(Case::new(
        None,
        (0..branches)
            .map(|i| {
                (
                    Box::new(col("flag")),
                    Box::new(lit(format!("branch-value-{i}"))),
                )
            })
            .collect(),
        Some(Box::new(lit("else-value"))),
    ));
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::new_from_impl(ExpensiveParse::new())),
        vec![case],
    ))
}

fn schema() -> Arc<DFSchema> {
    Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]))
        .unwrap(),
    )
}

fn simplifier(schema: Arc<DFSchema>) -> ExprSimplifier {
    ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build())
}

fn bench_simplify(c: &mut Criterion) {
    let mut group = c.benchmark_group("simplify_function_over_case/simplify");
    for branches in [2usize, 8, 32] {
        let expr = function_over_case(branches);
        let simplifier = simplifier(schema());
        group.bench_with_input(
            BenchmarkId::from_parameter(branches),
            &expr,
            |b, expr| {
                b.iter_batched(
                    || expr.clone(),
                    |expr| black_box(simplifier.simplify(expr).unwrap()),
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }
    group.finish();
}

fn bench_evaluate(c: &mut Criterion) {
    let df_schema = schema();
    let rows = 8192;
    let flags = BooleanArray::from_iter((0..rows).map(|i| Some(i % 2 == 0)));
    let batch =
        RecordBatch::try_new(Arc::clone(df_schema.inner()), vec![Arc::new(flags)])
            .unwrap();

    let expr = simplifier(Arc::clone(&df_schema))
        .simplify(function_over_case(2))
        .unwrap();
    let props = ExecutionProps::new();
    let planning_context = PhysicalPlanningContext::default();
    let expr =
        create_physical_expr(&expr, &df_schema, &props, &planning_context).unwrap();

    // Grouped so the result reads as rows per second; the benchmark id is
    // unchanged.
    let mut group = c.benchmark_group("simplify_function_over_case");
    group.throughput(Throughput::Elements(rows as u64));
    group.bench_function("evaluate_8192_rows", |b| {
        b.iter(|| black_box(expr.evaluate(&batch).unwrap()))
    });
    group.finish();
}

criterion_group!(benches, bench_simplify, bench_evaluate);
criterion_main!(benches);
