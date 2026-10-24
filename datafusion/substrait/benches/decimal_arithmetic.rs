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

//! Import, expression execution, and filtered aggregate benchmarks. The same
//! harness can run on main: mismatched declarations then measure the old native
//! calculation, which produces a different result type and sometimes fewer digits.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::arrow::array::Decimal128Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::{Expr, ExprSchemable, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::extensions::Extensions;
use datafusion_substrait::logical_plan::consumer::{
    DefaultSubstraitConsumer, SubstraitConsumer,
};
use datafusion_substrait::logical_plan::producer::{
    DefaultSubstraitProducer, SubstraitProducer,
};
use substrait::proto::FunctionArgument;
use substrait::proto::expression::ScalarFunction;
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Type, r#type};

fn decimal_type(precision: u8, scale: i8, nullable: bool) -> Type {
    Type {
        kind: Some(r#type::Kind::Decimal(r#type::Decimal {
            precision: i32::from(precision),
            scale: i32::from(scale),
            nullability: if nullable {
                r#type::Nullability::Nullable
            } else {
                r#type::Nullability::Required
            } as i32,
            ..Default::default()
        })),
    }
}

fn benchmark(c: &mut Criterion) {
    run(c).unwrap();
}

fn run(c: &mut Criterion) -> Result<()> {
    const ROWS: usize = 8192;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let state = ctx.state();
    let mut group = c.benchmark_group("decimal_execution");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, function_name, left_type, right_type, output_type) in [
        ("matching_add", "add", (10, 2), (5, 1), (11, 2)),
        ("matching_divide", "divide", (10, 2), (5, 1), (15, 6)),
        ("declared_divide", "divide", (10, 2), (5, 1), (21, 8)),
        ("precision_only_divide", "divide", (10, 2), (5, 1), (25, 6)),
        ("wide_add", "add", (38, 10), (38, 10), (38, 9)),
        ("wide_multiply", "multiply", (38, 10), (38, 10), (38, 6)),
    ] {
        for shape in ["columns", "constant", "nulls", "large"] {
            if shape == "large" && !name.starts_with("wide_") {
                continue;
            }
            let mut extensions = Extensions::default();
            extensions
                .functions
                .insert(0, format!("{function_name}:dec_dec"));
            let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
            let mut producer = DefaultSubstraitProducer::new(&state);
            let left_dt = DataType::Decimal128(left_type.0, left_type.1);
            let right_dt = DataType::Decimal128(right_type.0, right_type.1);
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", left_dt, shape == "nulls"),
                Field::new("b", right_dt, false),
            ]));
            let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone())?);
            let right = match (shape, function_name) {
                ("large", "add") => 9 * 10_i128.pow(37),
                ("large", "multiply") => 10_i128.pow(10),
                _ => 3 * 10_i128.pow(right_type.1 as u32),
            };
            let call = ScalarFunction {
                function_reference: 0,
                arguments: vec![
                    FunctionArgument {
                        arg_type: Some(ArgType::Value(
                            producer.handle_expr(&col("a"), &df_schema)?,
                        )),
                    },
                    FunctionArgument {
                        arg_type: Some(ArgType::Value(if shape == "constant" {
                            producer.handle_literal(&ScalarValue::Decimal128(
                                Some(right),
                                right_type.0,
                                right_type.1,
                            ))?
                        } else {
                            producer.handle_expr(&col("b"), &df_schema)?
                        })),
                    },
                ],
                output_type: Some(decimal_type(
                    output_type.0,
                    output_type.1,
                    shape == "nulls",
                )),
                ..Default::default()
            };
            let expression =
                runtime.block_on(consumer.consume_scalar_function(&call, &df_schema))?;
            // These values require wider intermediates before scale reduction.
            // Main does not implement that calculation, so compare only revisions
            // that preserve the declared type for these two cases.
            if shape == "large"
                && expression.get_type(&df_schema)?
                    != DataType::Decimal128(output_type.0, output_type.1)
            {
                continue;
            }
            let physical = state.create_physical_expr(expression, &df_schema)?;
            let left_values = (0..ROWS).map(|i| {
                if shape == "nulls" && i % 5 == 0 {
                    None
                } else if shape == "large" {
                    Some(9 * 10_i128.pow(37) - i as i128)
                } else {
                    Some((1 + (i % 100) as i128) * 10_i128.pow(left_type.1 as u32))
                }
            });
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(
                        Decimal128Array::from_iter(left_values)
                            .with_precision_and_scale(left_type.0, left_type.1)?,
                    ),
                    Arc::new(
                        Decimal128Array::from(vec![right; ROWS])
                            .with_precision_and_scale(right_type.0, right_type.1)?,
                    ),
                ],
            )?;
            // Surface execution errors before timing, including on comparison refs.
            physical.evaluate(&batch)?;
            group.bench_with_input(BenchmarkId::new(name, shape), &batch, |b, batch| {
                b.iter(|| physical.evaluate(black_box(batch)).unwrap());
            });
        }
    }
    group.finish();

    let mut extensions = Extensions::default();
    extensions.functions.insert(0, "divide:dec_dec".into());
    let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Decimal128(10, 2), false),
        Field::new("b", DataType::Decimal128(5, 1), false),
    ]));
    let df_schema = Arc::new(DFSchema::try_from(schema.as_ref().clone())?);
    let mut producer = DefaultSubstraitProducer::new(&state);
    let mut group = c.benchmark_group("decimal_import");
    let mut expressions: Vec<(&str, Expr)> = vec![];
    for (name, output) in [("matching", (15, 6)), ("declared", (21, 8))] {
        let call = ScalarFunction {
            function_reference: 0,
            arguments: (0..2)
                .map(|index| {
                    Ok(FunctionArgument {
                        arg_type: Some(ArgType::Value(producer.handle_expr(
                            &col(if index == 0 { "a" } else { "b" }),
                            &df_schema,
                        )?)),
                    })
                })
                .collect::<Result<_>>()?,
            output_type: Some(decimal_type(output.0, output.1, false)),
            ..Default::default()
        };
        group.bench_function(name, |b| {
            b.iter(|| {
                runtime
                    .block_on(
                        consumer.consume_scalar_function(black_box(&call), &df_schema),
                    )
                    .unwrap()
            });
        });
        expressions.push((
            name,
            runtime.block_on(consumer.consume_scalar_function(&call, &df_schema))?,
        ));
    }
    group.finish();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(
                Decimal128Array::from_iter_values(
                    (0..65536).map(|i| (i % 100 + 1) * 100),
                )
                .with_precision_and_scale(10, 2)?,
            ),
            Arc::new(
                Decimal128Array::from(vec![30; 65536]).with_precision_and_scale(5, 1)?,
            ),
        ],
    )?;
    ctx.register_batch("decimals", batch)?;
    let mut group = c.benchmark_group("decimal_query");
    for (name, expression) in expressions {
        let df = runtime
            .block_on(ctx.table("decimals"))?
            .select(vec![expression.alias("ratio")])?
            .filter(col("ratio").gt(lit(ScalarValue::Decimal128(Some(3_333_331), 8, 7))))?
            .aggregate(vec![], vec![sum(col("ratio"))])?;
        let plan = df.logical_plan().clone();
        group.bench_function(name, |b| {
            b.iter(|| {
                runtime
                    .block_on(async {
                        ctx.execute_logical_plan(black_box(plan.clone()))
                            .await?
                            .collect()
                            .await
                    })
                    .unwrap()
            });
        });
    }
    group.finish();
    Ok(())
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(30).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = benchmark
}
criterion_main!(benches);
