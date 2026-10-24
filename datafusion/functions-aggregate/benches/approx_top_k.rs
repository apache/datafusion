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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate::approx_top_k::ApproxTopK;
use datafusion_physical_expr::expressions::{col, lit};

const ROWS: usize = 100_000;
const GROUPS: usize = 25_000;
const CHURN_GROUPS: usize = 1_000;

fn prepare_accumulator(data_type: DataType, k: i64) -> Box<dyn Accumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        data_type.clone(),
        true,
    )]));
    let value = col("value", &schema).unwrap();
    let k_expr = lit(k);
    let exprs = [value, k_expr];
    let expr_fields = exprs
        .iter()
        .map(|expr| expr.return_field(&schema).unwrap())
        .collect::<Vec<_>>();
    let function = ApproxTopK::new();
    let return_type = function.return_type(&[data_type, DataType::Int64]).unwrap();
    function
        .accumulator(AccumulatorArgs {
            return_field: Arc::new(Field::new("result", return_type, true)),
            schema: &schema,
            expr_fields: &expr_fields,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "approx_top_k(value, k)",
            is_distinct: false,
            exprs: &exprs,
        })
        .unwrap()
}

fn prepare_groups_accumulator(data_type: DataType, k: i64) -> Box<dyn GroupsAccumulator> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        data_type.clone(),
        true,
    )]));
    let value = col("value", &schema).unwrap();
    let k_expr = lit(k);
    let exprs = [value, k_expr];
    let expr_fields = exprs
        .iter()
        .map(|expr| expr.return_field(&schema).unwrap())
        .collect::<Vec<_>>();
    let function = ApproxTopK::new();
    let return_type = function.return_type(&[data_type, DataType::Int64]).unwrap();
    function
        .create_groups_accumulator(AccumulatorArgs {
            return_field: Arc::new(Field::new("result", return_type, true)),
            schema: &schema,
            expr_fields: &expr_fields,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: "approx_top_k(value, k)",
            is_distinct: false,
            exprs: &exprs,
        })
        .unwrap()
}

fn high_cardinality_values() -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..ROWS).map(|row| format!("value_{row:08}")),
    ))
}

fn approx_top_k_benchmark(c: &mut Criterion) {
    let values = high_cardinality_values();
    let mut group = c.benchmark_group("approx_top_k_high_cardinality");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ROWS as u64));
    for k in [10, 1_000, 10_000] {
        group.bench_function(format!("update and evaluate utf8 k={k}"), |b| {
            b.iter_batched(
                || prepare_accumulator(DataType::Utf8, k),
                |mut accumulator| {
                    accumulator
                        .update_batch(std::slice::from_ref(&values))
                        .unwrap();
                    black_box(accumulator.evaluate().unwrap());
                },
                BatchSize::SmallInput,
            );
        });
        group.bench_function(format!("update only utf8 k={k}"), |b| {
            b.iter_batched(
                || prepare_accumulator(DataType::Utf8, k),
                |mut accumulator| {
                    accumulator
                        .update_batch(std::slice::from_ref(&values))
                        .unwrap();
                    black_box(accumulator);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();

    let mut group = c.benchmark_group("approx_top_k_grouped");
    group.sample_size(10);
    group.throughput(Throughput::Elements(ROWS as u64));
    for total_groups in [ROWS, GROUPS, CHURN_GROUPS] {
        let groups: Vec<usize> = (0..ROWS).map(|row| row % total_groups).collect();
        group.bench_function(format!("utf8 {total_groups} groups k=10"), |b| {
            b.iter_batched(
                || prepare_groups_accumulator(DataType::Utf8, 10),
                |mut accumulator| {
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            &groups,
                            None,
                            total_groups,
                        )
                        .unwrap();
                    black_box(accumulator.evaluate(EmitTo::All).unwrap());
                },
                BatchSize::SmallInput,
            );
        });
    }
    let groups: Vec<usize> = (0..ROWS).map(|row| row % CHURN_GROUPS).collect();
    group.bench_function(format!("utf8 {CHURN_GROUPS} groups k=10000"), |b| {
        b.iter_batched(
            || prepare_groups_accumulator(DataType::Utf8, 10_000),
            |mut accumulator| {
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &groups,
                        None,
                        CHURN_GROUPS,
                    )
                    .unwrap();
                black_box(accumulator.evaluate(EmitTo::All).unwrap());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    let mut group = c.benchmark_group("approx_top_k_state");
    group.sample_size(10);
    for k in [1_000, 10_000] {
        group.bench_function(format!("serialize utf8 k={k}"), |b| {
            b.iter_batched(
                || {
                    let mut accumulator = prepare_accumulator(DataType::Utf8, k);
                    accumulator
                        .update_batch(std::slice::from_ref(&values))
                        .unwrap();
                    accumulator
                },
                |mut accumulator| black_box(accumulator.state().unwrap()),
                BatchSize::SmallInput,
            );
        });

        let state = {
            let mut accumulator = prepare_accumulator(DataType::Utf8, k);
            accumulator
                .update_batch(std::slice::from_ref(&values))
                .unwrap();
            accumulator.state().unwrap()[0].to_array_of_size(1).unwrap()
        };
        group.bench_function(format!("deserialize and nonempty merge utf8 k={k}"), |b| {
            b.iter_batched(
                || {
                    let mut accumulator = prepare_accumulator(DataType::Utf8, k);
                    accumulator
                        .update_batch(std::slice::from_ref(&values))
                        .unwrap();
                    accumulator
                },
                |mut accumulator| {
                    accumulator
                        .merge_batch(std::slice::from_ref(&state))
                        .unwrap();
                    black_box(accumulator);
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, approx_top_k_benchmark);
criterion_main!(benches);
