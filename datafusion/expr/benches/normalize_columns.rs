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

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::expr_rewriter::normalize_cols;
use datafusion_expr::logical_plan::table_scan;
use datafusion_expr::{Expr, LogicalPlanBuilder, col, lit};

fn normalize_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("normalize_columns");
    for width in [10, 100, 500, 2000] {
        let schema = Schema::new(
            (0..width)
                .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
                .collect::<Vec<_>>(),
        );
        let input = table_scan(Some("t"), &schema, None)
            .unwrap()
            .project(
                (0..width)
                    .map(|i| (col(format!("t.c{i}")) + lit(1)).alias(format!("a{i}"))),
            )
            .unwrap()
            .build()
            .unwrap();

        for qualified in [false, true] {
            let (input, qualifier) = if qualified {
                (
                    LogicalPlanBuilder::from(input.clone())
                        .alias("s")
                        .unwrap()
                        .build()
                        .unwrap(),
                    "s.",
                )
            } else {
                (input.clone(), "")
            };
            let exprs: Vec<Expr> = (0..width)
                .map(|i| {
                    (col(format!("{qualifier}a{i}")) + lit(1)).alias(format!("b{i}"))
                })
                .collect();
            let kind = if qualified {
                "qualified"
            } else {
                "unqualified"
            };

            group.bench_with_input(
                BenchmarkId::new(format!("expressions/{kind}"), width),
                &width,
                |b, _| {
                    b.iter(|| {
                        normalize_cols(black_box(exprs.clone()), black_box(&input))
                            .unwrap()
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new(format!("projection/{kind}"), width),
                &width,
                |b, _| {
                    b.iter(|| {
                        LogicalPlanBuilder::from(black_box(input.clone()))
                            .project(black_box(exprs.clone()))
                            .unwrap()
                            .build()
                            .unwrap()
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, normalize_columns);
criterion_main!(benches);
