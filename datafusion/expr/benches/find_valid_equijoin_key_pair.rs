// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Microbenchmarks for [`find_valid_equijoin_key_pair`] with a candidate key
//! that does not belong to the join, when one join input is much wider than
//! the other:
//!
//! 1. **wide_left** – e.g. the accumulated left input of a left-deep join
//!    tree, as built by `EliminateCrossJoin`.
//!
//! 2. **wide_right** – e.g. `t JOIN (t1 JOIN t2 ...)`, as seen by
//!    `ExtractEquijoinPredicate`.

use std::hint::black_box;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::DFSchema;
use datafusion_expr::col;
use datafusion_expr::utils::find_valid_equijoin_key_pair;

/// Schema of table `name` with columns `c0`, `c1`, ..., `c{width - 1}`.
fn schema(name: &str, width: usize) -> DFSchema {
    let schema = Schema::new(
        (0..width)
            .map(|i| Field::new(format!("c{i}"), DataType::Int32, false))
            .collect::<Vec<_>>(),
    );
    DFSchema::try_from_qualified_schema(name, &schema).unwrap()
}

fn bench_find_valid_equijoin_key_pair(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_valid_equijoin_key_pair");
    let narrow = schema("narrow", 3);
    for width in [16, 128, 1024] {
        let wide = schema("wide", width);
        // `wide.c{width / 2} = other.c0`, where `other` is in neither input
        let left_key = col(format!("wide.c{}", width / 2));
        let right_key = col("other.c0");

        for (name, left_schema, right_schema) in [
            ("wide_left", &wide, &narrow),
            ("wide_right", &narrow, &wide),
        ] {
            group.bench_function(BenchmarkId::new(name, width), |b| {
                b.iter(|| {
                    black_box(
                        find_valid_equijoin_key_pair(
                            &left_key,
                            &right_key,
                            left_schema,
                            right_schema,
                        )
                        .unwrap(),
                    )
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_find_valid_equijoin_key_pair);
criterion_main!(benches);
