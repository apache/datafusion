// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod data_utils;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::execution::context::SessionContext;
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context() -> Arc<Mutex<SessionContext>> {
    let ctx = SessionContext::new();
    Arc::new(Mutex::new(ctx))
}

fn criterion_benchmark(c: &mut Criterion) {
    let ctx = create_context();
    let rt = Runtime::new().unwrap();

    c.bench_function("range(1000000)", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT value from range(1000000)"))
    });

    c.bench_function("generate_series(1000000)", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT value from generate_series(1000000)",
            )
        })
    });

    c.bench_function("range(0, 1000000, 5)", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT value from range(0, 1000000, 5)"))
    });

    c.bench_function("generate_series(0, 1000000, 5)", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT value from generate_series(0, 1000000, 5)",
            )
        })
    });

    c.bench_function("range(1000000, 0, -5)", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT value from range(1000000, 0, -5)"))
    });

    c.bench_function("generate_series(1000000, 0, -5)", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT value from generate_series(1000000, 0, -5)",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
