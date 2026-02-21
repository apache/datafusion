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
