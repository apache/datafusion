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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use data_utils::create_table_provider;
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContext;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

fn query(ctx: Arc<Mutex<ExecutionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    let df = ctx.lock().unwrap().sql(sql).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<ExecutionContext>>> {
    let mut ctx = ExecutionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
    let batch_size = 2048; // 2^11
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("aggregate_query_no_group_by 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_min_max_f64", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT MIN(f64), MAX(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_wide", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT COUNT(DISTINCT u64_wide) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_count_distinct_narrow", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT COUNT(DISTINCT u64_narrow) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_group_by", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT utf8, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY utf8",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t GROUP BY u64_narrow",
            )
        })
    });

    c.bench_function("aggregate_query_group_by_with_filter_u64 15 12", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT u64_narrow, MIN(f64), AVG(f64), COUNT(f64) \
                 FROM t \
                 WHERE f32 > 10 AND f32 < 20 GROUP BY u64_narrow",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
