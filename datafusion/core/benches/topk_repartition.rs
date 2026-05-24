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

//! Benchmark for the TopKRepartition optimizer rule.
//!
//! Measures the benefit of pushing TopK (Sort with fetch) below hash
//! repartition when running partitioned window functions with LIMIT.

mod data_utils;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_utils::create_table_provider;
use datafusion::prelude::{SessionConfig, SessionContext};
use parking_lot::Mutex;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    target_partitions: usize,
    enable_topk_repartition: bool,
) -> Arc<Mutex<SessionContext>> {
    let array_len = 1024 * 1024;
    let batch_size = 8 * 1024;
    let mut config = SessionConfig::new().with_target_partitions(target_partitions);
    config.options_mut().optimizer.enable_topk_repartition = enable_topk_repartition;
    let ctx = SessionContext::new_with_config(config);
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let provider =
            create_table_provider(partitions_len, array_len, batch_size).unwrap();
        ctx.register_table("t", provider).unwrap();
    });
    Arc::new(Mutex::new(ctx))
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let limits = [10, 1_000, 10_000, 100_000];
    let scans = 16;
    let target_partitions = 4;

    let group = format!("topk_repartition_{scans}_to_{target_partitions}");
    let mut group = c.benchmark_group(group);
    for limit in limits {
        let sql = format!(
            "SELECT \
                SUM(f64) OVER (PARTITION BY u64_narrow ORDER BY u64_wide ROWS UNBOUNDED PRECEDING) \
                FROM t \
                ORDER BY u64_narrow, u64_wide \
                LIMIT {limit}"
        );

        let ctx_disabled = create_context(scans, target_partitions, false);
        group.bench_function(BenchmarkId::new("disabled", limit), |b| {
            b.iter(|| query(ctx_disabled.clone(), &rt, &sql))
        });

        let ctx_enabled = create_context(scans, target_partitions, true);
        group.bench_function(BenchmarkId::new("enabled", limit), |b| {
            b.iter(|| query(ctx_enabled.clone(), &rt, &sql))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
