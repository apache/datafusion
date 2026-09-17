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

//! Criterion benchmark harness for SQL benchmark files under `sql_benchmarks`.
//!
//! SQL benchmarks describe setup, queries, result validation, and cleanup in
//! `.benchmark` files. Run them with `benchmarks/bench.sh` or directly with
//! Cargo, for example: `BENCH_NAME=tpch cargo bench --bench sql`.

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_benchmarks::sql_benchmark_runner::{
    criterion_harness_config_from_env, default_sql_benchmark_directory,
    run_criterion_benchmarks_impl_with_namespace,
};
use datafusion_common::instant::Instant;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn sql(c: &mut Criterion) {
    env_logger::init();

    let start = Instant::now();
    let (config, criterion_namespace) = criterion_harness_config_from_env();

    println!("Loading benchmarks...");

    run_criterion_benchmarks_impl_with_namespace(
        &default_sql_benchmark_directory(),
        &config,
        criterion_namespace.as_deref(),
        c,
    )
    .unwrap_or_else(|err| panic!("failed to run SQL benchmarks: {err:?}"));

    println!(
        "Completed benchmarks in {} ms ...",
        start.elapsed().as_millis()
    );
}

criterion_group!(benches, sql);
criterion_main!(benches);
