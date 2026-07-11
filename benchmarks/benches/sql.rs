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

use clap::Parser;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_benchmarks::sql_benchmark_runner::{
    BenchmarkFilter, SqlRunConfig, default_sql_benchmark_directory,
    run_criterion_benchmarks_impl,
};
use datafusion_benchmarks::util::CommonOpt;
use datafusion_common::instant::Instant;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(ignore_errors = true)]
struct EnvParser {
    #[command(flatten)]
    options: CommonOpt,

    #[arg(
        env = "BENCH_PERSIST_RESULTS",
        long = "persist_results",
        default_value = "false",
        action = clap::ArgAction::SetTrue
    )]
    persist_results: bool,

    #[arg(
        env = "BENCH_VALIDATE",
        long = "validate_results",
        default_value = "false",
        action = clap::ArgAction::SetTrue
    )]
    validate: bool,

    #[arg(env = "BENCH_NAME")]
    name: Option<String>,

    #[arg(env = "BENCH_SUBGROUP")]
    subgroup: Option<String>,

    #[arg(env = "BENCH_QUERY")]
    query: Option<String>,
}

pub fn sql(c: &mut Criterion) {
    env_logger::init();

    let start = Instant::now();
    let args = EnvParser::parse();
    let config = SqlRunConfig {
        common: args.options,
        filter: BenchmarkFilter {
            name: args.name,
            subgroup: args.subgroup,
            query: args.query,
        },
        persist_results: args.persist_results,
        validate_results: args.validate,
        output: None,
    };

    println!("Loading benchmarks...");

    run_criterion_benchmarks_impl(&default_sql_benchmark_directory(), &config, c)
        .unwrap_or_else(|err| panic!("failed to run SQL benchmarks: {err:?}"));

    println!(
        "Completed benchmarks in {} ms ...",
        start.elapsed().as_millis()
    );
}

criterion_group!(benches, sql);
criterion_main!(benches);
