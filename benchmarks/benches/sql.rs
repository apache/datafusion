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
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_benchmarks::sql_benchmark::SqlBenchmark;
use datafusion_benchmarks::util::{CommonOpt, print_memory_stats};
use datafusion_common::instant::Instant;
use log::{debug, info};
use std::collections::BTreeMap;
use std::fs;
use std::sync::LazyLock;
use tokio::runtime::Runtime;

static SQL_BENCHMARK_DIRECTORY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}{}{}",
        env!("CARGO_MANIFEST_DIR"),
        std::path::MAIN_SEPARATOR,
        "sql_benchmarks"
    )
});

#[cfg(all(feature = "snmalloc", feature = "mimalloc"))]
compile_error!(
    "feature \"snmalloc\" and feature \"mimalloc\" cannot be enabled at the same time"
);

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
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
    query: Option<i32>,
}

pub fn sql(c: &mut Criterion) {
    env_logger::init();

    let start = Instant::now();
    let args = EnvParser::parse();
    let rt = make_tokio_runtime();

    println!("Loading benchmarks...");

    let benchmarks = rt.block_on(async {
        let ctx = make_ctx(&args).expect("SessionContext creation failed");

        load_benchmarks(&args, &ctx, &SQL_BENCHMARK_DIRECTORY)
            .await
            .unwrap_or_else(|err| panic!("failed load benchmarks: {err:?}"))
    });

    println!(
        "Loaded benchmarks in {} ms ...",
        start.elapsed().as_millis()
    );

    for (group, benchmarks) in benchmarks {
        let mut group = c.benchmark_group(group);
        group.sample_size(10);
        group.sampling_mode(SamplingMode::Flat);

        for mut benchmark in benchmarks {
            // create a context
            let ctx = make_ctx(&args).expect("SessionContext creation failed");

            // initialize the benchmark. This parses the benchmark file and does any pre-execution
            // work such as loading data into tables
            rt.block_on(async {
                benchmark
                    .initialize(&ctx)
                    .await
                    .expect("initialization failed");

                // run assertions
                benchmark.assert(&ctx).await.expect("assertion failed");
            });

            let mut name = benchmark.name().to_string();
            if !benchmark.subgroup().is_empty() {
                name.push('_');
                name.push_str(benchmark.subgroup());
            }

            if args.persist_results {
                handle_persist(&rt, &ctx, &name, &mut benchmark);
            } else if args.validate {
                handle_verify(&rt, &ctx, &name, &mut benchmark);
            } else {
                info!("Running benchmark {name} ...");

                let name = name.clone();
                group.bench_function(name.clone(), |b| {
                    b.iter(|| handle_run(&rt, &ctx, &args, &mut benchmark, &name))
                });

                print_memory_stats();

                info!("Benchmark {name} completed");
            }

            // run cleanup
            rt.block_on(async {
                benchmark.cleanup(&ctx).await.expect("Cleanup failed");
            });
        }

        group.finish();
    }
}

fn handle_run(
    rt: &Runtime,
    ctx: &SessionContext,
    args: &EnvParser,
    benchmark: &mut SqlBenchmark,
    name: &str,
) {
    rt.block_on(async {
        benchmark
            .run(ctx, args.validate)
            .await
            .unwrap_or_else(|err| panic!("Failed to run benchmark {name}: {err:?}"))
    });
}

fn handle_persist(
    rt: &Runtime,
    ctx: &SessionContext,
    name: &str,
    benchmark: &mut SqlBenchmark,
) {
    info!("Running benchmark {name} prior to persisting results ...");

    rt.block_on(async {
        info!("Persisting benchmark {name} ...");

        benchmark
            .persist(ctx)
            .await
            .expect("Failed to persist results");
    });

    info!("Persisted benchmark {name} successfully");
}

fn handle_verify(
    rt: &Runtime,
    ctx: &SessionContext,
    name: &str,
    benchmark: &mut SqlBenchmark,
) {
    info!("Verifying benchmark {name} results ...");

    rt.block_on(async {
        benchmark
            .run(ctx, true)
            .await
            .unwrap_or_else(|err| panic!("Failed to run benchmark {name}: {err:?}"));
        benchmark
            .verify(ctx)
            .await
            .unwrap_or_else(|err| panic!("Verification failed: {err:?}"));
    });

    info!("Verified benchmark {name} results successfully");
}

criterion_group!(benches, sql);
criterion_main!(benches);

fn make_tokio_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn make_ctx(args: &EnvParser) -> Result<SessionContext> {
    let config = args.options.config()?;
    let rt = args.options.build_runtime()?;

    Ok(SessionContext::new_with_config_rt(config, rt))
}

/// Recursively walks the directory tree starting at `path` and
/// calls the call back function for every file encountered.
pub fn list_files<F>(path: &str, callback: &mut F)
where
    F: FnMut(&str),
{
    let mut entries: Vec<fs::DirEntry> =
        fs::read_dir(path).unwrap().filter_map(Result::ok).collect();
    entries.sort_by_key(|entry| entry.path());

    for dir_entry in entries {
        let path = dir_entry.path();
        if path.is_dir() {
            // Recurse into the sub‑directory
            list_files(&path.to_string_lossy(), callback);
        } else {
            // For files, invoke the callback with the full path as a string
            let full_str = path.to_string_lossy();
            callback(&full_str);
        }
    }
}

/// Loads all benchmark files in the `sql_benchmarks` directory.
/// For each file ending with `.benchmark` it creates a new
/// `SqlBenchmark` instance.
async fn load_benchmarks(
    args: &EnvParser,
    ctx: &SessionContext,
    path: &str,
) -> Result<BTreeMap<String, Vec<SqlBenchmark>>> {
    let mut benches = BTreeMap::new();
    let mut paths = Vec::new();

    list_files(path, &mut |path: &str| {
        if path.ends_with(".benchmark") {
            paths.push(path.to_string());
        }
    });

    for path in paths {
        debug!("Loading benchmark from {path}");

        let benchmark = SqlBenchmark::new(ctx, &path, &*SQL_BENCHMARK_DIRECTORY).await?;
        let entries = benches
            .entry(benchmark.group().to_string())
            .or_insert(vec![]);

        entries.push(benchmark);
    }

    benches = filter_benchmarks(args, benches);
    benches.iter_mut().for_each(|(_, benchmarks)| {
        benchmarks.sort_by(|b1, b2| b1.name().cmp(b2.name()))
    });

    Ok(benches)
}

fn filter_benchmarks(
    args: &EnvParser,
    benchmarks: BTreeMap<String, Vec<SqlBenchmark>>,
) -> BTreeMap<String, Vec<SqlBenchmark>> {
    match &args.name {
        Some(bench_name) => benchmarks
            .into_iter()
            .filter(|(key, _val)| key.eq_ignore_ascii_case(bench_name))
            .map(|(key, mut val)| {
                if let Some(subgroup) = &args.subgroup {
                    val.retain(|bench| bench.subgroup().eq_ignore_ascii_case(subgroup));
                }
                if let Some(query_number) = &args.query {
                    let padded = format!("Q{query_number:0>2}");
                    val.retain(|bench| bench.name().eq_ignore_ascii_case(&padded));
                }
                (key, val)
            })
            .collect(),
        None => benchmarks,
    }
}
