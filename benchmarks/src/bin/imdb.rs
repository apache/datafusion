// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! IMDB binary entrypoint

use clap::{Parser, Subcommand};
use datafusion::error::Result;
use datafusion_benchmarks::imdb;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(name = "IMDB", about = "IMDB Dataset Processing.")]
struct Cli {
    #[command(subcommand)]
    command: ImdbOpt,
}

#[derive(Debug, Subcommand)]
enum ImdbOpt {
    #[command(subcommand)]
    Benchmark(BenchmarkSubCommandOpt),
    Convert(imdb::ConvertOpt),
}

#[derive(Debug, Subcommand)]
enum BenchmarkSubCommandOpt {
    #[command(name = "datafusion")]
    DataFusionBenchmark(imdb::RunOpt),
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    match Cli::parse().command {
        ImdbOpt::Benchmark(BenchmarkSubCommandOpt::DataFusionBenchmark(opt)) => {
            Box::pin(opt.run()).await
        }
        ImdbOpt::Convert(opt) => opt.run().await,
    }
}
