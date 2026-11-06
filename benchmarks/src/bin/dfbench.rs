// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! DataFusion benchmark runner
use datafusion::error::Result;
use datafusion_benchmarks::sort_pushdown;

use clap::{Parser, Subcommand};

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use datafusion_benchmarks::{
    cancellation, clickbench, dict, h2o, hj, imdb, nlj, smj, sort_tpch, statistics,
    tpcds, tpch,
};

#[derive(Debug, Parser)]
#[command(about = "benchmark command")]
struct Cli {
    #[command(subcommand)]
    command: Options,
}

#[derive(Debug, Subcommand)]
enum Options {
    Cancellation(cancellation::RunOpt),
    Clickbench(clickbench::RunOpt),
    Dict(dict::RunOpt),
    H2o(h2o::RunOpt),
    HJ(hj::RunOpt),
    Imdb(imdb::RunOpt),
    Nlj(nlj::RunOpt),
    Smj(smj::RunOpt),
    Statistics(statistics::RunOpt),
    SortPushdown(sort_pushdown::RunOpt),
    SortTpch(sort_tpch::RunOpt),
    Tpch(tpch::RunOpt),
    Tpcds(tpcds::RunOpt),
}

// Main benchmark runner entrypoint
#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    match cli.command {
        Options::Cancellation(opt) => opt.run().await,
        Options::Clickbench(opt) => opt.run().await,
        Options::Dict(opt) => opt.run().await,
        Options::H2o(opt) => opt.run().await,
        Options::HJ(opt) => opt.run().await,
        Options::Imdb(opt) => Box::pin(opt.run()).await,
        Options::Nlj(opt) => opt.run().await,
        Options::Smj(opt) => opt.run().await,
        Options::Statistics(opt) => opt.run().await,
        Options::SortPushdown(opt) => opt.run().await,
        Options::SortTpch(opt) => opt.run().await,
        Options::Tpch(opt) => Box::pin(opt.run()).await,
        Options::Tpcds(opt) => Box::pin(opt.run()).await,
    }
}
