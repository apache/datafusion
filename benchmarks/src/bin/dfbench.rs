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

/// Never return freed memory to the OS (`purge_delay = -1`) unless the user
/// overrides `MIMALLOC_PURGE_DELAY`. Query execution repeatedly allocates and
/// frees large buffers; with mimalloc's default 10ms purge delay these are
/// decommitted and re-faulted, and the kernel spends a large fraction of the
/// runtime zeroing pages.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
fn configure_mimalloc() {
    unsafe extern "C" {
        fn mi_option_set(option: std::ffi::c_int, value: std::ffi::c_long);
    }
    /// `mi_option_purge_delay` in mimalloc v2's `mi_option_t`
    const MI_OPTION_PURGE_DELAY: std::ffi::c_int = 15;
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
        // SAFETY: setting a mimalloc option is thread safe and has no preconditions
        unsafe { mi_option_set(MI_OPTION_PURGE_DELAY, -1) };
    }
}

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
    #[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
    configure_mimalloc();
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
