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

//! IMDB binary entrypoint

use datafusion::error::Result;
use datafusion_benchmarks::imdb;
use structopt::StructOpt;

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

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum BenchmarkSubCommandOpt {
    #[structopt(name = "datafusion")]
    DataFusionBenchmark(imdb::RunOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "IMDB", about = "IMDB Dataset Processing.")]
enum ImdbOpt {
    Benchmark(BenchmarkSubCommandOpt),
    Convert(imdb::ConvertOpt),
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    match ImdbOpt::from_args() {
        ImdbOpt::Benchmark(BenchmarkSubCommandOpt::DataFusionBenchmark(opt)) => {
            opt.run().await
        }
        ImdbOpt::Convert(opt) => opt.run().await,
    }
}
