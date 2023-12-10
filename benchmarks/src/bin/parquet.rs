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

use datafusion::common::Result;

use datafusion_benchmarks::{parquet_filter, sort};
use structopt::StructOpt;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Benchmarks", about = "Apache Arrow Rust Benchmarks.")]
enum ParquetBenchCmd {
    /// Benchmark sorting parquet files
    Sort(sort::RunOpt),
    /// Benchmark parquet filter pushdown
    Filter(parquet_filter::RunOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = ParquetBenchCmd::from_args();
    match cmd {
        ParquetBenchCmd::Filter(opt) => {
            println!("running filter benchmarks");
            opt.run().await
        }
        ParquetBenchCmd::Sort(opt) => {
            println!("running sort benchmarks");
            opt.run().await
        }
    }
}
