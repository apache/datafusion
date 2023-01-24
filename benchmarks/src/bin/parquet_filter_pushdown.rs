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

use arrow::util::pretty;
use datafusion::common::Result;
use datafusion::logical_expr::{lit, or, Expr};
use datafusion::optimizer::utils::disjunction;
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, SessionContext};
use parquet::file::properties::WriterProperties;
use parquet_test_utils::{ParquetScanOptions, TestParquetFile};
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;
use test_utils::AccessLogGenerator;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "Benchmarks", about = "Apache Arrow Rust Benchmarks.")]
struct Opt {
    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of partitions to process in parallel
    #[structopt(long = "partitions", default_value = "2")]
    partitions: usize,

    /// Path to folder where access log file will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Data page size of the generated parquet file
    #[structopt(long = "page-size")]
    page_size: Option<usize>,

    /// Data page size of the generated parquet file
    #[structopt(long = "row-group-size")]
    row_group_size: Option<usize>,

    /// Total size of generated dataset. The default scale factor of 1.0 will generate a roughly 1GB parquet file
    #[structopt(short = "s", long = "scale-factor", default_value = "1.0")]
    scale_factor: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    println!("Running benchmarks with the following options: {opt:?}");

    let path = opt.path.join("logs.parquet");

    let mut props_builder = WriterProperties::builder();

    if let Some(s) = opt.page_size {
        props_builder = props_builder
            .set_data_pagesize_limit(s)
            .set_write_batch_size(s);
    }

    if let Some(s) = opt.row_group_size {
        props_builder = props_builder.set_max_row_group_size(s);
    }

    let test_file = gen_data(path, opt.scale_factor, props_builder.build())?;

    run_benchmarks(opt, &test_file).await?;

    Ok(())
}

async fn run_benchmarks(opt: Opt, test_file: &TestParquetFile) -> Result<()> {
    let scan_options_matrix = vec![
        ParquetScanOptions {
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
        },
        ParquetScanOptions {
            pushdown_filters: true,
            reorder_filters: true,
            enable_page_index: true,
        },
        ParquetScanOptions {
            pushdown_filters: true,
            reorder_filters: true,
            enable_page_index: false,
        },
    ];

    let filter_matrix = vec![
        // Selective-ish filter
        col("request_method").eq(lit("GET")),
        // Non-selective filter
        col("request_method").not_eq(lit("GET")),
        // Basic conjunction
        col("request_method")
            .eq(lit("POST"))
            .and(col("response_status").eq(lit(503_u16))),
        // Nested filters
        col("request_method").eq(lit("POST")).and(or(
            col("response_status").eq(lit(503_u16)),
            col("response_status").eq(lit(403_u16)),
        )),
        // Many filters
        disjunction([
            col("request_method").not_eq(lit("GET")),
            col("response_status").eq(lit(400_u16)),
            col("service").eq(lit("backend")),
        ])
        .unwrap(),
        // Filter everything
        col("response_status").eq(lit(429_u16)),
        // Filter nothing
        col("response_status").gt(lit(0_u16)),
    ];

    for filter_expr in &filter_matrix {
        println!("Executing with filter '{filter_expr}'");
        for scan_options in &scan_options_matrix {
            println!("Using scan options {scan_options:?}");
            for i in 0..opt.iterations {
                let start = Instant::now();

                let config = scan_options.config().with_target_partitions(opt.partitions);
                let ctx = SessionContext::with_config(config);

                let rows =
                    exec_scan(&ctx, test_file, filter_expr.clone(), opt.debug).await?;
                println!(
                    "Iteration {} returned {} rows in {} ms",
                    i,
                    rows,
                    start.elapsed().as_millis()
                );
            }
        }
        println!("\n");
    }
    Ok(())
}

async fn exec_scan(
    ctx: &SessionContext,
    test_file: &TestParquetFile,
    filter: Expr,
    debug: bool,
) -> Result<usize> {
    let exec = test_file.create_scan(filter).await?;

    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;

    if debug {
        pretty::print_batches(&result)?;
    }
    Ok(result.iter().map(|b| b.num_rows()).sum())
}

fn gen_data(
    path: PathBuf,
    scale_factor: f32,
    props: WriterProperties,
) -> Result<TestParquetFile> {
    let generator = AccessLogGenerator::new();

    let num_batches = 100_f32 * scale_factor;

    TestParquetFile::try_new(path, props, generator.take(num_batches as usize))
}
