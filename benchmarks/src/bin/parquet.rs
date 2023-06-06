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
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::prelude::{col, SessionConfig, SessionContext};
use datafusion::test_util::parquet::{ParquetScanOptions, TestParquetFile};
use datafusion_benchmarks::BenchmarkRun;
use parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;
use test_utils::AccessLogGenerator;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, Clone, StructOpt)]
#[structopt(name = "Benchmarks", about = "Apache Arrow Rust Benchmarks.")]
enum ParquetBenchCmd {
    /// Benchmark sorting parquet files
    Sort(Opt),
    /// Benchmark parquet filter pushdown
    Filter(Opt),
}

#[derive(Debug, StructOpt, Clone)]
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

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}
impl Opt {
    /// Initialize parquet test file given options.
    fn init_file(&self) -> Result<TestParquetFile> {
        let path = self.path.join("logs.parquet");

        let mut props_builder = WriterProperties::builder();

        if let Some(s) = self.page_size {
            props_builder = props_builder
                .set_data_pagesize_limit(s)
                .set_write_batch_size(s);
        }

        if let Some(s) = self.row_group_size {
            props_builder = props_builder.set_max_row_group_size(s);
        }

        gen_data(path, self.scale_factor, props_builder.build())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cmd = ParquetBenchCmd::from_args();
    match cmd {
        ParquetBenchCmd::Filter(opt) => {
            println!("running filter benchmarks");
            let test_file = opt.init_file()?;
            run_filter_benchmarks(opt, &test_file).await?;
        }
        ParquetBenchCmd::Sort(opt) => {
            println!("running sort benchmarks");
            let test_file = opt.init_file()?;
            run_sort_benchmarks(opt, &test_file).await?;
        }
    }
    Ok(())
}

async fn run_sort_benchmarks(opt: Opt, test_file: &TestParquetFile) -> Result<()> {
    use datafusion::physical_expr::expressions::col;
    let mut rundata = BenchmarkRun::new();
    let schema = test_file.schema();
    let sort_cases = vec![
        (
            "sort utf8",
            vec![PhysicalSortExpr {
                expr: col("request_method", &schema)?,
                options: Default::default(),
            }],
        ),
        (
            "sort int",
            vec![PhysicalSortExpr {
                expr: col("request_bytes", &schema)?,
                options: Default::default(),
            }],
        ),
        (
            "sort decimal",
            vec![
                // sort decimal
                PhysicalSortExpr {
                    expr: col("decimal_price", &schema)?,
                    options: Default::default(),
                },
            ],
        ),
        (
            "sort integer tuple",
            vec![
                PhysicalSortExpr {
                    expr: col("request_bytes", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("response_bytes", &schema)?,
                    options: Default::default(),
                },
            ],
        ),
        (
            "sort utf8 tuple",
            vec![
                // sort utf8 tuple
                PhysicalSortExpr {
                    expr: col("service", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("host", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("pod", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("image", &schema)?,
                    options: Default::default(),
                },
            ],
        ),
        (
            "sort mixed tuple",
            vec![
                PhysicalSortExpr {
                    expr: col("service", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("request_bytes", &schema)?,
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: col("decimal_price", &schema)?,
                    options: Default::default(),
                },
            ],
        ),
    ];
    for (title, expr) in sort_cases {
        println!("Executing '{title}' (sorting by: {expr:?})");
        rundata.start_new_case(title);
        for i in 0..opt.iterations {
            let config = SessionConfig::new().with_target_partitions(opt.partitions);
            let ctx = SessionContext::with_config(config);
            let (rows, elapsed) = exec_sort(&ctx, &expr, test_file, opt.debug).await?;
            let ms = elapsed.as_secs_f64() * 1000.0;
            println!("Iteration {i} finished in {ms} ms");
            rundata.write_iter(elapsed, rows);
        }
        println!("\n");
    }
    if let Some(path) = &opt.output_path {
        std::fs::write(path, rundata.to_json())?;
    }
    Ok(())
}
fn parquet_scan_disp(opts: &ParquetScanOptions) -> String {
    format!(
        "pushdown_filters={}, reorder_filters={}, page_index={}",
        opts.pushdown_filters, opts.reorder_filters, opts.enable_page_index
    )
}
async fn run_filter_benchmarks(opt: Opt, test_file: &TestParquetFile) -> Result<()> {
    let mut rundata = BenchmarkRun::new();
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
        ("Selective-ish filter", col("request_method").eq(lit("GET"))),
        (
            "Non-selective filter",
            col("request_method").not_eq(lit("GET")),
        ),
        (
            "Basic conjunction",
            col("request_method")
                .eq(lit("POST"))
                .and(col("response_status").eq(lit(503_u16))),
        ),
        (
            "Nested filters",
            col("request_method").eq(lit("POST")).and(or(
                col("response_status").eq(lit(503_u16)),
                col("response_status").eq(lit(403_u16)),
            )),
        ),
        (
            "Many filters",
            disjunction([
                col("request_method").not_eq(lit("GET")),
                col("response_status").eq(lit(400_u16)),
                col("service").eq(lit("backend")),
            ])
            .unwrap(),
        ),
        ("Filter everything", col("response_status").eq(lit(429_u16))),
        ("Filter nothing", col("response_status").gt(lit(0_u16))),
    ];

    for (name, filter_expr) in &filter_matrix {
        println!("Executing '{name}' (filter: {filter_expr})");
        for scan_options in &scan_options_matrix {
            println!("Using scan options {scan_options:?}");
            rundata
                .start_new_case(&format!("{name}: {}", parquet_scan_disp(scan_options)));
            for i in 0..opt.iterations {
                let config = scan_options.config().with_target_partitions(opt.partitions);
                let ctx = SessionContext::with_config(config);

                let (rows, elapsed) =
                    exec_scan(&ctx, test_file, filter_expr.clone(), opt.debug).await?;
                let ms = elapsed.as_secs_f64() * 1000.0;
                println!("Iteration {i} returned {rows} rows in {ms} ms");
                rundata.write_iter(elapsed, rows);
            }
        }
        println!("\n");
    }
    rundata.maybe_write_json(opt.output_path.as_ref())?;
    Ok(())
}

async fn exec_scan(
    ctx: &SessionContext,
    test_file: &TestParquetFile,
    filter: Expr,
    debug: bool,
) -> Result<(usize, std::time::Duration)> {
    let start = Instant::now();
    let exec = test_file.create_scan(Some(filter)).await?;

    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;
    let elapsed = start.elapsed();
    if debug {
        pretty::print_batches(&result)?;
    }
    let rows = result.iter().map(|b| b.num_rows()).sum();
    Ok((rows, elapsed))
}

async fn exec_sort(
    ctx: &SessionContext,
    expr: &[PhysicalSortExpr],
    test_file: &TestParquetFile,
    debug: bool,
) -> Result<(usize, std::time::Duration)> {
    let start = Instant::now();
    let scan = test_file.create_scan(None).await?;
    let exec = Arc::new(SortExec::new(expr.to_owned(), scan));
    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;
    let elapsed = start.elapsed();
    if debug {
        pretty::print_batches(&result)?;
    }
    let rows = result.iter().map(|b| b.num_rows()).sum();
    Ok((rows, elapsed))
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
