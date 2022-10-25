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

use arrow::datatypes::SchemaRef;
use arrow::util::pretty;
use datafusion::common::{Result, ToDFSchema};
use datafusion::config::{
    ConfigOptions, OPT_PARQUET_ENABLE_PAGE_INDEX, OPT_PARQUET_PUSHDOWN_FILTERS,
    OPT_PARQUET_REORDER_FILTERS,
};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{lit, or, Expr};
use datafusion::optimizer::utils::disjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::{col, SessionConfig, SessionContext};
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
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
    println!("Running benchmarks with the following options: {:?}", opt);

    let config = SessionConfig::new().with_target_partitions(opt.partitions);
    let mut ctx = SessionContext::with_config(config);

    let path = opt.path.join("logs.parquet");

    let (schema, object_store_url, object_meta) =
        gen_data(path, opt.scale_factor, opt.page_size, opt.row_group_size)?;

    run_benchmarks(
        &mut ctx,
        schema,
        object_store_url,
        object_meta,
        opt.iterations,
        opt.debug,
    )
    .await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ParquetScanOptions {
    pushdown_filters: bool,
    reorder_filters: bool,
    enable_page_index: bool,
}

async fn run_benchmarks(
    ctx: &mut SessionContext,
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    object_meta: ObjectMeta,
    iterations: usize,
    debug: bool,
) -> Result<()> {
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
            // TODO this fails in the FilterExec with Error: Internal("The type of Dictionary(Int32, Utf8) = Utf8 of binary physical should be same")
            // col("service").eq(lit("backend")),
        ])
        .unwrap(),
        // Filter everything
        col("response_status").eq(lit(429_u16)),
        // Filter nothing
        col("response_status").gt(lit(0_u16)),
    ];

    for filter_expr in &filter_matrix {
        println!("Executing with filter '{}'", filter_expr);
        for scan_options in &scan_options_matrix {
            println!("Using scan options {:?}", scan_options);
            for i in 0..iterations {
                let start = Instant::now();
                let rows = exec_scan(
                    ctx,
                    schema.clone(),
                    object_store_url.clone(),
                    object_meta.clone(),
                    filter_expr.clone(),
                    scan_options.clone(),
                    debug,
                )
                .await?;
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
    schema: SchemaRef,
    object_store_url: ObjectStoreUrl,
    object_meta: ObjectMeta,
    filter: Expr,
    scan_options: ParquetScanOptions,
    debug: bool,
) -> Result<usize> {
    let ParquetScanOptions {
        pushdown_filters,
        reorder_filters,
        enable_page_index,
    } = scan_options;

    let mut config_options = ConfigOptions::new();
    config_options.set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, pushdown_filters);
    config_options.set_bool(OPT_PARQUET_REORDER_FILTERS, reorder_filters);
    config_options.set_bool(OPT_PARQUET_ENABLE_PAGE_INDEX, enable_page_index);

    let scan_config = FileScanConfig {
        object_store_url,
        file_schema: schema.clone(),
        file_groups: vec![vec![PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            extensions: None,
        }]],
        statistics: Default::default(),
        projection: None,
        limit: None,
        table_partition_cols: vec![],
        config_options: config_options.into_shareable(),
    };

    let df_schema = schema.clone().to_dfschema()?;

    let physical_filter_expr = create_physical_expr(
        &filter,
        &df_schema,
        schema.as_ref(),
        &ExecutionProps::default(),
    )?;

    let parquet_exec = Arc::new(ParquetExec::new(scan_config, Some(filter), None));

    let exec = Arc::new(FilterExec::try_new(physical_filter_expr, parquet_exec)?);

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
    page_size: Option<usize>,
    row_group_size: Option<usize>,
) -> Result<(SchemaRef, ObjectStoreUrl, ObjectMeta)> {
    let generator = AccessLogGenerator::new();

    let file = File::create(&path).unwrap();

    let mut props_builder = WriterProperties::builder();

    if let Some(s) = page_size {
        props_builder = props_builder
            .set_data_pagesize_limit(s)
            .set_write_batch_size(s);
    }

    if let Some(s) = row_group_size {
        props_builder = props_builder.set_max_row_group_size(s);
    }

    let schema = generator.schema();
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props_builder.build())).unwrap();

    let mut num_rows = 0;

    let num_batches = 100_f32 * scale_factor;

    for batch in generator.take(num_batches as usize) {
        writer.write(&batch).unwrap();
        writer.flush()?;
        num_rows += batch.num_rows();
    }
    writer.close().unwrap();

    println!("Generated test dataset with {} rows", num_rows);

    let size = std::fs::metadata(&path)?.len() as usize;

    let canonical_path = path.canonicalize()?;

    let object_store_url =
        ListingTableUrl::parse(canonical_path.to_str().unwrap_or_default())?
            .object_store();

    let object_meta = ObjectMeta {
        location: Path::parse(canonical_path.to_str().unwrap_or_default())?,
        last_modified: Default::default(),
        size,
    };

    Ok((schema, object_store_url, object_meta))
}
