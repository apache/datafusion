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

//! Apache Arrow Rust Benchmarks

use std::collections::HashMap;
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty;

use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};

use datafusion::prelude::{CsvReadOptions, ParquetReadOptions};
use datafusion_benchmarks::BenchmarkRun;
use structopt::StructOpt;

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
    #[structopt(short = "p", long = "partitions", default_value = "2")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "csv")]
    file_format: String,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    println!("Running benchmarks with the following options: {opt:?}");

    let config = SessionConfig::new()
        .with_target_partitions(opt.partitions)
        .with_batch_size(opt.batch_size);
    let mut ctx = SessionContext::with_config(config);

    let path = opt.path.to_str().unwrap();

    match opt.file_format.as_str() {
        "csv" => {
            let schema = nyctaxi_schema();
            let options = CsvReadOptions::new().schema(&schema).has_header(true);
            ctx.register_csv("tripdata", path, options).await?
        }
        "parquet" => {
            ctx.register_parquet("tripdata", path, ParquetReadOptions::default())
                .await?
        }
        other => {
            println!("Invalid file format '{other}'");
            process::exit(-1);
        }
    }

    datafusion_sql_benchmarks(&mut ctx, opt).await
}

async fn datafusion_sql_benchmarks(ctx: &mut SessionContext, opt: Opt) -> Result<()> {
    let iterations = opt.iterations;
    let debug = opt.debug;
    let output = opt.output_path;
    let mut rundata = BenchmarkRun::new();
    let mut queries = HashMap::new();
    queries.insert("fare_amt_by_passenger", "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), SUM(fare_amount) FROM tripdata GROUP BY passenger_count");
    for (name, sql) in &queries {
        println!("Executing '{name}'");
        rundata.start_new_case(name);
        for i in 0..iterations {
            let (rows, elapsed) = execute_sql(ctx, sql, debug).await?;
            println!(
                "Query '{}' iteration {} took {} ms",
                name,
                i,
                elapsed.as_secs_f64() * 1000.0
            );
            rundata.write_iter(elapsed, rows);
        }
    }
    rundata.maybe_write_json(output.as_ref())?;
    Ok(())
}

async fn execute_sql(
    ctx: &SessionContext,
    sql: &str,
    debug: bool,
) -> Result<(usize, std::time::Duration)> {
    let start = Instant::now();
    let dataframe = ctx.sql(sql).await?;
    if debug {
        println!("Optimized logical plan:\n{:?}", dataframe.logical_plan());
    }
    let result = dataframe.collect().await?;
    let elapsed = start.elapsed();
    if debug {
        pretty::print_batches(&result)?;
    }
    let rowcount = result.iter().map(|b| b.num_rows()).sum();
    Ok((rowcount, elapsed))
}

fn nyctaxi_schema() -> Schema {
    Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Int32, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ])
}
