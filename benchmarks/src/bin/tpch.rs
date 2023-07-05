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

//! Benchmark derived from TPC-H. This is not an official TPC-H benchmark.
use log::info;

use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::{csv::CsvFormat, FileFormat};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::parquet::basic::Compression;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion::{
    arrow::record_batch::RecordBatch, datasource::file_format::parquet::ParquetFormat,
};
use datafusion::{
    arrow::util::pretty,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
};
use datafusion_benchmarks::{tpch::*, BenchmarkRun};
use std::{iter::Iterator, path::PathBuf, sync::Arc, time::Instant};

use datafusion::datasource::file_format::csv::DEFAULT_CSV_EXTENSION;
use datafusion::datasource::file_format::parquet::DEFAULT_PARQUET_EXTENSION;
use datafusion::datasource::listing::ListingTableUrl;
use structopt::StructOpt;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, StructOpt, Clone)]
struct DataFusionBenchmarkOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of partitions to process in parallel
    #[structopt(short = "n", long = "partitions", default_value = "2")]
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

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Whether to disable collection of statistics (and cost based optimizations) or not.
    #[structopt(short = "S", long = "disable-statistics")]
    disable_statistics: bool,
}

#[derive(Debug, StructOpt)]
struct ConvertOpt {
    /// Path to csv files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Output file format: `csv` or `parquet`
    #[structopt(short = "f", long = "format")]
    file_format: String,

    /// Compression to use when writing Parquet files
    #[structopt(short = "c", long = "compression", default_value = "zstd")]
    compression: String,

    /// Number of partitions to produce
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Batch size when reading CSV or Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum BenchmarkSubCommandOpt {
    #[structopt(name = "datafusion")]
    DataFusionBenchmark(DataFusionBenchmarkOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "TPC-H", about = "TPC-H Benchmarks.")]
enum TpchOpt {
    Benchmark(BenchmarkSubCommandOpt),
    Convert(ConvertOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    use BenchmarkSubCommandOpt::*;

    env_logger::init();
    match TpchOpt::from_args() {
        TpchOpt::Benchmark(DataFusionBenchmark(opt)) => {
            benchmark_datafusion(opt).await.map(|_| ())
        }
        TpchOpt::Convert(opt) => {
            let compression = match opt.compression.as_str() {
                "none" => Compression::UNCOMPRESSED,
                "snappy" => Compression::SNAPPY,
                "brotli" => Compression::BROTLI(Default::default()),
                "gzip" => Compression::GZIP(Default::default()),
                "lz4" => Compression::LZ4,
                "lz0" => Compression::LZO,
                "zstd" => Compression::ZSTD(Default::default()),
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Invalid compression format: {other}"
                    )));
                }
            };
            convert_tbl(
                opt.input_path.to_str().unwrap(),
                opt.output_path.to_str().unwrap(),
                &opt.file_format,
                opt.partitions,
                opt.batch_size,
                compression,
            )
            .await
        }
    }
}

const TPCH_QUERY_START_ID: usize = 1;
const TPCH_QUERY_END_ID: usize = 22;

async fn benchmark_datafusion(
    opt: DataFusionBenchmarkOpt,
) -> Result<Vec<Vec<RecordBatch>>> {
    println!("Running benchmarks with the following options: {opt:?}");
    let query_range = match opt.query {
        Some(query_id) => query_id..=query_id,
        None => TPCH_QUERY_START_ID..=TPCH_QUERY_END_ID,
    };

    let mut benchmark_run = BenchmarkRun::new();
    let mut results = vec![];
    for query_id in query_range {
        benchmark_run.start_new_case(&format!("Query {query_id}"));
        let (query_run, result) = benchmark_query(&opt, query_id).await?;
        results.push(result);
        for iter in query_run {
            benchmark_run.write_iter(iter.elapsed, iter.row_count);
        }
    }
    benchmark_run.maybe_write_json(opt.output_path.as_ref())?;
    Ok(results)
}

async fn benchmark_query(
    opt: &DataFusionBenchmarkOpt,
    query_id: usize,
) -> Result<(Vec<QueryResult>, Vec<RecordBatch>)> {
    let mut query_results = vec![];
    let config = SessionConfig::new()
        .with_target_partitions(opt.partitions)
        .with_batch_size(opt.batch_size)
        .with_collect_statistics(!opt.disable_statistics);
    let ctx = SessionContext::with_config(config);

    // register tables
    register_tables(opt, &ctx).await?;

    let mut millis = vec![];
    // run benchmark
    let mut result: Vec<RecordBatch> = Vec::with_capacity(1);
    for i in 0..opt.iterations {
        let start = Instant::now();

        let sql = &get_query_sql(query_id)?;

        // query 15 is special, with 3 statements. the second statement is the one from which we
        // want to capture the results
        if query_id == 15 {
            for (n, query) in sql.iter().enumerate() {
                if n == 1 {
                    result = execute_query(&ctx, query, opt.debug).await?;
                } else {
                    execute_query(&ctx, query, opt.debug).await?;
                }
            }
        } else {
            for query in sql {
                result = execute_query(&ctx, query, opt.debug).await?;
            }
        }

        let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;
        let ms = elapsed.as_secs_f64() * 1000.0;
        millis.push(ms);
        info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
        let row_count = result.iter().map(|b| b.num_rows()).sum();
        println!(
            "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
        );
        query_results.push(QueryResult { elapsed, row_count });
    }

    let avg = millis.iter().sum::<f64>() / millis.len() as f64;
    println!("Query {query_id} avg time: {avg:.2} ms");

    Ok((query_results, result))
}

async fn register_tables(
    opt: &DataFusionBenchmarkOpt,
    ctx: &SessionContext,
) -> Result<()> {
    for table in TPCH_TABLES {
        let table_provider = {
            get_table(
                ctx,
                opt.path.to_str().unwrap(),
                table,
                opt.file_format.as_str(),
                opt.partitions,
            )
            .await?
        };

        if opt.mem_table {
            println!("Loading table '{table}' into memory");
            let start = Instant::now();
            let memtable =
                MemTable::load(table_provider, Some(opt.partitions), &ctx.state())
                    .await?;
            println!(
                "Loaded table '{}' into memory in {} ms",
                table,
                start.elapsed().as_millis()
            );
            ctx.register_table(*table, Arc::new(memtable))?;
        } else {
            ctx.register_table(*table, table_provider)?;
        }
    }
    Ok(())
}

async fn execute_query(
    ctx: &SessionContext,
    sql: &str,
    debug: bool,
) -> Result<Vec<RecordBatch>> {
    let plan = ctx.sql(sql).await?;
    let (state, plan) = plan.into_parts();

    if debug {
        println!("=== Logical plan ===\n{plan:?}\n");
    }

    let plan = state.optimize(&plan)?;
    if debug {
        println!("=== Optimized logical plan ===\n{plan:?}\n");
    }
    let physical_plan = state.create_physical_plan(&plan).await?;
    if debug {
        println!(
            "=== Physical plan ===\n{}\n",
            displayable(physical_plan.as_ref()).indent(true)
        );
    }
    let result = collect(physical_plan.clone(), state.task_ctx()).await?;
    if debug {
        println!(
            "=== Physical plan with metrics ===\n{}\n",
            DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
        );
        if !result.is_empty() {
            // do not call print_batches if there are no batches as the result is confusing
            // and makes it look like there is a batch with no columns
            pretty::print_batches(&result)?;
        }
    }
    Ok(result)
}

async fn get_table(
    ctx: &SessionContext,
    path: &str,
    table: &str,
    table_format: &str,
    target_partitions: usize,
) -> Result<Arc<dyn TableProvider>> {
    // Obtain a snapshot of the SessionState
    let state = ctx.state();
    let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) =
        match table_format {
            // dbgen creates .tbl ('|' delimited) files without header
            "tbl" => {
                let path = format!("{path}/{table}.tbl");

                let format = CsvFormat::default()
                    .with_delimiter(b'|')
                    .with_has_header(false);

                (Arc::new(format), path, ".tbl")
            }
            "csv" => {
                let path = format!("{path}/{table}");
                let format = CsvFormat::default()
                    .with_delimiter(b',')
                    .with_has_header(true);

                (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
            }
            "parquet" => {
                let path = format!("{path}/{table}");
                let format = ParquetFormat::default().with_enable_pruning(Some(true));

                (Arc::new(format), path, DEFAULT_PARQUET_EXTENSION)
            }
            other => {
                unimplemented!("Invalid file format '{}'", other);
            }
        };

    let options = ListingOptions::new(format)
        .with_file_extension(extension)
        .with_target_partitions(target_partitions)
        .with_collect_stat(state.config().collect_statistics());

    let table_path = ListingTableUrl::parse(path)?;
    let config = ListingTableConfig::new(table_path).with_listing_options(options);

    let config = match table_format {
        "parquet" => config.infer_schema(&state).await?,
        "tbl" => config.with_schema(Arc::new(get_tbl_tpch_table_schema(table))),
        "csv" => config.with_schema(Arc::new(get_tpch_table_schema(table))),
        _ => unreachable!(),
    };

    Ok(Arc::new(ListingTable::try_new(config)?))
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}

#[cfg(test)]
#[cfg(feature = "ci")]
/// CI checks
mod tests {
    use std::path::Path;

    use super::*;
    use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};

    async fn serde_round_trip(query: usize) -> Result<()> {
        let ctx = SessionContext::default();
        let path = get_tpch_data_path()?;
        let opt = DataFusionBenchmarkOpt {
            query: Some(query),
            debug: false,
            iterations: 1,
            partitions: 2,
            batch_size: 8192,
            path: PathBuf::from(path.to_string()),
            file_format: "tbl".to_string(),
            mem_table: false,
            output_path: None,
            disable_statistics: false,
        };
        register_tables(&opt, &ctx).await?;
        let queries = get_query_sql(query)?;
        for query in queries {
            let plan = ctx.sql(&query).await?;
            let plan = plan.into_optimized_plan()?;
            let bytes = logical_plan_to_bytes(&plan)?;
            let plan2 = logical_plan_from_bytes(&bytes, &ctx)?;
            let plan_formatted = format!("{}", plan.display_indent());
            let plan2_formatted = format!("{}", plan2.display_indent());
            assert_eq!(plan_formatted, plan2_formatted);
        }
        Ok(())
    }

    #[tokio::test]
    async fn serde_q1() -> Result<()> {
        serde_round_trip(1).await
    }

    #[tokio::test]
    async fn serde_q2() -> Result<()> {
        serde_round_trip(2).await
    }

    #[tokio::test]
    async fn serde_q3() -> Result<()> {
        serde_round_trip(3).await
    }

    #[tokio::test]
    async fn serde_q4() -> Result<()> {
        serde_round_trip(4).await
    }

    #[tokio::test]
    async fn serde_q5() -> Result<()> {
        serde_round_trip(5).await
    }

    #[tokio::test]
    async fn serde_q6() -> Result<()> {
        serde_round_trip(6).await
    }

    #[tokio::test]
    async fn serde_q7() -> Result<()> {
        serde_round_trip(7).await
    }

    #[tokio::test]
    async fn serde_q8() -> Result<()> {
        serde_round_trip(8).await
    }

    #[tokio::test]
    async fn serde_q9() -> Result<()> {
        serde_round_trip(9).await
    }

    #[tokio::test]
    async fn serde_q10() -> Result<()> {
        serde_round_trip(10).await
    }

    #[tokio::test]
    async fn serde_q11() -> Result<()> {
        serde_round_trip(11).await
    }

    #[tokio::test]
    async fn serde_q12() -> Result<()> {
        serde_round_trip(12).await
    }

    #[tokio::test]
    async fn serde_q13() -> Result<()> {
        serde_round_trip(13).await
    }

    #[tokio::test]
    async fn serde_q14() -> Result<()> {
        serde_round_trip(14).await
    }

    #[tokio::test]
    async fn serde_q15() -> Result<()> {
        serde_round_trip(15).await
    }

    #[tokio::test]
    async fn serde_q16() -> Result<()> {
        serde_round_trip(16).await
    }

    #[tokio::test]
    async fn serde_q17() -> Result<()> {
        serde_round_trip(17).await
    }

    #[tokio::test]
    async fn serde_q18() -> Result<()> {
        serde_round_trip(18).await
    }

    #[tokio::test]
    async fn serde_q19() -> Result<()> {
        serde_round_trip(19).await
    }

    #[tokio::test]
    async fn serde_q20() -> Result<()> {
        serde_round_trip(20).await
    }

    #[tokio::test]
    async fn serde_q21() -> Result<()> {
        serde_round_trip(21).await
    }

    #[tokio::test]
    async fn serde_q22() -> Result<()> {
        serde_round_trip(22).await
    }

    fn get_tpch_data_path() -> Result<String> {
        let path =
            std::env::var("TPCH_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
        if !Path::new(&path).exists() {
            return Err(DataFusionError::Execution(format!(
                "Benchmark data not found (set TPCH_DATA env var to override): {}",
                path
            )));
        }
        Ok(path)
    }
}
