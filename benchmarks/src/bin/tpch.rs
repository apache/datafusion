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
            displayable(physical_plan.as_ref()).indent()
        );
    }
    let result = collect(physical_plan.clone(), state.task_ctx()).await?;
    if debug {
        println!(
            "=== Physical plan with metrics ===\n{}\n",
            DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent()
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
mod tests {
    use super::*;
    use datafusion::config::ConfigOptions;
    use datafusion::sql::TableReference;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::path::Path;
    use std::sync::Arc;

    #[tokio::test]
    async fn q1_expected_plan() -> Result<()> {
        expected_plan(1).await
    }

    #[tokio::test]
    async fn q2_expected_plan() -> Result<()> {
        expected_plan(2).await
    }

    #[tokio::test]
    async fn q3_expected_plan() -> Result<()> {
        expected_plan(3).await
    }

    #[tokio::test]
    async fn q4_expected_plan() -> Result<()> {
        expected_plan(4).await
    }

    #[tokio::test]
    async fn q5_expected_plan() -> Result<()> {
        expected_plan(5).await
    }

    #[tokio::test]
    async fn q6_expected_plan() -> Result<()> {
        expected_plan(6).await
    }

    #[tokio::test]
    async fn q7_expected_plan() -> Result<()> {
        expected_plan(7).await
    }

    #[tokio::test]
    async fn q8_expected_plan() -> Result<()> {
        expected_plan(8).await
    }

    #[tokio::test]
    async fn q9_expected_plan() -> Result<()> {
        expected_plan(9).await
    }

    #[tokio::test]
    async fn q10_expected_plan() -> Result<()> {
        expected_plan(10).await
    }

    #[tokio::test]
    async fn q11_expected_plan() -> Result<()> {
        expected_plan(11).await
    }

    #[tokio::test]
    async fn q12_expected_plan() -> Result<()> {
        expected_plan(12).await
    }

    #[tokio::test]
    async fn q13_expected_plan() -> Result<()> {
        expected_plan(13).await
    }

    #[tokio::test]
    async fn q14_expected_plan() -> Result<()> {
        expected_plan(14).await
    }

    #[tokio::test]
    async fn q15_expected_plan() -> Result<()> {
        expected_plan(15).await
    }

    #[tokio::test]
    async fn q16_expected_plan() -> Result<()> {
        expected_plan(16).await
    }

    #[tokio::test]
    async fn q17_expected_plan() -> Result<()> {
        expected_plan(17).await
    }

    #[tokio::test]
    async fn q18_expected_plan() -> Result<()> {
        expected_plan(18).await
    }

    #[tokio::test]
    async fn q19_expected_plan() -> Result<()> {
        expected_plan(19).await
    }

    #[tokio::test]
    async fn q20_expected_plan() -> Result<()> {
        expected_plan(20).await
    }

    #[tokio::test]
    async fn q21_expected_plan() -> Result<()> {
        expected_plan(21).await
    }

    #[tokio::test]
    async fn q22_expected_plan() -> Result<()> {
        expected_plan(22).await
    }

    async fn expected_plan(query: usize) -> Result<()> {
        let ctx = create_context()?;
        let mut actual = String::new();
        let sql = get_query_sql(query)?;
        for sql in &sql {
            // handle special q15 which contains "create view" sql statement
            if sql.starts_with("select") {
                let explain = "explain ".to_string() + sql;
                let result_batch = execute_query(&ctx, explain.as_str(), false).await?;
                if !actual.is_empty() {
                    actual += "\n";
                }
                use std::fmt::Write as _;
                write!(actual, "{}", pretty::pretty_format_batches(&result_batch)?)
                    .unwrap();
                // write to file for debugging
                // use std::io::Write;
                // let mut file = File::create(format!("expected-plans/q{}.txt", query))?;
                // file.write_all(actual.as_bytes())?;
            } else {
                execute_query(&ctx, sql.as_str(), false).await?;
            }
        }

        let possibilities = vec![
            format!("expected-plans/q{query}.txt"),
            format!("benchmarks/expected-plans/q{query}.txt"),
        ];

        let mut found = false;
        for path in &possibilities {
            let path = Path::new(&path);
            if let Ok(expected) = read_text_file(path) {
                assert_eq!(expected, actual,
                           // generate output that is easier to copy/paste/update
                           "\n\nMismatch of expected content in: {path:?}\nExpected:\n\n{expected}\n\nActual:\n\n{actual}\n\n");
                found = true;
                break;
            }
        }
        assert!(found);

        Ok(())
    }

    fn create_context() -> Result<SessionContext> {
        let mut config = ConfigOptions::new();
        // Ensure that the generated physical plans are the same in different machines.
        config.execution.target_partitions = 2;
        let ctx = SessionContext::with_config(config.into());
        for table in TPCH_TABLES {
            let table = table.to_string();
            let schema = get_tpch_table_schema(&table);
            let mem_table = MemTable::try_new(Arc::new(schema), vec![])?;
            ctx.register_table(
                TableReference::from(table.as_str()),
                Arc::new(mem_table),
            )?;
        }
        Ok(ctx)
    }

    /// we need to read line by line and add \n so tests work on Windows
    fn read_text_file(path: &Path) -> Result<String> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut str = String::new();
        for line in reader.lines() {
            let line = line?;
            if !str.is_empty() {
                str += "\n";
            }
            str += &line;
        }
        Ok(str)
    }

    #[tokio::test]
    async fn run_q1() -> Result<()> {
        run_query(1).await
    }

    #[tokio::test]
    async fn run_q2() -> Result<()> {
        run_query(2).await
    }

    #[tokio::test]
    async fn run_q3() -> Result<()> {
        run_query(3).await
    }

    #[tokio::test]
    async fn run_q4() -> Result<()> {
        run_query(4).await
    }

    #[tokio::test]
    async fn run_q5() -> Result<()> {
        run_query(5).await
    }

    #[tokio::test]
    async fn run_q6() -> Result<()> {
        run_query(6).await
    }

    #[tokio::test]
    async fn run_q7() -> Result<()> {
        run_query(7).await
    }

    #[tokio::test]
    async fn run_q8() -> Result<()> {
        run_query(8).await
    }

    #[tokio::test]
    async fn run_q9() -> Result<()> {
        run_query(9).await
    }

    #[tokio::test]
    async fn run_q10() -> Result<()> {
        run_query(10).await
    }

    #[tokio::test]
    async fn run_q11() -> Result<()> {
        run_query(11).await
    }

    #[tokio::test]
    async fn run_q12() -> Result<()> {
        run_query(12).await
    }

    #[tokio::test]
    async fn run_q13() -> Result<()> {
        run_query(13).await
    }

    #[tokio::test]
    async fn run_q14() -> Result<()> {
        run_query(14).await
    }

    #[tokio::test]
    async fn run_q15() -> Result<()> {
        run_query(15).await
    }

    #[tokio::test]
    async fn run_q16() -> Result<()> {
        run_query(16).await
    }

    #[tokio::test]
    async fn run_q17() -> Result<()> {
        run_query(17).await
    }

    #[tokio::test]
    async fn run_q18() -> Result<()> {
        run_query(18).await
    }

    #[tokio::test]
    async fn run_q19() -> Result<()> {
        run_query(19).await
    }

    #[tokio::test]
    async fn run_q20() -> Result<()> {
        run_query(20).await
    }

    #[tokio::test]
    async fn run_q21() -> Result<()> {
        run_query(21).await
    }

    #[tokio::test]
    async fn run_q22() -> Result<()> {
        run_query(22).await
    }

    async fn run_query(n: usize) -> Result<()> {
        // Tests running query with empty tables, to see whether they run successfully.

        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(10);
        let ctx = SessionContext::with_config(config);

        for &table in TPCH_TABLES {
            let schema = get_tpch_table_schema(table);
            let batch = RecordBatch::new_empty(Arc::new(schema.to_owned()));

            ctx.register_batch(table, batch)?;
        }

        let sql = &get_query_sql(n)?;
        for query in sql {
            execute_query(&ctx, query, false).await?;
        }

        Ok(())
    }
}

/// CI checks
#[cfg(test)]
#[cfg(feature = "ci")]
mod ci {
    use std::path::Path;

    use super::*;
    use arrow::datatypes::{DataType, Field};
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

    #[tokio::test]
    async fn verify_q1() -> Result<()> {
        verify_query(1).await
    }

    #[tokio::test]
    async fn verify_q2() -> Result<()> {
        verify_query(2).await
    }

    #[tokio::test]
    async fn verify_q3() -> Result<()> {
        verify_query(3).await
    }

    #[tokio::test]
    async fn verify_q4() -> Result<()> {
        verify_query(4).await
    }

    #[tokio::test]
    async fn verify_q5() -> Result<()> {
        verify_query(5).await
    }

    #[tokio::test]
    async fn verify_q6() -> Result<()> {
        verify_query(6).await
    }

    #[tokio::test]
    async fn verify_q7() -> Result<()> {
        verify_query(7).await
    }

    #[tokio::test]
    async fn verify_q8() -> Result<()> {
        verify_query(8).await
    }

    #[tokio::test]
    async fn verify_q9() -> Result<()> {
        verify_query(9).await
    }

    #[tokio::test]
    async fn verify_q10() -> Result<()> {
        verify_query(10).await
    }

    #[tokio::test]
    async fn verify_q11() -> Result<()> {
        verify_query(11).await
    }

    #[tokio::test]
    async fn verify_q12() -> Result<()> {
        verify_query(12).await
    }

    #[tokio::test]
    async fn verify_q13() -> Result<()> {
        verify_query(13).await
    }

    #[tokio::test]
    async fn verify_q14() -> Result<()> {
        verify_query(14).await
    }

    #[tokio::test]
    async fn verify_q15() -> Result<()> {
        verify_query(15).await
    }

    #[tokio::test]
    async fn verify_q16() -> Result<()> {
        verify_query(16).await
    }

    #[tokio::test]
    async fn verify_q17() -> Result<()> {
        verify_query(17).await
    }

    #[tokio::test]
    async fn verify_q18() -> Result<()> {
        verify_query(18).await
    }

    #[tokio::test]
    async fn verify_q19() -> Result<()> {
        verify_query(19).await
    }

    #[tokio::test]
    async fn verify_q20() -> Result<()> {
        verify_query(20).await
    }

    #[tokio::test]
    async fn verify_q21() -> Result<()> {
        verify_query(21).await
    }

    #[tokio::test]
    async fn verify_q22() -> Result<()> {
        verify_query(22).await
    }

    /// compares query results against stored answers from the git repo
    /// verifies that:
    ///  * datatypes returned in columns is correct
    ///  * the correct number of rows are returned
    ///  * the content of the rows is correct
    async fn verify_query(n: usize) -> Result<()> {
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::expr::Cast;

        let path = get_tpch_data_path()?;

        let answer_file = format!("{}/answers/q{}.out", path, n);
        if !Path::new(&answer_file).exists() {
            return Err(DataFusionError::Execution(format!(
                "Expected results not found: {}",
                answer_file
            )));
        }

        // load expected answers from tpch-dbgen
        // read csv as all strings, trim and cast to expected type as the csv string
        // to value parser does not handle data with leading/trailing spaces
        let ctx = SessionContext::new();
        let schema = string_schema(get_answer_schema(n));
        let options = CsvReadOptions::new()
            .schema(&schema)
            .delimiter(b'|')
            .file_extension(".out");
        let df = ctx.read_csv(&answer_file, options).await?;
        let df = df.select(
            get_answer_schema(n)
                .fields()
                .iter()
                .map(|field| {
                    match Field::data_type(field) {
                        DataType::Decimal128(_, _) => {
                            // there's no support for casting from Utf8 to Decimal, so
                            // we'll cast from Utf8 to Float64 to Decimal for Decimal types
                            let inner_cast = Box::new(Expr::Cast(Cast::new(
                                Box::new(trim(col(Field::name(field)))),
                                DataType::Float64,
                            )));
                            Expr::Cast(Cast::new(
                                inner_cast,
                                Field::data_type(field).to_owned(),
                            ))
                            .alias(Field::name(field))
                        }
                        _ => Expr::Cast(Cast::new(
                            Box::new(trim(col(Field::name(field)))),
                            Field::data_type(field).to_owned(),
                        ))
                        .alias(Field::name(field)),
                    }
                })
                .collect::<Vec<Expr>>(),
        )?;
        let expected = df.collect().await?;

        // run the query to compute actual results of the query
        let opt = DataFusionBenchmarkOpt {
            query: Some(n),
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
        let mut results = benchmark_datafusion(opt).await?;
        assert_eq!(results.len(), 1);

        let actual = results.remove(0);
        let transformed = transform_actual_result(actual, n).await?;

        // assert schema data types match
        let transformed_fields = &transformed[0].schema().fields;
        let expected_fields = &expected[0].schema().fields;
        let schema_matches =
            transformed_fields
                .iter()
                .zip(expected_fields.iter())
                .all(|(t, e)| match t.data_type() {
                    DataType::Decimal128(_, _) => {
                        matches!(e.data_type(), DataType::Decimal128(_, _))
                    }
                    data_type => data_type == e.data_type(),
                });
        if !schema_matches {
            panic!(
                "expected_fields: {:?}\ntransformed_fields: {:?}",
                expected_fields, transformed_fields
            )
        }

        // convert both datasets to Vec<Vec<String>> for simple comparison
        let expected_vec = result_vec(&expected);
        let actual_vec = result_vec(&transformed);

        // basic result comparison
        assert_eq!(expected_vec.len(), actual_vec.len());

        // compare each row. this works as all TPC-H queries have deterministically ordered results
        for i in 0..expected_vec.len() {
            let expected_row = &expected_vec[i];
            let actual_row = &actual_vec[i];
            assert_eq!(expected_row.len(), actual_row.len());

            let tolerance = 0.1;
            for j in 0..expected_row.len() {
                match (&expected_row[j], &actual_row[j]) {
                    (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
                        // allow for rounding errors until we move to decimal types
                        if (l - r).abs() > tolerance {
                            panic!(
                                "Expected: {}; Actual: {}; Tolerance: {}",
                                l, r, tolerance
                            )
                        }
                    }
                    (
                        ScalarValue::Decimal128(Some(l), _, s),
                        ScalarValue::Decimal128(Some(r), _, _),
                    ) => {
                        if ((l - r) as f64 / 10_i32.pow(*s as u32) as f64).abs()
                            > tolerance
                        {
                            panic!(
                                "Expected: {}; Actual: {}; Tolerance: {}",
                                l, r, tolerance
                            )
                        }
                    }
                    (l, r) => assert_eq!(format!("{:?}", l), format!("{:?}", r)),
                }
            }
        }

        Ok(())
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
