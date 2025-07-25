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

use std::path::PathBuf;
use std::sync::Arc;

use super::{
    get_query_sql, get_tbl_tpch_table_schema, get_tpch_table_schema, TPCH_QUERY_END_ID,
    TPCH_QUERY_START_ID, TPCH_TABLES,
};
use crate::util::{print_memory_stats, BenchmarkRun, CommonOpt, QueryResult};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::{self, pretty_format_batches};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common::{DEFAULT_CSV_EXTENSION, DEFAULT_PARQUET_EXTENSION};

use log::info;
use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
type BoolDefaultTrue = bool;

/// Run the tpch benchmark.
///
/// This benchmarks is derived from the [TPC-H][1] version
/// [2.17.1]. The data and answers are generated using `tpch-gen` from
/// [2].
///
/// [1]: http://www.tpc.org/tpch/
/// [2]: https://github.com/databricks/tpch-dbgen.git
/// [2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    pub query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

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

    /// If true then hash join used, if false then sort merge join
    /// True by default.
    #[structopt(short = "j", long = "prefer_hash_join", default_value = "true")]
    prefer_hash_join: BoolDefaultTrue,

    /// Mark the first column of each table as sorted in ascending order.
    /// The tables should have been created with the `--sort` option for this to have any effect.
    #[structopt(short = "t", long = "sorted")]
    sorted: bool,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => TPCH_QUERY_START_ID..=TPCH_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        let mut config = self
            .common
            .config()?
            .with_collect_statistics(!self.disable_statistics);
        config.options_mut().optimizer.prefer_hash_join = self.prefer_hash_join;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);
        // register tables
        self.register_tables(&ctx).await?;

        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(query_id, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    benchmark_run.mark_failed();
                    eprintln!("Query {query_id} failed: {e}");
                }
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        benchmark_run.maybe_print_failures();
        Ok(())
    }

    async fn benchmark_query(
        &self,
        query_id: usize,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];

        let sql = &get_query_sql(query_id)?;

        for i in 0..self.iterations() {
            let start = Instant::now();

            // query 15 is special, with 3 statements. the second statement is the one from which we
            // want to capture the results
            let mut result = vec![];
            if query_id == 15 {
                for (n, query) in sql.iter().enumerate() {
                    if n == 1 {
                        result = self.execute_query(ctx, query).await?;
                    } else {
                        self.execute_query(ctx, query).await?;
                    }
                }
            } else {
                for query in sql {
                    result = self.execute_query(ctx, query).await?;
                }
            }

            let elapsed = start.elapsed();
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

        // Print memory stats using mimalloc (only when compiled with --features mimalloc_extended)
        print_memory_stats();

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in TPCH_TABLES {
            let table_provider = { self.get_table(ctx, table).await? };

            if self.mem_table {
                println!("Loading table '{table}' into memory");
                let start = Instant::now();
                let memtable =
                    MemTable::load(table_provider, Some(self.partitions()), &ctx.state())
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
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
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
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                    .indent(true)
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
        &self,
        ctx: &SessionContext,
        table: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let path = self.path.to_str().unwrap();
        let table_format = self.file_format.as_str();
        let target_partitions = self.partitions();

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
                    let path = format!("{path}/csv/{table}");
                    let format = CsvFormat::default()
                        .with_delimiter(b',')
                        .with_has_header(true);

                    (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
                }
                "parquet" => {
                    let path = format!("{path}/{table}");
                    let format = ParquetFormat::default()
                        .with_options(ctx.state().table_options().parquet.clone());

                    (Arc::new(format), path, DEFAULT_PARQUET_EXTENSION)
                }
                other => {
                    unimplemented!("Invalid file format '{}'", other);
                }
            };

        let table_path = ListingTableUrl::parse(path)?;
        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_target_partitions(target_partitions)
            .with_collect_stat(state.config().collect_statistics());
        let schema = match table_format {
            "parquet" => options.infer_schema(&state, &table_path).await?,
            "tbl" => Arc::new(get_tbl_tpch_table_schema(table)),
            "csv" => Arc::new(get_tpch_table_schema(table)),
            _ => unreachable!(),
        };
        let options = if self.sorted {
            let key_column_name = schema.fields()[0].name();
            options
                .with_file_sort_order(vec![vec![col(key_column_name).sort(true, false)]])
        } else {
            options
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(schema);

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        self.common
            .partitions
            .unwrap_or_else(get_available_parallelism)
    }
}

#[cfg(test)]
// Only run with "ci" mode when we have the data
#[cfg(feature = "ci")]
mod tests {
    use std::path::Path;

    use super::*;

    use datafusion::common::exec_err;
    use datafusion::error::Result;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes, logical_plan_to_bytes, physical_plan_from_bytes,
        physical_plan_to_bytes,
    };

    fn get_tpch_data_path() -> Result<String> {
        let path =
            std::env::var("TPCH_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
        if !Path::new(&path).exists() {
            return exec_err!(
                "Benchmark data not found (set TPCH_DATA env var to override): {}",
                path
            );
        }
        Ok(path)
    }

    async fn round_trip_logical_plan(query: usize) -> Result<()> {
        let ctx = SessionContext::default();
        let path = get_tpch_data_path()?;
        let common = CommonOpt {
            iterations: 1,
            partitions: Some(2),
            batch_size: Some(8192),
            mem_pool_type: "fair".to_string(),
            memory_limit: None,
            sort_spill_reservation_bytes: None,
            debug: false,
        };
        let opt = RunOpt {
            query: Some(query),
            common,
            path: PathBuf::from(path.to_string()),
            file_format: "tbl".to_string(),
            mem_table: false,
            output_path: None,
            disable_statistics: false,
            prefer_hash_join: true,
            sorted: false,
        };
        opt.register_tables(&ctx).await?;
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

    async fn round_trip_physical_plan(query: usize) -> Result<()> {
        let ctx = SessionContext::default();
        let path = get_tpch_data_path()?;
        let common = CommonOpt {
            iterations: 1,
            partitions: Some(2),
            batch_size: Some(8192),
            mem_pool_type: "fair".to_string(),
            memory_limit: None,
            sort_spill_reservation_bytes: None,
            debug: false,
        };
        let opt = RunOpt {
            query: Some(query),
            common,
            path: PathBuf::from(path.to_string()),
            file_format: "tbl".to_string(),
            mem_table: false,
            output_path: None,
            disable_statistics: false,
            prefer_hash_join: true,
            sorted: false,
        };
        opt.register_tables(&ctx).await?;
        let queries = get_query_sql(query)?;
        for query in queries {
            let plan = ctx.sql(&query).await?;
            let plan = plan.create_physical_plan().await?;
            let bytes = physical_plan_to_bytes(plan.clone())?;
            let plan2 = physical_plan_from_bytes(&bytes, &ctx)?;
            let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
            let plan2_formatted =
                format!("{}", displayable(plan2.as_ref()).indent(false));
            assert_eq!(plan_formatted, plan2_formatted);
        }
        Ok(())
    }

    macro_rules! test_round_trip_logical {
        ($tn:ident, $query:expr) => {
            #[tokio::test]
            async fn $tn() -> Result<()> {
                round_trip_logical_plan($query).await
            }
        };
    }

    macro_rules! test_round_trip_physical {
        ($tn:ident, $query:expr) => {
            #[tokio::test]
            async fn $tn() -> Result<()> {
                round_trip_physical_plan($query).await
            }
        };
    }

    // logical plan tests
    test_round_trip_logical!(round_trip_logical_plan_q1, 1);
    test_round_trip_logical!(round_trip_logical_plan_q2, 2);
    test_round_trip_logical!(round_trip_logical_plan_q3, 3);
    test_round_trip_logical!(round_trip_logical_plan_q4, 4);
    test_round_trip_logical!(round_trip_logical_plan_q5, 5);
    test_round_trip_logical!(round_trip_logical_plan_q6, 6);
    test_round_trip_logical!(round_trip_logical_plan_q7, 7);
    test_round_trip_logical!(round_trip_logical_plan_q8, 8);
    test_round_trip_logical!(round_trip_logical_plan_q9, 9);
    test_round_trip_logical!(round_trip_logical_plan_q10, 10);
    test_round_trip_logical!(round_trip_logical_plan_q11, 11);
    test_round_trip_logical!(round_trip_logical_plan_q12, 12);
    test_round_trip_logical!(round_trip_logical_plan_q13, 13);
    test_round_trip_logical!(round_trip_logical_plan_q14, 14);
    test_round_trip_logical!(round_trip_logical_plan_q15, 15);
    test_round_trip_logical!(round_trip_logical_plan_q16, 16);
    test_round_trip_logical!(round_trip_logical_plan_q17, 17);
    test_round_trip_logical!(round_trip_logical_plan_q18, 18);
    test_round_trip_logical!(round_trip_logical_plan_q19, 19);
    test_round_trip_logical!(round_trip_logical_plan_q20, 20);
    test_round_trip_logical!(round_trip_logical_plan_q21, 21);
    test_round_trip_logical!(round_trip_logical_plan_q22, 22);

    // physical plan tests
    test_round_trip_physical!(round_trip_physical_plan_q1, 1);
    test_round_trip_physical!(round_trip_physical_plan_q2, 2);
    test_round_trip_physical!(round_trip_physical_plan_q3, 3);
    test_round_trip_physical!(round_trip_physical_plan_q4, 4);
    test_round_trip_physical!(round_trip_physical_plan_q5, 5);
    test_round_trip_physical!(round_trip_physical_plan_q6, 6);
    test_round_trip_physical!(round_trip_physical_plan_q7, 7);
    test_round_trip_physical!(round_trip_physical_plan_q8, 8);
    test_round_trip_physical!(round_trip_physical_plan_q9, 9);
    test_round_trip_physical!(round_trip_physical_plan_q10, 10);
    test_round_trip_physical!(round_trip_physical_plan_q11, 11);
    test_round_trip_physical!(round_trip_physical_plan_q12, 12);
    test_round_trip_physical!(round_trip_physical_plan_q13, 13);
    test_round_trip_physical!(round_trip_physical_plan_q14, 14);
    test_round_trip_physical!(round_trip_physical_plan_q15, 15);
    test_round_trip_physical!(round_trip_physical_plan_q16, 16);
    test_round_trip_physical!(round_trip_physical_plan_q17, 17);
    test_round_trip_physical!(round_trip_physical_plan_q18, 18);
    test_round_trip_physical!(round_trip_physical_plan_q19, 19);
    test_round_trip_physical!(round_trip_physical_plan_q20, 20);
    test_round_trip_physical!(round_trip_physical_plan_q21, 21);
    test_round_trip_physical!(round_trip_physical_plan_q22, 22);
}
