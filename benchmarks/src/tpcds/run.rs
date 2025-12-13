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

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::util::{print_memory_stats, BenchmarkRun, CommonOpt, QueryResult};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::{self, pretty_format_batches};
use datafusion::datasource::file_format::parquet::ParquetFormat;
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
use datafusion_common::{plan_err, DEFAULT_PARQUET_EXTENSION};

use log::info;
use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
type BoolDefaultTrue = bool;
pub const TPCDS_QUERY_START_ID: usize = 1;
pub const TPCDS_QUERY_END_ID: usize = 99;

pub const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "customer_address",
    "household_demographics",
    "promotion",
    "store_sales",
    "web_page",
    "catalog_page",
    "customer_demographics",
    "income_band",
    "reason",
    "store",
    "web_returns",
    "catalog_returns",
    "customer",
    "inventory",
    "ship_mode",
    "time_dim",
    "web_sales",
    "catalog_sales",
    "date_dim",
    "item",
    "store_returns",
    "warehouse",
    "web_site",
];

/// Get the SQL statements from the specified query file
pub fn get_query_sql(base_query_path: &str, query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 100 {
        let filename = format!("{base_query_path}/{query}.sql");
        let mut errors = vec![];
        match fs::read_to_string(&filename) {
            Ok(contents) => {
                return Ok(contents
                    .split(';')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect());
            }
            Err(e) => errors.push(format!("{filename}: {e}")),
        };

        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 99")
    }
}

/// Run the tpcds benchmark.
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

    /// Path to query files
    #[structopt(parse(from_os_str), required = true, short = "Q", long = "query_path")]
    query_path: PathBuf,

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

    /// If true then Piecewise Merge Join can be used, if false then it will opt for Nested Loop Join
    /// False by default.
    #[structopt(
        short = "w",
        long = "enable_piecewise_merge_join",
        default_value = "false"
    )]
    enable_piecewise_merge_join: BoolDefaultTrue,

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
            None => TPCDS_QUERY_START_ID..=TPCDS_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        let mut config = self
            .common
            .config()?
            .with_collect_statistics(!self.disable_statistics);
        config.options_mut().optimizer.prefer_hash_join = self.prefer_hash_join;
        config.options_mut().optimizer.enable_piecewise_merge_join =
            self.enable_piecewise_merge_join;
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

        let sql = &get_query_sql(self.query_path.to_str().unwrap(), query_id)?;

        if self.common.debug {
            println!("=== SQL for query {query_id} ===\n{}\n", sql.join(";\n"));
        }

        for i in 0..self.iterations() {
            let start = Instant::now();

            // query 15 is special, with 3 statements. the second statement is the one from which we
            // want to capture the results
            let mut result = vec![];

            for query in sql {
                result = self.execute_query(ctx, query).await?;
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
        for table in TPCDS_TABLES {
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
        let target_partitions = self.partitions();

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let path = format!("{path}/{table}.parquet");

        // Check if the file exists
        if !std::path::Path::new(&path).exists() {
            eprintln!("Warning registering {table}: Table file does not exist: {path}");
        }

        let format = ParquetFormat::default()
            .with_options(ctx.state().table_options().parquet.clone());

        let table_path = ListingTableUrl::parse(path)?;
        let options = ListingOptions::new(Arc::new(format))
            .with_file_extension(DEFAULT_PARQUET_EXTENSION)
            .with_target_partitions(target_partitions)
            .with_collect_stat(state.config().collect_statistics());
        let schema = options.infer_schema(&state, &table_path).await?;

        if self.common.debug {
            println!(
                "Inferred schema from {table_path} for table '{table}':\n{schema:#?}\n"
            );
        }

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
