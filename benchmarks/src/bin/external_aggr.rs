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

//! external_aggr binary entrypoint

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use structopt::StructOpt;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::memory_pool::{human_readable_size, units};
use datafusion::execution::runtime_env::RuntimeConfig;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_benchmarks::util::{BenchmarkRun, CommonOpt};
use datafusion_common::instant::Instant;
use datafusion_common::{exec_datafusion_err, exec_err, DEFAULT_PARQUET_EXTENSION};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "datafusion-external-aggregation",
    about = "DataFusion external aggregation benchmark"
)]
enum ExternalAggrOpt {
    Benchmark(ExternalAggrConfig),
}

#[derive(Debug, StructOpt)]
struct ExternalAggrConfig {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Memory limit (e.g. '100M', '1.5G'). If not specified, run all pre-defined memory limits for given query.
    #[structopt(long)]
    memory_limit: Option<String>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to data files (lineitem). Only parquet format is supported
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,

    /// Path to JSON benchmark result to be compare using `compare.py`
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}

/// Query Memory Limits
/// Map query id to predefined memory limits
///
/// Q1 requires 36MiB for aggregation
/// Memory limits to run: 64MiB, 32MiB, 16MiB
/// Q2 requires 250MiB for aggregation
/// Memory limits to run: 512MiB, 256MiB, 128MiB, 64MiB, 32MiB
static QUERY_MEMORY_LIMITS: OnceLock<HashMap<usize, Vec<u64>>> = OnceLock::new();

impl ExternalAggrConfig {
    const AGGR_TABLES: [&'static str; 1] = ["lineitem"];
    const AGGR_QUERIES: [&'static str; 2] = [
        // Q1: Output size is ~25% of lineitem table
        r#"
        SELECT count(*)
        FROM (
            SELECT DISTINCT l_orderkey
            FROM lineitem
        )
        "#,
        // Q2: Output size is ~99% of lineitem table
        r#"
        SELECT count(*)
        FROM (
            SELECT DISTINCT l_orderkey, l_suppkey
            FROM lineitem
        )
        "#,
    ];

    fn init_query_memory_limits() -> &'static HashMap<usize, Vec<u64>> {
        use units::*;
        QUERY_MEMORY_LIMITS.get_or_init(|| {
            let mut map = HashMap::new();
            map.insert(1, vec![64 * MB, 32 * MB, 16 * MB]);
            map.insert(2, vec![512 * MB, 256 * MB, 128 * MB, 64 * MB, 32 * MB]);
            map
        })
    }

    /// If `--query` and `--memory-limit` is not speicified, run all queries
    /// with pre-configured memory limits
    /// If only `--query` is specified, run the query with all memory limits
    /// for this query
    /// If both `--query` and `--memory-limit` are specified, run the query
    /// with the specified memory limit
    pub async fn run(&self) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        let memory_limit = match &self.memory_limit {
            Some(limit) => Some(Self::parse_memory_limit(limit)?),
            None => None,
        };

        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => 1..=Self::AGGR_QUERIES.len(),
        };

        // Each element is (query_id, memory_limit)
        // e.g. [(1, 64_000), (1, 32_000)...] means first run Q1 with 64KiB
        // memory limit, next run Q1 with 32KiB memory limit, etc.
        let mut query_executions = vec![];
        // Setup `query_executions`
        for query_id in query_range {
            if query_id > Self::AGGR_QUERIES.len() {
                return exec_err!(
                    "Invalid '--query'(query number) {} for external aggregation benchmark.",
                    query_id
                );
            }

            match memory_limit {
                Some(limit) => {
                    query_executions.push((query_id, limit));
                }
                None => {
                    let memory_limits_table = Self::init_query_memory_limits();
                    let memory_limits = memory_limits_table.get(&query_id).unwrap();
                    for limit in memory_limits {
                        query_executions.push((query_id, *limit));
                    }
                }
            }
        }

        for (query_id, mem_limit) in query_executions {
            benchmark_run.start_new_case(&format!(
                "{query_id}({})",
                human_readable_size(mem_limit as usize)
            ));

            let query_results = self.benchmark_query(query_id, mem_limit).await?;
            for iter in query_results {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;

        Ok(())
    }

    /// Benchmark query `query_id` in `AGGR_QUERIES`
    async fn benchmark_query(
        &self,
        query_id: usize,
        mem_limit: u64,
    ) -> Result<Vec<QueryResult>> {
        let query_name =
            format!("Q{query_id}({})", human_readable_size(mem_limit as usize));
        let mut config = self.common.config();
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = self.common.force_view_types;
        let runtime_config = RuntimeConfig::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(mem_limit as usize)))
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(config, runtime_config);

        // register tables
        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let query_idx = query_id - 1; // 1-indexed -> 0-indexed
            let sql = Self::AGGR_QUERIES[query_idx];

            let result = self.execute_query(&ctx, sql).await?;

            let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);

            let row_count = result.iter().map(|b| b.num_rows()).sum();
            println!(
                "{query_name} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("{query_name} avg time: {avg:.2} ms");

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in Self::AGGR_TABLES {
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
                ctx.register_table(table, Arc::new(memtable))?;
            } else {
                ctx.register_table(table, table_provider)?;
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

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let path = format!("{path}/{table}");
        let format = Arc::new(
            ParquetFormat::default()
                .with_options(ctx.state().table_options().parquet.clone()),
        );
        let extension = DEFAULT_PARQUET_EXTENSION;

        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_collect_stat(state.config().collect_statistics());

        let table_path = ListingTableUrl::parse(path)?;
        let config = ListingTableConfig::new(table_path).with_listing_options(options);
        let config = config.infer_schema(&state).await?;

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        self.common.partitions.unwrap_or(num_cpus::get())
    }

    /// Parse memory limit from string to number of bytes
    /// e.g. '1.5G', '100M' -> 1572864
    fn parse_memory_limit(limit: &str) -> Result<u64> {
        let (number, unit) = limit.split_at(limit.len() - 1);
        let number: f64 = number.parse().map_err(|_| {
            exec_datafusion_err!("Failed to parse number from memory limit '{}'", limit)
        })?;

        match unit {
            "K" => Ok((number * 1024.0) as u64),
            "M" => Ok((number * 1024.0 * 1024.0) as u64),
            "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as u64),
            _ => exec_err!("Unsupported unit '{}' in memory limit '{}'", unit, limit),
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    match ExternalAggrOpt::from_args() {
        ExternalAggrOpt::Benchmark(opt) => opt.run().await?,
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_limit_all() {
        // Test valid inputs
        assert_eq!(
            ExternalAggrConfig::parse_memory_limit("100K").unwrap(),
            102400
        );
        assert_eq!(
            ExternalAggrConfig::parse_memory_limit("1.5M").unwrap(),
            1572864
        );
        assert_eq!(
            ExternalAggrConfig::parse_memory_limit("2G").unwrap(),
            2147483648
        );

        // Test invalid unit
        assert!(ExternalAggrConfig::parse_memory_limit("500X").is_err());

        // Test invalid number
        assert!(ExternalAggrConfig::parse_memory_limit("abcM").is_err());
    }
}
