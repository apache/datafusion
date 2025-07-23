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

//! Integration benchmark for various window function queries on the
//! TPCH `lineitem` parquet dataset.
//!
//! The benchmark executes multiple SQL queries that exercise
//! different window functions (e.g. ROW_NUMBER, RANK, SUM OVER,
//! cumulative SUM) with varying partitioning and ordering keys.
//!
//! The implementation follows the same structure as `sort_tpch.rs`
//! to ensure consistency across benchmarks.

use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common::DEFAULT_PARQUET_EXTENSION;

use crate::util::{BenchmarkRun, CommonOpt, QueryResult};

#[derive(Debug, StructOpt)]
pub struct RunOpt {
    /// Common options shared with other benchmarks
    #[structopt(flatten)]
    common: CommonOpt,

    /// Window query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Path to data files (lineitem). Only parquet format is supported
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Path to JSON benchmark result to be compare using `compare.py`
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,
}

impl RunOpt {
    const WINDOW_TABLES: [&'static str; 1] = ["lineitem"];

    /// Window function queries exercising different window types
    /// and partition / order key configurations.
    ///
    /// All queries operate on TPCH `lineitem` table (16 columns, ~6M rows for SF1).
    ///
    /// Key columns used in PARTITION / ORDER clauses:
    /// - `l_partkey`   BIGINT (cardinality: 200k)
    /// - `l_orderkey`  BIGINT (cardinality: 1.5M)
    /// - `l_suppkey`   BIGINT (cardinality: 10k)
    /// - `l_linenumber` INTEGER (cardinality: 7)
    /// - `l_quantity`  DECIMAL (cardinality: 50)
    ///
    /// The array is 1-indexed for human readability and the queries are
    /// ordered to gradually introduce more sophisticated window features:
    ///
    /// 1. Row numbering        (ROW_NUMBER)
    /// 2. Ranking within group (RANK)
    /// 3. Dense ranking global (DENSE_RANK)
    /// 4. Partition row count  (COUNT)
    /// 5. Partitioned sum      (SUM)
    /// 6. Partitioned average  (AVG)
    /// 7. Cumulative running sum with explicit frame specification
    /// 8. Lag / Lead look-back and look-ahead values
    /// 9. NTILE quartile classification
    /// 10. Percent / Cume distribution analytics
    /// 11. Moving average + First/Last values
    /// 12. "Kitchen-sink" query mixing many window functions & frames
    const WINDOW_QUERIES: [&'static str; 12] = [
        //------------------------------------------------------------
        // 1. ROW_NUMBER – order-level sequencing by linenumber
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_linenumber,
               ROW_NUMBER() OVER (PARTITION BY l_orderkey ORDER BY l_linenumber) AS rn
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 2. RANK – supplier-level ranking by orderkey
        //------------------------------------------------------------
        r#"
        SELECT l_suppkey,
               l_orderkey,
               RANK() OVER (PARTITION BY l_suppkey ORDER BY l_orderkey) AS rnk
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 3. DENSE_RANK – global ranking of quantity (no PARTITION)
        //------------------------------------------------------------
        r#"
        SELECT l_quantity,
               DENSE_RANK() OVER (ORDER BY l_quantity DESC) AS dr
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 4. COUNT – number of rows per orderkey partition
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_linenumber,
               COUNT(*) OVER (PARTITION BY l_orderkey) AS cnt
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 5. SUM – total extended price per partkey partition
        //------------------------------------------------------------
        r#"
        SELECT l_partkey,
               SUM(l_extendedprice) OVER (PARTITION BY l_partkey) AS sum_price
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 6. AVG – average quantity per shipmode partition
        //------------------------------------------------------------
        r#"
        SELECT l_shipmode,
               AVG(l_quantity) OVER (PARTITION BY l_shipmode) AS avg_qty
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 7. Cumulative SUM – running quantity ordered by orderkey
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_linenumber,
               l_quantity,
               SUM(l_quantity) OVER (
                 ORDER BY l_orderkey
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_qty
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 8. Lag/Lead – previous and next quantity within each order
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_linenumber,
               l_quantity,
               LAG(l_quantity) OVER (PARTITION BY l_orderkey ORDER BY l_linenumber)  AS prev_qty,
               LEAD(l_quantity) OVER (PARTITION BY l_orderkey ORDER BY l_linenumber) AS next_qty
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 9. NTILE – quartile classification by extended price
        //------------------------------------------------------------
        r#"
        SELECT l_extendedprice,
               NTILE(4) OVER (ORDER BY l_extendedprice DESC) AS price_quartile
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 10. Percent / Cume distribution analytics
        //------------------------------------------------------------
        r#"
        SELECT l_extendedprice,
               PERCENT_RANK() OVER (ORDER BY l_extendedprice) AS pct_rank,
               CUME_DIST()   OVER (ORDER BY l_extendedprice) AS cume_dist
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 11. Moving average with FIRST_VALUE / LAST_VALUE in partition
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_linenumber,
               l_quantity,
               AVG(l_quantity)   OVER (
                   PARTITION BY l_orderkey
                   ORDER BY l_linenumber
                   ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
               ) AS moving_avg,
               FIRST_VALUE(l_quantity) OVER (
                   PARTITION BY l_orderkey
                   ORDER BY l_linenumber
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS first_qty,
               LAST_VALUE(l_quantity)  OVER (
                   PARTITION BY l_orderkey
                   ORDER BY l_linenumber
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS last_qty
        FROM lineitem
        "#,
        //------------------------------------------------------------
        // 12. "Kitchen-sink" query mixing many window functions & frames
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey,
               l_partkey,
               l_suppkey,
               l_linenumber,
               ROW_NUMBER() OVER (PARTITION BY l_orderkey ORDER BY l_linenumber)                             AS rn,
               SUM(l_quantity) OVER (PARTITION BY l_orderkey)                                               AS total_qty,
               SUM(l_extendedprice) OVER (
                   PARTITION BY l_shipmode
                   ORDER BY l_linenumber
                   ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
               ) AS rolling_price,
               COUNT(*) OVER (
                   ORDER BY l_orderkey
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_cnt,
               NTILE(4)       OVER (ORDER BY l_extendedprice DESC)                                          AS price_ntile,
               PERCENT_RANK() OVER (ORDER BY l_quantity)                                                    AS pct_rank,
               CUME_DIST()    OVER (ORDER BY l_quantity)                                                    AS cume_dist
        FROM lineitem
        "#,
    ];

    /// Run the specified window query (or all queries) and collect metrics
    pub async fn run(&self) -> Result<()> {
        let mut benchmark_run: BenchmarkRun = BenchmarkRun::new();

        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => 1..=Self::WINDOW_QUERIES.len(),
        };

        for query_id in query_range {
            benchmark_run.start_new_case(&format!("{query_id}"));

            let query_results = self.benchmark_query(query_id).await;
            match query_results {
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

    /// Benchmark query `query_id` in `WINDOW_QUERIES`
    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(rt_builder.build_arc()?)
            .with_default_features()
            .build();
        let ctx = SessionContext::from(state);

        // register tables
        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let query_idx = query_id - 1; // 1-indexed -> 0-indexed
            let sql = Self::WINDOW_QUERIES[query_idx];

            let row_count = self.execute_query(&ctx, sql).await?;

            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);

            println!(
                "Q{query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Q{query_id} avg time: {avg:.2} ms");

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in Self::WINDOW_TABLES {
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

    async fn execute_query(&self, ctx: &SessionContext, sql: &str) -> Result<usize> {
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

        let mut row_count = 0;

        let mut stream = execute_stream(physical_plan.clone(), state.task_ctx())?;
        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();
        }

        if debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                    .indent(true)
            );
        }

        Ok(row_count)
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
        let schema = options.infer_schema(&state, &table_path).await?;

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
