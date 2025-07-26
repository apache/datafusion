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

//! Integration benchmark for MATCH_RECOGNIZE pattern matching queries over the
//! TPCH `lineitem` parquet dataset.
//!
//! The benchmark follows the same structure as `sort_tpch.rs` and
//! `window_tpch.rs`, allowing easy comparison of end-to-end query
//! performance. Queries are stored in a static array and can be
//! executed individually or all together.

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
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Query number to execute. If omitted, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Path to data files (lineitem). Only parquet format is supported
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Optional path for JSON benchmark results (consumed by compare.py)
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Load data into a MemTable before executing queries
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,
}

impl RunOpt {
    const TABLES: [&'static str; 1] = ["lineitem"];

    // A curated set of 12 queries that progressively introduce more
    // sophisticated MATCH_RECOGNIZE constructs, starting from simple
    // predicates and building up to anchors, skip semantics, complex
    // quantifiers and large PERMUTE clauses.
    const MR_QUERIES: [&'static str; 12] = [
        //------------------------------------------------------------
        // 1.  Simple rising run (quantity strictly increasing)
        //------------------------------------------------------------
        r#"
        SELECT *
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          PATTERN (A B+)
          DEFINE
            B AS l_quantity > LAG(l_quantity)
        ) mr
        "#,
        //------------------------------------------------------------
        // 2.  Simple falling run (quantity strictly decreasing)
        //------------------------------------------------------------
        r#"
        SELECT *
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          PATTERN (A B+)
          DEFINE
            B AS l_quantity < LAG(l_quantity)
        ) mr
        "#,
        //------------------------------------------------------------
        // 3.  Peak pattern  (rise then fall in extended price)
        //------------------------------------------------------------
        r#"
        SELECT *
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          PATTERN (A B C)
          DEFINE
            B AS l_extendedprice > LAG(l_extendedprice),
            C AS l_extendedprice < LAG(l_extendedprice)
        ) mr
        "#,
        //------------------------------------------------------------
        // 4.  Constant-discount run (≥3 identical discounts)
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, l_discount, cnt
        FROM (
          SELECT *
          FROM lineitem
          MATCH_RECOGNIZE (
            PARTITION BY l_orderkey
            ORDER BY l_linenumber
            MEASURES COUNT(A.l_discount) AS cnt
            ALL ROWS PER MATCH
            PATTERN (A{3,})
            DEFINE
              A AS l_discount = LAG(l_discount)
          )
        ) t
        "#,
        //------------------------------------------------------------
        // 5.  Start-anchor example – first high-tax row per order
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, l_linenumber
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          ALL ROWS PER MATCH
          PATTERN (^ B)
          DEFINE B AS l_tax > 0.05
        ) mr
        "#,
        //------------------------------------------------------------
        // 6.  End-anchor example – last large-quantity row per order
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, l_linenumber
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          ALL ROWS PER MATCH
          PATTERN (A $)
          DEFINE A AS l_quantity > 20
        ) mr
        "#,
        //------------------------------------------------------------
        // 7.  AFTER MATCH SKIP PAST LAST ROW  (low-qty streak followed by high)
        //------------------------------------------------------------
        r#"
        SELECT l_partkey, l_linenumber, match_no
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_partkey
          ORDER BY l_linenumber
          MEASURES MATCH_NUMBER() AS match_no
          ALL ROWS PER MATCH
          AFTER MATCH SKIP PAST LAST ROW
          PATTERN (A+ B)
          DEFINE
            A AS l_quantity < 10,
            B AS l_quantity >= 10
        ) t
        "#,
        //------------------------------------------------------------
        // 8.  AFTER MATCH SKIP TO LAST B  (same pattern, different skip)
        //------------------------------------------------------------
        r#"
        SELECT l_partkey, l_linenumber, match_no
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_partkey
          ORDER BY l_linenumber
          MEASURES MATCH_NUMBER() AS match_no
          ALL ROWS PER MATCH
          AFTER MATCH SKIP TO LAST B
          PATTERN (A+ B)
          DEFINE
            A AS l_quantity < 10,
            B AS l_quantity >= 10
        ) t
        "#,
        //------------------------------------------------------------
        // 9.  ONE ROW PER MATCH with FIRST_VALUE / LAST_VALUE measures
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, first_qty, last_qty
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          MEASURES
            FIRST_VALUE(l_quantity) AS first_qty,
            LAST_VALUE(l_quantity) AS last_qty
          ROWS PER MATCH ONE ROW
          PATTERN (A+ B+)
          DEFINE
            A AS l_quantity < 15,
            B AS l_quantity >= 15
        ) t
        "#,
        //------------------------------------------------------------
        // 10. Range quantifier inside grouped pattern
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, cnt
        FROM (
          SELECT *
          FROM lineitem
          MATCH_RECOGNIZE (
            PARTITION BY l_orderkey
            ORDER BY l_linenumber
            MEASURES COUNT(A.l_quantity) AS cnt
            PATTERN ((A B){2,4} C)
            DEFINE
              A AS l_quantity < 5,
              B AS l_quantity BETWEEN 5 AND 10,
              C AS l_quantity > 10
          )
        ) sub
        "#,
        //------------------------------------------------------------
        // 11. PERMUTE of eight symbols with classification metrics
        //------------------------------------------------------------
        r#"
        SELECT l_orderkey, classifier, match_no
        FROM lineitem
        MATCH_RECOGNIZE (
          PARTITION BY l_orderkey
          ORDER BY l_linenumber
          MEASURES CLASSIFIER() AS classifier,
                   MATCH_NUMBER() AS match_no
          ALL ROWS PER MATCH
          PATTERN (PERMUTE(A,B,C,D,E,F,G,H))
          DEFINE
            A AS l_quantity < 5,
            B AS l_quantity BETWEEN 5 AND 10,
            C AS l_discount < 0.05,
            D AS l_discount >= 0.05,
            E AS l_extendedprice > 1000,
            F AS l_extendedprice <= 1000,
            G AS l_tax < 0.05,
            H AS l_tax >= 0.05
        ) t
        "#,
        //------------------------------------------------------------
        // 12. Stress-test PERMUTE with twelve unconstrained symbols
        //------------------------------------------------------------
        r#"
        SELECT l_partkey
        FROM lineitem
        MATCH_RECOGNIZE (
          ORDER BY l_linenumber
          ALL ROWS PER MATCH
          PATTERN (PERMUTE(A,B,C,D,E,F,G,H,I,J,K,L))
          DEFINE
            A AS TRUE,
            B AS TRUE,
            C AS TRUE,
            D AS TRUE,
            E AS TRUE,
            F AS TRUE,
            G AS TRUE,
            H AS TRUE,
            I AS TRUE,
            J AS TRUE,
            K AS TRUE,
            L AS TRUE
        ) t
        "#,
    ];

    pub async fn run(&self) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        let query_range = match self.query {
            Some(q) => q..=q,
            None => 1..=Self::MR_QUERIES.len(),
        };

        for query_id in query_range {
            benchmark_run.start_new_case(&query_id.to_string());

            let res = self.benchmark_query(query_id).await;
            match res {
                Ok(results) => {
                    for iter in results {
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

    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(rt_builder.build_arc()?)
            .with_default_features()
            .build();
        let ctx = SessionContext::from(state);

        // Register the TPCH table(s)
        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        let mut query_results = vec![];

        for i in 0..self.iterations() {
            let start = Instant::now();

            let sql = Self::MR_QUERIES[query_id - 1];
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
        for table in Self::TABLES {
            let provider = self.get_table(ctx, table).await?;
            if self.mem_table {
                println!("Loading table '{table}' into memory");
                let start = Instant::now();
                let memtable =
                    MemTable::load(provider, Some(self.partitions()), &ctx.state())
                        .await?;
                println!(
                    "Loaded table '{}' into memory in {} ms",
                    table,
                    start.elapsed().as_millis()
                );
                ctx.register_table(table, Arc::new(memtable))?;
            } else {
                ctx.register_table(table, provider)?;
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
        let base = self.path.to_str().unwrap();
        let path = format!("{base}/{table}");

        let state = ctx.state();
        let format = Arc::new(
            ParquetFormat::default().with_options(state.table_options().parquet.clone()),
        );
        let options = ListingOptions::new(format)
            .with_file_extension(DEFAULT_PARQUET_EXTENSION)
            .with_collect_stat(state.config().collect_statistics());

        let table_url = ListingTableUrl::parse(path)?;
        let schema = options.infer_schema(&state, &table_url).await?;

        let config = ListingTableConfig::new(table_url)
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
