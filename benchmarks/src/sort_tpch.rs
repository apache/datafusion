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

//! This module provides integration benchmark for sort operation.
//! It will run different sort SQL queries on TPCH `lineitem` parquet dataset.
//!
//! Another `Sort` benchmark focus on single core execution. This benchmark
//! runs end-to-end sort queries and test the performance on multiple CPU cores.

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

use crate::util::{BenchmarkRun, CommonOpt};

#[derive(Debug, StructOpt)]
pub struct RunOpt {
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Sort query number. If not specified, runs all queries
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

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}

impl RunOpt {
    const SORT_TABLES: [&'static str; 1] = ["lineitem"];

    /// Sort queries with different characteristics:
    /// - Sort key with fixed length or variable length (VARCHAR)
    /// - Sort key with different cardinality
    /// - Different number of sort keys
    /// - Different number of payload columns (thin: 1 additional column other
    ///   than sort keys; wide: all columns except sort keys)
    ///
    /// DataSet is `lineitem` table in TPCH dataset (16 columns, 6M rows for
    /// scale factor 1.0, cardinality is counted from SF1 dataset)
    ///
    /// Key Columns:
    /// - Column `l_linenumber`, type: `INTEGER`, cardinality: 7
    /// - Column `l_suppkey`, type: `BIGINT`, cardinality: 10k
    /// - Column `l_orderkey`, type: `BIGINT`, cardinality: 1.5M
    /// - Column `l_comment`, type: `VARCHAR`, cardinality: 4.5M (len is ~26 chars)
    ///
    /// Payload Columns:
    /// - Thin variant: `l_partkey` column with `BIGINT` type (1 column)
    /// - Wide variant: all columns except for possible key columns (12 columns)
    const SORT_QUERIES: [&'static str; 11] = [
        // Q1: 1 sort key (type: INTEGER, cardinality: 7) + 1 payload column
        r#"
        SELECT l_linenumber, l_partkey
        FROM lineitem
        ORDER BY l_linenumber
        "#,
        // Q2: 1 sort key (type: BIGINT, cardinality: 1.5M) + 1 payload column
        r#"
        SELECT l_orderkey, l_partkey
        FROM lineitem
        ORDER BY l_orderkey
        "#,
        // Q3: 1 sort key (type: VARCHAR, cardinality: 4.5M) + 1 payload column
        r#"
        SELECT l_comment, l_partkey
        FROM lineitem
        ORDER BY l_comment
        "#,
        // Q4: 2 sort keys {(BIGINT, 1.5M), (INTEGER, 7)} + 1 payload column
        r#"
        SELECT l_orderkey, l_linenumber, l_partkey
        FROM lineitem
        ORDER BY l_orderkey, l_linenumber
        "#,
        // Q5: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + no payload column
        r#"
        SELECT l_linenumber, l_suppkey, l_orderkey
        FROM lineitem
        ORDER BY l_linenumber, l_suppkey, l_orderkey
        "#,
        // Q6: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + 1 payload column
        r#"
        SELECT l_linenumber, l_suppkey, l_orderkey, l_partkey
        FROM lineitem
        ORDER BY l_linenumber, l_suppkey, l_orderkey
        "#,
        // Q7: 3 sort keys {(INTEGER, 7), (BIGINT, 10k), (BIGINT, 1.5M)} + 12 all other columns
        r#"
        SELECT l_linenumber, l_suppkey, l_orderkey, 
               l_partkey, l_quantity, l_extendedprice, l_discount, l_tax,
               l_returnflag, l_linestatus, l_shipdate, l_commitdate,
               l_receiptdate, l_shipinstruct, l_shipmode
        FROM lineitem
        ORDER BY l_linenumber, l_suppkey, l_orderkey
        "#,
        // Q8: 4 sort keys {(BIGINT, 1.5M), (BIGINT, 10k), (INTEGER, 7), (VARCHAR, 4.5M)} + no payload column
        r#"
        SELECT l_orderkey, l_suppkey, l_linenumber, l_comment
        FROM lineitem
        ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
        "#,
        // Q9: 4 sort keys {(BIGINT, 1.5M), (BIGINT, 10k), (INTEGER, 7), (VARCHAR, 4.5M)} + 1 payload column
        r#"
        SELECT l_orderkey, l_suppkey, l_linenumber, l_comment, l_partkey
        FROM lineitem
        ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
        "#,
        // Q10: 4 sort keys {(BIGINT, 1.5M), (BIGINT, 10k), (INTEGER, 7), (VARCHAR, 4.5M)} + 12 all other columns
        r#"
        SELECT l_orderkey, l_suppkey, l_linenumber, l_comment,
               l_partkey, l_quantity, l_extendedprice, l_discount, l_tax,
               l_returnflag, l_linestatus, l_shipdate, l_commitdate,
               l_receiptdate, l_shipinstruct, l_shipmode
        FROM lineitem
        ORDER BY l_orderkey, l_suppkey, l_linenumber, l_comment
        "#,
        // Q11: 1 sort key (type: VARCHAR, cardinality: 4.5M) + 1 payload column
        r#"
        SELECT l_shipmode, l_comment, l_partkey
        FROM lineitem
        ORDER BY l_shipmode;
        "#,
    ];

    /// If query is specified from command line, run only that query.
    /// Otherwise, run all queries.
    pub async fn run(&self) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => 1..=Self::SORT_QUERIES.len(),
        };

        for query_id in query_range {
            benchmark_run.start_new_case(&format!("{query_id}"));

            let query_results = self.benchmark_query(query_id).await?;
            for iter in query_results {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;

        Ok(())
    }

    /// Benchmark query `query_id` in `SORT_QUERIES`
    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let config = self.common.config();
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
        // run benchmark
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let query_idx = query_id - 1; // 1-indexed -> 0-indexed
            let sql = Self::SORT_QUERIES[query_idx];

            let row_count = self.execute_query(&ctx, sql).await?;

            let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;
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
        for table in Self::SORT_TABLES {
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
            row_count += batch.unwrap().num_rows();
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
        let config = ListingTableConfig::new(table_path).with_listing_options(options);
        let config = config.infer_schema(&state).await?;

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        self.common
            .partitions
            .unwrap_or(get_available_parallelism())
    }
}
