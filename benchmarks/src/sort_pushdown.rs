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

//! Benchmark for sort pushdown optimization.
//!
//! Tests performance of sort elimination when files are non-overlapping and
//! internally sorted (declared via `--sorted` / `WITH ORDER`).
//!
//! Queries are loaded from external SQL files under `queries/sort_pushdown/`
//! so they can also be run directly with `datafusion-cli`.
//!
//! # Usage
//!
//! ```text
//! # Prepare sorted TPCH lineitem data (SF=1)
//! ./bench.sh data sort_pushdown
//!
//! # Baseline (no WITH ORDER, full SortExec)
//! ./bench.sh run sort_pushdown
//!
//! # With sort elimination (WITH ORDER, SortExec removed)
//! ./bench.sh run sort_pushdown_sorted
//! ```

use clap::Args;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::*;
use datafusion_common::DEFAULT_PARQUET_EXTENSION;
use datafusion_common::instant::Instant;

use crate::util::{BenchmarkRun, CommonOpt, QueryResult, print_memory_stats};

/// Default path to query files, relative to the benchmark root
const SORT_PUSHDOWN_QUERY_DIR: &str = "queries/sort_pushdown";

#[derive(Debug, Args)]
pub struct RunOpt {
    /// Common options
    #[command(flatten)]
    common: CommonOpt,

    /// Sort pushdown query number (1-4). If not specified, runs all queries
    #[arg(short, long)]
    pub query: Option<usize>,

    /// Path to data files (lineitem). Only parquet format is supported.
    #[arg(required = true, short = 'p', long = "path")]
    path: PathBuf,

    /// Path to JSON benchmark result to be compared using `compare.py`
    #[arg(short = 'o', long = "output")]
    output_path: Option<PathBuf>,

    /// Path to directory containing query SQL files (q1.sql, q2.sql, ...).
    /// Defaults to `queries/sort_pushdown/` relative to current directory.
    #[arg(long = "queries-path")]
    queries_path: Option<PathBuf>,

    /// Mark the first column (l_orderkey) as sorted via WITH ORDER.
    /// When set, enables sort elimination for matching queries.
    #[arg(short = 't', long = "sorted")]
    sorted: bool,
}

impl RunOpt {
    const TABLES: [&'static str; 1] = ["lineitem"];

    fn queries_dir(&self) -> PathBuf {
        self.queries_path
            .clone()
            .unwrap_or_else(|| PathBuf::from(SORT_PUSHDOWN_QUERY_DIR))
    }

    fn load_query(&self, query_id: usize) -> Result<String> {
        let path = self.queries_dir().join(format!("q{query_id}.sql"));
        std::fs::read_to_string(&path).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "Failed to read query file {}: {e}",
                path.display()
            ))
        })
    }

    fn available_queries(&self) -> Vec<usize> {
        let dir = self.queries_dir();
        let mut ids = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(rest) = name.strip_prefix('q')
                    && let Some(num_str) = rest.strip_suffix(".sql")
                    && let Ok(id) = num_str.parse::<usize>()
                {
                    ids.push(id);
                }
            }
        }
        ids.sort();
        ids
    }

    pub async fn run(&self) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        let query_ids = match self.query {
            Some(query_id) => vec![query_id],
            None => self.available_queries(),
        };

        for query_id in query_ids {
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

    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let sql = self.load_query(query_id)?;

        let config = self.common.config()?;
        let rt = self.common.build_runtime()?;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(rt)
            .with_default_features()
            .build();
        let ctx = SessionContext::from(state);

        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let row_count = self.execute_query(&ctx, sql.as_str()).await?;

            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);

            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");

        print_memory_stats();

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in Self::TABLES {
            let table_provider = self.get_table(ctx, table).await?;
            ctx.register_table(table, table_provider)?;
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
        let state = ctx.state();
        let path = format!("{path}/{table}");
        let format = Arc::new(
            ParquetFormat::default()
                .with_options(ctx.state().table_options().parquet.clone()),
        );
        let extension = DEFAULT_PARQUET_EXTENSION;

        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_collect_stat(true); // Always collect statistics for sort pushdown

        let table_path = ListingTableUrl::parse(path)?;
        let schema = options.infer_schema(&state, &table_path).await?;
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
}
