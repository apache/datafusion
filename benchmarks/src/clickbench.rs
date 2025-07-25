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
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::util::{print_memory_stats, BenchmarkRun, CommonOpt, QueryResult};
use datafusion::logical_expr::{ExplainFormat, ExplainOption};
use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use datafusion_common::exec_datafusion_err;
use datafusion_common::instant::Instant;
use structopt::StructOpt;

/// Driver program to run the ClickBench benchmark
///
/// The ClickBench[1] benchmarks are widely cited in the industry and
/// focus on grouping / aggregation / filtering. This runner uses the
/// scripts and queries from [2].
///
/// [1]: https://github.com/ClickHouse/ClickBench
/// [2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 0 and 42). If not specified, runs all queries
    #[structopt(short, long)]
    pub query: Option<usize>,

    /// If specified, enables Parquet Filter Pushdown.
    ///
    /// Specifically, it enables:
    /// * `pushdown_filters = true`
    /// * `reorder_filters = true`
    #[structopt(long = "pushdown")]
    pushdown: bool,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to hits.parquet (single file) or `hits_partitioned`
    /// (partitioned, 100 files)
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "benchmarks/data/hits.parquet"
    )]
    path: PathBuf,

    /// Path to queries directory
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/clickbench/queries"
    )]
    pub queries_path: PathBuf,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

/// Get the SQL file path
pub fn get_query_path(query_dir: &Path, query: usize) -> PathBuf {
    let mut query_path = query_dir.to_path_buf();
    query_path.push(format!("q{query}.sql"));
    query_path
}

/// Get the SQL statement from the specified query file
pub fn get_query_sql(query_path: &Path) -> Result<Option<String>> {
    if fs::exists(query_path)? {
        Ok(Some(fs::read_to_string(query_path)?))
    } else {
        Ok(None)
    }
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");

        let query_dir_metadata = fs::metadata(&self.queries_path).map_err(|e| {
            if e.kind() == ErrorKind::NotFound {
                exec_datafusion_err!(
                    "Query path '{}' does not exist.",
                    &self.queries_path.to_str().unwrap()
                )
            } else {
                DataFusionError::External(Box::new(e))
            }
        })?;

        if !query_dir_metadata.is_dir() {
            return Err(exec_datafusion_err!(
                "Query path '{}' is not a directory.",
                &self.queries_path.to_str().unwrap()
            ));
        }

        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => 0..=usize::MAX,
        };

        // configure parquet options
        let mut config = self.common.config()?;
        {
            let parquet_options = &mut config.options_mut().execution.parquet;
            // The hits_partitioned dataset specifies string columns
            // as binary due to how it was written. Force it to strings
            parquet_options.binary_as_string = true;

            // Turn on Parquet filter pushdown if requested
            if self.pushdown {
                parquet_options.pushdown_filters = true;
                parquet_options.reorder_filters = true;
            }
        }

        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);
        self.register_hits(&ctx).await?;

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            let query_path = get_query_path(&self.queries_path, query_id);
            let Some(sql) = get_query_sql(&query_path)? else {
                if self.query.is_some() {
                    return Err(exec_datafusion_err!(
                        "Could not load query file '{}'.",
                        &query_path.to_str().unwrap()
                    ));
                }
                break;
            };
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(&sql, query_id, &ctx).await;
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
        sql: &str,
        query_id: usize,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        println!("Q{query_id}: {sql}");

        let mut millis = Vec::with_capacity(self.iterations());
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();
            let results = ctx.sql(sql).await?.collect().await?;
            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count })
        }
        if self.common.debug {
            ctx.sql(sql)
                .await?
                .explain_with_options(
                    ExplainOption::default().with_format(ExplainFormat::Tree),
                )?
                .show()
                .await?;
        }
        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");

        // Print memory usage stats using mimalloc (only when compiled with --features mimalloc_extended)
        print_memory_stats();

        Ok(query_results)
    }

    /// Registers the `hits.parquet` as a table named `hits`
    async fn register_hits(&self, ctx: &SessionContext) -> Result<()> {
        let options = Default::default();
        let path = self.path.as_os_str().to_str().unwrap();
        ctx.register_parquet("hits", path, options)
            .await
            .map_err(|e| {
                DataFusionError::Context(
                    format!("Registering 'hits' as {path}"),
                    Box::new(e),
                )
            })
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }
}
