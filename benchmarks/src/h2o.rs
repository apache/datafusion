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

//! H2O benchmark implementation for groupby, join and window operations
//! Reference:
//! - [H2O AI Benchmark](https://duckdb.org/2023/04/14/h2oai.html)
//! - [Extended window function benchmark](https://duckdb.org/2024/06/26/benchmarks-over-time.html#window-functions-benchmark)

use crate::util::{print_memory_stats, BenchmarkRun, CommonOpt};
use datafusion::logical_expr::{ExplainFormat, ExplainOption};
use datafusion::{error::Result, prelude::SessionContext};
use datafusion_common::{
    exec_datafusion_err, instant::Instant, internal_err, DataFusionError, TableReference,
};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Run the H2O benchmark
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    #[structopt(short, long)]
    pub query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to queries.sql (single file)
    /// default value is the groupby.sql file in the h2o benchmark
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/h2o/groupby.sql"
    )]
    pub queries_path: PathBuf,

    /// Path to data file (parquet or csv)
    /// Default value is the G1_1e7_1e7_100_0.csv file in the h2o benchmark
    /// This is the small csv file with 10^7 rows
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "benchmarks/data/h2o/G1_1e7_1e7_100_0.csv"
    )]
    path: PathBuf,

    /// Path to data files (parquet or csv), using , to separate the paths
    /// Default value is the small files for join x table, small table, medium table, big table files in the h2o benchmark
    /// This is the small csv file case
    #[structopt(
        short = "join-paths",
        long = "join-paths",
        default_value = "benchmarks/data/h2o/J1_1e7_NA_0.csv,benchmarks/data/h2o/J1_1e7_1e1_0.csv,benchmarks/data/h2o/J1_1e7_1e4_0.csv,benchmarks/data/h2o/J1_1e7_1e7_NA.csv"
    )]
    join_paths: String,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let queries = AllQueries::try_new(&self.queries_path)?;
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => queries.min_query_id()..=queries.max_query_id(),
        };

        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        // Register tables depending on which h2o benchmark is being run
        // (groupby/join/window)
        if self.queries_path.to_str().unwrap().ends_with("groupby.sql") {
            self.register_data("x", self.path.as_os_str().to_str().unwrap(), &ctx)
                .await?;
        } else if self.queries_path.to_str().unwrap().ends_with("join.sql") {
            let join_paths: Vec<&str> = self.join_paths.split(',').collect();
            let table_name: Vec<&str> = vec!["x", "small", "medium", "large"];
            for (i, path) in join_paths.iter().enumerate() {
                self.register_data(table_name[i], path, &ctx).await?;
            }
        } else if self.queries_path.to_str().unwrap().ends_with("window.sql") {
            // Only register the 'large' table in h2o-join dataset
            let h2o_join_large_path = self.join_paths.split(',').nth(3).unwrap();
            self.register_data("large", h2o_join_large_path, &ctx)
                .await?;
        } else {
            return internal_err!("Invalid query file path");
        }

        let iterations = self.common.iterations;
        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let sql = queries.get_query(query_id)?;
            println!("Q{query_id}: {sql}");

            let mut millis = Vec::with_capacity(iterations);
            for i in 1..=iterations {
                let start = Instant::now();
                let results = ctx.sql(sql).await?.collect().await?;
                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;
                millis.push(ms);
                let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                );
                benchmark_run.write_iter(elapsed, row_count);
            }
            let avg = millis.iter().sum::<f64>() / millis.len() as f64;
            println!("Query {query_id} avg time: {avg:.2} ms");

            // Print memory usage stats using mimalloc (only when compiled with --features mimalloc_extended)
            print_memory_stats();

            if self.common.debug {
                ctx.sql(sql)
                    .await?
                    .explain_with_options(
                        ExplainOption::default().with_format(ExplainFormat::Tree),
                    )?
                    .show()
                    .await?;
            }
            benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        }

        Ok(())
    }

    async fn register_data(
        &self,
        table_ref: impl Into<TableReference>,
        table_path: impl AsRef<str>,
        ctx: &SessionContext,
    ) -> Result<()> {
        let csv_options = Default::default();
        let parquet_options = Default::default();

        let table_path_str = table_path.as_ref();

        let extension = Path::new(table_path_str)
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("");

        match extension {
            "csv" => {
                ctx.register_csv(table_ref, table_path_str, csv_options)
                    .await
                    .map_err(|e| {
                        DataFusionError::Context(
                            format!("Registering 'table' as {table_path_str}"),
                            Box::new(e),
                        )
                    })
                    .expect("error registering csv");
            }
            "parquet" => {
                ctx.register_parquet(table_ref, table_path_str, parquet_options)
                    .await
                    .map_err(|e| {
                        DataFusionError::Context(
                            format!("Registering 'table' as {table_path_str}"),
                            Box::new(e),
                        )
                    })
                    .expect("error registering parquet");
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported file extension: {extension}",
                )));
            }
        }

        Ok(())
    }
}

pub struct AllQueries {
    queries: Vec<String>,
}

impl AllQueries {
    pub fn try_new(path: &Path) -> Result<Self> {
        let all_queries = std::fs::read_to_string(path)
            .map_err(|e| exec_datafusion_err!("Could not open {path:?}: {e}"))?;

        Ok(Self {
            queries: all_queries.split("\n\n").map(|s| s.to_string()).collect(),
        })
    }

    /// Returns the text of query `query_id`
    pub fn get_query(&self, query_id: usize) -> Result<&str> {
        self.queries
            .get(query_id - 1)
            .ok_or_else(|| {
                let min_id = self.min_query_id();
                let max_id = self.max_query_id();
                exec_datafusion_err!(
                    "Invalid query id {query_id}. Must be between {min_id} and {max_id}"
                )
            })
            .map(|s| s.as_str())
    }

    pub fn min_query_id(&self) -> usize {
        1
    }

    pub fn max_query_id(&self) -> usize {
        self.queries.len()
    }
}
