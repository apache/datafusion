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

use std::{path::PathBuf, time::Instant};

use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use structopt::StructOpt;

use crate::{BenchmarkRun, CommonOpt};

/// Run the clickbench benchmark
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
    query: Option<usize>,

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

    /// Path to queries.sql (single file)
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/clickbench/queries.sql"
    )]
    queries_path: PathBuf,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

const CLICKBENCH_QUERY_START_ID: usize = 0;
const CLICKBENCH_QUERY_END_ID: usize = 42;

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => CLICKBENCH_QUERY_START_ID..=CLICKBENCH_QUERY_END_ID,
        };

        let config = self.common.config();
        let ctx = SessionContext::with_config(config);
        self.register_hits(&ctx).await?;

        let iterations = self.common.iterations;
        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let sql = self.get_query(query_id)?;
            println!("Q{query_id}: {sql}");

            for i in 0..iterations {
                let start = Instant::now();
                let results = ctx.sql(&sql).await?.collect().await?;
                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;
                let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
                println!(
                    "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                );
                benchmark_run.write_iter(elapsed, row_count);
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Registrs the `hits.parquet` as a table named `hits`
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

    /// Returns the text of query `query_id`
    fn get_query(&self, query_id: usize) -> Result<String> {
        if query_id > CLICKBENCH_QUERY_END_ID {
            return Err(DataFusionError::Execution(format!(
                "Invalid query id {query_id}. Must be between {CLICKBENCH_QUERY_START_ID} and {CLICKBENCH_QUERY_END_ID}"
            )));
        }

        let path = self.queries_path.as_path();

        // ClickBench has all queries in a single file identified by line number
        let all_queries = std::fs::read_to_string(path).map_err(|e| {
            DataFusionError::Execution(format!("Could not open {path:?}: {e}"))
        })?;
        let all_queries: Vec<_> = all_queries.lines().collect();

        Ok(all_queries.get(query_id).map(|s| s.to_string()).unwrap())
    }
}
