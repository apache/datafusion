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

use crate::util::{BenchmarkRun, CommonOpt, QueryResult};
use clap::Args;
use datafusion::physical_plan::execute_stream;
use datafusion::{error::Result, prelude::SessionContext};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, exec_datafusion_err, exec_err};
use futures::StreamExt;

/// Run end-to-end ASOF join benchmarks.
///
/// The cases cover ordered-input reuse, optimizer-inserted sort/repartition,
/// wide payload materialization, and descending successor matching.
#[derive(Debug, Args, Clone)]
#[command(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 1 and 4). If not specified, runs all queries
    #[arg(short, long)]
    query: Option<usize>,

    /// Common options
    #[command(flatten)]
    common: CommonOpt,

    /// If present, write results json here
    #[arg(short = 'o', long = "output")]
    output_path: Option<std::path::PathBuf>,
}

const ASOF_QUERIES: &[&str] = &[
    // Q1: predecessor without equality keys, ordered range input can be reused
    r#"
        WITH left_input AS (
            SELECT value AS ts, value AS payload FROM range(1000000)
        ),
        right_input AS (
            SELECT value AS ts, value AS payload FROM range(1000000)
        )
        SELECT l.ts, l.payload, r.payload AS right_payload
        FROM left_input l
        ASOF JOIN right_input r MATCH_CONDITION (l.ts >= r.ts)
    "#,
    // Q2: grouped predecessor, optimizer supplies repartitioning and ordering
    r#"
        WITH left_input AS (
            SELECT value % 10000 AS key,
                   value / 10000 + 1 AS ts,
                   value AS payload
            FROM range(1000000)
        ),
        right_input AS (
            SELECT value % 10000 AS key,
                   value / 10000 AS ts,
                   value AS payload
            FROM range(1000000)
        )
        SELECT l.key, l.ts, l.payload, r.payload AS right_payload
        FROM left_input l
        ASOF JOIN right_input r MATCH_CONDITION (l.ts >= r.ts)
        ON l.key = r.key
    "#,
    // Q3: grouped predecessor with a wide payload
    r#"
        WITH left_input AS (
            SELECT value % 10000 AS key,
                   value / 10000 + 1 AS ts,
                   repeat('x', 256) AS payload
            FROM range(250000)
        ),
        right_input AS (
            SELECT value % 10000 AS key,
                   value / 10000 AS ts,
                   repeat('y', 256) AS payload
            FROM range(250000)
        )
        SELECT l.key, l.ts, l.payload, r.payload AS right_payload
        FROM left_input l
        ASOF JOIN right_input r MATCH_CONDITION (l.ts >= r.ts)
        ON l.key = r.key
    "#,
    // Q4: successor matching requires descending input order
    r#"
        WITH left_input AS (
            SELECT value AS ts, value AS payload FROM range(500000)
        ),
        right_input AS (
            SELECT value AS ts, value AS payload FROM range(500000)
        )
        SELECT l.ts, l.payload, r.payload AS right_payload
        FROM left_input l
        ASOF JOIN right_input r MATCH_CONDITION (l.ts <= r.ts)
    "#,
];

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running ASOF benchmarks with the following options: {self:#?}\n");

        let query_range = match self.query {
            Some(query_id) if (1..=ASOF_QUERIES.len()).contains(&query_id) => {
                query_id..=query_id
            }
            Some(query_id) => {
                return exec_err!(
                    "Query {query_id} not found. Available queries: 1 to {}",
                    ASOF_QUERIES.len()
                );
            }
            None => 1..=ASOF_QUERIES.len(),
        };

        let config = self.common.config()?;
        let runtime = self.common.build_runtime()?;
        let ctx = SessionContext::new_with_config_rt(config, runtime);
        let mut benchmark_run = BenchmarkRun::new();

        for query_id in query_range {
            let sql = ASOF_QUERIES[query_id - 1];
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            match self.benchmark_query(sql, &query_id.to_string(), &ctx).await {
                Ok(results) => {
                    for result in results {
                        benchmark_run.write_iter(result.elapsed, result.row_count);
                    }
                }
                Err(error) => {
                    return Err(DataFusionError::Context(
                        format!("ASOF benchmark Q{query_id} failed with error:"),
                        Box::new(error),
                    ));
                }
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    async fn benchmark_query(
        &self,
        sql: &str,
        query_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let physical_plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let plan_string = format!("{physical_plan:#?}");
        if !plan_string.contains("AsOfJoinExec") {
            return Err(exec_datafusion_err!(
                "Query {query_name} does not use AsOfJoinExec. Physical plan: {plan_string}"
            ));
        }

        let mut query_results = Vec::with_capacity(self.common.iterations);
        for iteration in 0..self.common.iterations {
            let start = Instant::now();
            let row_count = Self::execute_sql_without_result_buffering(sql, ctx).await?;
            let elapsed = start.elapsed();
            println!(
                "Query {query_name} iteration {iteration} returned {row_count} rows in {elapsed:?}"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }
        Ok(query_results)
    }

    async fn execute_sql_without_result_buffering(
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<usize> {
        let physical_plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;
        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();
        }
        Ok(row_count)
    }
}
