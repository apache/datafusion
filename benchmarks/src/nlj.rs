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
use datafusion::physical_plan::execute_stream;
use datafusion::{error::Result, prelude::SessionContext};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, exec_datafusion_err, exec_err};
use structopt::StructOpt;

use futures::StreamExt;

/// Run the Nested Loop Join (NLJ) benchmark
///
/// This micro-benchmark focuses on the performance characteristics of NLJs.
///
/// It always tries to use fast scanners (without decoding overhead) and
/// efficient predicate expressions to ensure it can reflect the performance
/// of the NLJ operator itself.
///
/// In this micro-benchmark, the following workload characteristics will be
/// varied:
/// - Join type: Inner/Left/Right/Full (all for the NestedLoopJoin physical
///   operator)
///   TODO: Include special join types (Semi/Anti/Mark joins)
/// - Input size: Different combinations of left (build) side and right (probe)
///   side sizes
/// - Selectivity of join filters
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 1 and 10). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<std::path::PathBuf>,
}

/// Inline SQL queries for NLJ benchmarks
///
/// Each query's comment includes:
///   - Left (build) side row count × Right (probe) side row count
///   - Join predicate selectivity (1% means the output size is 1% * input size)
const NLJ_QUERIES: &[&str] = &[
    // Q1: INNER 10K x 10K | LOW 0.1%
    r#"
        SELECT *
        FROM range(10000) AS t1
        JOIN range(10000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q2: INNER 10K x 10K | Medium 20%
    r#"
        SELECT *
        FROM range(10000) AS t1
        JOIN range(10000) AS t2
        ON (t1.value + t2.value) % 5 = 0;
    "#,
    // Q3: INNER 10K x 10K | High 90%
    r#"
        SELECT *
        FROM range(10000) AS t1
        JOIN range(10000) AS t2
        ON (t1.value + t2.value) % 10 <> 0;
    "#,
    // Q4: INNER 30K x 30K | Medium 20%
    r#"
        SELECT *
        FROM range(30000) AS t1
        JOIN range(30000) AS t2
        ON (t1.value + t2.value) % 5 = 0;
    "#,
    // Q5: INNER 10K x 200K | LOW 0.1% (small to large)
    r#"
        SELECT *
        FROM range(10000) AS t1
        JOIN range(200000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q6: INNER 200K x 10K | LOW 0.1% (large to small)
    r#"
        SELECT *
        FROM range(200000) AS t1
        JOIN range(10000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q7: RIGHT OUTER 10K x 200K | LOW 0.1%
    r#"
        SELECT *
        FROM range(10000) AS t1
        RIGHT JOIN range(200000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q8: LEFT OUTER 200K x 10K | LOW 0.1%
    r#"
        SELECT *
        FROM range(200000) AS t1
        LEFT JOIN range(10000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q9: FULL OUTER 30K x 30K | LOW 0.1%
    r#"
        SELECT *
        FROM range(30000) AS t1
        FULL JOIN range(30000) AS t2
        ON (t1.value + t2.value) % 1000 = 0;
    "#,
    // Q10: FULL OUTER 30K x 30K | High 90%
    r#"
        SELECT *
        FROM range(30000) AS t1
        FULL JOIN range(30000) AS t2
        ON (t1.value + t2.value) % 10 <> 0;
    "#,
    // Q11: INNER 30K x 30K | MEDIUM 50% | cheap predicate
    r#"
        SELECT *
        FROM range(30000) AS t1
        INNER JOIN range(30000) AS t2
        ON (t1.value > t2.value);
    "#,
    // Q12: FULL OUTER 30K x 30K | MEDIUM 50% | cheap predicate
    r#"
        SELECT *
        FROM range(30000) AS t1
        FULL JOIN range(30000) AS t2
        ON (t1.value > t2.value);
    "#,
    // Q13: LEFT SEMI 30K x 30K | HIGH 99.9%
    r#"
        SELECT t1.*
        FROM range(30000) AS t1
        LEFT SEMI JOIN range(30000) AS t2
        ON t1.value < t2.value;
    "#,
    // Q14: LEFT ANTI 30K x 30K | LOW 0.003%
    r#"
        SELECT t1.*
        FROM range(30000) AS t1
        LEFT ANTI JOIN range(30000) AS t2
        ON t1.value < t2.value;
    "#,
    // Q15: RIGHT SEMI 30K x 30K | HIGH 99.9%
    r#"
        SELECT t1.*
        FROM range(30000) AS t2
        RIGHT SEMI JOIN range(30000) AS t1
        ON t2.value < t1.value;
    "#,
    // Q16: RIGHT ANTI 30K x 30K | LOW 0.003%
    r#"
        SELECT t1.*
        FROM range(30000) AS t2
        RIGHT ANTI JOIN range(30000) AS t1
        ON t2.value < t1.value;
    "#,
    // Q17: LEFT MARK | HIGH 99.9%
    r#"
        SELECT *
        FROM range(30000) AS t2(k2)
        WHERE k2 > 0
        OR EXISTS (
            SELECT 1
            FROM range(30000) AS t1(k1)
            WHERE t2.k2 > t1.k1
        );
    "#,
];

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running NLJ benchmarks with the following options: {self:#?}\n");

        // Define query range
        let query_range = match self.query {
            Some(query_id) => {
                if query_id >= 1 && query_id <= NLJ_QUERIES.len() {
                    query_id..=query_id
                } else {
                    return exec_err!(
                        "Query {query_id} not found. Available queries: 1 to {}",
                        NLJ_QUERIES.len()
                    );
                }
            }
            None => 1..=NLJ_QUERIES.len(),
        };

        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            let query_index = query_id - 1; // Convert 1-based to 0-based index

            let sql = NLJ_QUERIES[query_index];
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(sql, &query_id.to_string(), &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    return Err(DataFusionError::Context(
                        "NLJ benchmark Q{query_id} failed with error:".to_string(),
                        Box::new(e),
                    ));
                }
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Validates that the query's physical plan uses a NestedLoopJoin (NLJ),
    /// then executes the query and collects execution times.
    ///
    /// TODO: ensure the optimizer won't change the join order (it's not at
    /// v48.0.0).
    async fn benchmark_query(
        &self,
        sql: &str,
        query_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let mut query_results = vec![];

        // Validate that the query plan includes a Nested Loop Join
        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;
        let plan_string = format!("{physical_plan:#?}");

        if !plan_string.contains("NestedLoopJoinExec") {
            return Err(exec_datafusion_err!(
                "Query {query_name} does not use Nested Loop Join. Physical plan: {plan_string}"
            ));
        }

        for i in 0..self.common.iterations {
            let start = Instant::now();

            let row_count = Self::execute_sql_without_result_buffering(sql, ctx).await?;

            let elapsed = start.elapsed();

            println!(
                "Query {query_name} iteration {i} returned {row_count} rows in {elapsed:?}"
            );

            query_results.push(QueryResult { elapsed, row_count });
        }

        Ok(query_results)
    }

    /// Executes the SQL query and drops each result batch after evaluation, to
    /// minimizes memory usage by not buffering results.
    ///
    /// Returns the total result row count
    async fn execute_sql_without_result_buffering(
        sql: &str,
        ctx: &SessionContext,
    ) -> Result<usize> {
        let mut row_count = 0;

        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;
        let mut stream = execute_stream(physical_plan, ctx.task_ctx())?;

        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();

            // Evaluate the result and do nothing, the result will be dropped
            // to reduce memory pressure
        }

        Ok(row_count)
    }
}
