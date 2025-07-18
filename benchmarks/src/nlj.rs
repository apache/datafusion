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
use datafusion::{error::Result, prelude::SessionContext};
use datafusion_common::exec_datafusion_err;
use datafusion_common::instant::Instant;
use structopt::StructOpt;

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
    query_name: Option<String>,

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
];

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running NLJ benchmarks with the following options: {self:#?}\n");

        // Define available queries
        let available_queries =
            vec!["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"];
        let query_list = match &self.query_name {
            Some(query_name) => {
                if available_queries.contains(&query_name.as_str()) {
                    vec![query_name.as_str()]
                } else {
                    return Err(exec_datafusion_err!(
                        "Query '{}' not found. Available queries: {:?}",
                        query_name,
                        available_queries
                    ));
                }
            }
            None => available_queries,
        };

        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        let mut benchmark_run = BenchmarkRun::new();
        for query_name in query_list {
            let query_index = match query_name {
                "q1" => 0,
                "q2" => 1,
                "q3" => 2,
                "q4" => 3,
                "q5" => 4,
                "q6" => 5,
                "q7" => 6,
                "q8" => 7,
                "q9" => 8,
                "q10" => 9,
                _ => {
                    if self.query_name.is_some() {
                        return Err(exec_datafusion_err!(
                            "Could not find query '{}'.",
                            query_name
                        ));
                    }
                    continue;
                }
            };

            let sql = NLJ_QUERIES[query_index];
            benchmark_run.start_new_case(&format!("Query {query_name}"));
            let query_run = self.benchmark_query(sql, query_name, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    eprintln!("Query {query_name} failed: {e}");
                    benchmark_run.write_iter(std::time::Duration::from_secs(0), 0);
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
        let plan_string = format!("{:#?}", physical_plan);

        if !plan_string.contains("NestedLoopJoinExec") {
            return Err(exec_datafusion_err!(
                "Query {query_name} does not use Nested Loop Join. Physical plan: {plan_string}"
            ));
        }

        for i in 0..self.common.iterations {
            let start = Instant::now();
            let df = ctx.sql(sql).await?;
            let batches = df.collect().await?;
            let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;

            let row_count = batches.iter().map(|b| b.num_rows()).sum();
            println!(
                    "Query {query_name} iteration {i} returned {row_count} rows in {elapsed:?}"
                );

            query_results.push(QueryResult { elapsed, row_count });
        }

        Ok(query_results)
    }
}
