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

// TODO: Add existence joins

/// Run the Hash Join benchmark
///
/// This micro-benchmark focuses on the performance characteristics of Hash Joins.
/// It uses simple equality predicates to ensure a hash join is selected.
/// Where we vary selectivity, we do so with additional cheap predicates that
/// do not change the join key (so the physical operator remains HashJoin).
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 1 and 12). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options (iterations, batch size, target_partitions, etc.)
    #[structopt(flatten)]
    common: CommonOpt,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<std::path::PathBuf>,
}

/// Inline SQL queries for Hash Join benchmarks
///
/// Each query's comment includes:
///   - Left row count × Right row count
///   - Join predicate selectivity (approximate output fraction).
///   - Q11 and Q12 selectivity is relative to cartesian product while the others are
///     relative to probe side.
const HASH_QUERIES: &[&str] = &[
    // Q1: INNER 10 x 10K | LOW ~0.1%
    // equality on key + cheap filter to downselect
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 9000, 1000) AS t1(value)
        JOIN range(10000) AS t2
        ON t1.value = t2.value;
    "#,
    // Q2: INNER 10 x 10K | LOW ~0.1%
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 9000, 1000) AS t1
        JOIN range(10000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 5 = 0
    "#,
    // Q3: INNER 10K x 10K | HIGH ~90%
    r#"
        SELECT t1.value, t2.value
        FROM range(10000) AS t1
        JOIN range(10000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 10 <> 0
    "#,
    // Q4: INNER 30 x 30K | LOW ~0.1%
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 29000, 1000) AS t1
        JOIN range(30000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 5 = 0
    "#,
    // Q5: INNER 10 x 200K | VERY LOW ~0.005% (small to large)
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 9000, 1000) AS t1
        JOIN range(200000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 1000 = 0
    "#,
    // Q6: INNER 200K x 10 | VERY LOW ~0.005% (large to small)
    r#"
        SELECT t1.value, t2.value
        FROM range(200000) AS t1
        JOIN generate_series(0, 9000, 1000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 1000 = 0
    "#,
    // Q7: RIGHT OUTER 10 x 200K | LOW ~0.1%
    // Outer join still uses HashJoin for equi-keys; the extra filter reduces matches
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM generate_series(0, 9000, 1000) AS t1
        RIGHT JOIN range(200000) AS t2
          ON t1.value = t2.value
        WHERE t2.value % 1000 = 0
    "#,
    // Q8: LEFT OUTER 200K x 10 | LOW ~0.1%
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM range(200000) AS t1
        LEFT JOIN generate_series(0, 9000, 1000) AS t2
          ON t1.value = t2.value
        WHERE t1.value % 1000 = 0
    "#,
    // Q9: FULL OUTER 30 x 30K | LOW ~0.1%
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM generate_series(0, 29000, 1000) AS t1
        FULL JOIN range(30000) AS t2
          ON t1.value = t2.value
        WHERE COALESCE(t1.value, t2.value) % 1000 = 0
    "#,
    // Q10: FULL OUTER 30 x 30K | HIGH ~90%
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM generate_series(0, 29000, 1000) AS t1
        FULL JOIN range(30000) AS t2
          ON t1.value = t2.value
        WHERE COALESCE(t1.value, t2.value) % 10 <> 0
    "#,
    // Q11: INNER 30 x 30K | MEDIUM ~50% | cheap predicate on parity
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 29000, 1000) AS t1
        INNER JOIN range(30000) AS t2
          ON (t1.value % 2) = (t2.value % 2)
    "#,
    // Q12: FULL OUTER 30 x 30K | MEDIUM ~50% | expression key
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM generate_series(0, 29000, 1000) AS t1
        FULL JOIN range(30000) AS t2
          ON (t1.value % 2) = (t2.value % 2)
    "#,
    // Q13: INNER 30 x 30K | LOW 0.1% | modulo with adding values
    r#"
        SELECT t1.value, t2.value
        FROM generate_series(0, 29000, 1000) AS t1
        INNER JOIN range(30000) AS t2
          ON (t1.value = t2.value) AND ((t1.value + t2.value) % 10 < 1)
    "#,
    // Q14: FULL OUTER 30 x 30K | ALL ~100% | modulo
    r#"
        SELECT t1.value AS l, t2.value AS r
        FROM generate_series(0, 29000, 1000) AS t1
        FULL JOIN range(30000) AS t2
          ON (t1.value = t2.value) AND ((t1.value + t2.value) % 10 = 0)
    "#,
];

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running Hash Join benchmarks with the following options: {self:#?}\n");

        let query_range = match self.query {
            Some(query_id) => {
                if query_id >= 1 && query_id <= HASH_QUERIES.len() {
                    query_id..=query_id
                } else {
                    return exec_err!(
                        "Query {query_id} not found. Available queries: 1 to {}",
                        HASH_QUERIES.len()
                    );
                }
            }
            None => 1..=HASH_QUERIES.len(),
        };

        let config = self.common.config()?;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        let mut benchmark_run = BenchmarkRun::new();

        for query_id in query_range {
            let query_index = query_id - 1;
            let sql = HASH_QUERIES[query_index];

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
                        format!("Hash Join benchmark Q{query_id} failed with error:"),
                        Box::new(e),
                    ));
                }
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    /// Validates that the physical plan uses a HashJoin, then executes.
    async fn benchmark_query(
        &self,
        sql: &str,
        query_name: &str,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let mut query_results = vec![];

        // Build/validate plan
        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;
        let plan_string = format!("{physical_plan:#?}");

        if !plan_string.contains("HashJoinExec") {
            return Err(exec_datafusion_err!(
                "Query {query_name} does not use Hash Join. Physical plan: {plan_string}"
            ));
        }

        // Execute without buffering
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

    /// Executes the SQL query and drops each batch to avoid result buffering.
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
            // Drop batches immediately to minimize memory pressure
        }

        Ok(row_count)
    }
}
