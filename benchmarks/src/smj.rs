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
use datafusion_common::{exec_datafusion_err, exec_err, DataFusionError};
use structopt::StructOpt;

use futures::StreamExt;

/// Run the Sort Merge Join (SMJ) benchmark
///
/// This micro-benchmark focuses on the performance characteristics of SMJs.
///
/// It uses equality join predicates (to ensure SMJ is selected) and varies:
/// - Join type: Inner/Left/Right/Full/LeftSemi/LeftAnti/RightSemi/RightAnti
/// - Key cardinality: 1:1, 1:N, N:M relationships
/// - Filter selectivity: Low (1%), Medium (10%), High (50%)
/// - Input sizes: Small to large, balanced and skewed
///
/// All inputs are pre-sorted in CTEs before the join to isolate join
/// performance from sort overhead.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 1 and 20). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<std::path::PathBuf>,
}

/// Inline SQL queries for SMJ benchmarks
///
/// Each query's comment includes:
///   - Join type
///   - Left row count × Right row count
///   - Key cardinality (rows per key)
///   - Filter selectivity (if applicable)
const SMJ_QUERIES: &[&str] = &[
    // Q1: INNER 100K x 100K | 1:1
    r#"
        WITH t1_sorted AS (
            SELECT value as key FROM range(100000) ORDER BY value
        ),
        t2_sorted AS (
            SELECT value as key FROM range(100000) ORDER BY value
        )
        SELECT t1_sorted.key as k1, t2_sorted.key as k2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
    "#,
    // Q2: INNER 100K x 1M | 1:10
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
    "#,
    // Q3: INNER 1M x 1M | 1:100
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
    "#,
    // Q4: INNER 100K x 1M | 1:10 | 1%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        WHERE t2_sorted.data % 100 = 0
    "#,
    // Q5: INNER 1M x 1M | 1:100 | 10%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        WHERE t1_sorted.data <> t2_sorted.data AND t2_sorted.data % 10 = 0
    "#,
    // Q6: LEFT 100K x 1M | 1:10
    r#"
        WITH t1_sorted AS (
            SELECT value % 10500 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted LEFT JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
    "#,
    // Q7: LEFT 100K x 1M | 1:10 | 50%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted LEFT JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        WHERE t2_sorted.data IS NULL OR t2_sorted.data % 2 = 0
    "#,
    // Q8: FULL 100K x 100K | 1:10
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 12500 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key as k1, t1_sorted.data as d1,
               t2_sorted.key as k2, t2_sorted.data as d2
        FROM t1_sorted FULL JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
    "#,
    // Q9: FULL 100K x 1M | 1:10 | 10%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key as k1, t1_sorted.data as d1,
               t2_sorted.key as k2, t2_sorted.data as d2
        FROM t1_sorted FULL JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        WHERE (t1_sorted.data IS NULL OR t2_sorted.data IS NULL
               OR t1_sorted.data <> t2_sorted.data)
          AND (t1_sorted.data IS NULL OR t1_sorted.data % 10 = 0)
    "#,
    // Q10: LEFT SEMI 100K x 1M | 1:10
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key
            FROM range(1000000)
            ORDER BY key
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
        )
    "#,
    // Q11: LEFT SEMI 100K x 1M | 1:10 | 1%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
              AND t2_sorted.data <> t1_sorted.data
              AND t2_sorted.data % 100 = 0
        )
    "#,
    // Q12: LEFT SEMI 100K x 1M | 1:10 | 50%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
              AND t2_sorted.data <> t1_sorted.data
              AND t2_sorted.data % 2 = 0
        )
    "#,
    // Q13: LEFT SEMI 100K x 1M | 1:10 | 90%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
              AND t2_sorted.data % 10 <> 0
        )
    "#,
    // Q14: LEFT ANTI 100K x 1M | 1:10
    r#"
        WITH t1_sorted AS (
            SELECT value % 10500 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key
            FROM range(1000000)
            ORDER BY key
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE NOT EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
        )
    "#,
    // Q15: LEFT ANTI 100K x 1M | 1:10 | partial match
    r#"
        WITH t1_sorted AS (
            SELECT value % 12000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key
            FROM range(1000000)
            ORDER BY key
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE NOT EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
        )
    "#,
    // Q16: LEFT ANTI 100K x 100K | 1:1 | stress
    r#"
        WITH t1_sorted AS (
            SELECT value % 11000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key
            FROM range(100000)
            ORDER BY key
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE NOT EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
        )
    "#,
    // Q17: INNER 100K x 5M | 1:50 | 5%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(5000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data as d1, t2_sorted.data as d2
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        WHERE t2_sorted.data <> t1_sorted.data AND t2_sorted.data % 20 = 0
    "#,
    // Q18: LEFT SEMI 100K x 5M | 1:50 | 2%
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(5000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
              AND t2_sorted.data <> t1_sorted.data
              AND t2_sorted.data % 50 = 0
        )
    "#,
    // Q19: LEFT ANTI 100K x 5M | 1:50 | partial match
    r#"
        WITH t1_sorted AS (
            SELECT value % 15000 as key, value as data
            FROM range(100000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key
            FROM range(5000000)
            ORDER BY key
        )
        SELECT t1_sorted.key, t1_sorted.data
        FROM t1_sorted
        WHERE NOT EXISTS (
            SELECT 1 FROM t2_sorted
            WHERE t2_sorted.key = t1_sorted.key
        )
    "#,
    // Q20: INNER 1M x 10M | 1:100 + GROUP BY
    r#"
        WITH t1_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(1000000)
            ORDER BY key, data
        ),
        t2_sorted AS (
            SELECT value % 10000 as key, value as data
            FROM range(10000000)
            ORDER BY key, data
        )
        SELECT t1_sorted.key, count(*) as cnt
        FROM t1_sorted JOIN t2_sorted ON t1_sorted.key = t2_sorted.key
        GROUP BY t1_sorted.key
    "#,
];

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running SMJ benchmarks with the following options: {self:#?}\n");

        // Define query range
        let query_range = match self.query {
            Some(query_id) => {
                if query_id >= 1 && query_id <= SMJ_QUERIES.len() {
                    query_id..=query_id
                } else {
                    return exec_err!(
                        "Query {query_id} not found. Available queries: 1 to {}",
                        SMJ_QUERIES.len()
                    );
                }
            }
            None => 1..=SMJ_QUERIES.len(),
        };

        let mut config = self.common.config()?;
        // Disable hash joins to force SMJ
        config = config.set_bool("datafusion.optimizer.prefer_hash_join", false);
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            let query_index = query_id - 1; // Convert 1-based to 0-based index

            let sql = SMJ_QUERIES[query_index];
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
                        format!("SMJ benchmark Q{query_id} failed with error:"),
                        Box::new(e),
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
        let mut query_results = vec![];

        // Validate that the query plan includes a Sort Merge Join
        let df = ctx.sql(sql).await?;
        let physical_plan = df.create_physical_plan().await?;
        let plan_string = format!("{physical_plan:#?}");

        if !plan_string.contains("SortMergeJoinExec") {
            return Err(exec_datafusion_err!(
                "Query {query_name} does not use Sort Merge Join. Physical plan: {plan_string}"
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
