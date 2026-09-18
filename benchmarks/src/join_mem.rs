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

//! Memory-limited join benchmark: one join workload through a fixed memory
//! budget, in each configuration a user can pick today.
//!
//! `HashJoinExec` cannot spill its build side, so some rows are *expected* to
//! fail with `ResourcesExhausted`. The benchmark keeps that matrix
//! reproducible while external hash join is built, and reports when a row
//! flips. Row 4 divided by row 3a is the "SMJ tax": what the only workaround
//! costs on a join that would have fit in memory.
//!
//! Both inputs read the same generated file, so there is no smaller side for
//! the planner to swap in: the failure is not one a better build side avoids.
//!
//! The rows and their recorded outcomes are in `benchmarks/README.md`;
//! `datafusion/core/tests/memory_limit/join_failure_matrix.rs` asserts the same
//! matrix at test scale.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use clap::Args;
use datafusion::error::Result;
use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, exec_err, human_readable_size};
use futures::StreamExt;

use crate::util::{BenchmarkRun, CommonOpt, QueryResult};

/// Budget the matrix was recorded under, used when none is configured.
const DEFAULT_MEMORY_LIMIT: usize = 300 * 1024 * 1024;

/// Partition count it was recorded under: the per-partition build side is what
/// does or does not fit in the budget.
const DEFAULT_PARTITIONS: usize = 4;

/// Row 4 / row 3a when the matrix was recorded.
const RECORDED_SMJ_TAX: f64 = 3.5;

/// Run the memory-limited join benchmark
///
/// Runs the join failure matrix under a fixed memory budget: each row is a
/// configuration a user can choose today, and its recorded outcome is the
/// baseline external hash join has to improve on.
#[derive(Debug, Args, Clone)]
#[command(verbatim_doc_comment)]
pub struct RunOpt {
    /// Matrix row to run (1, 2, 3a, 3b, 4 or 5). If not specified, runs all rows
    #[arg(short = 'q', long = "query")]
    query: Option<String>,

    /// Common options (iterations, memory limit, memory pool type, partitions, etc.)
    #[command(flatten)]
    common: CommonOpt,

    /// Directory holding the generated parquet file, generated on first use.
    /// Defaults to `datafusion-join-mem` under the system temp dir
    #[arg(short = 'p', long = "path")]
    path: Option<PathBuf>,

    /// Rows in the generated table. Both join inputs read it
    #[arg(long = "rows", default_value = "20000000")]
    rows: usize,

    /// Build-side row cap that fits in the budget (rows 3a and 4)
    #[arg(long = "fit-rows", default_value = "10000000")]
    fit_rows: usize,

    /// Build-side row cap just past the budget (row 3b)
    #[arg(long = "over-rows", default_value = "12000000")]
    over_rows: usize,

    /// If present, write results json here
    #[arg(short = 'o', long = "output")]
    output_path: Option<PathBuf>,
}

/// Which query a row runs. The two filtered variants bracket the point where
/// the build side stops fitting in the budget.
#[derive(Debug, Clone, Copy)]
enum MatrixQuery {
    Join,
    JoinFits,
    JoinOverflows,
    /// Hash aggregation over the same data, as a control
    Control,
}

/// One row of the failure matrix.
#[derive(Debug, Clone, Copy)]
struct MatrixRow {
    label: &'static str,
    /// What the row is here to show
    role: &'static str,
    query: MatrixQuery,
    prefer_hash_join: bool,
    /// Operator the row is about; the plan is checked to contain it
    operator: Option<&'static str>,
    /// Whether the row completed when the matrix was recorded
    completes: bool,
}

const MATRIX: &[MatrixRow] = &[
    MatrixRow {
        label: "1",
        role: "default settings - the planner picks HashJoinExec",
        query: MatrixQuery::Join,
        prefer_hash_join: true,
        operator: Some("HashJoinExec"),
        completes: false,
    },
    MatrixRow {
        label: "2",
        role: "the only workaround - prefer_hash_join=false, sorts spill",
        query: MatrixQuery::Join,
        prefer_hash_join: false,
        operator: Some("SortMergeJoinExec"),
        completes: true,
    },
    MatrixRow {
        label: "3a",
        role: "where the ceiling is - build side filtered to a fitting size",
        query: MatrixQuery::JoinFits,
        prefer_hash_join: true,
        operator: Some("HashJoinExec"),
        completes: true,
    },
    MatrixRow {
        label: "3b",
        role: "just past the ceiling - the same filter, slightly larger",
        query: MatrixQuery::JoinOverflows,
        prefer_hash_join: true,
        operator: Some("HashJoinExec"),
        completes: false,
    },
    MatrixRow {
        label: "4",
        role: "what the workaround costs - row 3a forced through SMJ",
        query: MatrixQuery::JoinFits,
        prefer_hash_join: false,
        operator: Some("SortMergeJoinExec"),
        completes: true,
    },
    MatrixRow {
        label: "5",
        role: "control - hash aggregation through the same budget",
        query: MatrixQuery::Control,
        prefer_hash_join: true,
        operator: None,
        completes: true,
    },
];

/// Spill metrics of one operator of an executed plan.
struct OperatorSpill {
    operator: String,
    spill_count: usize,
    spilled_bytes: usize,
}

/// What one matrix row did.
struct RowResult {
    iterations: Vec<QueryResult>,
    /// The allocation that failed, when the row ran out of budget
    error: Option<String>,
    /// Spill metrics of the last executed plan
    spills: Vec<OperatorSpill>,
}

impl RowResult {
    fn completes(&self) -> bool {
        self.error.is_none()
    }

    fn mean_elapsed(&self) -> Option<Duration> {
        let total: Duration = self.iterations.iter().map(|iter| iter.elapsed).sum();
        (!self.iterations.is_empty()).then(|| total / self.iterations.len() as u32)
    }
}

fn outcome(completes: bool) -> &'static str {
    if completes { "completes" } else { "exhausted" }
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let common = self.common_with_defaults();
        println!("Running memory-limited join benchmark: {self:#?}\n");

        let rows = match &self.query {
            None => MATRIX.iter().collect::<Vec<_>>(),
            Some(label) => match MATRIX.iter().find(|row| row.label == *label) {
                Some(row) => vec![row],
                None => return exec_err!("Matrix row {label} not found"),
            },
        };
        let data = self.ensure_data(&common).await?;

        let mut benchmark_run = BenchmarkRun::new();
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let sql = self.sql(row);
            let ctx = self.context(&common, row, &data).await?;
            benchmark_run.set_memory_pool(&ctx.runtime_env().memory_pool);
            benchmark_run.start_new_case(&format!("row {} ({})", row.label, row.role));

            println!("--- row {}: {}\n{sql}", row.label, row.role);
            let result = self.run_row(&ctx, row, &sql, common.iterations).await?;

            for iter in &result.iterations {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
            if !result.completes() {
                benchmark_run.mark_failed();
            }
            results.push((row, result));
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        self.report(&common, &results);
        Ok(())
    }

    /// Fill in the settings the matrix was recorded under, for whatever the
    /// caller left unset.
    fn common_with_defaults(&self) -> CommonOpt {
        let mut common = self.common.clone();
        // Leave the env var path in `runtime_env_builder` alone if that is the
        // one carrying the limit.
        if common.memory_limit.is_none()
            && std::env::var("DATAFUSION_RUNTIME_MEMORY_LIMIT").is_err()
        {
            common.memory_limit = Some(DEFAULT_MEMORY_LIMIT);
        }
        common.partitions = common.partitions.or(Some(DEFAULT_PARTITIONS));
        common
    }

    fn sql(&self, row: &MatrixRow) -> String {
        let join_with_build_limit = |limit| {
            format!(
                "SELECT count(*) FROM t_probe p \
                 JOIN (SELECT * FROM t_build WHERE k <= {limit}) b ON p.k = b.k"
            )
        };
        match row.query {
            MatrixQuery::Join => {
                "SELECT count(*) FROM t_probe p JOIN t_build b ON p.k = b.k".to_string()
            }
            MatrixQuery::JoinFits => join_with_build_limit(self.fit_rows),
            MatrixQuery::JoinOverflows => join_with_build_limit(self.over_rows),
            MatrixQuery::Control => {
                "SELECT count(DISTINCT payload) FROM t_build".to_string()
            }
        }
    }

    /// A context with its own budgeted runtime, so each row starts from an
    /// empty pool.
    async fn context(
        &self,
        common: &CommonOpt,
        row: &MatrixRow,
        data: &Path,
    ) -> Result<SessionContext> {
        let mut config = common.config()?;
        config.options_mut().optimizer.prefer_hash_join = row.prefer_hash_join;
        let ctx = SessionContext::new_with_config_rt(config, common.build_runtime()?);

        let path = data.to_str().ok_or_else(|| {
            DataFusionError::Execution(format!("non-UTF-8 data path {}", data.display()))
        })?;
        for table in ["t_build", "t_probe"] {
            ctx.register_parquet(table, path, Default::default())
                .await?;
        }
        Ok(ctx)
    }

    /// Run one row. Running out of budget is a result, not an error: it is what
    /// several rows are here to record.
    async fn run_row(
        &self,
        ctx: &SessionContext,
        row: &MatrixRow,
        sql: &str,
        iterations: usize,
    ) -> Result<RowResult> {
        let mut result = RowResult {
            iterations: vec![],
            error: None,
            spills: vec![],
        };

        for i in 0..iterations {
            let plan = ctx.sql(sql).await?.create_physical_plan().await?;
            if let Some(operator) = row.operator {
                let plan_display = displayable(plan.as_ref()).indent(true).to_string();
                if !plan_display.contains(operator) {
                    return exec_err!(
                        "row {} is about {operator}, but its plan does not use it:\n{plan_display}",
                        row.label
                    );
                }
            }

            let start = Instant::now();
            let executed = drain(Arc::clone(&plan), ctx).await;
            let elapsed = start.elapsed();
            result.spills = collect_spills(plan.as_ref());

            match executed {
                Ok(row_count) => {
                    println!(
                        "row {} iteration {i} returned {row_count} rows in {elapsed:?}",
                        row.label
                    );
                    result.iterations.push(QueryResult { elapsed, row_count });
                }
                // Anything but exhaustion is a real failure.
                Err(e)
                    if !matches!(
                        e.find_root(),
                        DataFusionError::ResourcesExhausted(_)
                    ) =>
                {
                    return Err(e);
                }
                Err(e) => {
                    println!("row {} iteration {i} failed in {elapsed:?}", row.label);
                    println!("  {}", e.find_root());
                    result.error = Some(failed_allocation(&e.find_root().to_string()));
                    return Ok(result);
                }
            }
        }

        Ok(result)
    }

    /// Generate the table once and cache it on disk.
    ///
    /// Generated with no memory limit on purpose: writing the file under the
    /// benchmark's budget is a different fight than the one under test.
    async fn ensure_data(&self, common: &CommonOpt) -> Result<PathBuf> {
        let dir = self
            .path
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("datafusion-join-mem"));
        std::fs::create_dir_all(&dir)?;

        let file = dir.join(format!("join_mem_{}_rows.parquet", self.rows));
        if file.exists() {
            println!("Using existing data file {}", file.display());
            return Ok(file);
        }

        println!("Generating {} rows into {}", self.rows, file.display());
        let start = Instant::now();
        let ctx =
            SessionContext::new_with_config(common.update_config(SessionConfig::new()));
        // Write under a temporary name, so an interrupted run leaves no
        // truncated file behind to be picked up as cached data.
        let partial = file.with_extension("parquet.partial");
        ctx.sql(&format!(
            "COPY (SELECT v AS k, \
                    concat('payload-', v, '-', repeat('x', 24)) AS payload \
             FROM generate_series(1, {}) AS t(v)) \
             TO '{}' STORED AS PARQUET",
            self.rows,
            partial.display()
        ))
        .await?
        .collect()
        .await?;
        std::fs::rename(&partial, &file)?;
        println!("Generated in {:?}", start.elapsed());

        Ok(file)
    }

    /// Print the matrix: what each row did, next to what it did when recorded.
    fn report(&self, common: &CommonOpt, results: &[(&MatrixRow, RowResult)]) {
        let budget = common
            .memory_limit
            .map(human_readable_size)
            .unwrap_or_else(|| "unlimited".to_string());
        println!(
            "\nJoin failure matrix: {budget} {} pool, {} partitions, {} rows\n",
            common.mem_pool_type,
            common.partitions.unwrap_or(DEFAULT_PARTITIONS),
            self.rows
        );
        println!(
            "{:<4} {:<11} {:<11} {:<10} role",
            "row", "baseline", "actual", "mean"
        );

        for (row, result) in results {
            let mean = result
                .mean_elapsed()
                .map(|mean| format!("{:.3}s", mean.as_secs_f64()))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<4} {:<11} {:<11} {mean:<10} {}",
                row.label,
                outcome(row.completes),
                outcome(result.completes()),
                row.role
            );
            if let Some(error) = &result.error {
                println!("       error: {error}");
            }
            for spill in &result.spills {
                println!(
                    "       spilled: {} spill_count={} spilled_bytes={}",
                    spill.operator,
                    spill.spill_count,
                    human_readable_size(spill.spilled_bytes)
                );
            }
        }

        if let Some(tax) = smj_tax(results) {
            println!(
                "\nSMJ tax (row 4 / row 3a): {tax:.1}x on a join that would have fit \
                 (recorded: {RECORDED_SMJ_TAX:.1}x)"
            );
        }

        let flipped: Vec<_> = results
            .iter()
            .filter(|(row, result)| result.completes() != row.completes)
            .map(|(row, result)| {
                format!("row {}: now {}", row.label, outcome(result.completes()))
            })
            .collect();
        match flipped.is_empty() {
            true => println!("\nEvery row matched its recorded baseline."),
            false => println!("\nFlipped from the baseline: {}", flipped.join(", ")),
        }
    }
}

/// Execute `plan`, dropping each batch so the budget is spent on the join
/// rather than on holding results.
async fn drain(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Result<usize> {
    let mut stream = execute_stream(plan, ctx.task_ctx())?;
    let mut row_count = 0;
    while let Some(batch) = stream.next().await {
        row_count += batch?.num_rows();
    }
    Ok(row_count)
}

/// Spill metrics of every operator in `plan` that spilled, in plan order and
/// merged by operator name.
fn collect_spills(plan: &dyn ExecutionPlan) -> Vec<OperatorSpill> {
    let metrics = plan.metrics();
    let spill_count = metrics.as_ref().and_then(|m| m.spill_count()).unwrap_or(0);
    let spilled_bytes = metrics
        .as_ref()
        .and_then(|m| m.spilled_bytes())
        .unwrap_or(0);

    let mut spills = vec![];
    if spill_count > 0 || spilled_bytes > 0 {
        spills.push(OperatorSpill {
            operator: plan.name().to_string(),
            spill_count,
            spilled_bytes,
        });
    }
    for child in plan.children() {
        for spill in collect_spills(child.as_ref()) {
            match spills
                .iter_mut()
                .find(|held| held.operator == spill.operator)
            {
                Some(held) => {
                    held.spill_count += spill.spill_count;
                    held.spilled_bytes += spill.spilled_bytes;
                }
                None => spills.push(spill),
            }
        }
    }
    spills
}

/// The line of a pool error naming the allocation that failed, which the pool
/// prints after its list of top consumers.
fn failed_allocation(error: &str) -> String {
    error
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or(error)
        .trim_start_matches("Error: ")
        .to_string()
}

/// Row 4 divided by row 3a: the same join, once as the planner would run it and
/// once forced through the workaround. `None` unless both rows completed.
fn smj_tax(results: &[(&MatrixRow, RowResult)]) -> Option<f64> {
    let mean = |label: &str| {
        results
            .iter()
            .find(|(row, _)| row.label == label)
            .and_then(|(_, result)| result.mean_elapsed())
            .map(|mean| mean.as_secs_f64())
    };
    let (fits, forced) = (mean("3a")?, mean("4")?);
    (fits > 0.0).then_some(forced / fits)
}
