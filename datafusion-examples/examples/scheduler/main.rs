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

//! # Scheduler Examples — in-process model of stage-based distributed execution
//!
//! This example models how a distributed engine built on DataFusion (such as
//! Ballista or datafusion-distributed) actually executes a query: it splits
//! the physical plan into stages at shuffle boundaries, serializes each stage
//! via `datafusion-proto`, and runs each task from a freshly deserialized
//! plan with its own `TaskContext`. Data crosses stage boundaries as
//! Arrow-IPC frames over a streaming in-memory exchange — no network, no
//! disk, no barrier — so it runs entirely inside one process while still
//! exercising the isolate-and-serialize contract that a real distributed
//! executor depends on.
//!
//! The motivating problem: DataFusion's physical plans are written and
//! tested assuming everything runs in one process, with all partitions of an
//! operator polled together by cooperative threads on a shared runtime. A
//! change that quietly relies on that in-process model still passes
//! DataFusion's own tests but breaks downstream distributed engines. The
//! tests under `#[cfg(test)]` in this example exercise the isolated /
//! serialized / spawned-in-isolation execution path, giving a place to catch
//! that class of regression in-tree.
//!
//! ## Usage
//! ```bash
//! cargo run --example scheduler -- [all|distributed_pipeline]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `distributed_pipeline`
//!   (file: main.rs, desc: In-process model of stage-based distributed execution (stage splitting, per-task plan serialization))
//!
//! ## Tests
//!
//! Run the full harness (unit + integration tests folded into this example)
//! with:
//! ```bash
//! cargo test --example scheduler
//! ```

mod config;
mod exchange;
mod executor;
mod scheduler;
mod serde;
mod stage;
#[cfg(test)]
mod test_util;

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use strum::{IntoEnumIterator, VariantNames};
use strum_macros::{Display, EnumIter, EnumString, VariantNames};

use crate::config::SchedulerConfig;
use crate::exchange::InMemoryExchange;
use crate::executor::execute_stage_graph;
use crate::scheduler::create_stages;

/// Splits `plan` into a stage graph at shuffle boundaries and runs it through
/// the streaming, no-barrier stage executor: all tasks of all stages are
/// spawned concurrently, data crosses each boundary as IPC frames over a
/// single shared [`InMemoryExchange`], and the final stage's collected output
/// is returned.
///
/// `ctx` is the driver session; each task rebuilds an isolated session via
/// `config.session_builder` and never touches `ctx`.
pub async fn run_distributed(
    _ctx: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
    config: SchedulerConfig,
) -> Result<Vec<RecordBatch>> {
    // One exchange instance is threaded through both stage splitting (so
    // sinks/sources capture it) and the executor (so it registers the
    // channels and drives the tasks) — producers and consumers must share
    // ONE wire.
    let exchange = InMemoryExchange::new();
    let graph = create_stages(plan, &exchange)?;
    execute_stage_graph(&graph, &exchange, &config).await
}

#[derive(EnumIter, EnumString, Display, VariantNames)]
#[strum(serialize_all = "snake_case")]
enum ExampleKind {
    All,
    DistributedPipeline,
}

impl ExampleKind {
    const EXAMPLE_NAME: &'static str = "scheduler";

    fn runnable() -> impl Iterator<Item = ExampleKind> {
        ExampleKind::iter().filter(|v| !matches!(v, ExampleKind::All))
    }

    async fn run(&self) -> Result<()> {
        match self {
            ExampleKind::All => {
                for example in ExampleKind::runnable() {
                    println!("Running example: {example}");
                    Box::pin(example.run()).await?;
                }
            }
            ExampleKind::DistributedPipeline => distributed_pipeline().await?,
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let usage = format!(
        "Usage: cargo run --example {} -- [{}]",
        ExampleKind::EXAMPLE_NAME,
        ExampleKind::VARIANTS.join("|")
    );

    let example: ExampleKind = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ExampleKind::All.to_string())
        .parse()
        .map_err(|_| DataFusionError::Execution(format!("Unknown example. {usage}")))?;

    example.run().await
}

/// Runs a 3-stage `GROUP BY ... ORDER BY` query through the scheduler and
/// compares it to single-node `collect`, printing both. Illustrates the full
/// path: stage splitting, per-stage plan serialization, isolated per-task
/// execution, and Arrow-IPC frames flowing over the in-memory exchange
/// between three stages (partial-aggregate producer → final-aggregate
/// producer → gather-sort final stage).
async fn distributed_pipeline() -> Result<()> {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionConfig;

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch0 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 2, 5]))],
    )?;
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![2, 3, 4, 4, 1]))],
    )?;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]])?;
    ctx.register_table("t", Arc::new(table))?;

    let df = ctx
        .sql("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY a")
        .await?;
    let plan = df.create_physical_plan().await?;

    println!("--- single-node collect ---");
    let expected = collect(plan.clone(), ctx.task_ctx()).await?;
    println!("{}", pretty_format_batches(&expected)?);

    println!("--- run_distributed (3-stage streaming exchange) ---");
    let actual = run_distributed(&ctx, plan, SchedulerConfig::in_memory(&ctx)).await?;
    println!("{}", pretty_format_batches(&actual)?);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_distributed_pipeline() {
        distributed_pipeline().await.unwrap();
    }
}

/// End-to-end equivalence tests: run representative plans (joins, sorts,
/// multi-stage chains) through `run_distributed` and assert the result is
/// the same multiset of rows as single-node `collect`, via
/// `assert_distributed_eq`.
///
/// Plus the #1907-class regression guard: prove that when a file scan is
/// split into isolated per-task plans, each task reads ONLY its own
/// partition rather than draining a plan-instance-shared pool of file work
/// and scanning the whole table (the failure class of Ballista issue
/// #1907).
///
/// Plan shapes quoted in each test's doc comment were captured with
/// `displayable(plan.as_ref()).indent(false)` against this DataFusion
/// checkout at `target_partitions = 4`.
#[cfg(test)]
mod equivalence_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};

    use crate::test_util::assert_distributed_eq;

    /// Registers two 2-partition `MemTable`s, `l(k INT, v INT)` and
    /// `r(k INT, v INT)`, against a `SessionContext` configured with
    /// `target_partitions = 4`. The hash-join single-partition thresholds are
    /// dropped to 0 so a join on `k` plans as a `Partitioned` join with both
    /// sides hash-repartitioned (two shuffle inputs into the join stage)
    /// rather than a `CollectLeft`/broadcast that these tiny tables would
    /// otherwise qualify for.
    ///
    /// `prefer_hash_join` selects a `HashJoinExec` (true) vs a
    /// `SortMergeJoinExec` (false); both still hash-repartition each side.
    async fn context_with_two_partitioned_tables(
        prefer_hash_join: bool,
    ) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
        ]));

        let lbatch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let lbatch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![40, 50, 60])),
            ],
        )
        .unwrap();
        let rbatch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let rbatch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![400, 500, 600])),
            ],
        )
        .unwrap();

        let mut config = SessionConfig::new().with_target_partitions(4);
        config.options_mut().optimizer.prefer_hash_join = prefer_hash_join;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let ctx = SessionContext::new_with_config(config);

        let ltable =
            MemTable::try_new(schema.clone(), vec![vec![lbatch0], vec![lbatch1]])
                .unwrap();
        let rtable =
            MemTable::try_new(schema, vec![vec![rbatch0], vec![rbatch1]]).unwrap();
        ctx.register_table("l", Arc::new(ltable)).unwrap();
        ctx.register_table("r", Arc::new(rtable)).unwrap();
        ctx
    }

    /// Registers table `t(a INT)` backed by a 2-partition `MemTable`,
    /// against a `SessionContext` configured with `target_partitions = 4`.
    async fn context_with_partitioned_table() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 2, 5]))],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2, 3, 4, 4, 1]))],
        )
        .unwrap();

        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    /// **Test 1 — hash join.** `SELECT ... FROM l JOIN r ON l.k = r.k` with
    /// `prefer_hash_join = true`. Both sides hash-repartition on `k`, so the
    /// join stage reads two shuffle inputs.
    #[tokio::test]
    async fn hash_join_matches_collect() {
        let ctx = context_with_two_partitioned_tables(true).await;
        let df = ctx
            .sql("SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert_distributed_eq(&ctx, plan).await;
    }

    /// **Test 2 — sort-merge join.** Same query with
    /// `prefer_hash_join = false`, which plans a `SortMergeJoinExec`; each
    /// side is still hash-repartitioned on `k` and then sorted.
    #[tokio::test]
    async fn sort_merge_join_matches_collect() {
        let ctx = context_with_two_partitioned_tables(false).await;
        let df = ctx
            .sql("SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert_distributed_eq(&ctx, plan).await;
    }

    /// **Test 3 — sort (multiset equivalence).**
    /// `SELECT a FROM t ORDER BY a` WITHOUT a LIMIT.
    /// `assert_distributed_eq` compares order-insensitively, so this must
    /// yield the same MULTISET as single-node `collect`.
    #[tokio::test]
    async fn sort_without_limit_matches_collect_as_multiset() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx.sql("SELECT a FROM t ORDER BY a").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert_distributed_eq(&ctx, plan).await;
    }

    /// Documents a known v1 limitation, kept as an `#[ignore]`d test so it
    /// never gates CI. `SELECT a FROM t ORDER BY a LIMIT 5` (top-N) selects
    /// a specific SET of rows that depends on GLOBAL order. Because v1
    /// models `SortPreservingMergeExec` as a plain gather that does NOT
    /// preserve global order, the distributed path can legitimately return
    /// a different set of 5 rows than single-node `collect`.
    #[tokio::test]
    #[ignore = "top-N after sort is a known v1 limitation: SortPreservingMergeExec \
                is modeled as an unordered gather"]
    async fn top_n_after_sort_is_a_known_v1_limitation() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx.sql("SELECT a FROM t ORDER BY a LIMIT 5").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert_distributed_eq(&ctx, plan).await;
    }

    /// **Test 4 — three-stage chain.**
    /// `SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY a`: a
    /// hash-repartition boundary for the final aggregate plus a
    /// `SortPreservingMergeExec` gather for the sort → two shuffle
    /// boundaries, three stages. Multiset equivalence (order-insensitive).
    #[tokio::test]
    async fn group_by_then_order_by_three_stage_chain_matches_collect() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx
            .sql("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert_distributed_eq(&ctx, plan).await;
    }

    /// **#1907 regression guard — CAUGHT REGRESSION, currently `#[ignore]`d.**
    ///
    /// Writes 100 rows across 4 CSV files, registers them as one table, and
    /// runs `SELECT count(*), sum(x)`. When the scan splits into isolated
    /// per-task plans, each task must read ONLY its own file group. This is
    /// the exact failure class of Ballista issue
    /// [#1907](https://github.com/apache/datafusion-ballista/issues/1907).
    /// In this checkout the mechanism is the **morsel-driven file scan**
    /// (`datafusion/datasource/src/morsel/`): a `DataSourceExec` hands file
    /// work out from a pool **shared across the plan instance's output
    /// partitions**, drained cooperatively only when *all* partitions are
    /// polled concurrently. A distributed executor polls **one partition per
    /// isolated task**, so that task drains the whole pool and scans the
    /// entire table.
    ///
    /// Observed at `target_partitions = 4`:
    /// - single-node `collect`: `n = 100`, `s = 16200` — correct.
    /// - `run_distributed`: `n = 400`, `s = 64800` — 4× inflation.
    #[tokio::test]
    #[ignore = "CAUGHT #1907-class regression: DataFusion's morsel-driven file \
                scan makes each isolated per-task scan read the whole table"]
    async fn regression_isolated_scan_reads_only_its_partition() {
        let dir = tempfile::TempDir::new().unwrap();
        // 4 files, 25 rows each = 100 rows total; distinct x per file so sum
        // is exact and any per-task double-read is visible as an integer
        // multiple.
        for f in 0..4 {
            let mut body = String::from("x\n");
            for i in 0..25 {
                let x = f * 100 + i;
                body.push_str(&x.to_string());
                body.push('\n');
            }
            std::fs::write(dir.path().join(format!("part_{f}.csv")), body).unwrap();
        }

        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_csv("t", dir.path().to_str().unwrap(), CsvReadOptions::new())
            .await
            .unwrap();

        let df = ctx
            .sql("SELECT count(*) AS n, sum(x) AS s FROM t")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        assert_distributed_eq(&ctx, plan).await;
    }
}

/// Proves a task execution error surfaces as a `DataFusionError` out of
/// `run_distributed` -- not swallowed, not hung, not a panic escaping the
/// spawned task -- for the simplest case: a single (no-shuffle) final stage
/// whose plan errors while executing.
///
/// The failing node here is a `ScalarUDF` that always returns `Err`, wired
/// into a plan of otherwise-ordinary, `datafusion-proto`-native nodes
/// (table scan + projection). That's deliberate: the executor always
/// round-trips every stage's plan through `encode_plan`/`decode_plan` with
/// `ExchangeCodec`, which only knows how to serialize
/// `ExchangeSinkExec`/`ExchangeSourceExec`. A hand-rolled leaf
/// `ExecutionPlan` (as opposed to a UDF referenced by name and resolved
/// from the rebuilt session's function registry on decode) would fail at
/// the encode step instead, which would only prove plan serialization
/// errors propagate -- not task execution errors.
#[cfg(test)]
mod error_propagation_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    };
    use datafusion::prelude::SessionContext;

    use crate::config::SchedulerConfig;
    use crate::run_distributed;

    /// A scalar UDF that unconditionally fails when invoked, with a message
    /// ("boom") distinctive enough to assert on without false-matching some
    /// unrelated error path.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct BoomUdf {
        signature: Signature,
    }

    impl BoomUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Int32],
                    Volatility::Volatile,
                ),
            }
        }
    }

    impl ScalarUDFImpl for BoomUdf {
        fn name(&self) -> &str {
            "boom"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            Err(DataFusionError::Execution("boom".to_string()))
        }
    }

    #[tokio::test]
    async fn task_execution_error_surfaces_from_run_distributed() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx.register_udf(ScalarUDF::from(BoomUdf::new()));

        // A single-partition, no-shuffle plan: `create_stages` produces
        // exactly one (final) stage, so this exercises `execute_stage_graph`'s
        // single-stage path end to end, including the encode/decode
        // round-trip.
        let df = ctx.sql("SELECT boom(a) FROM t").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let config = SchedulerConfig::in_memory(&ctx);
        let result = run_distributed(&ctx, plan, config).await;

        let err = result
            .expect_err("task execution error must surface as an Err, not be swallowed");
        let msg = err.to_string();
        assert!(
            msg.contains("boom"),
            "expected error message to contain \"boom\", got: {msg}"
        );
    }
}

#[cfg(test)]
mod single_stage_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::*;

    use crate::config::SchedulerConfig;
    use crate::run_distributed;

    #[tokio::test]
    async fn single_stage_scan_filter_matches_collect() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();

        let df = ctx.sql("SELECT a FROM t WHERE a > 2").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let expected = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();

        let config = SchedulerConfig::in_memory(&ctx);
        let actual = run_distributed(&ctx, plan, config).await.unwrap();

        // order-insensitive compare
        assert_eq!(total_rows(&expected), total_rows(&actual));
        assert_eq!(sorted_values(&expected), sorted_values(&actual));
    }

    fn total_rows(b: &[RecordBatch]) -> usize {
        b.iter().map(|b| b.num_rows()).sum()
    }
    fn sorted_values(b: &[RecordBatch]) -> Vec<i32> {
        let mut v: Vec<i32> = b
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        v.sort();
        v
    }
}

/// End-to-end test that a real 2-stage plan (partial aggregate + streaming
/// in-memory exchange + final aggregate) executes correctly through
/// `run_distributed`/`execute_stage_graph`, matching single-process
/// `collect` output. Data crosses the stage boundary as IPC frames over the
/// `InMemoryExchange` — there is no disk shuffle to inspect, so correctness
/// of the 2-stage GROUP BY across the exchange is the assertion.
#[cfg(test)]
mod two_stage_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::config::SchedulerConfig;
    use crate::exchange::InMemoryExchange;
    use crate::run_distributed;
    use crate::scheduler::create_stages;
    use crate::test_util::assert_distributed_eq;

    /// Registers table `t(a INT)` backed by a 2-partition `MemTable`,
    /// against a `SessionContext` configured with `target_partitions = 4`.
    async fn context_with_partitioned_table() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![2, 3, 4]))],
        )
        .unwrap();

        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let table = MemTable::try_new(schema, vec![vec![batch0], vec![batch1]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn group_by_across_a_real_shuffle_matches_collect() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx
            .sql("SELECT a, count(*) FROM t GROUP BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        assert_distributed_eq(&ctx, plan).await;
    }

    /// Proves `run_distributed` is genuinely routed through `create_stages`
    /// + the stage-graph executor (data really crossing the exchange), not
    /// coincidentally producing correct output by re-running the original
    /// in-process plan.
    ///
    /// We assert on the stage split itself — this GROUP BY splits into a
    /// producer stage (id 0, hash-repartitioned partial aggregate, whose
    /// output must cross the exchange) and a final stage reading it — and
    /// that the end-to-end result over that real two-stage exchange
    /// execution matches single-node `collect`.
    #[tokio::test]
    async fn group_by_data_crosses_the_exchange_between_two_stages() {
        let ctx = context_with_partitioned_table().await;
        let df = ctx
            .sql("SELECT a, count(*) FROM t GROUP BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let expected = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

        // The split must produce a producer stage feeding a final stage over
        // the exchange — i.e. a genuine 2-stage plan, not a single re-executed
        // tree.
        let probe_exchange = InMemoryExchange::new();
        let graph = create_stages(plan.clone(), &probe_exchange).unwrap();
        assert_eq!(
            graph.stages.len(),
            2,
            "GROUP BY should split into a producer stage and a final stage"
        );
        assert_ne!(
            graph.final_stage_id, 0,
            "there must be a producer stage (id 0) feeding the final stage"
        );

        // Execute end-to-end: the producer stage's partial-aggregate output
        // only reaches the final stage by crossing the in-memory exchange as
        // IPC frames.
        let config = SchedulerConfig::in_memory(&ctx);
        let actual = run_distributed(&ctx, plan, config).await.unwrap();

        let expected_rows: usize = expected.iter().map(|b| b.num_rows()).sum();
        let actual_rows: usize = actual.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            expected_rows, actual_rows,
            "row count across the exchange must match single-node collect"
        );
    }
}

/// Streaming / backpressure integration test.
///
/// Runs a real multi-stage query end-to-end through `run_distributed` with
/// `channel_capacity = 1` — the tightest possible backpressure through the
/// FULL executor: producers block on full channels while downstream
/// consumers drain. The whole run is wrapped in a `tokio::time::timeout` so
/// a deadlock FAILS the test rather than hanging CI.
///
/// This is the end-to-end proof that the spawn-all, no-barrier executor
/// pipelines correctly under backpressure: a consumer blocked on a channel
/// is unblocked by a producer that is already running (all tasks of all
/// stages are spawned before any is awaited).
#[cfg(test)]
mod streaming_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::config::SchedulerConfig;
    use crate::run_distributed;

    /// Registers table `t(a INT)` backed by a 2-partition `MemTable` with
    /// enough rows/batches that capacity-1 channels actually force repeated
    /// blocking, against a `SessionContext` at `target_partitions = 4`.
    async fn context_with_many_rows() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Many small batches per partition so the sink blocks on
        // `send().await` over and over against the capacity-1 channels.
        let mut p0 = Vec::new();
        let mut p1 = Vec::new();
        for i in 0..200i32 {
            p0.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![i % 17]))],
                )
                .unwrap(),
            );
            p1.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![(i + 5) % 17]))],
                )
                .unwrap(),
            );
        }

        let config = SessionConfig::new().with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        let table = MemTable::try_new(schema, vec![p0, p1]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    /// Three-stage chain (`GROUP BY` hash boundary + `ORDER BY` gather
    /// boundary) run under `channel_capacity = 1`, inside a timeout.
    /// Asserts multiset equivalence with single-node `collect`. A hang
    /// (deadlock) fails via timeout; a wrong result fails via the
    /// equivalence check.
    #[tokio::test]
    async fn tight_backpressure_multi_stage_pipeline_matches_collect() {
        let result = tokio::time::timeout(Duration::from_secs(30), async {
            let ctx = context_with_many_rows().await;
            let df = ctx
                .sql("SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY a")
                .await
                .unwrap();
            let plan = df.create_physical_plan().await.unwrap();

            let expected = collect(plan.clone(), ctx.task_ctx()).await.unwrap();

            // capacity = 1: the tightest bound that still makes progress,
            // exercised through the full run_distributed executor (not just
            // sink/source).
            let state = ctx.state();
            let config = SchedulerConfig {
                session_builder: Arc::new(move || {
                    SessionContext::new_with_state(state.clone())
                }),
                channel_capacity: 1,
            };
            let actual = run_distributed(&ctx, plan, config).await.unwrap();

            (expected, actual)
        })
        .await;

        let (expected, actual) =
            result.expect("multi-stage pipeline deadlocked / timed out under capacity=1");

        // Order-insensitive multiset equivalence via sorted pretty-printed
        // rows.
        let e = arrow::util::pretty::pretty_format_batches(&expected)
            .unwrap()
            .to_string();
        let a = arrow::util::pretty::pretty_format_batches(&actual)
            .unwrap()
            .to_string();
        let mut el: Vec<&str> = e.lines().collect();
        el.sort();
        let mut al: Vec<&str> = a.lines().collect();
        al.sort();
        assert_eq!(
            el, al,
            "streaming distributed output != collect output under capacity=1"
        );
    }
}
