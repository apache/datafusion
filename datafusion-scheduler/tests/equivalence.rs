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

//! End-to-end equivalence tests: run representative plans (joins, sorts,
//! multi-stage chains) through `run_distributed` and assert the result is the
//! same multiset of rows as single-node `collect`, via `assert_distributed_eq`.
//!
//! Plus the #1907-class regression guard: prove that when a file scan is split
//! into isolated per-task plans, each task reads ONLY its own partition rather
//! than draining a plan-instance-shared pool of file work and scanning the
//! whole table (the failure class of Ballista issue #1907).
//!
//! Plan shapes quoted in each test's doc comment were captured with
//! `displayable(plan.as_ref()).indent(false)` against this DataFusion checkout
//! at `target_partitions = 4`.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion_scheduler::test_util::assert_distributed_eq;

/// Registers two 2-partition `MemTable`s, `l(k INT, v INT)` and
/// `r(k INT, v INT)`, against a `SessionContext` configured with
/// `target_partitions = 4`. The hash-join single-partition thresholds are
/// dropped to 0 so a join on `k` plans as a `Partitioned` join with both sides
/// hash-repartitioned (two shuffle inputs into the join stage) rather than a
/// `CollectLeft`/broadcast that these tiny tables would otherwise qualify for.
///
/// `prefer_hash_join` selects a `HashJoinExec` (true) vs a `SortMergeJoinExec`
/// (false); both still hash-repartition each side.
async fn context_with_two_partitioned_tables(prefer_hash_join: bool) -> SessionContext {
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
        MemTable::try_new(schema.clone(), vec![vec![lbatch0], vec![lbatch1]]).unwrap();
    let rtable = MemTable::try_new(schema, vec![vec![rbatch0], vec![rbatch1]]).unwrap();
    ctx.register_table("l", Arc::new(ltable)).unwrap();
    ctx.register_table("r", Arc::new(rtable)).unwrap();
    ctx
}

/// Registers table `t(a INT)` backed by a 2-partition `MemTable`, against a
/// `SessionContext` configured with `target_partitions = 4`.
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
/// `prefer_hash_join = true`. Both sides hash-repartition on `k`, so the join
/// stage reads two shuffle inputs.
///
/// Plan (`displayable(..).indent(false)`, target_partitions = 4):
///
/// ```text
/// HashJoinExec: mode=Partitioned, join_type=Inner, on=[(k@0, k@0)], projection=[k@0, v@1, v@3]
///   RepartitionExec: partitioning=Hash([k@0], 4), input_partitions=2
///     DataSourceExec: partitions=2, partition_sizes=[1, 1]
///   RepartitionExec: partitioning=Hash([k@0], 4), input_partitions=2
///     DataSourceExec: partitions=2, partition_sizes=[1, 1]
/// ```
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

/// **Test 2 — sort-merge join.** Same query with `prefer_hash_join = false`,
/// which plans a `SortMergeJoinExec`; each side is still hash-repartitioned on
/// `k` and then sorted, so the join stage again reads two shuffle inputs.
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

/// **Test 3 — sort (multiset equivalence).** `SELECT a FROM t ORDER BY a`
/// WITHOUT a LIMIT. The top `SortPreservingMergeExec` is a shuffle boundary
/// (gather to 1 partition). `assert_distributed_eq` compares order-insensitively
/// and v1 models `SortPreservingMergeExec` as a plain gather (no global-order
/// preservation, a deliberate spec deferral — see the spec's "Open questions"),
/// so a full ORDER BY yields the same MULTISET and must pass.
///
/// Plan (`displayable(..).indent(false)`, target_partitions = 4):
///
/// ```text
/// SortPreservingMergeExec: [a@0 ASC NULLS LAST]
///   SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
///     DataSourceExec: partitions=2, partition_sizes=[1, 1]
/// ```
#[tokio::test]
async fn sort_without_limit_matches_collect_as_multiset() {
    let ctx = context_with_partitioned_table().await;
    let df = ctx.sql("SELECT a FROM t ORDER BY a").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    assert_distributed_eq(&ctx, plan).await;
}

/// Documents a known v1 limitation, kept as an `#[ignore]`d test so it never
/// gates CI. `SELECT a FROM t ORDER BY a LIMIT 5` (top-N) selects a specific
/// SET of rows that depends on GLOBAL order. Because v1 models
/// `SortPreservingMergeExec` as a plain gather that does NOT preserve global
/// order (see the spec's "Open questions": SortPreservingMergeExec global-order
/// fidelity), the distributed path can legitimately return a different set of 5
/// rows than single-node `collect`. This is expected until order-preserving
/// shuffle lands, so the test is ignored rather than asserted.
#[tokio::test]
#[ignore = "top-N after sort is a known v1 limitation: SortPreservingMergeExec \
            is modeled as an unordered gather (see spec Open questions)"]
async fn top_n_after_sort_is_a_known_v1_limitation() {
    let ctx = context_with_partitioned_table().await;
    let df = ctx.sql("SELECT a FROM t ORDER BY a LIMIT 5").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    assert_distributed_eq(&ctx, plan).await;
}

/// **Test 4 — three-stage chain.** `SELECT a, count(*) AS c FROM t GROUP BY a
/// ORDER BY a`: a hash-repartition boundary for the final aggregate plus a
/// `SortPreservingMergeExec` gather for the sort → two shuffle boundaries, three
/// stages. Multiset equivalence (order-insensitive).
///
/// Plan (`displayable(..).indent(false)`, target_partitions = 4):
///
/// ```text
/// SortPreservingMergeExec: [a@0 ASC NULLS LAST]
///   SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]
///     ProjectionExec: expr=[a@0 as a, count(Int64(1))@1 as c]
///       AggregateExec: mode=FinalPartitioned, gby=[a@0 as a], aggr=[count(Int64(1))]
///         RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=2
///           AggregateExec: mode=Partial, gby=[a@0 as a], aggr=[count(Int64(1))]
///             DataSourceExec: partitions=2, partition_sizes=[1, 1]
/// ```
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
/// Writes 100 rows (`x = 0..24, 100..124, 200..224, 300..324`) across 4 CSV
/// files, registers them as one table, and runs `SELECT count(*), sum(x)`. When
/// the scan splits into isolated per-task plans, each task must read ONLY its
/// own file group. This is the exact failure class of Ballista issue
/// [#1907](https://github.com/apache/datafusion-ballista/issues/1907). In this
/// checkout the mechanism is the **morsel-driven file scan**
/// (`datafusion/datasource/src/morsel/`): a `DataSourceExec` hands file work out
/// from a pool **shared across the plan instance's output partitions**, drained
/// cooperatively only when *all* partitions are polled concurrently. A
/// distributed executor polls **one partition per isolated task**, so that task
/// drains the whole pool and scans the entire table. (Reproduces with a bare
/// DataFusion plan and a single `execute(0, ..)` call — no scheduler involved.)
///
/// This harness surfaces exactly that. Observed at `target_partitions = 4`
/// (see the captured plan/stage dump in the Task 7 report):
///
/// - single-node `collect` (all 4 partitions polled together, queue drained
///   cooperatively): `n = 100`, `s = 16200` — correct.
/// - `run_distributed` (4 isolated tasks, each polling its 1 partition alone):
///   `n = 400`, `s = 64800` — **exactly 4× (= num_producer_tasks) inflation.**
///
/// The stage split is correct (stage 0 = `Partial` aggregate over the
/// `DataSourceExec: file_groups={4 groups}`, `num_producer_tasks = 4`; stage 1 =
/// `Final` aggregate reading the shuffle) and each task's file paths are
/// correct — the inflation is DataFusion's morsel-driven scan behavior, NOT a
/// defect in this harness. Per the crate's purpose we do NOT weaken the
/// assertion or force it green; the test is `#[ignore]`d as a caught regression
/// to be triaged. Run with `--ignored` to reproduce; it should pass once the
/// underlying DataFusion scan-isolation behavior is fixed.
#[tokio::test]
#[ignore = "CAUGHT #1907-class regression: DataFusion's morsel-driven file \
            scan makes each isolated per-task scan read the whole table \
            (observed 4x inflation: n=400 vs 100, s=64800 vs 16200). Genuine \
            DataFusion behavior surfaced by the harness, not a test defect."]
async fn regression_isolated_scan_reads_only_its_partition() {
    let dir = tempfile::TempDir::new().unwrap();
    // 4 files, 25 rows each = 100 rows total; distinct x per file so sum is
    // exact and any per-task double-read is visible as an integer multiple.
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

    // Distributed sum/count must equal single-node collect. A whole-table-per
    // -task scan inflates both by the task count; this assertion catches it.
    assert_distributed_eq(&ctx, plan).await;
}
