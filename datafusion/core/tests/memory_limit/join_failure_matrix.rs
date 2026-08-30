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

//! What one join query does under a fixed memory budget, in each configuration
//! a user can choose today.
//!
//! `HashJoinExec` cannot spill, so a build side that does not fit the budget
//! fails outright; the only way to run such a join today is
//! `prefer_hash_join=false`, which hands the work to `SortMergeJoinExec`, whose
//! sorts *can* spill. These tests record that matrix, so that the rows flip
//! deliberately rather than quietly:
//!
//! | row | role | today |
//! |-----|------|-------|
//! | 1 | default settings — the planner picks `HashJoinExec` | fails |
//! | 2 | the only workaround — `prefer_hash_join=false` | completes, sorts spill |
//! | 3a | where the ceiling is — build side filtered to a fitting size | completes |
//! | 3b | just past the ceiling — the same filter, slightly larger | fails |
//! | 4 | what the workaround costs when it isn't needed — row 3a forced through SMJ | completes |
//! | 5 | control — hash *aggregation* through the same budget | completes, spills |
//!
//! Rows 1, 3b are the ones external hash join is meant to turn into
//! `completes`; when that happens, these tests are the ones to update.
//!
//! Both inputs are the same relation, so there is no smaller side for the
//! planner to swap in: the failure is not one a better build side can avoid.
//! Row 4 divided by row 3a is the "SMJ tax" — the cost of the workaround on a
//! join that would have fit. Its *magnitude* (~3.5x at the recorded scale) is
//! measured by the `join_mem` benchmark; timings are not asserted here.

use std::sync::LazyLock;

use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use tempfile::TempDir;

use crate::memory_limit::budgeted_env::{BudgetedEnv, run_under_budget};

/// Budget every row runs under.
const BUDGET: usize = 16 * 1024 * 1024;

/// Rows in each join input. Sized so the build-side hash tables cannot fit in
/// [`BUDGET`]: `count(*)` projects the key alone, ~8 B/row of batches, and the
/// hash map on top of it costs ~19 B/row and is asked for in one allocation
/// once every build batch is already held.
const ROWS: usize = 2_000_000;

/// Build-side cap that comfortably fits the budget (rows 3a and 4).
const FIT_ROWS: usize = 200_000;

/// Build-side cap that comfortably exceeds it (row 3b).
const OVER_ROWS: usize = 1_000_000;

const HASH_JOIN: &str = "HashJoinExec";
const SORT_MERGE_JOIN: &str = "SortMergeJoinExec";
const SORT: &str = "SortExec";
const AGGREGATE: &str = "AggregateExec";

/// The join, over the whole build side.
const JOIN: &str = "SELECT count(*) FROM t_probe p JOIN t_build b ON p.k = b.k";

/// The join, with the build side filtered down to `limit` rows.
fn join_with_build_limit(limit: usize) -> String {
    format!(
        "SELECT count(*) FROM t_probe p \
         JOIN (SELECT * FROM t_build WHERE k <= {limit}) b ON p.k = b.k"
    )
}

/// The control: a hash aggregation over the same data, through the same budget.
const CONTROL: &str = "SELECT count(DISTINCT payload) FROM t_build";

/// The relation both join inputs read, generated once per test binary.
///
/// A parquet file rather than `generate_series` directly, because a series is a
/// sorted source with no statistics: the sort-merge rows would skip their sorts
/// entirely, and the planner could not tell which side of the filtered join is
/// smaller. Both are exactly what the matrix is about.
///
/// Generated with no memory limit, on a runtime of its own: writing the file is
/// not the thing under test, and a test's own runtime cannot block on this.
static DATA: LazyLock<TempDir> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("building the data generation runtime");
        runtime.block_on(async {
            let dir = TempDir::new().expect("creating the data directory");
            let ctx = SessionContext::new();
            ctx.sql(&format!(
                "COPY (SELECT v AS k, \
                        concat('payload-', v, '-', repeat('x', 24)) AS payload \
                 FROM generate_series(1, {ROWS}) AS t(v)) \
                 TO '{}' STORED AS PARQUET",
                data_path(&dir)
            ))
            .await
            .expect("generating the join inputs")
            .collect()
            .await
            .expect("generating the join inputs");
            dir
        })
    })
    .join()
    .expect("generating the join inputs")
});

fn data_path(dir: &TempDir) -> String {
    dir.path()
        .join("t.parquet")
        .to_str()
        .expect("temp dir path is not UTF-8")
        .to_string()
}

/// A context with the budget installed, both join inputs registered, and the
/// join algorithm pinned.
///
/// The sort spill reservation is lowered from its 10 MB default because the
/// sort-merge rows plan one sort per partition per side, and at this budget the
/// defaults would reserve most of the pool before any data is read.
async fn budgeted_ctx(prefer_hash_join: bool) -> SessionContext {
    let mut config = SessionConfig::new()
        .with_target_partitions(2)
        .with_sort_spill_reservation_bytes(1024 * 1024);
    config.options_mut().optimizer.prefer_hash_join = prefer_hash_join;

    let ctx = BudgetedEnv::new(BUDGET).with_config(config).build_ctx();

    let path = data_path(&DATA);
    for table in ["t_build", "t_probe"] {
        ctx.register_parquet(table, &path, ParquetReadOptions::default())
            .await
            .expect("registering an input table");
    }

    ctx
}

/// Row 1: at default settings the planner picks `HashJoinExec`, which has no
/// way to spill its build side, and the query fails.
#[tokio::test]
async fn row_1_hash_join_at_default_settings_exhausts_the_budget() {
    let ctx = budgeted_ctx(true).await;
    let outcome = run_under_budget(&ctx, JOIN).await;
    println!("row 1: {}", outcome.summary());

    outcome.assert_operator(HASH_JOIN).assert_exhausted_budget();
}

/// Row 2: the only workaround. `prefer_hash_join=false` plans a
/// `SortMergeJoinExec`, whose sorts spill, and the same query completes. The
/// join operator itself spills nothing — the sorts carry all of it.
#[tokio::test]
async fn row_2_sort_merge_join_workaround_completes() {
    let ctx = budgeted_ctx(false).await;
    let outcome = run_under_budget(&ctx, JOIN).await;
    println!("row 2: {}", outcome.summary());

    outcome
        .assert_operator(SORT_MERGE_JOIN)
        .assert_completed()
        .assert_spilled(SORT)
        .assert_did_not_spill(SORT_MERGE_JOIN);
}

/// Row 3a: where the ceiling is. The same join with a build side small enough
/// to fit runs on the hash join without trouble.
#[tokio::test]
async fn row_3a_hash_join_below_the_ceiling_completes() {
    let ctx = budgeted_ctx(true).await;
    let outcome = run_under_budget(&ctx, &join_with_build_limit(FIT_ROWS)).await;
    println!("row 3a: {}", outcome.summary());

    outcome.assert_operator(HASH_JOIN).assert_completed();
}

/// Row 3b: just past it. The same query with a larger build side fails, at the
/// hash table build.
#[tokio::test]
async fn row_3b_hash_join_above_the_ceiling_exhausts_the_budget() {
    let ctx = budgeted_ctx(true).await;
    let outcome = run_under_budget(&ctx, &join_with_build_limit(OVER_ROWS)).await;
    println!("row 3b: {}", outcome.summary());

    outcome.assert_operator(HASH_JOIN).assert_exhausted_budget();
}

/// Row 4: what the workaround costs when it isn't needed. Row 3a's join — one
/// that fits in memory — still completes when forced through the sort-merge
/// path, but pays for sorting both inputs to get there.
#[tokio::test]
async fn row_4_fitting_join_forced_through_sort_merge_completes() {
    let ctx = budgeted_ctx(false).await;
    let outcome = run_under_budget(&ctx, &join_with_build_limit(FIT_ROWS)).await;
    println!("row 4: {}", outcome.summary());

    outcome
        .assert_operator(SORT_MERGE_JOIN)
        .assert_completed()
        .assert_spilled(SORT);
}

/// Row 5: the control. It is not a way to run the join; it shows the budget
/// itself is workable, by pushing more data than the join ever holds through
/// the same pool in an operator that can spill.
#[tokio::test]
async fn row_5_control_hash_aggregation_completes_through_the_same_budget() {
    let ctx = budgeted_ctx(true).await;
    let outcome = run_under_budget(&ctx, CONTROL).await;
    println!("row 5: {}", outcome.summary());

    outcome.assert_completed().assert_spilled(AGGREGATE);
}
