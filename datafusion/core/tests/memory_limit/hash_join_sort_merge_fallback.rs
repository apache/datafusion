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

//! A partitioned hash join whose build side does not fit in memory finishes
//! as a sort-merge join (see `datafusion.execution.hash_join_max_build_size`)

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_common::assert_contains;
use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

use crate::helper::plan_metrics::plan_metric_sum;

fn table(rows: usize, value_column: &str, seed: u64) -> RecordBatch {
    table_with_keys(rows, value_column, seed, 500)
}

/// `rows` rows of a nullable `k` key drawn from `distinct_keys` values and a
/// nullable value column. A larger `distinct_keys` makes the join output
/// smaller without shrinking the build side.
fn table_with_keys(
    rows: usize,
    value_column: &str,
    seed: u64,
    distinct_keys: u64,
) -> RecordBatch {
    let mut state = seed;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, true),
        Field::new(value_column, DataType::Int32, true),
    ]));
    let keys: Vec<Option<i32>> = (0..rows)
        .map(|_| (next() % 50 != 0).then(|| (next() % distinct_keys) as i32))
        .collect();
    let values: Vec<Option<i32>> =
        (0..rows).map(|_| Some((next() % 60) as i32)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

/// Both tables are joined by partitioned hash joins whatever their size
fn config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(256)
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold",
            0,
        )
        .set_usize(
            "datafusion.optimizer.hash_join_single_partition_threshold_rows",
            0,
        )
        // Scaled down from the 10 MB default to match these small budgets. A
        // falling-back partition runs two sorts, each pre-reserving this much
        // for its merge, so the default would consume every budget small
        // enough to trigger the fallback in the first place.
        .with_sort_spill_reservation_bytes(64 * 1024)
}

fn context(config: SessionConfig, runtime: RuntimeEnvBuilder) -> SessionContext {
    let ctx = SessionContext::new_with_config_rt(config, runtime.build_arc().unwrap());
    ctx.register_batch("l", table(30_000, "v", 11)).unwrap();
    ctx.register_batch("r", table(20_000, "w", 16)).unwrap();
    ctx
}

fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = vec![];
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|col| {
                    datafusion_common::ScalarValue::try_from_array(batch.column(col), row)
                        .unwrap()
                        .to_string()
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// A context whose tables are big enough that each partition builds about 1 MB,
/// keyed widely so the join output stays small.
fn wide_context(
    memory_limit: Option<usize>,
    spilling: bool,
    max_build_size: Option<usize>,
) -> SessionContext {
    let mut cfg = config();
    if let Some(max_build_size) = max_build_size {
        cfg = cfg.set_usize(
            "datafusion.execution.hash_join_max_build_size",
            max_build_size,
        );
    }
    let mut runtime = RuntimeEnvBuilder::new();
    if let Some(limit) = memory_limit {
        runtime = runtime.with_memory_limit(limit, 1.0);
    }
    if !spilling {
        runtime = runtime.with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        );
    }
    let ctx = SessionContext::new_with_config_rt(cfg, runtime.build_arc().unwrap());
    ctx.register_batch("l", table_with_keys(240_000, "v", 11, 100_000))
        .unwrap();
    ctx.register_batch("r", table_with_keys(160_000, "w", 16, 100_000))
        .unwrap();
    ctx
}

/// Runs `sql` and returns its rows together with how many partitions of the
/// hash join finished as a sort-merge join.
async fn run_counting_fallbacks(ctx: &SessionContext, sql: &str) -> (Vec<String>, usize) {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();
    let fallbacks = plan_metric_sum(plan.as_ref(), "sort_merge_fallback_count");
    (rows(&batches), fallbacks)
}

/// Runs `sql` as an ordinary hash join and again with a build-size cap far
/// below the build side, returning both results and how many partitions fell
/// back in the second run.
///
/// Neither run is under a memory limit, so the cap alone decides the algorithm
/// and the comparison cannot be perturbed by whatever else is allocating.
async fn run_with_and_without_cap(sql: &str) -> (Vec<String>, Vec<String>, usize) {
    let plain = context(config(), RuntimeEnvBuilder::new());
    let (plain_rows, fallbacks) = run_counting_fallbacks(&plain, sql).await;
    assert_eq!(
        fallbacks, 0,
        "{sql}: without a cap nothing should fall back"
    );

    let capped = context(
        config().set_usize("datafusion.execution.hash_join_max_build_size", 16 * 1024),
        RuntimeEnvBuilder::new(),
    );
    let (capped_rows, fallbacks) = run_counting_fallbacks(&capped, sql).await;
    (plain_rows, capped_rows, fallbacks)
}

/// The whole decision in one table: which configuration runs a plain hash join,
/// which switches to a sort-merge join, and which still fails outright.
///
/// Every case runs the same query over the same data, so only the configuration
/// differs, and every successful case must return the same rows as an
/// unconstrained run.
#[tokio::test]
async fn config_matrix() {
    #[derive(Debug, PartialEq)]
    enum Expect {
        /// Stays a hash join, holding its whole build side in memory
        HashJoin,
        /// Both partitions finish as a sort-merge join
        Fallback,
        /// Nothing to fall back to, so the query fails as it did before
        Fails,
    }
    use Expect::*;

    // Aggregated so comparing answers is cheap, and keyed widely so the join
    // emits few rows while each partition still builds about 1 MB.
    let sql = "SELECT count(*), sum(l.v), sum(r.w) FROM l JOIN r ON l.k = r.k";
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;

    #[rustfmt::skip]
    let cases = [
        //  what the configuration is                       memory limit  spilling  cap        expected
        ("no memory limit and no cap",                       None,         true,     None,      HashJoin),
        ("no memory limit, cap under the build side",        None,         true,     Some(16 * KB), Fallback),
        ("memory limit under the build side, spilling on",   Some(512 * KB), true,   None,      Fallback),
        ("memory limit under the build side, spilling off",  Some(512 * KB), false,  None,      Fails),
        ("memory limit far above the build side",            Some(64 * MB), true,    None,      HashJoin),
    ];

    let reference = {
        let ctx = wide_context(None, true, None);
        let (rows, _) = run_counting_fallbacks(&ctx, sql).await;
        assert!(!rows.is_empty());
        rows
    };

    for (what, limit, spilling, cap, expect) in cases {
        let ctx = wide_context(limit, spilling, cap);
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let result =
            datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await;
        let fallbacks = plan_metric_sum(plan.as_ref(), "sort_merge_fallback_count");

        match expect {
            Fails => {
                let err = result.expect_err(&format!("{what}: expected failure"));
                assert_contains!(err.to_string(), "Resources exhausted");
            }
            HashJoin | Fallback => {
                let batches = result.unwrap_or_else(|e| panic!("{what}: {e}"));
                assert_eq!(rows(&batches), reference, "{what}: wrong answer");
                if expect == Fallback {
                    // Normally every partition switches. Only "at least one" is
                    // asserted because the `force_hash_collisions` test feature
                    // routes every row to a single partition, leaving the others
                    // with an empty build side and nothing to fall back for.
                    assert!(fallbacks >= 1, "{what}: expected the fallback");
                } else {
                    assert_eq!(fallbacks, 0, "{what}: expected a plain hash join");
                }
            }
        }
    }
}

#[tokio::test]
async fn semi_and_anti_joins_fall_back() {
    for sql in [
        "SELECT l.k, l.v FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.k = r.k)",
        "SELECT l.k, l.v FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE l.k = r.k)",
        "SELECT l.k, l.v FROM l WHERE l.k IN (SELECT r.k FROM r WHERE r.w > 30)",
    ] {
        let (plain, capped, fallbacks) = run_with_and_without_cap(sql).await;
        // See `config_matrix` on why this is not an exact count.
        assert!(fallbacks >= 1, "{sql}: expected the fallback");
        assert!(!plain.is_empty(), "{sql}");
        assert_eq!(capped, plain, "{sql}");
    }
}
