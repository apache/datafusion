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

//! Tests for `datafusion.optimizer.enable_adaptive_hash_join`

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, assert_contains};
use datafusion_physical_plan::{collect, displayable};

/// Writes parquet tables, so that hash joins push dynamic filters into their
/// probe side: `dim` (1000 keys), `fact` (100K rows over 2000 keys) and
/// `tiny` (1 row).
async fn write_tables(dir: &Path) -> Result<()> {
    let ctx = SessionContext::new();
    let tables = [
        ("dim", "SELECT value AS k FROM generate_series(1, 1000)"),
        (
            "fact",
            "SELECT value % 2000 AS k, value AS v FROM generate_series(1, 100000)",
        ),
        ("tiny", "SELECT 7 AS k"),
    ];
    for (name, query) in tables {
        let path = dir.join(format!("{name}.parquet"));
        let path = path.to_str().unwrap();
        ctx.sql(&format!("COPY ({query}) TO '{path}' STORED AS PARQUET"))
            .await?
            .collect()
            .await?;
    }
    Ok(())
}

/// A context over the tables in `dir` that never picks `CollectLeft` while
/// planning.
async fn context(
    dir: &Path,
    configure: impl FnOnce(&mut ConfigOptions),
) -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_target_partitions(8);
    config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold = 0;
    configure(config.options_mut());
    let ctx = SessionContext::new_with_config(config);
    for name in ["dim", "fact", "tiny"] {
        let path = dir.join(format!("{name}.parquet"));
        ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
            .await?;
    }
    Ok(ctx)
}

/// Runs `sql` and returns its results and the plan after execution.
async fn run(ctx: &SessionContext, sql: &str) -> Result<(String, String)> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = tokio::time::timeout(
        Duration::from_secs(30),
        collect(Arc::clone(&plan), ctx.task_ctx()),
    )
    .await
    .expect("query did not finish")?;
    let results = pretty_format_batches(&batches)?.to_string();
    Ok((
        results,
        displayable(plan.as_ref()).indent(false).to_string(),
    ))
}

/// Runs `sql` with and without `enable_adaptive_hash_join`, further
/// configured by `configure`, checks that the results are the same, and
/// returns the adaptive plan after execution.
async fn run_adaptive(
    sql: &str,
    configure: impl FnOnce(&mut ConfigOptions),
) -> Result<String> {
    let dir = tempfile::tempdir()?;
    write_tables(dir.path()).await?;
    let (expected, _) = run(&context(dir.path(), |_| {}).await?, sql).await?;
    let ctx = context(dir.path(), |options| {
        options.optimizer.enable_adaptive_hash_join = true;
        configure(options);
    })
    .await?;
    let (results, plan) = run(&ctx, sql).await?;
    assert_eq!(results, expected);
    Ok(plan)
}

const JOIN: &str = "SELECT count(*), sum(f.v) FROM dim d JOIN fact f ON d.k = f.k";

/// The outer join has a one-row build side, so all but one of its partitions
/// finish without polling the join below.
const NESTED_JOIN: &str = "SELECT count(*) FROM tiny t \
    JOIN (SELECT f.k FROM dim d JOIN fact f ON d.k = f.k) j ON t.k = j.k";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_build_side_is_collected() -> Result<()> {
    let plan = run_adaptive(JOIN, |_| {}).await?;
    assert_contains!(
        plan,
        "AdaptiveHashJoinExec: mode=CollectLeft, build_rows=1000,"
    );
    Ok(())
}

/// The rest of the build side, after the batches collected up to the
/// threshold, still reaches the join.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_build_side_stays_partitioned() -> Result<()> {
    let plan = run_adaptive(JOIN, |options| {
        options.optimizer.adaptive_hash_join_threshold_rows = 100;
        options
            .set("datafusion.execution.batch_size", "64")
            .unwrap();
    })
    .await?;
    assert_contains!(plan, "AdaptiveHashJoinExec: mode=Partitioned,");
    Ok(())
}

/// Joins that track matched build-side rows only collect build sides within
/// `hash_join_single_partition_threshold_rows`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn semi_join_uses_single_partition_threshold() -> Result<()> {
    let sql = "SELECT count(*) FROM dim WHERE k IN (SELECT k FROM fact)";
    let plan = run_adaptive(sql, |options| {
        options.optimizer.hash_join_single_partition_threshold_rows = 500;
    })
    .await?;
    assert_contains!(
        plan,
        "AdaptiveHashJoinExec: mode=Partitioned, join_type=LeftSemi"
    );
    Ok(())
}

/// The outer join relies on the join below being hash partitioned, so when
/// that join collects its build side it repartitions its output instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collected_join_keeps_required_partitioning() -> Result<()> {
    let plan = run_adaptive(NESTED_JOIN, |_| {}).await?;
    assert_contains!(
        plan,
        "AdaptiveHashJoinExec: mode=CollectLeft, build_rows=1000, output_repartitioned=true,"
    );
    Ok(())
}

/// Partitions of the adaptive join that the outer join never polls must still
/// report to its partitioned join, whose dynamic filter waits for all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpolled_partitions_do_not_hang() -> Result<()> {
    let plan = run_adaptive(NESTED_JOIN, |options| {
        options.optimizer.adaptive_hash_join_threshold_rows = 0;
    })
    .await?;
    assert_contains!(plan, "AdaptiveHashJoinExec: mode=Partitioned,");
    Ok(())
}
