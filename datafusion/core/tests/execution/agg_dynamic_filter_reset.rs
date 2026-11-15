// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Copies of a plan made with [`reset_plan_states`] must not share the
//! `AggregateExec` MIN/MAX dynamic filter. See
//! <https://github.com/apache/datafusion/issues/27452>.

use std::sync::Arc;

use datafusion::physical_plan::execution_plan::reset_plan_states;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::*;
use datafusion_common::Result;
use insta::assert_snapshot;

/// A parquet file with `x` = 1..=1000 in 10 row groups, so the dynamic filter
/// can prune whole row groups once a bound is known.
async fn setup() -> Result<(SessionContext, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("t.parquet");
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    ctx.sql(&format!(
        "COPY (SELECT CAST(value AS INT) AS x FROM generate_series(1, 1000)) \
         TO '{}' STORED AS PARQUET \
         OPTIONS ('format.max_row_group_size' '100')",
        path.display()
    ))
    .await?
    .collect()
    .await?;
    ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    Ok((ctx, dir))
}

async fn run(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
    let batches = collect(plan, ctx.task_ctx()).await?;
    Ok(arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
}

/// Two copies prepared before either runs, as when a cached plan is reused:
/// the copy that runs second must not read the bound the first one left.
#[tokio::test]
async fn reset_copies_have_independent_aggregate_dynamic_filters() -> Result<()> {
    let (ctx, _dir) = setup().await?;
    let plan = ctx
        .sql("SELECT max(x) FROM t WHERE x % 2 = 0")
        .await?
        .create_physical_plan()
        .await?;
    // the scan must have been given the aggregate dynamic filter
    let shown = displayable(plan.as_ref()).indent(true).to_string();
    assert!(shown.contains("DynamicFilter"), "{shown}");

    let first = reset_plan_states(Arc::clone(&plan))?;
    let second = reset_plan_states(Arc::clone(&plan))?;
    let expected = "+----------+\n| max(t.x) |\n+----------+\n| 1000     |\n+----------+";
    assert_eq!(run(&ctx, first).await?, expected);
    assert_eq!(run(&ctx, second).await?, expected);

    // and when the two copies run at the same time
    for _ in 0..10 {
        let first = reset_plan_states(Arc::clone(&plan))?;
        let second = reset_plan_states(Arc::clone(&plan))?;
        let (first, second) = tokio::join!(run(&ctx, first), run(&ctx, second));
        assert_eq!(first?, expected);
        assert_eq!(second?, expected);
    }
    Ok(())
}

/// Running a plan, resetting it and running it again still gives the same
/// result (<https://github.com/apache/datafusion/issues/26054>).
#[tokio::test]
async fn rerun_after_reset_keeps_aggregate_result() -> Result<()> {
    let (ctx, _dir) = setup().await?;
    let plan = ctx
        .sql("SELECT min(x), max(x) FROM t WHERE x % 2 = 0")
        .await?
        .create_physical_plan()
        .await?;

    let first = run(&ctx, Arc::clone(&plan)).await?;
    let again = run(&ctx, reset_plan_states(Arc::clone(&plan))?).await?;
    assert_snapshot!(first, @r"
    +----------+----------+
    | min(t.x) | max(t.x) |
    +----------+----------+
    | 2        | 1000     |
    +----------+----------+
    ");
    assert_eq!(first, again);
    Ok(())
}
