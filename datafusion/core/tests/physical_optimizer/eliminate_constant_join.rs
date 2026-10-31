// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Integration tests for the [`EliminateConstantJoin`] physical optimizer rule.
//! Cases that SQL can express are in `eliminate_constant_join.slt`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{JoinSide, JoinType, Result};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, lit};
use datafusion_physical_optimizer::PhysicalAnalyzerRule;
use datafusion_physical_optimizer::eliminate_constant_join::EliminateConstantJoin;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

/// A placeholder emits one row per partition, so a two-partition constant input
/// matches every row twice.
#[tokio::test]
async fn keeps_join_with_multi_row_constant() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![10, 30]))],
    )?;
    let events = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let constant = Arc::new(ProjectionExec::try_new(
        [(lit(25i64), "cutoff".to_string())],
        Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())).with_partitions(2)),
    )?);
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("t", 0)),
            Operator::Lt,
            Arc::new(Column::new("cutoff", 1)),
        )),
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, true),
            Field::new("cutoff", DataType::Int64, false),
        ])),
    );
    let join: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
        events,
        constant,
        Some(filter),
        &JoinType::Inner,
        None,
    )?);

    let optimized =
        EliminateConstantJoin::new().analyze(Arc::clone(&join), &ConfigOptions::new())?;
    assert_eq!(rows(optimized).await?, rows(join).await?);
    Ok(())
}

/// Lambda variables are numbered after the join filter's own columns, so a filter
/// over the wider input would read the wrong columns.
#[tokio::test]
async fn keeps_join_with_lambda_filter() -> Result<()> {
    // Plan without optimizer rules: projection pushdown cannot yet plan lambdas in
    // nested-loop join filters.
    let config =
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "databricks");
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build(),
    );
    ctx.sql(
        "CREATE TABLE lists AS \
         SELECT * FROM (VALUES ([1, 30], 'a', 7), ([40], 'b', 8)) AS v(l, k, n)",
    )
    .await?
    .collect()
    .await?;
    let join = ctx
        .sql(
            "SELECT s.k, s.n FROM (SELECT 25 AS cutoff) c \
             JOIN lists s ON array_any_match(s.l, x -> x < c.cutoff)",
        )
        .await?
        .create_physical_plan()
        .await?;

    let optimized =
        EliminateConstantJoin::new().analyze(Arc::clone(&join), &ConfigOptions::new())?;
    assert_eq!(rows(optimized).await?, rows(join).await?);
    Ok(())
}

/// The rewritten plan keeps the schema metadata that came from the constant input;
/// otherwise the optimizer's schema check fails the query.
#[tokio::test]
async fn keeps_metadata_of_constant_input() -> Result<()> {
    // With `*`, no projection above the join keeps the metadata.
    let sql = "SELECT * FROM (SELECT count(*) AS n FROM tagged) c \
               JOIN events e ON e.t >= c.n";
    let expected = rows(
        metadata_session(false)?
            .sql(sql)
            .await?
            .create_physical_plan()
            .await?,
    )
    .await?;
    let ctx = metadata_session(true)?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    assert_eq!(rows(plan).await?, expected);
    Ok(())
}

/// Registers `events` and `tagged`, a table whose schema carries metadata, with the
/// default rules, without [`EliminateConstantJoin`] unless `eliminate` is set.
fn metadata_session(eliminate: bool) -> Result<SessionContext> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    if !eliminate {
        let rules = state
            .physical_optimizers()
            .iter()
            .filter(|rule| rule.name() != EliminateConstantJoin::new().name())
            .cloned()
            .collect();
        state = SessionStateBuilder::new_from_existing(state)
            .with_physical_optimizer_rules(rules)
            .build();
    }
    let ctx = SessionContext::new_with_state(state);
    for (name, metadata) in [
        ("events", HashMap::new()),
        (
            "tagged",
            HashMap::from([("source".to_string(), "tagged".to_string())]),
        ),
    ] {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("t", DataType::Int64, true)],
            metadata,
        ));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
        )?;
    }
    Ok(ctx)
}

/// Runs `plan` and formats its rows in sorted order.
async fn rows(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<String>> {
    let schema = plan.schema();
    let batches = collect(plan, Arc::new(Default::default())).await?;
    let mut rows = pretty_format_batches(&[concat_batches(&schema, &batches)?])?
        .to_string()
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
    rows.sort_unstable();
    Ok(rows)
}
