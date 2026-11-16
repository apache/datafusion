// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext, col};
use datafusion_common::{Result, assert_batches_eq};
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, collect, displayable,
};

async fn query(
    native: bool,
    target_partitions: usize,
    input_partitions: usize,
    sql: &str,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<RecordBatch>)> {
    query_with_options(
        native,
        target_partitions,
        input_partitions,
        sql,
        false,
        true,
    )
    .await
}

async fn query_with_options(
    native: bool,
    target_partitions: usize,
    input_partitions: usize,
    sql: &str,
    sorted: bool,
    repartition: bool,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<RecordBatch>)> {
    let mut config = SessionConfig::new().with_target_partitions(target_partitions);
    config.options_mut().optimizer.single_distinct_prefer_native = native;
    config.options_mut().optimizer.repartition_aggregations = repartition;
    config
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = repartition;
    let ctx = SessionContext::new_with_config(config);
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int32, true),
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                None,
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(7),
                Some(7),
                Some(9),
                None,
                Some(4),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(2),
                None,
                Some(3),
                Some(3),
            ])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                None,
                Some("c"),
                None,
            ])),
        ],
    )?;
    let mut table = MemTable::try_new(schema, vec![vec![batch]; input_partitions])?;
    if sorted {
        table = table.with_sort_order(vec![vec![col("g").sort(true, false)]]);
    }
    ctx.register_table("t", Arc::new(table))?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    Ok((plan, batches))
}

fn native_grouped_count(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>()
        && matches!(agg.mode(), AggregateMode::Single)
        && !agg.group_expr().is_empty()
        && agg.group_expr().is_single()
        && agg.aggr_expr().len() == 1
        && agg.aggr_expr()[0].is_distinct()
    {
        assert_eq!(agg.input().output_partitioning().partition_count(), 1);
        return true;
    }
    plan.children().into_iter().any(native_grouped_count)
}

#[tokio::test]
async fn native_distinct_uses_actual_input_partitions() -> Result<()> {
    let sql = "SELECT g AS group_alias, count(DISTINCT x) AS n FROM t GROUP BY g ORDER BY group_alias";
    for target in [1, 4] {
        for input in [1, 2] {
            for enabled in [false, true] {
                let (plan, batches) = query(enabled, target, input, sql).await?;
                assert_eq!(
                    native_grouped_count(&plan),
                    enabled && input == 1 && target == 1,
                    "{}",
                    displayable(plan.as_ref()).indent(true)
                );
                assert_batches_eq!(
                    [
                        "+-------------+---+",
                        "| group_alias | n |",
                        "+-------------+---+",
                        "| 1           | 2 |",
                        "| 2           | 0 |",
                        "|             | 1 |",
                        "+-------------+---+",
                    ],
                    &batches
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_distinct_uses_distribution_instead_of_target() -> Result<()> {
    let sql = "SELECT g, count(DISTINCT x) AS n FROM t GROUP BY g ORDER BY g";
    let (native, native_batches) =
        query_with_options(true, 4, 1, sql, false, false).await?;
    let (rewrite, rewrite_batches) =
        query_with_options(false, 4, 1, sql, false, false).await?;
    assert!(
        native_grouped_count(&native),
        "{}",
        displayable(native.as_ref()).indent(true)
    );
    assert!(!native_grouped_count(&rewrite));
    assert_eq!(native.schema(), rewrite.schema());
    assert_eq!(
        pretty_format_batches(&native_batches)?.to_string(),
        pretty_format_batches(&rewrite_batches)?.to_string()
    );
    Ok(())
}

#[tokio::test]
async fn native_distinct_preserves_expression_groups_and_arguments() -> Result<()> {
    for sql in [
        "SELECT g + 1 AS group_alias, count(DISTINCT x + 1) AS n FROM t GROUP BY g + 1 ORDER BY group_alias",
        "SELECT y, g, count(DISTINCT x) AS n FROM t GROUP BY y, g ORDER BY y, g",
    ] {
        let (native, native_batches) = query(true, 1, 1, sql).await?;
        let (rewrite, rewrite_batches) = query(false, 1, 1, sql).await?;
        assert!(
            native_grouped_count(&native),
            "{}",
            displayable(native.as_ref()).indent(true)
        );
        assert!(!native_grouped_count(&rewrite));
        assert_eq!(native.schema(), rewrite.schema());
        assert_eq!(
            pretty_format_batches(&native_batches)?.to_string(),
            pretty_format_batches(&rewrite_batches)?.to_string()
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_distinct_preserves_unsupported_shapes() -> Result<()> {
    for sql in [
        "SELECT g, count(DISTINCT s) AS n FROM t GROUP BY g ORDER BY g",
        "SELECT g, count(DISTINCT CAST(x AS DOUBLE)) AS n FROM t GROUP BY g ORDER BY g",
        "SELECT g, count(DISTINCT x) AS n, sum(x) AS total FROM t GROUP BY g ORDER BY g",
        "SELECT count(DISTINCT x) AS n FROM t",
        "SELECT g, count(x) FILTER (WHERE x > 7) AS n FROM (SELECT DISTINCT g, x FROM t) pairs GROUP BY g ORDER BY g",
        "SELECT g, count(x) AS n FROM (SELECT DISTINCT g, x FROM t ORDER BY g, x LIMIT 2) pairs GROUP BY g ORDER BY g",
    ] {
        let (native, native_batches) = query(true, 1, 1, sql).await?;
        let (rewrite, rewrite_batches) = query(false, 1, 1, sql).await?;
        assert!(
            !native_grouped_count(&native),
            "{}",
            displayable(native.as_ref()).indent(true)
        );
        assert_eq!(native.schema(), rewrite.schema());
        assert_eq!(
            pretty_format_batches(&native_batches)?.to_string(),
            pretty_format_batches(&rewrite_batches)?.to_string()
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_distinct_retains_ordered_aggregation() -> Result<()> {
    let sql = "SELECT g, count(DISTINCT x) AS n FROM t GROUP BY g ORDER BY g";
    let (native, native_batches) =
        query_with_options(true, 1, 1, sql, true, true).await?;
    let (rewrite, rewrite_batches) =
        query_with_options(false, 1, 1, sql, true, true).await?;
    assert!(
        !native_grouped_count(&native),
        "{}",
        displayable(native.as_ref()).indent(true)
    );
    assert_eq!(native.schema(), rewrite.schema());
    assert_eq!(
        pretty_format_batches(&native_batches)?.to_string(),
        pretty_format_batches(&rewrite_batches)?.to_string()
    );
    Ok(())
}

#[tokio::test]
async fn native_distinct_preserves_filtered_and_multiple_distinct_results() -> Result<()>
{
    for sql in [
        "SELECT g, count(DISTINCT x) FILTER (WHERE y = 2) AS n FROM t GROUP BY g ORDER BY g",
        "SELECT g, count(DISTINCT x) AS nx, count(DISTINCT y) AS ny FROM t GROUP BY g ORDER BY g",
        "SELECT g, count(DISTINCT x) AS n FROM t GROUP BY ROLLUP(g) ORDER BY g, n",
    ] {
        let (_, native_batches) = query(true, 1, 1, sql).await?;
        let (_, rewrite_batches) = query(false, 1, 1, sql).await?;
        assert_eq!(
            pretty_format_batches(&native_batches)?.to_string(),
            pretty_format_batches(&rewrite_batches)?.to_string()
        );
    }
    Ok(())
}
