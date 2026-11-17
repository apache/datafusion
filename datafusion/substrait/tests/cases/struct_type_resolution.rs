// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, assert_batches_sorted_eq};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, Volatility, create_udf};
use datafusion::prelude::{DataFrame, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use datafusion_substrait::logical_plan::{
    consumer::from_substrait_plan, producer::to_substrait_plan,
};
use std::sync::Arc;

async fn context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.sql(
        "CREATE TABLE struct_rows AS
         SELECT flag, named_struct('a', 7) AS s, named_struct('b', 9) AS t
         FROM (VALUES (true), (false)) AS flags(flag)",
    )
    .await?
    .collect()
    .await?;
    Ok(ctx)
}

async fn consume_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let original = ctx.sql(sql).await?.into_optimized_plan()?;
    consume_plan(ctx, &original).await
}

async fn consume_plan(
    ctx: &SessionContext,
    original: &LogicalPlan,
) -> Result<Vec<RecordBatch>> {
    let proto = to_substrait_plan(original, &ctx.state())?;
    let consumed = from_substrait_plan(&ctx.state(), &proto).await?;
    assert_eq!(original.schema(), consumed.schema());
    DataFrame::new(ctx.state(), consumed).collect().await
}

#[tokio::test]
async fn case_names_literal_from_later_branch() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT CASE WHEN flag THEN named_struct('a', 42) ELSE s END AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| r       |",
            "+---------+",
            "| {a: 42} |",
            "| {a: 7}  |",
            "+---------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn outer_case_resolves_nested_literal_results() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT CASE WHEN flag THEN s
         ELSE CASE WHEN flag IS NOT NULL THEN named_struct('a', 42) ELSE NULL END
         END AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| r       |",
            "+---------+",
            "| {a: 42} |",
            "| {a: 7}  |",
            "+---------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn union_resolves_cast_to_anonymous_struct() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT NULL AS r FROM struct_rows UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+--------+",
            "| r      |",
            "+--------+",
            "|        |",
            "|        |",
            "| {a: 7} |",
            "| {a: 7} |",
            "+--------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn union_resolves_literal_below_limit_and_sort() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "(SELECT named_struct('a', 42) AS r FROM struct_rows ORDER BY flag LIMIT 1)
         UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| r       |",
            "+---------+",
            "| {a: 42} |",
            "| {a: 7}  |",
            "| {a: 7}  |",
            "+---------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn equal_struct_shapes_keep_independent_names() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT CASE WHEN flag THEN s ELSE named_struct('a', 42) END AS l,
                CASE WHEN flag THEN named_struct('b', 42) ELSE t END AS r
         FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------+---------+",
            "| l       | r       |",
            "+---------+---------+",
            "| {a: 7}  | {b: 42} |",
            "| {a: 42} | {b: 9}  |",
            "+---------+---------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn independent_function_arguments_keep_their_names() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT named_struct('left', s, 'right', named_struct('c0', 42)) AS r
         FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------------------------------+",
            "| r                               |",
            "+---------------------------------+",
            "| {left: {a: 7}, right: {c0: 42}} |",
            "| {left: {a: 7}, right: {c0: 42}} |",
            "+---------------------------------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn coalesce_resolves_empty_list_before_building_function() -> Result<()> {
    let ctx = context().await?;
    ctx.sql("CREATE TABLE lists AS SELECT [named_struct('a', 7)] AS s UNION ALL SELECT NULL AS s")
        .await?.collect().await?;
    let table = ctx.table("lists").await?;
    let DataType::List(element) = table.schema().field(0).data_type() else {
        unreachable!()
    };
    let empty =
        ScalarValue::List(ScalarValue::new_list_nullable(&[], element.data_type()));
    let query = table.select(vec![coalesce(vec![col("s"), lit(empty)]).alias("r")])?;
    // Keep the function call: optimization rewrites coalesce to CASE.
    let rows = consume_plan(&ctx, query.logical_plan()).await?;
    assert_batches_sorted_eq!(
        [
            "+----------+",
            "| r        |",
            "+----------+",
            "| []       |",
            "| [{a: 7}] |",
            "+----------+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn array_has_matches_literal_to_element_type() -> Result<()> {
    let ctx = context().await?;
    ctx.sql("CREATE TABLE lists AS SELECT [named_struct('a', 7)] AS s")
        .await?
        .collect()
        .await?;
    let rows = consume_sql(
        &ctx,
        "SELECT array_has(s, named_struct('a', 7)) AS r FROM lists",
    )
    .await?;
    assert_batches_sorted_eq!(
        ["+------+", "| r    |", "+------+", "| true |", "+------+"],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn union_traces_columns_through_volatile_projection() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "SELECT s AS r, random() AS marker FROM struct_rows
         UNION ALL SELECT NULL AS r, random() AS marker FROM struct_rows",
    )
    .await?;
    let structs = rows
        .iter()
        .map(|batch| batch.project(&[0]))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_batches_sorted_eq!(
        [
            "+--------+",
            "| r      |",
            "+--------+",
            "|        |",
            "|        |",
            "| {a: 7} |",
            "| {a: 7} |",
            "+--------+"
        ],
        &structs
    );
    Ok(())
}

#[tokio::test]
async fn overridden_coalesce_does_not_imply_common_arguments() -> Result<()> {
    let ctx = context().await?;
    let named = DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into());
    let anonymous =
        DataType::Struct(vec![Field::new("c0", DataType::Int64, true)].into());
    ctx.register_udf(create_udf(
        "coalesce",
        vec![named, anonymous],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(123))))),
    ));
    let rows = consume_sql(
        &ctx,
        "SELECT coalesce(s, named_struct('c0', 42)) AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+-----+", "| r   |", "+-----+", "| 123 |", "| 123 |", "+-----+"
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn case_resolves_names_inside_struct_list_elements() -> Result<()> {
    let ctx = context().await?;
    ctx.sql(
        "CREATE TABLE nested AS
         SELECT flag, named_struct('items', [named_struct('code', 7)]) AS s
         FROM struct_rows",
    )
    .await?
    .collect()
    .await?;
    let rows = consume_sql(
        &ctx,
        "SELECT CASE WHEN flag THEN s
         ELSE named_struct('items', [named_struct('code', 42)]) END AS r FROM nested",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+-----------------------+",
            "| r                     |",
            "+-----------------------+",
            "| {items: [{code: 42}]} |",
            "| {items: [{code: 7}]}  |",
            "+-----------------------+",
        ],
        &rows
    );
    Ok(())
}

#[tokio::test]
async fn outer_union_resolves_names_below_inner_distinct() -> Result<()> {
    let ctx = context().await?;
    let rows = consume_sql(
        &ctx,
        "(SELECT named_struct('a', 42) AS r FROM struct_rows
          UNION SELECT NULL AS r FROM struct_rows)
         UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await?;
    assert_batches_sorted_eq!(
        [
            "+---------+",
            "| r       |",
            "+---------+",
            "|         |",
            "| {a: 42} |",
            "| {a: 7}  |",
            "| {a: 7}  |",
            "+---------+",
        ],
        &rows
    );
    Ok(())
}
