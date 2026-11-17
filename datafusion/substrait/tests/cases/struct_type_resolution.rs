// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::test_util::batches_to_sort_string;
use datafusion::common::{Result, assert_batches_sorted_eq};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, Volatility, create_udf};
use datafusion::prelude::{DataFrame, SessionContext, col, lit};
use datafusion::scalar::ScalarValue;
use datafusion_substrait::logical_plan::{
    consumer::from_substrait_plan, producer::to_substrait_plan,
};
use std::sync::Arc;
use substrait::proto::expression::nested::{List, NestedType};
use substrait::proto::expression::{Nested, RexType};
use substrait::proto::function_argument::ArgType;
use substrait::proto::plan_rel;
use substrait::proto::rel::RelType;

#[tokio::test]
async fn roundtrip_case_names_literal_from_later_branch() -> Result<()> {
    roundtrip(
        "SELECT CASE WHEN flag THEN named_struct('a', 42) ELSE s END AS r FROM struct_rows",
    ).await
}

#[tokio::test]
async fn roundtrip_case_names_null_from_constructed_struct() -> Result<()> {
    roundtrip(
        "SELECT CASE WHEN column1 IS NOT NULL THEN named_struct('f1', column1) ELSE NULL END AS r
         FROM (VALUES ('a'), (NULL))",
    ).await
}

#[tokio::test]
async fn roundtrip_outer_case_resolves_nested_literal_results() -> Result<()> {
    roundtrip(
        "SELECT CASE WHEN flag THEN s
         ELSE CASE WHEN flag IS NOT NULL THEN named_struct('a', 42) ELSE NULL END
         END AS r FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_union_resolves_cast_to_anonymous_struct() -> Result<()> {
    roundtrip(
        "SELECT NULL AS r FROM struct_rows UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_union_resolves_literal_below_limit_and_sort() -> Result<()> {
    roundtrip(
        "(SELECT named_struct('a', 42) AS r FROM struct_rows ORDER BY flag LIMIT 1)
         UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_equal_struct_shapes_keep_independent_names() -> Result<()> {
    roundtrip(
        "SELECT CASE WHEN flag THEN s ELSE named_struct('a', 42) END AS l,
                CASE WHEN flag THEN named_struct('b', 42) ELSE t END AS r
         FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_independent_function_arguments_keep_their_names() -> Result<()> {
    roundtrip(
        "SELECT named_struct('left', s, 'right', named_struct('c0', 42)) AS r FROM struct_rows",
    ).await
}

#[tokio::test]
async fn roundtrip_array_has_matches_literal_to_element_type() -> Result<()> {
    roundtrip("SELECT array_has(items, named_struct('a', 7)) AS r FROM struct_rows").await
}

#[tokio::test]
async fn roundtrip_in_list_matches_literal_to_struct_column() -> Result<()> {
    roundtrip(
        "SELECT s IN (named_struct('a', 7), named_struct('a', 42)) AS r FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_window_default_matches_struct_value() -> Result<()> {
    roundtrip(
        "SELECT lag(s, 1, named_struct('a', 42)) OVER (ORDER BY flag) AS r FROM struct_rows",
    ).await
}

#[tokio::test]
async fn roundtrip_case_resolves_names_inside_struct_list_elements() -> Result<()> {
    roundtrip(
        "SELECT CASE WHEN flag THEN nested
         ELSE named_struct('items', [named_struct('code', 42)]) END AS r FROM struct_rows",
    ).await
}

#[tokio::test]
async fn roundtrip_outer_union_resolves_names_below_inner_distinct() -> Result<()> {
    roundtrip(
        "(SELECT named_struct('a', 42) AS r FROM struct_rows
          UNION SELECT NULL AS r FROM struct_rows)
         UNION ALL SELECT s AS r FROM struct_rows",
    )
    .await
}

#[tokio::test]
async fn roundtrip_overridden_coalesce_keeps_independent_arguments() -> Result<()> {
    roundtrip_with_ctx(
        "SELECT coalesce(s, named_struct('c0', 42)) AS r FROM struct_rows",
        independent_arguments_context("coalesce").await?,
    )
    .await
}

#[tokio::test]
async fn roundtrip_overridden_equal_keeps_independent_arguments() -> Result<()> {
    roundtrip_with_ctx(
        "SELECT equal(s, named_struct('c0', 42)) AS r FROM struct_rows",
        independent_arguments_context("equal").await?,
    )
    .await
}

#[tokio::test]
async fn consume_coalesce_with_typed_empty_list() -> Result<()> {
    let ctx = context().await?;
    let table = ctx.table("struct_rows").await?;
    let DataType::List(element) = table
        .schema()
        .field_with_unqualified_name("items")?
        .data_type()
    else {
        unreachable!()
    };
    let empty =
        ScalarValue::List(ScalarValue::new_list_nullable(&[], element.data_type()));
    let query =
        table.select(vec![coalesce(vec![col("items"), lit(empty)]).alias("r")])?;
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
async fn consume_union_through_volatile_projection() -> Result<()> {
    let ctx = context().await?;
    let plan = ctx
        .sql(
            "SELECT s AS r, random() AS marker FROM struct_rows
         UNION ALL SELECT NULL AS r, random() AS marker FROM struct_rows",
        )
        .await?
        .into_optimized_plan()?;
    let rows = consume_plan(&ctx, &plan).await?;
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
async fn consume_substrait_nested_list_with_struct_literal() -> Result<()> {
    let ctx = context().await?;
    let plan = ctx
        .sql("SELECT make_array(s, named_struct('a', 42)) AS r FROM struct_rows")
        .await?
        .into_optimized_plan()?;
    let mut proto = to_substrait_plan(&plan, &ctx.state())?;
    let Some(plan_rel::RelType::Root(root)) = &mut proto.relations[0].rel_type else {
        panic!("expected root");
    };
    let Some(RelType::Project(project)) =
        &mut root.input.as_mut().expect("root input").rel_type
    else {
        panic!("expected project");
    };
    let Some(RexType::ScalarFunction(array)) = project.expressions[0].rex_type.take()
    else {
        panic!("expected make_array");
    };
    let values = array
        .arguments
        .into_iter()
        .map(|argument| match argument.arg_type {
            Some(ArgType::Value(value)) => value,
            _ => panic!("expected array element"),
        })
        .collect();
    // An external producer can encode array construction directly as Nested::List.
    project.expressions[0].rex_type = Some(RexType::Nested(Nested {
        nullable: false,
        type_variation_reference: 0,
        nested_type: Some(NestedType::List(List { values })),
    }));
    let consumed = from_substrait_plan(&ctx.state(), &proto).await?;
    assert_eq!(plan.schema(), consumed.schema());
    let rows = DataFrame::new(ctx.state(), consumed).collect().await?;
    assert_batches_sorted_eq!(
        [
            "+-------------------+",
            "| r                 |",
            "+-------------------+",
            "| [{a: 7}, {a: 42}] |",
            "| [{a: 7}, {a: 42}] |",
            "+-------------------+"
        ],
        &rows
    );
    Ok(())
}

async fn context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.sql(
        "CREATE TABLE struct_rows AS
         SELECT flag, named_struct('a', 7) AS s, named_struct('b', 9) AS t,
                CASE WHEN flag THEN [named_struct('a', 7)] ELSE NULL END AS items,
                named_struct('items', [named_struct('code', 7)]) AS nested
         FROM (VALUES (true), (false)) AS flags(flag)",
    )
    .await?
    .collect()
    .await?;
    Ok(ctx)
}

async fn independent_arguments_context(name: &str) -> Result<SessionContext> {
    let ctx = context().await?;
    let named = DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into());
    let anonymous =
        DataType::Struct(vec![Field::new("c0", DataType::Int64, true)].into());
    ctx.register_udf(create_udf(
        name,
        vec![named, anonymous],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(123))))),
    ));
    Ok(ctx)
}

async fn roundtrip(sql: &str) -> Result<()> {
    roundtrip_with_ctx(sql, context().await?).await
}

async fn roundtrip_with_ctx(sql: &str, ctx: SessionContext) -> Result<()> {
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;
    let actual = consume_plan(&ctx, &plan).await?;
    let expected = DataFrame::new(ctx.state(), plan).collect().await?;
    assert_eq!(
        batches_to_sort_string(&expected),
        batches_to_sort_string(&actual)
    );
    Ok(())
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
