// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Column ids (`ColumnId`) survive a round trip through protobuf

use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ColumnId, Result};
use datafusion_expr::logical_plan::ColumnIdIssueKind;
use datafusion_expr::{DmlStatement, Expr, LogicalPlan, WriteOp};
use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
use std::sync::Arc;

/// The ids of each node of `plan` (subqueries included), in the order in
/// which the nodes are visited: the ids of its schema, of the source columns
/// of a scan, and of the columns its expressions read
fn column_ids(plan: &LogicalPlan) -> Vec<Vec<Option<ColumnId>>> {
    let mut ids = vec![];
    plan.apply_with_subqueries(|node| {
        let mut node_ids = node.schema().field_ids().to_vec();
        if let LogicalPlan::TableScan(scan) = node
            && let Some(source_ids) = &scan.source_column_ids
        {
            node_ids.extend(source_ids.iter().copied().map(Some));
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                match expr {
                    Expr::Column(col) | Expr::OuterReferenceColumn(_, col) => {
                        node_ids.push(col.id)
                    }
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        ids.push(node_ids);
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    ids
}

async fn context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_csv("t1", "tests/testdata/test.csv", CsvReadOptions::default())
        .await?;
    Ok(ctx)
}

async fn sorted_results(ctx: &SessionContext, plan: LogicalPlan) -> Result<Vec<String>> {
    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
    let mut lines: Vec<String> = pretty_format_batches(&batches)?
        .to_string()
        .lines()
        .map(String::from)
        .collect();
    lines.sort();
    Ok(lines)
}

/// Encodes and decodes the plan of `sql`, before and after optimization, and
/// checks that the decoded plan has the same column ids, the same issues
/// (none), and the same results
async fn assert_column_ids_round_trip(sql: &str) -> Result<()> {
    let ctx = context().await?;
    let df = ctx.sql(sql).await?;
    let plans = vec![df.logical_plan().clone(), df.into_optimized_plan()?];
    assert_plans_round_trip(sql, plans, true).await
}

/// [`assert_column_ids_round_trip`], after optimization only
async fn assert_optimized_column_ids_round_trip(sql: &str) -> Result<()> {
    let ctx = context().await?;
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;
    assert_plans_round_trip(sql, vec![plan], true).await
}

/// Checks that `plans` round trip with their column ids, and, if
/// `same_debug`, that they decode equal to themselves
async fn assert_plans_round_trip(
    sql: &str,
    plans: Vec<LogicalPlan>,
    same_debug: bool,
) -> Result<()> {
    for plan in plans {
        let ids = column_ids(&plan);
        assert!(
            ids.iter().flatten().any(Option::is_some),
            "no column ids in {plan}"
        );
        let bytes = logical_plan_to_bytes(&plan)?;
        let ctx = context().await?;
        let decoded = logical_plan_from_bytes(&bytes, &ctx.task_ctx())?;
        if same_debug {
            assert_eq!(format!("{plan:?}"), format!("{decoded:?}"));
        }
        assert_eq!(ids, column_ids(&decoded), "{sql}\n{plan}");
        assert_eq!(plan.column_id_issues(), decoded.column_id_issues());
        assert!(
            decoded
                .column_id_issues()
                .iter()
                .all(|issue| issue.kind != ColumnIdIssueKind::DuplicateDefinition)
        );
        assert_eq!(
            sorted_results(&ctx, plan).await?,
            sorted_results(&ctx, decoded).await?
        );
    }
    Ok(())
}

#[tokio::test]
async fn roundtrip_column_ids_self_join() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT l.a, r.a, r.b FROM t1 AS l JOIN t1 AS r ON l.a = r.b WHERE r.a > 0",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_repeated_subquery() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT a, (SELECT max(a) FROM t1) AS m1, (SELECT max(a) FROM t1) AS m2 FROM t1",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_aggregate_and_window() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT a, sum(b) AS s, row_number() OVER (ORDER BY a) AS r FROM t1 GROUP BY a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_grouping_sets() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT a, b, count(*) AS c FROM t1 GROUP BY ROLLUP (a, b)",
    )
    .await?;
    assert_column_ids_round_trip(
        "SELECT a, b + 1, count(*) AS c FROM t1 GROUP BY GROUPING SETS ((b + 1, a), (a), ())",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_union() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT a FROM t1 UNION ALL SELECT b FROM t1 UNION ALL SELECT a + b FROM t1",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_distinct_on() -> Result<()> {
    assert_column_ids_round_trip("SELECT DISTINCT ON (a) a, b FROM t1 ORDER BY a, b DESC")
        .await
}

#[tokio::test]
async fn roundtrip_column_ids_values() -> Result<()> {
    assert_column_ids_round_trip(
        "SELECT v.x, t1.a FROM (VALUES (1, 2), (3, 4)) AS v(x, y) JOIN t1 ON v.x = t1.a",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_mark_join() -> Result<()> {
    // EXISTS cannot be encoded: only the decorrelated plan
    assert_optimized_column_ids_round_trip(
        "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t1 AS t2 WHERE t2.a = t1.b) OR a > 1",
    )
    .await
}

#[tokio::test]
async fn roundtrip_column_ids_unnest() -> Result<()> {
    assert_column_ids_round_trip("SELECT a, unnest(make_array(a, b)) AS u FROM t1").await
}

#[tokio::test]
async fn roundtrip_column_ids_recursive_query() -> Result<()> {
    let sql = "WITH RECURSIVE cte AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM cte WHERE n < 5
        )
        SELECT * FROM cte";
    let ctx = context().await?;
    let df = ctx.sql(sql).await?;
    let plans = vec![df.logical_plan().clone(), df.into_optimized_plan()?];
    // the projection of the work table scan is not encoded
    assert_plans_round_trip(sql, plans, false).await
}

/// The ids of the target columns of a MERGE INTO, which the operation
/// defines (the target is not an input of the `Dml` node)
#[tokio::test]
async fn roundtrip_column_ids_merge_into() -> Result<()> {
    fn target_ids(plan: &LogicalPlan) -> Option<Arc<[ColumnId]>> {
        match plan {
            LogicalPlan::Dml(DmlStatement {
                op: WriteOp::MergeInto(op),
                ..
            }) => op.target_column_ids().cloned(),
            other => panic!("expected MERGE INTO, got {other}"),
        }
    }

    let ctx = context().await?;
    ctx.register_csv("t2", "tests/testdata/test.csv", CsvReadOptions::default())
        .await?;
    let plan = ctx
        .state()
        .create_logical_plan(
            "MERGE INTO t1 AS t USING t2 AS s ON t.a = s.a \
             WHEN MATCHED AND t.b > 0 THEN UPDATE SET b = s.b + t.b \
             WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.b)",
        )
        .await?;
    let ids = target_ids(&plan).expect("MERGE INTO target column ids");
    assert_eq!(ids.len(), 2);
    assert_eq!(plan.column_id_issues(), vec![]);

    let bytes = logical_plan_to_bytes(&plan)?;
    let decoded = logical_plan_from_bytes(&bytes, &ctx.task_ctx())?;
    assert_eq!(format!("{plan}"), format!("{decoded}"));
    assert_eq!(target_ids(&decoded), Some(ids));
    assert_eq!(column_ids(&plan), column_ids(&decoded));
    assert_eq!(decoded.column_id_issues(), vec![]);
    Ok(())
}

/// A plan encoded before column ids existed has none: its columns get new
/// ids when it is decoded, and the sides of a self-join are renumbered
#[cfg(feature = "json")]
#[tokio::test]
async fn decode_plan_without_column_ids() -> Result<()> {
    use datafusion_proto::bytes::{logical_plan_from_json, logical_plan_to_json};
    use serde_json::Value;

    /// Removes the column ids from an encoded plan
    fn strip_column_ids(value: &mut Value) {
        const ID_KEYS: &[&str] = &[
            "exprIds",
            "windowExprIds",
            "selectExprIds",
            "groupExprIds",
            "aggrExprIds",
            "outputIds",
            "markColumnId",
            "columnIds",
            "sourceColumnIds",
        ];
        match value {
            Value::Object(map) => {
                map.retain(|key, value| {
                    let column_id = key == "id"
                        && value
                            .as_str()
                            .is_some_and(|id| id.chars().all(|c| c.is_ascii_digit()));
                    !column_id && !ID_KEYS.contains(&key.as_str())
                });
                map.values_mut().for_each(strip_column_ids);
            }
            Value::Array(values) => values.iter_mut().for_each(strip_column_ids),
            _ => {}
        }
    }

    for sql in [
        "SELECT l.a, r.a, r.b FROM t1 AS l JOIN t1 AS r ON l.a = r.b WHERE r.a > 0",
        "SELECT a, sum(b) AS s FROM t1 GROUP BY a UNION ALL SELECT b, a FROM t1",
        "WITH RECURSIVE cte AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 FROM cte WHERE n < 5
        )
        SELECT * FROM cte",
    ] {
        let ctx = context().await?;
        let plan = ctx.sql(sql).await?.into_optimized_plan()?;
        let mut json: Value = serde_json::from_str(&logical_plan_to_json(&plan)?)
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        strip_column_ids(&mut json);
        let json = json.to_string();
        assert!(!json.contains("Ids"), "{json}");

        let ctx = context().await?;
        let decoded = logical_plan_from_json(&json, &ctx.task_ctx())?;
        if !sql.contains("RECURSIVE") {
            // the projection of the work table scan is not encoded
            assert_eq!(format!("{plan}"), format!("{decoded}"));
        }
        // the decoded plan defines its columns once, but its references
        // have no ids
        let issues = decoded.column_id_issues();
        assert!(
            issues
                .iter()
                .all(|issue| issue.kind == ColumnIdIssueKind::Missing),
            "{sql}: {issues:?}"
        );
        assert_ne!(column_ids(&plan), column_ids(&decoded));
        assert_eq!(
            sorted_results(&ctx, plan).await?,
            sorted_results(&ctx, decoded).await?
        );
    }
    Ok(())
}
