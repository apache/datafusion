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

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::memory::MemTable;
use datafusion::common::test_util::batches_to_string;
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::TableFactor;

/// A planner that creates an in-memory table with custom values
#[derive(Debug)]
struct CustomValuesPlanner;

impl RelationPlanner for CustomValuesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table { name, alias, .. }
                if name.to_string().eq_ignore_ascii_case("custom_values") =>
            {
                let plan = LogicalPlanBuilder::values(vec![
                    vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)],
                    vec![Expr::Literal(ScalarValue::Int64(Some(2)), None)],
                    vec![Expr::Literal(ScalarValue::Int64(Some(3)), None)],
                ])?
                .build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// A planner that handles string-based tables
#[derive(Debug)]
struct StringTablePlanner;

impl RelationPlanner for StringTablePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table { name, alias, .. }
                if name.to_string().eq_ignore_ascii_case("colors") =>
            {
                let plan = LogicalPlanBuilder::values(vec![
                    vec![Expr::Literal(ScalarValue::Utf8(Some("red".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("green".into())), None)],
                    vec![Expr::Literal(ScalarValue::Utf8(Some("blue".into())), None)],
                ])?
                .build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// A planner that intercepts nested joins and plans them recursively
#[derive(Debug)]
struct RecursiveJoinPlanner;

impl RelationPlanner for RecursiveJoinPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
                ..
            } if table_with_joins.joins.len() == 1 => {
                // Recursively plan both sides using context.plan()
                let left = context.plan(table_with_joins.relation.clone())?;
                let right = context.plan(table_with_joins.joins[0].relation.clone())?;

                // Create a cross join
                let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// A planner that always returns None to test delegation
#[derive(Debug)]
struct PassThroughPlanner;

impl RelationPlanner for PassThroughPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // Always return Original - delegates to next planner or default
        Ok(RelationPlanning::Original(relation))
    }
}

async fn collect_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

#[tokio::test]
async fn test_custom_planner_handles_relation() {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();

    let results = collect_sql(&ctx, "SELECT * FROM custom_values").await;

    let expected = "\
+---------+
| column1 |
+---------+
| 1       |
| 2       |
| 3       |
+---------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_multiple_planners_first_wins() {
    let ctx = SessionContext::new();

    // Register multiple planners - first one wins
    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(StringTablePlanner))
        .unwrap();

    // CustomValuesPlanner handles this
    let results = collect_sql(&ctx, "SELECT * FROM custom_values").await;
    let expected = "\
+---------+
| column1 |
+---------+
| 1       |
| 2       |
| 3       |
+---------+";
    assert_eq!(batches_to_string(&results), expected);

    // StringTablePlanner handles this
    let results = collect_sql(&ctx, "SELECT * FROM colors").await;
    let expected = "\
+---------+
| column1 |
+---------+
| red     |
| green   |
| blue    |
+---------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_planner_delegates_to_default() {
    let ctx = SessionContext::new();

    // Register a planner that always returns None
    ctx.register_relation_planner(Arc::new(PassThroughPlanner))
        .unwrap();

    // Also register a real table
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![42]))])
            .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("real_table", Arc::new(table)).unwrap();

    // PassThroughPlanner returns None, so it delegates to the default planner
    let results = collect_sql(&ctx, "SELECT * FROM real_table").await;
    let expected = "\
+-------+
| value |
+-------+
| 42    |
+-------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_planner_delegates_with_multiple_planners() {
    let ctx = SessionContext::new();

    // Register planners in order
    ctx.register_relation_planner(Arc::new(PassThroughPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(StringTablePlanner))
        .unwrap();

    // PassThroughPlanner returns None, CustomValuesPlanner handles it
    let results = collect_sql(&ctx, "SELECT * FROM custom_values").await;
    let expected = "\
+---------+
| column1 |
+---------+
| 1       |
| 2       |
| 3       |
+---------+";
    assert_eq!(batches_to_string(&results), expected);

    // PassThroughPlanner and CustomValuesPlanner both return None,
    // StringTablePlanner handles it
    let results = collect_sql(&ctx, "SELECT * FROM colors").await;
    let expected = "\
+---------+
| column1 |
+---------+
| red     |
| green   |
| blue    |
+---------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_recursive_planning_with_context_plan() {
    let ctx = SessionContext::new();

    // Register planners
    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(StringTablePlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(RecursiveJoinPlanner))
        .unwrap();

    // RecursiveJoinPlanner calls context.plan() on both sides,
    // which recursively invokes the planner pipeline
    let results = collect_sql(
        &ctx,
        "SELECT * FROM custom_values AS nums JOIN colors AS c ON true",
    )
    .await;

    // Should produce a cross join: 3 numbers × 3 colors = 9 rows
    let expected = "\
+---------+---------+
| column1 | column1 |
+---------+---------+
| 1       | red     |
| 1       | green   |
| 1       | blue    |
| 2       | red     |
| 2       | green   |
| 2       | blue    |
| 3       | red     |
| 3       | green   |
| 3       | blue    |
+---------+---------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_planner_with_filters_and_projections() {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();

    // Test that filters and projections work on custom-planned tables
    let results = collect_sql(
        &ctx,
        "SELECT column1 * 10 AS scaled FROM custom_values WHERE column1 > 1",
    )
    .await;

    let expected = "\
+--------+
| scaled |
+--------+
| 20     |
| 30     |
+--------+";
    assert_eq!(batches_to_string(&results), expected);
}

#[tokio::test]
async fn test_planner_falls_back_to_default_for_unknown_table() {
    let ctx = SessionContext::new();

    ctx.register_relation_planner(Arc::new(CustomValuesPlanner))
        .unwrap();

    // Register a regular table
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("users", Arc::new(table)).unwrap();

    // CustomValuesPlanner doesn't handle "users", falls back to default
    let results = collect_sql(&ctx, "SELECT * FROM users ORDER BY id").await;

    let expected = "\
+----+-------+
| id | name  |
+----+-------+
| 1  | Alice |
| 2  | Bob   |
+----+-------+";
    assert_eq!(batches_to_string(&results), expected);
}
