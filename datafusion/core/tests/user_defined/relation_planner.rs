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

//! Tests for the RelationPlanner extension point

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::memory::MemTable;
use datafusion::common::test_util::batches_to_string;
use datafusion::prelude::*;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_sql::sqlparser::ast::TableFactor;
use insta::assert_snapshot;

// ============================================================================
// Test Planners - Example Implementations
// ============================================================================

// The planners in this section are deliberately minimal, static examples used
// only for tests. In real applications a `RelationPlanner` would typically
// construct richer logical plans tailored to external systems or custom
// semantics rather than hard-coded in-memory tables.
//
// For more realistic examples, see `datafusion-examples/examples/relation_planner/`:
// - `table_sample.rs`: Full TABLESAMPLE implementation (parsing → execution)
// - `pivot_unpivot.rs`: PIVOT/UNPIVOT via SQL rewriting
// - `match_recognize.rs`: MATCH_RECOGNIZE logical planning

/// Helper to build simple static values-backed virtual tables used by the
/// example planners below.
fn plan_static_values_table(
    relation: TableFactor,
    table_name: &str,
    column_name: &str,
    values: Vec<ScalarValue>,
) -> Result<RelationPlanning> {
    match relation {
        TableFactor::Table { name, alias, .. }
            if name.to_string().eq_ignore_ascii_case(table_name) =>
        {
            let rows = values
                .into_iter()
                .map(|v| vec![Expr::Literal(v, None)])
                .collect::<Vec<_>>();

            let plan = LogicalPlanBuilder::values(rows)?
                .project(vec![col("column1").alias(column_name)])?
                .build()?;

            Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                plan, alias,
            ))))
        }
        other => Ok(RelationPlanning::Original(Box::new(other))),
    }
}

/// Example planner that provides a virtual `numbers` table with values
/// 1, 2, 3.
#[derive(Debug)]
struct NumbersPlanner;

impl RelationPlanner for NumbersPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            "numbers",
            "number",
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(3)),
            ],
        )
    }
}

/// Example planner that provides a virtual `colors` table with three string
/// values: `red`, `green`, `blue`.
#[derive(Debug)]
struct ColorsPlanner;

impl RelationPlanner for ColorsPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            "colors",
            "color",
            vec![
                ScalarValue::Utf8(Some("red".into())),
                ScalarValue::Utf8(Some("green".into())),
                ScalarValue::Utf8(Some("blue".into())),
            ],
        )
    }
}

/// Alternative implementation of `numbers` (returns 100, 200) used to
/// demonstrate planner precedence (last registered planner wins).
#[derive(Debug)]
struct AlternativeNumbersPlanner;

impl RelationPlanner for AlternativeNumbersPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            "numbers",
            "number",
            vec![ScalarValue::Int64(Some(100)), ScalarValue::Int64(Some(200))],
        )
    }
}

/// Example planner that intercepts nested joins and samples both sides (limit 2)
/// before joining, demonstrating recursive planning with `context.plan()`.
#[derive(Debug)]
struct SamplingJoinPlanner;

impl RelationPlanner for SamplingJoinPlanner {
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
                // Use context.plan() to recursively plan both sides
                // This ensures other planners (like NumbersPlanner) can handle them
                let left = context.plan(table_with_joins.relation.clone())?;
                let right = context.plan(table_with_joins.joins[0].relation.clone())?;

                // Sample each table to 2 rows
                let left_sampled =
                    LogicalPlanBuilder::from(left).limit(0, Some(2))?.build()?;

                let right_sampled =
                    LogicalPlanBuilder::from(right).limit(0, Some(2))?.build()?;

                // Cross join: 2 rows × 2 rows = 4 rows (instead of 3×3=9 without sampling)
                let plan = LogicalPlanBuilder::from(left_sampled)
                    .cross_join(right_sampled)?
                    .build()?;

                Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                    plan, alias,
                ))))
            }
            other => Ok(RelationPlanning::Original(Box::new(other))),
        }
    }
}

/// Example planner that never handles any relation and always delegates by
/// returning `RelationPlanning::Original`.
#[derive(Debug)]
struct PassThroughPlanner;

impl RelationPlanner for PassThroughPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // Never handles anything - always delegates
        Ok(RelationPlanning::Original(Box::new(relation)))
    }
}

/// Example planner that shows how planners can block specific constructs and
/// surface custom error messages by rejecting `UNNEST` relations (here framed
/// as a mock premium feature check).
#[derive(Debug)]
struct PremiumFeaturePlanner;

impl RelationPlanner for PremiumFeaturePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::UNNEST { .. } => Err(datafusion_common::DataFusionError::Plan(
                "UNNEST is a premium feature! Please upgrade to DataFusion Pro™ \
                     to unlock advanced array operations."
                    .to_string(),
            )),
            other => Ok(RelationPlanning::Original(Box::new(other))),
        }
    }
}

// ============================================================================
// Test Helpers - SQL Execution
// ============================================================================

/// Execute SQL and return results with better error messages.
async fn execute_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let df = ctx.sql(sql).await?;
    df.collect().await
}

/// Execute SQL and convert to string format for snapshot comparison.
async fn execute_sql_to_string(ctx: &SessionContext, sql: &str) -> String {
    let batches = execute_sql(ctx, sql)
        .await
        .expect("SQL execution should succeed");
    batches_to_string(&batches)
}

// ============================================================================
// Test Helpers - Context Builders
// ============================================================================

/// Create a SessionContext with a catalog table containing Int64 and Utf8 columns.
///
/// Creates a table with the specified name and sample data for fallback/integration tests.
fn create_context_with_catalog_table(
    table_name: &str,
    id_values: Vec<i64>,
    name_values: Vec<&str>,
) -> SessionContext {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(id_values)),
            Arc::new(StringArray::from(name_values)),
        ],
    )
    .unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();

    ctx
}

/// Create a SessionContext with a simple single-column Int64 table.
///
/// Useful for basic tests that need a real catalog table.
fn create_context_with_simple_table(
    table_name: &str,
    values: Vec<i64>,
) -> SessionContext {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));

    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
            .unwrap();

    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table(table_name, Arc::new(table)).unwrap();

    ctx
}

// ============================================================================
// TESTS: Ordered from Basic to Complex
// ============================================================================

/// Comprehensive test suite for RelationPlanner extension point.
/// Tests are ordered from simplest smoke test to most complex scenarios.
#[cfg(test)]
mod tests {
    use super::*;

    /// Small extension trait to make test setup read fluently.
    trait TestSessionExt {
        fn with_planner<P: RelationPlanner + 'static>(self, planner: P) -> Self;
    }

    impl TestSessionExt for SessionContext {
        fn with_planner<P: RelationPlanner + 'static>(self, planner: P) -> Self {
            self.register_relation_planner(Arc::new(planner)).unwrap();
            self
        }
    }

    /// Session context with only the `NumbersPlanner` registered.
    fn ctx_with_numbers() -> SessionContext {
        SessionContext::new().with_planner(NumbersPlanner)
    }

    /// Session context with virtual tables (`numbers`, `colors`) and the
    /// `SamplingJoinPlanner` registered for nested joins.
    fn ctx_with_virtual_tables_and_sampling() -> SessionContext {
        SessionContext::new()
            .with_planner(NumbersPlanner)
            .with_planner(ColorsPlanner)
            .with_planner(SamplingJoinPlanner)
    }

    // Basic smoke test: virtual table can be queried like a regular table.
    #[tokio::test]
    async fn virtual_table_basic_select() {
        let ctx = ctx_with_numbers();

        let result = execute_sql_to_string(&ctx, "SELECT * FROM numbers").await;

        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 1      |
        | 2      |
        | 3      |
        +--------+
        ");
    }

    // Virtual table supports standard SQL operations (projection, filter, aggregation).
    #[tokio::test]
    async fn virtual_table_filters_and_aggregation() {
        let ctx = ctx_with_numbers();

        let filtered = execute_sql_to_string(
            &ctx,
            "SELECT number * 10 AS scaled FROM numbers WHERE number > 1",
        )
        .await;

        assert_snapshot!(filtered, @r"
        +--------+
        | scaled |
        +--------+
        | 20     |
        | 30     |
        +--------+
        ");

        let aggregated = execute_sql_to_string(
            &ctx,
            "SELECT COUNT(*) as count, SUM(number) as total, AVG(number) as average \
             FROM numbers",
        )
        .await;

        assert_snapshot!(aggregated, @r"
        +-------+-------+---------+
        | count | total | average |
        +-------+-------+---------+
        | 3     | 6     | 2.0     |
        +-------+-------+---------+
        ");
    }

    // Multiple planners can coexist and each handles its own virtual table.
    #[tokio::test]
    async fn multiple_planners_virtual_tables() {
        let ctx = SessionContext::new()
            .with_planner(NumbersPlanner)
            .with_planner(ColorsPlanner);

        let result1 = execute_sql_to_string(&ctx, "SELECT * FROM numbers").await;
        assert_snapshot!(result1, @r"
        +--------+
        | number |
        +--------+
        | 1      |
        | 2      |
        | 3      |
        +--------+
        ");

        let result2 = execute_sql_to_string(&ctx, "SELECT * FROM colors").await;
        assert_snapshot!(result2, @r"
        +-------+
        | color |
        +-------+
        | red   |
        | green |
        | blue  |
        +-------+
        ");
    }

    // Last registered planner for the same table name takes precedence (LIFO).
    #[tokio::test]
    async fn lifo_precedence_last_planner_wins() {
        let ctx = SessionContext::new()
            .with_planner(AlternativeNumbersPlanner)
            .with_planner(NumbersPlanner);

        let result = execute_sql_to_string(&ctx, "SELECT * FROM numbers").await;

        // CustomValuesPlanner registered last, should win (returns 1,2,3 not 100,200)
        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 1      |
        | 2      |
        | 3      |
        +--------+
        ");
    }

    // Pass-through planner delegates to the catalog without changing behavior.
    #[tokio::test]
    async fn delegation_pass_through_to_catalog() {
        let ctx = create_context_with_simple_table("real_table", vec![42])
            .with_planner(PassThroughPlanner);

        let result = execute_sql_to_string(&ctx, "SELECT * FROM real_table").await;

        assert_snapshot!(result, @r"
        +-------+
        | value |
        +-------+
        | 42    |
        +-------+
        ");
    }

    // Catalog is used when no planner claims the relation.
    #[tokio::test]
    async fn catalog_fallback_when_no_planner() {
        let ctx =
            create_context_with_catalog_table("users", vec![1, 2], vec!["Alice", "Bob"])
                .with_planner(NumbersPlanner);

        let result = execute_sql_to_string(&ctx, "SELECT * FROM users ORDER BY id").await;

        assert_snapshot!(result, @r"
        +----+-------+
        | id | name  |
        +----+-------+
        | 1  | Alice |
        | 2  | Bob   |
        +----+-------+
        ");
    }

    // Planners can block specific constructs and surface custom error messages.
    #[tokio::test]
    async fn error_handling_premium_feature_blocking() {
        // Verify UNNEST works without planner
        let ctx_without_planner = SessionContext::new();
        let result =
            execute_sql(&ctx_without_planner, "SELECT * FROM UNNEST(ARRAY[1, 2, 3])")
                .await
                .expect("UNNEST should work by default");
        assert_eq!(result.len(), 1);

        // Same query with blocking planner registered
        let ctx = SessionContext::new().with_planner(PremiumFeaturePlanner);

        // Verify UNNEST is now rejected
        let error = execute_sql(&ctx, "SELECT * FROM UNNEST(ARRAY[1, 2, 3])")
            .await
            .expect_err("UNNEST should be rejected");

        let error_msg = error.to_string();
        assert!(
            error_msg.contains("premium feature") && error_msg.contains("DataFusion Pro"),
            "Expected custom rejection message, got: {error_msg}"
        );
    }

    // SamplingJoinPlanner recursively calls `context.plan()` on both sides of a
    // nested join before sampling, exercising recursive relation planning.
    #[tokio::test]
    async fn recursive_planning_sampling_join() {
        let ctx = ctx_with_virtual_tables_and_sampling();

        let result =
            execute_sql_to_string(&ctx, "SELECT * FROM (numbers JOIN colors ON true)")
                .await;

        // SamplingJoinPlanner limits each side to 2 rows: 2×2=4 (not 3×3=9)
        assert_snapshot!(result, @r"
        +--------+-------+
        | number | color |
        +--------+-------+
        | 1      | red   |
        | 1      | green |
        | 2      | red   |
        | 2      | green |
        +--------+-------+
        ");
    }
}
