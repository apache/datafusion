// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for the RelationPlanner extension point

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::catalog::memory::MemTable;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::test_util::batches_to_string;
use datafusion::prelude::*;
use datafusion_common::{
    DFSchema, Result, ScalarValue, TableReference, TableSelector, not_impl_err,
};
use datafusion_expr::Expr;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    TableSelectorContext, TableSelectorPlanning,
};
use datafusion_sql::sqlparser::ast::{TableAlias, TableFactor, TableVersion};
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
    context: &dyn RelationPlannerContext,
    table_name: &str,
    column_name: &str,
    values: Vec<ScalarValue>,
) -> Result<RelationPlanning> {
    let alias = match &relation {
        TableFactor::Table { name, alias, .. }
            if name.to_string().eq_ignore_ascii_case(table_name) =>
        {
            alias.clone()
        }
        _ => return Ok(RelationPlanning::Original(Box::new(relation))),
    };

    if let TableFactor::Table {
        name, args: None, ..
    } = &relation
    {
        let table_ref = context.object_name_to_table_reference(name.clone())?;

        // RelationPlanner extensions run before DataFusion resolves CTEs. A
        // name-based planner should defer when a CTE with that name is visible,
        // so SQL's normal lexical CTE precedence is preserved.
        if context.get_cte(&table_ref).is_some() {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
    }

    plan_static_values(alias, column_name, values)
}

fn plan_static_values(
    alias: Option<TableAlias>,
    column_name: &str,
    values: Vec<ScalarValue>,
) -> Result<RelationPlanning> {
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

/// Example planner that provides a virtual `numbers` table with values
/// 1, 2, 3.
#[derive(Debug)]
struct NumbersPlanner;

impl RelationPlanner for NumbersPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            context,
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

/// Example planner for a qualified virtual relation. This is kept separate
/// from `NumbersPlanner` so CTE tests can verify that extension lookup matches
/// DataFusion's existing qualified-name resolution.
#[derive(Debug)]
struct QualifiedNumbersPlanner;

impl RelationPlanner for QualifiedNumbersPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            context,
            "foo.bar",
            "number",
            vec![ScalarValue::Int64(Some(7))],
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
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            context,
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
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        plan_static_values_table(
            relation,
            context,
            "numbers",
            "number",
            vec![ScalarValue::Int64(Some(100)), ScalarValue::Int64(Some(200))],
        )
    }
}

/// Example planner with a deliberately reserved `numbers` name. Unlike the
/// ordinary name-based planners above, it intentionally does not defer to CTEs.
#[derive(Debug)]
struct ReservedNumbersPlanner;

impl RelationPlanner for ReservedNumbersPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Table { name, alias, .. }
                if name.to_string().eq_ignore_ascii_case("numbers") =>
            {
                plan_static_values(alias, "number", vec![ScalarValue::Int64(Some(999))])
            }
            other => Ok(RelationPlanning::Original(Box::new(other))),
        }
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

/// A schema with a single table `t`, which counts how often the current and
/// a specific version of `t` are looked up.
#[derive(Debug)]
struct CountingSchema {
    table: Arc<dyn TableProvider>,
    /// Whether `table_with_selector` is implemented, or returns "not implemented" like
    /// catalogs that predate it
    supports_time_travel: bool,
    table_calls: AtomicUsize,
    table_with_selector_calls: AtomicUsize,
}

#[async_trait]
impl SchemaProvider for CountingSchema {
    fn table_names(&self) -> Vec<String> {
        vec!["t".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.table_calls.fetch_add(1, Ordering::SeqCst);
        Ok((name == "t").then(|| Arc::clone(&self.table)))
    }

    async fn table_with_selector(
        &self,
        name: &str,
        selector: &TableSelector,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.table_with_selector_calls
            .fetch_add(1, Ordering::SeqCst);
        if !self.supports_time_travel {
            return not_impl_err!("Time travel is not supported");
        }
        if name != "t" {
            return Ok(None);
        }
        let table = MemTable::try_new(self.table.schema(), vec![vec![]])?
            .with_selector(Some(selector.clone()));
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "t"
    }
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

/// Example planner that implements time travel itself, for tables whose
/// catalog does not support [`SchemaProvider::table_with_selector`]. It emulates version
/// `n` of a table by only returning the rows with an `id` of at most `n`.
///
/// [`SchemaProvider::table_with_selector`]: datafusion::catalog::SchemaProvider::table_with_selector
#[derive(Debug)]
struct EmulatedVersionPlanner;

impl RelationPlanner for EmulatedVersionPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let TableFactor::Table {
            name,
            alias,
            args: None,
            version: Some(TableVersion::VersionAsOf(version)),
            ..
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let table_ref = context.object_name_to_table_reference(name)?;
        let version = context.sql_to_expr(version, &DFSchema::empty())?;
        let source = context
            .context_provider()
            .get_table_source(table_ref.clone())?;
        let plan = LogicalPlanBuilder::scan(table_ref, source, None)?
            .filter(col("id").lt_eq(version))?
            .build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
}

/// Example planner that reads a specific version of a table in the `counting`
/// schema, given by a table hint such as `t WITH (1 + 1)`, rather than a time
/// travel clause.
#[derive(Debug)]
struct VersionHintPlanner;

impl RelationPlanner for VersionHintPlanner {
    fn plan_table_selector(
        &self,
        table: &TableReference,
        relation: &TableFactor,
        context: &dyn TableSelectorContext,
    ) -> Result<TableSelectorPlanning> {
        if table.schema() != Some("counting") {
            return Ok(TableSelectorPlanning::Original);
        }
        let TableFactor::Table { with_hints, .. } = relation else {
            return Ok(TableSelectorPlanning::Original);
        };
        let [hint] = with_hints.as_slice() else {
            return Ok(TableSelectorPlanning::Original);
        };
        let version = context.evaluate_const_expr(hint.clone())?;
        Ok(TableSelectorPlanning::Planned(Some(
            TableSelector::Version(version),
        )))
    }
}

/// Example planner that ignores time travel clauses, reading the table as
/// returned by the catalog instead
#[derive(Debug)]
struct IgnoreTimeTravelPlanner;

impl RelationPlanner for IgnoreTimeTravelPlanner {
    fn plan_table_selector(
        &self,
        _table: &TableReference,
        _relation: &TableFactor,
        _context: &dyn TableSelectorContext,
    ) -> Result<TableSelectorPlanning> {
        Ok(TableSelectorPlanning::Planned(None))
    }
}

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

    // A name-based relation planner can defer to a visible CTE, preserving the
    // same shadowing behavior as a catalog table with that name.
    #[tokio::test]
    async fn cte_takes_precedence_over_virtual_table() {
        let ctx = ctx_with_numbers();

        let result = execute_sql_to_string(
            &ctx,
            "WITH numbers AS (SELECT 42 AS number) SELECT * FROM numbers",
        )
        .await;

        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 42     |
        +--------+
        ");
    }

    // A derived query inherits CTEs from its outer query, so a name-based
    // relation planner still defers to the visible CTE in the nested scope.
    #[tokio::test]
    async fn derived_query_inherits_outer_cte() {
        let ctx = ctx_with_numbers();

        let result = execute_sql_to_string(
            &ctx,
            "WITH numbers AS (SELECT 42 AS number) \
             SELECT * FROM (SELECT * FROM numbers) nested",
        )
        .await;

        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 42     |
        +--------+
        ");
    }

    // Deferring a visible CTE continues through the planner chain. A later
    // planner with a deliberately reserved name can still intercept it.
    #[tokio::test]
    async fn cte_defer_still_allows_reserved_planner() {
        let ctx = SessionContext::new()
            .with_planner(ReservedNumbersPlanner)
            .with_planner(NumbersPlanner);

        let result = execute_sql_to_string(
            &ctx,
            "WITH numbers AS (SELECT 42 AS number) SELECT * FROM numbers",
        )
        .await;

        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 999    |
        +--------+
        ");
    }

    // The extension lookup follows DataFusion's existing name matching. Both
    // the extension and the default planner therefore resolve the same CTE for
    // a quoted dotted name and a qualified reference with the same rendering.
    #[tokio::test]
    async fn cte_lookup_matches_default_qualified_name_resolution() {
        let ctx = SessionContext::new().with_planner(QualifiedNumbersPlanner);

        let result = execute_sql_to_string(
            &ctx,
            "WITH \"foo.bar\" AS (SELECT 42 AS number) SELECT * FROM foo.bar",
        )
        .await;

        assert_snapshot!(result, @r"
        +--------+
        | number |
        +--------+
        | 42     |
        +--------+
        ");
    }

    // A CTE shadows an ordinary table reference, not a same-named table
    // function call with arguments.
    #[tokio::test]
    async fn cte_does_not_shadow_same_named_table_function() {
        let ctx = ctx_with_numbers();

        let result = execute_sql_to_string(
            &ctx,
            "WITH numbers AS (SELECT 42 AS number) SELECT * FROM numbers(1)",
        )
        .await;

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

    // CTE visibility follows the query's lexical scope: the nested CTE wins
    // inside the derived table but does not hide the outer virtual relation.
    #[tokio::test]
    async fn cte_visibility_is_scoped_to_nested_query() {
        let ctx = ctx_with_numbers();

        let result = execute_sql_to_string(
            &ctx,
            "SELECT 'cte' AS source, number \
             FROM (WITH numbers AS (SELECT 42 AS number) SELECT * FROM numbers) nested \
             UNION ALL \
             SELECT 'virtual' AS source, number FROM numbers \
             ORDER BY source, number",
        )
        .await;

        assert_snapshot!(result, @r"
        +---------+--------+
        | source  | number |
        +---------+--------+
        | cte     | 42     |
        | virtual | 1      |
        | virtual | 2      |
        | virtual | 3      |
        +---------+--------+
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

    // A RelationPlanner can handle time travel clauses itself, for tables
    // whose catalog does not support time travel.
    #[tokio::test]
    async fn planner_handles_time_travel() {
        let ctx =
            create_context_with_catalog_table("t", vec![1, 2, 3], vec!["a", "b", "c"])
                .with_planner(EmulatedVersionPlanner);
        ctx.sql("SET datafusion.sql_parser.dialect = 'Databricks'")
            .await
            .unwrap();

        let result =
            execute_sql_to_string(&ctx, "SELECT * FROM t VERSION AS OF 2 ORDER BY id")
                .await;
        assert_snapshot!(result, @r"
        +----+------+
        | id | name |
        +----+------+
        | 1  | a    |
        | 2  | b    |
        +----+------+
        ");

        // Without the planner, the catalog's lack of time travel support is
        // reported
        let ctx =
            create_context_with_catalog_table("t", vec![1, 2, 3], vec!["a", "b", "c"]);
        ctx.sql("SET datafusion.sql_parser.dialect = 'Databricks'")
            .await
            .unwrap();
        let error = execute_sql(&ctx, "SELECT * FROM t VERSION AS OF 2")
            .await
            .expect_err("time travel should not be supported");
        assert_snapshot!(error.strip_backtrace(), @"This feature is not implemented: Table selectors are not supported for table 't@2'");
    }

    // The current version of a table referenced with a time travel clause is
    // only fetched when a RelationPlanner may handle the time travel clause
    // itself, whether or not the catalog implements `table_with_selector`.
    #[tokio::test]
    async fn time_travel_fetches_current_version_only_for_planners() {
        for (supports_time_travel, with_planner) in
            [(true, false), (true, true), (false, false), (false, true)]
        {
            let case = format!(
                "supports_time_travel={supports_time_travel}, with_planner={with_planner}"
            );
            let mut ctx =
                create_context_with_catalog_table("unused", vec![1, 2], vec!["a", "b"]);
            if with_planner {
                ctx = ctx.with_planner(EmulatedVersionPlanner);
            }
            ctx.sql("SET datafusion.sql_parser.dialect = 'Databricks'")
                .await
                .unwrap();
            let table = ctx.table_provider("unused").await.unwrap();
            let schema = Arc::new(CountingSchema {
                table,
                supports_time_travel,
                table_calls: AtomicUsize::new(0),
                table_with_selector_calls: AtomicUsize::new(0),
            });
            ctx.catalog("datafusion")
                .unwrap()
                .register_schema("counting", Arc::clone(&schema) as _)
                .unwrap();

            let result =
                execute_sql(&ctx, "SELECT * FROM counting.t VERSION AS OF 1").await;
            // Planned by the planner if registered, and by DataFusion
            // otherwise
            assert_eq!(
                result.is_ok(),
                supports_time_travel || with_planner,
                "{case}: {result:?}"
            );
            assert_eq!(
                schema.table_calls.load(Ordering::SeqCst),
                usize::from(with_planner),
                "{case}"
            );
            assert_eq!(
                schema.table_with_selector_calls.load(Ordering::SeqCst),
                1,
                "{case}"
            );
        }
    }

    /// Registers a [`CountingSchema`] named `counting` in `ctx`, with a table
    /// `t` that has the schema of table `unused`
    async fn register_counting_schema(
        ctx: &SessionContext,
        supports_time_travel: bool,
    ) -> Arc<CountingSchema> {
        let table = ctx.table_provider("unused").await.unwrap();
        let schema = Arc::new(CountingSchema {
            table,
            supports_time_travel,
            table_calls: AtomicUsize::new(0),
            table_with_selector_calls: AtomicUsize::new(0),
        });
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema("counting", Arc::clone(&schema) as _)
            .unwrap();
        schema
    }

    // A RelationPlanner can determine the selector of a table from syntax
    // other than a time travel clause
    #[tokio::test]
    async fn planner_determines_table_selector() {
        let ctx = create_context_with_catalog_table("unused", vec![1, 2], vec!["a", "b"])
            .with_planner(VersionHintPlanner);
        ctx.sql("SET datafusion.sql_parser.dialect = 'MsSQL'")
            .await
            .unwrap();
        let schema = register_counting_schema(&ctx, true).await;

        let plan = ctx
            .sql("SELECT * FROM counting.t WITH (1 + 1)")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert_snapshot!(plan, @r"
        Projection: counting.t.id, counting.t.name
          TableScan: counting.t@2
        ");
        assert_eq!(schema.table_with_selector_calls.load(Ordering::SeqCst), 1);

        // The planner only handles tables in the `counting` schema, so the
        // hint of other tables is ignored by DataFusion's default logic
        let plan = ctx
            .sql("SELECT * FROM unused WITH (1 + 1)")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert_snapshot!(plan, @r"
        Projection: unused.id, unused.name
          TableScan: unused
        ");

        // References to CTEs are read without a selector, so the planner is
        // not asked for one
        let plan = ctx
            .sql("WITH c AS (SELECT 1 AS id) SELECT * FROM c WITH (1 + 1)")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert_snapshot!(plan, @r"
        Projection: c.id
          SubqueryAlias: c
            Projection: Int64(1) AS id
              EmptyRelation: rows=1
        ");

        // Also not in prepared statements, where evaluating `now()` for a
        // selector would otherwise be rejected
        ctx.sql("PREPARE p AS WITH c AS (SELECT 1 AS id) SELECT * FROM c WITH (now())")
            .await
            .unwrap();
    }

    // A RelationPlanner can read a table as returned by the catalog,
    // regardless of its time travel clause. The first planner that determines
    // the selector of a table takes precedence, and the planner registered
    // last is asked first.
    #[tokio::test]
    async fn planner_ignores_time_travel() {
        let ctx = create_context_with_catalog_table("unused", vec![1, 2], vec!["a", "b"])
            .with_planner(IgnoreTimeTravelPlanner)
            .with_planner(VersionHintPlanner);
        ctx.sql("SET datafusion.sql_parser.dialect = 'MsSQL'")
            .await
            .unwrap();
        let schema = register_counting_schema(&ctx, false).await;

        let plan = ctx
            .sql("SELECT * FROM counting.t FOR SYSTEM_TIME AS OF '2024-01-01'")
            .await
            .unwrap()
            .into_unoptimized_plan();
        assert_snapshot!(plan, @r"
        Projection: counting.t.id, counting.t.name
          TableScan: counting.t
        ");
        assert_eq!(schema.table_with_selector_calls.load(Ordering::SeqCst), 0);

        // `VersionHintPlanner` takes precedence over `IgnoreTimeTravelPlanner`,
        // so the catalog's lack of support for table selectors is reported
        let error = execute_sql(&ctx, "SELECT * FROM counting.t WITH (2)")
            .await
            .expect_err("table selectors should not be supported");
        assert_snapshot!(error.strip_backtrace(), @"This feature is not implemented: Time travel is not supported");
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
