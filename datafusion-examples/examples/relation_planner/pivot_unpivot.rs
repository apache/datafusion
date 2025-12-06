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

//! # PIVOT and UNPIVOT Example
//!
//! This example demonstrates implementing SQL `PIVOT` and `UNPIVOT` operations
//! using a custom [`RelationPlanner`]. Unlike the other examples that create
//! custom logical/physical nodes, this example shows how to **rewrite** SQL
//! constructs into equivalent standard SQL operations:
//!
//! ## Supported Syntax
//!
//! ```sql
//! -- PIVOT: Transform rows into columns
//! SELECT * FROM sales
//!   PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
//!
//! -- UNPIVOT: Transform columns into rows
//! SELECT * FROM wide_table
//!   UNPIVOT (value FOR name IN (col1, col2, col3))
//! ```
//!
//! ## Rewrite Strategy
//!
//! **PIVOT** is rewritten to `GROUP BY` with `CASE` expressions:
//! ```sql
//! -- Original:
//! SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2'))
//!
//! -- Rewritten to:
//! SELECT region,
//!        SUM(CASE quarter WHEN 'Q1' THEN amount END) AS Q1,
//!        SUM(CASE quarter WHEN 'Q2' THEN amount END) AS Q2
//! FROM sales
//! GROUP BY region
//! ```
//!
//! **UNPIVOT** is rewritten to `UNION ALL` of projections:
//! ```sql
//! -- Original:
//! SELECT * FROM wide UNPIVOT (sales FOR quarter IN (q1, q2))
//!
//! -- Rewritten to:
//! SELECT region, 'q1' AS quarter, q1 AS sales FROM wide
//! UNION ALL
//! SELECT region, 'q2' AS quarter, q2 AS sales FROM wide
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_common::{plan_datafusion_err, Result, ScalarValue};
use datafusion_expr::{
    case, col, lit,
    logical_plan::builder::LogicalPlanBuilder,
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    Expr,
};
use datafusion_sql::sqlparser::ast::{NullInclusion, PivotValueSource, TableFactor};
use insta::assert_snapshot;

// ============================================================================
// Example Entry Point
// ============================================================================

/// Runs the PIVOT/UNPIVOT examples demonstrating data reshaping operations.
pub async fn pivot_unpivot() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(PivotUnpivotPlanner))?;
    register_sample_data(&ctx)?;

    println!("PIVOT and UNPIVOT Example");
    println!("=========================\n");

    run_examples(&ctx).await
}

async fn run_examples(ctx: &SessionContext) -> Result<()> {
    // ----- PIVOT Examples -----

    // Example 1: Basic PIVOT
    // Transforms: (region, quarter, amount) → (region, Q1, Q2)
    let results = run_example(
        ctx,
        "Example 1: Basic PIVOT",
        r#"SELECT * FROM quarterly_sales
           PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) AS p
           ORDER BY region"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+------+------+
    | region | Q1   | Q2   |
    +--------+------+------+
    | North  | 1000 | 1500 |
    | South  | 1200 | 1300 |
    +--------+------+------+
    ");

    // Example 2: PIVOT with multiple aggregates
    // Creates columns for each (aggregate, value) combination
    let results = run_example(
        ctx,
        "Example 2: PIVOT with multiple aggregates",
        r#"SELECT * FROM quarterly_sales
           PIVOT (SUM(amount), AVG(amount) FOR quarter IN ('Q1', 'Q2')) AS p
           ORDER BY region"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+--------+--------+--------+--------+
    | region | sum_Q1 | sum_Q2 | avg_Q1 | avg_Q2 |
    +--------+--------+--------+--------+--------+
    | North  | 1000   | 1500   | 1000.0 | 1500.0 |
    | South  | 1200   | 1300   | 1200.0 | 1300.0 |
    +--------+--------+--------+--------+--------+
    ");

    // Example 3: PIVOT with multiple grouping columns
    // Non-pivot, non-aggregate columns become GROUP BY columns
    let results = run_example(
        ctx,
        "Example 3: PIVOT with multiple grouping columns",
        r#"SELECT * FROM product_sales
           PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) AS p
           ORDER BY region, product"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+----------+-----+-----+
    | region | product  | Q1  | Q2  |
    +--------+----------+-----+-----+
    | North  | ProductA | 500 |     |
    | North  | ProductB | 500 |     |
    | South  | ProductA |     | 650 |
    +--------+----------+-----+-----+
    ");

    // ----- UNPIVOT Examples -----

    // Example 4: Basic UNPIVOT
    // Transforms: (region, q1, q2) → (region, quarter, sales)
    let results = run_example(
        ctx,
        "Example 4: Basic UNPIVOT",
        r#"SELECT * FROM wide_sales
           UNPIVOT (sales FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2')) AS u
           ORDER BY quarter, region"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+---------+-------+
    | region | quarter | sales |
    +--------+---------+-------+
    | North  | Q1      | 1000  |
    | South  | Q1      | 1200  |
    | North  | Q2      | 1500  |
    | South  | Q2      | 1300  |
    +--------+---------+-------+
    ");

    // Example 5: UNPIVOT with INCLUDE NULLS
    // By default, UNPIVOT excludes rows where the value column is NULL.
    // INCLUDE NULLS keeps them (same result here since no NULLs in data).
    let results = run_example(
        ctx,
        "Example 5: UNPIVOT INCLUDE NULLS",
        r#"SELECT * FROM wide_sales
           UNPIVOT INCLUDE NULLS (sales FOR quarter IN (q1 AS 'Q1', q2 AS 'Q2')) AS u
           ORDER BY quarter, region"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+---------+-------+
    | region | quarter | sales |
    +--------+---------+-------+
    | North  | Q1      | 1000  |
    | South  | Q1      | 1200  |
    | North  | Q2      | 1500  |
    | South  | Q2      | 1300  |
    +--------+---------+-------+
    ");

    // Example 6: PIVOT with column projection
    // Standard SQL operations work seamlessly after PIVOT
    let results = run_example(
        ctx,
        "Example 6: PIVOT with projection",
        r#"SELECT region FROM quarterly_sales
           PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) AS p
           ORDER BY region"#,
    )
    .await?;
    assert_snapshot!(results, @r"
    +--------+
    | region |
    +--------+
    | North  |
    | South  |
    +--------+
    ");

    Ok(())
}

/// Helper to run a single example query and capture results.
async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<String> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;
    println!("{}\n", df.logical_plan().display_indent());

    let batches = df.collect().await?;
    let results = arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    println!("{results}\n");

    Ok(results)
}

/// Register test data tables.
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    // quarterly_sales: normalized sales data (region, quarter, amount)
    ctx.register_batch(
        "quarterly_sales",
        RecordBatch::try_from_iter(vec![
            (
                "region",
                Arc::new(StringArray::from(vec!["North", "North", "South", "South"]))
                    as ArrayRef,
            ),
            (
                "quarter",
                Arc::new(StringArray::from(vec!["Q1", "Q2", "Q1", "Q2"])),
            ),
            (
                "amount",
                Arc::new(Int64Array::from(vec![1000, 1500, 1200, 1300])),
            ),
        ])?,
    )?;

    // product_sales: sales with additional grouping dimension
    ctx.register_batch(
        "product_sales",
        RecordBatch::try_from_iter(vec![
            (
                "region",
                Arc::new(StringArray::from(vec!["North", "North", "South"])) as ArrayRef,
            ),
            (
                "quarter",
                Arc::new(StringArray::from(vec!["Q1", "Q1", "Q2"])),
            ),
            (
                "product",
                Arc::new(StringArray::from(vec!["ProductA", "ProductB", "ProductA"])),
            ),
            ("amount", Arc::new(Int64Array::from(vec![500, 500, 650]))),
        ])?,
    )?;

    // wide_sales: denormalized/wide format (for UNPIVOT)
    ctx.register_batch(
        "wide_sales",
        RecordBatch::try_from_iter(vec![
            (
                "region",
                Arc::new(StringArray::from(vec!["North", "South"])) as ArrayRef,
            ),
            ("q1", Arc::new(Int64Array::from(vec![1000, 1200]))),
            ("q2", Arc::new(Int64Array::from(vec![1500, 1300]))),
        ])?,
    )?;

    Ok(())
}

// ============================================================================
// Relation Planner: PivotUnpivotPlanner
// ============================================================================

/// Relation planner that rewrites PIVOT and UNPIVOT into standard SQL.
#[derive(Debug)]
struct PivotUnpivotPlanner;

impl RelationPlanner for PivotUnpivotPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        ctx: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                alias,
                ..
            } => plan_pivot(
                ctx,
                *table,
                &aggregate_functions,
                &value_column,
                value_source,
                alias,
            ),

            TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                null_inclusion,
                alias,
            } => plan_unpivot(
                ctx,
                *table,
                &value,
                name,
                &columns,
                null_inclusion.as_ref(),
                alias,
            ),

            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

// ============================================================================
// PIVOT Implementation
// ============================================================================

/// Rewrite PIVOT to GROUP BY with CASE expressions.
fn plan_pivot(
    ctx: &mut dyn RelationPlannerContext,
    table: TableFactor,
    aggregate_functions: &[datafusion_sql::sqlparser::ast::ExprWithAlias],
    value_column: &[datafusion_sql::sqlparser::ast::Expr],
    value_source: PivotValueSource,
    alias: Option<datafusion_sql::sqlparser::ast::TableAlias>,
) -> Result<RelationPlanning> {
    // Plan the input table
    let input = ctx.plan(table)?;
    let schema = input.schema();

    // Parse aggregate functions
    let aggregates: Vec<Expr> = aggregate_functions
        .iter()
        .map(|agg| ctx.sql_to_expr(agg.expr.clone(), schema.as_ref()))
        .collect::<Result<_>>()?;

    // Get the pivot column (only single-column pivot supported)
    if value_column.len() != 1 {
        return Err(plan_datafusion_err!(
            "Only single-column PIVOT is supported"
        ));
    }
    let pivot_col = ctx.sql_to_expr(value_column[0].clone(), schema.as_ref())?;
    let pivot_col_name = extract_column_name(&pivot_col)?;

    // Parse pivot values
    let pivot_values = match value_source {
        PivotValueSource::List(list) => list
            .iter()
            .map(|item| {
                let alias = item
                    .alias
                    .as_ref()
                    .map(|id| ctx.normalize_ident(id.clone()));
                let expr = ctx.sql_to_expr(item.expr.clone(), schema.as_ref())?;
                Ok((alias, expr))
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(plan_datafusion_err!(
                "Dynamic PIVOT (ANY/Subquery) is not supported"
            ));
        }
    };

    // Determine GROUP BY columns (non-pivot, non-aggregate columns)
    let agg_input_cols: Vec<&str> = aggregates
        .iter()
        .filter_map(|agg| {
            if let Expr::AggregateFunction(f) = agg {
                f.params.args.first().and_then(|e| {
                    if let Expr::Column(c) = e {
                        Some(c.name.as_str())
                    } else {
                        None
                    }
                })
            } else {
                None
            }
        })
        .collect();

    let group_by_cols: Vec<Expr> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| *name != pivot_col_name.as_str() && !agg_input_cols.contains(name))
        .map(col)
        .collect();

    // Build CASE expressions for each (aggregate, pivot_value) pair
    let mut pivot_exprs = Vec::new();
    for agg in &aggregates {
        let Expr::AggregateFunction(agg_fn) = agg else {
            continue;
        };
        let Some(agg_input) = agg_fn.params.args.first().cloned() else {
            continue;
        };

        for (value_alias, pivot_value) in &pivot_values {
            // CASE pivot_col WHEN pivot_value THEN agg_input END
            let case_expr = case(col(&pivot_col_name))
                .when(pivot_value.clone(), agg_input.clone())
                .end()?;

            // Wrap in aggregate function
            let pivoted = agg_fn.func.call(vec![case_expr]);

            // Determine column alias
            let value_str = value_alias
                .clone()
                .unwrap_or_else(|| expr_to_string(pivot_value));
            let col_alias = if aggregates.len() > 1 {
                format!("{}_{}", agg_fn.func.name(), value_str)
            } else {
                value_str
            };

            pivot_exprs.push(pivoted.alias(col_alias));
        }
    }

    let plan = LogicalPlanBuilder::from(input)
        .aggregate(group_by_cols, pivot_exprs)?
        .build()?;

    Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
}

// ============================================================================
// UNPIVOT Implementation
// ============================================================================

/// Rewrite UNPIVOT to UNION ALL of projections.
fn plan_unpivot(
    ctx: &mut dyn RelationPlannerContext,
    table: TableFactor,
    value: &datafusion_sql::sqlparser::ast::Expr,
    name: datafusion_sql::sqlparser::ast::Ident,
    columns: &[datafusion_sql::sqlparser::ast::ExprWithAlias],
    null_inclusion: Option<&NullInclusion>,
    alias: Option<datafusion_sql::sqlparser::ast::TableAlias>,
) -> Result<RelationPlanning> {
    // Plan the input table
    let input = ctx.plan(table)?;
    let schema = input.schema();

    // Output column names
    let value_col_name = value.to_string();
    let name_col_name = ctx.normalize_ident(name);

    // Parse columns to unpivot: (source_column, label)
    let unpivot_cols: Vec<(String, String)> = columns
        .iter()
        .map(|c| {
            let label = c
                .alias
                .as_ref()
                .map(|id| ctx.normalize_ident(id.clone()))
                .unwrap_or_else(|| c.expr.to_string());
            let expr = ctx.sql_to_expr(c.expr.clone(), schema.as_ref())?;
            let col_name = extract_column_name(&expr)?;
            Ok((col_name.to_string(), label))
        })
        .collect::<Result<_>>()?;

    // Columns to preserve (not being unpivoted)
    let keep_cols: Vec<&str> = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| !unpivot_cols.iter().any(|(c, _)| c == *name))
        .collect();

    // Build UNION ALL: one SELECT per unpivot column
    if unpivot_cols.is_empty() {
        return Err(plan_datafusion_err!("UNPIVOT requires at least one column"));
    }

    let mut union_inputs: Vec<_> = unpivot_cols
        .iter()
        .map(|(col_name, label)| {
            let mut projection: Vec<Expr> = keep_cols.iter().map(|c| col(*c)).collect();
            projection.push(lit(label.clone()).alias(&name_col_name));
            projection.push(col(col_name).alias(&value_col_name));

            LogicalPlanBuilder::from(input.clone())
                .project(projection)?
                .build()
        })
        .collect::<Result<_>>()?;

    // Combine with UNION ALL
    let mut plan = union_inputs.remove(0);
    for branch in union_inputs {
        plan = LogicalPlanBuilder::from(plan).union(branch)?.build()?;
    }

    // Apply EXCLUDE NULLS filter (default behavior)
    let exclude_nulls = null_inclusion.is_none()
        || matches!(null_inclusion, Some(&NullInclusion::ExcludeNulls));
    if exclude_nulls {
        plan = LogicalPlanBuilder::from(plan)
            .filter(col(&value_col_name).is_not_null())?
            .build()?;
    }

    Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
}

// ============================================================================
// Helpers
// ============================================================================

/// Extract column name from an expression.
fn extract_column_name(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Column(c) => Ok(c.name.clone()),
        _ => Err(plan_datafusion_err!(
            "Expected column reference, got {expr}"
        )),
    }
}

/// Convert an expression to a string for use as column alias.
fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s.clone(),
        Expr::Literal(v, _) => v.to_string(),
        other => other.to_string(),
    }
}
