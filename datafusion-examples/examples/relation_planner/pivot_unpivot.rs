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

//! This example demonstrates using custom relation planners to implement
//! PIVOT and UNPIVOT operations for reshaping data.
//!
//! PIVOT transforms rows into columns (wide format), while UNPIVOT does the
//! reverse, transforming columns into rows (long format). This example shows
//! how to use custom planners to implement these SQL clauses by rewriting them
//! into equivalent standard SQL operations:
//!
//! - PIVOT is rewritten to GROUP BY with CASE expressions
//! - UNPIVOT is rewritten to UNION ALL of projections

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    case, lit,
    logical_plan::builder::LogicalPlanBuilder,
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    Expr,
};
use datafusion_sql::sqlparser::ast::TableFactor;

/// This example demonstrates using custom relation planners to implement
/// PIVOT and UNPIVOT operations for reshaping data.
pub async fn pivot_unpivot() -> Result<()> {
    let ctx = SessionContext::new();

    // Register sample data tables
    register_sample_data(&ctx)?;

    // Register custom planner
    ctx.register_relation_planner(Arc::new(PivotUnpivotPlanner))?;

    println!("Custom Relation Planner: PIVOT and UNPIVOT Operations");
    println!("======================================================\n");

    // Example 1: Basic PIVOT to transform monthly sales data from rows to columns
    // Shows: How to pivot sales data so each quarter becomes a column
    // The PIVOT is rewritten to: SELECT region, SUM(CASE WHEN quarter = 'Q1' THEN amount END) as Q1,
    //                             SUM(CASE WHEN quarter = 'Q2' THEN amount END) as Q2
    //                             FROM quarterly_sales GROUP BY region
    // Expected Output:
    // +--------+------+------+
    // | region | Q1   | Q2   |
    // +--------+------+------+
    // | North  | 1000 | 1500 |
    // | South  | 1200 | 1300 |
    // +--------+------+------+
    run_example(
        &ctx,
        "Example 1: Basic PIVOT - Transform quarters from rows to columns",
        r#"SELECT * FROM quarterly_sales
           PIVOT (
             SUM(amount)
             FOR quarter IN ('Q1', 'Q2')
           ) AS pivoted"#,
    )
    .await?;

    // Example 2: PIVOT with multiple aggregate functions
    // Shows: How to apply multiple aggregations (SUM and AVG) during pivot
    // Expected: Logical plan showing MiniPivot with both SUM and AVG aggregates
    // Actual (Logical Plan):
    // Projection: pivoted.region, pivoted.Q1, pivoted.Q2
    //   SubqueryAlias: pivoted
    //     MiniPivot aggregate=[SUM(amount), AVG(amount)] value_column=[quarter] values=["Q1", "Q2"]
    //       Values: (Utf8("North"), Utf8("Q1"), Int64(1000)), (Utf8("North"), Utf8("Q2"), Int64(1500)), ...
    run_example(
        &ctx,
        "Example 2: PIVOT with multiple aggregates (SUM and AVG)",
        r#"SELECT * FROM quarterly_sales
           PIVOT (
             SUM(amount), AVG(amount)
             FOR quarter IN ('Q1', 'Q2')
           ) AS pivoted"#,
    )
    .await?;

    // Example 3: PIVOT with additional grouping columns
    // Shows: How pivot works when there are multiple non-pivot columns
    // The region and product both appear in GROUP BY
    // Expected Output:
    // +--------+-----------+------+------+
    // | region | product   | Q1   | Q2   |
    // +--------+-----------+------+------+
    // | North  | ProductA  | 500  |      |
    // | North  | ProductB  | 500  |      |
    // | South  | ProductA  |      | 650  |
    // +--------+-----------+------+------+
    run_example(
        &ctx,
        "Example 3: PIVOT with multiple grouping columns",
        r#"SELECT * FROM product_sales
           PIVOT (
             SUM(amount)
             FOR quarter IN ('Q1', 'Q2')
           ) AS pivoted"#,
    )
    .await?;

    // Example 4: Basic UNPIVOT to transform columns back into rows
    // Shows: How to unpivot wide-format data into long format
    // The UNPIVOT is rewritten to:
    //   SELECT region, 'q1_label' as quarter, q1 as sales FROM wide_sales
    //   UNION ALL
    //   SELECT region, 'q2_label' as quarter, q2 as sales FROM wide_sales
    // Expected Output:
    // +--------+----------+-------+
    // | region | quarter  | sales |
    // +--------+----------+-------+
    // | North  | q1_label | 1000  |
    // | South  | q1_label | 1200  |
    // | North  | q2_label | 1500  |
    // | South  | q2_label | 1300  |
    // +--------+----------+-------+
    run_example(
        &ctx,
        "Example 4: Basic UNPIVOT - Transform columns to rows",
        r#"SELECT * FROM wide_sales
           UNPIVOT (
             sales FOR quarter IN (q1 AS 'q1_label', q2 AS 'q2_label')
           ) AS unpivoted"#,
    )
    .await?;

    // Example 5: UNPIVOT with INCLUDE NULLS
    // Shows: How null handling works in UNPIVOT operations
    // With INCLUDE NULLS, the filter `sales IS NOT NULL` is NOT added
    // Expected: Same output as Example 4 (no nulls in this dataset anyway)
    run_example(
        &ctx,
        "Example 5: UNPIVOT with INCLUDE NULLS",
        r#"SELECT * FROM wide_sales
           UNPIVOT INCLUDE NULLS (
             sales FOR quarter IN (q1 AS 'q1_label', q2 AS 'q2_label')
           ) AS unpivoted"#,
    )
    .await?;

    // Example 6: Simple PIVOT with projection
    // Shows: PIVOT works seamlessly with other SQL operations like projection
    // We can select specific columns after pivoting
    run_example(
        &ctx,
        "Example 6: PIVOT with projection",
        r#"SELECT region FROM quarterly_sales
           PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) AS pivoted"#,
    )
    .await?;

    Ok(())
}

/// Register sample data tables for the examples
fn register_sample_data(ctx: &SessionContext) -> Result<()> {
    // Create quarterly_sales table: region, quarter, amount
    let region: ArrayRef =
        Arc::new(StringArray::from(vec!["North", "North", "South", "South"]));
    let quarter: ArrayRef = Arc::new(StringArray::from(vec!["Q1", "Q2", "Q1", "Q2"]));
    let amount: ArrayRef = Arc::new(Int64Array::from(vec![1000, 1500, 1200, 1300]));
    let batch = RecordBatch::try_from_iter(vec![
        ("region", region),
        ("quarter", quarter),
        ("amount", amount),
    ])?;
    ctx.register_batch("quarterly_sales", batch)?;

    // Create product_sales table: region, quarter, product, amount
    let region: ArrayRef = Arc::new(StringArray::from(vec!["North", "North", "South"]));
    let quarter: ArrayRef = Arc::new(StringArray::from(vec!["Q1", "Q1", "Q2"]));
    let product: ArrayRef =
        Arc::new(StringArray::from(vec!["ProductA", "ProductB", "ProductA"]));
    let amount: ArrayRef = Arc::new(Int64Array::from(vec![500, 500, 650]));
    let batch = RecordBatch::try_from_iter(vec![
        ("region", region),
        ("quarter", quarter),
        ("product", product),
        ("amount", amount),
    ])?;
    ctx.register_batch("product_sales", batch)?;

    // Create wide_sales table: region, q1, q2
    let region: ArrayRef = Arc::new(StringArray::from(vec!["North", "South"]));
    let q1: ArrayRef = Arc::new(Int64Array::from(vec![1000, 1200]));
    let q2: ArrayRef = Arc::new(Int64Array::from(vec![1500, 1300]));
    let batch =
        RecordBatch::try_from_iter(vec![("region", region), ("q1", q1), ("q2", q2)])?;
    ctx.register_batch("wide_sales", batch)?;

    Ok(())
}

async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;

    // Show the logical plan to demonstrate the rewrite
    println!("Rewritten Logical Plan:");
    println!("{}\n", df.logical_plan().display_indent());

    // Execute and show results
    println!("Results:");
    df.show().await?;
    println!();
    Ok(())
}

/// Helper function to extract column name from an expression
fn get_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        _ => None,
    }
}

/// Custom planner that handles PIVOT and UNPIVOT table factor syntax
#[derive(Debug)]
struct PivotUnpivotPlanner;

impl RelationPlanner for PivotUnpivotPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            // Handle PIVOT operations
            TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                alias,
                ..
            } => {
                println!("[PivotUnpivotPlanner] Processing PIVOT clause");

                // Plan the input table
                let input = context.plan(*table)?;
                let input_schema = input.schema().clone();
                println!(
                    "[PivotUnpivotPlanner] Input schema has {} fields",
                    input_schema.fields().len()
                );

                // Process aggregate functions
                let aggregates = aggregate_functions
                    .iter()
                    .map(|agg| {
                        let expr = context
                            .sql_to_expr(agg.expr.clone(), input_schema.as_ref())?;
                        Ok(expr)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Get the pivot column (should be a single column for simple case)
                if value_column.len() != 1 {
                    return Err(DataFusionError::Plan(
                        "Only single-column pivot supported in this example".into(),
                    ));
                }
                let pivot_col = context
                    .sql_to_expr(value_column[0].clone(), input_schema.as_ref())?;
                let pivot_col_name = get_column_name(&pivot_col).ok_or_else(|| {
                    DataFusionError::Plan(
                        "Pivot column must be a column reference".into(),
                    )
                })?;

                // Process pivot values
                use datafusion_sql::sqlparser::ast::PivotValueSource;
                let pivot_values = match value_source {
                    PivotValueSource::List(list) => list
                        .iter()
                        .map(|item| {
                            let alias = item
                                .alias
                                .as_ref()
                                .map(|id| context.normalize_ident(id.clone()));
                            let expr = context
                                .sql_to_expr(item.expr.clone(), input_schema.as_ref())?;
                            Ok((alias, expr))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    _ => {
                        return Err(DataFusionError::Plan(
                            "Dynamic pivot (ANY/Subquery) not supported in this example"
                                .into(),
                        ));
                    }
                };

                // Build the rewritten plan: GROUP BY with CASE expressions
                // For each aggregate and each pivot value, create: aggregate(CASE WHEN pivot_col = value THEN agg_input END)

                let mut pivot_exprs = Vec::new();

                // Determine grouping columns (all non-pivot columns, excluding aggregate inputs)
                // Collect aggregate input column names
                let agg_input_cols: Vec<String> = aggregates
                    .iter()
                    .filter_map(|agg| {
                        if let Expr::AggregateFunction(agg_fn) = agg {
                            agg_fn.params.args.first().and_then(get_column_name)
                        } else {
                            None
                        }
                    })
                    .collect();

                let mut group_by_cols = Vec::new();
                for field in input_schema.fields() {
                    let field_name = field.name();
                    // Include in GROUP BY if it's not the pivot column and not an aggregate input
                    if field_name != &pivot_col_name
                        && !agg_input_cols.contains(field_name)
                    {
                        group_by_cols.push(col(field_name));
                    }
                }

                // Create CASE expressions for each (aggregate, pivot_value) pair
                for agg_func in &aggregates {
                    for (alias_opt, pivot_value) in &pivot_values {
                        // Get the input to the aggregate function
                        let agg_input = if let Expr::AggregateFunction(agg_fn) = agg_func
                        {
                            if agg_fn.params.args.len() == 1 {
                                agg_fn.params.args[0].clone()
                            } else {
                                return Err(DataFusionError::Plan(
                                    "Only single-argument aggregates supported in this example".into(),
                                ));
                            }
                        } else {
                            return Err(DataFusionError::Plan(
                                "Expected aggregate function".into(),
                            ));
                        };

                        // Create: CASE WHEN pivot_col = pivot_value THEN agg_input END
                        let case_expr = case(col(&pivot_col_name))
                            .when(pivot_value.clone(), agg_input)
                            .end()?;

                        // Wrap in aggregate function
                        let pivoted_expr =
                            if let Expr::AggregateFunction(agg_fn) = agg_func {
                                agg_fn.func.call(vec![case_expr])
                            } else {
                                return Err(DataFusionError::Plan(
                                    "Expected aggregate function".into(),
                                ));
                            };

                        // Determine the column alias
                        let value_part = alias_opt.clone().unwrap_or_else(|| {
                            if let Expr::Literal(ScalarValue::Utf8(Some(s)), _) =
                                pivot_value
                            {
                                s.clone()
                            } else if let Expr::Literal(lit, _) = pivot_value {
                                format!("{lit}")
                            } else {
                                format!("{pivot_value}")
                            }
                        });

                        // If there are multiple aggregates, prefix with function name
                        let col_alias = if aggregates.len() > 1 {
                            let agg_name =
                                if let Expr::AggregateFunction(agg_fn) = agg_func {
                                    agg_fn.func.name()
                                } else {
                                    "agg"
                                };
                            format!("{agg_name}_{value_part}")
                        } else {
                            value_part
                        };

                        pivot_exprs.push(pivoted_expr.alias(col_alias));
                    }
                }

                // Build the final plan: GROUP BY with aggregations
                let plan = LogicalPlanBuilder::from(input)
                    .aggregate(group_by_cols, pivot_exprs)?
                    .build()?;

                println!("[PivotUnpivotPlanner] Successfully rewrote PIVOT to GROUP BY with CASE");
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }

            // Handle UNPIVOT operations
            TableFactor::Unpivot {
                table,
                value,
                name,
                columns,
                null_inclusion,
                alias,
            } => {
                println!("[PivotUnpivotPlanner] Processing UNPIVOT clause");

                // Plan the input table
                let input = context.plan(*table)?;
                let input_schema = input.schema().clone();
                println!(
                    "[PivotUnpivotPlanner] Input schema has {} fields",
                    input_schema.fields().len()
                );

                // Get output column names
                let value_col_name = format!("{value}");
                let name_col_name = context.normalize_ident(name.clone());
                println!(
                    "[PivotUnpivotPlanner] Value column name (output): {value_col_name}"
                );
                println!(
                    "[PivotUnpivotPlanner] Name column name (output): {name_col_name}"
                );

                // Process columns to unpivot
                let unpivot_cols = columns
                    .iter()
                    .map(|col| {
                        let label = col
                            .alias
                            .as_ref()
                            .map(|id| context.normalize_ident(id.clone()))
                            .unwrap_or_else(|| format!("{}", col.expr));

                        let expr = context
                            .sql_to_expr(col.expr.clone(), input_schema.as_ref())?;
                        let col_name = get_column_name(&expr)
                            .ok_or_else(|| DataFusionError::Plan("Unpivot column must be a column reference".into()))?;

                        println!(
                            "[PivotUnpivotPlanner] Will unpivot column '{col_name}' with label '{label}'"
                        );

                        Ok((col_name, label))
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Determine which columns to keep (not being unpivoted)
                let keep_cols: Vec<String> = input_schema
                    .fields()
                    .iter()
                    .filter_map(|field| {
                        let field_name = field.name();
                        if !unpivot_cols.iter().any(|(col, _)| col == field_name) {
                            Some(field_name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Build UNION ALL of projections
                // For each unpivot column, create: SELECT keep_cols..., 'label' as name_col, col as value_col FROM input
                let mut union_inputs = Vec::new();
                for (col_name, label) in &unpivot_cols {
                    let mut projection = Vec::new();

                    // Add all columns we're keeping
                    for keep_col in &keep_cols {
                        projection.push(col(keep_col));
                    }

                    // Add the name column (constant label)
                    projection.push(lit(label.clone()).alias(&name_col_name));

                    // Add the value column (the column being unpivoted)
                    projection.push(col(col_name).alias(&value_col_name));

                    let projected = LogicalPlanBuilder::from(input.clone())
                        .project(projection)?
                        .build()?;

                    union_inputs.push(projected);
                }

                // Build UNION ALL
                if union_inputs.is_empty() {
                    return Err(DataFusionError::Plan(
                        "UNPIVOT requires at least one column".into(),
                    ));
                }

                let mut union_iter = union_inputs.into_iter();
                let mut plan = union_iter.next().unwrap();
                for union_input in union_iter {
                    plan = LogicalPlanBuilder::from(plan)
                        .union(LogicalPlanBuilder::from(union_input).build()?)?
                        .build()?;
                }

                // Handle EXCLUDE NULLS (default) by filtering out nulls
                if null_inclusion.is_none()
                    || matches!(
                        null_inclusion,
                        Some(datafusion_sql::sqlparser::ast::NullInclusion::ExcludeNulls)
                    )
                {
                    plan = LogicalPlanBuilder::from(plan)
                        .filter(col(&value_col_name).is_not_null())?
                        .build()?;
                }

                println!(
                    "[PivotUnpivotPlanner] Successfully rewrote UNPIVOT to UNION ALL"
                );
                Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
            }

            other => Ok(RelationPlanning::Original(other)),
        }
    }
}
