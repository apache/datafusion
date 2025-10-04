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

//! This example demonstrates using a custom relation planner to implement
//! PostgreSQL-style `generate_series` functionality as a table-valued function.
//!
//! Usage examples:
//!   - `SELECT * FROM generate_series(1, 10)` generates numbers 1-10
//!   - `SELECT * FROM generate_series(0, 100, 25)` generates 0, 25, 50, 75, 100
//!   - `SELECT * FROM generate_series(10, 1, -2)` generates 10, 8, 6, 4, 2

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    logical_plan::{builder::LogicalPlanBuilder, LogicalPlan},
    planner::{
        PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
    },
    Expr,
};
use datafusion_sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, ObjectNamePart, TableFactor,
};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register our custom relation planner
    ctx.register_relation_planner(Arc::new(GenerateSeriesPlanner))?;

    println!("Custom Relation Planner: Generate Series Example");
    println!("==================================================\n");

    // Example 1: Basic series generation from 1 to 5 with default step of 1
    // Shows: Simple integer sequence generation using PostgreSQL-style generate_series function
    // Expected: 5 rows with values 1, 2, 3, 4, 5
    // Actual:
    // +-------+
    // | value |
    // +-------+
    // | 1     |
    // | 2     |
    // | 3     |
    // | 4     |
    // | 5     |
    // +-------+
    run_example(
        &ctx,
        "Example 1: Basic series (1 to 5)",
        "SELECT * FROM generate_series(1, 5)",
    )
    .await?;

    // Example 2: Series with custom step size of 25 from 0 to 100
    // Shows: How to generate sequences with non-unit step sizes
    // Expected: 5 rows with values 0, 25, 50, 75, 100
    // Actual:
    // +-------+
    // | value |
    // +-------+
    // | 0     |
    // | 25    |
    // | 50    |
    // | 75    |
    // | 100   |
    // +-------+
    run_example(
        &ctx,
        "Example 2: Series with custom step (0 to 100, step 25)",
        "SELECT * FROM generate_series(0, 100, 25)",
    )
    .await?;

    // Example 3: Descending series using negative step from 10 down to 0
    // Shows: How to generate sequences in reverse order with negative step
    // Expected: 6 rows with values 10, 8, 6, 4, 2, 0
    // Actual:
    // +-------+
    // | value |
    // +-------+
    // | 10    |
    // | 8     |
    // | 6     |
    // | 4     |
    // | 2     |
    // | 0     |
    // +-------+
    run_example(
        &ctx,
        "Example 3: Descending series with negative step",
        "SELECT * FROM generate_series(10, 0, -2)",
    )
    .await?;

    // Example 4: Using TABLE() constructor syntax instead of direct function call
    // Shows: Alternative SQL syntax for calling table-valued functions
    // Expected: 3 rows with values 5, 10, 15 (note: column name changes to column1)
    // Actual:
    // +---------+
    // | column1 |
    // +---------+
    // | 5       |
    // | 10      |
    // | 15      |
    // +---------+
    run_example(
        &ctx,
        "Example 4: Using TABLE() constructor syntax",
        "SELECT * FROM TABLE(generate_series(5, 15, 5))",
    )
    .await?;

    // Example 5: Cartesian product (cross join) of two generate_series functions
    // Shows: How generate_series can be used in complex queries with joins
    // Expected: 9 rows showing all combinations of (1,2,3) x (1,2,3)
    // Actual:
    // +---+---+
    // | x | y |
    // +---+---+
    // | 1 | 1 |
    // | 1 | 2 |
    // ...
    // | 3 | 2 |
    // | 3 | 3 |
    // +---+---+
    run_example(
        &ctx,
        "Example 5: Cartesian product with generate_series",
        r#"SELECT a.value AS x, b.value AS y 
           FROM generate_series(1, 3) a 
           CROSS JOIN generate_series(1, 3) b 
           ORDER BY x, y"#,
    )
    .await?;

    // Example 6: Using generate_series with WHERE clause and calculated columns
    // Shows: How to filter and transform generated sequences
    // Expected: 5 rows with even numbers and their squares (2,4,6,8,10 and their squares)
    // Actual:
    // +-------+--------+
    // | value | square |
    // +-------+--------+
    // | 2     | 4      |
    // | 4     | 16     |
    // | 6     | 36     |
    // | 8     | 64     |
    // | 10    | 100    |
    // +-------+--------+
    run_example(
        &ctx,
        "Example 6: Using generate_series with filters and calculations",
        r#"SELECT value, value * value AS square 
           FROM generate_series(1, 10) 
           WHERE value % 2 = 0"#,
    )
    .await?;

    Ok(())
}

async fn run_example(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}:\n{sql}\n");
    let df = ctx.sql(sql).await?;
    df.show().await?;
    Ok(())
}

/// Custom relation planner that implements PostgreSQL-style `generate_series`
/// as a table-valued function.
///
/// This planner handles two SQL syntax forms:
/// 1. Function call: `SELECT * FROM generate_series(1, 10)`
/// 2. TABLE constructor: `SELECT * FROM TABLE(generate_series(1, 10))`
#[derive(Debug)]
struct GenerateSeriesPlanner;

impl RelationPlanner for GenerateSeriesPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            // Handle: SELECT * FROM generate_series(...)
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if let Some(planned) =
                    try_plan_generate_series(&name, &args, alias.clone())?
                {
                    Ok(RelationPlanning::Planned(planned))
                } else {
                    Ok(RelationPlanning::Original(TableFactor::Function {
                        lateral,
                        name,
                        args,
                        alias,
                    }))
                }
            }
            // Handle: SELECT * FROM TABLE(generate_series(...))
            TableFactor::TableFunction { expr, alias } => {
                if let ast::Expr::Function(func) = &expr {
                    if let ast::FunctionArguments::List(list) = &func.args {
                        if let Some(planned) = try_plan_generate_series(
                            &func.name,
                            &list.args,
                            alias.clone(),
                        )? {
                            return Ok(RelationPlanning::Planned(planned));
                        }
                    }
                }

                Ok(RelationPlanning::Original(TableFactor::TableFunction {
                    expr,
                    alias,
                }))
            }
            // Pass through any other relation types
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

/// Attempts to plan a `generate_series` function call.
///
/// Returns `Ok(Some(...))` if the function is `generate_series` and planning succeeds,
/// `Ok(None)` if the function is not `generate_series`, or an error if planning fails.
fn try_plan_generate_series(
    name: &ast::ObjectName,
    args: &[FunctionArg],
    alias: Option<ast::TableAlias>,
) -> Result<Option<PlannedRelation>> {
    if !is_generate_series(name) {
        return Ok(None);
    }

    let (start, end, step) = parse_series_args(args)?;

    if step == 0 {
        return Err(DataFusionError::Plan(
            "generate_series step argument must not be zero".into(),
        ));
    }

    let plan = build_series_plan(start, end, step)?;
    Ok(Some(PlannedRelation::new(plan, alias)))
}

/// Checks if the given object name refers to the `generate_series` function.
fn is_generate_series(name: &ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("generate_series"))
}

/// Parses function arguments into (start, end, step) values.
///
/// Accepts either 2 arguments (start, end with step=1) or 3 arguments (start, end, step).
fn parse_series_args(args: &[FunctionArg]) -> Result<(i64, i64, i64)> {
    let mut values = Vec::with_capacity(args.len());

    for arg in args {
        let value = match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
            | FunctionArg::Named {
                arg: FunctionArgExpr::Expr(expr),
                ..
            } => parse_i64(expr)?,
            _ => {
                return Err(DataFusionError::Plan(
                    "generate_series arguments must be literal integers".into(),
                ))
            }
        };
        values.push(value);
    }

    match values.len() {
        2 => Ok((values[0], values[1], 1)),
        3 => Ok((values[0], values[1], values[2])),
        other => Err(DataFusionError::Plan(format!(
            "generate_series expects 2 or 3 arguments, got {other}"
        ))),
    }
}

/// Parses a SQL expression into an i64 value.
///
/// Supports integer literals with optional unary + or - operators.
fn parse_i64(expr: &ast::Expr) -> Result<i64> {
    match expr {
        ast::Expr::Value(value) => match &value.value {
            ast::Value::Number(v, _) => v.to_string().parse().map_err(|_| {
                DataFusionError::Plan(format!(
                    "generate_series argument must be an integer literal, got {expr}"
                ))
            }),
            _ => Err(DataFusionError::Plan(format!(
                "generate_series argument must be an integer literal, got {expr}"
            ))),
        },
        ast::Expr::UnaryOp { op, expr } => match op {
            ast::UnaryOperator::Minus => parse_i64(expr).map(|v| -v),
            ast::UnaryOperator::Plus => parse_i64(expr),
            _ => Err(DataFusionError::Plan(format!(
                "generate_series argument must be an integer literal, got {expr}"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "generate_series argument must be an integer literal, got {expr}"
        ))),
    }
}

/// Builds a logical plan that generates a series of integers.
///
/// Creates a VALUES clause with one row per generated value.
fn build_series_plan(start: i64, end: i64, step: i64) -> Result<LogicalPlan> {
    let mut values = Vec::new();
    let mut current = start;

    if step > 0 {
        while current <= end {
            values.push(vec![Expr::Literal(ScalarValue::Int64(Some(current)), None)]);
            current += step;
        }
    } else {
        while current >= end {
            values.push(vec![Expr::Literal(ScalarValue::Int64(Some(current)), None)]);
            current += step;
        }
    }

    LogicalPlanBuilder::values(values)?.build()
}
