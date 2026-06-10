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

//! See `main.rs` for how to run it.

use std::fmt;
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::sqlparser::ast::Statement;
use datafusion::logical_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::prelude::*;
use datafusion::sql::unparser::ast::{
    DerivedRelationBuilder, QueryBuilder, RelationBuilder, SelectBuilder,
};
use datafusion::sql::unparser::dialect::CustomDialectBuilder;
use datafusion::sql::unparser::expr_to_sql;
use datafusion::sql::unparser::extension_unparser::UserDefinedLogicalNodeUnparser;
use datafusion::sql::unparser::extension_unparser::{
    UnparseToStatementResult, UnparseWithinStatementResult,
};
use datafusion::sql::unparser::{Unparser, plan_to_sql};
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};

/// This example demonstrates the programmatic construction of SQL strings using
/// the DataFusion Expr [`Expr`] and LogicalPlan [`LogicalPlan`] API.
///
///
/// The code in this example shows how to:
///
/// 1. [`simple_expr_to_sql_demo`]: Create a simple expression [`Exprs`] with
///    fluent API and convert to sql suitable for passing to another database
///
/// 2. [`simple_expr_to_pretty_sql_demo`] Create a simple expression
///    [`Exprs`] with fluent API and convert to sql without extra parentheses,
///    suitable for displaying to humans
///
/// 3. [`simple_expr_to_sql_demo_escape_mysql_style`]" Create a simple
///    expression [`Exprs`] with fluent API and convert to sql escaping column
///    names in MySQL style.
///
/// 4. [`simple_plan_to_sql_demo`]: Create a simple logical plan using the
///    DataFrames API and convert to sql string.
///
/// 5. [`round_trip_plan_to_sql_demo`]: Create a logical plan from a SQL string, modify it using the
///    DataFrames API and convert it back to a  sql string.
///
/// 6. [`unparse_my_logical_plan_as_statement`]: Create a custom logical plan and unparse it as a statement.
///
/// 7. [`unparse_my_logical_plan_as_subquery`]: Create a custom logical plan and unparse it as a subquery.
pub async fn plan_to_sql_examples() -> Result<()> {
    // See how to evaluate expressions
    simple_expr_to_sql_demo()?;
    simple_expr_to_pretty_sql_demo()?;
    simple_expr_to_sql_demo_escape_mysql_style()?;
    simple_plan_to_sql_demo().await?;
    round_trip_plan_to_sql_demo().await?;
    unparse_my_logical_plan_as_statement().await?;
    unparse_my_logical_plan_as_subquery().await?;
    Ok(())
}

/// DataFusion can convert expressions to SQL, using column name escaping
/// PostgreSQL style.
fn simple_expr_to_sql_demo() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let sql = expr_to_sql(&expr)?.to_string();
    assert_eq!(sql, r#"((a < 5) OR (a = 8))"#);
    Ok(())
}

/// DataFusion can remove parentheses when converting an expression to SQL.
/// Note that output is intended for humans, not for other SQL engines,
/// as difference in precedence rules can cause expressions to be parsed differently.
fn simple_expr_to_pretty_sql_demo() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let unparser = Unparser::default().with_pretty(true);
    let sql = unparser.expr_to_sql(&expr)?.to_string();
    assert_eq!(sql, r#"a < 5 OR a = 8"#);
    Ok(())
}

/// DataFusion can convert expressions to SQL without escaping column names using
/// using a custom dialect and an explicit unparser
fn simple_expr_to_sql_demo_escape_mysql_style() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let dialect = CustomDialectBuilder::new()
        .with_identifier_quote_style('`')
        .build();
    let unparser = Unparser::new(&dialect);
    let sql = unparser.expr_to_sql(&expr)?.to_string();
    assert_eq!(sql, r#"((`a` < 5) OR (`a` = 8))"#);
    Ok(())
}

/// DataFusion can convert a logic plan created using the DataFrames API to read from a parquet file
/// to SQL, using column name escaping PostgreSQL style.
async fn simple_plan_to_sql_demo() -> Result<()> {
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    let df = ctx
        .read_parquet(parquet_temp.path_str()?, ParquetReadOptions::default())
        .await?
        .select_columns(&["car", "speed", "time"])?;

    // Convert the data frame to a SQL string
    let sql = plan_to_sql(df.logical_plan())?.to_string();

    assert_eq!(
        sql,
        r#"SELECT "?table?".car, "?table?".speed, "?table?"."time" FROM "?table?""#
    );

    Ok(())
}

/// DataFusion can also be used to parse SQL, programmatically modify the query
/// (in this case adding a filter) and then and converting back to SQL.
async fn round_trip_plan_to_sql_demo() -> Result<()> {
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    // register parquet file with the execution context
    ctx.register_parquet(
        "cars",
        parquet_temp.path_str()?,
        ParquetReadOptions::default(),
    )
    .await?;

    // create a logical plan from a SQL string and then programmatically add new filters
    // select car, speed, time from cars where speed > 1 and car = 'red'
    let df = ctx
        // Use SQL to read some data from the parquet file
        .sql("SELECT car, speed, time FROM cars")
        .await?
        // Add speed > 1 and car = 'red' filter
        .filter(
            col("speed")
                .gt(lit(1))
                .and(col("car").eq(lit(ScalarValue::Utf8(Some("red".to_string()))))),
        )?;

    let sql = plan_to_sql(df.logical_plan())?.to_string();
    assert_eq!(
        sql,
        r#"SELECT cars.car, cars.speed, cars."time" FROM cars WHERE ((cars.speed > 1) AND (cars.car = 'red'))"#
    );

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct MyLogicalPlan {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for MyLogicalPlan {
    fn name(&self) -> &str {
        "MyLogicalPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MyLogicalPlan")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(MyLogicalPlan {
            input: inputs.into_iter().next().unwrap(),
        })
    }
}

struct PlanToStatement {}

impl UserDefinedLogicalNodeUnparser for PlanToStatement {
    fn unparse_to_statement(
        &self,
        node: &dyn UserDefinedLogicalNode,
        unparser: &Unparser,
    ) -> Result<UnparseToStatementResult> {
        if let Some(plan) = node.as_any().downcast_ref::<MyLogicalPlan>() {
            let input = unparser.plan_to_sql(&plan.input)?;
            Ok(UnparseToStatementResult::Modified(input))
        } else {
            Ok(UnparseToStatementResult::Unmodified)
        }
    }
}

/// This example demonstrates how to unparse a custom logical plan as a statement.
/// The custom logical plan is a simple extension of the logical plan that reads from a parquet file.
/// It can be unparse as a statement that reads from the same parquet file.
async fn unparse_my_logical_plan_as_statement() -> Result<()> {
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    let inner_plan = ctx
        .read_parquet(parquet_temp.path_str()?, ParquetReadOptions::default())
        .await?
        .select_columns(&["car", "speed", "time"])?
        .into_unoptimized_plan();

    let node = Arc::new(MyLogicalPlan { input: inner_plan });

    let my_plan = LogicalPlan::Extension(Extension { node });
    let unparser =
        Unparser::default().with_extension_unparsers(vec![Arc::new(PlanToStatement {})]);
    let sql = unparser.plan_to_sql(&my_plan)?.to_string();
    assert_eq!(
        sql,
        r#"SELECT "?table?".car, "?table?".speed, "?table?"."time" FROM "?table?""#
    );
    Ok(())
}

struct PlanToSubquery {}
impl UserDefinedLogicalNodeUnparser for PlanToSubquery {
    fn unparse(
        &self,
        node: &dyn UserDefinedLogicalNode,
        unparser: &Unparser,
        _query: &mut Option<&mut QueryBuilder>,
        _select: &mut Option<&mut SelectBuilder>,
        relation: &mut Option<&mut RelationBuilder>,
    ) -> Result<UnparseWithinStatementResult> {
        if let Some(plan) = node.as_any().downcast_ref::<MyLogicalPlan>() {
            let Statement::Query(input) = unparser.plan_to_sql(&plan.input)? else {
                return Ok(UnparseWithinStatementResult::Unmodified);
            };
            let mut derived_builder = DerivedRelationBuilder::default();
            derived_builder.subquery(input);
            derived_builder.lateral(false);
            if let Some(rel) = relation {
                rel.derived(derived_builder);
            }
        }
        Ok(UnparseWithinStatementResult::Modified)
    }
}

/// This example demonstrates how to unparse a custom logical plan as a subquery.
/// The custom logical plan is a simple extension of the logical plan that reads from a parquet file.
/// It can be unparse as a subquery that reads from the same parquet file, with some columns projected.
async fn unparse_my_logical_plan_as_subquery() -> Result<()> {
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    let inner_plan = ctx
        .read_parquet(parquet_temp.path_str()?, ParquetReadOptions::default())
        .await?
        .select_columns(&["car", "speed", "time"])?
        .into_unoptimized_plan();

    let node = Arc::new(MyLogicalPlan { input: inner_plan });

    let my_plan = LogicalPlan::Extension(Extension { node });
    let plan = LogicalPlanBuilder::from(my_plan)
        .project(vec![
            col("car").alias("my_car"),
            col("speed").alias("my_speed"),
        ])?
        .build()?;
    let unparser =
        Unparser::default().with_extension_unparsers(vec![Arc::new(PlanToSubquery {})]);
    let sql = unparser.plan_to_sql(&plan)?.to_string();
    assert_eq!(
        sql,
        "SELECT \"?table?\".car AS my_car, \"?table?\".speed AS my_speed FROM \
        (SELECT \"?table?\".car, \"?table?\".speed, \"?table?\".\"time\" FROM \"?table?\")",
    );
    Ok(())
}
