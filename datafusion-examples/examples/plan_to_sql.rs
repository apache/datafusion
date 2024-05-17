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

use datafusion::error::Result;

use datafusion::prelude::*;
use datafusion::sql::unparser::expr_to_sql;
use datafusion_sql::unparser::dialect::CustomDialect;
use datafusion_sql::unparser::{plan_to_sql, Unparser};

/// This example demonstrates the programmatic construction of
/// SQL using the DataFusion Expr [`Expr`] and LogicalPlan [`LogicalPlan`] API.
///
///
/// The code in this example shows how to:
/// 1. Create SQL from a variety of Expr and LogicalPlan: [`main`]`
/// 2. Create a simple expression [`Exprs`] with fluent API
/// and convert to sql: [`simple_expr_to_sql_demo`]
/// 3. Create a simple expression [`Exprs`] with fluent API
/// and convert to sql without escaping column names: [`simple_expr_to_sql_demo_no_escape`]
/// 4. Create a simple expression [`Exprs`] with fluent API
/// and convert to sql escaping column names a MySQL style: [`simple_expr_to_sql_demo_escape_mysql_style`]

#[tokio::main]
async fn main() -> Result<()> {
    // See how to evaluate expressions
    simple_expr_to_sql_demo()?;
    simple_expr_to_sql_demo_no_escape()?;
    simple_expr_to_sql_demo_escape_mysql_style()?;
    simple_plan_to_sql_parquest_dataframe_demo().await?;
    simple_plan_to_sql_csv_dataframe_demo().await?;
    round_trip_plan_to_sql_parquest_dataframe_demo().await?;
    round_trip_plan_to_sql_csv_dataframe_demo().await?;
    Ok(())
}

/// DataFusion can convert expressions to SQL, using column name escaping
/// PostgreSQL style.
fn simple_expr_to_sql_demo() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let ast = expr_to_sql(&expr)?;
    let sql = format!("{}", ast);
    assert_eq!(sql, r#"(("a" < 5) OR ("a" = 8))"#);
    Ok(())
}

/// DataFusion can convert expressions to SQL without escaping column names using
/// using a custom dialect and an explicit unparser
fn simple_expr_to_sql_demo_no_escape() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let dialect = CustomDialect::new(None);
    let unparser = Unparser::new(&dialect);
    let ast = unparser.expr_to_sql(&expr)?;
    let sql = format!("{}", ast);
    assert_eq!(sql, r#"((a < 5) OR (a = 8))"#);
    Ok(())
}

/// DataFusion can convert expressions to SQL without escaping column names using
/// using a custom dialect and an explicit unparser
fn simple_expr_to_sql_demo_escape_mysql_style() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let dialect = CustomDialect::new(Some('`'));
    let unparser = Unparser::new(&dialect);
    let ast = unparser.expr_to_sql(&expr)?;
    let sql = format!("{}", ast);
    assert_eq!(sql, r#"((`a` < 5) OR (`a` = 8))"#);
    Ok(())
}

/// DataFusion can convert a logic plan created using the DataFrames API to read from a parquet file
/// to SQL, using column name escaping PostgreSQL style.
async fn simple_plan_to_sql_parquest_dataframe_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new(); // define the query using the DataFrame trait

    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?
        .select_columns(&["id", "int_col", "double_col", "date_string_col"])?;

    let ast = plan_to_sql(&df.logical_plan())?;

    let sql = format!("{}", ast);

    assert_eq!(
        sql,
        r#"SELECT "?table?"."id", "?table?"."int_col", "?table?"."double_col", "?table?"."date_string_col" FROM "?table?""#
    );

    Ok(())
}

/// DataFusion can convert a logic plan created using the DataFrames API to read from a csv file
/// to SQL, using column name escaping PostgreSQL style.
async fn simple_plan_to_sql_csv_dataframe_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new(); // define the query using the DataFrame trait

    let testdata = datafusion::test_util::arrow_test_data();
    let df = ctx
        .read_csv(
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::default(),
        )
        .await?
        .select(vec![col("c1"), min(col("c12")), max(col("c12"))])?;

    let ast = plan_to_sql(&df.logical_plan())?;

    let sql = format!("{}", ast);

    assert_eq!(
        sql,
        r#"SELECT "?table?"."c1", MIN("?table?"."c12"), MAX("?table?"."c12") FROM "?table?""#
    );

    Ok(())
}

async fn round_trip_plan_to_sql_parquest_dataframe_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new(); // define the query using the DataFrame trait

    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain",
        )
        .await?
        .filter(
            col("id")
                .gt(lit(1))
                .and(col("tinyint_col").lt(col("double_col"))),
        )?;

    let ast = plan_to_sql(&df.logical_plan())?;

    let sql = format!("{}", ast);

    assert_eq!(
        sql,
        r#"SELECT "alltypes_plain"."int_col", "alltypes_plain"."double_col", CAST("alltypes_plain"."date_string_col" AS VARCHAR) FROM "alltypes_plain" WHERE (("alltypes_plain"."id" > 1) AND ("alltypes_plain"."tinyint_col" < "alltypes_plain"."double_col"))"#
    );

    Ok(())
}

async fn round_trip_plan_to_sql_csv_dataframe_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new(); // define the query using the DataFrame trait

    let testdata = datafusion::test_util::arrow_test_data();

    // register parquet file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT c1, MIN(c12), MAX(c12) \
        FROM aggregate_test_100
        GROUP BY c1",
        )
        .await?
        .filter(col("c1").gt(lit(0.1)).and(col("c1").lt(lit(0.9))))?;

    let ast = plan_to_sql(&df.logical_plan())?;

    let sql = format!("{}", ast);

    assert_eq!(
        sql,
        r#"SELECT "aggregate_test_100"."c1", MIN("aggregate_test_100"."c12"), MAX("aggregate_test_100"."c12") FROM "aggregate_test_100" GROUP BY "aggregate_test_100"."c1" HAVING (("aggregate_test_100"."c1" > 0.1) AND ("aggregate_test_100"."c1" < 0.9))"#
    );

    Ok(())
}
