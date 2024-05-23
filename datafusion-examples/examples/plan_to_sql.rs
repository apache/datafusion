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

/// This example demonstrates the programmatic construction of SQL strings using
/// the DataFusion Expr [`Expr`] and LogicalPlan [`LogicalPlan`] API.
///
///
/// The code in this example shows how to:
///
/// 1. [`simple_expr_to_sql_demo`]: Create a simple expression [`Exprs`] with
/// fluent API and convert to sql suitable for passing to another database
///
/// 2. [`simple_expr_to_sql_demo_no_escape`]  Create a simple expression
/// [`Exprs`] with fluent API and convert to sql without escaping column names
/// more suitable for displaying to humans.
///
/// 3. [`simple_expr_to_sql_demo_escape_mysql_style`]" Create a simple
/// expression [`Exprs`] with fluent API and convert to sql escaping column
/// names in MySQL style.
///
/// 4. [`simple_plan_to_sql_demo`]: Create a simple logical plan using the
/// DataFrames API and convert to sql string.
///
/// 5. [`round_trip_plan_to_sql_demo`]: Create a logical plan from a SQL string, modify it using the
/// DataFrames API and convert it back to a  sql string.

#[tokio::main]
async fn main() -> Result<()> {
    // See how to evaluate expressions
    simple_expr_to_sql_demo()?;
    simple_expr_to_sql_demo_escape_mysql_style()?;
    simple_plan_to_sql_demo().await?;
    round_trip_plan_to_sql_demo().await?;
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

/// DataFusion can convert expressions to SQL without escaping column names using
/// using a custom dialect and an explicit unparser
fn simple_expr_to_sql_demo_escape_mysql_style() -> Result<()> {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let dialect = CustomDialect::new(Some('`'));
    let unparser = Unparser::new(&dialect);
    let sql = unparser.expr_to_sql(&expr)?.to_string();
    assert_eq!(sql, r#"((`a` < 5) OR (`a` = 8))"#);
    Ok(())
}

/// DataFusion can convert a logic plan created using the DataFrames API to read from a parquet file
/// to SQL, using column name escaping PostgreSQL style.
async fn simple_plan_to_sql_demo() -> Result<()> {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?
        .select_columns(&["id", "int_col", "double_col", "date_string_col"])?;

    // Convert the data frame to a SQL string
    let sql = plan_to_sql(df.logical_plan())?.to_string();

    assert_eq!(
        sql,
        r#"SELECT "?table?".id, "?table?".int_col, "?table?".double_col, "?table?".date_string_col FROM "?table?""#
    );

    Ok(())
}

/// DataFusion can also be used to parse SQL, programmatically modify the query
/// (in this case adding a filter) and then and converting back to SQL.
async fn round_trip_plan_to_sql_demo() -> Result<()> {
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // create a logical plan from a SQL string and then programmatically add new filters
    let df = ctx
        // Use SQL to read some data from the parquet file
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain",
        )
        .await?
        // Add id > 1 and tinyint_col < double_col filter
        .filter(
            col("id")
                .gt(lit(1))
                .and(col("tinyint_col").lt(col("double_col"))),
        )?;

    let sql = plan_to_sql(df.logical_plan())?.to_string();
    assert_eq!(
        sql,
        r#"SELECT alltypes_plain.int_col, alltypes_plain.double_col, CAST(alltypes_plain.date_string_col AS VARCHAR) FROM alltypes_plain WHERE ((alltypes_plain.id > 1) AND (alltypes_plain.tinyint_col < alltypes_plain.double_col))"#
    );

    Ok(())
}
