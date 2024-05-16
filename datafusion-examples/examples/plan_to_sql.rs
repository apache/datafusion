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
use datafusion_sql::unparser::Unparser;

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
