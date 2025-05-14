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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_common::DFSchema;
use datafusion_common::{DFSchemaRef, Result, ToDFSchema};
use datafusion_expr::col;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_sql::unparser::Unparser;
/// A schema like:
///
/// a: Int32 (possibly with nulls)
/// b: Int32
/// s: Float32
fn schema() -> DFSchemaRef {
    Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Float32, false),
    ])
    .to_dfschema_ref()
    .unwrap()
}

#[tokio::test]
async fn round_trip_parse_sql_expr() -> Result<()> {
    let tests = vec![
        "(a = 10)",
        "((a = 10) AND (b <> 20))",
        "((a = 10) OR (b <> 20))",
        "(((a = 10) AND (b <> 20)) OR (c = a))",
        "((a = 10) AND b IN (20, 30))",
        "((a = 10) AND b NOT IN (20, 30))",
        "sum(a)",
        "(sum(a) + 1)",
        "(min(a) + max(b))",
        "(min(a) + (max(b) * sum(c)))",
        "(min(a) + ((max(b) * sum(c)) / 10))",
    ];

    for test in tests {
        round_trip_session_context(test)?;
        round_trip_dataframe(test).await?;
    }

    Ok(())
}

fn round_trip_session_context(sql: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let df_schema = schema();
    let expr = ctx.parse_sql_expr(sql, &df_schema)?;
    let sql2 = unparse_sql_expr(&expr)?;
    assert_eq!(sql, sql2);

    Ok(())
}

async fn round_trip_dataframe(sql: &str) -> Result<()> {
    let ctx = SessionContext::new();
    let df = ctx
        .read_csv(
            &"tests/data/example.csv".to_string(),
            CsvReadOptions::default(),
        )
        .await?;
    let expr = df.parse_sql_expr(sql)?;
    let sql2 = unparse_sql_expr(&expr)?;
    assert_eq!(sql, sql2);

    Ok(())
}

#[tokio::test]
async fn roundtrip_qualified_schema() -> Result<()> {
    let sql = "a < 5 OR a = 8";
    let expr = col("t.a").lt(lit(5_i64)).or(col("t.a").eq(lit(8_i64)));
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let df_schema = DFSchema::try_from_qualified_schema("t", &schema).unwrap();
    let ctx = SessionContext::new();
    let parsed_expr = ctx.parse_sql_expr(sql, &df_schema)?;
    assert_eq!(parsed_expr, expr);
    Ok(())
}

fn unparse_sql_expr(expr: &Expr) -> Result<String> {
    let unparser = Unparser::default();

    let round_trip_sql = unparser.expr_to_sql(expr)?.to_string();
    Ok(round_trip_sql)
}
