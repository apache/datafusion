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

use datafusion::{error::Result, prelude::{ParquetReadOptions, SessionContext}};
use datafusion_common::DFSchema;
use datafusion_expr::{col, lit};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_sql::unparser::Unparser;

#[tokio::main]
async fn main() -> Result<()> {
    // See how to evaluate expressions
    simple_session_context_parse_sql_expr_demo().await?;
    simple_dataframe_parse_sql_expr_demo().await?;
    round_trip_parse_sql_expr_demo().await?;
    Ok(())
}

/// DataFusion can parse a SQL text to an logical expression agianst a schema at [`SessionContext`].
async fn simple_session_context_parse_sql_expr_demo() -> Result<()> {
    let sql = "a < 5 OR a = 8";
    let expr = col("a").lt(lit(5_i64)).or(col("a").eq(lit(8_i64)));
    
    
    // provide type information that `a` is an Int32
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let df_schema = DFSchema::try_from(schema).unwrap();
    let ctx = SessionContext::new();

    let parsed_expr = ctx.parse_sql_expr(sql, &df_schema).await?;

    assert_eq!(parsed_expr, expr);

    Ok(())
}

/// DataFusion can parse a SQL text to an logical expression using schema at [`DataFrame`].
async fn simple_dataframe_parse_sql_expr_demo() -> Result<()> {
    let sql = "int_col < 5 OR double_col = 8.0";
    let expr = col("int_col").lt(lit(5_i64)).or(col("double_col").eq(lit(8.0_f64)));
    
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

    let parsed_expr = df.parse_sql_expr(sql).await?;

    assert_eq!(parsed_expr, expr);

    Ok(())
}

/// DataFusion can parse a SQL text and convert it back to SQL using [`Unparser`].
async fn round_trip_parse_sql_expr_demo() -> Result<()> {
    let sql = "((int_col < 5) OR (double_col = 8))";
    
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

    let parsed_expr = df.parse_sql_expr(sql).await?;

    let unparser = Unparser::default();
    let round_trip_sql = unparser.expr_to_sql(&parsed_expr)?.to_string();

    assert_eq!(sql, round_trip_sql);

    Ok(())
}
