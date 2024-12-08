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
use datafusion::{
    assert_batches_eq,
    error::Result,
    prelude::{ParquetReadOptions, SessionContext},
};
use datafusion_common::DFSchema;
use datafusion_expr::{col, lit};
use datafusion_sql::unparser::Unparser;

/// This example demonstrates the programmatic parsing of SQL expressions using
/// the DataFusion [`SessionContext::parse_sql_expr`] API or the [`DataFrame::parse_sql_expr`] API.
///
///
/// The code in this example shows how to:
///
/// 1. [`simple_session_context_parse_sql_expr_demo`]: Parse a simple SQL text into a logical
/// expression using a schema at [`SessionContext`].
///
/// 2. [`simple_dataframe_parse_sql_expr_demo`]: Parse a simple SQL text into a logical expression
/// using a schema at [`DataFrame`].
///
/// 3. [`query_parquet_demo`]: Query a parquet file using the parsed_sql_expr from a DataFrame.
///
/// 4. [`round_trip_parse_sql_expr_demo`]: Parse a SQL text and convert it back to SQL using [`Unparser`].

#[tokio::main]
async fn main() -> Result<()> {
    // See how to evaluate expressions
    simple_session_context_parse_sql_expr_demo()?;
    simple_dataframe_parse_sql_expr_demo().await?;
    query_parquet_demo().await?;
    round_trip_parse_sql_expr_demo().await?;
    Ok(())
}

/// DataFusion can parse a SQL text to a logical expression against a schema at [`SessionContext`].
fn simple_session_context_parse_sql_expr_demo() -> Result<()> {
    let sql = "a < 5 OR a = 8";
    let expr = col("a").lt(lit(5_i64)).or(col("a").eq(lit(8_i64)));

    // provide type information that `a` is an Int32
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let df_schema = DFSchema::try_from(schema).unwrap();
    let ctx = SessionContext::new();

    let parsed_expr = ctx.parse_sql_expr(sql, &df_schema)?;

    assert_eq!(parsed_expr, expr);

    Ok(())
}

/// DataFusion can parse a SQL text to an logical expression using schema at [`DataFrame`].
async fn simple_dataframe_parse_sql_expr_demo() -> Result<()> {
    let sql = "int_col < 5 OR double_col = 8.0";
    let expr = col("int_col")
        .lt(lit(5_i64))
        .or(col("double_col").eq(lit(8.0_f64)));

    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

    let parsed_expr = df.parse_sql_expr(sql)?;

    assert_eq!(parsed_expr, expr);

    Ok(())
}

async fn query_parquet_demo() -> Result<()> {
    let ctx = SessionContext::new();
    let testdata = datafusion::test_util::parquet_test_data();
    let df = ctx
        .read_parquet(
            &format!("{testdata}/alltypes_plain.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;

    let df = df
        .clone()
        .select(vec![
            df.parse_sql_expr("int_col")?,
            df.parse_sql_expr("double_col")?,
        ])?
        .filter(df.parse_sql_expr("int_col < 5 OR double_col = 8.0")?)?
        .aggregate(
            vec![df.parse_sql_expr("double_col")?],
            vec![df.parse_sql_expr("SUM(int_col) as sum_int_col")?],
        )?
        // Directly parsing the SQL text into a sort expression is not supported yet, so
        // construct it programmatically
        .sort(vec![col("double_col").sort(false, false)])?
        .limit(0, Some(1))?;

    let result = df.collect().await?;

    assert_batches_eq!(
        &[
            "+------------+-------------+",
            "| double_col | sum_int_col |",
            "+------------+-------------+",
            "| 10.1       | 4           |",
            "+------------+-------------+",
        ],
        &result
    );

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

    let parsed_expr = df.parse_sql_expr(sql)?;

    let unparser = Unparser::default();
    let round_trip_sql = unparser.expr_to_sql(&parsed_expr)?.to_string();

    assert_eq!(sql, round_trip_sql);

    // enable pretty-unparsing. This make the output more human-readable
    // but can be problematic when passed to other SQL engines due to
    // difference in precedence rules between DataFusion and target engines.
    let unparser = Unparser::default().with_pretty(true);

    let pretty = "int_col < 5 OR double_col = 8";
    let pretty_round_trip_sql = unparser.expr_to_sql(&parsed_expr)?.to_string();
    assert_eq!(pretty, pretty_round_trip_sql);

    Ok(())
}
