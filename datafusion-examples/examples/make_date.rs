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

use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::assert_contains;

/// This example demonstrates how to use the make_date
/// function in the DataFrame API as well as via sql.
#[tokio::main]
async fn main() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("y", DataType::Int32, false),
        Field::new("m", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![2020, 2021, 2022, 2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![15, 16, 17, 18, 19])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch).await?;
    let df = ctx.table("t").await?;

    // use make_date function to convert col 'y', 'm' & 'd' to a date
    let df = df.with_column("a", make_date(col("y"), col("m"), col("d")))?;
    // use make_date function to convert col 'y' & 'm' with a static day to a date
    let df = df.with_column("b", make_date(col("y"), col("m"), lit(22)))?;

    let df = df.select_columns(&["a", "b"])?;

    // print the results
    df.show().await?;

    // use sql to convert col 'y', 'm' & 'd' to a date
    let df = ctx.sql("select make_date(y, m, d) from t").await?;

    // print the results
    df.show().await?;

    // use sql to convert col 'y' & 'm' with a static string day to a date
    let df = ctx.sql("select make_date(y, m, '22') from t").await?;

    // print the results
    df.show().await?;

    // math expressions work
    let df = ctx.sql("select make_date(y + 1, m, d) from t").await?;

    // print the results
    df.show().await?;

    // you can cast to supported types (int, bigint, varchar) if required
    let df = ctx
        .sql("select make_date(2024::bigint, 01::bigint, 27::varchar(3))")
        .await?;

    // print the results
    df.show().await?;

    // arrow casts also work
    let df = ctx
        .sql("select make_date(arrow_cast(2024, 'Int64'), arrow_cast(1, 'Int64'), arrow_cast(27, 'Int64'))")
        .await?;

    // print the results
    df.show().await?;

    // invalid column values will result in an error
    let result = ctx
        .sql("select make_date(2024, null, 23)")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Unable to parse date from null/empty value";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // invalid date values will also result in an error
    let result = ctx
        .sql("select make_date(2024, 01, 32)")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Unable to parse date from 2024, 1, 32";
    assert_contains!(result.unwrap_err().to_string(), expected);

    Ok(())
}
