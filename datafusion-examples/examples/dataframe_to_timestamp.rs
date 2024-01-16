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

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;

/// This example demonstrates how to use the DataFrame API against in-memory data.
#[tokio::main]
async fn main() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["2020-09-08T13:42:29Z", "2020-09-08T13:42:29.190855-05:00", "2020-08-09 12:13:29", "2020-01-02"])),
            Arc::new(StringArray::from(vec!["2020-09-08T13:42:29Z", "2020-09-08T13:42:29.190855-05:00", "08-09-2020 13/42/29", "09-27-2020 13:42:29-05:30"])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // use to_timestamp function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_timestamp(vec![col("a")]))?;
    // use to_timestamp_seconds function to convert col 'b' to timestamp(Seconds) type using a list of chrono formats to try
    let df = df.with_column("b", to_timestamp_seconds(vec![col("b"), lit("%+"), lit("%d-%m-%Y %H/%M/%S"), lit("%m-%d-%Y %H:%M:%S%#z")]))?;

    let df = df.select_columns(&["a", "b"])?;

    // print the results
    df.show().await?;

    // use sql to convert col 'a' to timestamp using the default parsing
    let df = ctx.sql("select to_timestamp(a) from t").await?;

    // print the results
    df.show().await?;

    // use sql to convert col 'b' to timestamp using a list of chrono formats to try
    let df = ctx.sql("select to_timestamp(b, '%+', '%d-%m-%Y %H/%M/%S', '%m-%d-%Y %H:%M:%S%#z') from t").await?;

    // print the results
    df.show().await?;

    // use sql to convert a static string to a timestamp using a list of chrono formats to try
    let df = ctx.sql("select to_timestamp('01-14-2023 01:01:30+05:30', '%+', '%d-%m-%Y %H/%M/%S', '%m-%d-%Y %H:%M:%S%#z')").await?;

    // print the results
    df.show().await?;

    // use sql to convert a static string to a timestamp using a non-matching chrono format to try
    let result = ctx.sql("select to_timestamp('01-14-2023 01/01/30', '%d-%m-%Y %H:%M:%S')").await?.collect().await;

    if result.is_err() {
        println!("Received the expected error: {:?}", result.err().unwrap());
    }
    else {
        panic!("timestamp parsing with no matching formats should fail")
    }

    Ok(())
}
