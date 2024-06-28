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

//! This file contains several examples of how to run queries using DataFusion's
//! DataFrame API:
//!
//! * [`parquet`]: query a single Parquet file
//! * [`to_date_demo`]: use the `to_date` function to convert dates to strings
//! * [`to_timestamp_demo`]: use the `to_timestamp` function to convert strings to timestamps
//! * [`make_date_demo`]: use the `make_date` function to create dates from year, month, and day

use arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<()> {
    parquet().await?;
    to_date_demo().await?;
    to_timestamp_demo().await?;
    make_date_demo().await?;

    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (Parquet) and fetching results, using the DataFrame trait

async fn parquet() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    let filename = &format!("{testdata}/alltypes_plain.parquet");

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;

    // create a csv file waiting to be written
    let dir = tempdir()?;
    let file_path = dir.path().join("example.csv");
    let file = File::create(&file_path)?;
    write_csv_file(file);

    // Reading CSV file with inferred schema example
    let csv_df =
        example_read_csv_file_with_inferred_schema(file_path.to_str().unwrap()).await;
    csv_df.show().await?;

    // Reading CSV file with defined schema
    let csv_df = example_read_csv_file_with_schema(file_path.to_str().unwrap()).await;
    csv_df.show().await?;

    // Reading PARQUET file and print describe
    let parquet_df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?;
    parquet_df.describe().await.unwrap().show().await?;

    Ok(())
}

// Function to create an test CSV file
fn write_csv_file(mut file: File) {
    // Create the data to put into the csv file with headers
    let content = r#"id,time,vote,unixtime,rating
a1,"10 6, 2013",3,1381017600,5.0
a2,"08 9, 2013",2,1376006400,4.5"#;
    // write the data
    file.write_all(content.as_ref())
        .expect("Problem with writing file!");
}

// Example to read data from a csv file with inferred schema
async fn example_read_csv_file_with_inferred_schema(file_path: &str) -> DataFrame {
    // Create a session context
    let ctx = SessionContext::new();
    // Register a lazy DataFrame using the context
    ctx.read_csv(file_path, CsvReadOptions::default())
        .await
        .unwrap()
}

// Example to read csv file with a defined schema for the csv file
async fn example_read_csv_file_with_schema(file_path: &str) -> DataFrame {
    // Create a session context
    let ctx = SessionContext::new();
    // Define the schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("vote", DataType::Int32, true),
        Field::new("unixtime", DataType::Int64, false),
        Field::new("rating", DataType::Float32, true),
    ]);
    // Create a csv option provider with the desired schema
    let csv_read_option = CsvReadOptions {
        // Update the option provider with the defined schema
        schema: Some(&schema),
        ..Default::default()
    };
    // Register a lazy DataFrame by using the context and option provider
    ctx.read_csv(file_path, csv_read_option).await.unwrap()
}

/// This example demonstrates how to use the to_date series
/// of functions in the DataFrame API
async fn to_date_demo() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            "2020-09-08T13:42:29Z",
            "2020-09-08T13:42:29.190855-05:00",
            "2020-08-09 12:13:29",
            "2020-01-02",
        ]))],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // use to_date function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_date(vec![col("a")]))?;

    let df = df.select_columns(&["a"])?;

    // print the results
    df.show().await?;

    Ok(())
}

/// This example demonstrates how to use the to_timestamp series
/// of functions in the DataFrame API
async fn to_timestamp_demo() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "2020-09-08T13:42:29Z",
                "2020-09-08T13:42:29.190855-05:00",
                "2020-08-09 12:13:29",
                "2020-01-02",
            ])),
            Arc::new(StringArray::from(vec![
                "2020-09-08T13:42:29Z",
                "2020-09-08T13:42:29.190855-05:00",
                "08-09-2020 13/42/29",
                "09-27-2020 13:42:29-05:30",
            ])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // use to_timestamp function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_timestamp(vec![col("a")]))?;
    // use to_timestamp_seconds function to convert col 'b' to timestamp(Seconds) type using a list
    // of chrono formats (https://docs.rs/chrono/latest/chrono/format/strftime/index.html) to try
    let df = df.with_column(
        "b",
        to_timestamp_seconds(vec![
            col("b"),
            lit("%+"),
            lit("%d-%m-%Y %H/%M/%S"),
            lit("%m-%d-%Y %H:%M:%S%#z"),
        ]),
    )?;

    let df = df.select_columns(&["a", "b"])?;

    // print the results
    df.show().await?;

    Ok(())
}

/// This example demonstrates how to use the make_date
/// function in the DataFrame API as well as via sql.
async fn make_date_demo() -> Result<()> {
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
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // use make_date function to convert col 'y', 'm' & 'd' to a date
    let df = df.with_column("a", make_date(col("y"), col("m"), col("d")))?;
    // use make_date function to convert col 'y' & 'm' with a static day to a date
    let df = df.with_column("b", make_date(col("y"), col("m"), lit(22)))?;

    let df = df.select_columns(&["a", "b"])?;

    // print the results
    df.show().await?;

    Ok(())
}
