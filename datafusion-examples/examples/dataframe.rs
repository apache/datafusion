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

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
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

    let dyn_ctx = ctx.enable_url_table();
    let df = dyn_ctx
        .sql(&format!("SELECT * FROM '{}'", file_path.to_str().unwrap()))
        .await?;
    df.show().await?;

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
