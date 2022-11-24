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
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use std::fs;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    let filename = &format!("{}/alltypes_plain.parquet", testdata);

    // define the query using the DataFrame trait
    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    // print the results
    df.show().await?;

    Ok(())
}

// Example to read data from a csv file with inferred schema
async fn example_read_csv_file_with_inferred_schema() -> Arc<DataFrame> {
    let path = "example.csv";
    // Create the data to put into the csv file with headers
    let content = "id,time,vote,unixtime,rating\na1,\"10 6, 2013\",3,1381017600,5.0\na2,\"08 9, 2013\",2,1376006400,4.5";
    // write the data
    fs::write(path, content).expect("Problem with writing file!");
    // Create a session context and create a lazy
    let ctx = SessionContext::new();
    let df = ctx.read_csv(path, CsvReadOptions::default()).await.unwrap();
    df
}

// Example to read csv file with a given csv file
async fn example_read_csv_file_with_schema() -> Arc<DataFrame>{
    let path = "example.csv";
    // Create the data to put into the csv file with headers
    let content = "id,time,vote,unixtime,rating\na1,\"10 6, 2013\",3,1381017600,5.0\na2,\"08 9, 2013\",2,1376006400,4.5";
    // write the data
    fs::write(path, content).expect("Problem with writing file!");
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("vote", DataType::Int32, true),
        Field::new("unixtime", DataType::Int64, false),
        Field::new("rating", DataType::Float32, true),
    ]);
    let mut csv_read_option = CsvReadOptions::default();
    csv_read_option.schema = Some(&schema);
    let df = ctx.read_csv(path, csv_read_option).await.unwrap();
    df
}
