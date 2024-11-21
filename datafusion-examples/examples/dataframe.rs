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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::tempdir;

/// This example demonstrates using DataFusion's DataFrame API to
///
/// * [read_parquet]: execute queries against parquet files
/// * [read_csv]: execute queries against csv files
/// * [read_memory]: execute queries against in-memory arrow data
#[tokio::main]
async fn main() -> Result<()> {
    // The SessionContext is the main high level API for interacting with DataFusion
    let ctx = SessionContext::new();
    read_parquet(&ctx).await?;
    read_csv(&ctx).await?;
    read_memory(&ctx).await?;
    Ok(())
}

/// Use DataFrame API to
/// 1. Read parquet files,
/// 2. Show the schema
/// 3. Select columns and rows
async fn read_parquet(ctx: &SessionContext) -> Result<()> {
    // Find the local path of "alltypes_plain.parquet"
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = &format!("{testdata}/alltypes_plain.parquet");

    // Read the parquet files and show its schema using 'describe'
    let parquet_df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?;

    // show its schema using 'describe'
    parquet_df.clone().describe().await?.show().await?;

    // Select three columns and filter the results
    // so that only rows where id > 1 are returned
    parquet_df
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?
        .show()
        .await?;

    Ok(())
}

/// Use the DataFrame API to
/// 1. Read CSV files
/// 2. Optionally specify schema
async fn read_csv(ctx: &SessionContext) -> Result<()> {
    // create example.csv file in a temporary directory
    let dir = tempdir()?;
    let file_path = dir.path().join("example.csv");
    {
        let mut file = File::create(&file_path)?;
        // write CSV data
        file.write_all(
            r#"id,time,vote,unixtime,rating
    a1,"10 6, 2013",3,1381017600,5.0
    a2,"08 9, 2013",2,1376006400,4.5"#
                .as_bytes(),
        )?;
    } // scope closes the file
    let file_path = file_path.to_str().unwrap();

    // You can read a CSV file and DataFusion will infer the schema automatically
    let csv_df = ctx.read_csv(file_path, CsvReadOptions::default()).await?;
    csv_df.show().await?;

    // If you know the types of your data you can specify them explicitly
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
    let csv_df = ctx.read_csv(file_path, csv_read_option).await?;
    csv_df.show().await?;

    // You can also create DataFrames from the result of sql queries
    // and using the `enable_url_table` refer to local files directly
    let dyn_ctx = ctx.clone().enable_url_table();
    let csv_df = dyn_ctx
        .sql(&format!("SELECT rating, unixtime FROM '{}'", file_path))
        .await?;
    csv_df.show().await?;

    Ok(())
}

/// Use the DataFrame API to:
/// 1. Read in-memory data.
async fn read_memory(ctx: &SessionContext) -> Result<()> {
    // define data in memory
    let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 10, 10, 100]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

    // declare a table in memory. In Apache Spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // construct an expression corresponding to "SELECT a, b FROM t WHERE b = 10" in SQL
    let filter = col("b").eq(lit(10));
    let df = df.select_columns(&["a", "b"])?.filter(filter)?;

    // print the results
    df.show().await?;

    Ok(())
}
