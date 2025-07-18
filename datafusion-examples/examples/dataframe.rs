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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::MemTable;
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::DataFusionError;
use datafusion::common::ScalarValue;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::min_max::max;
use datafusion::prelude::*;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use tempfile::tempdir;

/// This example demonstrates using DataFusion's DataFrame API
///
/// # Reading from different formats
///
/// * [read_parquet]: execute queries against parquet files
/// * [read_csv]: execute queries against csv files
/// * [read_memory]: execute queries against in-memory arrow data
///
/// # Writing out to local storage
///
/// The following examples demonstrate how to write a DataFrame to local
/// storage. See `external_dependency/dataframe-to-s3.rs` for an example writing
/// to a remote object store.
///
/// * [write_out]: write out a DataFrame to a table, parquet file, csv file, or json file
///
/// # Executing subqueries
///
/// * [where_scalar_subquery]: execute a scalar subquery
/// * [where_in_subquery]: execute a subquery with an IN clause
/// * [where_exist_subquery]: execute a subquery with an EXISTS clause
///
/// # Querying data
///
/// * [query_to_date]: execute queries against parquet files
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    // The SessionContext is the main high level API for interacting with DataFusion
    let ctx = SessionContext::new();
    read_parquet(&ctx).await?;
    read_csv(&ctx).await?;
    read_memory(&ctx).await?;
    read_memory_macro().await?;
    write_out(&ctx).await?;
    register_aggregate_test_data("t1", &ctx).await?;
    register_aggregate_test_data("t2", &ctx).await?;
    where_scalar_subquery(&ctx).await?;
    where_in_subquery(&ctx).await?;
    where_exist_subquery(&ctx).await?;
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
        .sql(&format!("SELECT rating, unixtime FROM '{file_path}'"))
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

/// Use the DataFrame API to:
/// 1. Read in-memory data.
async fn read_memory_macro() -> Result<()> {
    // create a DataFrame using macro
    let df = dataframe!(
        "a" => ["a", "b", "c", "d"],
        "b" => [1, 10, 10, 100]
    )?;
    // print the results
    df.show().await?;

    // create empty DataFrame using macro
    let df_empty = dataframe!()?;
    df_empty.show().await?;

    Ok(())
}

/// Use the DataFrame API to:
/// 1. Write out a DataFrame to a table
/// 2. Write out a DataFrame to a parquet file
/// 3. Write out a DataFrame to a csv file
/// 4. Write out a DataFrame to a json file
async fn write_out(ctx: &SessionContext) -> std::result::Result<(), DataFusionError> {
    let array = StringViewArray::from(vec!["a", "b", "c"]);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "tablecol1",
        DataType::Utf8View,
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
    ctx.register_table("initial_data", Arc::new(mem_table))?;
    let df = ctx.table("initial_data").await?;

    ctx.sql(
        "create external table
    test(tablecol1 varchar)
    stored as parquet
    location './datafusion-examples/test_table/'",
    )
    .await?
    .collect()
    .await?;

    // This is equivalent to INSERT INTO test VALUES ('a'), ('b'), ('c').
    // The behavior of write_table depends on the TableProvider's implementation
    // of the insert_into method.
    df.clone()
        .write_table("test", DataFrameWriteOptions::new())
        .await?;

    df.clone()
        .write_parquet(
            "./datafusion-examples/test_parquet/",
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    df.clone()
        .write_csv(
            "./datafusion-examples/test_csv/",
            // DataFrameWriteOptions contains options which control how data is written
            // such as compression codec
            DataFrameWriteOptions::new(),
            Some(CsvOptions::default().with_compression(CompressionTypeVariant::GZIP)),
        )
        .await?;

    df.clone()
        .write_json(
            "./datafusion-examples/test_json/",
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    Ok(())
}

/// Use the DataFrame API to execute the following subquery:
/// select c1,c2 from t1 where (select avg(t2.c2) from t2 where t1.c1 = t2.c1)>0 limit 3;
async fn where_scalar_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(
            scalar_subquery(Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(out_ref_col(DataType::Utf8, "t1.c1").eq(col("t2.c1")))?
                    .aggregate(vec![], vec![avg(col("t2.c2"))])?
                    .select(vec![avg(col("t2.c2"))])?
                    .into_unoptimized_plan(),
            ))
            .gt(lit(0u8)),
        )?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

/// Use the DataFrame API to execute the following subquery:
/// select t1.c1, t1.c2 from t1 where t1.c2 in (select max(t2.c2) from t2 where t2.c1 > 0 ) limit 3;
async fn where_in_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(in_subquery(
            col("t1.c2"),
            Arc::new(
                ctx.table("t2")
                    .await?
                    .filter(col("t2.c1").gt(lit(ScalarValue::UInt8(Some(0)))))?
                    .aggregate(vec![], vec![max(col("t2.c2"))])?
                    .select(vec![max(col("t2.c2"))])?
                    .into_unoptimized_plan(),
            ),
        ))?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

/// Use the DataFrame API to execute the following subquery:
/// select t1.c1, t1.c2 from t1 where exists (select t2.c2 from t2 where t1.c1 = t2.c1) limit 3;
async fn where_exist_subquery(ctx: &SessionContext) -> Result<()> {
    ctx.table("t1")
        .await?
        .filter(exists(Arc::new(
            ctx.table("t2")
                .await?
                .filter(out_ref_col(DataType::Utf8, "t1.c1").eq(col("t2.c1")))?
                .select(vec![col("t2.c2")])?
                .into_unoptimized_plan(),
        )))?
        .select(vec![col("t1.c1"), col("t1.c2")])?
        .limit(0, Some(3))?
        .show()
        .await?;
    Ok(())
}

async fn register_aggregate_test_data(name: &str, ctx: &SessionContext) -> Result<()> {
    let testdata = datafusion::test_util::arrow_test_data();
    ctx.register_csv(
        name,
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::default(),
    )
    .await?;
    Ok(())
}
