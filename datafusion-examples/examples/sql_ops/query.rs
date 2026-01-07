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

//! See `main.rs` for how to run it.

use std::sync::Arc;

use datafusion::arrow::array::{UInt8Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::{assert_batches_eq, exec_datafusion_err};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use datafusion_examples::utils::{datasets::ExampleDataset, write_csv_to_parquet};
use object_store::local::LocalFileSystem;

/// Examples of various ways to execute queries using SQL
///
/// [`query_memtable`]: a simple query against a [`MemTable`]
/// [`query_parquet`]: a simple query against a directory with multiple Parquet files
pub async fn query() -> Result<()> {
    query_memtable().await?;
    query_parquet().await?;
    Ok(())
}

/// Run a simple query against a [`MemTable`]
pub async fn query_memtable() -> Result<()> {
    let mem_table = create_memtable()?;

    // create local execution context
    let ctx = SessionContext::new();

    // Register the in-memory table containing the data
    ctx.register_table("users", Arc::new(mem_table))?;

    // running a SQL query results in a "DataFrame", which can be used
    // to execute the query and collect the results
    let dataframe = ctx.sql("SELECT * FROM users;").await?;

    // Calling 'show' on the dataframe will execute the query and
    // print the results
    dataframe.clone().show().await?;

    // calling 'collect' on the dataframe will execute the query and
    // buffer the results into a vector of RecordBatch. There are other
    // APIs on DataFrame for incrementally generating results (e.g. streaming)
    let result = dataframe.collect().await?;

    // Use the assert_batches_eq macro to compare the results
    assert_batches_eq!(
        [
            "+----+--------------+",
            "| id | bank_account |",
            "+----+--------------+",
            "| 1  | 9000         |",
            "+----+--------------+",
        ],
        &result
    );

    Ok(())
}

fn create_memtable() -> Result<MemTable> {
    MemTable::try_new(get_schema(), vec![vec![create_record_batch()?]])
}

fn create_record_batch() -> Result<RecordBatch> {
    let id_array = UInt8Array::from(vec![1]);
    let account_array = UInt64Array::from(vec![9000]);

    Ok(RecordBatch::try_new(
        get_schema(),
        vec![Arc::new(id_array), Arc::new(account_array)],
    )
    .unwrap())
}

fn get_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("bank_account", DataType::UInt64, true),
    ]))
}

/// The simplest way to query parquet files is to use the
/// [`SessionContext::read_parquet`] API
///
/// For more control, you can use the lower level [`ListingOptions`] and
/// [`ListingTable`] APIS
///
/// This example shows how to use relative and absolute paths.
///
/// [`ListingTable`]: datafusion::datasource::listing::ListingTable
async fn query_parquet() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // Convert the CSV input into a temporary Parquet directory for querying
    let dataset = ExampleDataset::Cars;
    let parquet_temp = write_csv_to_parquet(&ctx, &dataset.path()).await?;

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    let table_path = parquet_temp.file_uri()?;

    // First example were we use an absolute path, which requires no additional setup.
    ctx.register_listing_table(
        "my_table",
        &table_path,
        listing_options.clone(),
        None,
        None,
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT * \
        FROM my_table \
        ORDER BY speed \
        LIMIT 1",
        )
        .await?;

    // print the results
    let results = df.collect().await?;
    assert_batches_eq!(
        [
            "+-----+-------+---------------------+",
            "| car | speed | time                |",
            "+-----+-------+---------------------+",
            "| red | 0.0   | 1996-04-12T12:05:15 |",
            "+-----+-------+---------------------+",
        ],
        &results
    );

    // Second example where we change the current working directory and explicitly
    // register a local filesystem object store. This demonstrates how listing tables
    // resolve paths via an ObjectStore, even when using filesystem-backed data.
    let cur_dir = std::env::current_dir()?;
    let test_data_path_parent = parquet_temp
        .tmp_dir
        .path()
        .parent()
        .ok_or(exec_datafusion_err!("test_data path needs a parent"))?;

    std::env::set_current_dir(test_data_path_parent)?;

    let local_fs = Arc::new(LocalFileSystem::default());

    let url = url::Url::parse("file://./")
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    ctx.register_object_store(&url, local_fs);

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "relative_table",
        parquet_temp.path_str()?,
        listing_options.clone(),
        None,
        None,
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT * \
        FROM relative_table \
        ORDER BY speed \
        LIMIT 1",
        )
        .await?;

    // print the results
    let results = df.collect().await?;
    assert_batches_eq!(
        [
            "+-----+-------+---------------------+",
            "| car | speed | time                |",
            "+-----+-------+---------------------+",
            "| red | 0.0   | 1996-04-12T12:05:15 |",
            "+-----+-------+---------------------+",
        ],
        &results
    );

    // Reset the current directory
    std::env::set_current_dir(cur_dir)?;

    Ok(())
}
