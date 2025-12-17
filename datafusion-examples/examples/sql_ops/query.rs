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

use datafusion::arrow::array::{UInt8Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{assert_batches_eq, exec_datafusion_err};
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use std::path::Path;
use std::sync::Arc;

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

    let test_data = datafusion::test_util::parquet_test_data();

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        // This is a workaround for this example since `test_data` contains
        // many different parquet different files,
        // in practice use FileType::PARQUET.get_ext().
        .with_file_extension("alltypes_plain.parquet");

    // First example were we use an absolute path, which requires no additional setup.
    ctx.register_listing_table(
        "my_table",
        &format!("file://{test_data}/"),
        listing_options.clone(),
        None,
        None,
    )
    .await
    .unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT * \
        FROM my_table \
        LIMIT 1",
        )
        .await?;

    // print the results
    let results = df.collect().await?;
    assert_batches_eq!(
        [
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        ],
        &results
    );

    // Second example were we temporarily move into the test data's parent directory and
    // simulate a relative path, this requires registering an ObjectStore.
    let cur_dir = std::env::current_dir()?;

    let test_data_path = Path::new(&test_data);
    let test_data_path_parent = test_data_path
        .parent()
        .ok_or(exec_datafusion_err!("test_data path needs a parent"))?;

    std::env::set_current_dir(test_data_path_parent)?;

    let local_fs = Arc::new(LocalFileSystem::default());

    let u = url::Url::parse("file://./")
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    ctx.register_object_store(&u, local_fs);

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "relative_table",
        "./data",
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
        LIMIT 1",
        )
        .await?;

    // print the results
    let results = df.collect().await?;
    assert_batches_eq!(
        [
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        ],
        &results
    );

    // Reset the current directory
    std::env::set_current_dir(cur_dir)?;

    Ok(())
}
