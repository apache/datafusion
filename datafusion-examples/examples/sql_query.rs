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

use datafusion::arrow::array::{UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion_common::exec_datafusion_err;
use object_store::local::LocalFileSystem;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Examples of various ways to execute queries using SQL
///
/// [`query_memtable`]: a simple query against a [`MemTable`]
/// [`query_parquet`]: a simple query against a directory with multiple Parquet files
///
#[tokio::main]
async fn main() -> Result<()> {
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

    let dataframe = ctx.sql("SELECT * FROM users;").await?;

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        assert_eq!(1, record_batch.column(0).len());
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

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
    df.show().await?;

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
    df.show().await?;

    // Reset the current directory
    std::env::set_current_dir(cur_dir)?;

    Ok(())
}
