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

use std::path::Path;
use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;

use object_store::local::LocalFileSystem;

/// This example demonstrates executing a simple query against an Arrow data source (a directory
/// with multiple Parquet files) and fetching results. The query is run twice, once showing
/// how to used `register_listing_table` with an absolute path, and once registering an
/// ObjectStore to use a relative path.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        .ok_or("test_data path needs a parent")?;

    std::env::set_current_dir(test_data_path_parent)?;

    let local_fs = Arc::new(LocalFileSystem::default());

    let u = url::Url::parse("file://./")?;
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
