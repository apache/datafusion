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

use datafusion::{dataframe::DataFrameWriteOptions, prelude::*};
use datafusion_common::DataFusionError;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use url::Url;

/// This example demonstrates the various methods to write out a DataFrame
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();
    let local = Arc::new(LocalFileSystem::new_with_prefix("./").unwrap());
    let local_url = Url::parse("file://local").unwrap();
    ctx.runtime_env().register_object_store(&local_url, local);

    let mut df = ctx.sql(
        "values ('a'), ('b'), ('c')"
    ).await
    .unwrap();

    // Ensure the column names and types match the target table
    df = df.with_column_renamed("column1", "tablecol1").unwrap();

    ctx.sql(
        "create external table 
    test(tablecol1 varchar)
    stored as parquet 
    location './datafusion-examples/test_table/'",
    )
    .await?
    .collect()
    .await?;

    df.clone()
        .write_table("test", DataFrameWriteOptions::new())
        .await?;

    df.clone()
        .write_parquet("file://local/datafusion-examples/test_parquet/", 
        DataFrameWriteOptions::new(),
        None,
    )
        .await?;

    df.clone()
        .write_csv("file://local/datafusion-examples/test_csv/", 
        DataFrameWriteOptions::new(),
        None,
    )
        .await?;

    df.clone()
        .write_json("file://local/datafusion-examples/test_json/", 
        DataFrameWriteOptions::new(),
    )
        .await?;

    Ok(())
}
