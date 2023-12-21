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
use datafusion_common::{parsers::CompressionTypeVariant, DataFusionError};

/// This example demonstrates the various methods to write out a DataFrame to local storage.
/// See datafusion-examples/examples/external_dependency/dataframe-to-s3.rs for an example
/// using a remote object store.
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();

    let mut df = ctx.sql("values ('a'), ('b'), ('c')").await.unwrap();

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
            DataFrameWriteOptions::new().with_compression(CompressionTypeVariant::GZIP),
            None,
        )
        .await?;

    df.clone()
        .write_json(
            "./datafusion-examples/test_json/",
            DataFrameWriteOptions::new(),
        )
        .await?;

    Ok(())
}
