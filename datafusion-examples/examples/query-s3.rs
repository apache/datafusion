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
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;

/// This example demonstrates querying data in an S3 bucket.
///
/// The following environment variables must be defined:
///
/// - AWS_DEFAULT_REGION
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
///
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let bucket_name = "nyc-tlc";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(env::var("AWS_DEFAULT_REGION").unwrap())
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .build()?;

    ctx.runtime_env()
        .register_object_store("s3", bucket_name, Arc::new(s3));

    ctx.register_parquet(
        "trips",
        &format!(
            "s3://{}/trip data/yellow_tripdata_2022-06.parquet",
            bucket_name
        ),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx.sql("SELECT * FROM trips LIMIT 10").await?;

    // print the results
    df.show().await?;

    Ok(())
}
