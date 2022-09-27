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
    // read AWS configs from the environment
    let config = SessionConfig::from_env();

    let ctx = SessionContext::with_config(config);

    ctx.register_parquet(
        "trips",
        "s3://nyc-tlc/trip data/yellow_tripdata_2022-06.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx.sql("SELECT * FROM trips LIMIT 10").await?;

    // print the results
    df.show().await?;

    Ok(())
}
