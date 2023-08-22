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

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{FileType, GetExt};

use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

/// This example demonstrates querying data from AmazonS3 and writing
/// the result of a query back to AmazonS3
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    //enter region and bucket to which your credentials have GET and PUT access
    let region = "<bucket-region-here>";
    let bucket_name = "<bucket-name-here>";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .build()?;

    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    let arc_s3 = Arc::new(s3);
    ctx.runtime_env()
        .register_object_store(&s3_url, arc_s3.clone());

    let path = format!("s3://{bucket_name}/test_data/");
    let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(FileType::PARQUET.get_ext());
    ctx.register_listing_table("test", &path, listing_options, None, None)
        .await?;

    // execute the query
    let df = ctx.sql("SELECT * from test").await?;

    let out_path = format!("s3://{bucket_name}/test_write/");
    df.clone().write_parquet(&out_path, None).await?;

    //write as JSON to s3
    let json_out = format!("s3://{bucket_name}/json_out");
    df.clone().write_json(&json_out).await?;

    //write as csv to s3
    let csv_out = format!("s3://{bucket_name}/csv_out");
    df.write_csv(&csv_out).await?;

    let file_format = ParquetFormat::default().with_enable_pruning(Some(true));
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(FileType::PARQUET.get_ext());
    ctx.register_listing_table("test2", &out_path, listing_options, None, None)
        .await?;

    let df = ctx
        .sql(
            "SELECT * \
        FROM test2 \
        ",
        )
        .await?;

    df.show_limit(20).await?;

    Ok(())
}
