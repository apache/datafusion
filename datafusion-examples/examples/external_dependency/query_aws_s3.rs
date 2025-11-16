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
use url::Url;

/// To run this example successfully, upload a Parquet file to
/// your own Amazon S3 bucket and update the path accordingly.
///
/// Make sure your AWS credentials allow `GetObject` access.
///
/// The following environment variables must be defined:
///
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
///
/// If you are using temporary session credentials (e.g. AWS SSO),
/// also set:
///
/// - AWS_SESSION_TOKEN
pub async fn query_aws_s3() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // enter region and bucket to which your credentials have GET access
    let region = "<bucket-region-here>";
    let bucket_name = "<bucket-name-here>";

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket_name)
        .with_region(region)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
        .with_token(env::var("AWS_SESSION_TOKEN").unwrap_or_default())
        .build()?;

    let path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&path).unwrap();
    ctx.register_object_store(&s3_url, Arc::new(s3));

    // point to your own file (must exist)
    let path = format!("s3://{bucket_name}/path/to/test.parquet");
    ctx.register_parquet("test", &path, ParquetReadOptions::default())
        .await?;

    // execute the query
    let df = ctx.sql("SELECT * FROM test LIMIT 10").await?;

    // print the results
    df.show().await?;

    // dynamic query by the file path
    let ctx = ctx.enable_url_table();
    let df = ctx
        .sql(format!(r#"SELECT * FROM '{}' LIMIT 10"#, &path).as_str())
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
