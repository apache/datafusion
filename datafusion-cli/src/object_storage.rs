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

use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::CreateExternalTable,
};
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder};
use url::Url;

pub fn get_s3_object_store_builder(
    url: &Url,
    cmd: &CreateExternalTable,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

    if let (Some(access_key_id), Some(secret_access_key)) = (
        cmd.options.get("access_key_id"),
        cmd.options.get("secret_access_key"),
    ) {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
    }

    if let Some(session_token) = cmd.options.get("session_token") {
        builder = builder.with_token(session_token);
    }

    if let Some(region) = cmd.options.get("region") {
        builder = builder.with_region(region);
    }

    Ok(builder)
}

pub fn get_oss_object_store_builder(
    url: &Url,
    cmd: &CreateExternalTable,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env()
        .with_virtual_hosted_style_request(true)
        .with_bucket_name(bucket_name)
        // oss don't care about the "region" field
        .with_region("do_not_care");

    if let (Some(access_key_id), Some(secret_access_key)) = (
        cmd.options.get("access_key_id"),
        cmd.options.get("secret_access_key"),
    ) {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
    }

    if let Some(endpoint) = cmd.options.get("endpoint") {
        builder = builder.with_endpoint(endpoint);
    }

    Ok(builder)
}

pub fn get_gcs_object_store_builder(
    url: &Url,
    cmd: &CreateExternalTable,
) -> Result<GoogleCloudStorageBuilder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);

    if let Some(service_account_path) = cmd.options.get("service_account_path") {
        builder = builder.with_service_account_path(service_account_path);
    }

    if let Some(service_account_key) = cmd.options.get("service_account_key") {
        builder = builder.with_service_account_key(service_account_key);
    }

    if let Some(application_credentials_path) =
        cmd.options.get("application_credentials_path")
    {
        builder = builder.with_application_credentials(application_credentials_path);
    }

    Ok(builder)
}

fn get_bucket_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse bucket name from url: {}",
            url.as_str()
        ))
    })
}

#[cfg(test)]
mod tests {
    use datafusion::{
        datasource::listing::ListingTableUrl, logical_expr::LogicalPlan,
        prelude::SessionContext,
    };

    use super::*;

    #[ignore] // https://github.com/apache/arrow-rs/issues/4021
    #[tokio::test]
    async fn oss_object_store_builder() -> Result<()> {
        let access_key_id = "access_key_id";
        let secret_access_key = "secret_access_key";
        let region = "us-east-2";
        let location = "s3://bucket/path/file.parquet";
        let table_url = ListingTableUrl::parse(location)?;
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'region' '{region}') LOCATION '{location}'");

        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(&sql).await?;

        match &plan {
            LogicalPlan::CreateExternalTable(cmd) => {
                let _builder = get_oss_object_store_builder(table_url.as_ref(), cmd)?;
                // get the actual configuration information, then assert_eq!
            }
            _ => assert!(false),
        }

        Ok(())
    }
}
