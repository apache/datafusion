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

use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::CreateExternalTable,
};
use object_store::aws::AwsCredential;
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, CredentialProvider,
};
use std::sync::Arc;
use url::Url;

pub async fn get_s3_object_store_builder(
    url: &Url,
    cmd: &mut CreateExternalTable,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

    if let (Some(access_key_id), Some(secret_access_key)) = (
        cmd.options.remove("access_key_id"),
        cmd.options.remove("secret_access_key"),
    ) {
        println!("removing secret access key!");
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        if let Some(session_token) = cmd.options.remove("session_token") {
            builder = builder.with_token(session_token);
        }
    } else {
        let config = aws_config::from_env().load().await;
        if let Some(region) = config.region() {
            builder = builder.with_region(region.to_string());
        }

        let credentials = config
            .credentials_provider()
            .ok_or_else(|| {
                DataFusionError::ObjectStore(object_store::Error::Generic {
                    store: "S3",
                    source: "Failed to get S3 credentials from environment".into(),
                })
            })?
            .clone();

        let credentials = Arc::new(S3CredentialProvider { credentials });
        builder = builder.with_credentials(credentials);
    }

    if let Some(region) = cmd.options.remove("region") {
        builder = builder.with_region(region);
    }

    Ok(builder)
}

#[derive(Debug)]
struct S3CredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds = self.credentials.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(ToString::to_string),
        }))
    }
}

pub fn get_oss_object_store_builder(
    url: &Url,
    cmd: &mut CreateExternalTable,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env()
        .with_virtual_hosted_style_request(true)
        .with_bucket_name(bucket_name)
        // oss don't care about the "region" field
        .with_region("do_not_care");

    if let (Some(access_key_id), Some(secret_access_key)) = (
        cmd.options.remove("access_key_id"),
        cmd.options.remove("secret_access_key"),
    ) {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
    }

    if let Some(endpoint) = cmd.options.remove("endpoint") {
        builder = builder.with_endpoint(endpoint);
    }

    Ok(builder)
}

pub fn get_gcs_object_store_builder(
    url: &Url,
    cmd: &mut CreateExternalTable,
) -> Result<GoogleCloudStorageBuilder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);

    if let Some(service_account_path) = cmd.options.remove("service_account_path") {
        builder = builder.with_service_account_path(service_account_path);
    }

    if let Some(service_account_key) = cmd.options.remove("service_account_key") {
        builder = builder.with_service_account_key(service_account_key);
    }

    if let Some(application_credentials_path) =
        cmd.options.remove("application_credentials_path")
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
    use super::*;
    use datafusion::common::plan_err;
    use datafusion::{
        datasource::listing::ListingTableUrl,
        logical_expr::{DdlStatement, LogicalPlan},
        prelude::SessionContext,
    };
    use object_store::{aws::AmazonS3ConfigKey, gcp::GoogleConfigKey};

    #[tokio::test]
    async fn s3_object_store_builder() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'region' '{region}', 'session_token' {session_token}) LOCATION '{location}'");

        let ctx = SessionContext::new();
        let mut plan = ctx.state().create_logical_plan(&sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &mut plan {
            let builder = get_s3_object_store_builder(table_url.as_ref(), cmd).await?;
            // get the actual configuration information, then assert_eq!
            let config = [
                (AmazonS3ConfigKey::AccessKeyId, access_key_id),
                (AmazonS3ConfigKey::SecretAccessKey, secret_access_key),
                (AmazonS3ConfigKey::Region, region),
                (AmazonS3ConfigKey::Token, session_token),
            ];
            for (key, value) in config {
                assert_eq!(value, builder.get_config_value(&key).unwrap());
            }
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        Ok(())
    }

    #[tokio::test]
    async fn oss_object_store_builder() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'endpoint' '{endpoint}') LOCATION '{location}'");

        let ctx = SessionContext::new();
        let mut plan = ctx.state().create_logical_plan(&sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &mut plan {
            let builder = get_oss_object_store_builder(table_url.as_ref(), cmd)?;
            // get the actual configuration information, then assert_eq!
            let config = [
                (AmazonS3ConfigKey::AccessKeyId, access_key_id),
                (AmazonS3ConfigKey::SecretAccessKey, secret_access_key),
                (AmazonS3ConfigKey::Endpoint, endpoint),
            ];
            for (key, value) in config {
                assert_eq!(value, builder.get_config_value(&key).unwrap());
            }
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        Ok(())
    }

    #[tokio::test]
    async fn gcs_object_store_builder() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('service_account_path' '{service_account_path}', 'service_account_key' '{service_account_key}', 'application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");

        let ctx = SessionContext::new();
        let mut plan = ctx.state().create_logical_plan(&sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &mut plan {
            let builder = get_gcs_object_store_builder(table_url.as_ref(), cmd)?;
            // get the actual configuration information, then assert_eq!
            let config = [
                (GoogleConfigKey::ServiceAccount, service_account_path),
                (GoogleConfigKey::ServiceAccountKey, service_account_key),
                (
                    GoogleConfigKey::ApplicationCredentials,
                    application_credentials_path,
                ),
            ];
            for (key, value) in config {
                assert_eq!(value, builder.get_config_value(&key).unwrap());
            }
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        Ok(())
    }
}
