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
use aws_config::BehaviorVersion;
use aws_credential_types::provider::{
    error::CredentialsError, ProvideCredentials, SharedCredentialsProvider,
};
use datafusion::{
    common::{
        config::ConfigEntry, config::ConfigExtension, config::ConfigField,
        config::ExtensionOptions, config::TableOptions, config::Visit, config_err,
        exec_datafusion_err, exec_err,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
};
use log::debug;
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential},
    gcp::GoogleCloudStorageBuilder,
    http::HttpBuilder,
    ClientOptions, CredentialProvider,
    Error::Generic,
    ObjectStore,
};
use std::{
    any::Any,
    error::Error,
    fmt::{Debug, Display},
    sync::Arc,
};
use url::Url;

#[cfg(not(test))]
use object_store::aws::resolve_bucket_region;

// Provide a local mock when running tests so we don't make network calls
#[cfg(test)]
async fn resolve_bucket_region(
    _bucket: &str,
    _client_options: &ClientOptions,
) -> object_store::Result<String> {
    Ok("eu-central-1".to_string())
}

pub async fn get_s3_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
    resolve_region: bool,
) -> Result<AmazonS3Builder> {
    let AwsOptions {
        access_key_id,
        secret_access_key,
        session_token,
        region,
        endpoint,
        allow_http,
        skip_signature,
    } = aws_options;

    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket_name);

    if let (Some(access_key_id), Some(secret_access_key)) =
        (access_key_id, secret_access_key)
    {
        debug!("Using explicitly provided S3 access_key_id and secret_access_key");
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);

        if let Some(session_token) = session_token {
            builder = builder.with_token(session_token);
        }
    } else {
        debug!("Using AWS S3 SDK to determine credentials");
        let CredentialsFromConfig {
            region,
            credentials,
        } = CredentialsFromConfig::try_new().await?;
        if let Some(region) = region {
            builder = builder.with_region(region);
        }
        if let Some(credentials) = credentials {
            let credentials = Arc::new(S3CredentialProvider { credentials });
            builder = builder.with_credentials(credentials);
        } else {
            debug!("No credentials found, defaulting to skip signature ");
            builder = builder.with_skip_signature(true);
        }
    }

    if let Some(region) = region {
        builder = builder.with_region(region);
    }

    // If the region is not set or auto_detect_region is true, resolve the region.
    if builder
        .get_config_value(&AmazonS3ConfigKey::Region)
        .is_none()
        || resolve_region
    {
        let region = resolve_bucket_region(bucket_name, &ClientOptions::new()).await?;
        builder = builder.with_region(region);
    }

    if let Some(endpoint) = endpoint {
        // Make a nicer error if the user hasn't allowed http and the endpoint
        // is http as the default message is "URL scheme is not allowed"
        if let Ok(endpoint_url) = Url::try_from(endpoint.as_str()) {
            if !matches!(allow_http, Some(true)) && endpoint_url.scheme() == "http" {
                return config_err!(
                    "Invalid endpoint: {endpoint}. \
                HTTP is not allowed for S3 endpoints. \
                To allow HTTP, set 'aws.allow_http' to true"
                );
            }
        }

        builder = builder.with_endpoint(endpoint);
    }

    if let Some(allow_http) = allow_http {
        builder = builder.with_allow_http(*allow_http);
    }

    if let Some(skip_signature) = skip_signature {
        builder = builder.with_skip_signature(*skip_signature);
    }

    Ok(builder)
}

/// Credentials from the AWS SDK
struct CredentialsFromConfig {
    region: Option<String>,
    credentials: Option<SharedCredentialsProvider>,
}

impl CredentialsFromConfig {
    /// Attempt find AWS S3 credentials via the AWS SDK
    pub async fn try_new() -> Result<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let region = config.region().map(|r| r.to_string());

        let credentials = config
            .credentials_provider()
            .ok_or_else(|| {
                DataFusionError::ObjectStore(Box::new(Generic {
                    store: "S3",
                    source: "Failed to get S3 credentials aws_config".into(),
                }))
            })?
            .clone();

        // The credential provider is lazy, so it does not fetch credentials
        // until they are needed. To ensure that the credentials are valid,
        // we can call `provide_credentials` here.
        let credentials = match credentials.provide_credentials().await {
            Ok(_) => Some(credentials),
            Err(CredentialsError::CredentialsNotLoaded(_)) => {
                debug!("Could not use AWS SDK to get credentials");
                None
            }
            // other errors like `CredentialsError::InvalidConfiguration`
            // should be returned to the user so they can be fixed
            Err(e) => {
                // Pass back underlying error to the user, including underlying source
                let source_message = if let Some(source) = e.source() {
                    format!(": {source}")
                } else {
                    String::new()
                };

                let message = format!(
                    "Error getting credentials from provider: {e}{source_message}",
                );

                return Err(DataFusionError::ObjectStore(Box::new(Generic {
                    store: "S3",
                    source: message.into(),
                })));
            }
        };
        Ok(Self {
            region,
            credentials,
        })
    }
}

#[derive(Debug)]
struct S3CredentialProvider {
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for S3CredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let creds =
            self.credentials
                .provide_credentials()
                .await
                .map_err(|e| Generic {
                    store: "S3",
                    source: Box::new(e),
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
    aws_options: &AwsOptions,
) -> Result<AmazonS3Builder> {
    get_object_store_builder(url, aws_options, true)
}

pub fn get_cos_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
) -> Result<AmazonS3Builder> {
    get_object_store_builder(url, aws_options, false)
}

fn get_object_store_builder(
    url: &Url,
    aws_options: &AwsOptions,
    virtual_hosted_style_request: bool,
) -> Result<AmazonS3Builder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = AmazonS3Builder::from_env()
        .with_virtual_hosted_style_request(virtual_hosted_style_request)
        .with_bucket_name(bucket_name)
        // oss/cos don't care about the "region" field
        .with_region("do_not_care");

    if let (Some(access_key_id), Some(secret_access_key)) =
        (&aws_options.access_key_id, &aws_options.secret_access_key)
    {
        builder = builder
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
    }

    if let Some(endpoint) = &aws_options.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    Ok(builder)
}

pub fn get_gcs_object_store_builder(
    url: &Url,
    gs_options: &GcpOptions,
) -> Result<GoogleCloudStorageBuilder> {
    let bucket_name = get_bucket_name(url)?;
    let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket_name);

    if let Some(service_account_path) = &gs_options.service_account_path {
        builder = builder.with_service_account_path(service_account_path);
    }

    if let Some(service_account_key) = &gs_options.service_account_key {
        builder = builder.with_service_account_key(service_account_key);
    }

    if let Some(application_credentials_path) = &gs_options.application_credentials_path {
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

/// This struct encapsulates AWS options one uses when setting up object storage.
#[derive(Default, Debug, Clone)]
pub struct AwsOptions {
    /// Access Key ID
    pub access_key_id: Option<String>,
    /// Secret Access Key
    pub secret_access_key: Option<String>,
    /// Session token
    pub session_token: Option<String>,
    /// AWS Region
    pub region: Option<String>,
    /// OSS or COS Endpoint
    pub endpoint: Option<String>,
    /// Allow HTTP (otherwise will always use https)
    pub allow_http: Option<bool>,
    /// Do not fetch credentials and do not sign requests
    ///
    /// This can be useful when interacting with public S3 buckets that deny
    /// authorized requests
    pub skip_signature: Option<bool>,
}

impl ExtensionOptions for AwsOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (_key, aws_key) = key.split_once('.').unwrap_or((key, ""));
        let (key, rem) = aws_key.split_once('.').unwrap_or((aws_key, ""));
        match key {
            "access_key_id" => {
                self.access_key_id.set(rem, value)?;
            }
            "secret_access_key" => {
                self.secret_access_key.set(rem, value)?;
            }
            "session_token" => {
                self.session_token.set(rem, value)?;
            }
            "region" => {
                self.region.set(rem, value)?;
            }
            "oss" | "cos" | "endpoint" => {
                self.endpoint.set(rem, value)?;
            }
            "allow_http" => {
                self.allow_http.set(rem, value)?;
            }
            "skip_signature" | "nosign" => {
                self.skip_signature.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on AwsOptions", rem);
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.access_key_id.visit(&mut v, "access_key_id", "");
        self.secret_access_key
            .visit(&mut v, "secret_access_key", "");
        self.session_token.visit(&mut v, "session_token", "");
        self.region.visit(&mut v, "region", "");
        self.endpoint.visit(&mut v, "endpoint", "");
        self.allow_http.visit(&mut v, "allow_http", "");
        v.0
    }
}

impl ConfigExtension for AwsOptions {
    const PREFIX: &'static str = "aws";
}

/// This struct encapsulates GCP options one uses when setting up object storage.
#[derive(Debug, Clone, Default)]
pub struct GcpOptions {
    /// Service account path
    pub service_account_path: Option<String>,
    /// Service account key
    pub service_account_key: Option<String>,
    /// Application credentials path
    pub application_credentials_path: Option<String>,
}

impl ExtensionOptions for GcpOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (_key, rem) = key.split_once('.').unwrap_or((key, ""));
        match rem {
            "service_account_path" => {
                self.service_account_path.set(rem, value)?;
            }
            "service_account_key" => {
                self.service_account_key.set(rem, value)?;
            }
            "application_credentials_path" => {
                self.application_credentials_path.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on GcpOptions", rem);
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.service_account_path
            .visit(&mut v, "service_account_path", "");
        self.service_account_key
            .visit(&mut v, "service_account_key", "");
        self.application_credentials_path.visit(
            &mut v,
            "application_credentials_path",
            "",
        );
        v.0
    }
}

impl ConfigExtension for GcpOptions {
    const PREFIX: &'static str = "gcp";
}

pub(crate) async fn get_object_store(
    state: &SessionState,
    scheme: &str,
    url: &Url,
    table_options: &TableOptions,
    resolve_region: bool,
) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!(
                    "Given table options incompatible with the 's3' scheme"
                );
            };
            let builder =
                get_s3_object_store_builder(url, options, resolve_region).await?;
            Arc::new(builder.build()?)
        }
        "oss" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!(
                    "Given table options incompatible with the 'oss' scheme"
                );
            };
            let builder = get_oss_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "cos" => {
            let Some(options) = table_options.extensions.get::<AwsOptions>() else {
                return exec_err!(
                    "Given table options incompatible with the 'cos' scheme"
                );
            };
            let builder = get_cos_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "gs" | "gcs" => {
            let Some(options) = table_options.extensions.get::<GcpOptions>() else {
                return exec_err!(
                    "Given table options incompatible with the 'gs'/'gcs' scheme"
                );
            };
            let builder = get_gcs_object_store_builder(url, options)?;
            Arc::new(builder.build()?)
        }
        "http" | "https" => Arc::new(
            HttpBuilder::new()
                .with_client_options(ClientOptions::new().with_allow_http(true))
                .with_url(url.origin().ascii_serialization())
                .build()?,
        ),
        _ => {
            // For other types, try to get from `object_store_registry`:
            state
                .runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    exec_datafusion_err!("Unsupported object store scheme: {}", scheme)
                })?
        }
    };
    Ok(store)
}

#[cfg(test)]
mod tests {
    use crate::cli_context::CliSessionContext;

    use super::*;

    use datafusion::{
        datasource::listing::ListingTableUrl,
        logical_expr::{DdlStatement, LogicalPlan},
        prelude::SessionContext,
    };

    use object_store::{aws::AmazonS3ConfigKey, gcp::GoogleConfigKey};

    #[tokio::test]
    async fn s3_object_store_builder_default() -> Result<()> {
        let location = "s3://bucket/path/FAKE/file.parquet";
        // Set it to a non-existent file to avoid reading the default configuration file
        std::env::set_var("AWS_CONFIG_FILE", "data/aws.config");
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", "data/aws.credentials");

        // No options
        let table_url = ListingTableUrl::parse(location)?;
        let scheme = table_url.scheme();
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");

        let ctx = SessionContext::new();
        ctx.register_table_options_extension_from_scheme(scheme);
        let table_options = get_table_options(&ctx, &sql).await;
        let aws_options = table_options.extensions.get::<AwsOptions>().unwrap();
        let builder =
            get_s3_object_store_builder(table_url.as_ref(), aws_options, false).await?;

        // If the environment variables are set (as they are in CI) use them
        let expected_access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let expected_secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let expected_region = Some(
            std::env::var("AWS_REGION").unwrap_or_else(|_| "eu-central-1".to_string()),
        );
        let expected_endpoint = std::env::var("AWS_ENDPOINT").ok();

        // get the actual configuration information, then assert_eq!
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            expected_access_key_id
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            expected_secret_access_key
        );
        // Default is to skip signature when no credentials are provided
        let expected_skip_signature =
            if expected_access_key_id.is_none() && expected_secret_access_key.is_none() {
                Some(String::from("true"))
            } else {
                Some(String::from("false"))
            };
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            expected_region
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            expected_endpoint
        );
        assert_eq!(builder.get_config_value(&AmazonS3ConfigKey::Token), None);
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SkipSignature),
            expected_skip_signature
        );
        Ok(())
    }

    #[tokio::test]
    async fn s3_object_store_builder() -> Result<()> {
        // "fake" is uppercase to ensure the values are not lowercased when parsed
        let access_key_id = "FAKE_access_key_id";
        let secret_access_key = "FAKE_secret_access_key";
        let region = "fake_us-east-2";
        let endpoint = "endpoint33";
        let session_token = "FAKE_session_token";
        let location = "s3://bucket/path/FAKE/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let scheme = table_url.scheme();
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS\
            ('aws.access_key_id' '{access_key_id}', \
            'aws.secret_access_key' '{secret_access_key}', \
            'aws.region' '{region}', \
            'aws.session_token' {session_token}, \
            'aws.endpoint' '{endpoint}'\
            ) LOCATION '{location}'"
        );

        let ctx = SessionContext::new();
        ctx.register_table_options_extension_from_scheme(scheme);
        let table_options = get_table_options(&ctx, &sql).await;
        let aws_options = table_options.extensions.get::<AwsOptions>().unwrap();
        let builder =
            get_s3_object_store_builder(table_url.as_ref(), aws_options, false).await?;
        // get the actual configuration information, then assert_eq!
        let config = [
            (AmazonS3ConfigKey::AccessKeyId, access_key_id),
            (AmazonS3ConfigKey::SecretAccessKey, secret_access_key),
            (AmazonS3ConfigKey::Region, region),
            (AmazonS3ConfigKey::Endpoint, endpoint),
            (AmazonS3ConfigKey::Token, session_token),
        ];
        for (key, value) in config {
            assert_eq!(value, builder.get_config_value(&key).unwrap());
        }
        // Should not skip signature when credentials are provided
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SkipSignature),
            Some("false".into())
        );

        Ok(())
    }

    #[tokio::test]
    async fn s3_object_store_builder_allow_http_error() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "http://endpoint33";
        let location = "s3://bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let scheme = table_url.scheme();
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS\
            ('aws.access_key_id' '{access_key_id}', \
            'aws.secret_access_key' '{secret_access_key}', \
            'aws.endpoint' '{endpoint}'\
            ) LOCATION '{location}'"
        );

        let ctx = SessionContext::new();
        ctx.register_table_options_extension_from_scheme(scheme);

        let table_options = get_table_options(&ctx, &sql).await;
        let aws_options = table_options.extensions.get::<AwsOptions>().unwrap();
        let err = get_s3_object_store_builder(table_url.as_ref(), aws_options, false)
            .await
            .unwrap_err();

        assert_eq!(err.to_string().lines().next().unwrap_or_default(), "Invalid or Unsupported Configuration: Invalid endpoint: http://endpoint33. HTTP is not allowed for S3 endpoints. To allow HTTP, set 'aws.allow_http' to true");

        // Now add `allow_http` to the options and check if it works
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS\
            ('aws.access_key_id' '{access_key_id}', \
            'aws.secret_access_key' '{secret_access_key}', \
            'aws.endpoint' '{endpoint}',\
            'aws.allow_http' 'true'\
            ) LOCATION '{location}'"
        );
        let table_options = get_table_options(&ctx, &sql).await;

        let aws_options = table_options.extensions.get::<AwsOptions>().unwrap();
        // ensure this isn't an error
        get_s3_object_store_builder(table_url.as_ref(), aws_options, false).await?;

        Ok(())
    }

    #[tokio::test]
    async fn s3_object_store_builder_resolves_region_when_none_provided() -> Result<()> {
        let expected_region = "eu-central-1";
        let location = "s3://test-bucket/path/file.parquet";
        // Set it to a non-existent file to avoid reading the default configuration file
        std::env::set_var("AWS_CONFIG_FILE", "data/aws.config");

        let table_url = ListingTableUrl::parse(location)?;
        let aws_options = AwsOptions {
            region: None, // No region specified - should auto-detect
            ..Default::default()
        };

        let builder =
            get_s3_object_store_builder(table_url.as_ref(), &aws_options, false).await?;

        // Verify that the region was auto-detected in test environment
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            Some(expected_region.to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn s3_object_store_builder_overrides_region_when_resolve_region_enabled(
    ) -> Result<()> {
        let original_region = "us-east-1";
        let expected_region = "eu-central-1"; // This should be the auto-detected region
        let location = "s3://test-bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let aws_options = AwsOptions {
            region: Some(original_region.to_string()), // Explicit region provided
            ..Default::default()
        };

        let builder =
            get_s3_object_store_builder(table_url.as_ref(), &aws_options, true).await?;

        // Verify that the region was overridden by auto-detection
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            Some(expected_region.to_string())
        );

        Ok(())
    }

    #[tokio::test]
    async fn oss_object_store_builder() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        let table_url = ListingTableUrl::parse(location)?;
        let scheme = table_url.scheme();
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.oss.endpoint' '{endpoint}') LOCATION '{location}'");

        let ctx = SessionContext::new();
        ctx.register_table_options_extension_from_scheme(scheme);
        let table_options = get_table_options(&ctx, &sql).await;

        let aws_options = table_options.extensions.get::<AwsOptions>().unwrap();
        let builder = get_oss_object_store_builder(table_url.as_ref(), aws_options)?;
        // get the actual configuration information, then assert_eq!
        let config = [
            (AmazonS3ConfigKey::AccessKeyId, access_key_id),
            (AmazonS3ConfigKey::SecretAccessKey, secret_access_key),
            (AmazonS3ConfigKey::Endpoint, endpoint),
        ];
        for (key, value) in config {
            assert_eq!(value, builder.get_config_value(&key).unwrap());
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
        let scheme = table_url.scheme();
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('gcp.service_account_path' '{service_account_path}', 'gcp.service_account_key' '{service_account_key}', 'gcp.application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");

        let ctx = SessionContext::new();
        ctx.register_table_options_extension_from_scheme(scheme);
        let table_options = get_table_options(&ctx, &sql).await;

        let gcp_options = table_options.extensions.get::<GcpOptions>().unwrap();
        let builder = get_gcs_object_store_builder(table_url.as_ref(), gcp_options)?;
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

        Ok(())
    }

    /// Plans the `CREATE EXTERNAL TABLE` SQL statement and returns the
    /// resulting resolved `CreateExternalTable` command.
    async fn get_table_options(ctx: &SessionContext, sql: &str) -> TableOptions {
        let mut plan = ctx.state().create_logical_plan(sql).await.unwrap();

        let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &mut plan else {
            panic!("plan is not a CreateExternalTable");
        };

        let mut table_options = ctx.state().default_table_options();
        table_options
            .alter_with_string_hash_map(&cmd.options)
            .unwrap();
        table_options
    }
}
