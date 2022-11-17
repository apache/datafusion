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
use std::{env, str::FromStr, sync::Arc};

use datafusion::{datasource::object_store::ObjectStoreProvider, error::DataFusionError};
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder};
use url::Url;

#[derive(Debug, PartialEq, Eq, clap::ArgEnum, Clone)]
pub enum ObjectStoreScheme {
    S3,
    GCS,
}

impl FromStr for ObjectStoreScheme {
    type Err = DataFusionError;

    fn from_str(input: &str) -> Result<Self> {
        match input {
            "s3" => Ok(ObjectStoreScheme::S3),
            "gcs" => Ok(ObjectStoreScheme::GCS),
            _ => Err(DataFusionError::Execution(format!(
                "Unsupported object store scheme {}",
                input
            ))),
        }
    }
}

#[derive(Debug)]
pub struct DatafusionCliObjectStoreProvider {}

/// ObjectStoreProvider for S3 and GCS
impl ObjectStoreProvider for DatafusionCliObjectStoreProvider {
    fn get_by_url(&self, url: &Url) -> Result<Arc<dyn object_store::ObjectStore>> {
        ObjectStoreScheme::from_str(url.scheme()).map(|scheme| match scheme {
            ObjectStoreScheme::S3 => build_s3_object_store(url),
            ObjectStoreScheme::GCS => build_gcs_object_store(url),
        })?
    }
}

fn build_s3_object_store(url: &Url) -> Result<Arc<dyn object_store::ObjectStore>> {
    let host = get_host_name(url)?;
    match AmazonS3Builder::from_env().with_bucket_name(host).build() {
        Ok(s3) => Ok(Arc::new(s3)),
        Err(err) => Err(DataFusionError::Execution(err.to_string())),
    }
}

fn build_gcs_object_store(url: &Url) -> Result<Arc<dyn object_store::ObjectStore>> {
    let host = get_host_name(url)?;
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(host);

    if let Ok(path) = env::var("GCP_SERVICE_ACCOUNT_PATH") {
        builder = builder.with_service_account_path(path);
    }
    match builder.build() {
        Ok(gcs) => Ok(Arc::new(gcs)),
        Err(err) => Err(DataFusionError::Execution(err.to_string())),
    }
}

fn get_host_name(url: &Url) -> Result<&str> {
    url.host_str().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Not able to parse hostname from url, {}",
            url.as_str()
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::{env, str::FromStr};

    use datafusion::datasource::object_store::ObjectStoreProvider;
    use url::Url;

    use super::DatafusionCliObjectStoreProvider;

    #[test]
    fn s3_provider_no_host() {
        let no_host_url = "s3:///";
        let provider = DatafusionCliObjectStoreProvider {};
        let err = provider
            .get_by_url(&Url::from_str(no_host_url).unwrap())
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Not able to parse hostname from url"))
    }

    #[test]
    fn gcs_provider_no_host() {
        let no_host_url = "gcs:///";
        let provider = DatafusionCliObjectStoreProvider {};
        let err = provider
            .get_by_url(&Url::from_str(no_host_url).unwrap())
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Not able to parse hostname from url"))
    }

    #[test]
    fn unknown_object_store_type() {
        let unknown = "unknown://bucket_name/path";
        let provider = DatafusionCliObjectStoreProvider {};
        let err = provider
            .get_by_url(&Url::from_str(unknown).unwrap())
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("Unsupported object store scheme unknown"))
    }

    #[test]
    fn s3_region_validation() {
        let s3 = "s3://bucket_name/path";
        let provider = DatafusionCliObjectStoreProvider {};
        let err = provider
            .get_by_url(&Url::from_str(s3).unwrap())
            .unwrap_err();
        assert!(err.to_string().contains("Generic S3 error: Missing region"));

        env::set_var("AWS_REGION", "us-east-1");
        let url = Url::from_str(s3).expect("Unable to parse s3 url");
        let res = provider.get_by_url(&url);
        let msg = match res {
            Err(e) => format!("{}", e),
            Ok(_) => "".to_string(),
        };
        assert_eq!("".to_string(), msg); // Fail with error message
        env::remove_var("AWS_REGION");
    }
}
