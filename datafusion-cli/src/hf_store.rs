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
use bytes::Bytes;
use datafusion::common::{config_err, Result};
use datafusion::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion::error::DataFusionError;
use futures::stream::BoxStream;
use futures::StreamExt;
use http::{header, HeaderMap};
use object_store::http::{HttpBuilder, HttpStore};
use object_store::path::Path;
use object_store::{
    ClientOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult,
    MultipartId, ObjectMeta, ObjectStore, PutOptions, PutResult,
    Result as ObjectStoreResult,
};
use std::any::Any;
use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use url::Url;

pub const STORE: &str = "hf";
pub const DEFAULT_ENDPOINT: &str = "https://huggingface.co";

pub enum HFConfigKey {
    Endpoint,
    UserAccessToken,
}

impl AsRef<str> for HFConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::Endpoint => "endpoint",
            Self::UserAccessToken => "user_access_token",
        }
    }
}

impl FromStr for HFConfigKey {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "endpoint" => Ok(Self::Endpoint),
            "user_access_token" => Ok(Self::UserAccessToken),
            _ => config_err!("Invalid HuggingFace configuration key: {}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedHFUrl {
    path: Option<String>,
    repository: Option<String>,
    revision: Option<String>,
    repo_type: Option<String>,
}

impl Default for ParsedHFUrl {
    fn default() -> Self {
        Self {
            path: None,
            repository: None,
            revision: Some("main".to_string()),
            repo_type: Some("datasets".to_string()),
        }
    }
}

impl ParsedHFUrl {
    pub const SCHEMA: &'static str = "hf://";

    /// Parse a HuggingFace URL into a ParsedHFUrl struct.
    /// The URL should be in the format `hf://<repo_type>/<repository>[@revision]/<path>`
    /// where `repo_type` is either `datasets` or `spaces`.
    /// If the revision is not provided, it defaults to `main`.
    /// If the endpoint is not provided, it defaults to `https://huggingface.co`.
    ///
    /// url: The HuggingFace URL to parse.
    pub fn parse(url: String) -> Result<Self> {
        let mut parsed_url = Self::default();
        let mut last_delim = 0;

        // parse repository type.
        if let Some(curr_delim) = url[last_delim..].find('/') {
            let repo_type = &url[last_delim..last_delim + curr_delim];
            if (repo_type != "datasets") && (repo_type != "spaces") {
                return config_err!(
                    "Invalid HuggingFace URL: {}, currently only 'datasets' or 'spaces' are supported",
                    url
                );
            }

            parsed_url.repo_type = Some(repo_type.to_string());
            last_delim += curr_delim + 1;
        } else {
            return config_err!("Invalid HuggingFace URL: {}, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'", url);
        }

        let start_delim = last_delim;
        // parse repository and revision.
        if let Some(curr_delim) = url[last_delim..].find('/') {
            last_delim += curr_delim + 1;
        } else {
            return config_err!("Invalid HuggingFace URL: {}, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'", url);
        }

        let next_slash = url[last_delim..].find('/');

        // next slash is not found
        if next_slash.is_none() {
            return config_err!("Invalid HuggingFace URL: {}, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'", url);
        }

        let next_at = url[last_delim..].find('@');
        // @ is found before the next slash.
        if let Some(at) = next_at {
            if let Some(slash) = next_slash {
                if at < slash {
                    let repo = &url[start_delim..last_delim + at];
                    let revision = &url[last_delim + at + 1..last_delim + slash];
                    parsed_url.repository = Some(repo.to_string());
                    parsed_url.revision = Some(revision.to_string());
                    last_delim += slash;
                }
            }
        }

        // @ is not found before the next slash.
        if parsed_url.repository.is_none() {
            last_delim += next_slash.unwrap();
            let repo = &url[start_delim..last_delim];
            parsed_url.repository = Some(repo.to_string());
        }

        if (last_delim + 1) >= url.len() {
            return config_err!(
                "Invalid HuggingFace URL: {}, please specify a path",
                url
            );
        }

        // parse path.
        let path = &url[last_delim + 1..];
        parsed_url.path = Some(path.to_string());

        Ok(parsed_url)
    }

    pub fn file_path(&self) -> String {
        let mut url = self.repo_type.clone().unwrap();
        url.push('/');
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/resolve/");
        url.push_str(self.revision.as_deref().unwrap());
        url.push('/');
        url.push_str(self.path.as_deref().unwrap());

        url
    }

    pub fn tree_path(&self) -> String {
        let mut url = "api/".to_string();
        url.push_str(self.repo_type.as_deref().unwrap());
        url.push('/');
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/tree/");
        url.push_str(self.revision.as_deref().unwrap());
        url.push('/');
        url.push_str(self.path.as_deref().unwrap());

        url
    }
}

/// HFOptions is the configuration options for the HFStoreBuilder.
#[derive(Debug, Clone, Default)]
pub struct HFOptions {
    endpoint: Option<String>,
    user_access_token: Option<String>,
}

impl ConfigExtension for HFOptions {
    const PREFIX: &'static str = STORE;
}

impl ExtensionOptions for HFOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::common::Result<()> {
        let (_key, rem) = key.split_once('.').unwrap_or((key, ""));
        match rem {
            "endpoint" => {
                self.endpoint.set(rem, value)?;
            }
            "user_access_token" => {
                self.user_access_token.set(rem, value)?;
            }
            _ => {
                return config_err!("Config value \"{}\" not found on HFOptions", rem);
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
        self.endpoint
            .visit(&mut v, "endpoint", "The HuggingFace API endpoint");
        self.user_access_token.visit(
            &mut v,
            "user_access_token",
            "The HuggingFace user access token",
        );
        v.0
    }
}

#[derive(Debug, Clone, Default)]
pub struct HFStoreBuilder {
    endpoint: Option<String>,
    repo_type: Option<String>,
    user_access_token: Option<String>,
}

impl HFStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_repo_type(mut self, repo_type: impl Into<String>) -> Self {
        self.repo_type = Some(repo_type.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());

        self
    }

    pub fn with_user_access_token(
        mut self,
        user_access_token: impl Into<String>,
    ) -> Self {
        self.user_access_token = Some(user_access_token.into());
        self
    }

    pub fn from_env() -> Self {
        let mut builder = Self::new();
        if let Ok(endpoint) = env::var("HF_ENDPOINT") {
            builder = builder.with_endpoint(endpoint);
        }

        if let Ok(user_access_token) = env::var("HF_USER_ACCESS_TOKEN") {
            builder = builder.with_user_access_token(user_access_token);
        }

        builder
    }

    pub fn build(&self) -> Result<HFStore> {
        let mut inner_builder = HttpBuilder::new();

        let repo_type = self.repo_type.clone().unwrap_or("datasets".to_string());

        let ep;
        if let Some(endpoint) = &self.endpoint {
            ep = endpoint.to_string();
        } else {
            ep = DEFAULT_ENDPOINT.to_string();
        }

        inner_builder = inner_builder.with_url(ep.clone());

        if let Some(user_access_token) = &self.user_access_token {
            if let Ok(token) = format!("Bearer {}", user_access_token).parse() {
                let mut header_map = HeaderMap::new();
                header_map.insert(header::AUTHORIZATION, token);
                let options = ClientOptions::new().with_default_headers(header_map);

                inner_builder = inner_builder.with_client_options(options);
            }
        }

        let builder = inner_builder.build().map_err(|e| {
            DataFusionError::Execution(format!("Unable to build HFStore: {}", e))
        })?;

        Ok(HFStore::new(ep, repo_type, Arc::new(builder)))
    }
}

pub fn get_hf_object_store_builder(
    url: &Url,
    options: &HFOptions,
) -> Result<HFStoreBuilder> {
    let mut builder = HFStoreBuilder::from_env();

    // The repo type is the first part of the path, which are treated as the origin in the process.
    let Some(repo_type) = url.domain() else {
        return config_err!(
            "Invalid HuggingFace URL: {}, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'",
            url
        );
    };

    if repo_type != "datasets" && repo_type != "spaces" {
        return config_err!(
            "Invalid HuggingFace URL: {}, currently only 'datasets' or 'spaces' are supported",
            url
        );
    }

    builder = builder.with_repo_type(repo_type);

    if let Some(endpoint) = &options.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(user_access_token) = &options.user_access_token {
        builder = builder.with_user_access_token(user_access_token);
    }

    Ok(builder)
}

#[derive(Debug)]
pub struct HFStore {
    endpoint: String,
    repo_type: String,
    store: Arc<HttpStore>,
}

impl HFStore {
    pub fn new(endpoint: String, repo_type: String, store: Arc<HttpStore>) -> Self {
        Self {
            endpoint,
            repo_type,
            store,
        }
    }
}

impl Display for HFStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HFStore({})", self.endpoint)
    }
}

#[async_trait]
impl ObjectStore for HFStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: Bytes,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> ObjectStoreResult<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        println!("GETTING: {}", location);

        let formatted_location = format!("{}/{}", self.repo_type, location);

        let Ok(parsed_url) = ParsedHFUrl::parse(formatted_location) else {
            return Err(ObjectStoreError::Generic {
                store: STORE,
                source: format!("Unable to parse url {location}").into(),
            });
        };

        let file_path = parsed_url.file_path();
        println!("FILE_PATH: {:?}", file_path);

        let Ok(file_path) = Path::parse(file_path.clone()) else {
            return Err(ObjectStoreError::Generic {
                store: STORE,
                source: format!("Invalid file path {}", file_path).into(),
            });
        };

        let mut res = self.store.get_opts(&file_path, options).await?;
        
        res.meta.location = location.clone();
        Ok(res)
    }

    async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> ObjectStoreResult<ListResult> {
        println!("LISTING_WITH_DELIMITER: {:?}", prefix);

        Err(ObjectStoreError::NotImplemented)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        let Some(prefix) = prefix else {
            return futures::stream::once(async {
                Err(ObjectStoreError::Generic {
                    store: STORE,
                    source: "Prefix is required".into(),
                })
            })
            .boxed();
        };

        let formatted_prefix = format!("{}/{}", self.repo_type, prefix);
        let Ok(parsed_url) = ParsedHFUrl::parse(formatted_prefix.clone()) else {
            return futures::stream::once(async move {
                Err(ObjectStoreError::Generic {
                    store: STORE,
                    source: format!("Unable to parse url {}", formatted_prefix.clone()).into(),
                })
            })
            .boxed();
        };

        let tree_path = Path::from(parsed_url.tree_path());
        println!("LISTING: {:?}", tree_path);

        futures::stream::once(async move {
            let result = self.store.get(&tree_path).await;
            
            println!("RESULT: {:?}", result);

            Err(ObjectStoreError::NotImplemented)
        })
        .boxed()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;

    use crate::hf_store::ParsedHFUrl;

    #[test]
    fn test_parse_hf_url() {
        let url = "datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url).unwrap();

        assert_eq!(parsed_url.repo_type, Some("datasets".to_string()));
        assert_eq!(
            parsed_url.repository,
            Some("datasets-examples/doc-formats-csv-1".to_string())
        );
        assert_eq!(parsed_url.revision, Some("main".to_string()));
        assert_eq!(parsed_url.path, Some("data.csv".to_string()));
    }

    #[test]
    fn test_parse_hf_url_with_revision() {
        let url =
            "datasets/datasets-examples/doc-formats-csv-1@~csv/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url).unwrap();

        assert_eq!(parsed_url.repo_type, Some("datasets".to_string()));
        assert_eq!(
            parsed_url.repository,
            Some("datasets-examples/doc-formats-csv-1".to_string())
        );
        assert_eq!(parsed_url.revision, Some("~csv".to_string()));
        assert_eq!(parsed_url.path, Some("data.csv".to_string()));
    }

    #[test]
    fn test_parse_hf_url_errors() {
        test_error(
            "datasets/datasets-examples/doc-formats-csv-1",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'",
        );

        test_error(
            "datadicts/datasets-examples/doc-formats-csv-1/data.csv",
            "Invalid HuggingFace URL: hf://datadicts/datasets-examples/doc-formats-csv-1/data.csv, currently only 'datasets' or 'spaces' are supported",
        );

        test_error(
            "datasets/datasets-examples/doc-formats-csv-1@~csv",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1@~csv, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'",
        );

        test_error(
            "datasets/datasets-examples/doc-formats-csv-1@~csv/",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1@~csv/, please specify a path",
        );
    }

    #[test]
    fn test_file_path() {
        let url = "datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url);

        assert!(parsed_url.is_ok());

        let file_path = parsed_url.unwrap().file_path();

        assert_eq!(
            file_path,
            "datasets/datasets-examples/doc-formats-csv-1/resolve/main/data.csv"
        );
    }

    #[test]
    fn test_tree_path() {
        let url = "datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url);

        assert!(parsed_url.is_ok());

        let tree_path = parsed_url.unwrap().tree_path();

        assert_eq!(
            tree_path,
            "api/datasets/datasets-examples/doc-formats-csv-1/tree/main/data.csv"
        );
    }

    fn test_error(url: &str, expected: &str) {
        let parsed_url_result = ParsedHFUrl::parse(url.to_string());

        match parsed_url_result {
            Ok(_) => panic!("Expected error, but got success"),
            Err(err) => match err {
                DataFusionError::Configuration(_) => {
                    assert_eq!(
                        err.to_string(),
                        format!("Invalid or Unsupported Configuration: {}", expected)
                    )
                }
                _ => panic!("Expected Configuration error, but got {:?}", err),
            },
        }
    }
}
