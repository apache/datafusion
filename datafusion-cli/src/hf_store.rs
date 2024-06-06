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

use datafusion::common::{config_err, Result};
use datafusion::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion::error::DataFusionError;
use http::{header, HeaderMap};
use object_store::http::{HttpBuilder, HttpStore};
use object_store::ClientOptions;
use url::Url;
use std::any::Any;
use std::env;
use std::fmt::Display;
use std::str::FromStr;

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
        if !url.starts_with(Self::SCHEMA) {
            return config_err!(
                "Invalid HuggingFace URL: {}, only 'hf://' URLs are supported",
                url
            );
        }

        let mut parsed_url = Self::default();
        let mut last_delim = 5;

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
    const PREFIX: &'static str = "hf";
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
        self.endpoint.visit(
            &mut v,
            "endpoint",
            "The HuggingFace API endpoint",
        );
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
    user_access_token: Option<String>,
    parsed_url: Option<ParsedHFUrl>,
}

impl HFStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());

        self
    }

    pub fn with_user_access_token(mut self, user_access_token:  impl Into<String>) -> Self {
        self.user_access_token = Some(user_access_token.into());
        self
    }

    pub fn with_parsed_url(mut self, parsed_url: ParsedHFUrl) -> Self {
        self.parsed_url = Some(parsed_url);
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

    pub fn build(&self) -> Result<HttpStore> {
        let mut builder = HttpBuilder::new();

        if self.parsed_url.is_none() {
            return config_err!("Parsed URL is required to build HFStore");
        }

        let ep;
        if let Some(endpoint) = &self.endpoint {
            ep = endpoint.to_string();
        } else {
            ep = DEFAULT_ENDPOINT.to_string();
        }

        let url = format!("{}/{}", ep, self.parsed_url.as_ref().unwrap().file_path());
        println!("URL: {}", url);

        builder = builder.with_url(url);

        if let Some(user_access_token) = &self.user_access_token {
            if let Ok(token) = format!("Bearer {}", user_access_token).parse() {
                let mut header_map = HeaderMap::new();
                header_map.insert(
                    header::AUTHORIZATION,
                    token,
                );
                let options = ClientOptions::new().with_default_headers(header_map);

                builder = builder.with_client_options(options);
            }
        }

        builder.build().map_err(|e| DataFusionError::Execution(format!("Unable to build HFStore: {}", e)))
    }
}

pub fn get_hf_object_store_builder(
    url: &Url,
    options: &HFOptions,
) -> Result<HFStoreBuilder>
 {
    let parsed_url = ParsedHFUrl::parse(url.to_string())?;
    let mut builder = HFStoreBuilder::from_env();
    builder =   builder.with_parsed_url(parsed_url);

    if let Some(endpoint) = &options.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(user_access_token) = &options.user_access_token {
        builder = builder.with_user_access_token(user_access_token);
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;

    use crate::hf_store::ParsedHFUrl;

    #[test]
    fn test_parse_hf_url() {
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

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
            "hf://datasets/datasets-examples/doc-formats-csv-1@~csv/data.csv".to_string();

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
            "hg://datasets/datasets-examples/doc-formats-csv-1/data.csv",
            "Invalid HuggingFace URL: hg://datasets/datasets-examples/doc-formats-csv-1/data.csv, only 'hf://' URLs are supported",
        );

        test_error(
            "hf://datasets/datasets-examples/doc-formats-csv-1",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'",
        );

        test_error(
            "hf://datadicts/datasets-examples/doc-formats-csv-1/data.csv",
            "Invalid HuggingFace URL: hf://datadicts/datasets-examples/doc-formats-csv-1/data.csv, currently only 'datasets' or 'spaces' are supported",
        );

        test_error(
            "hf://datasets/datasets-examples/doc-formats-csv-1@~csv",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1@~csv, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'",
        );

        test_error(
            "hf://datasets/datasets-examples/doc-formats-csv-1@~csv/",
            "Invalid HuggingFace URL: hf://datasets/datasets-examples/doc-formats-csv-1@~csv/, please specify a path",
        );
    }

    #[test]
    fn test_file_path() {
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

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
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

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
