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

use bytes::Bytes;
use datafusion::common::{config_err, Result};
use datafusion::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use futures::stream::BoxStream;
use object_store::http::HttpStore;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, PutOptions,
    PutResult,
};
use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;
use tokio::io::AsyncWrite;

#[derive(Debug, Clone)]
pub struct ParsedHFUrl {
    endpoint: Option<String>,
    path: Option<String>,
    repository: Option<String>,
    revision: Option<String>,
    repo_type: Option<String>,
}

impl Default for ParsedHFUrl {
    fn default() -> Self {
        Self {
            endpoint: Some("https://huggingface.com".to_string()),
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
    /// If the endpoint is not provided, it defaults to `https://huggingface.com`.
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

    pub fn file_url(&self) -> Result<String> {
        let mut url = self.endpoint.clone().unwrap();
        url.push_str("/");
        url.push_str(self.repo_type.as_deref().unwrap());
        url.push_str("/");
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/resolve/");
        url.push_str(self.revision.as_deref().unwrap());
        url.push_str("/");
        url.push_str(self.path.as_deref().unwrap());

        Ok(url)
    }

    pub fn tree_url(&self) -> Result<String> {
        let mut url = self.endpoint.clone().unwrap();
        url.push_str("/api/");
        url.push_str(self.repo_type.as_deref().unwrap());
        url.push_str("/");
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/tree/");
        url.push_str(self.revision.as_deref().unwrap());
        url.push_str("/");
        url.push_str(self.path.as_deref().unwrap());

        Ok(url)
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

// pub struct HFStoreBuilder {}

// #[derive(Debug, Clone)]
// pub struct HFStore {
//     inner: Arc<HttpStore>,
// }

// impl Display for HFStore {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "HFStore")
//     }
// }

// impl HFStore {}

// impl ObjectStore for HFStore {
//     async fn put_opts(
//         &self,
//         location: &Path,
//         bytes: Bytes,
//         opts: PutOptions,
//     ) -> object_store::Result<PutResult> {
//         todo!()
//     }

//     async fn put_multipart(
//         &self,
//         location: &Path,
//     ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
//         todo!()
//     }

//     async fn abort_multipart(
//         &self,
//         location: &Path,
//         multipart_id: &MultipartId,
//     ) -> object_store::Result<()> {
//         todo!()
//     }

//     async fn get_opts(
//         &self,
//         location: &Path,
//         options: GetOptions,
//     ) -> object_store::Result<GetResult> {
//         todo!()
//     }

//     async fn delete(&self, location: &Path) -> object_store::Result<()> {
//         todo!()
//     }

//     fn list(
//         &self,
//         prefix: Option<&Path>,
//     ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
//         todo!()
//     }

//     async fn list_with_delimiter(
//         &self,
//         prefix: Option<&Path>,
//     ) -> object_store::Result<ListResult> {
//         todo!()
//     }

//     async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
//         todo!()
//     }

//     async fn copy_if_not_exists(
//         &self,
//         from: &Path,
//         to: &Path,
//     ) -> object_store::Result<()> {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;

    use crate::hf_store::ParsedHFUrl;

    #[test]
    fn test_parse_hf_url() {
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url).unwrap();

        assert_eq!(
            parsed_url.endpoint,
            Some("https://huggingface.com".to_string())
        );
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

        assert_eq!(
            parsed_url.endpoint,
            Some("https://huggingface.com".to_string())
        );
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
    fn test_file_url() {
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url).unwrap();

        let file_url = parsed_url.file_url().unwrap();

        assert_eq!(
            file_url,
            "https://huggingface.com/datasets/datasets-examples/doc-formats-csv-1/resolve/main/data.csv"
        );
    }

    #[test]
    fn test_tree_url() {
        let url =
            "hf://datasets/datasets-examples/doc-formats-csv-1/data.csv".to_string();

        let parsed_url = ParsedHFUrl::parse(url).unwrap();

        let tree_url = parsed_url.tree_url().unwrap();

        assert_eq!(
            tree_url,
            "https://huggingface.com/api/datasets/datasets-examples/doc-formats-csv-1/tree/main/data.csv"
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
