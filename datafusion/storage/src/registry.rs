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

use crate::Storage;
use dashmap::DashMap;
use datafusion_common::{exec_err, DataFusionError, Result};
use std::sync::Arc;
use url::Url;

/// A parsed URL identifying a particular [`Storage`] instance
///
/// For example:
/// * `file://` for local file system
/// * `s3://bucket` for AWS S3 bucket
/// * `oss://bucket` for Aliyun OSS bucket
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageUrl {
    url: Url,
}

impl StorageUrl {
    /// Parse an [`StorageUrl`] from a string
    ///
    /// # Example
    /// ```
    /// # use url::Url;
    /// # use datafusion_storage::StorageUrl;
    /// let storage_url = StorageUrl::parse("s3://bucket").unwrap();
    /// assert_eq!(storage_url.as_str(), "s3://bucket/");
    /// // can also access the underlying `Url`
    /// let url: &Url = storage_url.as_ref();
    /// assert_eq!(url.scheme(), "s3");
    /// assert_eq!(url.host_str(), Some("bucket"));
    /// assert_eq!(url.path(), "/");
    /// ```
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let mut parsed =
            Url::parse(s.as_ref()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let remaining = &parsed[url::Position::BeforePath..];
        if !remaining.is_empty() && remaining != "/" {
            return exec_err!(
                "storage_url must only contain scheme and authority, got: {remaining}"
            );
        }

        // Always set path for consistency
        parsed.set_path("/");
        Ok(Self { url: parsed })
    }

    /// An [`StorageUrl`] for the local filesystem (`file://`)
    ///
    /// # Example
    /// ```
    /// # use datafusion_storage::StorageUrl;
    /// let local_fs = StorageUrl::parse("file://").unwrap();
    /// assert_eq!(local_fs, StorageUrl::local_filesystem())
    /// ```
    pub fn local_filesystem() -> Self {
        Self::parse("file://").unwrap()
    }

    /// Returns this [`StorageUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl AsRef<str> for StorageUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for StorageUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

/// [`StorageRegistry`] maps a URL to an [`Storage`] instance,
/// and allows DataFusion to read from different [`Storage`]
/// instances. For example DataFusion might be configured so that
///
/// 1. `s3://my_bucket/lineitem/` mapped to the `/lineitem` path on an
///    AWS S3 object store bound to `my_bucket`
///
/// 2. `s3://my_other_bucket/lineitem/` mapped to the (same)
///    `/lineitem` path on a *different* AWS S3 object store bound to
///    `my_other_bucket`
///
/// When given a [`ListingTableUrl`], DataFusion tries to find an
/// appropriate [`Storage`]. For example
///
/// ```sql
/// create external table unicorns stored as parquet location 's3://my_bucket/lineitem/';
/// ```
///
/// In this particular case, the url `s3://my_bucket/lineitem/` will be provided to
/// [`StorageRegistry::get_storage`] and one of three things will happen:
///
/// - If an [`Storage`] has been registered with [`StorageRegistry::register_storage`] with
///   `s3://my_bucket`, that [`Storage`] will be returned
///
/// - If an AWS S3 object store can be ad-hoc discovered by the url `s3://my_bucket/lineitem/`, this
///   object store will be registered with key `s3://my_bucket` and returned.
///
/// - Otherwise an error will be returned, indicating that no suitable [`Storage`] could
///   be found
///
/// This allows for two different use-cases:
///
/// 1. Systems where object store buckets are explicitly created using DDL, can register these
///    buckets using [`StorageRegistry::register_storage`]
///
/// 2. Systems relying on ad-hoc discovery, without corresponding DDL, can create [`Storage`]
///    lazily by providing a custom implementation of [`StorageRegistry`]
///
/// <!-- is in a different crate so normal rustdoc links don't work -->
/// [`ListingTableUrl`]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTableUrl.html
/// [`Storage`]: datafusion_storage::Storage
pub trait StorageRegistry: Send + Sync + std::fmt::Debug + 'static {
    /// If a store with the same key existed before, it is replaced and returned
    fn register_storage(
        &self,
        url: &Url,
        store: Arc<dyn Storage>,
    ) -> Option<Arc<dyn Storage>>;

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file:///` or no scheme will return the default LocalFS store
    /// - URL with scheme `s3://bucket/` will return the S3 store
    /// - URL with scheme `hdfs://hostname:port/` will return the hdfs store
    ///
    /// If no [`Storage`] found for the `url`, ad-hoc discovery may be executed depending on
    /// the `url` and [`StorageRegistry`] implementation. An [`Storage`] may be lazily
    /// created and registered.
    fn get_storage(&self, url: &Url) -> Result<Arc<dyn Storage>>;
}

/// The default [`StorageRegistry`]
pub struct DefaultStorageRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    stores: DashMap<String, Arc<dyn Storage>>,
}

impl std::fmt::Debug for DefaultStorageRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("DefaultStorageRegistry")
            .field(
                "schemes",
                &self
                    .stores
                    .iter()
                    .map(|o| o.key().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for DefaultStorageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultStorageRegistry {
    /// Create a new [`DefaultStorageRegistry`]
    pub fn new() -> Self {
        let stores: DashMap<String, Arc<dyn Storage>> = DashMap::new();
        Self { stores }
    }
}

/// Stores are registered based on the scheme, host and port of the provided URL
/// with a [`LocalFileSystem::new`] automatically registered for `file://` (if the
/// target arch is not `wasm32`).
///
/// For example:
///
/// - `file:///my_path` will return the default LocalFS store
/// - `s3://bucket/path` will return a store registered with `s3://bucket` if any
/// - `hdfs://host:port/path` will return a store registered with `hdfs://host:port` if any
impl StorageRegistry for DefaultStorageRegistry {
    fn register_storage(
        &self,
        url: &Url,
        store: Arc<dyn Storage>,
    ) -> Option<Arc<dyn Storage>> {
        let s = get_url_key(url);
        self.stores.insert(s, store)
    }

    fn get_storage(&self, url: &Url) -> Result<Arc<dyn Storage>> {
        let s = get_url_key(url);
        self.stores
            .get(&s)
            .map(|o| Arc::clone(o.value()))
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "No suitable object store found for {url}. See `RuntimeEnv::register_object_store`"
                ))
            })
    }
}

/// Get the key of a url for object store registration.
/// The credential info will be removed
fn get_url_key(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_url() {
        let file = StorageUrl::parse("file://").unwrap();
        assert_eq!(file.as_str(), "file:///");

        let url = StorageUrl::parse("s3://bucket").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/");

        let url = StorageUrl::parse("s3://username:password@host:123").unwrap();
        assert_eq!(url.as_str(), "s3://username:password@host:123/");

        let err = StorageUrl::parse("s3://bucket:invalid").unwrap_err();
        assert_eq!(err.strip_backtrace(), "External error: invalid port number");

        let err = StorageUrl::parse("s3://bucket?").unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Execution error: StorageUrl must only contain scheme and authority, got: ?"
        );

        let err = StorageUrl::parse("s3://bucket?foo=bar").unwrap_err();
        assert_eq!(err.strip_backtrace(), "Execution error: StorageUrl must only contain scheme and authority, got: ?foo=bar");

        let err = StorageUrl::parse("s3://host:123/foo").unwrap_err();
        assert_eq!(err.strip_backtrace(), "Execution error: StorageUrl must only contain scheme and authority, got: /foo");

        let err = StorageUrl::parse("s3://username:password@host:123/foo").unwrap_err();
        assert_eq!(err.strip_backtrace(), "Execution error: StorageUrl must only contain scheme and authority, got: /foo");
    }

    #[test]
    fn test_get_url_key() {
        let file = StorageUrl::parse("file://").unwrap();
        let key = get_url_key(&file.url);
        assert_eq!(key.as_str(), "file://");

        let url = StorageUrl::parse("s3://bucket").unwrap();
        let key = get_url_key(&url.url);
        assert_eq!(key.as_str(), "s3://bucket");

        let url = StorageUrl::parse("s3://username:password@host:123").unwrap();
        let key = get_url_key(&url.url);
        assert_eq!(key.as_str(), "s3://host:123");
    }
}
