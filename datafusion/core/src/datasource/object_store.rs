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

//! ObjectStoreRegistry holds all the object stores at Runtime with a scheme for each store.
//! This allows the user to extend DataFusion with different storage systems such as S3 or HDFS
//! and query data inside these systems.

use dashmap::DashMap;
use datafusion_common::{DataFusionError, Result};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

/// A parsed URL identifying a particular [`ObjectStore`]
#[derive(Debug, Clone)]
pub struct ObjectStoreUrl {
    url: Url,
}

impl ObjectStoreUrl {
    /// Parse an [`ObjectStoreUrl`] from a string
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let mut parsed =
            Url::parse(s.as_ref()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let remaining = &parsed[url::Position::BeforePath..];
        if !remaining.is_empty() && remaining != "/" {
            return Err(DataFusionError::Execution(format!(
                "ObjectStoreUrl must only contain scheme and authority, got: {}",
                remaining
            )));
        }

        // Always set path for consistency
        parsed.set_path("/");
        Ok(Self { url: parsed })
    }

    /// An [`ObjectStoreUrl`] for the local filesystem
    pub fn local_filesystem() -> Self {
        Self::parse("file://").unwrap()
    }

    /// Returns this [`ObjectStoreUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }
}

impl AsRef<str> for ObjectStoreUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for ObjectStoreUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for ObjectStoreUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

/// Provides a mechanism for lazy, on-demand creation of [`ObjectStore`]
///
/// See [`ObjectStoreRegistry::new_with_provider`]
pub trait ObjectStoreProvider: Send + Sync + 'static {
    /// Return an ObjectStore for the provided url, called by [`ObjectStoreRegistry::get_by_url`]
    /// when no matching store has already been registered. The result will be cached based
    /// on its schema and authority. Any error will be returned to the caller
    fn get_by_url(&self, url: &Url) -> Result<Arc<dyn ObjectStore>>;
}

/// [`ObjectStoreRegistry`] stores [`ObjectStore`] keyed by url scheme and authority, that is
/// the part of a URL preceding the path
///
/// This is used by DataFusion to find an appropriate [`ObjectStore`] for a [`ListingTableUrl`]
/// provided in a query such as
///
/// ```sql
/// create external table unicorns stored as parquet location 's3://my_bucket/lineitem/';
/// ```
///
/// In this particular case the url `s3://my_bucket/lineitem/` will be provided to
/// [`ObjectStoreRegistry::get_by_url`] and one of three things will happen:
///
/// - If an [`ObjectStore`] has been registered with [`ObjectStoreRegistry::register_store`] with
/// scheme `s3` and host `my_bucket`, this [`ObjectStore`] will be returned
///
/// - If an [`ObjectStoreProvider`] has been associated with this [`ObjectStoreRegistry`] using
/// [`ObjectStoreRegistry::new_with_provider`], [`ObjectStoreProvider::get_by_url`] will be invoked,
/// and the returned [`ObjectStore`] registered on this [`ObjectStoreRegistry`]. Any error will
/// be returned to the caller
///
/// - Otherwise an error will be returned, indicating that no suitable [`ObjectStore`] could
/// be found
///
/// This allows for two different use-cases:
///
/// * DBMS systems where object store buckets are explicitly created using DDL, can register these
/// buckets using [`ObjectStoreRegistry::register_store`]
/// * DMBS systems relying on ad-hoc discovery, without corresponding DDL, can create [`ObjectStore`]
/// lazily, on-demand using [`ObjectStoreProvider`]
///
/// [`ListingTableUrl`]: crate::datasource::listing::ListingTableUrl
pub struct ObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    object_stores: DashMap<String, Arc<dyn ObjectStore>>,
    provider: Option<Arc<dyn ObjectStoreProvider>>,
}

impl std::fmt::Debug for ObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ObjectStoreRegistry")
            .field(
                "schemes",
                &self
                    .object_stores
                    .iter()
                    .map(|o| o.key().clone())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Default for ObjectStoreRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStoreRegistry {
    /// Create an [`ObjectStoreRegistry`] with no [`ObjectStoreProvider`].
    ///
    /// This will register [`LocalFileSystem`] to handle `file://` paths, further stores
    /// will need to be explicitly registered with calls to [`ObjectStoreRegistry::register_store`]
    pub fn new() -> Self {
        ObjectStoreRegistry::new_with_provider(None)
    }

    /// Create an [`ObjectStoreRegistry`] with the provided [`ObjectStoreProvider`]
    ///
    /// This will register [`LocalFileSystem`] to handle `file://` paths, further stores
    /// may be explicity registered with calls to [`ObjectStoreRegistry::register_store`] or
    /// created lazily, on-demand by the provided [`ObjectStoreProvider`]
    pub fn new_with_provider(provider: Option<Arc<dyn ObjectStoreProvider>>) -> Self {
        let object_stores: DashMap<String, Arc<dyn ObjectStore>> = DashMap::new();
        object_stores.insert("file://".to_string(), Arc::new(LocalFileSystem::new()));
        Self {
            object_stores,
            provider,
        }
    }

    /// Adds a new store to this registry.
    ///
    /// If a store with the same schema and host existed before, it is replaced and returned
    pub fn register_store(
        &self,
        scheme: impl AsRef<str>,
        host: impl AsRef<str>,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let s = format!("{}://{}", scheme.as_ref(), host.as_ref());
        self.object_stores.insert(s, store)
    }

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file:///` or no schema will return the default LocalFS store
    /// - URL with scheme `s3://bucket/` will return the S3 store if it's registered
    /// - URL with scheme `hdfs://hostname:port/` will return the hdfs store if it's registered
    ///
    pub fn get_by_url(&self, url: impl AsRef<Url>) -> Result<Arc<dyn ObjectStore>> {
        let url = url.as_ref();
        // First check whether can get object store from registry
        let s = &url[url::Position::BeforeScheme..url::Position::BeforePath];
        let store = self.object_stores.get(s).map(|o| o.value().clone());

        match store {
            Some(store) => Ok(store),
            None => match &self.provider {
                Some(provider) => {
                    let store = provider.get_by_url(url)?;
                    let key =
                        &url[url::Position::BeforeScheme..url::Position::BeforePath];
                    self.object_stores.insert(key.to_owned(), store.clone());
                    Ok(store)
                }
                None => Err(DataFusionError::Internal(format!(
                    "No suitable object store found for {}",
                    url
                ))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::ListingTableUrl;
    use std::sync::Arc;

    #[test]
    fn test_object_store_url() {
        let listing = ListingTableUrl::parse("file:///").unwrap();
        let store = listing.object_store();
        assert_eq!(store.as_str(), "file:///");

        let file = ObjectStoreUrl::parse("file://").unwrap();
        assert_eq!(file.as_str(), "file:///");

        let listing = ListingTableUrl::parse("s3://bucket/").unwrap();
        let store = listing.object_store();
        assert_eq!(store.as_str(), "s3://bucket/");

        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/");

        let url = ObjectStoreUrl::parse("s3://username:password@host:123").unwrap();
        assert_eq!(url.as_str(), "s3://username:password@host:123/");

        let err = ObjectStoreUrl::parse("s3://bucket:invalid").unwrap_err();
        assert_eq!(err.to_string(), "External error: invalid port number");

        let err = ObjectStoreUrl::parse("s3://bucket?").unwrap_err();
        assert_eq!(err.to_string(), "Execution error: ObjectStoreUrl must only contain scheme and authority, got: ?");

        let err = ObjectStoreUrl::parse("s3://bucket?foo=bar").unwrap_err();
        assert_eq!(err.to_string(), "Execution error: ObjectStoreUrl must only contain scheme and authority, got: ?foo=bar");

        let err = ObjectStoreUrl::parse("s3://host:123/foo").unwrap_err();
        assert_eq!(err.to_string(), "Execution error: ObjectStoreUrl must only contain scheme and authority, got: /foo");

        let err =
            ObjectStoreUrl::parse("s3://username:password@host:123/foo").unwrap_err();
        assert_eq!(err.to_string(), "Execution error: ObjectStoreUrl must only contain scheme and authority, got: /foo");
    }

    #[test]
    fn test_get_by_url_hdfs() {
        let sut = ObjectStoreRegistry::default();
        sut.register_store("hdfs", "localhost:8020", Arc::new(LocalFileSystem::new()));
        let url = ListingTableUrl::parse("hdfs://localhost:8020/key").unwrap();
        sut.get_by_url(&url).unwrap();
    }

    #[test]
    fn test_get_by_url_s3() {
        let sut = ObjectStoreRegistry::default();
        sut.register_store("s3", "bucket", Arc::new(LocalFileSystem::new()));
        let url = ListingTableUrl::parse("s3://bucket/key").unwrap();
        sut.get_by_url(&url).unwrap();
    }

    #[test]
    fn test_get_by_url_file() {
        let sut = ObjectStoreRegistry::default();
        let url = ListingTableUrl::parse("file:///bucket/key").unwrap();
        sut.get_by_url(&url).unwrap();
    }

    #[test]
    fn test_get_by_url_local() {
        let sut = ObjectStoreRegistry::default();
        let url = ListingTableUrl::parse("../").unwrap();
        sut.get_by_url(&url).unwrap();
    }
}
