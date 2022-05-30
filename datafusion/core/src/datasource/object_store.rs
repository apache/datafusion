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

use datafusion_common::{DataFusionError, Result};
use datafusion_data_access::object_store::local::{LocalFileSystem, LOCAL_SCHEME};
use datafusion_data_access::object_store::ObjectStore;
use parking_lot::RwLock;
use std::collections::HashMap;
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

/// Object store registry
pub struct ObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    pub object_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl std::fmt::Debug for ObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ObjectStoreRegistry")
            .field(
                "schemes",
                &self.object_stores.read().keys().collect::<Vec<_>>(),
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
    /// Create the registry that object stores can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read local files natively.
    pub fn new() -> Self {
        let mut map: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        map.insert(LOCAL_SCHEME.to_string(), Arc::new(LocalFileSystem));

        Self {
            object_stores: RwLock::new(map),
        }
    }

    /// Adds a new store to this registry.
    /// If a store of the same prefix existed before, it is replaced in the registry and returned.
    pub fn register_store(
        &self,
        scheme: String,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mut stores = self.object_stores.write();
        stores.insert(scheme, store)
    }

    /// Get the store registered for scheme
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn ObjectStore>> {
        let stores = self.object_stores.read();
        stores.get(scheme).cloned()
    }

    /// Get a suitable store for the provided URL. For example:
    ///
    /// - URL with scheme `file://` or no schema will return the default LocalFS store
    /// - URL with scheme `s3://` will return the S3 store if it's registered
    ///
    pub fn get_by_url(&self, url: impl AsRef<Url>) -> Result<Arc<dyn ObjectStore>> {
        let url = url.as_ref();
        let store = self.get(url.scheme()).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "No suitable object store found for {}",
                url
            ))
        })?;

        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::listing::ListingTableUrl;
    use datafusion_data_access::object_store::local::LocalFileSystem;
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
    fn test_get_by_url_s3() {
        let sut = ObjectStoreRegistry::default();
        sut.register_store("s3".to_string(), Arc::new(LocalFileSystem {}));
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
