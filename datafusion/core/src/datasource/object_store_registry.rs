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
use std::fmt;
use std::sync::Arc;

/// Object store registry
pub struct ObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    pub object_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl fmt::Debug for ObjectStoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

    /// Get a suitable store for the URI based on it's scheme. For example:
    /// - URI with scheme `file://` or no schema will return the default LocalFS store
    /// - URI with scheme `s3://` will return the S3 store if it's registered
    /// Returns a tuple with the store and the self-described uri of the file in that store
    pub fn get_by_uri<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        if let Some((scheme, path)) = uri.split_once("://") {
            let stores = self.object_stores.read();
            let store = stores
                .get(&*scheme.to_lowercase())
                .map(Clone::clone)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "No suitable object store found for {}",
                        scheme
                    ))
                })?;
            Ok((store, path))
        } else {
            Ok((Arc::new(LocalFileSystem), uri))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ObjectStoreRegistry;
    use datafusion_data_access::object_store::local::LocalFileSystem;
    use std::sync::Arc;

    #[test]
    fn test_get_by_uri_s3() {
        let sut = ObjectStoreRegistry::default();
        sut.register_store("s3".to_string(), Arc::new(LocalFileSystem {}));
        let uri = "s3://bucket/key";
        let (_, path) = sut.get_by_uri(uri).unwrap();
        assert_eq!(path, "bucket/key");
    }

    #[test]
    fn test_get_by_uri_file() {
        let sut = ObjectStoreRegistry::default();
        let uri = "file:///bucket/key";
        let (_, path) = sut.get_by_uri(uri).unwrap();
        assert_eq!(path, "/bucket/key");
    }

    #[test]
    fn test_get_by_uri_local() {
        let sut = ObjectStoreRegistry::default();
        let uri = "/bucket/key";
        let (_, path) = sut.get_by_uri(uri).unwrap();
        assert_eq!(path, "/bucket/key");
    }
}
