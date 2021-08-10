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

//! Object Store abstracts access to an underlying file/object storage.

use crate::datasource::local::LocalFileSystem;
use crate::error::Result;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Read;
use std::sync::{Arc, RwLock};

/// Objct Reader for one file in a object store
pub trait ObjectReader {
    /// Get reader for a part [start, start + length] in the file
    fn get_reader(&self, start: u64, length: usize) -> Box<dyn Read>;

    /// Get lenght for the file
    fn length(&self) -> u64;
}

/// A ObjectStore abstracts access to an underlying file/object storage.
/// It maps strings (e.g. URLs, filesystem paths, etc) to sources of bytes
pub trait ObjectStore: Sync + Send + Debug {
    /// Returns the object store as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns all the files with filename extension `ext` in path `prefix`
    fn list_all_files(&self, prefix: &str, ext: &str) -> Result<Vec<String>>;

    /// Get object reader for one file
    fn get_reader(&self, file_path: &str) -> Result<Arc<dyn ObjectReader>>;
}

static LOCAL_SCHEME: &str = "file";

/// A Registry holds all the object stores at runtime with a scheme for each store.
/// This allows the user to extend DataFusion with different storage systems such as S3 or HDFS
/// and query data inside these systems.
pub struct ObjectStoreRegistry {
    /// A map from scheme to object store that serve list / read operations for the store
    pub object_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreRegistry {
    /// Create the registry that object stores can registered into.
    /// ['LocalFileSystem'] store is registered in by default to support read from localfs natively.
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
        let mut stores = self.object_stores.write().unwrap();
        stores.insert(scheme, store)
    }

    /// Get the store registered for scheme
    pub fn get(&self, scheme: &str) -> Option<Arc<dyn ObjectStore>> {
        let stores = self.object_stores.read().unwrap();
        stores.get(scheme).cloned()
    }

    /// Get a suitable store for the path based on it's scheme. For example:
    /// path with prefix file:/// or no prefix will return the default LocalFS store,
    /// path with prefix s3:/// will return the S3 store if it's registered,
    /// and will always return LocalFS store when a prefix is not registered in the path.
    pub fn store_for_path(&self, path: &str) -> Arc<dyn ObjectStore> {
        if let Some((scheme, _)) = path.split_once(':') {
            let stores = self.object_stores.read().unwrap();
            if let Some(store) = stores.get(&*scheme.to_lowercase()) {
                return store.clone();
            }
        }
        self.object_stores
            .read()
            .unwrap()
            .get(LOCAL_SCHEME)
            .unwrap()
            .clone()
    }
}
