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

pub mod local;

use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use futures::{AsyncRead, Stream};
use lazy_static::lazy_static;

use local::LocalFileSystem;

use crate::error::{DataFusionError, Result};
use chrono::Utc;

lazy_static! {
    /// A singleton to help making the object store registry
    /// consitent throughout the code. For instance in a distributed
    /// system like Ballista, it ensures that the stores configured
    /// here will be consistent on the executor and the scheduler.
    pub static ref OBJECT_STORES: Arc<ObjectStoreRegistry> = {
        let reg = Arc::new(ObjectStoreRegistry::new());
        reg.register_store(LOCAL_SCHEME.to_string(), Arc::new(LocalFileSystem));
        // YOU CAN ADD YOUR CUSTOM STORES HERE
        reg
    };
}

/// Object Reader for one file in a object store
#[async_trait]
pub trait ObjectReader {
    /// Get reader for a part [start, start + length] in the file asynchronously
    async fn chunk_reader(&self, start: u64, length: usize)
        -> Result<Arc<dyn AsyncRead>>;

    /// Get length for the file
    fn length(&self) -> u64;
}

/// Represents a file or a prefix that may require further resolution
#[derive(Debug)]
pub enum ListEntry {
    /// File metadata
    FileMeta(FileMeta),
    /// Prefix to be further resolved during partition discovery
    Prefix(String),
}

/// File meta we got from object store
#[derive(Debug)]
pub struct FileMeta {
    /// Path of the file
    pub path: String,
    /// Last time the file was modified in UTC
    pub last_modified: Option<chrono::DateTime<Utc>>,
    /// File size in total
    pub size: u64,
}

/// Stream of files get listed from object store
pub type FileMetaStream =
    Pin<Box<dyn Stream<Item = Result<FileMeta>> + Send + Sync + 'static>>;

/// Stream of list entries get from object store
pub type ListEntryStream =
    Pin<Box<dyn Stream<Item = Result<ListEntry>> + Send + Sync + 'static>>;

/// A ObjectStore abstracts access to an underlying file/object storage.
/// It maps strings (e.g. URLs, filesystem paths, etc) to sources of bytes
#[async_trait]
pub trait ObjectStore: Sync + Send + Debug {
    /// Returns all the files in path `prefix`
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream>;

    /// Returns all the files in `prefix` if the `prefix` is already a leaf dir,
    /// or all paths between the `prefix` and the first occurrence of the `delimiter` if it is provided.
    async fn list_dir(
        &self,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Result<ListEntryStream>;

    /// Get object reader for one file
    fn file_reader(&self, file: FileMeta) -> Result<Arc<dyn ObjectReader>>;
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
    /// Create an empty registry that object stores can be registered into.
    pub fn new() -> Self {
        Self {
            object_stores: RwLock::new(HashMap::new()),
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

    /// Get a suitable store for the URI based on it's scheme. For example:
    /// URI with scheme file or no schema will return the default LocalFS store,
    /// URI with scheme s3 will return the S3 store if it's registered.
    pub fn get_by_uri(&self, uri: &str) -> Result<Arc<dyn ObjectStore>> {
        if let Some((scheme, _)) = uri.split_once(':') {
            let stores = self.object_stores.read().unwrap();
            stores
                .get(&*scheme.to_lowercase())
                .map(Clone::clone)
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "No suitable object store found for {}",
                        scheme
                    ))
                })
        } else {
            Ok(Arc::new(LocalFileSystem))
        }
    }
}
