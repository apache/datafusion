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

//! Virtual object store implementation for DataFusion.
//!
//! `VirtualObjectStore` enables routing object operations to multiple underlying stores
//! based on the first segment (prefix) of the object path. This allows, for example,
//! mixing S3, local filesystem, or other stores under a single unified interface.
//!
//! Currently `VirtualObjectStore` only supports read operations such as `get` and
//! `list`. Write operations like `put` and `delete` are not implemented.
//!
//! # Configuration
//!
//! Create a mapping from string prefixes to concrete `ObjectStore` implementations:
//!
//! ```rust
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use object_store::{memory::InMemory, path::Path, ObjectStore};
//! use datafusion_execution::virtual_object_store::VirtualObjectStore;
//!
//! let mut stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
//! // Prefix "s3" routes to S3 store
//! // stores.insert("s3".into(), Arc::new(S3::new(...)));
//! // Prefix "fs" routes to local filesystem
//! // stores.insert("fs".into(), Arc::new(LocalFileSystem::new()));
//! // For testing, use an in-memory store at prefix "mem"
//! stores.insert("mem".into(), Arc::new(InMemory::new()));
//!
//! let vos = VirtualObjectStore::new(stores);
//! ```
//!
//! # Example Usage
//!
//! ```rust, ignore
//! use object_store::path::Path;
//! # let vos: VirtualObjectStore = unimplemented!();
//!
//! // List objects under the "mem" prefix
//! let all = vos.list(Some(&Path::from("mem/"))).collect::<Vec<_>>().await?;
//! # Ok::<_, object_store::Error>(())
//! ```

use async_trait::async_trait;
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{
    path::Path, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use std::{collections::HashMap, fmt, sync::Arc};
/// A virtual [`ObjectStore`] that routes requests to underlying stores based on
/// the first path segment.
#[derive(Clone)]
pub struct VirtualObjectStore {
    /// Mapping from path prefix to concrete [`ObjectStore`]
    pub stores: HashMap<String, Arc<dyn ObjectStore>>,
}

impl VirtualObjectStore {
    /// Create a new [`VirtualObjectStore`] from the provided mapping
    pub fn new(stores: HashMap<String, Arc<dyn ObjectStore>>) -> Self {
        Self { stores }
    }

    /// Resolve the given [`Path`] to the underlying store and the remaining path
    fn resolve(&self, location: &Path) -> Result<(&Arc<dyn ObjectStore>, Path)> {
        let mut parts = location.parts();
        let key = parts
            .next()
            .ok_or_else(|| Error::Generic {
                store: "VirtualObjectStore",
                source: format!("empty path in location '{location}'").into(),
            })?
            .as_ref()
            .to_string();
        let path: Path = parts.collect();
        let store = self.stores.get(&key).ok_or_else(|| Error::Generic {
            store: "VirtualObjectStore",
            source: format!(
                "ObjectStore not found for prefix '{key}' in location '{location}'"
            )
            .into(),
        })?;
        Ok((store, path))
    }
}

impl fmt::Display for VirtualObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VirtualObjectStore")
    }
}

impl fmt::Debug for VirtualObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VirtualObjectStore")
            .field("stores", &self.stores.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[async_trait]
impl ObjectStore for VirtualObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        // TODO: Implement write operations if needed
        Err(Error::NotSupported {
            source: std::io::Error::other(
                "VirtualObjectStore does not support write operations",
            )
            .into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // TODO: Implement write operations if needed
        Err(Error::NotSupported {
            source: std::io::Error::other(
                "VirtualObjectStore does not support write operations",
            )
            .into(),
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (store, path) = self.resolve(location)?;
        store.get_opts(&path, options).await
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        // TODO: Implement write operations if needed
        Err(Error::NotSupported {
            source: std::io::Error::other(
                "VirtualObjectStore does not support write operations",
            )
            .into(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_owned = prefix.cloned();
        // Short-circuit when prefix targets a single store key
        if let Some(ref pfx) = prefix_owned {
            // Normalize prefix: skip empty segments to handle trailing slashes
            let mut p_parts = pfx.parts().filter(|seg| !seg.as_ref().is_empty());
            if let Some(store_key) = p_parts.next().map(|s| s.as_ref().to_string()) {
                // Collect remaining path segments
                let remainder: Path = p_parts.collect();
                if let Some(store) = self.stores.get(&store_key) {
                    let base = Path::from(store_key.clone());
                    let inner_prefix = if remainder.as_ref().is_empty() {
                        None
                    } else {
                        Some(&remainder)
                    };
                    let single_stream =
                        store.list(inner_prefix).map_ok(move |mut meta| {
                            // Join base and meta.location using string formatting to
                            // preserve any nested paths
                            meta.location = Path::from(format!(
                                "{}/{}",
                                base.as_ref(),
                                meta.location.as_ref()
                            ));
                            meta
                        });
                    return single_stream.boxed();
                } else {
                    // No matching store, return empty stream
                    return stream::empty().boxed();
                }
            }
        }
        let entries: Vec<(String, Arc<dyn ObjectStore>)> = self
            .stores
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let streams = entries.into_iter().map(move |(key, store)| {
            let base = Path::from(key);
            // Join prefix using child if present
            let prefix = prefix_owned.as_ref().map(|p| base.child(p.as_ref()));
            store.list(prefix.as_ref()).map_ok(move |mut meta| {
                // Use Path::child to join base and meta.location
                meta.location = base.child(meta.location.as_ref());
                meta
            })
        });
        stream::iter(streams).flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        for (key, store) in &self.stores {
            let base = Path::from(key.clone());
            let p =
                prefix.map(|x| Path::from(format!("{}/{}", base.as_ref(), x.as_ref())));
            let result = store.list_with_delimiter(p.as_ref()).await?;
            objects.extend(result.objects.into_iter().map(|mut m| {
                m.location =
                    Path::from(format!("{}/{}", base.as_ref(), m.location.as_ref()));
                m
            }));
            common_prefixes.extend(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|p| Path::from(format!("{}/{}", base.as_ref(), p.as_ref()))),
            );
        }
        // Sort merged results for deterministic ordering across stores
        objects.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));
        common_prefixes.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(ListResult {
            objects,
            common_prefixes,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        // TODO: Implement write operations if needed
        Err(Error::NotSupported {
            source: std::io::Error::other(
                "VirtualObjectStore does not support write operations",
            )
            .into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        // TODO: Implement write operations if needed
        Err(Error::NotSupported {
            source: std::io::Error::other(
                "VirtualObjectStore does not support write operations",
            )
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::{memory::InMemory, path::Path, ObjectStore};

    /// Helper to collect list results into Vec<String>
    async fn collect_list(
        store: &VirtualObjectStore,
        prefix: Option<&Path>,
    ) -> Vec<String> {
        store
            .list(prefix)
            .map_ok(|meta| meta.location.as_ref().to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn list_empty_prefix_returns_all() {
        let mut stores = HashMap::new();
        let a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let b = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        a.put(&Path::from("a1"), b"data1".to_vec().into())
            .await
            .unwrap();
        b.put(&Path::from("b1"), b"data2".to_vec().into())
            .await
            .unwrap();
        stores.insert("a".to_string(), a);
        stores.insert("b".to_string(), b);
        let v = VirtualObjectStore::new(stores);
        let result = collect_list(&v, None).await;
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"a/a1".to_string()));
        assert!(result.contains(&"b/b1".to_string()));
    }

    #[tokio::test]
    async fn list_no_matching_prefix_empty() {
        let mut stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        stores.insert(
            "x".to_string(),
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        );
        let v = VirtualObjectStore::new(stores);
        let result = collect_list(&v, Some(&Path::from("nope"))).await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn list_nested_prefix_passes_remainder() {
        let mut stores = HashMap::new();
        let a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        a.put(&Path::from("sub/key"), b"x".to_vec().into())
            .await
            .unwrap();
        stores.insert("a".to_string(), a);
        let v = VirtualObjectStore::new(stores);
        let result = collect_list(&v, Some(&Path::from("a/sub"))).await;
        assert_eq!(result, vec!["a/sub/key".to_string()]);
    }

    #[tokio::test]
    async fn list_with_delimiter_sorted() {
        let mut stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        let a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        a.put(&Path::from("z1"), b"x".to_vec().into())
            .await
            .unwrap();
        let b = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        b.put(&Path::from("a1"), b"y".to_vec().into())
            .await
            .unwrap();
        stores.insert("a".to_string(), a);
        stores.insert("b".to_string(), b);
        let v = VirtualObjectStore::new(stores);
        let res = v.list_with_delimiter(None).await.unwrap();
        let locs: Vec<_> = res
            .objects
            .into_iter()
            .map(|o| o.location.as_ref().to_string())
            .collect();
        assert_eq!(locs, vec!["a/z1".to_string(), "b/a1".to_string()]);
    }
}
