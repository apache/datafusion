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
//!
//! // Copy a file from one prefix to another
//! vos.copy(&Path::from("mem/file1"), &Path::from("mem_backup/file1")).await?;
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
                source: format!("empty path in location '{}'", location).into(),
            })?
            .as_ref()
            .to_string();
        let path: Path = parts.collect();
        let store = self.stores.get(&key).ok_or_else(|| Error::Generic {
            store: "VirtualObjectStore",
            source: format!(
                "ObjectStore not found for prefix '{}' in location '{}'",
                key, location
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
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let (store, path) = self.resolve(location)?;
        store.put_opts(&path, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let (store, path) = self.resolve(location)?;
        store.put_multipart_opts(&path, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (store, path) = self.resolve(location)?;
        store.get_opts(&path, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let (store, path) = self.resolve(location)?;
        store.delete(&path).await
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
                            // Use Path::child to join base and meta.location
                            meta.location = base.child(meta.location.as_ref());
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

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let (from_store, from_path) = self.resolve(from)?;
        let (to_store, to_path) = self.resolve(to)?;
        if Arc::ptr_eq(from_store, to_store) {
            from_store.copy(&from_path, &to_path).await
        } else {
            let bytes = from_store.get(&from_path).await?.bytes().await?;
            to_store.put(&to_path, bytes.into()).await.map(|_| ())
        }
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let (from_store, from_path) = self.resolve(from)?;
        let (to_store, to_path) = self.resolve(to)?;
        if Arc::ptr_eq(from_store, to_store) {
            from_store.copy_if_not_exists(&from_path, &to_path).await
        } else {
            match to_store.head(&to_path).await {
                Ok(_) => Err(Error::AlreadyExists {
                    path: to_path.to_string(),
                    source: "destination exists".into(),
                }),
                Err(Error::NotFound { .. }) => {
                    let bytes = from_store.get(&from_path).await?.bytes().await?;
                    to_store.put(&to_path, bytes.into()).await.map(|_| ())
                }
                Err(e) => Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::{
        memory::InMemory, path::Path, Error, ObjectStore, PutMultipartOptions,
    };

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
    async fn copy_if_not_exists_destination_exists() {
        let mut stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        let from = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let to = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        from.put(&Path::from("f1"), b"v".to_vec().into())
            .await
            .unwrap();
        to.put(&Path::from("f1"), b"old".to_vec().into())
            .await
            .unwrap();
        stores.insert("s".to_string(), from);
        stores.insert("t".to_string(), to.clone());
        let v = VirtualObjectStore::new(stores);
        let err = v
            .copy_if_not_exists(&Path::from("t/f1"), &Path::from("t/f1"))
            .await;
        assert!(matches!(err.unwrap_err(), Error::AlreadyExists { .. }));
    }

    #[tokio::test]
    async fn copy_if_not_exists_streams_copy() {
        let mut stores: HashMap<String, Arc<dyn ObjectStore>> = HashMap::new();
        let from = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let to = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        from.put(&Path::from("f1"), b"123".to_vec().into())
            .await
            .unwrap();
        stores.insert("src".to_string(), from.clone());
        stores.insert("dst".to_string(), to.clone());
        let v = VirtualObjectStore::new(stores);
        v.copy_if_not_exists(&Path::from("src/f1"), &Path::from("dst/f1"))
            .await
            .unwrap();
        // Verify data copied correctly
        let data = to
            .get(&Path::from("f1"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, b"123".to_vec());
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

    #[tokio::test]
    async fn multipart_upload_basic_and_abort() {
        let mut stores = HashMap::new();
        let a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        stores.insert("a".to_string(), a.clone());
        let v = VirtualObjectStore::new(stores);
        // Initiate multipart upload
        let mut upload = v
            .put_multipart_opts(&Path::from("a/file"), PutMultipartOptions::default())
            .await
            .expect("multipart upload should succeed");
        // Abort should succeed
        let res = upload.abort().await;
        assert!(res.is_ok(), "abort of multipart upload failed");
    }

    #[tokio::test]
    async fn multipart_upload_no_matching_prefix_error() {
        let mut stores = HashMap::new();
        stores.insert(
            "x".to_string(),
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        );
        let v = VirtualObjectStore::new(stores);
        let err = v
            .put_multipart_opts(&Path::from("nope/file"), PutMultipartOptions::default())
            .await;
        assert!(err.is_err(), "expected error for no matching prefix");
        match err.unwrap_err() {
            Error::Generic { store, source } => {
                assert_eq!(store, "VirtualObjectStore");
                assert!(format!("{}", source).contains("prefix 'nope'"));
            }
            other => panic!("unexpected error type: {:?}", other),
        }
    }

    #[tokio::test]
    async fn multipart_upload_complete_and_put_part() {
        let mut stores = HashMap::new();
        let a = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        stores.insert("a".to_string(), a.clone());
        let v = VirtualObjectStore::new(stores);
        // Initiate multipart upload
        let mut upload = v
            .put_multipart_opts(&Path::from("a/complete"), PutMultipartOptions::default())
            .await
            .expect("multipart upload should succeed");
        // Upload parts
        upload.put_part(b"foo".to_vec().into()).await.unwrap();
        upload.put_part(b"bar".to_vec().into()).await.unwrap();
        // Complete upload
        upload.complete().await.unwrap();
        // Verify data on underlying store
        let data = a
            .get(&Path::from("complete"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data, b"foobar".to_vec());
    }
}
