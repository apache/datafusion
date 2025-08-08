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
                source: "empty path".into(),
            })?
            .as_ref()
            .to_string();
        let path: Path = parts.collect();
        let store = self.stores.get(&key).ok_or_else(|| Error::Generic {
            store: "VirtualObjectStore",
            source: format!("ObjectStore not found for prefix {key}").into(),
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
        let entries: Vec<(String, Arc<dyn ObjectStore>)> = self
            .stores
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        let streams = entries.into_iter().map(move |(key, store)| {
            let base = Path::from(key);
            let prefix = prefix_owned.as_ref().map(|p| base.child(p.as_ref()));
            store.list(prefix.as_ref()).map_ok(move |mut meta| {
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
            let p = prefix.map(|x| base.child(x.as_ref()));
            let result = store.list_with_delimiter(p.as_ref()).await?;
            objects.extend(result.objects.into_iter().map(|mut m| {
                m.location = base.child(m.location.as_ref());
                m
            }));
            common_prefixes.extend(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|p| base.child(p.as_ref())),
            );
        }
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
