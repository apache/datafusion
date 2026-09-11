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

use crate::{Error, Result, Storage};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc,
};
use url::{Position, Url};

/// A storage namespace, normalized independently of the chosen backend.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageUrl(Url);
impl StorageUrl {
    pub fn parse(value: impl AsRef<str>) -> Result<Self> {
        Self::new(&Url::parse(value.as_ref())?)
    }
    pub fn new(url: &Url) -> Result<Self> {
        let key = format!(
            "{}://{}",
            url.scheme(),
            &url[Position::BeforeHost..Position::AfterPort]
        );
        let mut url = Url::parse(&key)?;
        url.set_path("/");
        Ok(Self(url))
    }
    pub fn local_filesystem() -> Self {
        Self::parse("file://").unwrap()
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl AsRef<Url> for StorageUrl {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}
impl Display for StorageUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// A single authoritative map of storage registrations.
#[derive(Debug, Default)]
pub struct StorageRegistry {
    storages: RwLock<HashMap<StorageUrl, Arc<StorageBinding>>>,
}
impl StorageRegistry {
    pub fn register(
        &self,
        url: &Url,
        storage: Arc<dyn Storage>,
    ) -> Result<Option<Arc<StorageBinding>>> {
        let key = StorageUrl::new(url)?;
        let storage = Arc::new(StorageBinding::new(key.clone(), storage));
        Ok(self.storages.write().insert(key, storage))
    }
    /// Install a default without replacing an application registration.
    pub fn register_default(&self, url: &Url, storage: Arc<dyn Storage>) -> Result<()> {
        let key = StorageUrl::new(url)?;
        self.storages
            .write()
            .entry(key.clone())
            .or_insert_with(|| Arc::new(StorageBinding::new(key, storage)));
        Ok(())
    }
    pub fn get(&self, url: &Url) -> Result<Arc<StorageBinding>> {
        let key = StorageUrl::new(url)?;
        self.storages
            .read()
            .get(&key)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("storage registration {key}")))
    }
    pub fn deregister(&self, url: &Url) -> Result<Arc<StorageBinding>> {
        let key = StorageUrl::new(url)?;
        self.storages
            .write()
            .remove(&key)
            .ok_or_else(|| Error::NotFound(format!("storage registration {key}")))
    }
}

impl AsRef<str> for StorageUrl {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// An immutable registration: a namespace and its backend.
/// Plans retain this binding across replacement or deregistration.
#[derive(Debug)]
pub struct StorageBinding {
    id: u64,
    url: StorageUrl,
    storage: Arc<dyn Storage>,
}
impl StorageBinding {
    pub fn new(url: StorageUrl, storage: Arc<dyn Storage>) -> Self {
        static NEXT_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_ne!(id, 0, "storage binding identity exhausted");
        Self { id, url, storage }
    }
    /// Identity of this registration, independent of its replaceable URL.
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn url(&self) -> &StorageUrl {
        &self.url
    }
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
    pub async fn open(
        &self,
        file: &crate::FileInfo,
        context: crate::FileAccessContext,
    ) -> Result<Arc<dyn crate::FileReader>> {
        self.storage.open(&file.location, context).await
    }
    pub async fn stat(
        &self,
        path: &crate::path::Path,
        context: &crate::FileAccessContext,
    ) -> Result<crate::FileInfo> {
        self.storage.stat(path, context).await
    }
    pub fn list(
        &self,
        path: &crate::path::Path,
        context: crate::FileAccessContext,
    ) -> futures::stream::BoxStream<'_, Result<crate::FileInfo>> {
        self.storage.list(path, context)
    }
    pub async fn list_with_delimiter(
        &self,
        path: &crate::path::Path,
        context: &crate::FileAccessContext,
    ) -> Result<crate::DirectoryListing> {
        self.storage.list_with_delimiter(path, context).await
    }
    pub async fn writer(
        &self,
        path: &crate::path::Path,
        options: crate::WriterOptions,
        context: crate::FileAccessContext,
    ) -> Result<crate::FileOutput> {
        self.storage.writer(path, options, context).await
    }
}
