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

use std::{
    fmt,
    str::FromStr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreRegistry};
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};
use parking_lot::RwLock;
use url::Url;

/// The profiling mode to use for an [`InstrumentedObjectStore`] instance. Collecting profiling
/// data will have a small negative impact on both CPU and memory usage. Default is `Disabled`
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum InstrumentedObjectStoreMode {
    /// Disable collection of profiling data
    #[default]
    Disabled,
    /// Enable collection of profiling data
    Enabled,
}

impl fmt::Display for InstrumentedObjectStoreMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for InstrumentedObjectStoreMode {
    type Err = DataFusionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "enabled" => Ok(Self::Enabled),
            _ => Err(DataFusionError::Execution(format!("Unrecognized mode {s}"))),
        }
    }
}

impl From<u8> for InstrumentedObjectStoreMode {
    fn from(value: u8) -> Self {
        match value {
            1 => InstrumentedObjectStoreMode::Enabled,
            _ => InstrumentedObjectStoreMode::Disabled,
        }
    }
}

/// Wrapped [`ObjectStore`] instances that record information for reporting on the usage of the
/// inner [`ObjectStore`]
#[derive(Debug)]
pub struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    instrument_mode: AtomicU8,
}

impl InstrumentedObjectStore {
    /// Returns a new [`InstrumentedObjectStore`] that wraps the provided [`ObjectStore`]
    fn new(object_store: Arc<dyn ObjectStore>, instrument_mode: AtomicU8) -> Self {
        Self {
            inner: object_store,
            instrument_mode,
        }
    }

    fn set_instrument_mode(&self, mode: InstrumentedObjectStoreMode) {
        self.instrument_mode.store(mode as u8, Ordering::Relaxed)
    }
}

impl fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode: InstrumentedObjectStoreMode =
            self.instrument_mode.load(Ordering::Relaxed).into();
        write!(
            f,
            "Instrumented Object Store: instrument_mode: {mode}, inner: {}",
            self.inner
        )
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }
}

/// Provides access to [`InstrumentedObjectStore`] instances that record requests for reporting
#[derive(Debug)]
pub struct InstrumentedObjectStoreRegistry {
    inner: Arc<dyn ObjectStoreRegistry>,
    instrument_mode: AtomicU8,
    stores: RwLock<Vec<Arc<InstrumentedObjectStore>>>,
}

impl InstrumentedObjectStoreRegistry {
    /// Returns a new [`InstrumentedObjectStoreRegistry`] that wraps the provided
    /// [`ObjectStoreRegistry`]
    pub fn new(
        registry: Arc<dyn ObjectStoreRegistry>,
        default_mode: InstrumentedObjectStoreMode,
    ) -> Self {
        Self {
            inner: registry,
            instrument_mode: AtomicU8::new(default_mode as u8),
            stores: RwLock::new(Vec::new()),
        }
    }

    /// Provides access to all of the [`InstrumentedObjectStore`]s managed by this
    /// [`InstrumentedObjectStoreRegistry`]
    pub fn stores(&self) -> Vec<Arc<InstrumentedObjectStore>> {
        self.stores.read().clone()
    }

    /// Returns the current [`InstrumentedObjectStoreMode`] for this
    /// [`InstrumentedObjectStoreRegistry`]
    pub fn mode(&self) -> InstrumentedObjectStoreMode {
        self.instrument_mode.load(Ordering::Relaxed).into()
    }

    /// Sets the [`InstrumentedObjectStoreMode`] for this [`InstrumentedObjectStoreRegistry`]
    pub fn set_instrument_mode(&self, mode: InstrumentedObjectStoreMode) {
        self.instrument_mode.store(mode as u8, Ordering::Relaxed);
        for s in self.stores.read().iter() {
            s.set_instrument_mode(mode)
        }
    }
}

impl ObjectStoreRegistry for InstrumentedObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        let mode = self.instrument_mode.load(Ordering::Relaxed);
        let instrumented =
            Arc::new(InstrumentedObjectStore::new(store, AtomicU8::new(mode)));
        self.stores.write().push(Arc::clone(&instrumented));
        self.inner.register_store(url, instrumented)
    }

    fn get_store(&self, url: &Url) -> datafusion::common::Result<Arc<dyn ObjectStore>> {
        self.inner.get_store(url)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;

    use super::*;

    #[test]
    fn instrumented_mode() {
        assert!(matches!(
            InstrumentedObjectStoreMode::default(),
            InstrumentedObjectStoreMode::Disabled
        ));

        assert!(matches!(
            "dIsABleD".parse().unwrap(),
            InstrumentedObjectStoreMode::Disabled
        ));
        assert!(matches!(
            "EnABlEd".parse().unwrap(),
            InstrumentedObjectStoreMode::Enabled
        ));
        assert!("does_not_exist"
            .parse::<InstrumentedObjectStoreMode>()
            .is_err());

        assert!(matches!(0.into(), InstrumentedObjectStoreMode::Disabled));
        assert!(matches!(1.into(), InstrumentedObjectStoreMode::Enabled));
        assert!(matches!(2.into(), InstrumentedObjectStoreMode::Disabled));
    }

    #[test]
    fn instrumented_registry() {
        let reg = Arc::new(InstrumentedObjectStoreRegistry::new(
            Arc::new(DefaultObjectStoreRegistry::new()),
            InstrumentedObjectStoreMode::default(),
        ));
        assert!(reg.stores().is_empty());

        let store = object_store::memory::InMemory::new();
        let url = "mem://test".parse().unwrap();
        let registered = reg.register_store(&url, Arc::new(store));
        assert!(registered.is_none());

        let fetched = reg.get_store(&url);
        assert!(fetched.is_ok());
        assert_eq!(reg.stores().len(), 1);
    }
}
