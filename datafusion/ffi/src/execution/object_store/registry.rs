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

//! FFI support for [`ObjectStoreRegistry`].
//!
//! Sharing the registry is what makes object stores visible across the
//! boundary in both directions: a store registered by a table provider during
//! planning is found by the session at execution time, and a store registered
//! on the session by the host is found by the provider.

use std::ffi::c_void;
use std::sync::Arc;

use datafusion_common::{Result, ffi_datafusion_err};
use datafusion_execution::object_store::ObjectStoreRegistry;
use object_store::ObjectStore;
use stabby::string::String as SString;
use tokio::runtime::Handle;
use url::Url;

use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult, sresult_return};

use super::store::FFI_ObjectStore;

/// A stable struct for sharing [`ObjectStoreRegistry`] across FFI boundaries.
///
/// All three methods are synchronous, so unlike most of this crate no futures
/// are involved; a registry lookup never performs I/O.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ObjectStoreRegistry {
    /// Register a store, returning the store it replaced if any.
    pub register_store: unsafe extern "C" fn(
        registry: &Self,
        url: SString,
        store: FFI_ObjectStore,
    ) -> FFI_Option<FFI_ObjectStore>,

    /// Deregister the store registered for `url`.
    pub deregister_store: unsafe extern "C" fn(
        registry: &Self,
        url: SString,
    ) -> FFI_Result<FFI_ObjectStore>,

    /// Get a suitable store for `url`.
    pub get_store: unsafe extern "C" fn(
        registry: &Self,
        url: SString,
    ) -> FFI_Result<FFI_ObjectStore>,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the registry.
    pub clone: unsafe extern "C" fn(registry: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the
    /// registry. The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ObjectStoreRegistry {}
unsafe impl Sync for FFI_ObjectStoreRegistry {}

struct RegistryPrivateData {
    registry: Arc<dyn ObjectStoreRegistry>,
    runtime: Option<Handle>,
}

impl FFI_ObjectStoreRegistry {
    fn private_data(&self) -> &RegistryPrivateData {
        unsafe { &*(self.private_data as *const RegistryPrivateData) }
    }

    fn inner(&self) -> &Arc<dyn ObjectStoreRegistry> {
        &self.private_data().registry
    }

    fn runtime(&self) -> &Option<Handle> {
        &self.private_data().runtime
    }

    /// Create a new [`FFI_ObjectStoreRegistry`] from a local registry.
    ///
    /// `runtime` is the tokio runtime handle of the providing library, attached
    /// to any store handed out by this registry so that stores which spawn
    /// tasks work when driven by a foreign executor.
    pub fn new(registry: Arc<dyn ObjectStoreRegistry>, runtime: Option<Handle>) -> Self {
        Self {
            register_store: register_store_fn_wrapper,
            deregister_store: deregister_store_fn_wrapper,
            get_store: get_store_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(RegistryPrivateData {
                registry,
                runtime,
            })) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }

    /// If this registry originated in the current library, return the
    /// underlying [`ObjectStoreRegistry`] directly.
    pub fn as_local(&self) -> Option<Arc<dyn ObjectStoreRegistry>> {
        ((self.library_marker_id)() == crate::get_library_marker_id())
            .then(|| Arc::clone(self.inner()))
    }
}

fn parse_url(url: &SString) -> Result<Url> {
    Url::parse(url.as_str())
        .map_err(|e| ffi_datafusion_err!("Invalid object store URL '{url}': {e}"))
}

unsafe extern "C" fn register_store_fn_wrapper(
    registry: &FFI_ObjectStoreRegistry,
    url: SString,
    store: FFI_ObjectStore,
) -> FFI_Option<FFI_ObjectStore> {
    let Ok(url) = parse_url(&url) else {
        // `register_store` cannot report an error. An unparseable URL can only
        // come from a caller that built one by hand, and silently dropping the
        // registration would be worse than a log line.
        log::warn!("Ignoring object store registration for unparseable URL '{url}'");
        return FFI_Option::None;
    };

    let runtime = registry.runtime().clone();
    let store = Arc::<dyn ObjectStore>::from(store);

    match registry.inner().register_store(&url, store) {
        Some(previous) => FFI_Option::Some(FFI_ObjectStore::new(previous, runtime)),
        None => FFI_Option::None,
    }
}

unsafe extern "C" fn deregister_store_fn_wrapper(
    registry: &FFI_ObjectStoreRegistry,
    url: SString,
) -> FFI_Result<FFI_ObjectStore> {
    let url = sresult_return!(parse_url(&url));
    let runtime = registry.runtime().clone();
    let store = sresult_return!(registry.inner().deregister_store(&url));

    FFI_Result::Ok(FFI_ObjectStore::new(store, runtime))
}

unsafe extern "C" fn get_store_fn_wrapper(
    registry: &FFI_ObjectStoreRegistry,
    url: SString,
) -> FFI_Result<FFI_ObjectStore> {
    let url = sresult_return!(parse_url(&url));
    let runtime = registry.runtime().clone();

    sresult!(
        registry
            .inner()
            .get_store(&url)
            .map(|store| FFI_ObjectStore::new(store, runtime))
    )
}

unsafe extern "C" fn clone_fn_wrapper(
    registry: &FFI_ObjectStoreRegistry,
) -> FFI_ObjectStoreRegistry {
    let private_data = registry.private_data();
    FFI_ObjectStoreRegistry::new(
        Arc::clone(&private_data.registry),
        private_data.runtime.clone(),
    )
}

unsafe extern "C" fn release_fn_wrapper(registry: &mut FFI_ObjectStoreRegistry) {
    unsafe {
        debug_assert!(!registry.private_data.is_null());
        drop(Box::from_raw(
            registry.private_data as *mut RegistryPrivateData,
        ));
        registry.private_data = std::ptr::null_mut();
    }
}

impl Clone for FFI_ObjectStoreRegistry {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_ObjectStoreRegistry {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// An [`ObjectStoreRegistry`] backed by a foreign [`FFI_ObjectStoreRegistry`].
///
/// Unlike [`super::ForeignObjectStore`], no attempt is made to unwrap a
/// registry that is being sent back toward its owning library. A registry
/// crosses the boundary once per session and its methods perform no I/O, so an
/// extra layer of indirection costs nothing measurable; the stores it returns
/// are still unwrapped to their local form by
/// [`FFI_ObjectStore::as_local`].
#[derive(Debug)]
pub struct ForeignObjectStoreRegistry {
    registry: FFI_ObjectStoreRegistry,
}

unsafe impl Send for ForeignObjectStoreRegistry {}
unsafe impl Sync for ForeignObjectStoreRegistry {}

impl From<FFI_ObjectStoreRegistry> for ForeignObjectStoreRegistry {
    fn from(registry: FFI_ObjectStoreRegistry) -> Self {
        Self { registry }
    }
}

impl From<FFI_ObjectStoreRegistry> for Arc<dyn ObjectStoreRegistry> {
    fn from(registry: FFI_ObjectStoreRegistry) -> Self {
        match registry.as_local() {
            Some(local) => local,
            None => Arc::new(ForeignObjectStoreRegistry::from(registry)),
        }
    }
}

impl ObjectStoreRegistry for ForeignObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        // Attach the runtime this registration is happening on. A store that
        // spawns tasks or uses timers needs it once the other side starts
        // driving it from its own executor.
        let runtime = Handle::try_current().ok();
        let previous = unsafe {
            (self.registry.register_store)(
                &self.registry,
                url.as_str().into(),
                FFI_ObjectStore::new(store, runtime),
            )
        };

        previous.into_option().map(Arc::<dyn ObjectStore>::from)
    }

    fn deregister_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let store = unsafe {
            (self.registry.deregister_store)(&self.registry, url.as_str().into())
        };
        df_result!(store).map(Arc::<dyn ObjectStore>::from)
    }

    fn get_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        let store =
            unsafe { (self.registry.get_store)(&self.registry, url.as_str().into()) };
        df_result!(store).map(Arc::<dyn ObjectStore>::from)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_execution::object_store::DefaultObjectStoreRegistry;
    use object_store::memory::InMemory;

    use super::*;

    fn foreign_registry(
        registry: Arc<dyn ObjectStoreRegistry>,
    ) -> ForeignObjectStoreRegistry {
        let mut ffi = FFI_ObjectStoreRegistry::new(registry, None);
        // Force the foreign path so the test exercises the FFI functions rather
        // than the local unwrap.
        ffi.library_marker_id = crate::mock_foreign_marker_id;
        ForeignObjectStoreRegistry::from(ffi)
    }

    #[test]
    fn register_then_get_round_trip() -> Result<()> {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let foreign =
            foreign_registry(Arc::clone(&local) as Arc<dyn ObjectStoreRegistry>);

        let url = Url::parse("s3://bucket").unwrap();
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        assert!(foreign.register_store(&url, store).is_none());

        // Visible through the foreign handle...
        assert!(foreign.get_store(&url).is_ok());
        // ...and in the underlying local registry.
        assert!(local.get_store(&url).is_ok());

        Ok(())
    }

    /// A store registered on the local registry must be reachable through the
    /// foreign handle, so a host that registers a store on the session before
    /// handing it across the boundary is honoured on the far side.
    #[test]
    fn store_registered_locally_is_visible_to_foreign() -> Result<()> {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let url = Url::parse("s3://host-registered").unwrap();
        local.register_store(&url, Arc::new(InMemory::new()));

        let foreign =
            foreign_registry(Arc::clone(&local) as Arc<dyn ObjectStoreRegistry>);
        assert!(foreign.get_store(&url).is_ok());

        Ok(())
    }

    #[test]
    fn missing_store_reports_error() {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let foreign = foreign_registry(local as Arc<dyn ObjectStoreRegistry>);

        let url = Url::parse("s3://never-registered").unwrap();
        assert!(foreign.get_store(&url).is_err());
    }

    #[test]
    fn register_returns_replaced_store() -> Result<()> {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let foreign = foreign_registry(local as Arc<dyn ObjectStoreRegistry>);

        let url = Url::parse("s3://bucket").unwrap();
        assert!(
            foreign
                .register_store(&url, Arc::new(InMemory::new()))
                .is_none()
        );
        assert!(
            foreign
                .register_store(&url, Arc::new(InMemory::new()))
                .is_some()
        );

        Ok(())
    }

    #[test]
    fn deregister_round_trip() -> Result<()> {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let foreign = foreign_registry(local as Arc<dyn ObjectStoreRegistry>);

        let url = Url::parse("s3://bucket").unwrap();
        foreign.register_store(&url, Arc::new(InMemory::new()));

        assert!(foreign.deregister_store(&url).is_ok());
        assert!(foreign.get_store(&url).is_err());

        Ok(())
    }

    #[test]
    fn local_registry_is_unwrapped() {
        let local = Arc::new(DefaultObjectStoreRegistry::new());
        let ffi = FFI_ObjectStoreRegistry::new(
            Arc::clone(&local) as Arc<dyn ObjectStoreRegistry>,
            None,
        );
        assert!(ffi.as_local().is_some());

        let registry = Arc::<dyn ObjectStoreRegistry>::from(ffi);
        // The same registry instance, not a wrapper.
        let url = Url::parse("s3://bucket").unwrap();
        registry.register_store(&url, Arc::new(InMemory::new()));
        assert!(local.get_store(&url).is_ok());
    }
}
