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

use std::any::Any;
use std::ffi::c_void;
use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RString, RVec};
use datafusion_catalog::{CatalogProvider, CatalogProviderList};
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use tokio::runtime::Handle;

use crate::catalog_provider::{FFI_CatalogProvider, ForeignCatalogProvider};
use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;

/// A stable struct for sharing [`CatalogProviderList`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_CatalogProviderList {
    /// Register a catalog
    pub register_catalog: unsafe extern "C" fn(
        &Self,
        name: RString,
        catalog: &FFI_CatalogProvider,
    ) -> ROption<FFI_CatalogProvider>,

    /// List of existing catalogs
    pub catalog_names: unsafe extern "C" fn(&Self) -> RVec<RString>,

    /// Access a catalog
    pub catalog:
        unsafe extern "C" fn(&Self, name: RString) -> ROption<FFI_CatalogProvider>,

    pub logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignCatalogProviderList`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_CatalogProviderList {}
unsafe impl Sync for FFI_CatalogProviderList {}

struct ProviderPrivateData {
    provider: Arc<dyn CatalogProviderList + Send>,
    runtime: Option<Handle>,
}

impl FFI_CatalogProviderList {
    unsafe fn inner(&self) -> &Arc<dyn CatalogProviderList + Send> {
        unsafe {
            let private_data = self.private_data as *const ProviderPrivateData;
            &(*private_data).provider
        }
    }

    unsafe fn runtime(&self) -> Option<Handle> {
        unsafe {
            let private_data = self.private_data as *const ProviderPrivateData;
            (*private_data).runtime.clone()
        }
    }
}

unsafe extern "C" fn catalog_names_fn_wrapper(
    provider: &FFI_CatalogProviderList,
) -> RVec<RString> {
    unsafe {
        let names = provider.inner().catalog_names();
        names.into_iter().map(|s| s.into()).collect()
    }
}

unsafe extern "C" fn register_catalog_fn_wrapper(
    provider: &FFI_CatalogProviderList,
    name: RString,
    catalog: &FFI_CatalogProvider,
) -> ROption<FFI_CatalogProvider> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();
        let catalog: Arc<dyn CatalogProvider + Send> = catalog.into();

        inner_provider
            .register_catalog(name.into(), catalog)
            .map(|catalog| {
                FFI_CatalogProvider::new_with_ffi_codec(
                    catalog,
                    runtime,
                    provider.logical_codec.clone(),
                )
            })
            .into()
    }
}

unsafe extern "C" fn catalog_fn_wrapper(
    provider: &FFI_CatalogProviderList,
    name: RString,
) -> ROption<FFI_CatalogProvider> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();
        inner_provider
            .catalog(name.as_str())
            .map(|catalog| {
                FFI_CatalogProvider::new_with_ffi_codec(
                    catalog,
                    runtime,
                    provider.logical_codec.clone(),
                )
            })
            .into()
    }
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_CatalogProviderList) {
    unsafe {
        debug_assert!(!provider.private_data.is_null());
        let private_data =
            Box::from_raw(provider.private_data as *mut ProviderPrivateData);
        drop(private_data);
        provider.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_CatalogProviderList,
) -> FFI_CatalogProviderList {
    unsafe {
        let old_private_data = provider.private_data as *const ProviderPrivateData;
        let runtime = (*old_private_data).runtime.clone();

        let private_data = Box::into_raw(Box::new(ProviderPrivateData {
            provider: Arc::clone(&(*old_private_data).provider),
            runtime,
        })) as *mut c_void;

        FFI_CatalogProviderList {
            register_catalog: register_catalog_fn_wrapper,
            catalog_names: catalog_names_fn_wrapper,
            catalog: catalog_fn_wrapper,
            logical_codec: provider.logical_codec.clone(),
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_CatalogProviderList {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_CatalogProviderList {
    /// Creates a new [`FFI_CatalogProviderList`].
    pub fn new(
        provider: Arc<dyn CatalogProviderList + Send>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let logical_codec =
            logical_codec.unwrap_or_else(|| Arc::new(DefaultLogicalExtensionCodec {}));
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.clone(),
            task_ctx_provider.clone(),
        );
        Self::new_with_ffi_codec(provider, runtime, logical_codec)
    }
    pub fn new_with_ffi_codec(
        provider: Arc<dyn CatalogProviderList + Send>,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            register_catalog: register_catalog_fn_wrapper,
            catalog_names: catalog_names_fn_wrapper,
            catalog: catalog_fn_wrapper,
            logical_codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_CatalogProviderList to interact with the foreign catalog provider list.
#[derive(Debug)]
pub struct ForeignCatalogProviderList(FFI_CatalogProviderList);

unsafe impl Send for ForeignCatalogProviderList {}
unsafe impl Sync for ForeignCatalogProviderList {}

impl From<&FFI_CatalogProviderList> for Arc<dyn CatalogProviderList + Send> {
    fn from(provider: &FFI_CatalogProviderList) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(unsafe { provider.inner() });
        }

        Arc::new(ForeignCatalogProviderList(provider.clone()))
            as Arc<dyn CatalogProviderList + Send>
    }
}

impl Clone for FFI_CatalogProviderList {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl CatalogProviderList for ForeignCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unsafe {
            let catalog = match catalog.as_any().downcast_ref::<ForeignCatalogProvider>()
            {
                Some(s) => &s.0,
                None => &FFI_CatalogProvider::new_with_ffi_codec(
                    catalog,
                    None,
                    self.0.logical_codec.clone(),
                ),
            };

            (self.0.register_catalog)(&self.0, name.into(), catalog)
                .map(|s| Arc::new(ForeignCatalogProvider(s)) as Arc<dyn CatalogProvider>)
                .into()
        }
    }

    fn catalog_names(&self) -> Vec<String> {
        unsafe {
            (self.0.catalog_names)(&self.0)
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        unsafe {
            (self.0.catalog)(&self.0, name.into())
                .map(|catalog| {
                    Arc::new(ForeignCatalogProvider(catalog)) as Arc<dyn CatalogProvider>
                })
                .into()
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::{MemoryCatalogProvider, MemoryCatalogProviderList};

    use super::*;

    #[test]
    fn test_round_trip_ffi_catalog_provider_list() {
        let prior_catalog = Arc::new(MemoryCatalogProvider::new());

        let catalog_list = Arc::new(MemoryCatalogProviderList::new());
        assert!(
            catalog_list
                .as_ref()
                .register_catalog("prior_catalog".to_owned(), prior_catalog)
                .is_none()
        );

        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let mut ffi_catalog_list =
            FFI_CatalogProviderList::new(catalog_list, None, task_ctx_provider, None);
        ffi_catalog_list.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_catalog_list: Arc<dyn CatalogProviderList + Send> =
            (&ffi_catalog_list).into();

        let prior_catalog_names = foreign_catalog_list.catalog_names();
        assert_eq!(prior_catalog_names.len(), 1);
        assert_eq!(prior_catalog_names[0], "prior_catalog");

        // Replace an existing catalog with one of the same name
        let returned_catalog = foreign_catalog_list.register_catalog(
            "prior_catalog".to_owned(),
            Arc::new(MemoryCatalogProvider::new()),
        );
        assert!(returned_catalog.is_some());
        assert_eq!(foreign_catalog_list.catalog_names().len(), 1);

        // Add a new catalog
        let returned_catalog = foreign_catalog_list.register_catalog(
            "second_catalog".to_owned(),
            Arc::new(MemoryCatalogProvider::new()),
        );
        assert!(returned_catalog.is_none());
        assert_eq!(foreign_catalog_list.catalog_names().len(), 2);

        // Retrieve non-existent catalog
        let returned_catalog = foreign_catalog_list.catalog("non_existent_catalog");
        assert!(returned_catalog.is_none());

        // Retrieve valid catalog
        let returned_catalog = foreign_catalog_list.catalog("second_catalog");
        assert!(returned_catalog.is_some());
    }

    #[test]
    fn test_ffi_catalog_provider_list_local_bypass() {
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());

        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let mut ffi_catalog_list =
            FFI_CatalogProviderList::new(catalog_list, None, task_ctx_provider, None);

        // Verify local libraries can be downcast to their original
        let foreign_catalog_list: Arc<dyn CatalogProviderList + Send> =
            (&ffi_catalog_list).into();
        assert!(
            foreign_catalog_list
                .as_any()
                .downcast_ref::<MemoryCatalogProviderList>()
                .is_some()
        );

        // Verify different library markers generate foreign providers
        ffi_catalog_list.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_catalog_list: Arc<dyn CatalogProviderList + Send> =
            (&ffi_catalog_list).into();
        assert!(
            foreign_catalog_list
                .as_any()
                .downcast_ref::<ForeignCatalogProviderList>()
                .is_some()
        );
    }
}
