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
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use datafusion_catalog::{CatalogProvider, SchemaProvider};
use datafusion_common::error::Result;
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::schema_provider::{FFI_SchemaProvider, ForeignSchemaProvider};
use crate::util::FFIResult;
use crate::{df_result, rresult_return};

/// A stable struct for sharing [`CatalogProvider`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_CatalogProvider {
    pub schema_names: unsafe extern "C" fn(provider: &Self) -> RVec<RString>,

    pub schema: unsafe extern "C" fn(
        provider: &Self,
        name: RString,
    ) -> ROption<FFI_SchemaProvider>,

    pub register_schema: unsafe extern "C" fn(
        provider: &Self,
        name: RString,
        schema: &FFI_SchemaProvider,
    )
        -> FFIResult<ROption<FFI_SchemaProvider>>,

    pub deregister_schema: unsafe extern "C" fn(
        provider: &Self,
        name: RString,
        cascade: bool,
    )
        -> FFIResult<ROption<FFI_SchemaProvider>>,

    pub logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignCatalogProvider`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_CatalogProvider {}
unsafe impl Sync for FFI_CatalogProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn CatalogProvider + Send>,
    runtime: Option<Handle>,
}

impl FFI_CatalogProvider {
    unsafe fn inner(&self) -> &Arc<dyn CatalogProvider + Send> {
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

unsafe extern "C" fn schema_names_fn_wrapper(
    provider: &FFI_CatalogProvider,
) -> RVec<RString> {
    unsafe {
        let names = provider.inner().schema_names();
        names.into_iter().map(|s| s.into()).collect()
    }
}

unsafe extern "C" fn schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
) -> ROption<FFI_SchemaProvider> {
    unsafe {
        let maybe_schema = provider.inner().schema(name.as_str());
        maybe_schema
            .map(|schema| {
                FFI_SchemaProvider::new_with_ffi_codec(
                    schema,
                    provider.runtime(),
                    provider.logical_codec.clone(),
                )
            })
            .into()
    }
}

unsafe extern "C" fn register_schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
    schema: &FFI_SchemaProvider,
) -> FFIResult<ROption<FFI_SchemaProvider>> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();
        let schema: Arc<dyn SchemaProvider + Send> = schema.into();

        let returned_schema =
            rresult_return!(inner_provider.register_schema(name.as_str(), schema))
                .map(|schema| {
                    FFI_SchemaProvider::new_with_ffi_codec(
                        schema,
                        runtime,
                        provider.logical_codec.clone(),
                    )
                })
                .into();

        RResult::ROk(returned_schema)
    }
}

unsafe extern "C" fn deregister_schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
    cascade: bool,
) -> FFIResult<ROption<FFI_SchemaProvider>> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();

        let maybe_schema =
            rresult_return!(inner_provider.deregister_schema(name.as_str(), cascade));

        RResult::ROk(
            maybe_schema
                .map(|schema| {
                    FFI_SchemaProvider::new_with_ffi_codec(
                        schema,
                        runtime,
                        provider.logical_codec.clone(),
                    )
                })
                .into(),
        )
    }
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_CatalogProvider) {
    unsafe {
        debug_assert!(!provider.private_data.is_null());
        let private_data =
            Box::from_raw(provider.private_data as *mut ProviderPrivateData);
        drop(private_data);
        provider.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_CatalogProvider,
) -> FFI_CatalogProvider {
    unsafe {
        let old_private_data = provider.private_data as *const ProviderPrivateData;
        let runtime = (*old_private_data).runtime.clone();

        let private_data = Box::into_raw(Box::new(ProviderPrivateData {
            provider: Arc::clone(&(*old_private_data).provider),
            runtime,
        })) as *mut c_void;

        FFI_CatalogProvider {
            schema_names: schema_names_fn_wrapper,
            schema: schema_fn_wrapper,
            register_schema: register_schema_fn_wrapper,
            deregister_schema: deregister_schema_fn_wrapper,
            logical_codec: provider.logical_codec.clone(),
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_CatalogProvider {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_CatalogProvider {
    /// Creates a new [`FFI_CatalogProvider`].
    pub fn new(
        provider: Arc<dyn CatalogProvider + Send>,
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
        provider: Arc<dyn CatalogProvider + Send>,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            schema_names: schema_names_fn_wrapper,
            schema: schema_fn_wrapper,
            register_schema: register_schema_fn_wrapper,
            deregister_schema: deregister_schema_fn_wrapper,
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
/// FFI_CatalogProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignCatalogProvider(pub(crate) FFI_CatalogProvider);

unsafe impl Send for ForeignCatalogProvider {}
unsafe impl Sync for ForeignCatalogProvider {}

impl From<&FFI_CatalogProvider> for Arc<dyn CatalogProvider + Send> {
    fn from(provider: &FFI_CatalogProvider) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(unsafe { provider.inner() });
        }

        Arc::new(ForeignCatalogProvider(provider.clone()))
            as Arc<dyn CatalogProvider + Send>
    }
}

impl Clone for FFI_CatalogProvider {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl CatalogProvider for ForeignCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        unsafe {
            (self.0.schema_names)(&self.0)
                .into_iter()
                .map(|s| s.into())
                .collect()
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        unsafe {
            let maybe_provider: Option<FFI_SchemaProvider> =
                (self.0.schema)(&self.0, name.into()).into();

            maybe_provider.map(|provider| {
                Arc::new(ForeignSchemaProvider(provider)) as Arc<dyn SchemaProvider>
            })
        }
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unsafe {
            let schema = match schema.as_any().downcast_ref::<ForeignSchemaProvider>() {
                Some(s) => &s.0,
                None => &FFI_SchemaProvider::new_with_ffi_codec(
                    schema,
                    None,
                    self.0.logical_codec.clone(),
                ),
            };
            let returned_schema: Option<FFI_SchemaProvider> =
                df_result!((self.0.register_schema)(&self.0, name.into(), schema))?
                    .into();

            Ok(returned_schema
                .map(|s| Arc::new(ForeignSchemaProvider(s)) as Arc<dyn SchemaProvider>))
        }
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unsafe {
            let returned_schema: Option<FFI_SchemaProvider> =
                df_result!((self.0.deregister_schema)(&self.0, name.into(), cascade))?
                    .into();

            Ok(returned_schema
                .map(|s| Arc::new(ForeignSchemaProvider(s)) as Arc<dyn SchemaProvider>))
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider};

    use super::*;

    #[test]
    fn test_round_trip_ffi_catalog_provider() {
        let prior_schema = Arc::new(MemorySchemaProvider::new());

        let catalog = Arc::new(MemoryCatalogProvider::new());
        assert!(
            catalog
                .as_ref()
                .register_schema("prior_schema", prior_schema)
                .unwrap()
                .is_none()
        );
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_catalog =
            FFI_CatalogProvider::new(catalog, None, task_ctx_provider, None);
        ffi_catalog.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_catalog: Arc<dyn CatalogProvider + Send> = (&ffi_catalog).into();

        let prior_schema_names = foreign_catalog.schema_names();
        assert_eq!(prior_schema_names.len(), 1);
        assert_eq!(prior_schema_names[0], "prior_schema");

        // Replace an existing schema with one of the same name
        let returned_schema = foreign_catalog
            .register_schema("prior_schema", Arc::new(MemorySchemaProvider::new()))
            .expect("Unable to register schema");
        assert!(returned_schema.is_some());
        assert_eq!(foreign_catalog.schema_names().len(), 1);

        // Add a new schema name
        let returned_schema = foreign_catalog
            .register_schema("second_schema", Arc::new(MemorySchemaProvider::new()))
            .expect("Unable to register schema");
        assert!(returned_schema.is_none());
        assert_eq!(foreign_catalog.schema_names().len(), 2);

        // Remove a schema
        let returned_schema = foreign_catalog
            .deregister_schema("prior_schema", false)
            .expect("Unable to deregister schema");
        assert!(returned_schema.is_some());
        assert_eq!(foreign_catalog.schema_names().len(), 1);

        // Retrieve non-existent schema
        let returned_schema = foreign_catalog.schema("prior_schema");
        assert!(returned_schema.is_none());

        // Retrieve valid schema
        let returned_schema = foreign_catalog.schema("second_schema");
        assert!(returned_schema.is_some());
    }

    #[test]
    fn test_ffi_catalog_provider_local_bypass() {
        let catalog = Arc::new(MemoryCatalogProvider::new());

        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let mut ffi_catalog =
            FFI_CatalogProvider::new(catalog, None, task_ctx_provider, None);

        // Verify local libraries can be downcast to their original
        let foreign_catalog: Arc<dyn CatalogProvider + Send> = (&ffi_catalog).into();
        assert!(
            foreign_catalog
                .as_any()
                .downcast_ref::<MemoryCatalogProvider>()
                .is_some()
        );

        // Verify different library markers generate foreign providers
        ffi_catalog.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_catalog: Arc<dyn CatalogProvider + Send> = (&ffi_catalog).into();
        assert!(
            foreign_catalog
                .as_any()
                .downcast_ref::<ForeignCatalogProvider>()
                .is_some()
        );
    }
}
