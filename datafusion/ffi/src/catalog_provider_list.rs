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

use std::ffi::c_void;
use std::sync::Arc;

use datafusion_catalog::{CatalogProvider, CatalogProviderList};
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::catalog_provider::{FFI_CatalogProvider, ForeignCatalogProvider};
use crate::proto::extension_codec_bundle::FFI_ExtensionCodecBundle;
use crate::util::FFI_Option;

/// A stable struct for sharing [`CatalogProviderList`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_CatalogProviderList {
    /// Register a catalog
    pub register_catalog: unsafe extern "C" fn(
        &Self,
        name: SString,
        catalog: &FFI_CatalogProvider,
    ) -> FFI_Option<FFI_CatalogProvider>,

    /// List of existing catalogs
    pub catalog_names: unsafe extern "C" fn(&Self) -> SVec<SString>,

    /// Access a catalog
    pub catalog:
        unsafe extern "C" fn(&Self, name: SString) -> FFI_Option<FFI_CatalogProvider>,

    /// The serialization environment propagated to every catalog reached through
    /// this list.
    pub codecs: FFI_ExtensionCodecBundle,

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
    provider: Arc<dyn CatalogProviderList>,
    runtime: Option<Handle>,
}

impl FFI_CatalogProviderList {
    unsafe fn inner(&self) -> &Arc<dyn CatalogProviderList> {
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
) -> SVec<SString> {
    unsafe {
        let names = provider.inner().catalog_names();
        names.into_iter().map(|s| s.into()).collect()
    }
}

unsafe extern "C" fn register_catalog_fn_wrapper(
    provider: &FFI_CatalogProviderList,
    name: SString,
    catalog: &FFI_CatalogProvider,
) -> FFI_Option<FFI_CatalogProvider> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();
        let catalog: Arc<dyn CatalogProvider> = catalog.into();

        inner_provider
            .register_catalog(name.into(), catalog)
            .map(|catalog| {
                FFI_CatalogProvider::new(catalog, runtime, provider.codecs.clone())
            })
            .into()
    }
}

unsafe extern "C" fn catalog_fn_wrapper(
    provider: &FFI_CatalogProviderList,
    name: SString,
) -> FFI_Option<FFI_CatalogProvider> {
    unsafe {
        let runtime = provider.runtime();
        let inner_provider = provider.inner();
        inner_provider
            .catalog(name.as_str())
            .map(|catalog| {
                FFI_CatalogProvider::new(catalog, runtime, provider.codecs.clone())
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
            codecs: provider.codecs.clone(),
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
    ///
    /// `codecs` must describe the extension nodes used by every table below this
    /// list, since catalogs, schemas, and tables reached through it inherit it.
    pub fn new(
        provider: Arc<dyn CatalogProviderList>,
        runtime: Option<Handle>,
        codecs: FFI_ExtensionCodecBundle,
    ) -> Self {
        if let Some(provider) = provider.downcast_ref::<ForeignCatalogProviderList>() {
            return provider.0.clone();
        }

        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            register_catalog: register_catalog_fn_wrapper,
            catalog_names: catalog_names_fn_wrapper,
            catalog: catalog_fn_wrapper,
            codecs,
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

impl From<&FFI_CatalogProviderList> for Arc<dyn CatalogProviderList> {
    fn from(provider: &FFI_CatalogProviderList) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(unsafe { provider.inner() });
        }

        Arc::new(ForeignCatalogProviderList(provider.clone()))
            as Arc<dyn CatalogProviderList>
    }
}

impl Clone for FFI_CatalogProviderList {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl CatalogProviderList for ForeignCatalogProviderList {
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        unsafe {
            let catalog = match catalog.downcast_ref::<ForeignCatalogProvider>() {
                Some(s) => &s.0,
                None => &FFI_CatalogProvider::new(catalog, None, self.0.codecs.clone()),
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
    use arrow::array::record_batch;
    use datafusion::catalog::{MemoryCatalogProvider, MemoryCatalogProviderList};
    use datafusion_catalog::{MemTable, MemorySchemaProvider, SchemaProvider};
    use datafusion_common::Result;
    use datafusion_expr::ptr_eq::arc_ptr_eq;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    use super::*;
    use crate::proto::physical_extension_codec::tests::TestExtensionCodec;
    use crate::schema_provider::FFI_SchemaProvider;
    use crate::table_provider::FFI_TableProvider;

    /// Walking a catalog hierarchy must hand the same serialization environment to
    /// every wrapper it creates. Otherwise a table found this way would export
    /// sessions that cannot serialize the nodes its owner's codecs describe.
    #[tokio::test]
    async fn test_nested_construction_preserves_codecs() -> Result<()> {
        let batch = record_batch!(("a", Int32, [1, 2, 3]))?;
        let table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?);
        let schema = Arc::new(MemorySchemaProvider::new());
        schema.register_table("t".to_owned(), table)?;
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("s", schema)?;
        let catalog_list = Arc::new(MemoryCatalogProviderList::new());
        catalog_list.register_catalog("c".to_owned(), catalog);

        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let physical_codec =
            Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>;
        let codecs = FFI_ExtensionCodecBundle::new(
            task_ctx_provider,
            None,
            Arc::new(datafusion_proto::logical_plan::DefaultLogicalExtensionCodec {}),
            Arc::clone(&physical_codec),
        );

        let assert_carries_codec = |codecs: &FFI_ExtensionCodecBundle, label: &str| {
            assert!(
                arc_ptr_eq(&codecs.to_physical_codec(), &physical_codec),
                "{label} lost the physical codec"
            );
        };

        let ffi_catalog_list = FFI_CatalogProviderList::new(catalog_list, None, codecs);
        assert_carries_codec(&ffi_catalog_list.codecs, "catalog list");

        let ffi_catalog: Option<FFI_CatalogProvider> =
            unsafe { (ffi_catalog_list.catalog)(&ffi_catalog_list, "c".into()) }.into();
        let ffi_catalog = ffi_catalog.expect("catalog \"c\" should exist");
        assert_carries_codec(&ffi_catalog.codecs, "catalog");

        let ffi_schema: Option<FFI_SchemaProvider> =
            unsafe { (ffi_catalog.schema)(&ffi_catalog, "s".into()) }.into();
        let ffi_schema = ffi_schema.expect("schema \"s\" should exist");
        assert_carries_codec(&ffi_schema.codecs, "schema");

        let ffi_table = crate::df_result!(unsafe {
            (ffi_schema.table)(&ffi_schema, "t".into()).await
        })?;
        let ffi_table: Option<FFI_TableProvider> = ffi_table.into();
        let ffi_table = ffi_table.expect("table \"t\" should exist");
        assert_carries_codec(&ffi_table.codecs, "table provider");

        Ok(())
    }

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
        let codecs = FFI_ExtensionCodecBundle::new_default(task_ctx_provider, None);
        let mut ffi_catalog_list =
            FFI_CatalogProviderList::new(catalog_list, None, codecs);
        ffi_catalog_list.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_catalog_list: Arc<dyn CatalogProviderList> =
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
        let codecs = FFI_ExtensionCodecBundle::new_default(task_ctx_provider, None);
        let mut ffi_catalog_list =
            FFI_CatalogProviderList::new(catalog_list, None, codecs);

        // Verify local libraries can be downcast to their original
        let foreign_catalog_list: Arc<dyn CatalogProviderList> =
            (&ffi_catalog_list).into();
        assert!(
            foreign_catalog_list
                .downcast_ref::<MemoryCatalogProviderList>()
                .is_some()
        );

        // Verify different library markers generate foreign providers
        ffi_catalog_list.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_catalog_list: Arc<dyn CatalogProviderList> =
            (&ffi_catalog_list).into();
        assert!(
            foreign_catalog_list
                .downcast_ref::<ForeignCatalogProviderList>()
                .is_some()
        );
    }
}
