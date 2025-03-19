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

use std::{any::Any, ffi::c_void, sync::Arc};

use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use tokio::runtime::Handle;

use crate::{
    df_result, rresult_return,
    schema_provider::{FFI_SchemaProvider, ForeignSchemaProvider},
};

use datafusion::error::Result;

/// A stable struct for sharing [`CatalogProvider`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_CatalogProvider {
    pub schema_names: unsafe extern "C" fn(provider: &Self) -> RVec<RString>,

    pub schema: unsafe extern "C" fn(
        provider: &Self,
        name: RString,
    ) -> ROption<FFI_SchemaProvider>,

    pub register_schema:
        unsafe extern "C" fn(
            provider: &Self,
            name: RString,
            schema: &FFI_SchemaProvider,
        ) -> RResult<ROption<FFI_SchemaProvider>, RString>,

    pub deregister_schema:
        unsafe extern "C" fn(
            provider: &Self,
            name: RString,
            cascade: bool,
        ) -> RResult<ROption<FFI_SchemaProvider>, RString>,

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
}

unsafe impl Send for FFI_CatalogProvider {}
unsafe impl Sync for FFI_CatalogProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn CatalogProvider + Send>,
    runtime: Option<Handle>,
}

impl FFI_CatalogProvider {
    unsafe fn inner(&self) -> &Arc<dyn CatalogProvider + Send> {
        let private_data = self.private_data as *const ProviderPrivateData;
        &(*private_data).provider
    }

    unsafe fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const ProviderPrivateData;
        (*private_data).runtime.clone()
    }
}

unsafe extern "C" fn schema_names_fn_wrapper(
    provider: &FFI_CatalogProvider,
) -> RVec<RString> {
    let names = provider.inner().schema_names();
    names.into_iter().map(|s| s.into()).collect()
}

unsafe extern "C" fn schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
) -> ROption<FFI_SchemaProvider> {
    let maybe_schema = provider.inner().schema(name.as_str());
    maybe_schema
        .map(|schema| FFI_SchemaProvider::new(schema, provider.runtime()))
        .into()
}

unsafe extern "C" fn register_schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
    schema: &FFI_SchemaProvider,
) -> RResult<ROption<FFI_SchemaProvider>, RString> {
    let runtime = provider.runtime();
    let provider = provider.inner();
    let schema = Arc::new(ForeignSchemaProvider::from(schema));

    let returned_schema =
        rresult_return!(provider.register_schema(name.as_str(), schema))
            .map(|schema| FFI_SchemaProvider::new(schema, runtime))
            .into();

    RResult::ROk(returned_schema)
}

unsafe extern "C" fn deregister_schema_fn_wrapper(
    provider: &FFI_CatalogProvider,
    name: RString,
    cascade: bool,
) -> RResult<ROption<FFI_SchemaProvider>, RString> {
    let runtime = provider.runtime();
    let provider = provider.inner();

    let maybe_schema =
        rresult_return!(provider.deregister_schema(name.as_str(), cascade));

    RResult::ROk(
        maybe_schema
            .map(|schema| FFI_SchemaProvider::new(schema, runtime))
            .into(),
    )
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_CatalogProvider) {
    let private_data = Box::from_raw(provider.private_data as *mut ProviderPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_CatalogProvider,
) -> FFI_CatalogProvider {
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
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
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
    ) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            schema_names: schema_names_fn_wrapper,
            schema: schema_fn_wrapper,
            register_schema: register_schema_fn_wrapper,
            deregister_schema: deregister_schema_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_CatalogProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignCatalogProvider(FFI_CatalogProvider);

unsafe impl Send for ForeignCatalogProvider {}
unsafe impl Sync for ForeignCatalogProvider {}

impl From<&FFI_CatalogProvider> for ForeignCatalogProvider {
    fn from(provider: &FFI_CatalogProvider) -> Self {
        Self(provider.clone())
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
                None => &FFI_SchemaProvider::new(schema, None),
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
        assert!(catalog
            .as_ref()
            .register_schema("prior_schema", prior_schema)
            .unwrap()
            .is_none());

        let ffi_catalog = FFI_CatalogProvider::new(catalog, None);

        let foreign_catalog: ForeignCatalogProvider = (&ffi_catalog).into();

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

        // Retrieve non-existant schema
        let returned_schema = foreign_catalog.schema("prior_schema");
        assert!(returned_schema.is_none());

        // Retrieve valid schema
        let returned_schema = foreign_catalog.schema("second_schema");
        assert!(returned_schema.is_some());
    }
}
