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
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::{
    catalog::{SchemaProvider, TableProvider},
    error::DataFusionError,
};
use tokio::runtime::Handle;

use crate::{
    df_result, rresult_return,
    table_provider::{FFI_TableProvider, ForeignTableProvider},
};

use datafusion::error::Result;

/// A stable struct for sharing [`SchemaProvider`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_SchemaProvider {
    pub owner_name: ROption<RString>,

    pub table_names: unsafe extern "C" fn(provider: &Self) -> RVec<RString>,

    pub table: unsafe extern "C" fn(
        provider: &Self,
        name: RString,
    ) -> FfiFuture<
        RResult<ROption<FFI_TableProvider>, RString>,
    >,

    pub register_table:
        unsafe extern "C" fn(
            provider: &Self,
            name: RString,
            table: FFI_TableProvider,
        ) -> RResult<ROption<FFI_TableProvider>, RString>,

    pub deregister_table:
        unsafe extern "C" fn(
            provider: &Self,
            name: RString,
        ) -> RResult<ROption<FFI_TableProvider>, RString>,

    pub table_exist: unsafe extern "C" fn(provider: &Self, name: RString) -> bool,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignSchemaProvider`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_SchemaProvider {}
unsafe impl Sync for FFI_SchemaProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn SchemaProvider + Send>,
    runtime: Option<Handle>,
}

impl FFI_SchemaProvider {
    unsafe fn inner(&self) -> &Arc<dyn SchemaProvider + Send> {
        let private_data = self.private_data as *const ProviderPrivateData;
        &(*private_data).provider
    }

    unsafe fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const ProviderPrivateData;
        (*private_data).runtime.clone()
    }
}

unsafe extern "C" fn table_names_fn_wrapper(
    provider: &FFI_SchemaProvider,
) -> RVec<RString> {
    let provider = provider.inner();

    let table_names = provider.table_names();
    table_names.into_iter().map(|s| s.into()).collect()
}

unsafe extern "C" fn table_fn_wrapper(
    provider: &FFI_SchemaProvider,
    name: RString,
) -> FfiFuture<RResult<ROption<FFI_TableProvider>, RString>> {
    let runtime = provider.runtime();
    let provider = Arc::clone(provider.inner());

    async move {
        let table = rresult_return!(provider.table(name.as_str()).await)
            .map(|t| FFI_TableProvider::new(t, true, runtime))
            .into();

        RResult::ROk(table)
    }
    .into_ffi()
}

unsafe extern "C" fn register_table_fn_wrapper(
    provider: &FFI_SchemaProvider,
    name: RString,
    table: FFI_TableProvider,
) -> RResult<ROption<FFI_TableProvider>, RString> {
    let runtime = provider.runtime();
    let provider = provider.inner();

    let table = Arc::new(ForeignTableProvider(table));

    let returned_table = rresult_return!(provider.register_table(name.into(), table))
        .map(|t| FFI_TableProvider::new(t, true, runtime));

    RResult::ROk(returned_table.into())
}

unsafe extern "C" fn deregister_table_fn_wrapper(
    provider: &FFI_SchemaProvider,
    name: RString,
) -> RResult<ROption<FFI_TableProvider>, RString> {
    let runtime = provider.runtime();
    let provider = provider.inner();

    let returned_table = rresult_return!(provider.deregister_table(name.as_str()))
        .map(|t| FFI_TableProvider::new(t, true, runtime));

    RResult::ROk(returned_table.into())
}

unsafe extern "C" fn table_exist_fn_wrapper(
    provider: &FFI_SchemaProvider,
    name: RString,
) -> bool {
    provider.inner().table_exist(name.as_str())
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_SchemaProvider) {
    let private_data = Box::from_raw(provider.private_data as *mut ProviderPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_SchemaProvider,
) -> FFI_SchemaProvider {
    let old_private_data = provider.private_data as *const ProviderPrivateData;
    let runtime = (*old_private_data).runtime.clone();

    let private_data = Box::into_raw(Box::new(ProviderPrivateData {
        provider: Arc::clone(&(*old_private_data).provider),
        runtime,
    })) as *mut c_void;

    FFI_SchemaProvider {
        owner_name: provider.owner_name.clone(),
        table_names: table_names_fn_wrapper,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        table: table_fn_wrapper,
        register_table: register_table_fn_wrapper,
        deregister_table: deregister_table_fn_wrapper,
        table_exist: table_exist_fn_wrapper,
    }
}

impl Drop for FFI_SchemaProvider {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_SchemaProvider {
    /// Creates a new [`FFI_SchemaProvider`].
    pub fn new(
        provider: Arc<dyn SchemaProvider + Send>,
        runtime: Option<Handle>,
    ) -> Self {
        let owner_name = provider.owner_name().map(|s| s.into()).into();
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            owner_name,
            table_names: table_names_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            table: table_fn_wrapper,
            register_table: register_table_fn_wrapper,
            deregister_table: deregister_table_fn_wrapper,
            table_exist: table_exist_fn_wrapper,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_SchemaProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignSchemaProvider(pub FFI_SchemaProvider);

unsafe impl Send for ForeignSchemaProvider {}
unsafe impl Sync for ForeignSchemaProvider {}

impl From<&FFI_SchemaProvider> for ForeignSchemaProvider {
    fn from(provider: &FFI_SchemaProvider) -> Self {
        Self(provider.clone())
    }
}

impl Clone for FFI_SchemaProvider {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl SchemaProvider for ForeignSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn owner_name(&self) -> Option<&str> {
        let name: Option<&RString> = self.0.owner_name.as_ref().into();
        name.map(|s| s.as_str())
    }

    fn table_names(&self) -> Vec<String> {
        unsafe {
            (self.0.table_names)(&self.0)
                .into_iter()
                .map(|s| s.into())
                .collect()
        }
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        unsafe {
            let table: Option<FFI_TableProvider> =
                df_result!((self.0.table)(&self.0, name.into()).await)?.into();

            let table = table.as_ref().map(|t| {
                Arc::new(ForeignTableProvider::from(t)) as Arc<dyn TableProvider>
            });

            Ok(table)
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        unsafe {
            let ffi_table = match table.as_any().downcast_ref::<ForeignTableProvider>() {
                Some(t) => t.0.clone(),
                None => FFI_TableProvider::new(table, true, None),
            };

            let returned_provider: Option<FFI_TableProvider> =
                df_result!((self.0.register_table)(&self.0, name.into(), ffi_table))?
                    .into();

            Ok(returned_provider
                .map(|t| Arc::new(ForeignTableProvider(t)) as Arc<dyn TableProvider>))
        }
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let returned_provider: Option<FFI_TableProvider> = unsafe {
            df_result!((self.0.deregister_table)(&self.0, name.into()))?.into()
        };

        Ok(returned_provider
            .map(|t| Arc::new(ForeignTableProvider(t)) as Arc<dyn TableProvider>))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        unsafe { (self.0.table_exist)(&self.0, name.into()) }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use datafusion::{catalog::MemorySchemaProvider, datasource::empty::EmptyTable};

    use super::*;

    fn empty_table() -> Arc<dyn TableProvider> {
        Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
    }

    #[tokio::test]
    async fn test_round_trip_ffi_schema_provider() {
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        assert!(schema_provider
            .as_ref()
            .register_table("prior_table".to_string(), empty_table())
            .unwrap()
            .is_none());

        let ffi_schema_provider = FFI_SchemaProvider::new(schema_provider, None);

        let foreign_schema_provider: ForeignSchemaProvider =
            (&ffi_schema_provider).into();

        let prior_table_names = foreign_schema_provider.table_names();
        assert_eq!(prior_table_names.len(), 1);
        assert_eq!(prior_table_names[0], "prior_table");

        // Replace an existing table with one of the same name generates an error
        let returned_schema = foreign_schema_provider
            .register_table("prior_table".to_string(), empty_table());
        assert!(returned_schema.is_err());
        assert_eq!(foreign_schema_provider.table_names().len(), 1);

        // Add a new table
        let returned_schema = foreign_schema_provider
            .register_table("second_table".to_string(), empty_table())
            .expect("Unable to register table");
        assert!(returned_schema.is_none());
        assert_eq!(foreign_schema_provider.table_names().len(), 2);

        // Remove a table
        let returned_schema = foreign_schema_provider
            .deregister_table("prior_table")
            .expect("Unable to deregister table");
        assert!(returned_schema.is_some());
        assert_eq!(foreign_schema_provider.table_names().len(), 1);

        // Retrieve non-existant table
        let returned_schema = foreign_schema_provider
            .table("prior_table")
            .await
            .expect("Unable to query table");
        assert!(returned_schema.is_none());
        assert!(!foreign_schema_provider.table_exist("prior_table"));

        // Retrieve valid table
        let returned_schema = foreign_schema_provider
            .table("second_table")
            .await
            .expect("Unable to query table");
        assert!(returned_schema.is_some());
        assert!(foreign_schema_provider.table_exist("second_table"));
    }
}
