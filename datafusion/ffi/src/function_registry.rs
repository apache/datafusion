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

use crate::session::task_ctx_accessor::FFI_TaskContextAccessor;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::{df_result, rresult_return};
use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use datafusion_common::{exec_datafusion_err, not_impl_err, DataFusionError};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use log::warn;
use std::collections::HashSet;
use std::sync::Weak;
use std::{ffi::c_void, sync::Arc};

/// A stable struct for sharing [`FunctionRegistry`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_WeakFunctionRegistry {
    pub udfs: unsafe extern "C" fn(&Self) -> RResult<RVec<RString>, RString>,
    pub udafs: unsafe extern "C" fn(&Self) -> RResult<RVec<RString>, RString>,
    pub udwfs: unsafe extern "C" fn(&Self) -> RResult<RVec<RString>, RString>,

    pub udf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_ScalarUDF, RString>,
    pub udaf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_AggregateUDF, RString>,
    pub udwf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_WindowUDF, RString>,

    pub task_ctx_accessor: FFI_TaskContextAccessor,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this registry.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignWeakFunctionRegistry`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> u64,
}

unsafe impl Send for FFI_WeakFunctionRegistry {}
unsafe impl Sync for FFI_WeakFunctionRegistry {}

struct RegistryPrivateData {
    registry: Weak<dyn FunctionRegistry + Send + Sync>,
}

impl FFI_WeakFunctionRegistry {
    fn inner(&self) -> Result<Arc<dyn FunctionRegistry + Send + Sync>, DataFusionError> {
        let private_data = self.private_data as *const RegistryPrivateData;
        unsafe {
            (*private_data)
                .registry
                .upgrade()
                .ok_or_else(|| exec_datafusion_err!("Unable to access FunctionRegistry via FFI. Ensure owning object has not gone out of scope."))
        }
    }
}

unsafe extern "C" fn udfs_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
) -> RResult<RVec<RString>, RString> {
    let inner = rresult_return!(registry.inner());
    let udfs = inner.udfs().into_iter().map(|s| s.into()).collect();
    RResult::ROk(udfs)
}
unsafe extern "C" fn udafs_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
) -> RResult<RVec<RString>, RString> {
    let inner = rresult_return!(registry.inner());
    let udafs = inner.udafs().into_iter().map(|s| s.into()).collect();
    RResult::ROk(udafs)
}
unsafe extern "C" fn udwfs_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
) -> RResult<RVec<RString>, RString> {
    let inner = rresult_return!(registry.inner());
    let udwfs = inner.udwfs().into_iter().map(|s| s.into()).collect();
    RResult::ROk(udwfs)
}

unsafe extern "C" fn udf_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
    name: RString,
) -> RResult<FFI_ScalarUDF, RString> {
    let inner = rresult_return!(registry.inner());
    let udf = rresult_return!(inner.udf(name.as_str()));
    RResult::ROk(FFI_ScalarUDF::from(udf))
}
unsafe extern "C" fn udaf_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
    name: RString,
) -> RResult<FFI_AggregateUDF, RString> {
    let inner = rresult_return!(registry.inner());
    let udaf = rresult_return!(inner.udaf(name.as_str()));
    RResult::ROk(FFI_AggregateUDF::new(
        udaf,
        registry.task_ctx_accessor.clone(),
    ))
}
unsafe extern "C" fn udwf_fn_wrapper(
    registry: &FFI_WeakFunctionRegistry,
    name: RString,
) -> RResult<FFI_WindowUDF, RString> {
    let inner = rresult_return!(registry.inner());
    let udwf = rresult_return!(inner.udwf(name.as_str()));
    RResult::ROk(FFI_WindowUDF::new(udwf, registry.task_ctx_accessor.clone()))
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_WeakFunctionRegistry) {
    let private_data = Box::from_raw(provider.private_data as *mut RegistryPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_WeakFunctionRegistry,
) -> FFI_WeakFunctionRegistry {
    let old_private_data = provider.private_data as *const RegistryPrivateData;

    let private_data = Box::into_raw(Box::new(RegistryPrivateData {
        registry: Weak::clone(&(*old_private_data).registry),
    })) as *mut c_void;

    FFI_WeakFunctionRegistry {
        udfs: udfs_fn_wrapper,
        udafs: udafs_fn_wrapper,
        udwfs: udwfs_fn_wrapper,

        udf: udf_fn_wrapper,
        udaf: udaf_fn_wrapper,
        udwf: udwf_fn_wrapper,

        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        task_ctx_accessor: provider.task_ctx_accessor.clone(),
        private_data,
        library_marker_id: crate::get_library_marker_id,
    }
}

impl Drop for FFI_WeakFunctionRegistry {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_WeakFunctionRegistry {
    /// Creates a new [`FFI_WeakFunctionRegistry`].
    pub fn new(
        registry: Arc<dyn FunctionRegistry + Send + Sync>,
        task_ctx_accessor: FFI_TaskContextAccessor,
    ) -> Self {
        let registry = Arc::downgrade(&registry);
        let private_data = Box::new(RegistryPrivateData { registry });

        Self {
            udfs: udfs_fn_wrapper,
            udafs: udafs_fn_wrapper,
            udwfs: udwfs_fn_wrapper,

            udf: udf_fn_wrapper,
            udaf: udaf_fn_wrapper,
            udwf: udwf_fn_wrapper,

            task_ctx_accessor,
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
/// FFI_FunctionRegistry to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignWeakFunctionRegistry(FFI_WeakFunctionRegistry);

unsafe impl Send for ForeignWeakFunctionRegistry {}
unsafe impl Sync for ForeignWeakFunctionRegistry {}

impl TryFrom<&FFI_WeakFunctionRegistry> for Arc<dyn FunctionRegistry + Send + Sync> {
    type Error = DataFusionError;
    fn try_from(value: &FFI_WeakFunctionRegistry) -> Result<Self, Self::Error> {
        if (value.library_marker_id)() == crate::get_library_marker_id() {
            return value.inner();
        }

        Ok(Arc::new(ForeignWeakFunctionRegistry(value.clone())))
    }
}

impl Clone for FFI_WeakFunctionRegistry {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl FunctionRegistry for ForeignWeakFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        let udfs = unsafe { (self.0.udfs)(&self.0) };
        match udfs {
            RResult::ROk(udfs) => udfs.into_iter().map(String::from).collect(),
            RResult::RErr(err) => {
                warn!("{err}");
                HashSet::with_capacity(0)
            }
        }
    }

    fn udafs(&self) -> HashSet<String> {
        let udafs = unsafe { (self.0.udafs)(&self.0) };
        match udafs {
            RResult::ROk(udafs) => udafs.into_iter().map(String::from).collect(),
            RResult::RErr(err) => {
                warn!("{err}");
                HashSet::with_capacity(0)
            }
        }
    }

    fn udwfs(&self) -> HashSet<String> {
        let udwfs = unsafe { (self.0.udwfs)(&self.0) };
        match udwfs {
            RResult::ROk(udwfs) => udwfs.into_iter().map(String::from).collect(),
            RResult::RErr(err) => {
                warn!("{err}");
                HashSet::with_capacity(0)
            }
        }
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        let udf = df_result!(unsafe { (self.0.udf)(&self.0, name.into()) })?;

        let udf = <Arc<dyn ScalarUDFImpl>>::try_from(&udf)?;
        Ok(Arc::new(ScalarUDF::new_from_shared_impl(udf)))
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        let udaf = df_result!(unsafe { (self.0.udaf)(&self.0, name.into()) })?;

        let udaf = <Arc<dyn AggregateUDFImpl>>::try_from(&udaf)?;
        Ok(Arc::new(AggregateUDF::new_from_shared_impl(udaf)))
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        let udwf = df_result!(unsafe { (self.0.udwf)(&self.0, name.into()) })?;

        let udwf = <Arc<dyn WindowUDFImpl>>::try_from(&udwf)?;
        Ok(Arc::new(WindowUDF::new_from_shared_impl(udwf)))
    }

    fn register_udf(
        &mut self,
        _udf: Arc<ScalarUDF>,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn register_udaf(
        &mut self,
        _udaf: Arc<AggregateUDF>,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn register_udwf(
        &mut self,
        _udwf: Arc<WindowUDF>,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn deregister_udf(
        &mut self,
        _name: &str,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn deregister_udaf(
        &mut self,
        _name: &str,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn deregister_udwf(
        &mut self,
        _name: &str,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn register_function_rewrite(
        &mut self,
        _rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        warn!("FFI Function Registry does not support expression planners.");
        vec![]
    }

    fn register_expr_planner(
        &mut self,
        _expr_planner: Arc<dyn ExprPlanner>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("Function Registry does not allow mutation via FFI")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_execution::TaskContextAccessor;
    use datafusion_expr::registry::FunctionRegistry;

    #[tokio::test]
    async fn test_round_trip_ffi_function_registry() -> Result<(), DataFusionError> {
        let ctx = Arc::new(SessionContext::new());
        let function_registry =
            Arc::clone(&ctx) as Arc<dyn FunctionRegistry + Send + Sync>;
        let task_ctx_accessor = Arc::clone(&ctx) as Arc<dyn TaskContextAccessor>;

        let mut ffi_registry =
            FFI_WeakFunctionRegistry::new(function_registry, task_ctx_accessor.into());
        ffi_registry.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_registry: Arc<dyn FunctionRegistry + Send + Sync> =
            (&ffi_registry).try_into()?;

        let udf_names = foreign_registry.udfs();
        assert!(!udf_names.is_empty());
        let udf = foreign_registry.udf(udf_names.iter().next().unwrap())?;

        let udaf_names = foreign_registry.udafs();
        assert!(!udaf_names.is_empty());
        let udaf = foreign_registry.udaf(udaf_names.iter().next().unwrap())?;

        let udwf_names = foreign_registry.udwfs();
        assert!(!udwf_names.is_empty());
        let udwf = foreign_registry.udwf(udwf_names.iter().next().unwrap())?;

        // The following tests exist to ensure that if we do add support
        // for mutable function registry in the future that we have
        // added test coverage.

        // Manually create foreign registry so we can make it mutable
        let mut foreign_registry = ForeignWeakFunctionRegistry(ffi_registry);

        fn expect_not_implemented<T>(input: Result<T, DataFusionError>) {
            let Err(DataFusionError::NotImplemented(_)) = input else {
                panic!("Expected not implemented feature");
            };
        }
        expect_not_implemented(foreign_registry.register_udf(udf));
        expect_not_implemented(foreign_registry.register_udaf(udaf));
        expect_not_implemented(foreign_registry.register_udwf(udwf));
        expect_not_implemented(foreign_registry.deregister_udf("a"));
        expect_not_implemented(foreign_registry.deregister_udaf("a"));
        expect_not_implemented(foreign_registry.deregister_udwf("a"));

        Ok(())
    }
}
