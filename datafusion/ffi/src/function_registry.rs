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

use crate::udaf::{FFI_AggregateUDF, ForeignAggregateUDF};
use crate::udf::{FFI_ScalarUDF, ForeignScalarUDF};
use crate::udwf::{FFI_WindowUDF, ForeignWindowUDF};
use crate::{df_result, rresult_return};
use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use datafusion_common::not_impl_err;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use std::collections::HashSet;
use std::sync::Mutex;
use std::{ffi::c_void, sync::Arc};

/// A stable struct for sharing [`FunctionRegistry`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_FunctionRegistry {
    pub udfs: unsafe extern "C" fn(&Self) -> RVec<RString>,
    pub udafs: unsafe extern "C" fn(&Self) -> RVec<RString>,
    pub udwfs: unsafe extern "C" fn(&Self) -> RVec<RString>,

    pub udf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_ScalarUDF, RString>,
    pub udaf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_AggregateUDF, RString>,
    pub udwf:
        unsafe extern "C" fn(&Self, name: RString) -> RResult<FFI_WindowUDF, RString>,

    pub register_udf: unsafe extern "C" fn(
        &mut Self,
        udf: FFI_ScalarUDF,
    )
        -> RResult<ROption<FFI_ScalarUDF>, RString>,
    pub register_udaf:
        unsafe extern "C" fn(
            &mut Self,
            udf: FFI_AggregateUDF,
        ) -> RResult<ROption<FFI_AggregateUDF>, RString>,
    pub register_udwf: unsafe extern "C" fn(
        &mut Self,
        udf: FFI_WindowUDF,
    )
        -> RResult<ROption<FFI_WindowUDF>, RString>,

    pub deregister_udf: unsafe extern "C" fn(
        &mut Self,
        name: RString,
    )
        -> RResult<ROption<FFI_ScalarUDF>, RString>,
    pub deregister_udaf:
        unsafe extern "C" fn(
            &mut Self,
            name: RString,
        ) -> RResult<ROption<FFI_AggregateUDF>, RString>,
    pub deregister_udwf: unsafe extern "C" fn(
        &mut Self,
        name: RString,
    )
        -> RResult<ROption<FFI_WindowUDF>, RString>,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this registry.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignFunctionRegistry`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_FunctionRegistry {}
unsafe impl Sync for FFI_FunctionRegistry {}

struct RegistryPrivateData {
    registry: Arc<Mutex<dyn FunctionRegistry + Send>>,
}

impl FFI_FunctionRegistry {
    unsafe fn inner(&self) -> &Arc<Mutex<dyn FunctionRegistry + Send>> {
        let private_data = self.private_data as *const RegistryPrivateData;
        &(*private_data).registry
    }
}

unsafe extern "C" fn udfs_fn_wrapper(registry: &FFI_FunctionRegistry) -> RVec<RString> {
    let Ok(registry) = registry.inner().lock() else {
        log::error!("udwfs_fn_wrapper is unable to get a mutex lock");
        return RVec::new();
    };
    let udfs = registry.udfs();
    udfs.into_iter().map(|s| s.into()).collect()
}
unsafe extern "C" fn udafs_fn_wrapper(registry: &FFI_FunctionRegistry) -> RVec<RString> {
    let Ok(registry) = registry.inner().lock() else {
        log::error!("udwfs_fn_wrapper is unable to get a mutex lock");
        return RVec::new();
    };
    let udafs = registry.udafs();
    udafs.into_iter().map(|s| s.into()).collect()
}
unsafe extern "C" fn udwfs_fn_wrapper(registry: &FFI_FunctionRegistry) -> RVec<RString> {
    let Ok(registry) = registry.inner().lock() else {
        log::error!("udwfs_fn_wrapper is unable to get a mutex lock");
        return RVec::new();
    };
    let udwfs = registry.udwfs();
    udwfs.into_iter().map(|s| s.into()).collect()
}

unsafe extern "C" fn udf_fn_wrapper(
    registry: &FFI_FunctionRegistry,
    name: RString,
) -> RResult<FFI_ScalarUDF, RString> {
    let registry = rresult_return!(registry.inner().lock());
    let udf = rresult_return!(registry.udf(name.as_str()));
    RResult::ROk(FFI_ScalarUDF::from(udf))
}
unsafe extern "C" fn udaf_fn_wrapper(
    registry: &FFI_FunctionRegistry,
    name: RString,
) -> RResult<FFI_AggregateUDF, RString> {
    let registry = rresult_return!(registry.inner().lock());
    let udaf = rresult_return!(registry.udaf(name.as_str()));
    RResult::ROk(FFI_AggregateUDF::from(udaf))
}
unsafe extern "C" fn udwf_fn_wrapper(
    registry: &FFI_FunctionRegistry,
    name: RString,
) -> RResult<FFI_WindowUDF, RString> {
    let registry = rresult_return!(registry.inner().lock());
    let udwf = rresult_return!(registry.udwf(name.as_str()));
    RResult::ROk(FFI_WindowUDF::from(udwf))
}

unsafe extern "C" fn register_udf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    udf: FFI_ScalarUDF,
) -> RResult<ROption<FFI_ScalarUDF>, RString> {
    let udf: ForeignScalarUDF = rresult_return!((&udf).try_into());
    let mut registry = rresult_return!(registry.inner().lock());
    let udf = rresult_return!(registry.register_udf(Arc::new(udf.into())))
        .map(FFI_ScalarUDF::from);
    RResult::ROk(udf.into())
}
unsafe extern "C" fn register_udaf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    udaf: FFI_AggregateUDF,
) -> RResult<ROption<FFI_AggregateUDF>, RString> {
    let udaf: ForeignAggregateUDF = rresult_return!((&udaf).try_into());
    let mut registry = rresult_return!(registry.inner().lock());
    let udaf = rresult_return!(registry.register_udaf(Arc::new(udaf.into())))
        .map(FFI_AggregateUDF::from);
    RResult::ROk(udaf.into())
}
unsafe extern "C" fn register_udwf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    udwf: FFI_WindowUDF,
) -> RResult<ROption<FFI_WindowUDF>, RString> {
    let udwf: ForeignWindowUDF = rresult_return!((&udwf).try_into());
    let mut registry = rresult_return!(registry.inner().lock());
    let udwf = rresult_return!(registry.register_udwf(Arc::new(udwf.into())))
        .map(FFI_WindowUDF::from);
    RResult::ROk(udwf.into())
}

unsafe extern "C" fn deregister_udf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    name: RString,
) -> RResult<ROption<FFI_ScalarUDF>, RString> {
    let mut registry = rresult_return!(registry.inner().lock());
    let udf =
        rresult_return!(registry.deregister_udf(name.as_str())).map(FFI_ScalarUDF::from);
    RResult::ROk(udf.into())
}
unsafe extern "C" fn deregister_udaf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    name: RString,
) -> RResult<ROption<FFI_AggregateUDF>, RString> {
    let mut registry = rresult_return!(registry.inner().lock());
    let udaf = rresult_return!(registry.deregister_udaf(name.as_str()))
        .map(FFI_AggregateUDF::from);
    RResult::ROk(udaf.into())
}
unsafe extern "C" fn deregister_udwf_fn_wrapper(
    registry: &mut FFI_FunctionRegistry,
    name: RString,
) -> RResult<ROption<FFI_WindowUDF>, RString> {
    let mut registry = rresult_return!(registry.inner().lock());
    let udwf =
        rresult_return!(registry.deregister_udwf(name.as_str())).map(FFI_WindowUDF::from);
    RResult::ROk(udwf.into())
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_FunctionRegistry) {
    let private_data = Box::from_raw(provider.private_data as *mut RegistryPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_FunctionRegistry,
) -> FFI_FunctionRegistry {
    let old_private_data = provider.private_data as *const RegistryPrivateData;

    let private_data = Box::into_raw(Box::new(RegistryPrivateData {
        registry: Arc::clone(&(*old_private_data).registry),
    })) as *mut c_void;

    FFI_FunctionRegistry {
        udfs: udfs_fn_wrapper,
        udafs: udafs_fn_wrapper,
        udwfs: udwfs_fn_wrapper,

        udf: udf_fn_wrapper,
        udaf: udaf_fn_wrapper,
        udwf: udwf_fn_wrapper,

        register_udf: register_udf_fn_wrapper,
        register_udaf: register_udaf_fn_wrapper,
        register_udwf: register_udwf_fn_wrapper,

        deregister_udf: deregister_udf_fn_wrapper,
        deregister_udaf: deregister_udaf_fn_wrapper,
        deregister_udwf: deregister_udwf_fn_wrapper,

        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
    }
}

impl Drop for FFI_FunctionRegistry {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_FunctionRegistry {
    /// Creates a new [`FFI_FunctionRegistry`].
    pub fn new(registry: Arc<Mutex<dyn FunctionRegistry + Send>>) -> Self {
        let private_data = Box::new(RegistryPrivateData { registry });

        Self {
            udfs: udfs_fn_wrapper,
            udafs: udafs_fn_wrapper,
            udwfs: udwfs_fn_wrapper,

            udf: udf_fn_wrapper,
            udaf: udaf_fn_wrapper,
            udwf: udwf_fn_wrapper,

            register_udf: register_udf_fn_wrapper,
            register_udaf: register_udaf_fn_wrapper,
            register_udwf: register_udwf_fn_wrapper,

            deregister_udf: deregister_udf_fn_wrapper,
            deregister_udaf: deregister_udaf_fn_wrapper,
            deregister_udwf: deregister_udwf_fn_wrapper,

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
/// FFI_FunctionRegistry to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignFunctionRegistry(FFI_FunctionRegistry);

unsafe impl Send for ForeignFunctionRegistry {}
unsafe impl Sync for ForeignFunctionRegistry {}

impl From<&FFI_FunctionRegistry> for ForeignFunctionRegistry {
    fn from(provider: &FFI_FunctionRegistry) -> Self {
        Self(provider.clone())
    }
}

impl Clone for FFI_FunctionRegistry {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl FunctionRegistry for ForeignFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        let udfs = unsafe { (self.0.udfs)(&self.0) };

        udfs.into_iter().map(String::from).collect()
    }

    fn udafs(&self) -> HashSet<String> {
        let udafs = unsafe { (self.0.udafs)(&self.0) };

        udafs.into_iter().map(String::from).collect()
    }

    fn udwfs(&self) -> HashSet<String> {
        let udwfs = unsafe { (self.0.udwfs)(&self.0) };

        udwfs.into_iter().map(String::from).collect()
    }

    fn udf(&self, name: &str) -> datafusion_common::Result<Arc<ScalarUDF>> {
        let udf = df_result!(unsafe { (self.0.udf)(&self.0, name.into()) })?;

        let udf = ForeignScalarUDF::try_from(&udf)?;
        Ok(Arc::new(udf.into()))
    }

    fn udaf(&self, name: &str) -> datafusion_common::Result<Arc<AggregateUDF>> {
        let udaf = df_result!(unsafe { (self.0.udaf)(&self.0, name.into()) })?;

        let udaf = ForeignAggregateUDF::try_from(&udaf)?;
        Ok(Arc::new(udaf.into()))
    }

    fn udwf(&self, name: &str) -> datafusion_common::Result<Arc<WindowUDF>> {
        let udwf = df_result!(unsafe { (self.0.udwf)(&self.0, name.into()) })?;

        let udwf = ForeignWindowUDF::try_from(&udwf)?;
        Ok(Arc::new(udwf.into()))
    }

    fn register_udf(
        &mut self,
        udf: Arc<ScalarUDF>,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        let udf = FFI_ScalarUDF::from(udf);
        let ROption::RSome(udf) =
            df_result!(unsafe { (self.0.register_udf)(&mut self.0, udf) })?
        else {
            return Ok(None);
        };
        let udf = ForeignScalarUDF::try_from(&udf)?;

        Ok(Some(Arc::new(ScalarUDF::from(udf))))
    }

    fn register_udaf(
        &mut self,
        udaf: Arc<AggregateUDF>,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        let udaf = FFI_AggregateUDF::from(udaf);
        let ROption::RSome(udaf) =
            df_result!(unsafe { (self.0.register_udaf)(&mut self.0, udaf) })?
        else {
            return Ok(None);
        };
        let udaf = ForeignAggregateUDF::try_from(&udaf)?;

        Ok(Some(Arc::new(AggregateUDF::from(udaf))))
    }

    fn register_udwf(
        &mut self,
        udwf: Arc<WindowUDF>,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        let udwf = FFI_WindowUDF::from(udwf);
        let ROption::RSome(udwf) =
            df_result!(unsafe { (self.0.register_udwf)(&mut self.0, udwf) })?
        else {
            return Ok(None);
        };
        let udwf = ForeignWindowUDF::try_from(&udwf)?;

        Ok(Some(Arc::new(WindowUDF::from(udwf))))
    }

    fn deregister_udf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<ScalarUDF>>> {
        let ROption::RSome(udf) =
            df_result!(unsafe { (self.0.deregister_udf)(&mut self.0, name.into()) })?
        else {
            return Ok(None);
        };
        let udf = ForeignScalarUDF::try_from(&udf)?;

        Ok(Some(Arc::new(ScalarUDF::from(udf))))
    }

    fn deregister_udaf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<AggregateUDF>>> {
        let ROption::RSome(udaf) =
            df_result!(unsafe { (self.0.deregister_udaf)(&mut self.0, name.into()) })?
        else {
            return Ok(None);
        };
        let udwf = ForeignAggregateUDF::try_from(&udaf)?;

        Ok(Some(Arc::new(AggregateUDF::from(udwf))))
    }

    fn deregister_udwf(
        &mut self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<WindowUDF>>> {
        let ROption::RSome(udwf) =
            df_result!(unsafe { (self.0.deregister_udwf)(&mut self.0, name.into()) })?
        else {
            return Ok(None);
        };
        let udwf = ForeignWindowUDF::try_from(&udwf)?;

        Ok(Some(Arc::new(WindowUDF::from(udwf))))
    }

    fn register_function_rewrite(
        &mut self,
        _rewrite: Arc<dyn FunctionRewrite + Send + Sync>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("register_function_rewrite not implemented in FFI")
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }

    fn register_expr_planner(
        &mut self,
        _expr_planner: Arc<dyn ExprPlanner>,
    ) -> datafusion_common::Result<()> {
        not_impl_err!("register_function_rewrite not implemented in FFI")
    }
}
