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

use crate::execution::task_ctx_provider::FFI_TaskContextProvider;
use crate::session_config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use abi_stable::pmr::ROption;
use abi_stable::std_types::RHashMap;
use abi_stable::{std_types::RString, StableAbi};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use std::{ffi::c_void, sync::Arc};

/// A stable struct for sharing [`TaskContext`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TaskContext {
    pub session_id: unsafe extern "C" fn(&Self) -> RString,

    pub task_id: unsafe extern "C" fn(&Self) -> ROption<RString>,

    pub session_config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    pub scalar_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    pub aggregate_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

    pub window_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_WindowUDF>,

    /// Provider for TaskContext to be used during protobuf serialization
    /// and deserialization.
    pub task_ctx_provider: FFI_TaskContextProvider,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> u64,
}

struct TaskContextPrivateData {
    ctx: Arc<TaskContext>,
}

impl FFI_TaskContext {
    unsafe fn inner(&self) -> &TaskContext {
        let private_data = self.private_data as *const TaskContextPrivateData;
        &(*private_data).ctx
    }
}

unsafe extern "C" fn session_id_fn_wrapper(ctx: &FFI_TaskContext) -> RString {
    let ctx = ctx.inner();
    ctx.session_id().into()
}

unsafe extern "C" fn task_id_fn_wrapper(ctx: &FFI_TaskContext) -> ROption<RString> {
    let ctx = ctx.inner();
    ctx.task_id().map(|s| s.as_str().into()).into()
}

unsafe extern "C" fn session_config_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> FFI_SessionConfig {
    let ctx = ctx.inner();
    ctx.session_config().into()
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_ScalarUDF> {
    let ctx = ctx.inner();
    ctx.scalar_functions()
        .iter()
        .map(|(name, udf)| (name.to_owned().into(), Arc::clone(udf).into()))
        .collect()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_AggregateUDF> {
    let task_ctx_provider = &ctx.task_ctx_provider;
    let ctx = ctx.inner();
    ctx.aggregate_functions()
        .iter()
        .map(|(name, udaf)| {
            (
                name.to_owned().into(),
                FFI_AggregateUDF::new(Arc::clone(udaf), task_ctx_provider.clone()),
            )
        })
        .collect()
}

unsafe extern "C" fn window_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_WindowUDF> {
    let ctx = ctx.inner();
    ctx.window_functions()
        .iter()
        .map(|(name, udf)| (name.to_owned().into(), FFI_WindowUDF::from(Arc::clone(udf))))
        .collect()
}

unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_TaskContext) {
    let private_data = Box::from_raw(ctx.private_data as *mut TaskContextPrivateData);
    drop(private_data);
}

impl Drop for FFI_TaskContext {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_TaskContext {
    pub fn new(
        ctx: Arc<TaskContext>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let private_data = Box::new(TaskContextPrivateData { ctx });

        FFI_TaskContext {
            session_id: session_id_fn_wrapper,
            task_id: task_id_fn_wrapper,
            session_config: session_config_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            task_ctx_provider,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl From<FFI_TaskContext> for TaskContext {
    fn from(ffi_ctx: FFI_TaskContext) -> Self {
        unsafe {
            if (ffi_ctx.library_marker_id)() == crate::get_library_marker_id() {
                return ffi_ctx.inner().clone();
            }

            let task_id = (ffi_ctx.task_id)(&ffi_ctx).map(|s| s.to_string()).into();
            let session_id = (ffi_ctx.session_id)(&ffi_ctx).into();
            let session_config = (ffi_ctx.session_config)(&ffi_ctx);
            let session_config =
                SessionConfig::try_from(&session_config).unwrap_or_default();

            let scalar_functions = (ffi_ctx.scalar_functions)(&ffi_ctx)
                .into_iter()
                .filter_map(|kv_pair| {
                    let udf = <Arc<dyn ScalarUDFImpl>>::try_from(&kv_pair.1);

                    if let Err(err) = &udf {
                        log::error!("Unable to create WindowUDF in FFI: {err}")
                    }

                    udf.ok().map(|udf| {
                        (
                            kv_pair.0.into_string(),
                            Arc::new(ScalarUDF::new_from_shared_impl(udf)),
                        )
                    })
                })
                .collect();
            let aggregate_functions = (ffi_ctx.aggregate_functions)(&ffi_ctx)
                .into_iter()
                .filter_map(|kv_pair| {
                    let udaf = <Arc<dyn AggregateUDFImpl>>::try_from(&kv_pair.1);

                    if let Err(err) = &udaf {
                        log::error!("Unable to create AggregateUDF in FFI: {err}")
                    }

                    udaf.ok().map(|udaf| {
                        (
                            kv_pair.0.into_string(),
                            Arc::new(AggregateUDF::new_from_shared_impl(udaf)),
                        )
                    })
                })
                .collect();
            let window_functions = (ffi_ctx.window_functions)(&ffi_ctx)
                .into_iter()
                .filter_map(|kv_pair| {
                    let udwf = <Arc<dyn WindowUDFImpl>>::try_from(&kv_pair.1);

                    if let Err(err) = &udwf {
                        log::error!("Unable to create WindowUDF in FFI: {err}")
                    }

                    udwf.ok().map(|udwf| {
                        (
                            kv_pair.0.into_string(),
                            Arc::new(WindowUDF::new_from_shared_impl(udwf)),
                        )
                    })
                })
                .collect();

            let runtime = Arc::new(RuntimeEnv::default());

            TaskContext::new(
                task_id,
                session_id,
                session_config,
                scalar_functions,
                aggregate_functions,
                window_functions,
                runtime,
            )
        }
    }
}
