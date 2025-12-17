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

use abi_stable::StableAbi;
use abi_stable::pmr::ROption;
use abi_stable::std_types::{RHashMap, RString};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};

use crate::session::config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;

/// A stable struct for sharing [`TaskContext`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TaskContext {
    /// Return the session ID.
    pub session_id: unsafe extern "C" fn(&Self) -> RString,

    /// Return the task ID.
    pub task_id: unsafe extern "C" fn(&Self) -> ROption<RString>,

    /// Return the session configuration.
    pub session_config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    /// Returns a hashmap of names to scalar functions.
    pub scalar_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    /// Returns a hashmap of names to aggregate functions.
    pub aggregate_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

    /// Returns a hashmap of names to window functions.
    pub window_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_WindowUDF>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

struct TaskContextPrivateData {
    ctx: Arc<TaskContext>,
}

impl FFI_TaskContext {
    unsafe fn inner(&self) -> &Arc<TaskContext> {
        unsafe {
            let private_data = self.private_data as *const TaskContextPrivateData;
            &(*private_data).ctx
        }
    }
}

unsafe extern "C" fn session_id_fn_wrapper(ctx: &FFI_TaskContext) -> RString {
    unsafe {
        let ctx = ctx.inner();
        ctx.session_id().into()
    }
}

unsafe extern "C" fn task_id_fn_wrapper(ctx: &FFI_TaskContext) -> ROption<RString> {
    unsafe {
        let ctx = ctx.inner();
        ctx.task_id().map(|s| s.as_str().into()).into()
    }
}

unsafe extern "C" fn session_config_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> FFI_SessionConfig {
    unsafe {
        let ctx = ctx.inner();
        ctx.session_config().into()
    }
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_ScalarUDF> {
    unsafe {
        let ctx = ctx.inner();
        ctx.scalar_functions()
            .iter()
            .map(|(name, udf)| (name.to_owned().into(), Arc::clone(udf).into()))
            .collect()
    }
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_AggregateUDF> {
    unsafe {
        let ctx = ctx.inner();
        ctx.aggregate_functions()
            .iter()
            .map(|(name, udaf)| {
                (
                    name.to_owned().into(),
                    FFI_AggregateUDF::from(Arc::clone(udaf)),
                )
            })
            .collect()
    }
}

unsafe extern "C" fn window_functions_fn_wrapper(
    ctx: &FFI_TaskContext,
) -> RHashMap<RString, FFI_WindowUDF> {
    unsafe {
        let ctx = ctx.inner();
        ctx.window_functions()
            .iter()
            .map(|(name, udf)| {
                (name.to_owned().into(), FFI_WindowUDF::from(Arc::clone(udf)))
            })
            .collect()
    }
}

unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_TaskContext) {
    unsafe {
        let private_data = Box::from_raw(ctx.private_data as *mut TaskContextPrivateData);
        drop(private_data);
    }
}

impl Drop for FFI_TaskContext {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl From<Arc<TaskContext>> for FFI_TaskContext {
    fn from(ctx: Arc<TaskContext>) -> Self {
        let private_data = Box::new(TaskContextPrivateData { ctx });

        FFI_TaskContext {
            session_id: session_id_fn_wrapper,
            task_id: task_id_fn_wrapper,
            session_config: session_config_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl From<FFI_TaskContext> for Arc<TaskContext> {
    fn from(ffi_ctx: FFI_TaskContext) -> Self {
        unsafe {
            if (ffi_ctx.library_marker_id)() == crate::get_library_marker_id() {
                return Arc::clone(ffi_ctx.inner());
            }

            let task_id = (ffi_ctx.task_id)(&ffi_ctx).map(|s| s.to_string()).into();
            let session_id = (ffi_ctx.session_id)(&ffi_ctx).into();
            let session_config = (ffi_ctx.session_config)(&ffi_ctx);
            let session_config =
                SessionConfig::try_from(&session_config).unwrap_or_default();

            let scalar_functions = (ffi_ctx.scalar_functions)(&ffi_ctx)
                .into_iter()
                .map(|kv_pair| {
                    let udf = <Arc<dyn ScalarUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(ScalarUDF::new_from_shared_impl(udf)),
                    )
                })
                .collect();
            let aggregate_functions = (ffi_ctx.aggregate_functions)(&ffi_ctx)
                .into_iter()
                .map(|kv_pair| {
                    let udaf = <Arc<dyn AggregateUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(AggregateUDF::new_from_shared_impl(udaf)),
                    )
                })
                .collect();
            let window_functions = (ffi_ctx.window_functions)(&ffi_ctx)
                .into_iter()
                .map(|kv_pair| {
                    let udwf = <Arc<dyn WindowUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(WindowUDF::new_from_shared_impl(udwf)),
                    )
                })
                .collect();

            let runtime = Arc::new(RuntimeEnv::default());

            Arc::new(TaskContext::new(
                task_id,
                session_id,
                session_config,
                scalar_functions,
                aggregate_functions,
                window_functions,
                runtime,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_execution::TaskContext;

    use crate::execution::FFI_TaskContext;

    #[test]
    fn ffi_task_ctx_round_trip() -> Result<()> {
        let session_ctx = SessionContext::new();
        let original = session_ctx.task_ctx();
        let mut ffi_task_ctx = FFI_TaskContext::from(Arc::clone(&original));
        ffi_task_ctx.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_task_ctx: Arc<TaskContext> = ffi_task_ctx.into();

        // TaskContext doesn't implement Eq (nor should it) so check some of the
        // data is round tripping correctly.

        assert_eq!(
            original.scalar_functions(),
            foreign_task_ctx.scalar_functions()
        );
        assert_eq!(
            original.aggregate_functions(),
            foreign_task_ctx.aggregate_functions()
        );
        assert_eq!(
            original.window_functions(),
            foreign_task_ctx.window_functions()
        );
        assert_eq!(original.task_id(), foreign_task_ctx.task_id());
        assert_eq!(original.session_id(), foreign_task_ctx.session_id());
        assert_eq!(
            format!("{:?}", original.session_config()),
            format!("{:?}", foreign_task_ctx.session_config())
        );

        Ok(())
    }
}
