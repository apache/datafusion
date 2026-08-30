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

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use tokio::runtime::Handle;

use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

use crate::execution::runtime_env::FFI_RuntimeEnv;
use crate::session::config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFI_Option;

/// A stable struct for sharing [`TaskContext`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_TaskContext {
    /// Return the session ID.
    pub session_id: unsafe extern "C" fn(&Self) -> SString,

    /// Return the task ID.
    pub task_id: unsafe extern "C" fn(&Self) -> FFI_Option<SString>,

    /// Return the session configuration.
    pub session_config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    /// Returns a vec of name-function pairs for scalar functions.
    pub scalar_functions: unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_ScalarUDF)>,

    /// Returns a vec of name-function pairs for aggregate functions.
    pub aggregate_functions:
        unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_AggregateUDF)>,

    /// Returns a vec of name-function pairs for window functions.
    pub window_functions: unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_WindowUDF)>,

    /// Returns the runtime environment.
    ///
    /// A plan executing on the far side of the boundary reaches the object
    /// stores and the memory budget of the executing session through this,
    /// so a store registered on the session during planning is available at
    /// execution time and allocations count against the session's memory
    /// limit.
    pub runtime_env: unsafe extern "C" fn(&Self) -> FFI_RuntimeEnv,

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
    /// Tokio runtime handle of the providing library, attached to object stores
    /// handed out by this context's runtime environment.
    runtime: Option<Handle>,
}

impl FFI_TaskContext {
    unsafe fn inner(&self) -> &Arc<TaskContext> {
        unsafe {
            let private_data = self.private_data as *const TaskContextPrivateData;
            &(*private_data).ctx
        }
    }
}

unsafe extern "C" fn session_id_fn_wrapper(ctx: &FFI_TaskContext) -> SString {
    unsafe {
        let ctx = ctx.inner();
        ctx.session_id().into()
    }
}

unsafe extern "C" fn task_id_fn_wrapper(ctx: &FFI_TaskContext) -> FFI_Option<SString> {
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
) -> SVec<(SString, FFI_ScalarUDF)> {
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
) -> SVec<(SString, FFI_AggregateUDF)> {
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
) -> SVec<(SString, FFI_WindowUDF)> {
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

unsafe extern "C" fn runtime_env_fn_wrapper(ctx: &FFI_TaskContext) -> FFI_RuntimeEnv {
    unsafe {
        let private_data = ctx.private_data as *const TaskContextPrivateData;
        FFI_RuntimeEnv::new(
            Arc::clone(&(*private_data).ctx.runtime_env()),
            (*private_data).runtime.clone(),
        )
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

impl FFI_TaskContext {
    /// Create a new [`FFI_TaskContext`] from a local task context.
    ///
    /// `runtime` is the tokio runtime handle of the library creating this
    /// context. It is attached to the object stores reached through this
    /// context's runtime environment and entered while they are polled, so
    /// that stores which spawn tasks or use timers work when driven by a
    /// foreign executor.
    ///
    /// Pass `None` only when the context will not be used to reach an object
    /// store, such as when it serves purely as a
    /// [`FunctionRegistry`](datafusion_expr::registry::FunctionRegistry)
    /// while encoding or decoding a plan.
    pub fn new(ctx: Arc<TaskContext>, runtime: Option<Handle>) -> Self {
        let private_data = Box::new(TaskContextPrivateData { ctx, runtime });

        FFI_TaskContext {
            session_id: session_id_fn_wrapper,
            task_id: task_id_fn_wrapper,
            session_config: session_config_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            runtime_env: runtime_env_fn_wrapper,
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
                        kv_pair.0.to_string(),
                        Arc::new(ScalarUDF::new_from_shared_impl(udf)),
                    )
                })
                .collect();
            let aggregate_functions = (ffi_ctx.aggregate_functions)(&ffi_ctx)
                .into_iter()
                .map(|kv_pair| {
                    let udaf = <Arc<dyn AggregateUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.to_string(),
                        Arc::new(AggregateUDF::new_from_shared_impl(udaf)),
                    )
                })
                .collect();
            let window_functions = (ffi_ctx.window_functions)(&ffi_ctx)
                .into_iter()
                .map(|kv_pair| {
                    let udwf = <Arc<dyn WindowUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.to_string(),
                        Arc::new(WindowUDF::new_from_shared_impl(udwf)),
                    )
                })
                .collect();

            // The providing side's runtime environment carries the registered
            // object stores and the memory pool this context should execute
            // against.
            let ffi_runtime_env = (ffi_ctx.runtime_env)(&ffi_ctx);
            let runtime_env = <Arc<RuntimeEnv>>::try_from(&ffi_runtime_env)
                .unwrap_or_else(|e| {
                    log::warn!(
                        "Unable to reconstruct the runtime environment across \
                         the FFI boundary, falling back to a default: {e}"
                    );
                    Arc::new(RuntimeEnv::default())
                });

            Arc::new(TaskContext::new(
                task_id,
                session_id,
                session_config,
                scalar_functions,
                HashMap::new(),
                aggregate_functions,
                window_functions,
                runtime_env,
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
        let mut ffi_task_ctx = FFI_TaskContext::new(Arc::clone(&original), None);
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
