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
use std::sync::{Arc, Weak};

use abi_stable::StableAbi;
use datafusion_common::{DataFusionError, ffi_datafusion_err};
use datafusion_execution::{TaskContext, TaskContextProvider};

use crate::execution::task_ctx::FFI_TaskContext;
use crate::util::FFIResult;
use crate::{df_result, rresult};

/// Struct for accessing the [`TaskContext`]. This method contains a weak
/// reference, so there are no guarantees that the [`TaskContext`] remains
/// valid. This is used primarily for protobuf encoding and decoding of
/// data passed across the FFI boundary. See the crate README for
/// additional information.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TaskContextProvider {
    /// Retrieve the current [`TaskContext`] provided the provider has not
    /// gone out of scope. This function will return an error if the weakly
    /// held reference to the underlying [`TaskContextProvider`] is no longer
    /// available.
    pub task_ctx: unsafe extern "C" fn(&Self) -> FFIResult<FFI_TaskContext>,

    /// Used to create a clone on the task context accessor. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

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

unsafe impl Send for FFI_TaskContextProvider {}
unsafe impl Sync for FFI_TaskContextProvider {}

struct TaskContextProviderPrivateData {
    ctx: Weak<dyn TaskContextProvider>,
}

impl FFI_TaskContextProvider {
    unsafe fn inner(&self) -> Option<Arc<TaskContext>> {
        unsafe {
            let private_data = self.private_data as *const TaskContextProviderPrivateData;
            (*private_data).ctx.upgrade().map(|ctx| ctx.task_ctx())
        }
    }
}

unsafe extern "C" fn task_ctx_fn_wrapper(
    ctx_provider: &FFI_TaskContextProvider,
) -> FFIResult<FFI_TaskContext> {
    unsafe {
        rresult!(
            ctx_provider
                .inner()
                .map(FFI_TaskContext::from)
                .ok_or_else(|| {
                    ffi_datafusion_err!(
                        "TaskContextProvider went out of scope over FFI boundary."
                    )
                })
        )
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_TaskContextProvider,
) -> FFI_TaskContextProvider {
    unsafe {
        let private_data = provider.private_data as *const TaskContextProviderPrivateData;
        let ctx = Weak::clone(&(*private_data).ctx);

        let private_data = Box::new(TaskContextProviderPrivateData { ctx });

        FFI_TaskContextProvider {
            task_ctx: task_ctx_fn_wrapper,
            release: release_fn_wrapper,
            clone: clone_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}
unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_TaskContextProvider) {
    unsafe {
        let private_data =
            Box::from_raw(ctx.private_data as *mut TaskContextProviderPrivateData);
        drop(private_data);
    }
}
impl Drop for FFI_TaskContextProvider {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Clone for FFI_TaskContextProvider {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<&Arc<dyn TaskContextProvider>> for FFI_TaskContextProvider {
    fn from(ctx: &Arc<dyn TaskContextProvider>) -> Self {
        let ctx = Arc::downgrade(ctx);
        let private_data = Box::new(TaskContextProviderPrivateData { ctx });

        FFI_TaskContextProvider {
            task_ctx: task_ctx_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl TryFrom<&FFI_TaskContextProvider> for Arc<TaskContext> {
    type Error = DataFusionError;
    fn try_from(ffi_ctx: &FFI_TaskContextProvider) -> Result<Self, Self::Error> {
        unsafe {
            if (ffi_ctx.library_marker_id)() == crate::get_library_marker_id() {
                return ffi_ctx.inner().ok_or_else(|| {
                    ffi_datafusion_err!(
                        "TaskContextProvider went out of scope over FFI boundary."
                    )
                });
            }

            df_result!((ffi_ctx.task_ctx)(ffi_ctx)).map(Into::into)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::{DataFusionError, Result};
    use datafusion_execution::{TaskContext, TaskContextProvider};

    use crate::execution::FFI_TaskContextProvider;

    #[derive(Default)]
    struct TestCtxProvider {
        ctx: Arc<TaskContext>,
    }

    impl TaskContextProvider for TestCtxProvider {
        fn task_ctx(&self) -> Arc<TaskContext> {
            Arc::clone(&self.ctx)
        }
    }

    #[test]
    fn ffi_task_context_provider_round_trip() -> Result<()> {
        let ctx = Arc::new(TestCtxProvider::default()) as Arc<dyn TaskContextProvider>;
        let mut ffi_ctx_provider: FFI_TaskContextProvider = (&Arc::clone(&ctx)).into();
        ffi_ctx_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_task_ctx: Arc<TaskContext> = (&ffi_ctx_provider).try_into()?;

        assert_eq!(
            format!("{foreign_task_ctx:?}"),
            format!("{:?}", ctx.task_ctx())
        );

        Ok(())
    }

    #[test]
    fn ffi_task_context_provider_clone() -> Result<()> {
        let ctx = Arc::new(TestCtxProvider::default()) as Arc<dyn TaskContextProvider>;
        let first_provider: FFI_TaskContextProvider = (&ctx).into();

        let second_provider = first_provider.clone();

        let first_ctx: Arc<TaskContext> = (&first_provider).try_into()?;
        let second_ctx: Arc<TaskContext> = (&second_provider).try_into()?;

        assert!(Arc::ptr_eq(&first_ctx, &second_ctx));

        Ok(())
    }

    #[test]
    fn ffi_task_context_provider_out_of_scope() {
        fn create_ffi_out_of_scope() -> FFI_TaskContextProvider {
            let ctx =
                Arc::new(TestCtxProvider::default()) as Arc<dyn TaskContextProvider>;
            (&ctx).into()
        }

        let provider = create_ffi_out_of_scope();
        let failed_ctx = <Arc<TaskContext>>::try_from(&provider);

        let Err(DataFusionError::Ffi(_)) = failed_ctx else {
            panic!("Expected out of scope error")
        };
    }
}
