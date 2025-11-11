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

use crate::session::task_context::FFI_TaskContext;
use crate::{df_result, rresult};
use abi_stable::std_types::RResult;
use abi_stable::{std_types::RString, StableAbi};
use datafusion_common::{exec_datafusion_err, DataFusionError};
use datafusion_execution::{TaskContext, TaskContextAccessor};
use std::sync::Weak;
use std::{ffi::c_void, sync::Arc};

/// Struct for accessing the [`TaskContext`]. This method contains a weak
/// reference, so there are no guarantees that the [`TaskContext`] remains
/// valid. This is used primarily for protobuf encoding and decoding of
/// data passed across the FFI boundary. See the crate README for
/// additional information.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TaskContextAccessor {
    pub get_task_context:
        unsafe extern "C" fn(&Self) -> RResult<FFI_TaskContext, RString>,

    /// Used to create a clone on the task context accessor. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> u64,
}

unsafe impl Send for FFI_TaskContextAccessor {}
unsafe impl Sync for FFI_TaskContextAccessor {}

struct TaskContextAccessorPrivateData {
    ctx: Weak<dyn TaskContextAccessor>,
}

impl FFI_TaskContextAccessor {
    unsafe fn inner(&self) -> Option<Arc<TaskContext>> {
        let private_data = self.private_data as *const TaskContextAccessorPrivateData;
        (*private_data)
            .ctx
            .upgrade()
            .map(|ctx| ctx.get_task_context())
    }
}

unsafe extern "C" fn get_task_context_fn_wrapper(
    ctx_accessor: &FFI_TaskContextAccessor,
) -> RResult<FFI_TaskContext, RString> {
    rresult!(ctx_accessor
        .inner()
        .map(|ctx| FFI_TaskContext::new(ctx, ctx_accessor.clone()))
        .ok_or_else(|| {
            exec_datafusion_err!(
                "TaskContextAccessor went out of scope over FFI boundary."
            )
        }))
}

unsafe extern "C" fn clone_fn_wrapper(
    accessor: &FFI_TaskContextAccessor,
) -> FFI_TaskContextAccessor {
    let private_data = accessor.private_data as *const TaskContextAccessorPrivateData;
    let ctx = Weak::clone(&(*private_data).ctx);

    let private_data = Box::new(TaskContextAccessorPrivateData { ctx });

    FFI_TaskContextAccessor {
        get_task_context: get_task_context_fn_wrapper,
        release: release_fn_wrapper,
        clone: clone_fn_wrapper,
        private_data: Box::into_raw(private_data) as *mut c_void,
        library_marker_id: crate::get_library_marker_id,
    }
}
unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_TaskContextAccessor) {
    let private_data =
        Box::from_raw(ctx.private_data as *mut TaskContextAccessorPrivateData);
    drop(private_data);
}
impl Drop for FFI_TaskContextAccessor {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Clone for FFI_TaskContextAccessor {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<dyn TaskContextAccessor>> for FFI_TaskContextAccessor {
    fn from(ctx: Arc<dyn TaskContextAccessor>) -> Self {
        (&ctx).into()
    }
}

impl From<&Arc<dyn TaskContextAccessor>> for FFI_TaskContextAccessor {
    fn from(ctx: &Arc<dyn TaskContextAccessor>) -> Self {
        let ctx = Arc::downgrade(ctx);
        let private_data = Box::new(TaskContextAccessorPrivateData { ctx });

        FFI_TaskContextAccessor {
            get_task_context: get_task_context_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl TryFrom<&FFI_TaskContextAccessor> for Arc<TaskContext> {
    type Error = DataFusionError;
    fn try_from(ffi_ctx: &FFI_TaskContextAccessor) -> Result<Self, Self::Error> {
        unsafe {
            if (ffi_ctx.library_marker_id)() == crate::get_library_marker_id() {
                return ffi_ctx.inner().ok_or_else(|| {
                    exec_datafusion_err!(
                        "TaskContextAccessor went out of scope over FFI boundary."
                    )
                });
            }

            df_result!((ffi_ctx.get_task_context)(ffi_ctx))
                .map(Into::into)
                .map(Arc::new)
        }
    }
}
