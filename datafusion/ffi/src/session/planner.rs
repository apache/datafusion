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
use abi_stable::std_types::RVec;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec,
    logical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use datafusion_session::{QueryPlanner, Session};
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::session::{FFI_SessionRef, ForeignSession};
use crate::util::FFIResult;
use crate::{df_result, rresult, rresult_return};

#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_QueryPlanner {
    // it would make sense, for ballista, to get access to this method.
    // as the plan is going to be decode at the scheduler side.
    //
    // at the moment,FFI_SessionRef is not public, so we'd need to change it
    /// Given a [`LogicalPlan`], create an [`ExecutionPlan`] suitable for execution
    create_physical_plan: unsafe extern "C" fn(
        &Self,
        logical_plan_serialized: RVec<u8>,
        session: FFI_SessionRef,
    )
        -> FfiFuture<FFIResult<FFI_ExecutionPlan>>,

    /// Logical codec used to provide encoding and decoding of plans.
    pub logical_codec: FFI_LogicalExtensionCodec,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the planner of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Used to create a clone on the planner . This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_QueryPlanner {}
unsafe impl Sync for FFI_QueryPlanner {}

struct QueryPlannerPrivateData {
    planner: Arc<dyn QueryPlanner>,
}

impl FFI_QueryPlanner {
    fn inner(&self) -> &Arc<dyn QueryPlanner> {
        unsafe {
            let private_data = self.private_data as *const QueryPlannerPrivateData;
            &(*private_data).planner
        }
    }
}

unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_QueryPlanner) {
    unsafe {
        let private_data =
            Box::from_raw(ctx.private_data as *mut QueryPlannerPrivateData);
        drop(private_data);
    }
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    planner: &FFI_QueryPlanner,
    logical_plan_serialized: RVec<u8>,
    session: FFI_SessionRef,
) -> FfiFuture<FFIResult<FFI_ExecutionPlan>> {
    unsafe {
        let planner = Arc::clone(planner.inner());
        let codec: Arc<dyn LogicalExtensionCodec> = (&session.logical_codec).into();
        let runtime = session.runtime().clone();

        async move {
            let mut foreign_session = None;
            let session = rresult_return!(
                session
                    .as_local()
                    .map(Ok::<&(dyn Session + Send + Sync), DataFusionError>)
                    .unwrap_or_else(|| {
                        foreign_session = Some(ForeignSession::try_from(&session)?);
                        Ok(foreign_session.as_ref().unwrap())
                    })
            );

            let task_ctx: Arc<TaskContext> = session.task_ctx();

            let logical_plan =
                rresult_return!(logical_plan_from_bytes_with_extension_codec(
                    logical_plan_serialized.as_slice(),
                    task_ctx.as_ref(),
                    codec.as_ref()
                ));

            let physical_plan =
                planner.create_physical_plan(&logical_plan, session).await;

            rresult!(physical_plan.map(|plan| FFI_ExecutionPlan::new(plan, runtime)))
        }
        .into_ffi()
    }
}

unsafe extern "C" fn clone_fn_wrapper(planner: &FFI_QueryPlanner) -> FFI_QueryPlanner {
    let codec = planner.logical_codec.clone();
    let planner = Arc::clone(planner.inner());
    FFI_QueryPlanner::new_with_ffi_codec(planner, codec)
}

impl Drop for FFI_QueryPlanner {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_QueryPlanner {
    pub fn new(
        planner: Arc<dyn QueryPlanner>,
        runtime: Option<&Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    ) -> Self {
        let logical_codec =
            logical_codec.unwrap_or_else(|| Arc::new(DefaultLogicalExtensionCodec {}));
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.cloned(),
            task_ctx_provider.into(),
        );

        Self::new_with_ffi_codec(planner, logical_codec)
    }

    pub fn new_with_ffi_codec(
        planner: Arc<dyn QueryPlanner>,
        codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let private_data = Box::new(QueryPlannerPrivateData { planner });

        Self {
            create_physical_plan: create_physical_plan_fn_wrapper,
            logical_codec: codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Clone for FFI_QueryPlanner {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[derive(Debug)]
pub struct ForeignQueryPlanner(pub FFI_QueryPlanner);

impl From<&FFI_QueryPlanner> for Arc<dyn QueryPlanner> {
    fn from(planner: &FFI_QueryPlanner) -> Self {
        if (planner.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(planner.inner())
        } else {
            Arc::new(ForeignQueryPlanner(planner.clone()))
        }
    }
}

#[async_trait]
impl QueryPlanner for ForeignQueryPlanner {
    /// Given a [`LogicalPlan`], create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();
        let logical_plan_buf =
            logical_plan_to_bytes_with_extension_codec(logical_plan, codec.as_ref())?;

        let session_ref =
            FFI_SessionRef::new(session_state, None, self.0.logical_codec.clone());

        let plan = df_result!(unsafe {
            (self.0.create_physical_plan)(
                &self.0,
                logical_plan_buf.as_ref().into(),
                session_ref,
            )
            .await
        })?;

        Ok((&plan).try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::LogicalPlan;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_session::{QueryPlanner, Session};

    use crate::session::planner::FFI_QueryPlanner;

    #[derive(Debug, Default)]
    struct DummyPlanner {}

    #[async_trait::async_trait]
    impl QueryPlanner for DummyPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            _session_state: &dyn Session,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            let schema = logical_plan.schema().as_arrow().clone();
            // will need better test
            Ok(Arc::new(EmptyExec::new(Arc::new(schema))))
        }
    }

    #[tokio::test]
    async fn test_end_to_end() -> datafusion::common::Result<()> {
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let df = ctx.sql("select 1 as i").await?;
        let logical_plan = df.logical_plan();

        let planner: Arc<dyn QueryPlanner> = Arc::new(DummyPlanner::default());

        let mut ffi_planner =
            FFI_QueryPlanner::new(planner, None, task_ctx_provider, None);
        ffi_planner.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_planner: Arc<dyn QueryPlanner> = (&ffi_planner).into();

        let empty_exec = foreign_planner
            .create_physical_plan(logical_plan, &ctx.state())
            .await?;

        assert!(empty_exec.as_any().downcast_ref::<EmptyExec>().is_some());
        Ok(())
    }
}
