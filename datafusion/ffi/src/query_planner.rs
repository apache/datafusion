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

use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_common::error::{DataFusionError, Result};
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
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::session::{FFI_SessionRef, ForeignSession};
use crate::util::FFI_Result;
use crate::{df_result, sresult_return};

/// A stable struct for sharing [`QueryPlanner`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_QueryPlanner {
    create_physical_plan:
        unsafe extern "C" fn(
            &Self,
            logical_plan_serialized: SVec<u8>,
            session: FFI_SessionRef,
        ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    pub logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone of the query planner.
    clone: unsafe extern "C" fn(planner: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this planner.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the planner.
    /// A [`ForeignQueryPlanner`] should never attempt to access this data.
    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`].
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_QueryPlanner {}
unsafe impl Sync for FFI_QueryPlanner {}

struct QueryPlannerPrivateData {
    planner: Arc<dyn QueryPlanner + Send + Sync>,
    runtime: Option<Handle>,
}

impl FFI_QueryPlanner {
    fn inner(&self) -> &Arc<dyn QueryPlanner + Send + Sync> {
        let private_data = self.private_data as *const QueryPlannerPrivateData;
        unsafe { &(*private_data).planner }
    }

    fn runtime(&self) -> &Option<Handle> {
        let private_data = self.private_data as *const QueryPlannerPrivateData;
        unsafe { &(*private_data).runtime }
    }
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    planner: &FFI_QueryPlanner,
    logical_plan_serialized: SVec<u8>,
    session: FFI_SessionRef,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let runtime = planner.runtime().clone();
    let internal_planner = Arc::clone(planner.inner());
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&planner.logical_codec).into();

    async move {
        let mut foreign_session = None;
        let session = sresult_return!(
            session
                .as_local()
                .map(Ok::<&(dyn Session + Send + Sync), DataFusionError>)
                .unwrap_or_else(|| {
                    foreign_session = Some(ForeignSession::try_from(&session)?);
                    Ok(foreign_session.as_ref().unwrap())
                })
        );

        let logical_plan = sresult_return!(logical_plan_from_bytes_with_extension_codec(
            logical_plan_serialized.as_slice(),
            session.task_ctx().as_ref(),
            logical_codec.as_ref(),
        ));

        let physical_plan = sresult_return!(
            internal_planner
                .create_physical_plan(&logical_plan, session)
                .await
        );

        FFI_Result::Ok(FFI_ExecutionPlan::new(physical_plan, runtime.clone()))
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(planner: &mut FFI_QueryPlanner) {
    unsafe {
        debug_assert!(!planner.private_data.is_null());
        let private_data =
            Box::from_raw(planner.private_data as *mut QueryPlannerPrivateData);
        drop(private_data);
        planner.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(planner: &FFI_QueryPlanner) -> FFI_QueryPlanner {
    let runtime = planner.runtime().clone();
    let old_planner = Arc::clone(planner.inner());

    let private_data = Box::into_raw(Box::new(QueryPlannerPrivateData {
        planner: old_planner,
        runtime,
    })) as *mut c_void;

    FFI_QueryPlanner {
        create_physical_plan: create_physical_plan_fn_wrapper,
        logical_codec: planner.logical_codec.clone(),
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        library_marker_id: crate::get_library_marker_id,
    }
}

impl Drop for FFI_QueryPlanner {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl Clone for FFI_QueryPlanner {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl FFI_QueryPlanner {
    /// Creates a new [`FFI_QueryPlanner`].
    pub fn new(
        planner: Arc<dyn QueryPlanner + Send + Sync>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let logical_codec =
            logical_codec.unwrap_or_else(|| Arc::new(DefaultLogicalExtensionCodec {}));
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.clone(),
            task_ctx_provider,
        );
        Self::new_with_ffi_codec(planner, runtime, logical_codec)
    }

    pub fn new_with_ffi_codec(
        planner: Arc<dyn QueryPlanner + Send + Sync>,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let any_ref: &dyn std::any::Any = planner.as_ref();
        if let Some(planner) = any_ref.downcast_ref::<ForeignQueryPlanner>() {
            return planner.0.clone();
        }

        let private_data = Box::new(QueryPlannerPrivateData { planner, runtime });

        Self {
            create_physical_plan: create_physical_plan_fn_wrapper,
            logical_codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// Consumer-side wrapper for a foreign [`QueryPlanner`].
#[derive(Debug)]
pub struct ForeignQueryPlanner(pub FFI_QueryPlanner);

unsafe impl Send for ForeignQueryPlanner {}
unsafe impl Sync for ForeignQueryPlanner {}

impl From<&FFI_QueryPlanner> for Arc<dyn QueryPlanner + Send + Sync> {
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
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();
        let logical_plan =
            logical_plan_to_bytes_with_extension_codec(logical_plan, codec.as_ref())?;
        let logical_plan = logical_plan.iter().copied().collect();
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());

        let plan = unsafe {
            let maybe_plan =
                (self.0.create_physical_plan)(&self.0, logical_plan, session).await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use datafusion_common::Result;
    use datafusion_execution::TaskContextProvider;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_physical_plan::empty::EmptyExec;

    use super::*;

    #[derive(Debug)]
    struct EmptyQueryPlanner;

    #[async_trait]
    impl QueryPlanner for EmptyQueryPlanner {
        async fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _session: &dyn Session,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
            Ok(Arc::new(EmptyExec::new(schema)))
        }
    }

    fn create_ffi_query_planner(ctx: Arc<SessionContext>) -> FFI_QueryPlanner {
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        FFI_QueryPlanner::new(Arc::new(EmptyQueryPlanner), None, &task_ctx_provider, None)
    }

    #[test]
    fn test_ffi_query_planner_local_bypass() {
        let ctx = Arc::new(SessionContext::new());
        let ffi_planner = create_ffi_query_planner(ctx);
        let planner: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_planner).into();
        let any_ref: &dyn std::any::Any = planner.as_ref();
        assert!(any_ref.downcast_ref::<EmptyQueryPlanner>().is_some());
    }

    #[tokio::test]
    async fn test_round_trip_ffi_query_planner_create_physical_plan() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        let mut ffi_planner = create_ffi_query_planner(Arc::clone(&ctx));
        ffi_planner.library_marker_id = crate::mock_foreign_marker_id;

        let planner: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_planner).into();
        let any_ref: &dyn std::any::Any = planner.as_ref();
        assert!(any_ref.downcast_ref::<ForeignQueryPlanner>().is_some());

        let logical_plan = LogicalPlanBuilder::empty(false).build()?;
        let state = ctx.state();
        let physical_plan = planner.create_physical_plan(&logical_plan, &state).await?;
        assert_eq!(physical_plan.name(), "EmptyExec");

        Ok(())
    }
}
