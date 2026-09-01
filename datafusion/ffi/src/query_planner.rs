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

//! FFI support for [`QueryPlanner`].
//!
//! A typical deployment has three libraries. Library A (for example,
//! `datafusion-python`) owns the [`Session`] and codec registry. Library B owns
//! a custom table provider and its extension nodes. Library C (for example,
//! Ballista or `datafusion-distributed`) owns the query planner. A serializes a
//! logical plan and invokes C, while `FFI_SessionRef` lets C call session
//! services in A. C deserializes the logical plan, creates a physical plan,
//! serializes that result, and returns it for A to deserialize. The logical and
//! physical extension codecs preserve nodes supplied by B.
//!
//! The physical result is serialized instead of returned as an
//! [`crate::execution_plan::FFI_ExecutionPlan`]. An FFI execution-plan handle is
//! a foreign trait-object proxy, so even a built-in plan created in C cannot be
//! downcast to its concrete
//! type in A. Serialization reconstructs known plan nodes with A's local Rust
//! type identities, allowing A's optimizers and other consumers to downcast
//! them. Extension codecs control how custom nodes are reconstructed.
//!
//! A node returned by B while C is planning is still foreign to C unless a
//! codec boundary reconstructs it in C. The query-planner boundary guarantees
//! that C-local serializable nodes, and extension nodes understood by the
//! configured codecs, are reconstructed for A when the completed plan returns.
//!
//! # Delegating back to library A
//!
//! C commonly wants A's built-in planning as a starting point, then rewrites the
//! result. A must export its planner *before* installing C's planner on the
//! session, and C must retain that handle: after the swap,
//! [`Session::query_planner`] reports C's own planner, and
//! [`Session::create_physical_plan`] dispatches to it, so either one is a
//! self-call. Delegating to the retained handle is safe, because DataFusion's
//! built-in physical planner never re-dispatches through [`Session`].
//!
//! Retain the planner rather than the session. [`FFI_QueryPlanner`] owns a
//! reference-counted planner, so it outlives A's original session, whereas
//! `FFI_SessionRef` borrows its session with the lifetime erased.

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
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_session::{QueryPlanner, Session};
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use crate::session::{FFI_SessionRef, ForeignSession};
use crate::util::FFI_Result;
use crate::{df_result, sresult_return};

/// An ABI-stable handle to a [`QueryPlanner`] owned by another library.
///
/// The Rust-facing adapters serialize the input [`LogicalPlan`] and resulting
/// [`ExecutionPlan`]; callers do not invoke the byte-oriented function pointer
/// directly.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_QueryPlanner {
    create_physical_plan: unsafe extern "C" fn(
        &Self,
        logical_plan_serialized: SVec<u8>,
        session: FFI_SessionRef,
    ) -> FfiFuture<FFI_Result<SVec<u8>>>,

    /// Codec used to encode and decode logical plans and extension nodes.
    logical_codec: FFI_LogicalExtensionCodec,

    /// Codec used to encode and decode physical plans and extension nodes.
    physical_codec: FFI_PhysicalExtensionCodec,

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
}

impl FFI_QueryPlanner {
    fn inner(&self) -> &Arc<dyn QueryPlanner + Send + Sync> {
        let private_data = self.private_data as *const QueryPlannerPrivateData;
        unsafe { &(*private_data).planner }
    }
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    planner: &FFI_QueryPlanner,
    logical_plan_serialized: SVec<u8>,
    session: FFI_SessionRef,
) -> FfiFuture<FFI_Result<SVec<u8>>> {
    let internal_planner = Arc::clone(planner.inner());
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&planner.logical_codec).into();
    let physical_codec: Arc<dyn PhysicalExtensionCodec> =
        (&planner.physical_codec).into();

    async move {
        let mut foreign_session = None;
        let session = sresult_return!(
            session
                .as_local()
                .map(Ok::<&dyn Session, DataFusionError>)
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
        let physical_plan = sresult_return!(physical_plan_to_bytes_with_extension_codec(
            physical_plan,
            physical_codec.as_ref(),
        ));

        FFI_Result::Ok(SVec::from(physical_plan.as_ref()))
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(planner: &mut FFI_QueryPlanner) {
    unsafe {
        debug_assert!(!planner.private_data.is_null());
        let private_data =
            Box::from_raw(planner.private_data.cast::<QueryPlannerPrivateData>());
        drop(private_data);
        planner.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(planner: &FFI_QueryPlanner) -> FFI_QueryPlanner {
    let old_planner = Arc::clone(planner.inner());

    let private_data = Box::into_raw(Box::new(QueryPlannerPrivateData {
        planner: old_planner,
    }))
    .cast::<c_void>();

    FFI_QueryPlanner {
        create_physical_plan: create_physical_plan_fn_wrapper,
        logical_codec: planner.logical_codec.clone(),
        physical_codec: planner.physical_codec.clone(),
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
    /// Creates an [`FFI_QueryPlanner`] with native extension codecs.
    ///
    /// Both codecs are required so that the caller states which extension nodes
    /// survive the boundary. Pass
    /// [`DefaultLogicalExtensionCodec`](datafusion_proto::logical_plan::DefaultLogicalExtensionCodec)
    /// and
    /// [`DefaultPhysicalExtensionCodec`](datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec)
    /// when no custom nodes are involved. `runtime` and `task_ctx_provider`
    /// support codec callbacks across the FFI boundary.
    pub fn new(
        planner: Arc<dyn QueryPlanner + Send + Sync>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        logical_codec: Arc<dyn LogicalExtensionCodec>,
        physical_codec: Arc<dyn PhysicalExtensionCodec>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.clone(),
            task_ctx_provider.clone(),
        );
        let physical_codec =
            FFI_PhysicalExtensionCodec::new(physical_codec, runtime, task_ctx_provider);
        Self::new_with_ffi_codecs(planner, logical_codec, physical_codec)
    }

    /// Creates an [`FFI_QueryPlanner`] using prebuilt FFI extension codecs.
    ///
    /// If `planner` is already foreign, this re-exports its original FFI handle
    /// rather than adding another wrapper layer. The handle still adopts the
    /// codecs supplied here, so they are never silently discarded.
    pub fn new_with_ffi_codecs(
        planner: Arc<dyn QueryPlanner + Send + Sync>,
        logical_codec: FFI_LogicalExtensionCodec,
        physical_codec: FFI_PhysicalExtensionCodec,
    ) -> Self {
        let any_ref: &dyn std::any::Any = planner.as_ref();
        if let Some(planner) = any_ref.downcast_ref::<ForeignQueryPlanner>() {
            let mut planner = planner.0.clone();
            planner.logical_codec = logical_codec;
            planner.physical_codec = physical_codec;
            return planner;
        }

        let private_data = Box::new(QueryPlannerPrivateData { planner });

        Self {
            create_physical_plan: create_physical_plan_fn_wrapper,
            logical_codec,
            physical_codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data).cast::<c_void>(),
            library_marker_id: crate::get_library_marker_id,
        }
    }

    /// Creates a physical plan through this planner's FFI interface.
    ///
    /// This serializes `logical_plan`, exports `session` as an
    /// `FFI_SessionRef`, invokes the planner's owning library, and
    /// deserializes its physical-plan response. `session_runtime` is attached
    /// to the exported session for callbacks that need its Tokio runtime.
    ///
    /// The [`QueryPlanner`] implementation for [`ForeignQueryPlanner`] cannot
    /// obtain the session owner's runtime from the trait API, so it calls this
    /// method with `None`. Embedders that own the runtime and need session
    /// callbacks to enter it must call this method directly with `Some(handle)`.
    pub async fn create_physical_plan_with_session_runtime(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
        session_runtime: Option<Handle>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let codec: Arc<dyn LogicalExtensionCodec> = (&self.logical_codec).into();
        let logical_plan =
            logical_plan_to_bytes_with_extension_codec(logical_plan, codec.as_ref())?;
        let logical_plan = SVec::from(logical_plan.as_ref());
        let task_ctx = session.task_ctx();
        let session = FFI_SessionRef::new_with_ffi_codecs(
            session,
            session_runtime,
            self.logical_codec.clone(),
            self.physical_codec.clone(),
        );

        let physical_plan = unsafe {
            df_result!((self.create_physical_plan)(self, logical_plan, session).await)?
        };
        let physical_codec: Arc<dyn PhysicalExtensionCodec> =
            (&self.physical_codec).into();

        physical_plan_from_bytes_with_extension_codec(
            physical_plan.as_slice(),
            task_ctx.as_ref(),
            physical_codec.as_ref(),
        )
    }
}

/// Consumer-side [`QueryPlanner`] adapter for an [`FFI_QueryPlanner`].
///
/// Calls serialize the logical plan, invoke the producing library, and
/// deserialize its physical-plan response.
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
        self.0
            .create_physical_plan_with_session_runtime(logical_plan, session, None)
            .await
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
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
    use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;

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
        FFI_QueryPlanner::new(
            Arc::new(EmptyQueryPlanner),
            None,
            &task_ctx_provider,
            Arc::new(DefaultLogicalExtensionCodec {}),
            Arc::new(DefaultPhysicalExtensionCodec {}),
        )
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
        assert!(physical_plan.is::<EmptyExec>());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_physical_plan_with_session_runtime() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        let ffi_planner = create_ffi_query_planner(Arc::clone(&ctx));
        let logical_plan = LogicalPlanBuilder::empty(false).build()?;
        let state = ctx.state();

        let physical_plan = ffi_planner
            .create_physical_plan_with_session_runtime(
                &logical_plan,
                &state,
                Some(Handle::current()),
            )
            .await?;

        assert_eq!(physical_plan.name(), "EmptyExec");
        assert!(physical_plan.is::<EmptyExec>());

        Ok(())
    }

    // Control for https://github.com/apache/datafusion/issues/24722: this
    // constructor adopts the supplied codecs on the already-foreign path.
    #[test]
    fn test_rebind_foreign_query_planner_adopts_codecs() {
        use datafusion_execution::TaskContext;

        let ctx_a = Arc::new(SessionContext::new());
        let ctx_b = Arc::new(SessionContext::new());
        let provider_b = Arc::clone(&ctx_b) as Arc<dyn TaskContextProvider>;

        let mut ffi_a = create_ffi_query_planner(Arc::clone(&ctx_a));
        ffi_a.library_marker_id = crate::mock_foreign_marker_id;
        let imported: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_a).into();
        let any_ref: &dyn std::any::Any = imported.as_ref();
        assert!(any_ref.downcast_ref::<ForeignQueryPlanner>().is_some());

        let rebound = FFI_QueryPlanner::new_with_ffi_codecs(
            imported,
            FFI_LogicalExtensionCodec::new(
                Arc::new(DefaultLogicalExtensionCodec {}),
                None,
                &provider_b,
            ),
            FFI_PhysicalExtensionCodec::new(
                Arc::new(DefaultPhysicalExtensionCodec {}),
                None,
                &provider_b,
            ),
        );

        let bound_to: Arc<TaskContext> = (&rebound.logical_codec.task_ctx_provider)
            .try_into()
            .unwrap();
        assert_eq!(bound_to.session_id(), ctx_b.task_ctx().session_id());
    }
}
