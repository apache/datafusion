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

use arrow::datatypes::SchemaRef;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::Statistics;
use datafusion_common::error::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    logical_exprs_from_bytes_with_extension_codec,
    logical_exprs_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::{
    DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};

use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use super::execution_plan::FFI_ExecutionPlan;
use super::insert_op::FFI_InsertOp;
use crate::arrow_wrappers::WrappedSchema;
use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::session::{FFI_SessionRef, ForeignSession};
use crate::statistics::{deserialize_statistics, serialize_statistics};
use crate::table_source::{FFI_TableProviderFilterPushDown, FFI_TableType};
use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult_return};

/// A stable struct for sharing [`TableProvider`] across FFI boundaries.
///
/// # Struct Layout
///
/// The following description applies to all structs provided in this crate.
///
/// Each of the exposed structs in this crate is provided with a variant prefixed
/// with `Foreign`. This variant is designed to be used by the consumer of the
/// foreign code. The `Foreign` structs should _never_ access the `private_data`
/// fields. Instead they should only access the data returned through the function
/// calls defined on the `FFI_` structs. The second purpose of the `Foreign`
/// structs is to contain additional data that may be needed by the traits that
/// are implemented on them. Some of these traits require borrowing data which
/// can be far more convenient to be locally stored.
///
/// For example, we have a struct `FFI_TableProvider` to give access to the
/// `TableProvider` functions like `table_type()` and `scan()`. If we write a
/// library that wishes to expose it's `TableProvider`, then we can access the
/// private data that contains the Arc reference to the `TableProvider` via
/// `FFI_TableProvider`. This data is local to the library.
///
/// If we have a program that accesses a `TableProvider` via FFI, then it
/// will use `ForeignTableProvider`. When using `ForeignTableProvider` we **must**
/// not attempt to access the `private_data` field in `FFI_TableProvider`. If a
/// user is testing locally, you may be able to successfully access this field, but
/// it will only work if you are building against the exact same version of
/// `DataFusion` for both libraries **and** the same compiler. It will not work
/// in general.
///
/// It is worth noting that which library is the `local` and which is `foreign`
/// depends on which interface we are considering. For example, suppose we have a
/// Python library called `my_provider` that exposes a `TableProvider` called
/// `MyProvider` via `FFI_TableProvider`. Within the library `my_provider` we can
/// access the `private_data` via `FFI_TableProvider`. We connect this to
/// `datafusion-python`, where we access it as a `ForeignTableProvider`. Now when
/// we call `scan()` on this interface, we have to pass it a `FFI_SessionConfig`.
/// The `SessionConfig` is local to `datafusion-python` and **not** `my_provider`.
/// It is important to be careful when expanding these functions to be certain which
/// side of the interface each object refers to.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_TableProvider {
    /// Return the table schema
    schema: unsafe extern "C" fn(provider: &Self) -> WrappedSchema,

    /// Perform a scan on the table. See [`TableProvider`] for detailed usage information.
    ///
    /// # Arguments
    ///
    /// * `provider` - the table provider
    /// * `session` - session
    /// * `projections` - if specified, only a subset of the columns are returned
    /// * `filters_serialized` - filters to apply to the scan, which are a
    ///   [`LogicalExprList`][datafusion_proto::protobuf::LogicalExprList] protobuf message serialized into bytes to pass
    ///   across the FFI boundary.
    /// * `limit` - if specified, limit the number of rows returned
    scan: unsafe extern "C" fn(
        provider: &Self,
        session: FFI_SessionRef,
        projections: FFI_Option<SVec<usize>>,
        filters_serialized: SVec<u8>,
        limit: FFI_Option<usize>,
    ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    /// Return the type of table. See [`TableType`] for options.
    table_type: unsafe extern "C" fn(provider: &Self) -> FFI_TableType,

    /// Based upon the input filters, identify which are supported. The filters
    /// are a [`LogicalExprList`][datafusion_proto::protobuf::LogicalExprList] protobuf message serialized into bytes to pass
    /// across the FFI boundary.
    supports_filters_pushdown: Option<
        unsafe extern "C" fn(
            provider: &FFI_TableProvider,
            filters_serialized: SVec<u8>,
        )
            -> FFI_Result<SVec<FFI_TableProviderFilterPushDown>>,
    >,

    insert_into: unsafe extern "C" fn(
        provider: &Self,
        session: FFI_SessionRef,
        input: &FFI_ExecutionPlan,
        insert_op: FFI_InsertOp,
    ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    /// Snapshot the provider's table-level statistics. [`FFI_Option::None`]
    /// corresponds to [`TableProvider::statistics`] returning `None`;
    /// `Some(bytes)` is a prost-encoded `datafusion_proto_common::Statistics`.
    pub statistics: unsafe extern "C" fn(provider: &Self) -> FFI_Option<SVec<u8>>,

    pub logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignTableProvider`] should never attempt to access this data.
    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,

    delete_from: unsafe extern "C" fn(
        provider: &Self,
        session: FFI_SessionRef,
        filters_serialized: SVec<u8>,
    ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    update: unsafe extern "C" fn(
        provider: &Self,
        session: FFI_SessionRef,
        assignments: SVec<FFI_TableProviderUpdateAssignment>,
        filters_serialized: SVec<u8>,
    ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    truncate: unsafe extern "C" fn(
        provider: &Self,
        session: FFI_SessionRef,
    ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

#[repr(C)]
#[derive(Debug)]
struct FFI_TableProviderUpdateAssignment {
    column: SString,
    expr_serialized: SVec<u8>,
}

struct ProviderPrivateData {
    provider: Arc<dyn TableProvider>,
    runtime: Option<Handle>,
}

impl FFI_TableProvider {
    fn inner(&self) -> &Arc<dyn TableProvider> {
        let private_data = self.private_data as *const ProviderPrivateData;
        unsafe { &(*private_data).provider }
    }

    fn runtime(&self) -> Option<&Handle> {
        let private_data = self.private_data as *const ProviderPrivateData;
        unsafe { (*private_data).runtime.as_ref() }
    }
}

unsafe extern "C" fn schema_fn_wrapper(provider: &FFI_TableProvider) -> WrappedSchema {
    provider.inner().schema().into()
}

unsafe extern "C" fn statistics_fn_wrapper(
    provider: &FFI_TableProvider,
) -> FFI_Option<SVec<u8>> {
    let serialized: Option<SVec<u8>> = provider
        .inner()
        .statistics()
        .map(|s| SVec::from(&*serialize_statistics(&s)));
    serialized.into()
}

unsafe extern "C" fn table_type_fn_wrapper(
    provider: &FFI_TableProvider,
) -> FFI_TableType {
    provider.inner().table_type().into()
}

fn parse_serialized_exprs(
    exprs_serialized: &[u8],
    task_ctx: &Arc<TaskContext>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<Expr>> {
    logical_exprs_from_bytes_with_extension_codec(
        exprs_serialized,
        task_ctx.as_ref(),
        codec,
    )
}

fn serialize_expr_list<'a>(
    exprs: impl IntoIterator<Item = &'a Expr>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<SVec<u8>> {
    let bytes = logical_exprs_to_bytes_with_extension_codec(exprs, codec)?;
    Ok(SVec::from(bytes.as_ref()))
}

fn supports_filters_pushdown_internal(
    provider: &Arc<dyn TableProvider>,
    filters_serialized: &[u8],
    task_ctx: &Arc<TaskContext>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<SVec<FFI_TableProviderFilterPushDown>> {
    let filters = parse_serialized_exprs(filters_serialized, task_ctx, codec)?;
    let filters_borrowed: Vec<&Expr> = filters.iter().collect();

    let results: SVec<_> = provider
        .supports_filters_pushdown(&filters_borrowed)?
        .iter()
        .map(|v| v.into())
        .collect();

    Ok(results)
}

unsafe extern "C" fn supports_filters_pushdown_fn_wrapper(
    provider: &FFI_TableProvider,
    filters_serialized: SVec<u8>,
) -> FFI_Result<SVec<FFI_TableProviderFilterPushDown>> {
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&provider.logical_codec).into();
    let task_ctx = sresult_return!(<Arc<TaskContext>>::try_from(
        &provider.logical_codec.task_ctx_provider
    ));
    supports_filters_pushdown_internal(
        provider.inner(),
        &filters_serialized,
        &task_ctx,
        logical_codec.as_ref(),
    )
    .into()
}

unsafe extern "C" fn scan_fn_wrapper(
    provider: &FFI_TableProvider,
    session: FFI_SessionRef,
    projections: FFI_Option<SVec<usize>>,
    filters_serialized: SVec<u8>,
    limit: FFI_Option<usize>,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let task_ctx: Result<Arc<TaskContext>, DataFusionError> =
        (&provider.logical_codec.task_ctx_provider).try_into();
    let runtime = provider.runtime().cloned();
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&provider.logical_codec).into();
    let internal_provider = Arc::clone(provider.inner());

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

        let task_ctx = sresult_return!(task_ctx);
        let filters = sresult_return!(parse_serialized_exprs(
            &filters_serialized,
            &task_ctx,
            logical_codec.as_ref(),
        ));

        let projections: Option<Vec<usize>> =
            projections.into_option().map(|p| p.into_iter().collect());

        let plan = sresult_return!(
            internal_provider
                .scan(session, projections.as_deref(), &filters, limit.into())
                .await
        );

        FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime.clone()))
    }
    .into_ffi()
}

unsafe extern "C" fn insert_into_fn_wrapper(
    provider: &FFI_TableProvider,
    session: FFI_SessionRef,
    input: &FFI_ExecutionPlan,
    insert_op: FFI_InsertOp,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let runtime = provider.runtime().cloned();
    let internal_provider = Arc::clone(provider.inner());
    let input = input.clone();

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

        let input = sresult_return!(<Arc<dyn ExecutionPlan>>::try_from(&input));

        let insert_op = InsertOp::from(insert_op);

        let plan = sresult_return!(
            internal_provider
                .insert_into(session, input, insert_op)
                .await
        );

        FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime.clone()))
    }
    .into_ffi()
}

unsafe extern "C" fn delete_from_fn_wrapper(
    provider: &FFI_TableProvider,
    session: FFI_SessionRef,
    filters_serialized: SVec<u8>,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let task_ctx: Result<Arc<TaskContext>, DataFusionError> =
        (&provider.logical_codec.task_ctx_provider).try_into();
    let runtime = provider.runtime().cloned();
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&provider.logical_codec).into();
    let internal_provider = Arc::clone(provider.inner());

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

        let task_ctx = sresult_return!(task_ctx);
        let filters = sresult_return!(parse_serialized_exprs(
            &filters_serialized,
            &task_ctx,
            logical_codec.as_ref(),
        ));

        let plan = sresult_return!(internal_provider.delete_from(session, filters).await);

        FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime))
    }
    .into_ffi()
}

unsafe extern "C" fn update_fn_wrapper(
    provider: &FFI_TableProvider,
    session: FFI_SessionRef,
    assignments: SVec<FFI_TableProviderUpdateAssignment>,
    filters_serialized: SVec<u8>,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let task_ctx: Result<Arc<TaskContext>, DataFusionError> =
        (&provider.logical_codec.task_ctx_provider).try_into();
    let runtime = provider.runtime().cloned();
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&provider.logical_codec).into();
    let internal_provider = Arc::clone(provider.inner());

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

        let task_ctx = sresult_return!(task_ctx);
        let assignments = sresult_return!(
            assignments
                .into_iter()
                .map(|assignment| {
                    let column = assignment.column.to_string();
                    let mut exprs = parse_serialized_exprs(
                        &assignment.expr_serialized,
                        &task_ctx,
                        logical_codec.as_ref(),
                    )?;
                    let expression_count = exprs.len();
                    let expr = match expression_count {
                        1 => exprs.remove(0),
                        _ => {
                            return Err(DataFusionError::Plan(format!(
                                "Expected exactly one expression for update assignment to column \
                                 '{column}', got {expression_count}"
                            )));
                        }
                    };
                    Ok((column, expr))
                })
                .collect::<Result<Vec<_>>>()
        );
        let filters = sresult_return!(parse_serialized_exprs(
            &filters_serialized,
            &task_ctx,
            logical_codec.as_ref(),
        ));

        let plan = sresult_return!(
            internal_provider
                .update(session, assignments, filters)
                .await
        );

        FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime))
    }
    .into_ffi()
}

unsafe extern "C" fn truncate_fn_wrapper(
    provider: &FFI_TableProvider,
    session: FFI_SessionRef,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    let runtime = provider.runtime().cloned();
    let internal_provider = Arc::clone(provider.inner());

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

        let plan = sresult_return!(internal_provider.truncate(session).await);

        FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime))
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_TableProvider) {
    unsafe {
        debug_assert!(!provider.private_data.is_null());
        let private_data =
            Box::from_raw(provider.private_data.cast::<ProviderPrivateData>());
        drop(private_data);
        provider.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_TableProvider) -> FFI_TableProvider {
    let runtime = provider.runtime().cloned();
    let old_provider = Arc::clone(provider.inner());

    let private_data = Box::into_raw(Box::new(ProviderPrivateData {
        provider: old_provider,
        runtime,
    }))
    .cast::<c_void>();

    FFI_TableProvider {
        schema: schema_fn_wrapper,
        scan: scan_fn_wrapper,
        table_type: table_type_fn_wrapper,
        supports_filters_pushdown: provider.supports_filters_pushdown,
        insert_into: provider.insert_into,
        statistics: statistics_fn_wrapper,
        logical_codec: provider.logical_codec.clone(),
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        library_marker_id: crate::get_library_marker_id,
        delete_from: provider.delete_from,
        update: provider.update,
        truncate: provider.truncate,
    }
}

impl Drop for FFI_TableProvider {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(
        provider: Arc<dyn TableProvider>,
        can_support_pushdown_filters: bool,
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
            task_ctx_provider.clone(),
        );
        Self::new_with_ffi_codec(
            provider,
            can_support_pushdown_filters,
            runtime,
            logical_codec,
        )
    }

    /// Creates an [`FFI_TableProvider`] using a prebuilt FFI logical codec.
    ///
    /// If `provider` is already foreign, this re-exports its original FFI
    /// handle rather than adding another wrapper layer. The handle still adopts
    /// the `logical_codec` supplied here, so it is never silently discarded and
    /// an imported provider can be rebound to a different session.
    ///
    /// `runtime` is only honored when a new wrapper is created. An
    /// already-foreign handle keeps the runtime of the library that owns it,
    /// because that value lives in private data this side cannot reach.
    pub fn new_with_ffi_codec(
        provider: Arc<dyn TableProvider>,
        can_support_pushdown_filters: bool,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        if let Some(provider) = provider.downcast_ref::<ForeignTableProvider>() {
            let mut provider = provider.0.clone();
            provider.logical_codec = logical_codec;
            return provider;
        }
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            schema: schema_fn_wrapper,
            scan: scan_fn_wrapper,
            table_type: table_type_fn_wrapper,
            supports_filters_pushdown: match can_support_pushdown_filters {
                true => Some(supports_filters_pushdown_fn_wrapper),
                false => None,
            },
            insert_into: insert_into_fn_wrapper,
            statistics: statistics_fn_wrapper,
            logical_codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data).cast::<c_void>(),
            library_marker_id: crate::get_library_marker_id,
            delete_from: delete_from_fn_wrapper,
            update: update_fn_wrapper,
            truncate: truncate_fn_wrapper,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_TableProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignTableProvider(pub FFI_TableProvider);

unsafe impl Send for ForeignTableProvider {}
unsafe impl Sync for ForeignTableProvider {}

impl From<&FFI_TableProvider> for Arc<dyn TableProvider> {
    fn from(provider: &FFI_TableProvider) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(provider.inner()) as Arc<dyn TableProvider>
        } else {
            Arc::new(ForeignTableProvider(provider.clone()))
        }
    }
}

impl Clone for FFI_TableProvider {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl TableProvider for ForeignTableProvider {
    fn schema(&self) -> SchemaRef {
        let wrapped_schema = unsafe { (self.0.schema)(&self.0) };
        wrapped_schema.into()
    }

    fn table_type(&self) -> TableType {
        unsafe { (self.0.table_type)(&self.0).into() }
    }

    fn statistics(&self) -> Option<Statistics> {
        let ffi_opt = unsafe { (self.0.statistics)(&self.0) };
        let bytes: Option<SVec<u8>> = ffi_opt.into();
        let bytes = bytes?;
        match deserialize_statistics(bytes.as_slice()) {
            Ok(stats) => Some(stats),
            Err(e) => {
                log::warn!("Failed to deserialize FFI statistics: {e}");
                // Fires in debug builds to surface encoding bugs early; callers see None.
                debug_assert!(false, "Failed to deserialize FFI statistics: {e}");
                None
            }
        }
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());

        let projections: FFI_Option<SVec<usize>> = projection
            .map(|p| p.iter().map(|v| v.to_owned()).collect())
            .into();

        let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();
        let filters_serialized = serialize_expr_list(filters.iter(), codec.as_ref())?;

        let plan = unsafe {
            let maybe_plan = (self.0.scan)(
                &self.0,
                session,
                projections,
                filters_serialized,
                limit.into(),
            )
            .await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        unsafe {
            let Some(pushdown_fn) = self.0.supports_filters_pushdown else {
                return Ok(vec![
                    TableProviderFilterPushDown::Unsupported;
                    filters.len()
                ]);
            };

            let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();

            let serialized_filters =
                serialize_expr_list(filters.iter().copied(), codec.as_ref())?;

            let pushdowns = df_result!(pushdown_fn(
                &self.0,
                serialized_filters.into_iter().collect()
            ))?;

            Ok(pushdowns.iter().map(|v| v.into()).collect())
        }
    }

    async fn insert_into(
        &self,
        session: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());

        let rc = Handle::try_current().ok();
        let input = FFI_ExecutionPlan::new(input, rc);
        let insert_op: FFI_InsertOp = insert_op.into();

        let plan = unsafe {
            let maybe_plan =
                (self.0.insert_into)(&self.0, session, &input, insert_op).await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }

    async fn delete_from(
        &self,
        session: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());
        let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();
        let filters_serialized = serialize_expr_list(filters.iter(), codec.as_ref())?;

        let plan = unsafe {
            let maybe_plan =
                (self.0.delete_from)(&self.0, session, filters_serialized).await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }

    async fn update(
        &self,
        session: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());
        let codec: Arc<dyn LogicalExtensionCodec> = (&self.0.logical_codec).into();

        let assignments: SVec<_> = assignments
            .iter()
            .map(|(column, expr)| {
                Ok(FFI_TableProviderUpdateAssignment {
                    column: SString::from(column.as_str()),
                    expr_serialized: serialize_expr_list(
                        std::iter::once(expr),
                        codec.as_ref(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect();
        let filters_serialized = serialize_expr_list(filters.iter(), codec.as_ref())?;

        let plan = unsafe {
            let maybe_plan =
                (self.0.update)(&self.0, session, assignments, filters_serialized).await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }

    async fn truncate(&self, session: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());

        let plan = unsafe {
            let maybe_plan = (self.0.truncate)(&self.0, session).await;

            <Arc<dyn ExecutionPlan>>::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::{SessionContext, col, lit};
    use datafusion_execution::TaskContextProvider;

    use super::*;

    fn create_test_table_provider() -> Result<Arc<dyn TableProvider>> {
        use arrow::datatypes::Field;
        use datafusion::arrow::array::Float32Array;
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![64.0]))],
        )?;

        Ok(Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch1], vec![batch2]],
        )?))
    }

    #[derive(Debug)]
    struct TableWithStats {
        inner: Arc<dyn TableProvider>,
        stats: Option<Statistics>,
        delete_calls: AtomicUsize,
        update_calls: AtomicUsize,
    }

    impl TableWithStats {
        fn new(inner: Arc<dyn TableProvider>, stats: Option<Statistics>) -> Self {
            Self {
                inner,
                stats,
                delete_calls: AtomicUsize::new(0),
                update_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl TableProvider for TableWithStats {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> TableType {
            self.inner.table_type()
        }

        fn statistics(&self) -> Option<Statistics> {
            self.stats.clone()
        }

        async fn scan(
            &self,
            session: &dyn Session,
            projection: Option<&[usize]>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.inner.scan(session, projection, filters, limit).await
        }

        async fn delete_from(
            &self,
            _state: &dyn Session,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let call = self.delete_calls.fetch_add(1, Ordering::Relaxed);
            let valid = match call {
                0 => filters == vec![col("a").gt(lit(10_i64)), col("b").lt(lit(2.5_f64))],
                1 => filters.is_empty(),
                _ => false,
            };

            if !valid {
                return Err(DataFusionError::Internal(format!(
                    "Unexpected DELETE filters for call {call}"
                )));
            }
            Ok(dml_count_plan())
        }

        async fn update(
            &self,
            _state: &dyn Session,
            assignments: Vec<(String, Expr)>,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if assignments
                != vec![
                    ("b".to_string(), lit(42_f64)),
                    ("a".to_string(), lit(7_i64)),
                ]
            {
                return Err(DataFusionError::Internal(
                    "Unexpected UPDATE assignments".to_string(),
                ));
            }
            let call = self.update_calls.fetch_add(1, Ordering::Relaxed);
            let valid = match call {
                0 => filters == vec![col("a").eq(lit(7_i64)), col("b").gt(lit(1.5_f64))],
                1 => filters.is_empty(),
                _ => false,
            };

            if !valid {
                return Err(DataFusionError::Internal(format!(
                    "Unexpected UPDATE filters for call {call}"
                )));
            }
            Ok(dml_count_plan())
        }

        async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(dml_count_plan())
        }
    }

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_scan() -> Result<()> {
        let provider = create_test_table_provider()?;
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let mut ffi_provider =
            FFI_TableProvider::new(provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_provider).into();

        ctx.register_table("t", foreign_table_provider)?;

        let df = ctx.table("t").await?;

        df.select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_insert_into() -> Result<()> {
        let provider = create_test_table_provider()?;
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let mut ffi_provider =
            FFI_TableProvider::new(provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_provider).into();

        ctx.register_table("t", foreign_table_provider)?;

        let result = ctx
            .sql("INSERT INTO t VALUES (128.0);")
            .await?
            .collect()
            .await?;

        assert!(result.len() == 1 && result[0].num_rows() == 1);

        ctx.table("t")
            .await?
            .select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }

    fn dml_count_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        Arc::new(crate::execution_plan::tests::EmptyExec::new(schema))
    }

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_dml() -> Result<()> {
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, true),
        ]));
        let inner = Arc::new(MemTable::try_new(schema, vec![vec![]])?);
        let provider = Arc::new(TableWithStats::new(inner, None));
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let mut ffi_provider =
            FFI_TableProvider::new(provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_provider).into();
        assert!(
            foreign_table_provider
                .downcast_ref::<ForeignTableProvider>()
                .is_some()
        );

        let state = ctx.state();
        let delete_filters = vec![col("a").gt(lit(10_i64)), col("b").lt(lit(2.5_f64))];

        let delete_plan = foreign_table_provider
            .delete_from(&state, delete_filters)
            .await?;
        assert_eq!(delete_plan.schema().field(0).name(), "count");

        let delete_all_plan = foreign_table_provider.delete_from(&state, vec![]).await?;
        assert_eq!(delete_all_plan.schema().field(0).name(), "count");

        let update_assignments = vec![
            ("b".to_string(), lit(42_f64)),
            ("a".to_string(), lit(7_i64)),
        ];
        let update_filters = vec![col("a").eq(lit(7_i64)), col("b").gt(lit(1.5_f64))];

        let update_plan = foreign_table_provider
            .update(&state, update_assignments.clone(), update_filters)
            .await?;
        assert_eq!(update_plan.schema().field(0).name(), "count");

        let update_all_plan = foreign_table_provider
            .update(&state, update_assignments, vec![])
            .await?;
        assert_eq!(update_all_plan.schema().field(0).name(), "count");

        let truncate_plan = foreign_table_provider.truncate(&state).await?;
        assert_eq!(truncate_plan.schema().field(0).name(), "count");

        let session =
            FFI_SessionRef::new(&state, None, ffi_provider.logical_codec.clone());
        let assignments = std::iter::once(FFI_TableProviderUpdateAssignment {
            column: SString::from("b"),
            expr_serialized: SVec::new(),
        })
        .collect();
        let result = unsafe {
            (ffi_provider.update)(&ffi_provider, session, assignments, SVec::new()).await
        };
        assert!(df_result!(result).unwrap_err().to_string().contains(
            "Expected exactly one expression for update assignment to column 'b', got 0",
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_aggregation() -> Result<()> {
        use datafusion::arrow::array::Float32Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::common::assert_batches_eq;
        use datafusion::datasource::MemTable;

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
        )?;

        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let provider = Arc::new(MemTable::try_new(schema, vec![vec![batch1]])?);

        let mut ffi_provider =
            FFI_TableProvider::new(provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_provider).into();

        ctx.register_table("t", foreign_table_provider)?;

        let result = ctx
            .sql("SELECT COUNT(*) as cnt FROM t")
            .await?
            .collect()
            .await?;
        #[rustfmt::skip]
        let expected = [
            "+-----+",
            "| cnt |",
            "+-----+",
            "| 3   |",
            "+-----+"
        ];
        assert_batches_eq!(expected, &result);
        Ok(())
    }

    #[test]
    fn test_ffi_table_provider_local_bypass() -> Result<()> {
        let table_provider = create_test_table_provider()?;

        let ctx = Arc::new(SessionContext::new()) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&ctx);
        let mut ffi_table =
            FFI_TableProvider::new(table_provider, false, None, task_ctx_provider, None);

        // Verify local libraries can be downcast to their original
        let foreign_table: Arc<dyn TableProvider> = (&ffi_table).into();
        assert!(
            foreign_table
                .downcast_ref::<datafusion::datasource::MemTable>()
                .is_some()
        );

        // Verify different library markers generate foreign providers
        ffi_table.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_table: Arc<dyn TableProvider> = (&ffi_table).into();
        assert!(
            foreign_table
                .downcast_ref::<ForeignTableProvider>()
                .is_some()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_none_projection_returns_all_columns() -> Result<()> {
        use arrow::datatypes::Field;
        use datafusion::arrow::array::Float32Array;
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::collect;

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Float32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![1.0, 2.0])),
                Arc::new(Float32Array::from(vec![3.0, 4.0])),
                Arc::new(Float32Array::from(vec![5.0, 6.0])),
            ],
        )?;

        let provider =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?);

        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        // Wrap in FFI and force the foreign path (not local bypass)
        let mut ffi_provider =
            FFI_TableProvider::new(provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_table_provider: Arc<dyn TableProvider> = (&ffi_provider).into();

        // Call scan with projection=None, meaning "return all columns"
        let plan = foreign_table_provider
            .scan(&ctx.state(), None, &[], None)
            .await?;
        assert_eq!(
            plan.schema().fields().len(),
            3,
            "scan(projection=None) should return all columns; got {}",
            plan.schema().fields().len()
        );

        // Also verify we can execute and get correct data
        let batches = collect(plan, ctx.task_ctx()).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 3);
        assert_eq!(batches[0].num_rows(), 2);

        Ok(())
    }

    #[test]
    fn test_ffi_table_provider_statistics_round_trip() -> Result<()> {
        use arrow::datatypes::{DataType, Field};
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion_common::stats::Precision;
        use datafusion_common::{ColumnStatistics, ScalarValue};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        // Provider without statistics should cross the boundary as None.
        let no_stats_inner = Arc::new(MemTable::try_new(
            Arc::clone(&schema),
            vec![vec![batch.clone()]],
        )?);
        let no_stats_provider = Arc::new(TableWithStats::new(no_stats_inner, None));
        let mut ffi_provider = FFI_TableProvider::new(
            no_stats_provider,
            true,
            None,
            task_ctx_provider.clone(),
            None,
        );
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;
        let foreign: Arc<dyn TableProvider> = (&ffi_provider).into();
        assert!(foreign.statistics().is_none());

        // Provider with statistics should round-trip faithfully.
        let original_stats = Statistics {
            num_rows: Precision::Exact(3),
            total_byte_size: Precision::Inexact(12),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(1))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(6))),
                distinct_count: Precision::Exact(3),
                byte_size: Precision::Exact(12),
            }],
        };
        let stats_inner =
            Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])?);
        let stats_provider = Arc::new(TableWithStats::new(
            stats_inner,
            Some(original_stats.clone()),
        ));
        let mut ffi_provider =
            FFI_TableProvider::new(stats_provider, true, None, task_ctx_provider, None);
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;
        let foreign: Arc<dyn TableProvider> = (&ffi_provider).into();
        assert_eq!(foreign.statistics().as_ref(), Some(&original_stats));

        Ok(())
    }

    /// Re-wrapping an imported provider with a rebuilt logical codec must adopt
    /// that codec. See <https://github.com/apache/datafusion/issues/24722>.
    #[test]
    fn test_rebind_foreign_table_provider_adopts_logical_codec() -> Result<()> {
        let (_ctx_a, provider_a) = crate::util::tests::test_session_and_ctx();
        let (ctx_b, provider_b) = crate::util::tests::test_session_and_ctx();

        let mut ffi_provider = FFI_TableProvider::new(
            create_test_table_provider()?,
            true,
            None,
            provider_a,
            None,
        );
        ffi_provider.library_marker_id = crate::mock_foreign_marker_id;

        let imported: Arc<dyn TableProvider> = (&ffi_provider).into();
        assert!(imported.downcast_ref::<ForeignTableProvider>().is_some());

        // Rebuild the codec against session B and re-wrap.
        let codec_b = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            provider_b,
        );
        let rebound =
            FFI_TableProvider::new_with_ffi_codec(imported, true, None, codec_b);

        let task_ctx: Arc<TaskContext> = (&rebound.logical_codec.task_ctx_provider)
            .try_into()
            .expect("rebound provider's codec resolves");
        assert_eq!(task_ctx.session_id(), ctx_b.task_ctx().session_id());

        Ok(())
    }
}
