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

use std::any::Any;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use abi_stable::std_types::{RHashMap, RResult, RStr, RString, RVec};
use abi_stable::StableAbi;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, LogicalPlan, ScalarUDF, ScalarUDFImpl,
    WindowUDF, WindowUDFImpl,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprNode;
use datafusion_session::Session;
use prost::Message;
use tokio::runtime::Handle;

use crate::arrow_wrappers::WrappedSchema;
use crate::execution::FFI_TaskContext;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::session::config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFIResult;
use crate::{df_result, rresult, rresult_return};

pub mod config;

/// A stable struct for sharing [`Session`] across FFI boundaries.
///
/// Care must be taken when using this struct. Unlike most of the structs in
/// this crate, the private data for [`FFI_SessionRef`] contains borrowed data.
/// The lifetime of the borrow is lost when hidden within the ``*mut c_void``
/// of the private data. For this reason, it is the user's responsibility to
/// ensure the lifetime of the [`Session`] remains valid.
///
/// The reason for storing `&dyn Session` is because the primary motivation
/// for implementing this struct is [`crate::table_provider::FFI_TableProvider`]
/// which has methods that require `&dyn Session`. For usage within this crate
/// we know the [`Session`] lifetimes are valid.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_SessionRef {
    session_id: unsafe extern "C" fn(&Self) -> RStr,

    config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    create_physical_plan: unsafe extern "C" fn(
        &Self,
        logical_plan_serialized: RVec<u8>,
    )
        -> FfiFuture<FFIResult<FFI_ExecutionPlan>>,

    create_physical_expr: unsafe extern "C" fn(
        &Self,
        expr_serialized: RVec<u8>,
        schema: WrappedSchema,
    ) -> FFIResult<FFI_PhysicalExpr>,

    scalar_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    aggregate_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

    window_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_WindowUDF>,

    table_options: unsafe extern "C" fn(&Self) -> RHashMap<RString, RString>,

    default_table_options: unsafe extern "C" fn(&Self) -> RHashMap<RString, RString>,

    task_ctx: unsafe extern "C" fn(&Self) -> FFI_TaskContext,

    logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the plan.
    clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this registry.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignSession`] should never attempt to access this data.
    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_SessionRef {}
unsafe impl Sync for FFI_SessionRef {}

struct SessionPrivateData<'a> {
    session: &'a (dyn Session + Send + Sync),
    runtime: Option<Handle>,
}

impl FFI_SessionRef {
    fn inner(&self) -> &(dyn Session + Send + Sync) {
        let private_data = self.private_data as *const SessionPrivateData;
        unsafe { (*private_data).session }
    }

    unsafe fn runtime(&self) -> &Option<Handle> {
        let private_data = self.private_data as *const SessionPrivateData;
        &(*private_data).runtime
    }
}

unsafe extern "C" fn session_id_fn_wrapper(session: &FFI_SessionRef) -> RStr<'_> {
    let session = session.inner();
    session.session_id().into()
}

unsafe extern "C" fn config_fn_wrapper(session: &FFI_SessionRef) -> FFI_SessionConfig {
    let session = session.inner();
    session.config().into()
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    session: &FFI_SessionRef,
    logical_plan_serialized: RVec<u8>,
) -> FfiFuture<FFIResult<FFI_ExecutionPlan>> {
    let runtime = session.runtime().clone();
    let session = session.clone();
    async move {
        let session = session.inner();
        let task_ctx = session.task_ctx();

        let logical_plan = rresult_return!(logical_plan_from_bytes(
            logical_plan_serialized.as_slice(),
            task_ctx.as_ref(),
        ));

        let physical_plan = session.create_physical_plan(&logical_plan).await;

        rresult!(physical_plan.map(|plan| FFI_ExecutionPlan::new(plan, task_ctx, runtime)))
    }
    .into_ffi()
}

unsafe extern "C" fn create_physical_expr_fn_wrapper(
    session: &FFI_SessionRef,
    expr_serialized: RVec<u8>,
    schema: WrappedSchema,
) -> FFIResult<FFI_PhysicalExpr> {
    let codec: Arc<dyn LogicalExtensionCodec> = (&session.logical_codec).into();
    let session = session.inner();

    let logical_expr = LogicalExprNode::decode(expr_serialized.as_slice()).unwrap();
    let logical_expr =
        parse_expr(&logical_expr, session.task_ctx().as_ref(), codec.as_ref()).unwrap();
    let schema: SchemaRef = schema.into();
    let schema: DFSchema = rresult_return!(schema.try_into());

    let physical_expr =
        rresult_return!(session.create_physical_expr(logical_expr, &schema));

    RResult::ROk(physical_expr.into())
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> RHashMap<RString, FFI_ScalarUDF> {
    let session = session.inner();
    session
        .scalar_functions()
        .iter()
        .map(|(name, udf)| (name.clone().into(), FFI_ScalarUDF::from(Arc::clone(udf))))
        .collect()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> RHashMap<RString, FFI_AggregateUDF> {
    let session = session.inner();
    session
        .aggregate_functions()
        .iter()
        .map(|(name, udaf)| {
            (
                name.clone().into(),
                FFI_AggregateUDF::from(Arc::clone(udaf)),
            )
        })
        .collect()
}

unsafe extern "C" fn window_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> RHashMap<RString, FFI_WindowUDF> {
    let session = session.inner();
    session
        .window_functions()
        .iter()
        .map(|(name, udwf)| (name.clone().into(), FFI_WindowUDF::from(Arc::clone(udwf))))
        .collect()
}

fn table_options_to_rhash(options: &TableOptions) -> RHashMap<RString, RString> {
    options
        .entries()
        .into_iter()
        .filter_map(|entry| entry.value.map(|v| (entry.key.into(), v.into())))
        .collect()
}

unsafe extern "C" fn table_options_fn_wrapper(
    session: &FFI_SessionRef,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    let table_options = session.table_options();
    table_options_to_rhash(table_options)
}

unsafe extern "C" fn default_table_options_fn_wrapper(
    session: &FFI_SessionRef,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    let table_options = session.default_table_options();

    table_options_to_rhash(&table_options)
}

unsafe extern "C" fn task_ctx_fn_wrapper(session: &FFI_SessionRef) -> FFI_TaskContext {
    session.inner().task_ctx().into()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_SessionRef) {
    let private_data = Box::from_raw(provider.private_data as *mut SessionPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_SessionRef) -> FFI_SessionRef {
    let old_private_data = provider.private_data as *const SessionPrivateData;

    let private_data = Box::into_raw(Box::new(SessionPrivateData {
        session: (*old_private_data).session,
        runtime: (*old_private_data).runtime.clone(),
    })) as *mut c_void;

    FFI_SessionRef {
        session_id: session_id_fn_wrapper,
        config: config_fn_wrapper,
        create_physical_plan: create_physical_plan_fn_wrapper,
        create_physical_expr: create_physical_expr_fn_wrapper,
        scalar_functions: scalar_functions_fn_wrapper,
        aggregate_functions: aggregate_functions_fn_wrapper,
        window_functions: window_functions_fn_wrapper,
        table_options: table_options_fn_wrapper,
        default_table_options: default_table_options_fn_wrapper,
        task_ctx: task_ctx_fn_wrapper,
        logical_codec: provider.logical_codec.clone(),

        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        library_marker_id: crate::get_library_marker_id,
    }
}

impl Drop for FFI_SessionRef {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_SessionRef {
    /// Creates a new [`FFI_SessionRef`].
    pub fn new(
        session: &(dyn Session + Send + Sync),
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let private_data = Box::new(SessionPrivateData { session, runtime });

        Self {
            session_id: session_id_fn_wrapper,
            config: config_fn_wrapper,
            create_physical_plan: create_physical_plan_fn_wrapper,
            create_physical_expr: create_physical_expr_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            table_options: table_options_fn_wrapper,
            default_table_options: default_table_options_fn_wrapper,
            task_ctx: task_ctx_fn_wrapper,
            logical_codec,

            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_Session to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignSession {
    session: FFI_SessionRef,
    config: SessionConfig,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    table_options: TableOptions,
    runtime_env: Arc<RuntimeEnv>,
    props: ExecutionProps,
}

unsafe impl Send for ForeignSession {}
unsafe impl Sync for ForeignSession {}

impl FFI_SessionRef {
    pub fn as_local(&self) -> Option<&(dyn Session + Send + Sync)> {
        if (self.library_marker_id)() == crate::get_library_marker_id() {
            return Some(self.inner());
        }
        None
    }
}

impl TryFrom<&FFI_SessionRef> for ForeignSession {
    type Error = DataFusionError;
    fn try_from(session: &FFI_SessionRef) -> Result<Self, Self::Error> {
        unsafe {
            let table_options =
                table_options_from_rhashmap((session.table_options)(session));

            let config = (session.config)(session);
            let config = SessionConfig::try_from(&config)?;

            let scalar_functions = (session.scalar_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udf = <Arc<dyn ScalarUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(ScalarUDF::new_from_shared_impl(udf)),
                    )
                })
                .collect();
            let aggregate_functions = (session.aggregate_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udaf = <Arc<dyn AggregateUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(AggregateUDF::new_from_shared_impl(udaf)),
                    )
                })
                .collect();
            let window_functions = (session.window_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udwf = <Arc<dyn WindowUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.into_string(),
                        Arc::new(WindowUDF::new_from_shared_impl(udwf)),
                    )
                })
                .collect();

            Ok(Self {
                session: session.clone(),
                config,
                table_options,
                scalar_functions,
                aggregate_functions,
                window_functions,
                runtime_env: Default::default(),
                props: Default::default(),
            })
        }
    }
}

impl Clone for FFI_SessionRef {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

fn table_options_from_rhashmap(options: RHashMap<RString, RString>) -> TableOptions {
    let options = options
        .into_iter()
        .map(|kv_pair| (kv_pair.0.into_string(), kv_pair.1.into_string()))
        .collect();

    TableOptions::from_string_hash_map(&options).unwrap_or_else(|err| {
        log::warn!("Error parsing default table options: {err}");
        TableOptions::default()
    })
}

#[async_trait]
impl Session for ForeignSession {
    fn session_id(&self) -> &str {
        unsafe { (self.session.session_id)(&self.session).as_str() }
    }

    fn config(&self) -> &SessionConfig {
        &self.config
    }

    fn config_options(&self) -> &ConfigOptions {
        self.config.options()
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        unsafe {
            let logical_plan = logical_plan_to_bytes(logical_plan)?;
            let physical_plan = df_result!(
                (self.session.create_physical_plan)(
                    &self.session,
                    logical_plan.as_ref().into()
                )
                .await
            )?;
            let physical_plan = <Arc<dyn ExecutionPlan>>::try_from(&physical_plan)?;

            Ok(physical_plan)
        }
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        unsafe {
            let codec: Arc<dyn LogicalExtensionCodec> =
                (&self.session.logical_codec).into();
            let logical_expr = serialize_expr(&expr, codec.as_ref())?.encode_to_vec();
            let schema = WrappedSchema(FFI_ArrowSchema::try_from(df_schema.as_arrow())?);

            let physical_expr = df_result!((self.session.create_physical_expr)(
                &self.session,
                logical_expr.into(),
                schema
            ))?;

            Ok((&physical_expr).into())
        }
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.props
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    fn default_table_options(&self) -> TableOptions {
        unsafe {
            table_options_from_rhashmap((self.session.default_table_options)(
                &self.session,
            ))
        }
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        log::warn!("Mutating table options is not supported via FFI. Changes will not have an effect.");
        &mut self.table_options
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        unsafe { (self.session.task_ctx)(&self.session).into() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_expr::col;
    use datafusion_expr::registry::FunctionRegistry;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

    #[tokio::test]
    async fn test_ffi_session() -> Result<(), DataFusionError> {
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let state = ctx.state();
        let logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );

        let local_session = FFI_SessionRef::new(&state, None, logical_codec);
        let foreign_session = ForeignSession::try_from(&local_session)?;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let df_schema = schema.try_into()?;
        let physical_expr = foreign_session.create_physical_expr(col("a"), &df_schema)?;
        assert_eq!(
            format!("{physical_expr:?}"),
            "Column { name: \"a\", index: 0 }"
        );

        assert_eq!(foreign_session.session_id(), state.session_id());

        let logical_plan = LogicalPlan::default();
        let physical_plan = foreign_session.create_physical_plan(&logical_plan).await?;
        assert_eq!(format!("{physical_plan:?}"), "EmptyExec { schema: Schema { fields: [], metadata: {} }, partitions: 1, cache: PlanProperties { eq_properties: EquivalenceProperties { eq_group: EquivalenceGroup { map: {}, classes: [] }, oeq_class: OrderingEquivalenceClass { orderings: [] }, oeq_cache: OrderingEquivalenceCache { normal_cls: OrderingEquivalenceClass { orderings: [] }, leading_map: {} }, constraints: Constraints { inner: [] }, schema: Schema { fields: [], metadata: {} } }, partitioning: UnknownPartitioning(1), emission_type: Incremental, boundedness: Bounded, evaluation_type: Lazy, scheduling_type: Cooperative, output_ordering: None } }");

        assert_eq!(
            format!("{:?}", foreign_session.default_table_options()),
            format!("{:?}", state.default_table_options())
        );

        assert_eq!(
            format!("{:?}", foreign_session.table_options()),
            format!("{:?}", state.table_options())
        );

        let local_udfs = state.udfs();
        for udf in foreign_session.scalar_functions().keys() {
            assert!(local_udfs.contains(udf));
        }
        let local_udafs = state.udafs();
        for udaf in foreign_session.aggregate_functions().keys() {
            assert!(local_udafs.contains(udaf));
        }
        let local_udwfs = state.udwfs();
        for udwf in foreign_session.window_functions().keys() {
            assert!(local_udwfs.contains(udwf));
        }

        Ok(())
    }
}
