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

use crate::arrow_wrappers::WrappedSchema;
use crate::execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan};
use crate::session::config::{FFI_SessionConfig, ForeignSessionConfig};
use crate::session::task::FFI_TaskContext;
use crate::udaf::{FFI_AggregateUDF, ForeignAggregateUDF};
use crate::udf::{FFI_ScalarUDF, ForeignScalarUDF};
use crate::udwf::{FFI_WindowUDF, ForeignWindowUDF};
use crate::{df_result, rresult, rresult_return};
use abi_stable::std_types::{RHashMap, RStr};
use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_proto::bytes::{logical_plan_from_bytes, logical_plan_to_bytes};
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::{LogicalExprNode, PhysicalExprNode};
use prost::Message;
use std::any::Any;
use std::collections::HashMap;
use std::{ffi::c_void, sync::Arc};
use tokio::runtime::Handle;

pub mod config;
mod task;

/// A stable struct for sharing [`Session`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Session {
    pub session_id: unsafe extern "C" fn(&Self) -> RStr,

    pub config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    pub create_physical_plan:
        unsafe extern "C" fn(
            &Self,
            logical_plan_serialized: RVec<u8>,
        ) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>>,

    pub create_physical_expr: unsafe extern "C" fn(
        &Self,
        expr_serialized: RVec<u8>,
        schema: WrappedSchema,
    ) -> RResult<RVec<u8>, RString>,

    pub scalar_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    pub aggregate_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

    pub window_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_WindowUDF>,

    // TODO: Expand scope of FFI to include runtime environment
    // pub runtime_env: unsafe extern "C" fn(&Self) -> FFI_RuntimeEnv,

    // pub execution_props: unsafe extern "C" fn(&Self) -> FFI_ExecutionProps,
    pub table_options: unsafe extern "C" fn(&Self) -> RHashMap<RString, RString>,

    pub default_table_options: unsafe extern "C" fn(&Self) -> RHashMap<RString, RString>,

    pub task_ctx: unsafe extern "C" fn(&Self) -> FFI_TaskContext,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this registry.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignSession`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_Session {}
unsafe impl Sync for FFI_Session {}

struct SessionPrivateData {
    session: Arc<dyn Session + Send>,
    function_registry: Arc<dyn FunctionRegistry>,
    runtime: Handle,
}

impl FFI_Session {
    unsafe fn inner(&self) -> &Arc<dyn Session + Send> {
        let private_data = self.private_data as *const SessionPrivateData;
        &(*private_data).session
    }

    unsafe fn function_registry(&self) -> &Arc<dyn FunctionRegistry> {
        let private_data = self.private_data as *const SessionPrivateData;
        &(*private_data).function_registry
    }

    unsafe fn runtime(&self) -> &Handle {
        let private_data = self.private_data as *const SessionPrivateData;
        &(*private_data).runtime
    }
}

unsafe extern "C" fn session_id_fn_wrapper(session: &FFI_Session) -> RStr<'_> {
    let session = session.inner();
    session.session_id().into()
}

unsafe extern "C" fn config_fn_wrapper(session: &FFI_Session) -> FFI_SessionConfig {
    let session = session.inner();
    session.config().into()
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    session: &FFI_Session,
    logical_plan_serialized: RVec<u8>,
) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>> {
    let runtime = session.runtime().clone();
    let session = Arc::clone(session.inner());
    async move {
        let task_ctx = session.task_ctx();

        let logical_plan = rresult_return!(logical_plan_from_bytes(
            logical_plan_serialized.as_slice(),
            &task_ctx
        ));

        let physical_plan = session.create_physical_plan(&logical_plan).await;

        rresult!(physical_plan.map(|plan| FFI_ExecutionPlan::new(
            plan,
            task_ctx,
            Some(runtime)
        )))
    }
    .into_ffi()
}

unsafe extern "C" fn create_physical_expr_fn_wrapper(
    session: &FFI_Session,
    expr_serialized: RVec<u8>,
    schema: WrappedSchema,
) -> RResult<RVec<u8>, RString> {
    let function_registry = session.function_registry();
    let session = session.inner();

    let codec = DefaultLogicalExtensionCodec {};
    let logical_expr = LogicalExprNode::decode(expr_serialized.as_slice()).unwrap();
    let logical_expr =
        parse_expr(&logical_expr, function_registry.as_ref(), &codec).unwrap();
    let schema: SchemaRef = schema.into();
    let schema: DFSchema = rresult_return!((schema).try_into());

    let physical_expr =
        rresult_return!(session.create_physical_expr(logical_expr, &schema));
    let codec = DefaultPhysicalExtensionCodec {};
    let physical_expr =
        rresult_return!(serialize_physical_expr(&physical_expr, &codec)).encode_to_vec();

    RResult::ROk(physical_expr.into())
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, FFI_ScalarUDF> {
    let session = session.inner();
    session
        .scalar_functions()
        .iter()
        .map(|(name, udf)| (name.clone().into(), FFI_ScalarUDF::from(Arc::clone(udf))))
        .collect()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    session: &FFI_Session,
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
    session: &FFI_Session,
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
    session: &FFI_Session,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    let table_options = session.table_options();
    table_options_to_rhash(table_options)
}

unsafe extern "C" fn default_table_options_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    let table_options = session.default_table_options();

    table_options_to_rhash(&table_options)
}

unsafe extern "C" fn task_ctx_fn_wrapper(session: &FFI_Session) -> FFI_TaskContext {
    let session = session.inner();
    FFI_TaskContext::from(session.task_ctx())
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_Session) {
    let private_data = Box::from_raw(provider.private_data as *mut SessionPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_Session) -> FFI_Session {
    let old_private_data = provider.private_data as *const SessionPrivateData;

    let private_data = Box::into_raw(Box::new(SessionPrivateData {
        session: Arc::clone(&(*old_private_data).session),
        function_registry: Arc::clone(&(*old_private_data).function_registry),
        runtime: (*old_private_data).runtime.clone(),
    })) as *mut c_void;

    FFI_Session {
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

        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
    }
}

impl Drop for FFI_Session {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_Session {
    /// Creates a new [`FFI_Session`].
    pub fn new(
        session: Arc<dyn Session + Send>,
        function_registry: Arc<dyn FunctionRegistry>,
        runtime: Handle,
    ) -> Self {
        let private_data = Box::new(SessionPrivateData {
            session,
            function_registry,
            runtime,
        });

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

            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_Session to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignSession {
    session: FFI_Session,
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

impl TryFrom<&FFI_Session> for ForeignSession {
    type Error = DataFusionError;
    fn try_from(session: &FFI_Session) -> Result<Self, Self::Error> {
        unsafe {
            let table_options =
                table_options_from_rhashmap((session.table_options)(session));

            let config = (session.config)(session);
            let config = ForeignSessionConfig::try_from(&config)?.0;

            let scalar_functions = (session.scalar_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udf = ForeignScalarUDF::try_from(&kv_pair.1)?;

                    Ok((
                        kv_pair.0.into_string(),
                        Arc::new(ScalarUDF::new_from_impl(udf)),
                    ))
                })
                .collect::<Result<_, DataFusionError>>()?;
            let aggregate_functions = (session.aggregate_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udaf = ForeignAggregateUDF::try_from(&kv_pair.1)?;

                    Ok((
                        kv_pair.0.into_string(),
                        Arc::new(AggregateUDF::new_from_impl(udaf)),
                    ))
                })
                .collect::<Result<_, DataFusionError>>()?;
            let window_functions = (session.window_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udwf = ForeignWindowUDF::try_from(&kv_pair.1)?;

                    Ok((
                        kv_pair.0.into_string(),
                        Arc::new(WindowUDF::new_from_impl(udwf)),
                    ))
                })
                .collect::<Result<_, DataFusionError>>()?;

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

impl Clone for FFI_Session {
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
            let physical_plan = ForeignExecutionPlan::try_from(&physical_plan)?;

            Ok(Arc::new(physical_plan))
        }
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        unsafe {
            let codec = DefaultLogicalExtensionCodec {};
            let logical_expr = serialize_expr(&expr, &codec)?.encode_to_vec();
            let schema = WrappedSchema(FFI_ArrowSchema::try_from(df_schema.as_arrow())?);

            let physical_expr = df_result!((self.session.create_physical_expr)(
                &self.session,
                logical_expr.into(),
                schema
            ))?;

            let physical_expr = PhysicalExprNode::decode(physical_expr.as_slice())
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            let codec = DefaultPhysicalExtensionCodec {};
            let physical_expr = parse_physical_expr(
                &physical_expr,
                self.task_ctx().as_ref(),
                df_schema.as_arrow(),
                &codec,
            )?;

            Ok(physical_expr)
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
        unsafe { Arc::new((self.session.task_ctx)(&self.session).into()) }
    }
}
