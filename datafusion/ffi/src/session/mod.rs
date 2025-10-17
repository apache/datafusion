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

use crate::udaf::{FFI_AggregateUDF, ForeignAggregateUDF};
use crate::udf::{FFI_ScalarUDF, ForeignScalarUDF};
use crate::udwf::{FFI_WindowUDF, ForeignWindowUDF};
use crate::{df_result, rresult_return};
use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use datafusion_common::{not_impl_err, DFSchema};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::{ffi::c_void, sync::Arc};
use std::any::Any;
use abi_stable::std_types::{RHashMap, RStr};
use async_ffi::FfiFuture;
use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_common::config::{ConfigOptions, TableOptions};
use datafusion_expr::execution_props::ExecutionProps;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::plan_properties::{FFI_EmissionType, FFI_PlanProperties};
use crate::session::config::FFI_SessionConfig;
use crate::session::task::FFI_TaskContext;
use crate::tests::create_test_schema;

pub mod config;
mod task;

/// A stable struct for sharing [`Session`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_Session {

    pub session_id: unsafe extern "C" fn(&Self) -> RStr,

    pub config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    pub config_options: unsafe extern "C" fn(&Self) -> RHashMap<RString, RString>,

    pub create_physical_plan: unsafe extern "C" fn(
        &Self,
        logical_plan_serialized: &RVec<u8>,
    ) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>>,

    pub create_physical_expr: unsafe extern "C" fn(&Self, expr: Expr, df_schema: &DFSchema) -> RResult<RVec<u8>, RString>,

    pub scalar_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    pub aggregate_functions: unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

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
    registry: Arc<dyn Session + Send>,
}

impl FFI_Session {
    unsafe fn inner(&self) -> &Arc<dyn Session + Send> {
        let private_data = self.private_data as *const SessionPrivateData;
        &(*private_data).registry
    }
}

unsafe extern "C" fn session_id_fn_wrapper(
    session: &FFI_Session,
) -> RStr {
    let session = session.inner();
    session.session_id().into()
}

unsafe extern "C" fn config_fn_wrapper(
    session: &FFI_Session,
) -> FFI_SessionConfig {
    let session = session.inner();
    session.config().into()
}


unsafe extern "C" fn config_options_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    session.config_options().into()
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    session: &FFI_Session,
    logical_plan_serialized: &RVec<u8>,
) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>> {
    let session = session.inner();
    session.create_physical_plan().into()
}

unsafe extern "C" fn create_physical_expr_fn_wrapper(
    session: &FFI_Session,
    expr: Expr, df_schema: &DFSchema,
) -> RResult<RVec<u8>, RString> {
    let session = session.inner();
    session.create_physical_expr().into()
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, FFI_ScalarUDF> {
    let session = session.inner();
    session.scalar_functions().into()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, FFI_AggregateUDF> {
    let session = session.inner();
    session.aggregate_functions().into()
}

unsafe extern "C" fn window_functions_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, FFI_WindowUDF> {
    let session = session.inner();
    session.window_functions().into()
}

unsafe extern "C" fn table_options_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    session.table_options().into()
}

unsafe extern "C" fn default_table_options_fn_wrapper(
    session: &FFI_Session,
) -> RHashMap<RString, RString> {
    let session = session.inner();
    session.default_table_options().into()
}


unsafe extern "C" fn task_ctx_fn_wrapper(
    session: &FFI_Session,
) -> FFI_TaskContext {
    let session = session.inner();
    session.task_ctx().into()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_Session) {
    let private_data = Box::from_raw(provider.private_data as *mut SessionPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: &FFI_Session,
) -> FFI_Session {
    let old_private_data = provider.private_data as *const SessionPrivateData;

    let private_data = Box::into_raw(Box::new(SessionPrivateData {
        registry: Arc::clone(&(*old_private_data).registry),
    })) as *mut c_void;

    FFI_Session {
        session_id: session_id_fn_wrapper,
        config: config_fn_wrapper,
        config_options: config_options_fn_wrapper,
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
    pub fn new(session: Arc<dyn Session + Send>) -> Self {
        let private_data = Box::new(SessionPrivateData { registry });

        Self {
            session_id: session_id_fn_wrapper,
            config: config_fn_wrapper,
            config_options: config_options_fn_wrapper,
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
pub struct ForeignSession(FFI_Session);

unsafe impl Send for ForeignSession {}
unsafe impl Sync for ForeignSession {}

impl From<&FFI_Session> for ForeignSession {
    fn from(provider: &FFI_Session) -> Self {
        Self(provider.clone())
    }
}

impl Clone for FFI_Session {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Session for ForeignSession {
    fn session_id(&self) -> &str {
        todo!()
    }

    fn config(&self) -> &SessionConfig {
        todo!()
    }

    fn config_options(&self) -> &ConfigOptions {
        todo!()
    }

    async fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn create_physical_expr(&self, expr: Expr, df_schema: &DFSchema) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        todo!()
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        todo!()
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        todo!()
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        todo!()
    }

    fn execution_props(&self) -> &ExecutionProps {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn table_options(&self) -> &TableOptions {
        todo!()
    }

    fn default_table_options(&self) -> TableOptions {
        todo!()
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        todo!()
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        todo!()
    }
}
