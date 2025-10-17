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

use crate::session::config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::{arrow_wrappers::WrappedSchema, df_result, rresult_return};
use abi_stable::pmr::ROption;
use abi_stable::std_types::{RHashMap, RStr};
use abi_stable::{
    std_types::{
        RResult::{self, ROk},
        RString, RVec,
    },
    StableAbi,
};
use arrow::datatypes::SchemaRef;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::EquivalenceProperties,
    physical_plan::execution_plan::{Boundedness, EmissionType},
    prelude::SessionContext,
};
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_proto::{
    physical_plan::{
        from_proto::{parse_physical_sort_exprs, parse_protobuf_partitioning},
        to_proto::{serialize_partitioning, serialize_physical_sort_exprs},
        DefaultPhysicalExtensionCodec,
    },
    protobuf::{Partitioning, PhysicalSortExprNodeCollection},
};
use prost::Message;
use std::collections::HashMap;
use std::{ffi::c_void, sync::Arc};
use datafusion::catalog::SchemaProvider;

/// A stable struct for sharing [`TaskContext`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TaskContext {
    pub session_id: unsafe extern "C" fn(&Self) -> RStr,

    pub task_id: unsafe extern "C" fn(&Self) -> ROption<RStr>,

    pub session_config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    pub scalar_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_ScalarUDF>,

    pub aggregate_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_AggregateUDF>,

    pub window_functions:
        unsafe extern "C" fn(&Self) -> RHashMap<RString, FFI_WindowUDF>,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

struct TaskContextPrivateData {
    ctx: TaskContext,
}

impl FFI_TaskContext {
    unsafe fn inner(&self) -> &TaskContext {
        let private_data = self.private_data as *const TaskContextPrivateData;
        &(*private_data).ctx
    }
}

unsafe extern "C" fn session_id_fn_wrapper(ctx: &FFI_TaskContext) -> RStr {
    let ctx = ctx.inner();
    ctx.session_id().as_str().into()
}

unsafe extern "C" fn task_id_fn_wrapper(ctx: &FFI_TaskContext) -> ROption<RStr> {
    let ctx = ctx.inner();
    ctx.session_id().map(|s| s.as_str().into()).into()
}

unsafe extern "C" fn session_config_fn_wrapper(ctx: &FFI_TaskContext) -> FFI_SessionConfig {
    let ctx = ctx.inner();
    ctx.session_config().into()
}

unsafe extern "C" fn scalar_functions_fn_wrapper(ctx: &FFI_TaskContext) -> RHashMap<RString, FFI_ScalarUDF> {
    let ctx = ctx.inner();
    ctx.scalar_functions()
        .into_iter()
        .map(|(name, udf)| (name.into(), udf.into()))
        .collect()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(ctx: &FFI_TaskContext) -> RHashMap<RString, FFI_AggregateUDF> {
    let ctx = ctx.inner();
    ctx.aggregate_functions()
        .into_iter()
        .map(|(name, udf)| (name.into(), udf.into()))
        .collect()
}

unsafe extern "C" fn window_functions_fn_wrapper(ctx: &FFI_TaskContext) -> RHashMap<RString, FFI_WindowUDF> {
    let ctx = ctx.inner();
    ctx.window_functions()
        .into_iter()
        .map(|(name, udf)| (name.into(), udf.into()))
        .collect()
}

unsafe extern "C" fn release_fn_wrapper(ctx: &mut FFI_TaskContext) {
    let private_data = Box::from_raw(ctx.private_data as *mut TaskContextPrivateData);
    drop(private_data);
}

impl Drop for FFI_TaskContext {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl From<&TaskContext> for FFI_TaskContext {
    fn from(ctx: TaskContext) -> Self {
        let private_data = Box::new(TaskContextPrivateData { ctx });

        FFI_TaskContext {
            session_id: session_id_fn_wrapper,
            task_id: task_id_fn_wrapper,
            session_config: session_config_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl TryFrom<FFI_TaskContext> for TaskContext {
    type Error = DataFusionError;

    fn try_from(ffi_ctx: FFI_TaskContext) -> Result<Self, Self::Error> {
        unsafe {
            let task_id = (ffi_ctx.task_id)(&ffi_ctx).map(|s| s.to_string()).into();
            let sesion_id = (ffi_ctx.session_id)(&ffi_ctx).into();
            let session_config = (ffi_ctx.session_config)(&ffi_ctx).into();

            let scalar_functions = (ffi_ctx.scalar_functions)(&ffi_ctx).into();
            let aggregate_functions = (ffi_ctx.aggregate_functions)(&ffi_ctx).into();
            let window_functions = (ffi_ctx.window_functions)(&ffi_ctx).into();

            let runtime = Arc::new(RuntimeEnv::default());

            Ok(TaskContext::new(
                task_id,
                sesion_id,
                session_config,
                scalar_functions,
                aggregate_functions,
                window_functions,
                runtime,
            ))
        }
    }
}
//
// #[cfg(test)]
// mod tests {
//     use datafusion::{physical_expr::PhysicalSortExpr, physical_plan::Partitioning};
//
//     use super::*;
//
//     #[test]
//     fn test_round_trip_ffi_plan_properties() -> Result<()> {
//         use arrow::datatypes::{DataType, Field, Schema};
//         let schema =
//             Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
//
//         let mut eqp = EquivalenceProperties::new(Arc::clone(&schema));
//         let _ = eqp.reorder([PhysicalSortExpr::new_default(
//             datafusion::physical_plan::expressions::col("a", &schema)?,
//         )]);
//         let original_ctx = TaskContext::new(
//             eqp,
//             Partitioning::RoundRobinBatch(3),
//             EmissionType::Incremental,
//             Boundedness::Bounded,
//         );
//
//         let local_ctx_ptr = FFI_TaskContext::from(&original_ctx);
//
//         let foreign_ctx: TaskContext = local_ctx_ptr.try_into()?;
//
//         assert_eq!(format!("{foreign_props:?}"), format!("{original_props:?}"));
//
//         Ok(())
//     }
// }
