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

use std::{any::Any, ffi::c_void, sync::Arc};
use std::fmt::Formatter;
use abi_stable::{
    std_types::{ROption, FFIResult, RString, RVec},
    StableAbi,
};
use abi_stable::pmr::RSlice;
use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::{DataType, FieldRef, Schema};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_proto::{
    logical_plan::{
        from_proto::parse_exprs, to_proto::serialize_exprs, DefaultLogicalExtensionCodec,
    },
    protobuf::LogicalExprList,
};
use prost::Message;
use tokio::runtime::Handle;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr_common::interval_arithmetic::Interval;
use datafusion::logical_expr_common::sort_properties::ExprProperties;
use datafusion::logical_expr_common::statistics::Distribution;
use crate::session::{FFI_Session, ForeignSession};
use crate::{
    arrow_wrappers::WrappedSchema,
    df_result,
    execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan},
    insert_op::FFI_InsertOp,
    rresult_return,
    table_source::FFI_TableType,
};
use datafusion_catalog::Session;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::registry::MemoryFunctionRegistry;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use crate::arrow_wrappers::WrappedArray;
use crate::expr::columnar_value::FFI_ColumnarValue;
use crate::expr::distribution::FFI_Distribution;
use crate::expr::expr_properties::FFI_ExprProperties;
use crate::expr::interval::FFI_Interval;
use crate::table_provider::FFI_TableProvider;
use crate::util::FFIResult;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PhysicalExpr {
    pub data_type: unsafe extern "C" fn(&Self, input_schema: WrappedSchema) -> FFIResult<WrappedSchema>,

    pub nullable: unsafe extern "C" fn(&Self, input_schema: WrappedSchema) -> FFIResult<bool>,

    pub evaluate: unsafe extern "C" fn(&Self, batch: WrappedArray) -> FFIResult<FFI_ColumnarValue>,

    pub return_field: unsafe extern "C" fn(&Self, input_schema: WrappedSchema) -> FFIResult<WrappedSchema>,

    pub aggregate: unsafe extern "C" fn(&Self, batch: WrappedArray, selection: FFI_ArrowArray) -> FFIResult<FFI_ColumnarValue>,

    pub children: unsafe extern "C" fn(&Self) -> FFIResult<Vec<FFI_PhysicalExpr>>,

    pub new_with_children: unsafe extern "C" fn(&Self, children: RVec<FFI_PhysicalExpr>) -> FFIResult<Self>,

    pub evalutate_bounds: unsafe extern "C" fn(&Self, children: RSlice<FFI_Interval>) -> FFIResult<Interval>,

    pub propagate_constraints: unsafe extern "C" fn(&Self, interval: &FFI_Interval, children: RSlice<FFI_Interval>) -> FFIResult<ROption<RVec<FFI_Interval>>>,

    pub evaluate_statistics: unsafe extern "C" fn(&Self, children: &[&FFI_Distribution]) -> FFIResult<FFI_Distribution>,

    pub propagate_statistics: unsafe extern "C" fn(&Self, parent: &FFI_Distribution, children: &[&FFI_Distribution]) -> FFIResult<ROption<RVec<FFI_Distribution>>>,

    pub get_properties: unsafe extern "C" fn(&Self, children: &[FFI_ExprProperties]) -> FFIResult<FFI_ExprProperties>,

    pub fmt_sql: unsafe extern "C" fn(&Self, f: &mut Formatter<'_>) -> FFIResult<RString>,

    pub snapshot: unsafe extern "C" fn(&Self) -> FFIResult<ROption<FFI_PhysicalExpr>>,

    pub snapshot_generation: unsafe extern "C" fn(&Self) -> u64,

    pub is_volatile_node: unsafe extern "C" fn(&Self) -> bool,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignExecutionPlan`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_PhysicalExpr {}
unsafe impl Sync for FFI_PhysicalExpr {}

struct PhysicalExprPrivateData {
    expr: Arc<dyn PhysicalExpr + Send>,
}


unsafe extern "C" fn release_fn_wrapper(expr: &mut FFI_PhysicalExpr) {
    let private_data = Box::from_raw(expr.private_data as *mut PhysicalExprPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(expr: &FFI_PhysicalExpr) -> FFI_PhysicalExpr {
    let old_private_data = expr.private_data as *const PhysicalExprPrivateData;

    let private_data = Box::into_raw(Box::new(PhysicalExprPrivateData {
        expr: Arc::clone(&(*old_private_data).expr),
    })) as *mut c_void;

    FFI_PhysicalExpr {
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
    }
}

impl Drop for FFI_PhysicalExpr {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_PhysicalExpr {
    /// Creates a new [`FFI_PhysicalExpr`].
    pub fn new(
        expr: Arc<dyn PhysicalExpr + Send>,
    ) -> Self {
        let private_data = Box::new(PhysicalExprPrivateData { expr });

        Self {

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
/// FFI_PhysicalExpr to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignPhysicalExpr(pub FFI_PhysicalExpr);

unsafe impl Send for ForeignPhysicalExpr {}
unsafe impl Sync for ForeignPhysicalExpr {}

impl From<&FFI_PhysicalExpr> for ForeignPhysicalExpr {
    fn from(provider: &FFI_PhysicalExpr) -> Self {
        Self(provider.clone())
    }
}

impl Clone for FFI_PhysicalExpr {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl PhysicalExpr for ForeignPhysicalExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        todo!()
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        todo!()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        todo!()
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        todo!()
    }

    fn evaluate_selection(&self, batch: &RecordBatch, selection: &BooleanArray) -> Result<ColumnarValue> {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {
        todo!()
    }

    fn evaluate_bounds(&self, _children: &[&Interval]) -> Result<Interval> {
        todo!()
    }

    fn propagate_constraints(&self, _interval: &Interval, _children: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        todo!()
    }

    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        todo!()
    }

    fn propagate_statistics(&self, parent: &Distribution, children: &[&Distribution]) -> Result<Option<Vec<Distribution>>> {
        todo!()
    }

    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        todo!()
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        todo!()
    }

    fn snapshot_generation(&self) -> u64 {
        todo!()
    }

    fn is_volatile_node(&self) -> bool {
        todo!()
    }
}
