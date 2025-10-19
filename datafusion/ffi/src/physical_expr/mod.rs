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

use crate::arrow_wrappers::WrappedArray;
use crate::execution_plan::ExecutionPlanPrivateData;
use crate::expr::columnar_value::FFI_ColumnarValue;
use crate::expr::distribution::FFI_Distribution;
use crate::expr::expr_properties::FFI_ExprProperties;
use crate::expr::interval::FFI_Interval;
use crate::record_batch_stream::{
    record_batch_to_wrapped_array, wrapped_array_to_record_batch,
};
use crate::table_provider::FFI_TableProvider;
use crate::util::FFIResult;
use crate::{
    arrow_wrappers::WrappedSchema,
    df_result,
    execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan},
    insert_op::FFI_InsertOp,
    rresult, rresult_return,
    table_source::FFI_TableType,
};
use abi_stable::pmr::RSlice;
use abi_stable::std_types::RResult;
use abi_stable::{
    std_types::{ROption, RString, RVec},
    StableAbi,
};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow::ffi::FFI_ArrowArray;
use arrow_schema::{DataType, FieldRef, Schema};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr_common::interval_arithmetic::Interval;
use datafusion::logical_expr_common::sort_properties::ExprProperties;
use datafusion::logical_expr_common::statistics::Distribution;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_proto::{
    logical_plan::{
        from_proto::parse_exprs, to_proto::serialize_exprs, DefaultLogicalExtensionCodec,
    },
    protobuf::LogicalExprList,
};
use prost::Message;
use std::fmt::Formatter;
use std::{any::Any, ffi::c_void, sync::Arc};
use tokio::runtime::Handle;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PhysicalExpr {
    pub data_type: unsafe extern "C" fn(
        &Self,
        input_schema: WrappedSchema,
    ) -> FFIResult<WrappedSchema>,

    pub nullable:
        unsafe extern "C" fn(&Self, input_schema: WrappedSchema) -> FFIResult<bool>,

    pub evaluate:
        unsafe extern "C" fn(&Self, batch: WrappedArray) -> FFIResult<FFI_ColumnarValue>,

    pub return_field: unsafe extern "C" fn(
        &Self,
        input_schema: WrappedSchema,
    ) -> FFIResult<WrappedSchema>,

    pub evaluate_selection: unsafe extern "C" fn(
        &Self,
        batch: WrappedArray,
        selection: FFI_ArrowArray,
    ) -> FFIResult<FFI_ColumnarValue>,

    pub children: unsafe extern "C" fn(&Self) -> RVec<FFI_PhysicalExpr>,

    pub new_with_children:
        unsafe extern "C" fn(&Self, children: RVec<FFI_PhysicalExpr>) -> FFIResult<Self>,

    pub evalutate_bounds: unsafe extern "C" fn(
        &Self,
        children: RSlice<FFI_Interval>,
    ) -> FFIResult<Interval>,

    pub propagate_constraints:
        unsafe extern "C" fn(
            &Self,
            interval: &FFI_Interval,
            children: RSlice<FFI_Interval>,
        ) -> FFIResult<ROption<RVec<FFI_Interval>>>,

    pub evaluate_statistics: unsafe extern "C" fn(
        &Self,
        children: &[&FFI_Distribution],
    ) -> FFIResult<FFI_Distribution>,

    pub propagate_statistics:
        unsafe extern "C" fn(
            &Self,
            parent: &FFI_Distribution,
            children: &[&FFI_Distribution],
        ) -> FFIResult<ROption<RVec<FFI_Distribution>>>,

    pub get_properties: unsafe extern "C" fn(
        &Self,
        children: &[FFI_ExprProperties],
    ) -> FFIResult<FFI_ExprProperties>,

    pub fmt_sql: unsafe extern "C" fn(&Self) -> FFIResult<RString>,

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

impl FFI_PhysicalExpr {
    fn inner(&self) -> &Arc<dyn PhysicalExpr> {
        unsafe {
            let private_data = self.private_data as *const PhysicalExprPrivateData;
            &(*private_data).expr
        }
    }
}

struct PhysicalExprPrivateData {
    expr: Arc<dyn PhysicalExpr + Send>,
}

unsafe extern "C" fn data_type_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<WrappedSchema> {
    let expr = expr.inner();
    let schema = input_schema.into();
    let data_type = expr.data_type(schema);
    rresult!(data_type.map(|dt| dt.into()))
}

unsafe extern "C" fn nullable_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<bool> {
    let expr = expr.inner();
    let schema = input_schema.into();
    rresult!(expr.nullable(schema))
}

unsafe extern "C" fn evaluate_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    let value = rresult_return!(expr.inner().evaluate(&batch));
    RResult::ROk(value.into())
}

unsafe extern "C" fn return_field_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<WrappedSchema> {
    let expr = expr.inner();
    let schema = input_schema.into();
    rresult_return!(expr.return_field(schema).map(|field| field.into()))
}

unsafe extern "C" fn evaluate_selection_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
    selection: FFI_ArrowArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    let selection: ArrayRef = selection.into();
    let selection = rresult_return!(selection.as_any().downcast_ref::<BooleanArray>());
    let value = rresult_return!(expr.inner().evaluate_selection(&batch, selection));
    RResult::ROk(value.into())
}

unsafe extern "C" fn children_fn_wrapper(
    expr: &FFI_PhysicalExpr,
) -> RVec<FFI_PhysicalExpr> {
    let expr = expr.inner();
    let children = expr.children();
    children.into_iter().map(|child| child.into()).collect()
}

unsafe extern "C" fn new_with_children_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: RVec<FFI_PhysicalExpr>,
) -> FFIResult<FFI_PhysicalExpr> {
    let expr = expr.inner();
    let children = children.into_iter().map(|child| child.into()).collect();
    rresult!(expr.new_with_children(children).map(|child| child.into()))
}

unsafe extern "C" fn evalutate_bounds_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: RSlice<FFI_Interval>,
) -> FFIResult<Interval> {
    let expr = expr.inner();
    let children = children
        .iter()
        .map(|child| child.into())
        .collect::<Vec<_>>();

    rresult!(expr.evaluate_bounds(&children))
}

unsafe extern "C" fn propagate_constraints_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    interval: &FFI_Interval,
    children: RSlice<FFI_Interval>,
) -> FFIResult<ROption<RVec<FFI_Interval>>> {
    let expr = expr.inner();
    let interval = interval.into();
    let children = children
        .iter()
        .map(|child| child.into())
        .collect::<Vec<_>>();

    rresult!(expr.propagate_constraints(&interval, &children).into())
}

unsafe extern "C" fn evaluate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &[&FFI_Distribution],
) -> FFIResult<FFI_Distribution> {
    let expr = expr.inner();
    let children = children
        .iter()
        .map(|child| child.into())
        .collect::<Vec<_>>();
    rresult!(expr.evaluate_statistics(&children).into())
}

unsafe extern "C" fn propagate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    parent: &FFI_Distribution,
    children: &[&FFI_Distribution],
) -> FFIResult<ROption<RVec<FFI_Distribution>>> {
    let expr = expr.inner();
    let parent = parent.into();
    let children = children
        .iter()
        .map(|child| child.into())
        .collect::<Vec<_>>();

    rresult!(expr.propagate_statistics(&parent, &children).into())
}

unsafe extern "C" fn get_properties_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &[FFI_ExprProperties],
) -> FFIResult<FFI_ExprProperties> {
    let expr = expr.inner();
    let children = children.iter().map(|child| child.into()).collect();
    rresult!(expr.get_properties(&children).into())
}

unsafe extern "C" fn fmt_sql_fn_wrapper(expr: &FFI_PhysicalExpr) -> FFIResult<RString> {
    let expr = expr.inner();
    let result = fmt_sql(expr).to_string();
    RResult::ROk(result.into())
}

unsafe extern "C" fn snapshot_fn_wrapper(
    expr: &FFI_PhysicalExpr,
) -> FFIResult<ROption<FFI_PhysicalExpr>> {
    let expr = expr.inner();
    rresult!(expr.snapshot().map(|snapshot| snapshot.into()))
}

unsafe extern "C" fn snapshot_generation_fn_wrapper(expr: &FFI_PhysicalExpr) -> u64 {
    let expr = expr.inner();
    expr.snapshot_generation()
}

unsafe extern "C" fn is_volatile_node_fn_wrapper(expr: &FFI_PhysicalExpr) -> bool {
    let expr = expr.inner();
    expr.is_volatile_node()
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
        data_type: data_type_fn_wrapper,
        nullable: nullable_fn_wrapper,
        evaluate: evaluate_fn_wrapper,
        return_field: return_field_fn_wrapper,
        evaluate_selection: evaluate_selection_fn_wrapper,
        children: children_fn_wrapper,
        new_with_children: new_with_children_fn_wrapper,
        evalutate_bounds: evalutate_bounds_fn_wrapper,
        propagate_constraints: propagate_constraints_fn_wrapper,
        evaluate_statistics: evaluate_statistics_fn_wrapper,
        propagate_statistics: propagate_statistics_fn_wrapper,
        get_properties: get_properties_fn_wrapper,
        fmt_sql: fmt_sql_fn_wrapper,
        snapshot: snapshot_fn_wrapper,
        snapshot_generation: snapshot_generation_fn_wrapper,
        is_volatile_node: is_volatile_node_fn_wrapper,
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
    pub fn new(expr: Arc<dyn PhysicalExpr + Send>) -> Self {
        let private_data = Box::new(PhysicalExprPrivateData { expr });

        Self {
            data_type: data_type_fn_wrapper,
            nullable: nullable_fn_wrapper,
            evaluate: evaluate_fn_wrapper,
            return_field: return_field_fn_wrapper,
            evaluate_selection: evaluate_selection_fn_wrapper,
            children: children_fn_wrapper,
            new_with_children: new_with_children_fn_wrapper,
            evalutate_bounds: evalutate_bounds_fn_wrapper,
            propagate_constraints: propagate_constraints_fn_wrapper,
            evaluate_statistics: evaluate_statistics_fn_wrapper,
            propagate_statistics: propagate_statistics_fn_wrapper,
            get_properties: get_properties_fn_wrapper,
            fmt_sql: fmt_sql_fn_wrapper,
            snapshot: snapshot_fn_wrapper,
            snapshot_generation: snapshot_generation_fn_wrapper,
            is_volatile_node: is_volatile_node_fn_wrapper,
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
        unsafe {
            let schema = input_schema.into();
            df_result!((self.0.data_type)(&self.0, schema).into())
        }
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        unsafe {
            let schema = input_schema.into();
            df_result!((self.0.nullable)(&self.0, schema))
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        unsafe {
            let batch = record_batch_to_wrapped_array(batch.clone())?;
            df_result!((self.0.evaluate)(&self.0, batch).map(|v| v.into()))
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        unsafe {
            let schema = input_schema.into();
            df_result!((self.0.return_field)(&self.0, schema).map(|f| f.into()))
        }
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        unsafe {
            let batch = record_batch_to_wrapped_array(batch)?;
            let selection = selection.into();
            df_result!(
                (self.0.evaluate_selection)(&self.0, batch, selection).map(|f| f.into())
            )
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        unsafe {
            (self.0.children)(&self.0)
                .into_iter()
                .map(Into::into)
                .collect()
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        unsafe {
            let children = children.into_iter().map(Into::into).collect();
            df_result!((self.0.new_with_children)(&self.0, children).map(Into::into))
        }
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        unsafe {
            let children = children.iter().map(Into::into).collect();
            df_result!((self.0.evalutate_bounds)(&self.0, children))
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        unsafe {
            let interval = interval.into();
            let children = children.iter().map(Into::into).collect();
            df_result!((self.0.propagate_constraints)(&self.0, interval, children)
                .map(Into::into)
                .into())
        }
    }

    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        unsafe {
            let children = children.iter().map(Into::into).collect::<Vec<_>>();
            df_result!((self.0.evaluate_statistics)(&self.0, &children).map(Into::into))
        }
    }

    fn propagate_statistics(
        &self,
        parent: &Distribution,
        children: &[&Distribution],
    ) -> Result<Option<Vec<Distribution>>> {
        unsafe {
            let parent = parent.into();
            let children = children.iter().map(Into::into).collect();
            df_result!(
                (self.0.propagate_statistics)(&self.0, parent, children).map(Into::into)
            )
        }
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        unsafe {
            let children = children.iter().map(Into::into).collect();
            df_result!((self.0.get_properties)(&self.0, children).map(Into::into))
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match (self.0.fmt_sql)(&self.0) {
                RResult::ROk(sql) => write!(f, "{sql}"),
                RResult::RErr(_) => Err(std::fmt::Error),
            }
        }
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        unsafe { df_result!((self.0.snapshot)(&self.0).map(Into::into)) }
    }

    fn snapshot_generation(&self) -> u64 {
        unsafe { (self.0.snapshot_generation)(&self.0) }
    }

    fn is_volatile_node(&self) -> bool {
        unsafe { (self.0.is_volatile_node)(&self.0) }
    }
}
