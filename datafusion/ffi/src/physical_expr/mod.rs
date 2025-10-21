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
use crate::udaf::ForeignAggregateUDF;
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
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::logical_expr::expr_rewriter::unalias;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr_common::interval_arithmetic::Interval;
use datafusion::logical_expr_common::sort_properties::ExprProperties;
use datafusion::logical_expr_common::statistics::Distribution;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::physical_expr::{fmt_sql, DynEq};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_common::{exec_datafusion_err, DataFusionError};
use datafusion_proto::{
    logical_plan::{
        from_proto::parse_exprs, to_proto::serialize_exprs, DefaultLogicalExtensionCodec,
    },
    protobuf::LogicalExprList,
};
use prost::Message;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
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
        selection: WrappedArray,
    ) -> FFIResult<FFI_ColumnarValue>,

    pub children: unsafe extern "C" fn(&Self) -> RVec<FFI_PhysicalExpr>,

    pub new_with_children:
        unsafe extern "C" fn(&Self, children: &RVec<FFI_PhysicalExpr>) -> FFIResult<Self>,

    pub evaluate_bounds: unsafe extern "C" fn(
        &Self,
        children: &RVec<FFI_Interval>,
    ) -> FFIResult<FFI_Interval>,

    pub propagate_constraints:
        unsafe extern "C" fn(
            &Self,
            interval: &FFI_Interval,
            children: &RVec<FFI_Interval>,
        ) -> FFIResult<ROption<RVec<FFI_Interval>>>,

    pub evaluate_statistics: unsafe extern "C" fn(
        &Self,
        children: &RVec<FFI_Distribution>,
    ) -> FFIResult<FFI_Distribution>,

    pub propagate_statistics:
        unsafe extern "C" fn(
            &Self,
            parent: &FFI_Distribution,
            children: &RVec<FFI_Distribution>,
        ) -> FFIResult<ROption<RVec<FFI_Distribution>>>,

    pub get_properties: unsafe extern "C" fn(
        &Self,
        children: &RVec<FFI_ExprProperties>,
    ) -> FFIResult<FFI_ExprProperties>,

    pub fmt_sql: unsafe extern "C" fn(&Self) -> FFIResult<RString>,

    pub snapshot: unsafe extern "C" fn(&Self) -> FFIResult<ROption<FFI_PhysicalExpr>>,

    pub snapshot_generation: unsafe extern "C" fn(&Self) -> u64,

    pub is_volatile_node: unsafe extern "C" fn(&Self) -> bool,

    // Display trait
    pub display: unsafe extern "C" fn(&Self) -> RString,

    // Hash trait
    pub hash: unsafe extern "C" fn(&Self) -> u64,

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
    expr: Arc<dyn PhysicalExpr>,
}

unsafe extern "C" fn data_type_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<WrappedSchema> {
    let expr = expr.inner();
    let schema: SchemaRef = rresult_return!(input_schema.try_into());
    let data_type = expr
        .data_type(&schema)
        .and_then(|dt| FFI_ArrowSchema::try_from(dt).map_err(Into::into))
        .map(WrappedSchema);
    rresult!(data_type)
}

unsafe extern "C" fn nullable_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<bool> {
    let expr = expr.inner();
    let schema: SchemaRef = rresult_return!(input_schema.try_into());
    rresult!(expr.nullable(&schema))
}

unsafe extern "C" fn evaluate_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    rresult!(expr
        .inner()
        .evaluate(&batch)
        .and_then(FFI_ColumnarValue::try_from))
}

unsafe extern "C" fn return_field_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<WrappedSchema> {
    let expr = expr.inner();
    let schema: SchemaRef = rresult_return!(input_schema.try_into());
    rresult!(expr
        .return_field(&schema)
        .and_then(|f| FFI_ArrowSchema::try_from(&f).map_err(Into::into))
        .map(WrappedSchema))
}

unsafe extern "C" fn evaluate_selection_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
    selection: WrappedArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    let selection: ArrayRef = rresult_return!(selection.try_into());
    let selection = rresult_return!(selection
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or(exec_datafusion_err!("Unexpected selection array type")));
    rresult!(expr
        .inner()
        .evaluate_selection(&batch, selection)
        .and_then(FFI_ColumnarValue::try_from))
}

unsafe extern "C" fn children_fn_wrapper(
    expr: &FFI_PhysicalExpr,
) -> RVec<FFI_PhysicalExpr> {
    let expr = expr.inner();
    let children = expr.children();
    children
        .into_iter()
        .map(|child| FFI_PhysicalExpr::from(Arc::clone(child)))
        .collect()
}

unsafe extern "C" fn new_with_children_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &RVec<FFI_PhysicalExpr>,
) -> FFIResult<FFI_PhysicalExpr> {
    let expr = Arc::clone(expr.inner());
    let children = children
        .iter()
        .map(|e| ForeignPhysicalExpr::from(e.clone()))
        .map(|e| Arc::new(e) as Arc<dyn PhysicalExpr>)
        .collect::<Vec<_>>();
    rresult!(expr.with_new_children(children).map(FFI_PhysicalExpr::from))
}

unsafe extern "C" fn evaluate_bounds_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &RVec<FFI_Interval>,
) -> FFIResult<FFI_Interval> {
    let expr = expr.inner();
    let children = rresult_return!(children
        .iter()
        .map(Interval::try_from)
        .collect::<Result<Vec<_>>>());
    let children_borrowed = children.iter().collect::<Vec<_>>();

    rresult!(expr
        .evaluate_bounds(&children_borrowed)
        .and_then(FFI_Interval::try_from))
}

unsafe extern "C" fn propagate_constraints_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    interval: &FFI_Interval,
    children: &RVec<FFI_Interval>,
) -> FFIResult<ROption<RVec<FFI_Interval>>> {
    let expr = expr.inner();
    let interval = rresult_return!(Interval::try_from(interval));
    let children = rresult_return!(children
        .iter()
        .map(Interval::try_from)
        .collect::<Result<Vec<_>>>());
    let children_borrowed = children.iter().collect::<Vec<_>>();

    let result =
        rresult_return!(expr.propagate_constraints(&interval, &children_borrowed));

    let result = rresult_return!(result
        .map(|intervals| intervals
            .into_iter()
            .map(FFI_Interval::try_from)
            .collect::<Result<RVec<_>>>())
        .transpose());

    RResult::ROk(result.into())
}

unsafe extern "C" fn evaluate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &RVec<FFI_Distribution>,
) -> FFIResult<FFI_Distribution> {
    let expr = expr.inner();
    let children = rresult_return!(children
        .iter()
        .map(Distribution::try_from)
        .collect::<Result<Vec<_>>>());
    let children_borrowed = children.iter().collect::<Vec<_>>();
    rresult!(expr.evaluate_statistics(&children_borrowed).and_then(|dist| FFI_Distribution::try_from(&dist)))
}

unsafe extern "C" fn propagate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    parent: &FFI_Distribution,
    children: &RVec<FFI_Distribution>,
) -> FFIResult<ROption<RVec<FFI_Distribution>>> {
    let expr = expr.inner();
    let parent = rresult_return!(Distribution::try_from(parent));
    let children = rresult_return!(children
        .iter()
        .map(Distribution::try_from)
        .collect::<Result<Vec<_>>>());
    let children_borrowed = children.iter().collect::<Vec<_>>();

    let result = rresult_return!(expr.propagate_statistics(&parent, &children_borrowed));
    let result = rresult_return!(result
        .map(|dists| dists
            .iter()
            .map(FFI_Distribution::try_from)
            .collect::<Result<RVec<_>>>())
        .transpose());

    RResult::ROk(result.into())
}

unsafe extern "C" fn get_properties_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: &RVec<FFI_ExprProperties>,
) -> FFIResult<FFI_ExprProperties> {
    let expr = expr.inner();
    let children = children
        .iter()
        .map(|child| child.into())
        .collect::<Vec<_>>();
    rresult!(expr.get_properties(&children).map(Into::into))
}

unsafe extern "C" fn fmt_sql_fn_wrapper(expr: &FFI_PhysicalExpr) -> FFIResult<RString> {
    let expr = expr.inner();
    let result = fmt_sql(expr.as_ref()).to_string();
    RResult::ROk(result.into())
}

unsafe extern "C" fn snapshot_fn_wrapper(
    expr: &FFI_PhysicalExpr,
) -> FFIResult<ROption<FFI_PhysicalExpr>> {
    let expr = expr.inner();
    rresult!(expr
        .snapshot()
        .map(|snapshot| snapshot.map(FFI_PhysicalExpr::from).into()))
}

unsafe extern "C" fn snapshot_generation_fn_wrapper(expr: &FFI_PhysicalExpr) -> u64 {
    let expr = expr.inner();
    expr.snapshot_generation()
}

unsafe extern "C" fn is_volatile_node_fn_wrapper(expr: &FFI_PhysicalExpr) -> bool {
    let expr = expr.inner();
    expr.is_volatile_node()
}
unsafe extern "C" fn display_fn_wrapper(expr: &FFI_PhysicalExpr) -> RString {
    let expr = expr.inner();
    format!("{expr}").into()
}

unsafe extern "C" fn hash_fn_wrapper(expr: &FFI_PhysicalExpr) -> u64 {
    let mut expr = expr.inner();
    let mut hasher = DefaultHasher::new();
    expr.hash(&mut hasher);
    hasher.finish()
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
        evaluate_bounds: evaluate_bounds_fn_wrapper,
        propagate_constraints: propagate_constraints_fn_wrapper,
        evaluate_statistics: evaluate_statistics_fn_wrapper,
        propagate_statistics: propagate_statistics_fn_wrapper,
        get_properties: get_properties_fn_wrapper,
        fmt_sql: fmt_sql_fn_wrapper,
        snapshot: snapshot_fn_wrapper,
        snapshot_generation: snapshot_generation_fn_wrapper,
        is_volatile_node: is_volatile_node_fn_wrapper,
        display: display_fn_wrapper,
        hash: hash_fn_wrapper,
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

impl From<Arc<dyn PhysicalExpr>> for FFI_PhysicalExpr {
    /// Creates a new [`FFI_PhysicalExpr`].
    fn from(expr: Arc<dyn PhysicalExpr>) -> Self {
        let private_data = Box::new(PhysicalExprPrivateData { expr });

        Self {
            data_type: data_type_fn_wrapper,
            nullable: nullable_fn_wrapper,
            evaluate: evaluate_fn_wrapper,
            return_field: return_field_fn_wrapper,
            evaluate_selection: evaluate_selection_fn_wrapper,
            children: children_fn_wrapper,
            new_with_children: new_with_children_fn_wrapper,
            evaluate_bounds: evaluate_bounds_fn_wrapper,
            propagate_constraints: propagate_constraints_fn_wrapper,
            evaluate_statistics: evaluate_statistics_fn_wrapper,
            propagate_statistics: propagate_statistics_fn_wrapper,
            get_properties: get_properties_fn_wrapper,
            fmt_sql: fmt_sql_fn_wrapper,
            snapshot: snapshot_fn_wrapper,
            snapshot_generation: snapshot_generation_fn_wrapper,
            is_volatile_node: is_volatile_node_fn_wrapper,
            display: display_fn_wrapper,
            hash: hash_fn_wrapper,
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
pub struct ForeignPhysicalExpr {
    pub expr: FFI_PhysicalExpr,
    children: Vec<Arc<dyn PhysicalExpr>>,
}

unsafe impl Send for ForeignPhysicalExpr {}
unsafe impl Sync for ForeignPhysicalExpr {}

impl From<FFI_PhysicalExpr> for ForeignPhysicalExpr {
    fn from(expr: FFI_PhysicalExpr) -> Self {
        let children = unsafe {
            (expr.children)(&expr)
                .into_iter()
                .map(|child| {
                    Arc::new(ForeignPhysicalExpr::from(child)) as Arc<dyn PhysicalExpr>
                })
                .collect()
        };

        Self { expr, children }
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
            let schema = WrappedSchema::from(Arc::new(input_schema.clone()));
            df_result!((self.expr.data_type)(&self.expr, schema))
                .and_then(|d| DataType::try_from(&d.0).map_err(Into::into))
        }
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        unsafe {
            let schema = WrappedSchema::from(Arc::new(input_schema.clone()));
            df_result!((self.expr.nullable)(&self.expr, schema))
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        unsafe {
            let batch = df_result!(record_batch_to_wrapped_array(batch.clone()))?;
            df_result!((self.expr.evaluate)(&self.expr, batch))
                .and_then(ColumnarValue::try_from)
        }
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        unsafe {
            let schema = WrappedSchema::from(Arc::new(input_schema.clone()));
            let result = df_result!((self.expr.return_field)(&self.expr, schema))?;
            Field::try_from(&result.0).map(Arc::new).map_err(Into::into)
        }
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> Result<ColumnarValue> {
        unsafe {
            let batch = df_result!(record_batch_to_wrapped_array(batch.clone()))?;
            // This is not ideal - we are cloning the selection array
            // This is not terrible since it will be a small array.
            // The other alternative is to modify the trait signature.
            let selection: ArrayRef = Arc::new(selection.clone());
            let selection = WrappedArray::try_from(&selection)?;
            df_result!((self.expr.evaluate_selection)(&self.expr, batch, selection))
                .and_then(ColumnarValue::try_from)
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        unsafe {
            let children = children
                .into_iter()
                .map(|expr| FFI_PhysicalExpr::from(expr))
                .collect();
            df_result!((self.expr.new_with_children)(&self.expr, &children)
                .map(|expr| Arc::new(ForeignPhysicalExpr::from(expr))))
        }
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        unsafe {
            let children = children
                .iter()
                .map(|interval| FFI_Interval::try_from(*interval))
                .collect::<Result<RVec<_>>>()?;
            df_result!((self.expr.evaluate_bounds)(&self.expr, &children))
                .and_then(Interval::try_from)
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        unsafe {
            let interval = interval.try_into()?;
            let children = children
                .iter()
                .map(|interval| FFI_Interval::try_from(*interval))
                .collect::<Result<RVec<_>>>()?;
            let result = df_result!((self.expr.propagate_constraints)(
                &self.expr, &interval, &children
            ))?;

            let result: Option<_> = result
                .map(|intervals| {
                    intervals
                        .into_iter()
                        .map(Interval::try_from)
                        .collect::<Result<Vec<_>>>()
                })
                .into();
            result.transpose()
        }
    }

    fn evaluate_statistics(&self, children: &[&Distribution]) -> Result<Distribution> {
        unsafe {
            let children = children
                .iter()
                .map(|dist| FFI_Distribution::try_from(*dist))
                .collect::<Result<RVec<_>>>()?;

            let result =
                df_result!((self.expr.evaluate_statistics)(&self.expr, &children))?;
            Distribution::try_from(&result)
        }
    }

    fn propagate_statistics(
        &self,
        parent: &Distribution,
        children: &[&Distribution],
    ) -> Result<Option<Vec<Distribution>>> {
        unsafe {
            let parent = FFI_Distribution::try_from(parent)?;
            let children = children
                .iter()
                .map(|dist| FFI_Distribution::try_from(*dist))
                .collect::<Result<RVec<_>>>()?;
            let result = df_result!((self.expr.propagate_statistics)(
                &self.expr, &parent, &children
            ))?;

            let result: Option<Result<Vec<Distribution>>> = result
                .map(|dists| {
                    dists
                        .iter()
                        .map(Distribution::try_from)
                        .collect::<Result<Vec<_>>>()
                })
                .into();

            result.transpose()
        }
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        unsafe {
            let children = children.iter().map(Into::into).collect::<RVec<_>>();
            df_result!((self.expr.get_properties)(&self.expr, &children).map(Into::into))
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match (self.expr.fmt_sql)(&self.expr) {
                RResult::ROk(sql) => write!(f, "{sql}"),
                RResult::RErr(_) => Err(std::fmt::Error),
            }
        }
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        unsafe {
            let result = df_result!((self.expr.snapshot)(&self.expr))?;
            Ok(result
                .map(|ffi_expr| {
                    Arc::new(ForeignPhysicalExpr::from(ffi_expr)) as Arc<dyn PhysicalExpr>
                })
                .into())
        }
    }

    fn snapshot_generation(&self) -> u64 {
        unsafe { (self.expr.snapshot_generation)(&self.expr) }
    }

    fn is_volatile_node(&self) -> bool {
        unsafe { (self.expr.is_volatile_node)(&self.expr) }
    }
}

impl Eq for ForeignPhysicalExpr {}
impl PartialEq for ForeignPhysicalExpr {
    fn eq(&self, other: &Self) -> bool {
        // FFI_PhysicalExpr cannot be compared, so identity equality is the best we can do.
        std::ptr::eq(self, other)
    }
}
impl Hash for ForeignPhysicalExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let value = unsafe { (self.expr.hash)(&self.expr) };
        value.hash(state)
    }
}

impl Display for ForeignPhysicalExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let display = unsafe { (self.expr.display)(&self.expr) };
        write!(f, "{display}")
    }
}
