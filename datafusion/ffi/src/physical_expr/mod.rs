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

pub(crate) mod partitioning;
pub(crate) mod sort;

use std::any::Any;
use std::ffi::c_void;
use std::fmt::{Display, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion_common::{Result, ffi_datafusion_err};
use datafusion_expr::ColumnarValue;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::statistics::Distribution;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::fmt_sql;

use crate::arrow_wrappers::{WrappedArray, WrappedSchema};
use crate::expr::columnar_value::FFI_ColumnarValue;
use crate::expr::distribution::FFI_Distribution;
use crate::expr::expr_properties::FFI_ExprProperties;
use crate::expr::interval::FFI_Interval;
use crate::record_batch_stream::{
    record_batch_to_wrapped_array, wrapped_array_to_record_batch,
};
use crate::util::FFIResult;
use crate::{df_result, rresult, rresult_return};

#[repr(C)]
#[derive(Debug, StableAbi)]
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
        children: RVec<FFI_Interval>,
    ) -> FFIResult<FFI_Interval>,

    pub propagate_constraints:
        unsafe extern "C" fn(
            &Self,
            interval: FFI_Interval,
            children: RVec<FFI_Interval>,
        ) -> FFIResult<ROption<RVec<FFI_Interval>>>,

    pub evaluate_statistics: unsafe extern "C" fn(
        &Self,
        children: RVec<FFI_Distribution>,
    ) -> FFIResult<FFI_Distribution>,

    pub propagate_statistics:
        unsafe extern "C" fn(
            &Self,
            parent: FFI_Distribution,
            children: RVec<FFI_Distribution>,
        ) -> FFIResult<ROption<RVec<FFI_Distribution>>>,

    pub get_properties: unsafe extern "C" fn(
        &Self,
        children: RVec<FFI_ExprProperties>,
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
    /// A [`ForeignPhysicalExpr`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> usize,
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
    let schema: SchemaRef = input_schema.into();
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
    let schema: SchemaRef = input_schema.into();
    rresult!(expr.nullable(&schema))
}

unsafe extern "C" fn evaluate_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    rresult!(
        expr.inner()
            .evaluate(&batch)
            .and_then(FFI_ColumnarValue::try_from)
    )
}

unsafe extern "C" fn return_field_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    input_schema: WrappedSchema,
) -> FFIResult<WrappedSchema> {
    let expr = expr.inner();
    let schema: SchemaRef = input_schema.into();
    rresult!(
        expr.return_field(&schema)
            .and_then(|f| FFI_ArrowSchema::try_from(&f).map_err(Into::into))
            .map(WrappedSchema)
    )
}

unsafe extern "C" fn evaluate_selection_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    batch: WrappedArray,
    selection: WrappedArray,
) -> FFIResult<FFI_ColumnarValue> {
    let batch = rresult_return!(wrapped_array_to_record_batch(batch));
    let selection: ArrayRef = rresult_return!(selection.try_into());
    let selection = rresult_return!(
        selection
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or(ffi_datafusion_err!("Unexpected selection array type"))
    );
    rresult!(
        expr.inner()
            .evaluate_selection(&batch, selection)
            .and_then(FFI_ColumnarValue::try_from)
    )
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
    let children = children.iter().map(Into::into).collect::<Vec<_>>();
    rresult!(expr.with_new_children(children).map(FFI_PhysicalExpr::from))
}

unsafe extern "C" fn evaluate_bounds_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: RVec<FFI_Interval>,
) -> FFIResult<FFI_Interval> {
    let expr = expr.inner();
    let children = rresult_return!(
        children
            .into_iter()
            .map(Interval::try_from)
            .collect::<Result<Vec<_>>>()
    );
    let children_borrowed = children.iter().collect::<Vec<_>>();

    rresult!(
        expr.evaluate_bounds(&children_borrowed)
            .and_then(FFI_Interval::try_from)
    )
}

unsafe extern "C" fn propagate_constraints_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    interval: FFI_Interval,
    children: RVec<FFI_Interval>,
) -> FFIResult<ROption<RVec<FFI_Interval>>> {
    let expr = expr.inner();
    let interval = rresult_return!(Interval::try_from(interval));
    let children = rresult_return!(
        children
            .into_iter()
            .map(Interval::try_from)
            .collect::<Result<Vec<_>>>()
    );
    let children_borrowed = children.iter().collect::<Vec<_>>();

    let result =
        rresult_return!(expr.propagate_constraints(&interval, &children_borrowed));

    let result = rresult_return!(
        result
            .map(|intervals| intervals
                .into_iter()
                .map(FFI_Interval::try_from)
                .collect::<Result<RVec<_>>>())
            .transpose()
    );

    RResult::ROk(result.into())
}

unsafe extern "C" fn evaluate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: RVec<FFI_Distribution>,
) -> FFIResult<FFI_Distribution> {
    let expr = expr.inner();
    let children = rresult_return!(
        children
            .into_iter()
            .map(Distribution::try_from)
            .collect::<Result<Vec<_>>>()
    );
    let children_borrowed = children.iter().collect::<Vec<_>>();
    rresult!(
        expr.evaluate_statistics(&children_borrowed)
            .and_then(|dist| FFI_Distribution::try_from(&dist))
    )
}

unsafe extern "C" fn propagate_statistics_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    parent: FFI_Distribution,
    children: RVec<FFI_Distribution>,
) -> FFIResult<ROption<RVec<FFI_Distribution>>> {
    let expr = expr.inner();
    let parent = rresult_return!(Distribution::try_from(parent));
    let children = rresult_return!(
        children
            .into_iter()
            .map(Distribution::try_from)
            .collect::<Result<Vec<_>>>()
    );
    let children_borrowed = children.iter().collect::<Vec<_>>();

    let result = rresult_return!(expr.propagate_statistics(&parent, &children_borrowed));
    let result = rresult_return!(
        result
            .map(|dists| dists
                .iter()
                .map(FFI_Distribution::try_from)
                .collect::<Result<RVec<_>>>())
            .transpose()
    );

    RResult::ROk(result.into())
}

unsafe extern "C" fn get_properties_fn_wrapper(
    expr: &FFI_PhysicalExpr,
    children: RVec<FFI_ExprProperties>,
) -> FFIResult<FFI_ExprProperties> {
    let expr = expr.inner();
    let children = rresult_return!(
        children
            .into_iter()
            .map(ExprProperties::try_from)
            .collect::<Result<Vec<_>>>()
    );
    rresult!(
        expr.get_properties(&children)
            .and_then(|p| FFI_ExprProperties::try_from(&p))
    )
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
    rresult!(
        expr.snapshot()
            .map(|snapshot| snapshot.map(FFI_PhysicalExpr::from).into())
    )
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
    let expr = expr.inner();
    let mut hasher = DefaultHasher::new();
    expr.hash(&mut hasher);
    hasher.finish()
}

unsafe extern "C" fn release_fn_wrapper(expr: &mut FFI_PhysicalExpr) {
    unsafe {
        debug_assert!(!expr.private_data.is_null());
        let private_data =
            Box::from_raw(expr.private_data as *mut PhysicalExprPrivateData);
        drop(private_data);
        expr.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(expr: &FFI_PhysicalExpr) -> FFI_PhysicalExpr {
    unsafe {
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
            library_marker_id: crate::get_library_marker_id,
        }
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
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_PhysicalExpr to interact with the expression.
#[derive(Debug)]
pub struct ForeignPhysicalExpr {
    expr: FFI_PhysicalExpr,
    children: Vec<Arc<dyn PhysicalExpr>>,
}

unsafe impl Send for ForeignPhysicalExpr {}
unsafe impl Sync for ForeignPhysicalExpr {}

impl From<&FFI_PhysicalExpr> for Arc<dyn PhysicalExpr> {
    fn from(ffi_expr: &FFI_PhysicalExpr) -> Self {
        if (ffi_expr.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(ffi_expr.inner())
        } else {
            let children = unsafe {
                (ffi_expr.children)(ffi_expr)
                    .into_iter()
                    .map(|expr| <Arc<dyn PhysicalExpr>>::from(&expr))
                    .collect()
            };

            Arc::new(ForeignPhysicalExpr {
                expr: ffi_expr.clone(),
                children,
            })
        }
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
            let children = children.into_iter().map(FFI_PhysicalExpr::from).collect();
            df_result!(
                (self.expr.new_with_children)(&self.expr, &children).map(|expr| <Arc<
                    dyn PhysicalExpr,
                >>::from(
                    &expr
                ))
            )
        }
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        unsafe {
            let children = children
                .iter()
                .map(|interval| FFI_Interval::try_from(*interval))
                .collect::<Result<RVec<_>>>()?;
            df_result!((self.expr.evaluate_bounds)(&self.expr, children))
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
                &self.expr, interval, children
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
                df_result!((self.expr.evaluate_statistics)(&self.expr, children))?;
            Distribution::try_from(result)
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
                &self.expr, parent, children
            ))?;

            let result: Option<Result<Vec<Distribution>>> = result
                .map(|dists| {
                    dists
                        .into_iter()
                        .map(Distribution::try_from)
                        .collect::<Result<Vec<_>>>()
                })
                .into();

            result.transpose()
        }
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        unsafe {
            let children = children
                .iter()
                .map(FFI_ExprProperties::try_from)
                .collect::<Result<RVec<_>>>()?;
            df_result!((self.expr.get_properties)(&self.expr, children))
                .and_then(ExprProperties::try_from)
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
                .map(|expr| <Arc<dyn PhysicalExpr>>::from(&expr))
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

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hash, Hasher};
    use std::sync::Arc;

    use arrow::array::{BooleanArray, RecordBatch, record_batch};
    use datafusion_common::tree_node::DynTreeNode;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::interval_arithmetic::Interval;
    use datafusion_expr::statistics::Distribution;
    use datafusion_physical_expr::expressions::{Column, NegativeExpr, NotExpr};
    use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, fmt_sql};

    use crate::physical_expr::FFI_PhysicalExpr;

    fn create_test_expr() -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
        let original = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let mut ffi_expr = FFI_PhysicalExpr::from(Arc::clone(&original));
        ffi_expr.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_expr: Arc<dyn PhysicalExpr> = (&ffi_expr).into();

        (original, foreign_expr)
    }

    fn test_record_batch() -> RecordBatch {
        record_batch!(("a", Int32, [1, 2, 3])).unwrap()
    }

    #[test]
    fn ffi_physical_expr_fields() -> Result<(), DataFusionError> {
        let (original, foreign_expr) = create_test_expr();
        let schema = test_record_batch().schema();

        // Verify the mock marker worked, otherwise tests to follow are not useful
        assert_ne!(original.as_ref(), foreign_expr.as_ref());

        assert_eq!(
            original.return_field(&schema)?,
            foreign_expr.return_field(&schema)?
        );

        assert_eq!(
            original.data_type(&schema)?,
            foreign_expr.data_type(&schema)?
        );
        assert_eq!(original.nullable(&schema)?, foreign_expr.nullable(&schema)?);

        Ok(())
    }
    #[test]
    fn ffi_physical_expr_evaluate() -> Result<(), DataFusionError> {
        let (original, foreign_expr) = create_test_expr();
        let rb = test_record_batch();

        assert_eq!(
            original.evaluate(&rb)?.to_array(3)?.as_ref(),
            foreign_expr.evaluate(&rb)?.to_array(3)?.as_ref()
        );

        Ok(())
    }
    #[test]
    fn ffi_physical_expr_selection() -> Result<(), DataFusionError> {
        let (original, foreign_expr) = create_test_expr();
        let rb = test_record_batch();

        let selection = BooleanArray::from(vec![true, false, true]);

        assert_eq!(
            original
                .evaluate_selection(&rb, &selection)?
                .to_array(3)?
                .as_ref(),
            foreign_expr
                .evaluate_selection(&rb, &selection)?
                .to_array(3)?
                .as_ref()
        );
        Ok(())
    }

    #[test]
    fn ffi_physical_expr_with_children() -> Result<(), DataFusionError> {
        let (original, _) = create_test_expr();
        let not_expr =
            Arc::new(NotExpr::new(Arc::clone(&original))) as Arc<dyn PhysicalExpr>;
        let mut ffi_not = FFI_PhysicalExpr::from(not_expr);
        ffi_not.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_not: Arc<dyn PhysicalExpr> = (&ffi_not).into();

        let replacement = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let updated =
            Arc::clone(&foreign_not).with_new_children(vec![Arc::clone(&replacement)])?;
        assert_eq!(
            format!("{updated:?}").as_str(),
            "NotExpr { arg: Column { name: \"b\", index: 1 } }"
        );

        let updated = foreign_not
            .with_new_arc_children(Arc::clone(&foreign_not), vec![replacement])?;
        assert_eq!(format!("{updated}").as_str(), "NOT b@1");

        Ok(())
    }

    fn create_test_negative_expr() -> (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>) {
        let (original, _) = create_test_expr();

        let negative_expr =
            Arc::new(NegativeExpr::new(Arc::clone(&original))) as Arc<dyn PhysicalExpr>;
        let mut ffi_neg = FFI_PhysicalExpr::from(Arc::clone(&negative_expr));
        ffi_neg.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_neg: Arc<dyn PhysicalExpr> = (&ffi_neg).into();

        (negative_expr, foreign_neg)
    }

    #[test]
    fn ffi_physical_expr_bounds() -> Result<(), DataFusionError> {
        let (negative_expr, foreign_neg) = create_test_negative_expr();

        let interval =
            Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(10)))?;
        let left = negative_expr.evaluate_bounds(&[&interval])?;
        let right = foreign_neg.evaluate_bounds(&[&interval])?;

        assert_eq!(left, right);

        Ok(())
    }

    #[test]
    fn ffi_physical_expr_constraints() -> Result<(), DataFusionError> {
        let (negative_expr, foreign_neg) = create_test_negative_expr();

        let interval =
            Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(10)))?;

        let child =
            Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(10)))?;
        let left = negative_expr.propagate_constraints(&interval, &[&child])?;
        let right = foreign_neg.propagate_constraints(&interval, &[&child])?;

        assert_eq!(left, right);
        Ok(())
    }

    #[test]
    fn ffi_physical_expr_statistics() -> Result<(), DataFusionError> {
        let (negative_expr, foreign_neg) = create_test_negative_expr();
        let interval =
            Interval::try_new(ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(10)))?;

        for distribution in [
            Distribution::new_uniform(interval.clone())?,
            Distribution::new_exponential(
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(10)),
                true,
            )?,
            Distribution::new_gaussian(
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(10)),
            )?,
            Distribution::new_generic(
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(10)),
                interval,
            )?,
        ] {
            let left = negative_expr.evaluate_statistics(&[&distribution])?;
            let right = foreign_neg.evaluate_statistics(&[&distribution])?;

            assert_eq!(left, right);

            let left =
                negative_expr.propagate_statistics(&distribution, &[&distribution])?;
            let right =
                foreign_neg.propagate_statistics(&distribution, &[&distribution])?;

            assert_eq!(left, right);
        }
        Ok(())
    }

    #[test]
    fn ffi_physical_expr_properties() -> Result<(), DataFusionError> {
        let (original, foreign_expr) = create_test_expr();

        let left = original.get_properties(&[])?;
        let right = foreign_expr.get_properties(&[])?;

        assert_eq!(left.sort_properties, right.sort_properties);
        assert_eq!(left.range, right.range);

        Ok(())
    }

    #[test]
    fn ffi_physical_formatting() {
        let (original, foreign_expr) = create_test_expr();

        let left = format!("{}", fmt_sql(original.as_ref()));
        let right = format!("{}", fmt_sql(foreign_expr.as_ref()));
        assert_eq!(left, right);
    }

    #[test]
    fn ffi_physical_expr_snapshots() -> Result<(), DataFusionError> {
        let (original, foreign_expr) = create_test_expr();

        let left = original.snapshot()?;
        let right = foreign_expr.snapshot()?;
        assert_eq!(left, right);

        assert_eq!(
            original.snapshot_generation(),
            foreign_expr.snapshot_generation()
        );

        Ok(())
    }

    #[test]
    fn ffi_physical_expr_volatility() {
        let (original, foreign_expr) = create_test_expr();
        assert_eq!(original.is_volatile_node(), foreign_expr.is_volatile_node());
    }

    #[test]
    fn ffi_physical_expr_hash() {
        let (_, foreign_1) = create_test_expr();
        let (_, foreign_2) = create_test_expr();

        assert_ne!(&foreign_1, &foreign_2);

        let mut hasher = DefaultHasher::new();
        foreign_1.as_ref().hash(&mut hasher);
        let hash_1 = hasher.finish();

        let mut hasher = DefaultHasher::new();
        foreign_2.as_ref().hash(&mut hasher);
        let hash_2 = hasher.finish();

        // We cannot compare a local object and a foreign object
        // so create two foreign objects that *should* be identical
        // even though they were created differently.
        assert_eq!(hash_1, hash_2);
    }

    #[test]
    fn ffi_physical_expr_display() {
        let (original, foreign_expr) = create_test_expr();
        assert_eq!(format!("{original}"), format!("{foreign_expr}"));
    }
}
