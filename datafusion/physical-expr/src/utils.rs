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

//! Collection of utility functions for Physical Expr optimization

use crate::PhysicalExpr;
use datafusion_common::DataFusionError;
use std::result;
use std::sync::Arc;

pub type Result<T> = result::Result<T, DataFusionError>;

/// Apply transform `F` to the PhysicalExpr's children, the transform `F` might have a direction(Preorder or Postorder)
fn map_children<F>(
    expr: Arc<dyn PhysicalExpr>,
    transform: F,
) -> Result<Arc<dyn PhysicalExpr>>
where
    F: Fn(Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>,
{
    if !expr.children().is_empty() {
        let new_children: Result<Vec<_>> =
            expr.children().into_iter().map(transform).collect();
        with_new_children_if_necessary(expr, new_children?)
    } else {
        Ok(expr)
    }
}

/// Convenience utils for writing optimizers rule: recursively apply the given `op` to the PhysicalExpr tree.
/// When `op` does not apply to a given expr, it is left unchanged.
/// The default tree traversal direction is transform_down(Preorder Traversal).
pub fn transform<F>(expr: Arc<dyn PhysicalExpr>, op: &F) -> Result<Arc<dyn PhysicalExpr>>
where
    F: Fn(Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>>,
{
    transform_down(expr, op)
}

/// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the PhysicalExpr and all of its
/// children(Preorder Traversal). When the `op` does not apply to a given PhysicalExpr, it is left unchanged.
pub fn transform_down<F>(
    expr: Arc<dyn PhysicalExpr>,
    op: &F,
) -> Result<Arc<dyn PhysicalExpr>>
where
    F: Fn(Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>>,
{
    let expr_cloned = expr.clone();
    let after_op = match op(expr_cloned) {
        Some(value) => value,
        None => expr,
    };
    map_children(after_op.clone(), |expr: Arc<dyn PhysicalExpr>| {
        transform_down(expr, op)
    })
}

/// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
/// children and then itself(Postorder Traversal). When the `op` does not apply to a given PhysicalExpr, it is left unchanged.
#[allow(dead_code)]
pub fn transform_up<F>(
    expr: Arc<dyn PhysicalExpr>,
    op: &F,
) -> Result<Arc<dyn PhysicalExpr>>
where
    F: Fn(Arc<dyn PhysicalExpr>) -> Option<Arc<dyn PhysicalExpr>>,
{
    let after_op_children =
        map_children(expr, |expr: Arc<dyn PhysicalExpr>| transform_up(expr, op))?;

    let after_op_children_clone = after_op_children.clone();
    let new_expr = match op(after_op_children) {
        Some(value) => value,
        None => after_op_children_clone,
    };
    Ok(new_expr)
}

/// Returns a copy of this expr if we change any child according to the pointer comparison.
/// The size of `children` must be equal to the size of `PhysicalExpr::children()`.
/// Allow the vtable address comparisons for PhysicalExpr Trait Objects，it is harmless even
/// in the case of 'false-native'.
#[allow(clippy::vtable_address_comparisons)]
pub fn with_new_children_if_necessary(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    if children.len() != expr.children().len() {
        Err(DataFusionError::Internal(
            "PhysicalExpr: Wrong number of children".to_string(),
        ))
    } else if children.is_empty()
        || children
            .iter()
            .zip(expr.children().iter())
            .any(|(c1, c2)| !Arc::ptr_eq(c1, c2))
    {
        expr.with_new_children(children)
    } else {
        Ok(expr)
    }
}
