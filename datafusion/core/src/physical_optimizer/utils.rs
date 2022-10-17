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

//! Collection of utility functions that are leveraged by the query optimizer rules

use super::optimizer::PhysicalOptimizerRule;
use crate::execution::context::SessionConfig;

use crate::error::Result;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
pub fn optimize_children(
    optimizer: &impl PhysicalOptimizerRule,
    plan: Arc<dyn ExecutionPlan>,
    session_config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .iter()
        .map(|child| optimizer.optimize(Arc::clone(child), session_config))
        .collect::<Result<Vec<_>>>()?;

    if children.is_empty() {
        Ok(Arc::clone(&plan))
    } else {
        with_new_children_if_necessary(plan, children)
    }
}

/// Apply transform `F` to the plan's children, the transform `F` might have a direction(Preorder or Postorder)
fn map_children<F>(
    plan: Arc<dyn ExecutionPlan>,
    transform: F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
{
    if !plan.children().is_empty() {
        let new_children: Result<Vec<_>> =
            plan.children().into_iter().map(transform).collect();
        with_new_children_if_necessary(plan, new_children?)
    } else {
        Ok(plan)
    }
}

/// Convenience utils for writing optimizers rule: recursively apply the given `op` to the plan tree.
/// When `op` does not apply to a given plan, it is left unchanged.
/// The default tree traversal direction is transform_down(Preorder Traversal).
#[allow(dead_code)]
pub fn transform<F>(
    plan: Arc<dyn ExecutionPlan>,
    op: &F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>>,
{
    transform_down(plan, op)
}

/// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the plan and all of its
/// children(Preorder Traversal). When the `op` does not apply to a given plan, it is left unchanged.
#[allow(dead_code)]
pub fn transform_down<F>(
    plan: Arc<dyn ExecutionPlan>,
    op: &F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>>,
{
    let plan_cloned = plan.clone();
    let after_op = match op(plan_cloned) {
        Some(value) => value,
        None => plan,
    };
    map_children(after_op.clone(), |plan: Arc<dyn ExecutionPlan>| {
        transform_down(plan, op)
    })
}

/// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
/// children and then itself(Postorder Traversal). When the `op` does not apply to a given plan, it is left unchanged.
pub fn transform_up<F>(
    plan: Arc<dyn ExecutionPlan>,
    op: &F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: Fn(Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>>,
{
    let after_op_children =
        map_children(plan, |plan: Arc<dyn ExecutionPlan>| transform_up(plan, op))?;

    let after_op_children_clone = after_op_children.clone();
    let new_plan = match op(after_op_children) {
        Some(value) => value,
        None => after_op_children_clone,
    };
    Ok(new_plan)
}
