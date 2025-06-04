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

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_physical_plan::execution_plan::EmissionType;
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// WrapLeaves is a PhysicalOptimizerRule that finds every
/// pipeline‐breaking node (emission_type == Final) and then
/// wraps all of its leaf children in YieldStreamExec.
pub struct WrapLeaves {}

impl WrapLeaves {
    pub fn new() -> Self {
        Self {}
    }

    /// This function is called on every plan node during transform_down().
    /// If the node is a leaf (no children), we wrap it in a new YieldStreamExec
    /// and stop recursing further under that branch (TreeNodeRecursion::Jump).
    fn wrap_leaves(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if plan.children().is_empty() {
            // Leaf: wrap it in YieldStreamExec, and do not descend further
            let wrapped = Arc::new(YieldStreamExec::new(plan));
            Ok(Transformed::new(
                wrapped,
                /* changed */ true,
                TreeNodeRecursion::Jump,
            ))
        } else {
            // Not a leaf: leave unchanged and keep recursing
            Ok(Transformed::no(plan))
        }
    }

    /// This function is called on every plan node during transform_down().
    ///
    /// If this node itself is a pipeline breaker (emission_type == Final),
    /// we perform a second pass of transform_down with wrap_leaves. Then we
    /// set TreeNodeRecursion::Jump so that we do not descend any deeper under
    /// this subtree (we’ve already wrapped its leaves).
    fn wrap_leaves_of_pipeline_breakers(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let is_pipeline_breaker = plan.properties().emission_type == EmissionType::Final;
        if is_pipeline_breaker {
            // Transform all leaf descendants of this node by calling wrap_leaves
            let mut transformed = plan.transform_down(Self::wrap_leaves)?;
            // Once we’ve handled the leaves of this subtree, we skip deeper recursion
            transformed.tnr = TreeNodeRecursion::Jump;
            Ok(transformed)
        } else {
            // Not a pipeline breaker: do nothing here, let transform_down recurse
            Ok(Transformed::no(plan))
        }
    }
}

impl Default for WrapLeaves {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for WrapLeaves {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrapLeaves").finish()
    }
}

impl PhysicalOptimizerRule for WrapLeaves {
    fn name(&self) -> &str {
        "wrap_leaves"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.enable_add_yield_for_pipeline_break {
            // We run a top‐level transform_down: for every node, call wrap_leaves_of_pipeline_breakers.
            // If a node is a pipeline breaker, we then wrap all of its leaf children in YieldStreamExec.
            plan.transform_down(Self::wrap_leaves_of_pipeline_breakers)
                .map(|t| t.data)
        } else {
            Ok(plan)
        }
    }

    fn schema_check(&self) -> bool {
        // Wrapping a leaf in YieldStreamExec preserves the schema, so we’re fine
        true
    }
}
