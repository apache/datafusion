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
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// WrapLeaves is a PhysicalOptimizerRule that finds every *leaf* node in the
/// entire plan, and replaces it with a variant that can cooperatively yield
/// (either using its built‐in `with_cooperative_yields()` or, if none exists,
/// by wrapping it in a `YieldStreamExec` wrapper).
///
/// In contrast to the previous behavior (which only looked at “Final”/pipeline‐
/// breaking nodes), this modified rule simply wraps *every* leaf no matter what.
pub struct WrapLeaves {}

impl WrapLeaves {
    pub fn new() -> Self {
        Self {}
    }

    /// Called when we encounter any node during `transform_down()`.  If the node
    /// has no children, it is a leaf.  We check if it has a built‐in cooperative
    /// yield variant (`with_cooperative_yields()`); if so, we replace it with that.
    /// Otherwise, we wrap it in a `YieldStreamExec`.
    ///
    /// We then return `TreeNodeRecursion::Jump` so that we do not attempt to go
    /// deeper under this node (there are no children, anyway).
    fn wrap_leaves(
        plan: Arc<dyn ExecutionPlan>,
        yield_frequency: usize,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if plan.children().is_empty() {
            // This is a leaf.  Try to see if the plan itself has a cooperative‐yield variant.
            if let Some(coop_variant) = Arc::clone(&plan).with_cooperative_yields() {
                // Replace with the built‐in cooperative yield version.
                Ok(Transformed::new(
                    coop_variant,
                    /* changed= */ true,
                    TreeNodeRecursion::Jump,
                ))
            } else {
                // Otherwise wrap it in a YieldStreamExec to enforce periodic yielding.
                let wrapped = Arc::new(YieldStreamExec::new(plan, yield_frequency));
                Ok(Transformed::new(
                    wrapped,
                    /* changed= */ true,
                    TreeNodeRecursion::Jump,
                ))
            }
        } else {
            // Not a leaf: leave unchanged for now, keep recursing down.
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
        // Only activate if user has configured a nonzero yield frequency.
        if config.optimizer.yield_frequency_for_pipeline_break != 0 {
            let yield_frequency = config.optimizer.yield_frequency_for_pipeline_break;

            // We perform a single top‐level transform_down over the entire plan.
            // For each node encountered, we call `wrap_leaves`.  If the node is
            // a leaf, it will be replaced with a yielding variant (either its
            // built‐in cooperative version or an explicit YieldStreamExec).
            let new_plan = plan.transform_down(|node: Arc<dyn ExecutionPlan>| {
                Self::wrap_leaves(node, yield_frequency)
            })?;

            Ok(new_plan.data)
        } else {
            // If yield_frequency is zero, we do nothing.
            Ok(plan)
        }
    }

    fn schema_check(&self) -> bool {
        // Wrapping a leaf in YieldStreamExec preserves the schema, so it is safe.
        true
    }
}
