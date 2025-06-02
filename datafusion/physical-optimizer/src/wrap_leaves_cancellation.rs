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
use datafusion_common::Result;
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// `WrapLeaves` is a `PhysicalOptimizerRule` that traverses a physical plan
/// and, for every operator whose `emission_type` is `Final`, wraps its direct
/// children inside a `YieldStreamExec`. This ensures that pipeline‐breaking
/// operators (i.e. those with `Final` emission) have a “yield point” immediately
/// upstream, without having to wait until the leaves.
pub struct WrapLeaves {}

impl WrapLeaves {
    /// Create a new instance of the WrapLeaves rule.
    pub fn new() -> Self {
        Self {}
    }

    /// Recursively walk the plan:
    /// - If `plan.children_any().is_empty()`, it’s a leaf, so wrap it.
    /// - Otherwise, recurse into its children, rebuild the node with
    ///   `with_new_children_any(...)`, and return that.
    #[allow(clippy::only_used_in_recursion)]
    fn wrap_recursive(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let children = plan.children();
        if children.is_empty() {
            // Leaf node: wrap it in `YieldStreamExec`
            let wrapped = Arc::new(YieldStreamExec::new(plan));
            Ok(wrapped)
        } else {
            // Non-leaf: first process all children recursively
            let mut new_children = Vec::with_capacity(children.len());
            for child in children {
                let wrapped_child = self.wrap_recursive(Arc::clone(child))?;
                new_children.push(wrapped_child);
            }
            // Rebuild this node with the new children
            let new_plan = plan.with_new_children(new_children)?;
            Ok(new_plan)
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

    /// Apply the rule by calling `wrap_recursive` on the root plan.
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.wrap_recursive(plan)
    }

    /// Since we only add `YieldStreamExec` wrappers (which preserve schema), schema_check remains true.
    fn schema_check(&self) -> bool {
        true
    }
}
