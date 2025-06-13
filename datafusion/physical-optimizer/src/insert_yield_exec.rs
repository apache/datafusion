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

//! The `InsertYieldExec` optimizer rule inspects the physical plan to look for
//! tight-looping operators and inserts explicit yielding mechanisms (whether
//! as a separate operator, or via a yielding variant) at leaf nodes to make
//! the plan cancellation friendly.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::PhysicalOptimizerRule;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_physical_plan::yield_stream::YieldStreamExec;
use datafusion_physical_plan::ExecutionPlan;

/// `InsertYieldExec` is a [`PhysicalOptimizerRule`] that finds every leaf node in
/// the plan, and replaces it with a variant that cooperatively yields
/// either using the its yielding variant given by `with_cooperative_yields`,
/// or, if none exists, by inserting a [`YieldStreamExec`] operator as a parent.
pub struct InsertYieldExec {}

impl InsertYieldExec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for InsertYieldExec {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for InsertYieldExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertYieldExec").finish()
    }
}

impl PhysicalOptimizerRule for InsertYieldExec {
    fn name(&self) -> &str {
        "insert_yield_exec"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Only activate if user has configured a non-zero yield frequency.
        let yield_period = config.optimizer.yield_period;
        if yield_period != 0 {
            plan.transform_down(|plan| {
                if !plan.children().is_empty() {
                    // Not a leaf, keep recursing down.
                    return Ok(Transformed::no(plan));
                }
                let new_plan = Arc::clone(&plan)
                    .with_cooperative_yields()
                    .unwrap_or_else(|| {
                        // Otherwise, insert a `YieldStreamExec` to enforce periodic yielding.
                        Arc::new(YieldStreamExec::new(plan, yield_period))
                    });
                Ok(Transformed::new(new_plan, true, TreeNodeRecursion::Jump))
            })
            .map(|t| t.data)
        } else {
            Ok(plan)
        }
    }

    fn schema_check(&self) -> bool {
        // Wrapping a leaf in YieldStreamExec preserves the schema, so it is safe.
        true
    }
}
