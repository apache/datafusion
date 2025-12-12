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

//! The [`EnsureCooperative`] optimizer rule inspects the physical plan to find all
//! portions of the plan that will not yield cooperatively.
//! It will insert `CooperativeExec` nodes where appropriate to ensure execution plans
//! always yield cooperatively.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::{OptimizerContext, PhysicalOptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::execution_plan::{EvaluationType, SchedulingType};
use datafusion_physical_plan::ExecutionPlan;

/// `EnsureCooperative` is a [`PhysicalOptimizerRule`] that inspects the physical plan for
/// sub plans that do not participate in cooperative scheduling. The plan is subdivided into sub
/// plans on eager evaluation boundaries. Leaf nodes and eager evaluation roots are checked
/// to see if they participate in cooperative scheduling. Those that do no are wrapped in
/// a [`CooperativeExec`] parent.
pub struct EnsureCooperative {}

impl EnsureCooperative {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for EnsureCooperative {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for EnsureCooperative {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(self.name()).finish()
    }
}

impl PhysicalOptimizerRule for EnsureCooperative {
    fn name(&self) -> &str {
        "EnsureCooperative"
    }

    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let is_leaf = plan.children().is_empty();
            let is_exchange = plan.properties().evaluation_type == EvaluationType::Eager;
            if (is_leaf || is_exchange)
                && plan.properties().scheduling_type != SchedulingType::Cooperative
            {
                // Wrap non-cooperative leaves or eager evaluation roots in a cooperative exec to
                // ensure the plans they participate in are properly cooperative.
                Ok(Transformed::new(
                    Arc::new(CooperativeExec::new(Arc::clone(&plan))),
                    true,
                    TreeNodeRecursion::Continue,
                ))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .map(|t| t.data)
    }

    fn schema_check(&self) -> bool {
        // Wrapping a leaf in YieldStreamExec preserves the schema, so it is safe.
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_plan::{displayable, test::scan_partitioned};
    use insta::assert_snapshot;

    #[tokio::test]
    async fn test_cooperative_exec_for_custom_exec() {
        let test_custom_exec = scan_partitioned(1);
        let session_config = SessionConfig::new();
        let optimizer_context = OptimizerContext::new(session_config);
        let optimized = EnsureCooperative::new()
            .optimize_plan(test_custom_exec, &optimizer_context)
            .unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        // Use insta snapshot to ensure full plan structure
        assert_snapshot!(display, @r###"
            CooperativeExec
              DataSourceExec: partitions=1, partition_sizes=[1]
            "###);
    }
}
