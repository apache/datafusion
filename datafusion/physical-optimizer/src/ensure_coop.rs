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

use crate::PhysicalOptimizerRule;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::execution_plan::{EvaluationType};

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

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use std::cell::Cell;

        // Track depth: 0 means not under any CooperativeExec
        // Using Cell to allow interior mutability from multiple closures
        let coop_depth = Cell::new(0usize);

        plan.transform_down_up(
            // Down phase: Track when entering CooperativeExec subtrees
            |plan| {
                if plan.as_any().downcast_ref::<CooperativeExec>().is_some() {
                    coop_depth.set(coop_depth.get() + 1);
                }
                Ok(Transformed::no(plan))
            },
            // Up phase: Wrap nodes with CooperativeExec if needed, then restore depth
            |plan| {
                let is_coop_node =
                    plan.as_any().downcast_ref::<CooperativeExec>().is_some();
                let is_leaf = plan.children().is_empty();
                let is_exchange =
                    plan.properties().evaluation_type == EvaluationType::Eager;

                // Wrap if:
                // 1. Node is a leaf or exchange point
                // 2. Node is not already a CooperativeExec
                // 3. Not under any CooperativeExec (depth == 0)
                if (is_leaf || is_exchange) && !is_coop_node && coop_depth.get() == 0 {
                    // Note: We don't decrement depth here because this node
                    // wasn't a CooperativeExec before wrapping
                    return Ok(Transformed::yes(Arc::new(CooperativeExec::new(plan))));
                }

                // Restore depth when leaving a CooperativeExec node
                if is_coop_node {
                    coop_depth.set(coop_depth.get() - 1);
                }

                Ok(Transformed::no(plan))
            },
        )
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
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_plan::execution_plan::EvaluationType;
    use datafusion_physical_plan::{displayable, test::scan_partitioned};
    use insta::assert_snapshot;

    #[tokio::test]
    async fn test_cooperative_exec_for_custom_exec() {
        let test_custom_exec = scan_partitioned(1);
        let config = ConfigOptions::new();
        let optimized = EnsureCooperative::new()
            .optimize(test_custom_exec, &config)
            .unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        // Use insta snapshot to ensure full plan structure
        assert_snapshot!(display, @r"
        CooperativeExec
          DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_optimizer_is_idempotent() {
        // Comprehensive idempotency test: verify f(f(...f(x))) = f(x)
        // This test covers:
        // 1. Multiple runs on unwrapped plan
        // 2. Multiple runs on already-wrapped plan
        // 3. No accumulation of CooperativeExec nodes

        let config = ConfigOptions::new();
        let rule = EnsureCooperative::new();

        // Test 1: Start with unwrapped plan, run multiple times
        let unwrapped_plan = scan_partitioned(1);
        let mut current = unwrapped_plan;
        let mut stable_result = String::new();

        for run in 1..=5 {
            current = rule.optimize(current, &config).unwrap();
            let display = displayable(current.as_ref()).indent(true).to_string();

            if run == 1 {
                stable_result = display.clone();
                assert_eq!(display.matches("CooperativeExec").count(), 1);
            } else {
                assert_eq!(
                    display, stable_result,
                    "Run {run} should match run 1 (idempotent)"
                );
                assert_eq!(
                    display.matches("CooperativeExec").count(),
                    1,
                    "Should always have exactly 1 CooperativeExec, not accumulate"
                );
            }
        }

        // Test 2: Start with already-wrapped plan, verify no double wrapping
        let pre_wrapped = Arc::new(CooperativeExec::new(scan_partitioned(1)));
        let result = rule.optimize(pre_wrapped, &config).unwrap();
        let display = displayable(result.as_ref()).indent(true).to_string();

        assert_eq!(
            display.matches("CooperativeExec").count(),
            1,
            "Should not double-wrap already cooperative plans"
        );
        assert_eq!(
            display, stable_result,
            "Pre-wrapped plan should produce same result as unwrapped after optimization"
        );
    }

    #[tokio::test]
    async fn test_selective_wrapping() {
        // Test that wrapping is selective: only leaf/eager nodes, not intermediate nodes
        // Also verify depth tracking prevents double wrapping in subtrees
        use datafusion_physical_expr::expressions::lit;
        use datafusion_physical_plan::filter::FilterExec;

        let config = ConfigOptions::new();
        let rule = EnsureCooperative::new();

        // Case 1: Filter -> Scan (middle node should not be wrapped)
        let scan = scan_partitioned(1);
        let filter = Arc::new(FilterExec::try_new(lit(true), scan).unwrap());
        let optimized = rule.optimize(filter, &config).unwrap();
        let display = displayable(optimized.as_ref()).indent(true).to_string();

        assert_eq!(display.matches("CooperativeExec").count(), 1);
        assert!(display.contains("FilterExec"));

        // Case 2: Filter -> CoopExec -> Scan (depth tracking prevents double wrap)
        let scan2 = scan_partitioned(1);
        let wrapped_scan = Arc::new(CooperativeExec::new(scan2));
        let filter2 = Arc::new(FilterExec::try_new(lit(true), wrapped_scan).unwrap());
        let optimized2 = rule.optimize(filter2, &config).unwrap();
        let display2 = displayable(optimized2.as_ref()).indent(true).to_string();

        assert_eq!(display2.matches("CooperativeExec").count(), 1);
    }

    #[tokio::test]
    async fn test_multiple_leaf_nodes() {
        // When there are multiple leaf nodes, each should be wrapped separately
        use datafusion_physical_plan::union::UnionExec;

        let scan1 = scan_partitioned(1);
        let scan2 = scan_partitioned(1);
        let union = UnionExec::try_new(vec![scan1, scan2]).unwrap();

        let config = ConfigOptions::new();
        let optimized = EnsureCooperative::new()
            .optimize(union as Arc<dyn ExecutionPlan>, &config)
            .unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();

        // Each leaf should have its own CooperativeExec
        assert_eq!(
            display.matches("CooperativeExec").count(),
            2,
            "Each leaf node should be wrapped separately"
        );
        assert_eq!(
            display.matches("DataSourceExec").count(),
            2,
            "Both data sources should be present"
        );
    }

    #[tokio::test]
    async fn test_eager_exchange_nodes() {
        // Test eager evaluation nodes (exchange points like RepartitionExec)
        // Should wrap both the eager node and its leaf child, and be idempotent
        use datafusion_physical_plan::Partitioning;
        use datafusion_physical_plan::repartition::RepartitionExec;

        let scan = scan_partitioned(1);
        let repartition = Arc::new(
            RepartitionExec::try_new(scan, Partitioning::RoundRobinBatch(4)).unwrap(),
        );

        assert_eq!(
            repartition.properties().evaluation_type,
            EvaluationType::Eager
        );

        let config = ConfigOptions::new();
        let rule = EnsureCooperative::new();

        // First run: should wrap both eager node and leaf
        let first = rule.optimize(repartition, &config).unwrap();
        let first_display = displayable(first.as_ref()).indent(true).to_string();
        assert_eq!(first_display.matches("CooperativeExec").count(), 2);

        // Idempotency check
        let second = rule.optimize(Arc::clone(&first), &config).unwrap();
        let second_display = displayable(second.as_ref()).indent(true).to_string();
        assert_eq!(first_display, second_display);
    }
}
