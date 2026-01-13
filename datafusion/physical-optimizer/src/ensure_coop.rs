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
use datafusion_physical_plan::execution_plan::{EvaluationType, SchedulingType};

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
        use std::cell::RefCell;

        let ancestry_stack = RefCell::new(Vec::<(SchedulingType, EvaluationType)>::new());

        plan.transform_down_up(
            // Down phase: Push parent properties <SchedulingType, EvaluationType> into the stack
            |plan| {
                let props = plan.properties();
                ancestry_stack
                    .borrow_mut()
                    .push((props.scheduling_type, props.evaluation_type));
                Ok(Transformed::no(plan))
            },
            // Up phase: Wrap nodes with CooperativeExec if needed
            |plan| {
                ancestry_stack.borrow_mut().pop();

                let props = plan.properties();
                let is_cooperative = props.scheduling_type == SchedulingType::Cooperative;
                let is_leaf = plan.children().is_empty();
                let is_exchange = props.evaluation_type == EvaluationType::Eager;

                let mut is_under_cooperative_context = false;
                for (scheduling_type, evaluation_type) in
                    ancestry_stack.borrow().iter().rev()
                {
                    // If nearest ancestor is cooperative, we are under a cooperative context
                    if *scheduling_type == SchedulingType::Cooperative {
                        is_under_cooperative_context = true;
                        break;
                    // If nearest ancestor is eager, the cooperative context will be reset
                    } else if *evaluation_type == EvaluationType::Eager {
                        is_under_cooperative_context = false;
                        break;
                    }
                }

                // Wrap if:
                // 1. Node is a leaf or exchange point
                // 2. Node is not already cooperative
                // 3. Not under any Cooperative context
                if (is_leaf || is_exchange)
                    && !is_cooperative
                    && !is_under_cooperative_context
                {
                    return Ok(Transformed::yes(Arc::new(CooperativeExec::new(plan))));
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
    async fn test_eager_evaluation_resets_cooperative_context() {
        // Test that cooperative context is reset when encountering an eager evaluation boundary.
        use arrow::datatypes::Schema;
        use datafusion_common::{Result, internal_err};
        use datafusion_execution::TaskContext;
        use datafusion_physical_expr::EquivalenceProperties;
        use datafusion_physical_plan::{
            DisplayAs, DisplayFormatType, Partitioning, PlanProperties,
            SendableRecordBatchStream,
            execution_plan::{Boundedness, EmissionType},
        };
        use std::any::Any;
        use std::fmt::Formatter;

        #[derive(Debug)]
        struct DummyExec {
            name: String,
            input: Arc<dyn ExecutionPlan>,
            scheduling_type: SchedulingType,
            evaluation_type: EvaluationType,
            properties: Arc<PlanProperties>,
        }

        impl DummyExec {
            fn new(
                name: &str,
                input: Arc<dyn ExecutionPlan>,
                scheduling_type: SchedulingType,
                evaluation_type: EvaluationType,
            ) -> Self {
                let properties = PlanProperties::new(
                    EquivalenceProperties::new(Arc::new(Schema::empty())),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )
                .with_scheduling_type(scheduling_type)
                .with_evaluation_type(evaluation_type);

                Self {
                    name: name.to_string(),
                    input,
                    scheduling_type,
                    evaluation_type,
                    properties: Arc::new(properties),
                }
            }
        }

        impl DisplayAs for DummyExec {
            fn fmt_as(
                &self,
                _: DisplayFormatType,
                f: &mut Formatter,
            ) -> std::fmt::Result {
                write!(f, "{}", self.name)
            }
        }

        impl ExecutionPlan for DummyExec {
            fn name(&self) -> &str {
                &self.name
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                &self.properties
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![&self.input]
            }
            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(DummyExec::new(
                    &self.name,
                    Arc::clone(&children[0]),
                    self.scheduling_type,
                    self.evaluation_type,
                )))
            }
            fn execute(
                &self,
                _: usize,
                _: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                internal_err!("DummyExec does not support execution")
            }
        }

        // Build a plan similar to the original test:
        // scan -> exch1(NonCoop,Eager) -> CoopExec -> filter -> exch2(Coop,Eager) -> filter
        let scan = scan_partitioned(1);
        let exch1 = Arc::new(DummyExec::new(
            "exch1",
            scan,
            SchedulingType::NonCooperative,
            EvaluationType::Eager,
        ));
        let coop = Arc::new(CooperativeExec::new(exch1));
        let filter1 = Arc::new(DummyExec::new(
            "filter1",
            coop,
            SchedulingType::NonCooperative,
            EvaluationType::Lazy,
        ));
        let exch2 = Arc::new(DummyExec::new(
            "exch2",
            filter1,
            SchedulingType::Cooperative,
            EvaluationType::Eager,
        ));
        let filter2 = Arc::new(DummyExec::new(
            "filter2",
            exch2,
            SchedulingType::NonCooperative,
            EvaluationType::Lazy,
        ));

        let config = ConfigOptions::new();
        let optimized = EnsureCooperative::new().optimize(filter2, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();

        // Expected wrapping:
        // - Scan (leaf) gets wrapped
        // - exch1 (eager+noncoop) keeps its manual CooperativeExec wrapper
        // - filter1 is protected by exch2's cooperative context, no extra wrap
        // - exch2 (already Cooperative) does NOT get wrapped
        // - filter2 (not leaf or eager) does NOT get wrapped
        assert_eq!(
            display.matches("CooperativeExec").count(),
            2,
            "Should have 2 CooperativeExec: one wrapping scan, one wrapping exch1"
        );

        assert_snapshot!(display, @r"
        filter2
          exch2
            filter1
              CooperativeExec
                exch1
                  CooperativeExec
                    DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }
}
