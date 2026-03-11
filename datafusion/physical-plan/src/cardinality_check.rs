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

//! Post-execution cardinality validation.
//!
//! Provides [`validate_cardinality_effect`] which walks an executed plan tree
//! and verifies that every operator declaring [`CardinalityEffect::Equal`]
//! produced exactly as many output rows as its input.

use crate::ExecutionPlan;
use crate::execution_plan::CardinalityEffect;
use crate::visitor::{ExecutionPlanVisitor, visit_execution_plan};
use datafusion_common::{DataFusionError, Result};

/// Visitor that checks cardinality invariants after execution.
struct CardinalityCheckVisitor;

impl ExecutionPlanVisitor for CardinalityCheckVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, DataFusionError> {
        if !matches!(plan.cardinality_effect(), CardinalityEffect::Equal) {
            return Ok(true);
        }

        let children = plan.children();
        if children.len() != 1 {
            return Ok(true);
        }

        // Operators with a fetch limit may legitimately produce fewer rows
        // even if they declare CardinalityEffect::Equal (e.g. CoalesceBatchesExec).
        if plan.fetch().is_some() {
            return Ok(true);
        }

        let child = children[0];

        let parent_metrics = match plan.metrics() {
            Some(m) => m,
            None => return Ok(true),
        };
        let child_metrics = match child.metrics() {
            Some(m) => m,
            None => return Ok(true),
        };

        let parent_rows = match parent_metrics.output_rows() {
            Some(r) => r,
            None => return Ok(true),
        };
        let child_rows = match child_metrics.output_rows() {
            Some(r) => r,
            None => return Ok(true),
        };

        if parent_rows != child_rows {
            return Err(DataFusionError::Internal(format!(
                "CardinalityEffect::Equal violation in {}: \
                 operator produced {parent_rows} output rows \
                 but its input produced {child_rows} rows",
                plan.name(),
            )));
        }

        Ok(true)
    }
}

/// Walk the execution plan tree and verify that every operator declaring
/// [`CardinalityEffect::Equal`] produced exactly as many output rows as its
/// single child input, based on post-execution metrics.
///
/// Nodes are silently skipped when:
/// - They do not declare `CardinalityEffect::Equal`
/// - They are not unary (zero or multiple children)
/// - They have a `fetch` limit set
/// - Metrics are unavailable on either the node or its child
///
/// Returns `Err(DataFusionError::Internal)` on the first violation found.
pub fn validate_cardinality_effect(plan: &dyn ExecutionPlan) -> Result<()> {
    let mut visitor = CardinalityCheckVisitor;
    visit_execution_plan(plan, &mut visitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plan::{Boundedness, EmissionType};
    use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use crate::{DisplayAs, DisplayFormatType, PlanProperties};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_physical_expr::EquivalenceProperties;
    use std::any::Any;
    use std::fmt;
    use std::sync::Arc;

    /// A mock execution plan for testing cardinality validation.
    #[derive(Debug)]
    struct MockExec {
        name: &'static str,
        children: Vec<Arc<dyn ExecutionPlan>>,
        effect: CardinalityEffect,
        mock_output_rows: Option<usize>,
        fetch: Option<usize>,
        metrics: ExecutionPlanMetricsSet,
        cache: Arc<PlanProperties>,
    }

    impl MockExec {
        fn new(
            name: &'static str,
            children: Vec<Arc<dyn ExecutionPlan>>,
            effect: CardinalityEffect,
            mock_output_rows: Option<usize>,
            fetch: Option<usize>,
        ) -> Self {
            let schema = test_schema();
            let cache = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                crate::Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            ));
            let metrics = ExecutionPlanMetricsSet::new();
            if let Some(rows) = mock_output_rows {
                let counter = MetricBuilder::new(&metrics).output_rows(0);
                counter.add(rows);
            }
            Self {
                name,
                children,
                effect,
                mock_output_rows,
                fetch,
                metrics,
                cache,
            }
        }
    }

    impl DisplayAs for MockExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockExec: {}", self.name)
        }
    }

    impl ExecutionPlan for MockExec {
        fn name(&self) -> &'static str {
            self.name
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &dyn crate::PhysicalExpr,
            )
                -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!("not needed for tests")
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion_execution::TaskContext>,
        ) -> Result<crate::SendableRecordBatchStream> {
            unimplemented!("not needed for tests")
        }

        fn cardinality_effect(&self) -> CardinalityEffect {
            self.effect
        }

        fn fetch(&self) -> Option<usize> {
            self.fetch
        }

        fn metrics(&self) -> Option<crate::metrics::MetricsSet> {
            if self.mock_output_rows.is_some() {
                Some(self.metrics.clone_inner())
            } else {
                None
            }
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    #[test]
    fn test_equal_cardinality_passes() {
        // Parent and child both produce 100 rows
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "parent",
            vec![child],
            CardinalityEffect::Equal,
            Some(100),
            None,
        ));

        assert!(validate_cardinality_effect(parent.as_ref()).is_ok());
    }

    #[test]
    fn test_equal_cardinality_violation() {
        // Parent produces 90 rows but child produced 100
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "TestRepartition",
            vec![child],
            CardinalityEffect::Equal,
            Some(90),
            None,
        ));

        let err = validate_cardinality_effect(parent.as_ref()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("CardinalityEffect::Equal violation"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("TestRepartition"), "unexpected error: {msg}");
        assert!(msg.contains("90"), "unexpected error: {msg}");
        assert!(msg.contains("100"), "unexpected error: {msg}");
    }

    #[test]
    fn test_skips_operator_with_fetch() {
        // Operator declares Equal but has fetch set — should be skipped
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "parent_with_fetch",
            vec![child],
            CardinalityEffect::Equal,
            Some(50),
            Some(50),
        ));

        assert!(validate_cardinality_effect(parent.as_ref()).is_ok());
    }

    #[test]
    fn test_skips_operator_without_metrics() {
        // Parent declares Equal but has no metrics — should be skipped
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "no_metrics_parent",
            vec![child],
            CardinalityEffect::Equal,
            None, // no metrics
            None,
        ));

        assert!(validate_cardinality_effect(parent.as_ref()).is_ok());
    }

    #[test]
    fn test_skips_non_equal_operator() {
        // LowerEqual operator with fewer output rows — should not trigger
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "filter",
            vec![child],
            CardinalityEffect::LowerEqual,
            Some(50),
            None,
        ));

        assert!(validate_cardinality_effect(parent.as_ref()).is_ok());
    }

    #[test]
    fn test_deep_tree_catches_nested_violation() {
        // Build: grandparent(Equal, 100) -> parent(Equal, 90!) -> child(Unknown, 100)
        // The violation is at the parent level (90 != 100)
        let child: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "child",
            vec![],
            CardinalityEffect::Unknown,
            Some(100),
            None,
        ));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "BadRepartition",
            vec![child],
            CardinalityEffect::Equal,
            Some(90),
            None,
        ));
        let grandparent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "projection",
            vec![parent],
            CardinalityEffect::Equal,
            Some(90), // matches its child (parent), so this is fine
            None,
        ));

        let err = validate_cardinality_effect(grandparent.as_ref()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("BadRepartition"),
            "should identify the violating operator: {msg}"
        );
    }
}
