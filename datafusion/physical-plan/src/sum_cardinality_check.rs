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

//! Post-execution sum-cardinality validation.
//!
//! Provides [`validate_sum_cardinality`] which walks an executed plan tree and
//! verifies that every operator declaring [`CardinalityEffect::Sum`] produced
//! exactly the sum of its inputs' output rows.

use crate::ExecutionPlan;
use crate::execution_plan::CardinalityEffect;
use crate::visitor::{ExecutionPlanVisitor, visit_execution_plan};
use datafusion_common::{DataFusionError, Result, assert_eq_or_internal_err};

/// Visitor that checks sum-cardinality invariants after execution.
struct SumCardinalityVisitor;

impl ExecutionPlanVisitor for SumCardinalityVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, DataFusionError> {
        if matches!(plan.cardinality_effect(), CardinalityEffect::Sum) {
            check_sum_cardinality(plan)?;
        }
        Ok(true)
    }
}

/// Compare a node's recorded output row count against the sum of its children's
/// output row counts. Silently skips when metrics are unavailable on the node
/// or any of its children.
fn check_sum_cardinality(plan: &dyn ExecutionPlan) -> Result<()> {
    let Some(parent_metrics) = plan.metrics() else {
        return Ok(());
    };
    let Some(parent_rows) = parent_metrics.output_rows() else {
        return Ok(());
    };

    let mut child_total: usize = 0;
    for child in plan.children() {
        let Some(child_metrics) = child.metrics() else {
            return Ok(());
        };
        let Some(child_rows) = child_metrics.output_rows() else {
            return Ok(());
        };
        child_total = child_total.saturating_add(child_rows);
    }

    assert_eq_or_internal_err!(
        parent_rows,
        child_total,
        "Sum cardinality violation in {}",
        plan.name()
    );

    Ok(())
}

/// Walk the execution plan tree and verify that every operator declaring
/// [`CardinalityEffect::Sum`] produced exactly as many output rows as the sum
/// of its children's output rows, based on post-execution metrics.
///
/// Nodes are silently skipped when:
/// - They do not declare `CardinalityEffect::Sum`
/// - Metrics are unavailable on the node or any of its children
///
/// Returns `Err(DataFusionError::Internal)` on the first violation found.
pub fn validate_sum_cardinality(plan: &dyn ExecutionPlan) -> Result<()> {
    let mut visitor = SumCardinalityVisitor;
    visit_execution_plan(plan, &mut visitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plan::{Boundedness, EmissionType};
    use crate::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use crate::union::UnionExec;
    use crate::{DisplayAs, DisplayFormatType, PlanProperties};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_physical_expr::EquivalenceProperties;
    use std::fmt;
    use std::sync::Arc;

    /// Mock execution plan used to drive the helper and visitor without
    /// actually executing anything. Each instance optionally records an
    /// `output_rows` metric so we can stage matched / mismatched cardinality
    /// scenarios.
    #[derive(Debug)]
    struct MockExec {
        name: &'static str,
        children: Vec<Arc<dyn ExecutionPlan>>,
        mock_output_rows: Option<usize>,
        metrics: ExecutionPlanMetricsSet,
        cache: Arc<PlanProperties>,
    }

    impl MockExec {
        fn new(
            name: &'static str,
            children: Vec<Arc<dyn ExecutionPlan>>,
            mock_output_rows: Option<usize>,
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
                mock_output_rows,
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
    fn test_check_passes_when_sum_matches() {
        let child_a: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("a", vec![], Some(60)));
        let child_b: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("b", vec![], Some(40)));
        let parent = MockExec::new("union_like", vec![child_a, child_b], Some(100));

        assert!(check_sum_cardinality(&parent).is_ok());
    }

    #[test]
    fn test_check_violation_when_sum_mismatched() {
        let child_a: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("a", vec![], Some(60)));
        let child_b: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("b", vec![], Some(40)));
        let parent = MockExec::new("BadUnion", vec![child_a, child_b], Some(90));

        let err = check_sum_cardinality(&parent).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Sum cardinality violation"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("BadUnion"), "unexpected error: {msg}");
        assert!(msg.contains("90"), "unexpected error: {msg}");
        assert!(msg.contains("100"), "unexpected error: {msg}");
    }

    #[test]
    fn test_skips_when_parent_metrics_missing() {
        // Parent has no metrics — should be skipped
        let child_a: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("a", vec![], Some(60)));
        let child_b: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("b", vec![], Some(40)));
        let parent = MockExec::new("no_metrics", vec![child_a, child_b], None);

        assert!(check_sum_cardinality(&parent).is_ok());
    }

    #[test]
    fn test_skips_when_any_child_metrics_missing() {
        // One child has no metrics — should be skipped even though parent is set
        let child_a: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("a", vec![], Some(60)));
        let child_b: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new("b", vec![], None));
        let parent = MockExec::new("partial_metrics", vec![child_a, child_b], Some(100));

        assert!(check_sum_cardinality(&parent).is_ok());
    }

    #[test]
    fn test_visitor_skips_unrelated_nodes() {
        // Even when child counts don't sum to the parent, the visitor should
        // ignore nodes that are neither UnionExec nor InterleaveExec.
        let child_a: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("a", vec![], Some(60)));
        let child_b: Arc<dyn ExecutionPlan> =
            Arc::new(MockExec::new("b", vec![], Some(40)));
        let parent: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new(
            "not_a_union",
            vec![child_a, child_b],
            Some(7),
        ));

        assert!(validate_sum_cardinality(parent.as_ref()).is_ok());
    }

    #[test]
    fn test_visitor_recognises_real_union_exec() {
        // An unexecuted UnionExec has no recorded output_rows, so the visitor
        // must hit the missing-metrics skip path rather than erroring.
        let child_a: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new("a", vec![], None));
        let child_b: Arc<dyn ExecutionPlan> = Arc::new(MockExec::new("b", vec![], None));
        let union = UnionExec::try_new(vec![child_a, child_b]).unwrap();

        assert!(validate_sum_cardinality(union.as_ref()).is_ok());
    }
}
