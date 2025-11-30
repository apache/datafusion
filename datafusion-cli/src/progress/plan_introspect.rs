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

//! Plan introspection for extracting total size estimates

use datafusion::common::Statistics;
use datafusion::physical_plan::{
    visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor,
};
use std::sync::Arc;

/// Extracts total size estimates from a physical plan
pub struct PlanIntrospector {
    plan: Arc<dyn ExecutionPlan>,
}

impl PlanIntrospector {
    pub fn new(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan: Arc::clone(plan),
        }
    }

    /// Extract total bytes and rows from the plan's statistics
    pub fn get_totals(&self) -> PlanTotals {
        let mut visitor = TotalsVisitor::new();
        // Handle potential errors in plan visitation
        if let Err(e) = visit_execution_plan(self.plan.as_ref(), &mut visitor) {
            // Log the error but continue with default totals
            eprintln!(
                "Warning: Failed to analyze execution plan for progress tracking: {}",
                e
            );
        }
        visitor.into_totals()
    }
}

/// Accumulated totals from plan statistics
#[derive(Debug, Clone)]
pub struct PlanTotals {
    pub total_bytes: usize,
    pub total_rows: usize,
    pub has_exact_bytes: bool,
    pub has_exact_rows: bool,
    pub has_blocking_operators: bool,
}

impl PlanTotals {
    fn new() -> Self {
        Self {
            total_bytes: 0,
            total_rows: 0,
            has_exact_bytes: false,
            has_exact_rows: false,
            has_blocking_operators: false,
        }
    }
}

/// Visitor to collect statistics from plan nodes
struct TotalsVisitor {
    totals: PlanTotals,
}

impl TotalsVisitor {
    fn new() -> Self {
        Self {
            totals: PlanTotals::new(),
        }
    }

    fn into_totals(self) -> PlanTotals {
        self.totals
    }
}

impl ExecutionPlanVisitor for TotalsVisitor {
    type Error = datafusion::error::DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        // Focus on leaf nodes that actually read data
        if self.is_leaf_node(plan) {
            if let Ok(stats) = plan.partition_statistics(None) {
                self.accumulate_statistics(&stats);
            }
        }

        // Check for pipeline-breaking (blocking) operators
        if self.is_blocking_operator(plan) {
            self.totals.has_blocking_operators = true;
        }

        // Continue visiting children
        Ok(true)
    }
}

impl TotalsVisitor {
    /// Check if this plan node is a leaf node (no children)
    /// Leaf nodes are typically data sources that actually read data
    fn is_leaf_node(&self, plan: &dyn ExecutionPlan) -> bool {
        plan.children().is_empty()
    }

    /// Check if this plan node is a blocking/pipeline-breaking operator
    /// These operators consume all input before producing any output
    fn is_blocking_operator(&self, plan: &dyn ExecutionPlan) -> bool {
        let name = plan.name();

        // Check for known blocking operators using multiple strategies for robustness
        self.is_known_blocking_operator(name)
            || self.has_blocking_characteristics(plan)
            || self.uses_blocking_patterns(name)
    }

    /// Check for explicitly known blocking operators
    fn is_known_blocking_operator(&self, name: &str) -> bool {
        matches!(
            name,
            "SortExec" | "SortMergeJoinExec" | "HashJoinExec" | 
            "AggregateExec" | "WindowAggExec" | "GlobalLimitExec" |
            "SortPreservingMergeExec" | "CoalescePartitionsExec" |
            "SortPreservingRepartitionExec" | "RepartitionExec" |
            // Additional known blocking operators from real usage
            "SortPreservingMergeSort" | "PartialSortExec" | "TopKExec" |
            "UnionExec" | "CrossJoinExec" | "NestedLoopJoinExec" |
            "SymmetricHashJoinExec" | "BoundedWindowAggExec"
        )
    }

    /// Check if plan has characteristics that suggest blocking behavior
    fn has_blocking_characteristics(&self, plan: &dyn ExecutionPlan) -> bool {
        // Operators that require full input to determine output ordering are typically blocking
        // This is a heuristic that may need refinement
        let properties = plan.properties();

        // If an operator has a specific output ordering that differs significantly
        // from input ordering, it's likely blocking
        if let Some(output_ordering) = properties.output_ordering() {
            if !output_ordering.is_empty() {
                // If we have multiple children with potentially different orderings,
                // and we need to produce a specific order, likely blocking
                return plan.children().len() > 1;
            }
        }

        false
    }

    /// Check for blocking patterns in operator names
    fn uses_blocking_patterns(&self, name: &str) -> bool {
        // Pattern-based detection for operators we might not know about
        name.ends_with("SortExec")
            || name.ends_with("JoinExec")
            || name.ends_with("AggregateExec")
            || name.ends_with("WindowExec")
            || name.contains("Sort") && name.contains("Exec")
            || name.contains("Merge") && name.contains("Exec")
            || name.contains("Union") && name.contains("Exec")
    }

    /// Accumulate statistics from a plan node
    fn accumulate_statistics(&mut self, stats: &Statistics) {
        // Accumulate byte sizes
        if let Some(bytes) = stats.total_byte_size.get_value() {
            self.totals.total_bytes += *bytes;
        }
        if stats.total_byte_size.is_exact().unwrap_or(false) {
            self.totals.has_exact_bytes = true;
        }

        // Accumulate row counts
        if let Some(rows) = stats.num_rows.get_value() {
            self.totals.total_rows += *rows;
        }
        if stats.num_rows.is_exact().unwrap_or(false) {
            self.totals.has_exact_rows = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn test_plan_introspector() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let empty_exec = EmptyExec::new(schema);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(empty_exec);

        let introspector = PlanIntrospector::new(&plan);
        let totals = introspector.get_totals();

        // EmptyExec should have zero totals
        assert_eq!(totals.total_bytes, 0);
        assert_eq!(totals.total_rows, 0);
    }
}
