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

//! Collection of testing utility functions that are leveraged by the query optimizer rules

use std::sync::Arc;

use std::any::Any;
use std::fmt::Formatter;

use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{JoinFilter, JoinOn};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::memory::MemoryExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::windows::{create_window_expr, BoundedWindowAggExec};
use datafusion_physical_plan::{InputOrderMode, Partitioning};

use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, LexRequirement};
use datafusion_physical_plan::ExecutionPlan;

use datafusion_physical_plan::tree_node::PlanContext;
use datafusion_physical_plan::{
    displayable, DisplayAs, DisplayFormatType, PlanProperties,
};

pub fn sort_merge_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_on: &JoinOn,
    join_type: &JoinType,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        SortMergeJoinExec::try_new(
            left,
            right,
            join_on.clone(),
            None,
            *join_type,
            vec![SortOptions::default(); join_on.len()],
            false,
        )
        .unwrap(),
    )
}

/// make PhysicalSortExpr with default options
pub fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
    sort_expr_options(name, schema, SortOptions::default())
}

/// PhysicalSortExpr with specified options
pub fn sort_expr_options(
    name: &str,
    schema: &Schema,
    options: SortOptions,
) -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: col(name, schema).unwrap(),
        options,
    }
}

pub fn coalesce_partitions_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalescePartitionsExec::new(input))
}

pub fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(MemoryExec::try_new(&[vec![]], Arc::clone(schema), None).unwrap())
}

pub fn hash_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    filter: Option<JoinFilter>,
    join_type: &JoinType,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(HashJoinExec::try_new(
        left,
        right,
        on,
        filter,
        join_type,
        None,
        PartitionMode::Partitioned,
        true,
    )?))
}

pub fn bounded_window_exec(
    col_name: &str,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs: LexOrdering = sort_exprs.into_iter().collect();
    let schema = input.schema();

    Arc::new(
        BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunctionDefinition::AggregateUDF(count_udaf()),
                "count".to_owned(),
                &[col(col_name, &schema).unwrap()],
                &[],
                sort_exprs.as_ref(),
                Arc::new(WindowFrame::new(Some(false))),
                schema.as_ref(),
                false,
            )
            .unwrap()],
            Arc::clone(&input),
            vec![],
            InputOrderMode::Sorted,
        )
        .unwrap(),
    )
}

pub fn filter_exec(
    predicate: Arc<dyn PhysicalExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(FilterExec::try_new(predicate, input).unwrap())
}

pub fn sort_preserving_merge_exec(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
}

pub fn union_exec(input: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
    Arc::new(UnionExec::new(input))
}

pub fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    global_limit_exec(local_limit_exec(input))
}

pub fn local_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(LocalLimitExec::new(input, 100))
}

pub fn global_limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(input, 0, Some(100)))
}

pub fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap())
}

pub fn spr_repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10))
            .unwrap()
            .with_preserve_order(),
    )
}

pub fn aggregate_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            PhysicalGroupBy::default(),
            vec![],
            vec![],
            input,
            schema,
        )
        .unwrap(),
    )
}

pub fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(CoalesceBatchesExec::new(input, 128))
}

pub fn sort_exec(
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
    input: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new(SortExec::new(sort_exprs, input))
}

/// A test [`ExecutionPlan`] whose requirements can be configured.
#[derive(Debug)]
pub struct RequirementsTestExec {
    required_input_ordering: LexOrdering,
    maintains_input_order: bool,
    input: Arc<dyn ExecutionPlan>,
}

impl RequirementsTestExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            required_input_ordering: LexOrdering::default(),
            maintains_input_order: true,
            input,
        }
    }

    /// sets the required input ordering
    pub fn with_required_input_ordering(
        mut self,
        required_input_ordering: LexOrdering,
    ) -> Self {
        self.required_input_ordering = required_input_ordering;
        self
    }

    /// set the maintains_input_order flag
    pub fn with_maintains_input_order(mut self, maintains_input_order: bool) -> Self {
        self.maintains_input_order = maintains_input_order;
        self
    }

    /// returns this ExecutionPlan as an `Arc<dyn ExecutionPlan>`
    pub fn into_arc(self) -> Arc<dyn ExecutionPlan> {
        Arc::new(self)
    }
}

impl DisplayAs for RequirementsTestExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RequiredInputOrderingExec")
    }
}

impl ExecutionPlan for RequirementsTestExec {
    fn name(&self) -> &str {
        "RequiredInputOrderingExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        let requirement = LexRequirement::from(self.required_input_ordering.clone());
        vec![Some(requirement)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.maintains_input_order]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(RequirementsTestExec::new(Arc::clone(&children[0]))
            .with_required_input_ordering(self.required_input_ordering.clone())
            .with_maintains_input_order(self.maintains_input_order)
            .into_arc())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("Test exec does not support execution")
    }
}

/// A [`PlanContext`] object is susceptible to being left in an inconsistent state after
/// untested mutable operations. It is crucial that there be no discrepancies between a plan
/// associated with the root node and the plan generated after traversing all nodes
/// within the [`PlanContext`] tree. In addition to verifying the plans resulting from optimizer
/// rules, it is essential to ensure that the overall tree structure corresponds with the plans
/// contained within the node contexts.
/// TODO: Once [`ExecutionPlan`] implements [`PartialEq`], string comparisons should be
/// replaced with direct plan equality checks.
pub fn check_integrity<T: Clone>(context: PlanContext<T>) -> Result<PlanContext<T>> {
    context
        .transform_up(|node| {
            let children_plans = node.plan.children();
            assert_eq!(node.children.len(), children_plans.len());
            for (child_plan, child_node) in
                children_plans.iter().zip(node.children.iter())
            {
                assert_eq!(
                    displayable(child_plan.as_ref()).one_line().to_string(),
                    displayable(child_node.plan.as_ref()).one_line().to_string()
                );
            }
            Ok(Transformed::no(node))
        })
        .data()
}
