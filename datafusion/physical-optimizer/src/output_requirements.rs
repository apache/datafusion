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

//! The GlobalOrderRequire optimizer rule either:
//! - Adds an auxiliary `OutputRequirementExec` operator to keep track of global
//!   ordering and distribution requirement across rules, or
//! - Removes the auxiliary `OutputRequirementExec` operator from the physical plan.
//!   Since the `OutputRequirementExec` operator is only a helper operator, it
//!   shouldn't occur in the final plan (i.e. the executed plan).

use std::sync::Arc;

use crate::{OptimizerContext, PhysicalOptimizerRule};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Distribution;
use datafusion_physical_expr_common::sort_expr::OrderingRequirements;
use datafusion_physical_plan::execution_plan::Boundedness;
use datafusion_physical_plan::projection::{
    make_with_child, update_expr, update_ordering_requirement, ProjectionExec,
};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};

/// This rule either adds or removes [`OutputRequirements`]s to/from the physical
/// plan according to its `mode` attribute, which is set by the constructors
/// `new_add_mode` and `new_remove_mode`. With this rule, we can keep track of
/// the global requirements (ordering and distribution) across rules.
///
/// The primary use case of this node and rule is to specify and preserve the desired output
/// ordering and distribution the entire plan. When sending to a single client, a single partition may
/// be desirable, but when sending to a multi-partitioned writer, keeping multiple partitions may be
/// better.
#[derive(Debug)]
pub struct OutputRequirements {
    mode: RuleMode,
}

impl OutputRequirements {
    /// Create a new rule which works in `Add` mode; i.e. it simply adds a
    /// top-level [`OutputRequirementExec`] into the physical plan to keep track
    /// of global ordering and distribution requirements if there are any.
    /// Note that this rule should run at the beginning.
    pub fn new_add_mode() -> Self {
        Self {
            mode: RuleMode::Add,
        }
    }

    /// Create a new rule which works in `Remove` mode; i.e. it simply removes
    /// the top-level [`OutputRequirementExec`] from the physical plan if there is
    /// any. We do this because a `OutputRequirementExec` is an ancillary,
    /// non-executable operator whose sole purpose is to track global
    /// requirements during optimization. Therefore, a
    /// `OutputRequirementExec` should not appear in the final plan.
    pub fn new_remove_mode() -> Self {
        Self {
            mode: RuleMode::Remove,
        }
    }
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq, Hash)]
enum RuleMode {
    Add,
    Remove,
}

/// An ancillary, non-executable operator whose sole purpose is to track global
/// requirements during optimization. It imposes
/// - the ordering requirement in its `order_requirement` attribute.
/// - the distribution requirement in its `dist_requirement` attribute.
///
/// See [`OutputRequirements`] for more details
#[derive(Debug)]
pub struct OutputRequirementExec {
    input: Arc<dyn ExecutionPlan>,
    order_requirement: Option<OrderingRequirements>,
    dist_requirement: Distribution,
    cache: PlanProperties,
    fetch: Option<usize>,
}

impl OutputRequirementExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        requirements: Option<OrderingRequirements>,
        dist_requirement: Distribution,
        fetch: Option<usize>,
    ) -> Self {
        let cache = Self::compute_properties(&input, &fetch);
        Self {
            input,
            order_requirement: requirements,
            dist_requirement,
            cache,
            fetch,
        }
    }

    pub fn input(&self) -> Arc<dyn ExecutionPlan> {
        Arc::clone(&self.input)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        fetch: &Option<usize>,
    ) -> PlanProperties {
        let boundedness = if fetch.is_some() {
            Boundedness::Bounded
        } else {
            input.boundedness()
        };

        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.pipeline_behavior(),              // Pipeline Behavior
            boundedness,                            // Boundedness
        )
    }

    /// Get fetch
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for OutputRequirementExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let order_cols = self
                    .order_requirement
                    .as_ref()
                    .map(|reqs| reqs.first())
                    .map(|lex| {
                        let pairs: Vec<String> = lex
                            .iter()
                            .map(|req| {
                                let direction = req
                                    .options
                                    .as_ref()
                                    .map(
                                        |opt| if opt.descending { "desc" } else { "asc" },
                                    )
                                    .unwrap_or("unspecified");
                                format!("({}, {direction})", req.expr)
                            })
                            .collect();
                        format!("[{}]", pairs.join(", "))
                    })
                    .unwrap_or_else(|| "[]".to_string());

                write!(
                    f,
                    "OutputRequirementExec: order_by={}, dist_by={}",
                    order_cols, self.dist_requirement
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for OutputRequirementExec {
    fn name(&self) -> &'static str {
        "OutputRequirementExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![self.dist_requirement.clone()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![self.order_requirement.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            children.remove(0), // has a single child
            self.order_requirement.clone(),
            self.dist_requirement.clone(),
            self.fetch,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!();
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down:
        let proj_exprs = projection.expr();
        if proj_exprs.len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        let mut requirements = self.required_input_ordering().swap_remove(0);
        if let Some(reqs) = requirements {
            let mut updated_reqs = vec![];
            let (lexes, soft) = reqs.into_alternatives();
            for lex in lexes.into_iter() {
                let Some(updated_lex) = update_ordering_requirement(lex, proj_exprs)?
                else {
                    return Ok(None);
                };
                updated_reqs.push(updated_lex);
            }
            requirements = OrderingRequirements::new_alternatives(updated_reqs, soft);
        }

        let dist_req = match &self.required_input_distribution()[0] {
            Distribution::HashPartitioned(exprs) => {
                let mut updated_exprs = vec![];
                for expr in exprs {
                    let Some(new_expr) = update_expr(expr, projection.expr(), false)?
                    else {
                        return Ok(None);
                    };
                    updated_exprs.push(new_expr);
                }
                Distribution::HashPartitioned(updated_exprs)
            }
            dist => dist.clone(),
        };

        make_with_child(projection, &self.input()).map(|input| {
            let e = OutputRequirementExec::new(input, requirements, dist_req, self.fetch);
            Some(Arc::new(e) as _)
        })
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl PhysicalOptimizerRule for OutputRequirements {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.mode {
            RuleMode::Add => require_top_ordering(plan),
            RuleMode::Remove => plan
                .transform_up(|plan| {
                    if let Some(sort_req) =
                        plan.as_any().downcast_ref::<OutputRequirementExec>()
                    {
                        Ok(Transformed::yes(sort_req.input()))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                })
                .data(),
        }
    }

    fn name(&self) -> &str {
        "OutputRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This functions adds ancillary `OutputRequirementExec` to the physical plan, so that
/// global requirements are not lost during optimization.
fn require_top_ordering(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let (new_plan, is_changed) = require_top_ordering_helper(plan)?;
    if is_changed {
        Ok(new_plan)
    } else {
        // Add `OutputRequirementExec` to the top, with no specified ordering and distribution requirement.
        Ok(Arc::new(OutputRequirementExec::new(
            new_plan,
            // there is no ordering requirement
            None,
            Distribution::UnspecifiedDistribution,
            None,
        )) as _)
    }
}

/// Helper function that adds an ancillary `OutputRequirementExec` to the given plan.
/// First entry in the tuple is resulting plan, second entry indicates whether any
/// `OutputRequirementExec` is added to the plan.
fn require_top_ordering_helper(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    let mut children = plan.children();
    // Global ordering defines desired ordering in the final result.
    if children.len() != 1 {
        Ok((plan, false))
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        // In case of constant columns, output ordering of the `SortExec` would
        // be an empty set. Therefore; we check the sort expression field to
        // assign the requirements.
        let req_dist = sort_exec.required_input_distribution().swap_remove(0);
        let req_ordering = sort_exec.expr();
        let reqs = OrderingRequirements::from(req_ordering.clone());
        let fetch = sort_exec.fetch();

        Ok((
            Arc::new(OutputRequirementExec::new(
                plan,
                Some(reqs),
                req_dist,
                fetch,
            )) as _,
            true,
        ))
    } else if let Some(spm) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let reqs = OrderingRequirements::from(spm.expr().clone());
        let fetch = spm.fetch();
        Ok((
            Arc::new(OutputRequirementExec::new(
                plan,
                Some(reqs),
                Distribution::SinglePartition,
                fetch,
            )) as _,
            true,
        ))
    } else if plan.maintains_input_order()[0]
        && (plan.required_input_ordering()[0]
            .as_ref()
            .is_none_or(|o| matches!(o, OrderingRequirements::Soft(_))))
    {
        // Keep searching for a `SortExec` as long as ordering is maintained,
        // and on-the-way operators do not themselves require an ordering.
        // When an operator requires an ordering, any `SortExec` below can not
        // be responsible for (i.e. the originator of) the global ordering.
        let (new_child, is_changed) =
            require_top_ordering_helper(Arc::clone(children.swap_remove(0)))?;
        Ok((plan.with_new_children(vec![new_child])?, is_changed))
    } else {
        // Stop searching, there is no global ordering desired for the query.
        Ok((plan, false))
    }
}

// See tests in datafusion/core/tests/physical_optimizer
