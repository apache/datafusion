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
//! - Adds an auxiliary `GlobalRequirementExec` operator to keep track of global
//!   ordering and distribution requirement across rules, or
//! - Removes the auxiliary `GlobalRequirementExec` operator from the physical plan.
//!   Since the `GlobalRequirementExec` operator is only a helper operator, it
//!   shouldn't occur in the final plan (i.e. the executed plan).

use std::sync::Arc;

use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};

use arrow_schema::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::{
    Distribution, LexOrderingReq, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

/// This rule either adds or removes [`GlobalRequirements`]s to/from the physical
/// plan according to its `mode` attribute, which is set by the constructors
/// `new_add_mode` and `new_remove_mode`. With this rule, we can keep track of
/// the global requirements (ordering and distribution) across rules.
#[derive(Debug)]
pub struct GlobalRequirements {
    mode: RuleMode,
}

impl GlobalRequirements {
    /// Create a new rule which works in `Add` mode; i.e. it simply adds a
    /// top-level [`GlobalRequirementExec`] into the physical plan to keep track
    /// of global ordering, and global distribution requirements if there is any.
    /// Note that this rule should run at the beginning.
    pub fn new_add_mode() -> Self {
        Self {
            mode: RuleMode::Add,
        }
    }

    /// Create a new rule which works in `Remove` mode; i.e. it simply removes
    /// the top-level [`GlobalRequirementExec`] from the physical plan if there is
    /// any. We do this because a `GlobalRequirementExec` is an ancillary,
    /// non-executable operator whose sole purpose is to track global
    /// requirements during optimization. Therefore, a
    /// `GlobalRequirementExec` should not appear in the final plan.
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
#[derive(Debug)]
struct GlobalRequirementExec {
    input: Arc<dyn ExecutionPlan>,
    order_requirement: Option<LexOrderingReq>,
    dist_requirement: Distribution,
}

impl GlobalRequirementExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        requirements: Option<LexOrderingReq>,
        dist_requirement: Distribution,
    ) -> Self {
        Self {
            input,
            order_requirement: requirements,
            dist_requirement,
        }
    }

    fn input(&self) -> Arc<dyn ExecutionPlan> {
        self.input.clone()
    }
}

impl DisplayAs for GlobalRequirementExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "GlobalRequirementExec")
    }
}

impl ExecutionPlan for GlobalRequirementExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![self.dist_requirement.clone()]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![self.order_requirement.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let child = children.remove(0);
        Ok(Arc::new(Self::new(
            child,
            self.order_requirement.clone(),
            self.dist_requirement.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<crate::execution::context::TaskContext>,
    ) -> Result<crate::physical_plan::SendableRecordBatchStream> {
        unreachable!();
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl PhysicalOptimizerRule for GlobalRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.mode {
            RuleMode::Add => require_top_ordering(plan),
            RuleMode::Remove => plan.transform_up(&|plan| {
                if let Some(sort_req) =
                    plan.as_any().downcast_ref::<GlobalRequirementExec>()
                {
                    Ok(Transformed::Yes(sort_req.input().clone()))
                } else {
                    Ok(Transformed::No(plan))
                }
            }),
        }
    }

    fn name(&self) -> &str {
        "GlobalRequirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This functions adds ancillary `GlobalRequirementExec` to the the physical plan, so that
/// global requirements are not lost during optimization.
fn require_top_ordering(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let (new_plan, is_changed) = require_top_ordering_helper(plan)?;
    if is_changed {
        Ok(new_plan)
    } else {
        // Add `GlobalRequirementExec` to the top, with no specified ordering and distribution requirement.
        Ok(Arc::new(GlobalRequirementExec::new(
            new_plan,
            // there is no ordering requirement
            None,
            Distribution::UnspecifiedDistribution,
        )) as _)
    }
}

/// Helper function that adds an ancillary `GlobalRequirementExec` to the given plan.
/// First entry in the tuple is resulting plan, second entry indicates whether any
/// `GlobalRequirementExec` is added to the plan.
fn require_top_ordering_helper(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<(Arc<dyn ExecutionPlan>, bool)> {
    let mut children = plan.children();
    // Global ordering defines desired ordering in the final result.
    if children.len() != 1 {
        Ok((plan, false))
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let req_ordering = sort_exec.output_ordering().unwrap_or(&[]);
        let req_dist = sort_exec.required_input_distribution()[0].clone();
        let reqs = PhysicalSortRequirement::from_sort_exprs(req_ordering);
        Ok((
            Arc::new(GlobalRequirementExec::new(
                plan.clone(),
                Some(reqs),
                req_dist,
            )) as _,
            true,
        ))
    } else if let Some(spm) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let req_ordering = spm.expr();
        let reqs = PhysicalSortRequirement::from_sort_exprs(req_ordering);
        Ok((
            Arc::new(GlobalRequirementExec::new(
                plan.clone(),
                Some(reqs),
                Distribution::SinglePartition,
            )) as _,
            true,
        ))
    } else if plan.maintains_input_order()[0]
        && plan.required_input_ordering()[0].is_none()
    {
        // Keep searching for a `SortExec` as long as ordering is maintained,
        // and on-the-way operators do not themselves require an ordering.
        // When an operator requires an ordering, any `SortExec` below can not
        // be responsible for (i.e. the originator of) the global ordering.
        let (new_child, is_changed) =
            require_top_ordering_helper(children.swap_remove(0))?;
        Ok((plan.with_new_children(vec![new_child])?, is_changed))
    } else {
        // Stop searching, there is no global ordering desired for the query.
        Ok((plan, false))
    }
}
