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

//! GlobalOrderRequire optimizer rule either
//! - Adds an auxiliary `SortRequiringExec` executor
//!    to keep track of global ordering requirement across rules
//! - OR Remove the auxiliary `SortRequiringExec` executor
//!   from the physical plan. `SortRequiringExec` is only a helper executor
//!   it shouldn't occur in the final plan (executed plan.)

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
use std::sync::Arc;

/// This rule either adds or removes [`SortRequiringExec`]'s
/// (determined by the constructors `new_add_mode`, `new_remove_mode`)
/// to/from the physical plan. With this rule, we can keep track of
/// global ordering requirement across rules.
#[derive(Debug)]
pub struct GlobalOrderRequire {
    mode: RuleMode,
}

impl GlobalOrderRequire {
    /// Create a new Rule which works in Add mode
    /// e.g Add SortRequiringExec into the physical plan
    /// to keep track of global ordering if any (this rule
    /// should work at the beginning)
    pub fn new_add_mode() -> Self {
        Self {
            mode: RuleMode::Add,
        }
    }

    /// Create a new Rule which works in Remove mode
    /// e.g Remove SortRequiringExec from the physical plan
    /// if any (`SortRequiringExec` is an auxiliary executor to
    /// keep information about global ordering requirement across rules.
    /// `SortRequiringExec` shouldn't occur in the final plan, since is not executable.)
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

/// Adds SortRequiringExec (an auxiliary executor to keep track of global ordering asked by the query)
/// on top of global sort exec.
fn require_global_ordering(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut children = plan.children();
    if children.len() != 1 {
        Ok(plan)
    } else if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let req_ordering = sort_exec.output_ordering().unwrap_or(&[]);
        let reqs = PhysicalSortRequirement::from_sort_exprs(req_ordering);
        Ok(Arc::new(SortRequiringExec::new(plan.clone(), reqs)) as _)
    } else if plan.maintains_input_order()[0]
        && plan.required_input_ordering()[0].is_none()
    {
        // Keep searching SortExec as long as ordering is maintained,
        // and executor doesn't itself require ordering.
        let new_child = require_global_ordering(children.swap_remove(0))?;
        plan.with_new_children(vec![new_child])
    } else {
        // Stop searching, there is no global ordering desired according to query
        Ok(plan)
    }
}

/// Executor requires the ordering in its `requirements`
#[derive(Debug)]
struct SortRequiringExec {
    input: Arc<dyn ExecutionPlan>,
    requirements: LexOrderingReq,
}

impl SortRequiringExec {
    fn new(input: Arc<dyn ExecutionPlan>, requirements: LexOrderingReq) -> Self {
        Self {
            input,
            requirements,
        }
    }

    fn input(&self) -> Arc<dyn ExecutionPlan> {
        self.input.clone()
    }
}

impl DisplayAs for SortRequiringExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "SortRequiringExec")
    }
}

impl ExecutionPlan for SortRequiringExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> crate::physical_plan::Partitioning {
        self.input.output_partitioning()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    // model that it requires the output ordering of its input
    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(self.requirements.clone())]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let child = children.pop().unwrap();
        Ok(Arc::new(Self::new(child, self.requirements.clone())))
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

impl PhysicalOptimizerRule for GlobalOrderRequire {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = match self.mode {
            RuleMode::Add => require_global_ordering(plan)?,
            RuleMode::Remove => plan.transform_up(&|plan| {
                if let Some(sort_req) = plan.as_any().downcast_ref::<SortRequiringExec>()
                {
                    Ok(Transformed::Yes(sort_req.input().clone()))
                } else {
                    Ok(Transformed::No(plan))
                }
            })?,
        };
        Ok(plan)
    }

    fn name(&self) -> &str {
        "GlobalOrderRequire"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
