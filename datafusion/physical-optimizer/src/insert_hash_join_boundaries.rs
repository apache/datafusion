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

//! [`InsertHashJoinBoundaries`] wraps each input of every `HashJoinExec`
//! in a [`StageBoundaryBuffer`]. Both sides are gated so that neither
//! has been consumed by the join by the time runtime stats arrive — the
//! precondition for the build-side-swap rule actually swapping (rather
//! than just logging intent). Memory cost is the size of both inputs;
//! spill is a follow-up, OomGuard catches genuine OOM in the meantime.
//!
//! Stage numbers are assigned bottom-up: a new boundary's stage is one
//! more than the highest stage of any boundary already in its input
//! subtree, or 0 if there is none. Lowest stage releases first;
//! [`RuntimeOptimizerExec`] walks the subtree at runtime and primes the
//! next stage as the previous one's boundaries become ready.
//!
//! `transform_up` ensures deeper joins are visited before outer joins,
//! so when an outer-join boundary is computed the inner-join boundaries
//! already exist and their stage numbers are visible.
//!
//! [`RuntimeOptimizerExec`]: datafusion_physical_plan::runtime_optimizer::RuntimeOptimizerExec

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::stage_boundary_buffer::StageBoundaryBuffer;

#[derive(Default, Debug)]
pub struct InsertHashJoinBoundaries;

impl InsertHashJoinBoundaries {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for InsertHashJoinBoundaries {
    fn name(&self) -> &str {
        "InsertHashJoinBoundaries"
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let with_buffers = plan
            .transform_up(|node| {
                if node.downcast_ref::<HashJoinExec>().is_none() {
                    return Ok(Transformed::no(node));
                }
                let mut new_children: Vec<Arc<dyn ExecutionPlan>> =
                    Vec::with_capacity(node.children().len());
                for child in node.children() {
                    if child.downcast_ref::<StageBoundaryBuffer>().is_some() {
                        new_children.push(Arc::clone(child));
                        continue;
                    }
                    let stage =
                        max_buffer_stage_in_subtree(child).map_or(0, |max| max + 1);
                    new_children.push(Arc::new(StageBoundaryBuffer::new(
                        Arc::clone(child),
                        stage,
                    )));
                }
                Ok(Transformed::yes(node.with_new_children(new_children)?))
            })?
            .data;
        Ok(with_buffers)
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn max_buffer_stage_in_subtree(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let mut max_stage = plan.downcast_ref::<StageBoundaryBuffer>().map(|b| b.stage());
    for child in plan.children() {
        if let Some(child_max) = max_buffer_stage_in_subtree(child) {
            max_stage = Some(max_stage.map_or(child_max, |m| m.max(child_max)));
        }
    }
    max_stage
}
