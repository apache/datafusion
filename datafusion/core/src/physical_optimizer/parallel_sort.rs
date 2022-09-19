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

//! Parralel sort parallelizes sorts if a limit is present after a sort (`ORDER BY LIMIT N`)
use crate::{
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        limit::GlobalLimitExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        with_new_children_if_necessary,
    },
};
use std::sync::Arc;

/// Optimizer rule that makes sort parallel if a limit is used after sort (`ORDER BY LIMIT N`)
/// The plan will use `SortPreservingMergeExec` to merge the results
#[derive(Default)]
pub struct ParallelSort {}

impl ParallelSort {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
impl PhysicalOptimizerRule for ParallelSort {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        config: &crate::execution::context::SessionConfig,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        if plan.children().is_empty() {
            // leaf node, children cannot be replaced
            Ok(plan.clone())
        } else {
            // recurse down first
            let children = plan
                .children()
                .iter()
                .map(|child| self.optimize(child.clone(), config))
                .collect::<Result<Vec<_>>>()?;
            let plan = with_new_children_if_necessary(plan, children)?;
            let children = plan.children();
            let plan_any = plan.as_any();
            // GlobalLimitExec (SortExec preserve_partitioning=False)
            // -> GlobalLimitExec (SortExec preserve_partitioning=True)
            let parallel_sort = plan_any.downcast_ref::<GlobalLimitExec>().is_some()
                && children.len() == 1
                && children[0].as_any().downcast_ref::<SortExec>().is_some()
                && !children[0]
                    .as_any()
                    .downcast_ref::<SortExec>()
                    .unwrap()
                    .preserve_partitioning();

            Ok(if parallel_sort {
                let sort = children[0].as_any().downcast_ref::<SortExec>().unwrap();
                let new_sort = SortExec::new_with_partitioning(
                    sort.expr().to_vec(),
                    sort.input().clone(),
                    true,
                );
                let merge = SortPreservingMergeExec::new(
                    sort.expr().to_vec(),
                    Arc::new(new_sort),
                );
                with_new_children_if_necessary(plan, vec![Arc::new(merge)])?
            } else {
                plan.clone()
            })
        }
    }

    fn name(&self) -> &str {
        "parallel_sort"
    }
}
