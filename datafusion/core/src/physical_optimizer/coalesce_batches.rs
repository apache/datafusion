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

//! CoalesceBatches optimizer that groups batches together rows
//! in bigger batches to avoid overhead with small batches

use crate::{
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, filter::FilterExec,
        hash_join::HashJoinExec, repartition::RepartitionExec,
        with_new_children_if_necessary,
    },
};
use std::sync::Arc;

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default)]
pub struct CoalesceBatches {
    /// Target batch size
    target_batch_size: usize,
}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new(target_batch_size: usize) -> Self {
        Self { target_batch_size }
    }
}
impl PhysicalOptimizerRule for CoalesceBatches {
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
            // The goal here is to detect operators that could produce small batches and only
            // wrap those ones with a CoalesceBatchesExec operator. An alternate approach here
            // would be to build the coalescing logic directly into the operators
            // See https://github.com/apache/arrow-datafusion/issues/139
            let plan_any = plan.as_any();
            let wrap_in_coalesce = plan_any.downcast_ref::<FilterExec>().is_some()
                || plan_any.downcast_ref::<HashJoinExec>().is_some()
                || plan_any.downcast_ref::<RepartitionExec>().is_some();
            Ok(if wrap_in_coalesce {
                Arc::new(CoalesceBatchesExec::new(
                    plan.clone(),
                    self.target_batch_size,
                ))
            } else {
                plan.clone()
            })
        }
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }
}
