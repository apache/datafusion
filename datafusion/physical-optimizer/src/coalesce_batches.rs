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

use crate::{OptimizerContext, PhysicalOptimizerRule};

use std::sync::Arc;

use datafusion_common::assert_eq_or_internal_err;
use datafusion_common::error::Result;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::{
    async_func::AsyncFuncExec, coalesce_batches::CoalesceBatchesExec,
    joins::HashJoinExec, repartition::RepartitionExec, ExecutionPlan,
};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default, Debug)]
pub struct CoalesceBatches {}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}
impl PhysicalOptimizerRule for CoalesceBatches {
    fn optimize_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &OptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config = context.session_config().options();
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }

        let target_batch_size = config.execution.batch_size;
        plan.transform_up(|plan| {
            let plan_any = plan.as_any();
            let wrap_in_coalesce = plan_any.downcast_ref::<HashJoinExec>().is_some()
                // Don't need to add CoalesceBatchesExec after a round robin RepartitionExec
                || plan_any
                    .downcast_ref::<RepartitionExec>()
                    .map(|repart_exec| {
                        !matches!(
                            repart_exec.partitioning().clone(),
                            Partitioning::RoundRobinBatch(_)
                        )
                    })
                    .unwrap_or(false);

            if wrap_in_coalesce {
                Ok(Transformed::yes(Arc::new(CoalesceBatchesExec::new(
                    plan,
                    target_batch_size,
                ))))
            } else if let Some(async_exec) = plan_any.downcast_ref::<AsyncFuncExec>() {
                // Coalesce inputs to async functions to reduce number of async function invocations
                let children = async_exec.children();
                assert_eq_or_internal_err!(
                    children.len(),
                    1,
                    "Expected AsyncFuncExec to have exactly one child"
                );

                let coalesce_exec = Arc::new(CoalesceBatchesExec::new(
                    Arc::clone(children[0]),
                    target_batch_size,
                ));
                let new_plan = plan.with_new_children(vec![coalesce_exec])?;
                Ok(Transformed::yes(new_plan))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
