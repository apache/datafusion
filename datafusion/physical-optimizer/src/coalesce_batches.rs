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

use crate::PhysicalOptimizerRule;

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::{
    aggregates::{AggregateExec, AggregateMode}, coalesce_batches::CoalesceBatchesExec, filter::FilterExec,
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
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }

        let target_batch_size = config.execution.batch_size;
        plan.transform_up(|plan| {
            let plan_any = plan.as_any();
            // The goal here is to detect operators that could produce small batches and only
            // wrap those ones with a CoalesceBatchesExec operator. An alternate approach here
            // would be to build the coalescing logic directly into the operators
            // See https://github.com/apache/datafusion/issues/139
            let wrap_in_coalesce = plan_any.downcast_ref::<FilterExec>().is_some()
                || plan_any.downcast_ref::<HashJoinExec>().is_some()
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

/// Remove CoalesceBatchesExec that are in front of a AggregateExec
#[derive(Default, Debug)]
pub struct UnCoalesceBatches {}

impl UnCoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl PhysicalOptimizerRule for UnCoalesceBatches {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            if let Some(aggregate) = plan.as_any().downcast_ref::<AggregateExec>() {
                let agg_input = aggregate.input();

                if aggregate.mode() != &AggregateMode::Partial {
                    if let Some(coalesce) =
                        plan.as_any().downcast_ref::<CoalesceBatchesExec>()
                    {
                        let coalesce_input = coalesce.input();

                        return Ok(Transformed::yes(
                            agg_input
                                .clone()
                                .with_new_children(vec![coalesce_input.clone()])?,
                        ));
                    }
                }
            }

            if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                let children = hash_join.children();
                if let Some(coalesce) = hash_join
                    .left()
                    .as_any()
                    .downcast_ref::<CoalesceBatchesExec>()
                {
                    let coalesce_input = coalesce.input();

                    return Ok(Transformed::yes(plan.clone().with_new_children(vec![
                        coalesce_input.clone(),
                        children[1].clone(),
                    ])?));
                }
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &str {
        "uncoalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
