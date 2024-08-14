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

use std::{any::Any, sync::Arc};

use crate::{
    config::ConfigOptions,
    error::Result,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, filter::FilterExec, joins::HashJoinExec,
        repartition::RepartitionExec, Partitioning,
    },
};

use crate::arrow::util::bit_util::ceil;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::{
    aggregates::AggregateExec,
    joins::SortMergeJoinExec,
    limit::{GlobalLimitExec, LocalLimitExec},
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    windows::WindowAggExec,
};

/// Optimizer rule that introduces CoalesceBatchesExec to avoid overhead with small batches that
/// are produced by highly selective filters
#[derive(Default)]
pub struct CoalesceBatches {}

impl CoalesceBatches {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

#[inline]
fn get_limit(plan: &dyn Any) -> Option<usize> {
    if let Some(limit_exec) = plan.downcast_ref::<GlobalLimitExec>() {
        limit_exec.fetch()
    } else {
        plan.downcast_ref::<LocalLimitExec>()
            .map(|limit_exec| limit_exec.fetch())
    }
}

#[inline]
fn need_scan_all(plan: &dyn Any) -> bool {
    plan.downcast_ref::<SortMergeJoinExec>().is_some()
        || plan.downcast_ref::<AggregateExec>().is_some()
        || plan.downcast_ref::<SortExec>().is_some()
        || plan.downcast_ref::<SortPreservingMergeExec>().is_some()
        || plan.downcast_ref::<WindowAggExec>().is_some()
}

#[inline]
fn need_wrap_in_coalesce(plan: &dyn Any) -> bool {
    // The goal here is to detect operators that could produce small batches and only
    // wrap those ones with a CoalesceBatchesExec operator. An alternate approach here
    // would be to build the coalescing logic directly into the operators
    // See https://github.com/apache/arrow-datafusion/issues/139
    plan.downcast_ref::<FilterExec>().is_some()
                || plan.downcast_ref::<HashJoinExec>().is_some()
                // Don't need to add CoalesceBatchesExec after a round robin RepartitionExec
                || plan
                    .downcast_ref::<RepartitionExec>()
                    .map(|repart_exec| {
                        !matches!(
                            repart_exec.partitioning().clone(),
                            Partitioning::RoundRobinBatch(_)
                        )
                    })
                    .unwrap_or(false)
}

fn wrap_in_coalesce_rewrite_inner(
    mut limit: Option<usize>,
    partition: usize,
    default_batch_size: usize,
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
    // If the entire table needs to be scanned, the limit at the upper level does not take effect
    if need_scan_all(plan.as_any()) {
        limit = None
    }
    let children = plan
        .children()
        .iter()
        .map(|&child| {
            // Update to downstream limit
            let limit = match get_limit(child.as_any()) {
                None => limit,
                v => v,
            };
            wrap_in_coalesce_rewrite_inner(
                limit,
                partition,
                default_batch_size,
                child.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let wrap_in_coalesce = need_wrap_in_coalesce(plan.as_any());

    // Take the smaller of `limit/partition` and `default_batch_size` as target_batch_size
    let target_batch_size = match limit {
        Some(limit) => std::cmp::min(ceil(limit, partition), default_batch_size),
        None => default_batch_size,
    };

    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };

    Ok(if wrap_in_coalesce {
        Arc::new(CoalesceBatchesExec::new(plan, target_batch_size))
    } else {
        plan
    })
}

fn wrap_in_coalesce_rewrite(
    mut partition: usize,
    default_batch_size: usize,
    plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
    // The partition is at least 1
    if partition == 0 {
        partition = 1;
    }
    wrap_in_coalesce_rewrite_inner(
        get_limit(plan.as_any()),
        partition,
        default_batch_size,
        plan,
    )
}

impl PhysicalOptimizerRule for CoalesceBatches {
    fn optimize(
        &self,
        plan: Arc<dyn crate::physical_plan::ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn crate::physical_plan::ExecutionPlan>> {
        if !config.execution.coalesce_batches {
            return Ok(plan);
        }
        wrap_in_coalesce_rewrite(
            config.execution.target_partitions,
            config.execution.batch_size,
            plan,
        )
    }

    fn name(&self) -> &str {
        "coalesce_batches"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
