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

//! Replace eligible [`SortPreservingMergeExec`] operators with the parallel
//! [`ParallelSortPreservingMergeExec`].
//!
//! Gated by the `datafusion.optimizer.enable_parallel_sort_merge` config flag.
//! See [`ParallelSortPreservingMergeExec`] for the algorithm.

use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::execution_plan::Boundedness;
use datafusion_physical_plan::sorts::parallel_merge::ParallelSortPreservingMergeExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::statistics::StatisticsArgs;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Optimizer rule that swaps [`SortPreservingMergeExec`] for the parallel
/// [`ParallelSortPreservingMergeExec`] when it is both safe and beneficial.
#[derive(Debug, Clone, Default)]
pub struct ParallelSortMerge {}

impl ParallelSortMerge {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ParallelSortMerge {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_parallel_sort_merge {
            return Ok(plan);
        }
        // With a single target partition there is nothing to parallelize.
        if config.execution.target_partitions <= 1 {
            return Ok(plan);
        }

        // Only parallelize merges whose input is large enough to split into
        // more than one bucket; below this the materialization + sampling
        // overhead outweighs the benefit and a single-threaded merge is faster.
        let min_rows = config
            .execution
            .batch_size
            .get()
            .saturating_mul(config.execution.target_partitions);

        plan.transform_up(|plan: Arc<dyn ExecutionPlan>| {
            let Some(spm) = plan.downcast_ref::<SortPreservingMergeExec>() else {
                return Ok(Transformed::no(plan));
            };
            let input = spm.input();

            // The parallel merge materializes its input and emits no rows until
            // it has merged everything, so it cannot satisfy a fetch/limit
            // (which `SortPreservingMergeExec` streams and stops early on) and
            // must not be used on unbounded input. It also needs more than one
            // input partition to be worth parallelizing.
            let eligible = spm.fetch().is_none()
                && matches!(input.boundedness(), Boundedness::Bounded)
                && input.output_partitioning().partition_count() > 1;

            if !eligible {
                return Ok(Transformed::no(plan));
            }

            // Only parallelize when the input is *known* to be large enough to
            // benefit. The parallel merge materializes its input and pays a
            // sampling + task-spawn overhead, so for small (or unknown-size)
            // inputs the single-threaded merge is faster; when the row count is
            // unavailable we conservatively keep the serial merge.
            let Some(rows) = input
                .statistics_with_args(&StatisticsArgs::new())
                .ok()
                .and_then(|stats| stats.num_rows.get_value().copied())
            else {
                return Ok(Transformed::no(plan));
            };
            if rows < min_rows {
                return Ok(Transformed::no(plan));
            }

            let parallel = ParallelSortPreservingMergeExec::new(
                spm.expr().clone(),
                Arc::clone(input),
            );
            Ok(Transformed::yes(Arc::new(parallel)))
        })
        .data()
    }

    fn name(&self) -> &str {
        "ParallelSortMerge"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
