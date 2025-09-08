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

use std::sync::Arc;

use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::joins::utils as join_utils;
use crate::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec,
};
use crate::physical_plan::ExecutionPlan;
use arrow::compute::SortOptions;
use datafusion_common::plan_err;
use datafusion_expr::JoinType;

/// Build the appropriate join `ExecutionPlan` for the given join type, filter, and
/// configurations.
///
/// For example, given an equi-join, the planner may execute it as a Nested Loop
/// Join, Hash Join, or another strategy. Configuration settings determine which
/// ExecutionPlan is used.
///
/// # Strategy
/// - Step 1: Find all possible physical join types for the given join logical plan
///     - No join on keys and no filter => CrossJoin
///     - With equality? => HJ and SMJ(if with multiple partition)
///         TODO: The constraint on SMJ is added previously for optimization. Should
///              we remove it for configurability?
///         TODO: Allow NLJ for equal join for better configurability.
///     - Without equality? => NLJ
/// - Step 2: Filter the possible join types from step 1 according to the configuration
///     , by checking if they're enabled by options like `datafusion.optimizer.enable_hash_join`
/// - Step 3: Choose one according to the built-in heuristics and also the preference
///     in the configuration, e.g. `datafusion.optimizer.prefer_hash_join`
pub(super) fn plan_join_exec(
    session_state: &SessionState,
    physical_left: Arc<dyn ExecutionPlan>,
    physical_right: Arc<dyn ExecutionPlan>,
    join_on: join_utils::JoinOn,
    join_filter: Option<join_utils::JoinFilter>,
    join_type: &JoinType,
    null_equality: &datafusion_common::NullEquality,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Short-circuit: handle pure cross join (existing behavior)
    if join_on.is_empty() {
        if join_filter.is_none() && matches!(join_type, JoinType::Inner) {
            return Ok(Arc::new(CrossJoinExec::new(physical_left, physical_right)));
        }
    }

    // Step 1: Find possible join types for the given Logical Plan
    // ----------------------------------------------------------------------

    // Build the list of possible algorithms for this join
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Algo {
        NLJ,
        HJ,
        SMJ,
    }

    let cfg = &session_state.config_options().optimizer;
    let can_smj = session_state.config().target_partitions() > 1
        && session_state.config().repartition_joins();

    let mut possible: Vec<Algo> = Vec::new();
    if join_on.is_empty() {
        possible.push(Algo::NLJ);
    } else {
        possible.push(Algo::HJ);
        if can_smj {
            possible.push(Algo::SMJ);
        }
    }

    // Step 2: Filter the possible list according to enable flags from config
    // ----------------------------------------------------------------------

    // Filter by enable flags
    let enabled_and_possible: Vec<Algo> = possible
        .iter()
        .copied()
        .filter(|a| match a {
            Algo::NLJ => cfg.enable_nested_loop_join,
            Algo::HJ => cfg.enable_hash_join,
            Algo::SMJ => cfg.enable_sort_merge_join,
        })
        .collect();

    if enabled_and_possible.is_empty() {
        return plan_err!(
            "No enabled join algorithm is applicable for this join. Possible join types are {:?}. Try to enable them through configurations like `datafusion.optimizer.enable_hash_join`", &possible
        );
    }

    // Step 3: Choose and plan the physical join type according to preference
    // from the configuration, and also the built-in heuristics
    // ----------------------------------------------------------------------

    // Collect preferred algorithms
    let mut preferred: Vec<Algo> = Vec::new();
    if cfg.prefer_hash_join {
        preferred.push(Algo::HJ);
    }
    if cfg.prefer_sort_merge_join {
        preferred.push(Algo::SMJ);
    }
    if cfg.prefer_nested_loop_join {
        preferred.push(Algo::NLJ);
    }

    // Helper to pick by priority HJ > SMJ > NLJ
    let pick_by_priority = |candidates: &[Algo]| -> Algo {
        if candidates.iter().any(|a| *a == Algo::HJ) {
            Algo::HJ
        } else if candidates.iter().any(|a| *a == Algo::SMJ) {
            Algo::SMJ
        } else {
            Algo::NLJ
        }
    };

    // If there is overlap with preferred, use that; otherwise use enabled list by priority
    let chosen = if !preferred.is_empty() {
        let overlaps: Vec<Algo> = enabled_and_possible
            .iter()
            .copied()
            .filter(|a| preferred.contains(a))
            .collect();
        if !overlaps.is_empty() {
            pick_by_priority(&overlaps)
        } else {
            pick_by_priority(&enabled_and_possible)
        }
    } else {
        pick_by_priority(&enabled_and_possible)
    };

    match chosen {
        Algo::NLJ => Ok(Arc::new(NestedLoopJoinExec::try_new(
            physical_left,
            physical_right,
            join_filter,
            join_type,
            None,
        )?)),
        Algo::HJ => {
            // Determine partition mode based solely on partitioning configuration
            let partition_mode = if session_state.config().target_partitions() > 1
                && session_state.config().repartition_joins()
            {
                PartitionMode::Auto
            } else {
                PartitionMode::CollectLeft
            };

            Ok(Arc::new(HashJoinExec::try_new(
                physical_left,
                physical_right,
                join_on,
                join_filter,
                join_type,
                None,
                partition_mode,
                *null_equality,
            )?))
        }
        Algo::SMJ => {
            let join_on_len = join_on.len();
            Ok(Arc::new(SortMergeJoinExec::try_new(
                physical_left,
                physical_right,
                join_on,
                join_filter,
                *join_type,
                vec![SortOptions::default(); join_on_len],
                *null_equality,
            )?))
        }
    }
}
