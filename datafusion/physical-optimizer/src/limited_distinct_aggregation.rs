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

//! Stop unordered DISTINCT aggregation once enough groups have been found.

use std::sync::Arc;

use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ExecutionPlanProperties,
    ReplaceChildrenOptions,
};

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};

use crate::PhysicalOptimizerRule;

/// Pushes a soft limit into unordered DISTINCT aggregations. For
/// `SELECT DISTINCT a FROM t LIMIT 10`, any ten distinct values are sufficient:
/// later rows cannot change the groups already found. The same reasoning applies
/// to `GROUP BY a` without aggregate expressions. Aggregates such as `SUM` need
/// every row in each group.
///
/// Each partial stage can also stop after finding ten distinct keys: those keys
/// remain distinct in the final stage, even if other partitions contribute
/// duplicates.
///
/// Before:
///
/// ```txt
/// Limit(10)
///   Aggregate(Final, a)
///     Aggregate(Partial, a)
///       Scan
/// ```
///
/// After:
///
/// ```txt
/// Limit(10)
///   Aggregate(Final, a, soft_limit=10)
///     Aggregate(Partial, a, soft_limit=10)
///       Scan
/// ```
///
/// # Invariants before and after the rewrite
///
/// ## Before this rule
///
/// The default physical planner produces two adjacent aggregate stages with
/// compatible grouping expressions. This rule runs before other rules can combine
/// them into a `Single` aggregate or insert operators between them.
///
/// ```txt
/// AggregateExec(mode=Final)
///   AggregateExec(mode=Partial)
/// ```
///
/// ## After this rule
///
/// For an eligible pair, both stages receive a soft-limit hint. Otherwise, both
/// stages are left unchanged.
#[derive(Debug)]
pub struct LimitedDistinctAggregation {}

impl LimitedDistinctAggregation {
    /// Create a new `LimitedDistinctAggregation`
    pub fn new() -> Self {
        Self {}
    }

    /// Rewrite a limit and its immediately adjacent final/partial aggregate pair.
    fn transform_limit(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        // Step 1: Identify the plan shape,
        //
        // Limit
        //   Aggregate(final)
        //     Aggregate(partial)

        // Check the current plan is limit, and extract limit value, input plan.
        let (limit, input) = match (
            plan.downcast_ref::<LocalLimitExec>(),
            plan.downcast_ref::<GlobalLimitExec>(),
        ) {
            (Some(local), _) => (local.fetch(), local.input()),
            (_, Some(global)) => match global.fetch() {
                Some(fetch) => (global.skip() + fetch, global.input()),
                None => return Ok(Transformed::no(plan)),
            },
            _ => return Ok(Transformed::no(plan)),
        };

        if plan.output_ordering().is_some() || plan.required_input_ordering()[0].is_some()
        {
            return Ok(Transformed::no(plan));
        }

        let Some(final_agg) = input.downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };
        let Some(partial_agg) = final_agg.input().downcast_ref::<AggregateExec>() else {
            return Ok(Transformed::no(plan));
        };

        // Step 2: Verify partial/final aggregate is compatible, then apply optimization
        match (final_agg.mode(), partial_agg.mode()) {
            (
                AggregateMode::Final | AggregateMode::FinalPartitioned,
                AggregateMode::Partial,
            ) if final_agg.group_expr() == &partial_agg.group_expr().as_final() => {}
            _ => return Ok(Transformed::no(plan)),
        }

        // Validate both stages before replacing either one.
        let (Some(final_agg), Some(partial_agg)) = (
            final_agg.clone().try_optimize_distinct_soft_limit(limit),
            partial_agg.clone().try_optimize_distinct_soft_limit(limit),
        ) else {
            return Ok(Transformed::no(plan));
        };
        if !final_agg.transformed && !partial_agg.transformed {
            return Ok(Transformed::no(plan));
        }

        let input = Arc::new(final_agg.data).replace_children(
            vec![Arc::new(partial_agg.data)],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        plan.replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .map(Transformed::yes)
    }
}

impl Default for LimitedDistinctAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for LimitedDistinctAggregation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if config.optimizer.enable_distinct_aggregation_soft_limit {
            plan.transform_down(Self::transform_limit).data()
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        "LimitedDistinctAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// See tests in datafusion/core/tests/physical_optimizer
