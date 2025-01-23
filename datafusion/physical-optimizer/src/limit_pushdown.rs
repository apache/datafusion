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

//! [`LimitPushdown`] pushes `LIMIT` down through `ExecutionPlan`s to reduce
//! data transfer as much as possible.

use std::fmt::Debug;
use std::sync::Arc;

use crate::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_common::utils::combine_limit;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default, Debug)]
pub struct LimitPushdown {}

/// This is a "data class" we use within the [`LimitPushdown`] rule to push
/// down [`LimitExec`] in the plan. GlobalRequirements are hold as a rule-wide state
/// and holds the fetch and skip information. The struct also has a field named
/// satisfied which means if the "current" plan is valid in terms of limits or not.
///
/// For example: If the plan is satisfied with current fetch info, we decide to not add a LocalLimit
///
/// [`LimitPushdown`]: crate::limit_pushdown::LimitPushdown
/// [`LimitExec`]: crate::limit_pushdown::LimitExec
#[derive(Default, Clone, Debug)]
pub struct GlobalRequirements {
    fetch: Option<usize>,
    skip: usize,
    satisfied: bool,
}

impl LimitPushdown {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for LimitPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let global_state = GlobalRequirements {
            fetch: None,
            skip: 0,
            satisfied: false,
        };
        pushdown_limits(plan, global_state)
    }

    fn name(&self) -> &str {
        "LimitPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This enumeration makes `skip` and `fetch` calculations easier by providing
/// a single API for both local and global limit operators.
#[derive(Debug)]
pub enum LimitExec {
    Global(GlobalLimitExec),
    Local(LocalLimitExec),
}

impl LimitExec {
    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        match self {
            Self::Global(global) => global.input(),
            Self::Local(local) => local.input(),
        }
    }

    fn fetch(&self) -> Option<usize> {
        match self {
            Self::Global(global) => global.fetch(),
            Self::Local(local) => Some(local.fetch()),
        }
    }

    fn skip(&self) -> usize {
        match self {
            Self::Global(global) => global.skip(),
            Self::Local(_) => 0,
        }
    }
}

impl From<LimitExec> for Arc<dyn ExecutionPlan> {
    fn from(limit_exec: LimitExec) -> Self {
        match limit_exec {
            LimitExec::Global(global) => Arc::new(global),
            LimitExec::Local(local) => Arc::new(local),
        }
    }
}

/// This function is the main helper function of the `LimitPushDown` rule.
/// The helper takes an `ExecutionPlan` and a global (algorithm) state which is
/// an instance of `GlobalRequirements` and modifies these parameters while
/// checking if the limits can be pushed down or not.
///
/// If a limit is encountered, a [`TreeNodeRecursion::Stop`] is returned. Otherwise,
/// return a [`TreeNodeRecursion::Continue`].
pub fn pushdown_limit_helper(
    mut pushdown_plan: Arc<dyn ExecutionPlan>,
    mut global_state: GlobalRequirements,
) -> Result<(Transformed<Arc<dyn ExecutionPlan>>, GlobalRequirements)> {
    // Extract limit, if exist, and return child inputs.
    if let Some(limit_exec) = extract_limit(&pushdown_plan) {
        // If we have fetch/skip info in the global state already, we need to
        // decide which one to continue with:
        let (skip, fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            limit_exec.skip(),
            limit_exec.fetch(),
        );
        global_state.skip = skip;
        global_state.fetch = fetch;

        // Now the global state has the most recent information, we can remove
        // the `LimitExec` plan. We will decide later if we should add it again
        // or not.
        return Ok((
            Transformed {
                data: Arc::clone(limit_exec.input()),
                transformed: true,
                tnr: TreeNodeRecursion::Stop,
            },
            global_state,
        ));
    }

    // If we have a non-limit operator with fetch capability, update global
    // state as necessary:
    if pushdown_plan.fetch().is_some() {
        if global_state.fetch.is_none() {
            global_state.satisfied = true;
        }
        (global_state.skip, global_state.fetch) = combine_limit(
            global_state.skip,
            global_state.fetch,
            0,
            pushdown_plan.fetch(),
        );
    }

    let Some(global_fetch) = global_state.fetch else {
        // There's no valid fetch information, exit early:
        return if global_state.skip > 0 && !global_state.satisfied {
            // There might be a case with only offset, if so add a global limit:
            global_state.satisfied = true;
            Ok((
                Transformed::yes(add_global_limit(
                    pushdown_plan,
                    global_state.skip,
                    None,
                )),
                global_state,
            ))
        } else {
            // There's no info on offset or fetch, nothing to do:
            Ok((Transformed::no(pushdown_plan), global_state))
        };
    };

    let skip_and_fetch = Some(global_fetch + global_state.skip);

    if pushdown_plan.supports_limit_pushdown() {
        if !combines_input_partitions(&pushdown_plan) {
            // We have information in the global state and the plan pushes down,
            // continue:
            Ok((Transformed::no(pushdown_plan), global_state))
        } else if let Some(plan_with_fetch) = pushdown_plan.with_fetch(skip_and_fetch) {
            // This plan is combining input partitions, so we need to add the
            // fetch info to plan if possible. If not, we must add a `LimitExec`
            // with the information from the global state.
            let mut new_plan = plan_with_fetch;
            // Execution plans can't (yet) handle skip, so if we have one,
            // we still need to add a global limit
            if global_state.skip > 0 {
                new_plan =
                    add_global_limit(new_plan, global_state.skip, global_state.fetch);
            }
            global_state.fetch = skip_and_fetch;
            global_state.skip = 0;
            global_state.satisfied = true;
            Ok((Transformed::yes(new_plan), global_state))
        } else if global_state.satisfied {
            // If the plan is already satisfied, do not add a limit:
            Ok((Transformed::no(pushdown_plan), global_state))
        } else {
            global_state.satisfied = true;
            Ok((
                Transformed::yes(add_limit(
                    pushdown_plan,
                    global_state.skip,
                    global_fetch,
                )),
                global_state,
            ))
        }
    } else {
        // The plan does not support push down and it is not a limit. We will need
        // to add a limit or a fetch. If the plan is already satisfied, we will try
        // to add the fetch info and return the plan.

        // There's no push down, change fetch & skip to default values:
        let global_skip = global_state.skip;
        global_state.fetch = None;
        global_state.skip = 0;

        let maybe_fetchable = pushdown_plan.with_fetch(skip_and_fetch);
        if global_state.satisfied {
            if let Some(plan_with_fetch) = maybe_fetchable {
                Ok((Transformed::yes(plan_with_fetch), global_state))
            } else {
                Ok((Transformed::no(pushdown_plan), global_state))
            }
        } else {
            // Add fetch or a `LimitExec`:

            // If the plan's children have limit, we shouldn't change the global state to true,
            // because the children limit will be overridden if the global state is changed.
            if pushdown_plan.children().iter().any(|child| {
                child.as_any().is::<GlobalLimitExec>()
                    || child.as_any().is::<LocalLimitExec>()
            }) {
                global_state.satisfied = false;
            }
            pushdown_plan = if let Some(plan_with_fetch) = maybe_fetchable {
                if global_skip > 0 {
                    add_global_limit(plan_with_fetch, global_skip, Some(global_fetch))
                } else {
                    plan_with_fetch
                }
            } else {
                add_limit(pushdown_plan, global_skip, global_fetch)
            };
            Ok((Transformed::yes(pushdown_plan), global_state))
        }
    }
}

/// Pushes down the limit through the plan.
pub(crate) fn pushdown_limits(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    global_state: GlobalRequirements,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Call pushdown_limit_helper.
    // This will either extract the limit node (returning the child), or apply the limit pushdown.
    let (mut new_node, mut global_state) =
        pushdown_limit_helper(pushdown_plan, global_state)?;

    // While limits exist, continue combining the global_state.
    while new_node.tnr == TreeNodeRecursion::Stop {
        (new_node, global_state) = pushdown_limit_helper(new_node.data, global_state)?;
    }

    // Apply pushdown limits in children
    let children = new_node.data.children();
    let new_children = children
        .into_iter()
        .map(|child| {
            pushdown_limits(Arc::<dyn ExecutionPlan>::clone(child), global_state.clone())
        })
        .collect::<Result<_>>()?;
    new_node.data.with_new_children(new_children)
}

/// Transforms the [`ExecutionPlan`] into a [`LimitExec`] if it is a
/// [`GlobalLimitExec`] or a [`LocalLimitExec`].
fn extract_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<LimitExec> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        Some(LimitExec::Global(GlobalLimitExec::new(
            Arc::clone(global_limit.input()),
            global_limit.skip(),
            global_limit.fetch(),
        )))
    } else {
        plan.as_any()
            .downcast_ref::<LocalLimitExec>()
            .map(|local_limit| {
                LimitExec::Local(LocalLimitExec::new(
                    Arc::clone(local_limit.input()),
                    local_limit.fetch(),
                ))
            })
    }
}

/// Checks if the given plan combines input partitions.
fn combines_input_partitions(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan = plan.as_any();
    plan.is::<CoalescePartitionsExec>() || plan.is::<SortPreservingMergeExec>()
}

/// Adds a limit to the plan, chooses between global and local limits based on
/// skip value and the number of partitions.
fn add_limit(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: usize,
) -> Arc<dyn ExecutionPlan> {
    if skip > 0 || pushdown_plan.output_partitioning().partition_count() == 1 {
        add_global_limit(pushdown_plan, skip, Some(fetch))
    } else {
        Arc::new(LocalLimitExec::new(pushdown_plan, fetch + skip)) as _
    }
}

/// Adds a global limit to the plan.
fn add_global_limit(
    pushdown_plan: Arc<dyn ExecutionPlan>,
    skip: usize,
    fetch: Option<usize>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(GlobalLimitExec::new(pushdown_plan, skip, fetch)) as _
}

// See tests in datafusion/core/tests/physical_optimizer

#[cfg(test)]
mod test {
    use super::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::config::ConfigOptions;
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use datafusion_physical_expr::expressions::{col, lit};
    use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::sorts::sort::SortExec;
    use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion_physical_plan::{
        get_plan_string, ExecutionPlan, ExecutionPlanProperties,
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct DummyStreamPartition {
        schema: SchemaRef,
    }
    impl PartitionStream for DummyStreamPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            unreachable!()
        }
    }

    #[test]
    fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_zero(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(schema)?;
        let global_limit = global_limit_exec(streaming_table, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=5"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn transforms_streaming_table_exec_into_fetching_version_and_keeps_the_global_limit_when_skip_is_nonzero(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(schema)?;
        let global_limit = global_limit_exec(streaming_table, 2, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=2, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "GlobalLimitExec: skip=2, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=7"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn transforms_coalesce_batches_exec_into_fetching_version_and_removes_local_limit(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(Arc::clone(&schema))?;
        let repartition = repartition_exec(streaming_table)?;
        let filter = filter_exec(schema, repartition)?;
        let coalesce_batches = coalesce_batches_exec(filter);
        let local_limit = local_limit_exec(coalesce_batches, 5);
        let coalesce_partitions = coalesce_partitions_exec(local_limit);
        let global_limit = global_limit_exec(coalesce_partitions, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    LocalLimitExec: fetch=5",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c3@2 > 0",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    CoalesceBatchesExec: target_batch_size=8192, fetch=5",
            "      FilterExec: c3@2 > 0",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn pushes_global_limit_exec_through_projection_exec() -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(Arc::clone(&schema))?;
        let filter = filter_exec(Arc::clone(&schema), streaming_table)?;
        let projection = projection_exec(schema, filter)?;
        let global_limit = global_limit_exec(projection, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "    FilterExec: c3@2 > 0",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "  GlobalLimitExec: skip=0, fetch=5",
            "    FilterExec: c3@2 > 0",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn pushes_global_limit_exec_through_projection_exec_and_transforms_coalesce_batches_exec_into_fetching_version(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(Arc::clone(&schema)).unwrap();
        let coalesce_batches = coalesce_batches_exec(streaming_table);
        let projection = projection_exec(schema, coalesce_batches)?;
        let global_limit = global_limit_exec(projection, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "  CoalesceBatchesExec: target_batch_size=8192, fetch=5",
            "    StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn pushes_global_limit_into_multiple_fetch_plans() -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(Arc::clone(&schema)).unwrap();
        let coalesce_batches = coalesce_batches_exec(streaming_table);
        let projection = projection_exec(Arc::clone(&schema), coalesce_batches)?;
        let repartition = repartition_exec(projection)?;
        let sort = sort_exec(
            vec![PhysicalSortExpr {
                expr: col("c1", &schema)?,
                options: SortOptions::default(),
            }],
            repartition,
        );
        let spm =
            sort_preserving_merge_exec(sort.output_ordering().unwrap().to_vec(), sort);
        let global_limit = global_limit_exec(spm, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  SortPreservingMergeExec: [c1@0 ASC]",
            "    SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "SortPreservingMergeExec: [c1@0 ASC], fetch=5",
            "  SortExec: TopK(fetch=5), expr=[c1@0 ASC], preserve_partitioning=[false]",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn keeps_pushed_local_limit_exec_when_there_are_multiple_input_partitions(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(Arc::clone(&schema))?;
        let repartition = repartition_exec(streaming_table)?;
        let filter = filter_exec(schema, repartition)?;
        let coalesce_partitions = coalesce_partitions_exec(filter);
        let global_limit = global_limit_exec(coalesce_partitions, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    FilterExec: c3@2 > 0",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    FilterExec: c3@2 > 0",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn merges_local_limit_with_local_limit() -> Result<()> {
        let schema = create_schema();
        let empty_exec = empty_exec(schema);
        let child_local_limit = local_limit_exec(empty_exec, 10);
        let parent_local_limit = local_limit_exec(child_local_limit, 20);

        let initial = get_plan_string(&parent_local_limit);
        let expected_initial = [
            "LocalLimitExec: fetch=20",
            "  LocalLimitExec: fetch=10",
            "    EmptyExec",
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(parent_local_limit, &ConfigOptions::new())?;

        let expected = ["GlobalLimitExec: skip=0, fetch=10", "  EmptyExec"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn merges_global_limit_with_global_limit() -> Result<()> {
        let schema = create_schema();
        let empty_exec = empty_exec(schema);
        let child_global_limit = global_limit_exec(empty_exec, 10, Some(30));
        let parent_global_limit = global_limit_exec(child_global_limit, 10, Some(20));

        let initial = get_plan_string(&parent_global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=10, fetch=20",
            "  GlobalLimitExec: skip=10, fetch=30",
            "    EmptyExec",
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(parent_global_limit, &ConfigOptions::new())?;

        let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn merges_global_limit_with_local_limit() -> Result<()> {
        let schema = create_schema();
        let empty_exec = empty_exec(schema);
        let local_limit = local_limit_exec(empty_exec, 40);
        let global_limit = global_limit_exec(local_limit, 20, Some(30));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=20, fetch=30",
            "  LocalLimitExec: fetch=40",
            "    EmptyExec",
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

        let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    #[test]
    fn merges_local_limit_with_global_limit() -> Result<()> {
        let schema = create_schema();
        let empty_exec = empty_exec(schema);
        let global_limit = global_limit_exec(empty_exec, 20, Some(30));
        let local_limit = local_limit_exec(global_limit, 20);

        let initial = get_plan_string(&local_limit);
        let expected_initial = [
            "LocalLimitExec: fetch=20",
            "  GlobalLimitExec: skip=20, fetch=30",
            "    EmptyExec",
        ];

        assert_eq!(initial, expected_initial);

        let after_optimize =
            LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

        let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
        assert_eq!(get_plan_string(&after_optimize), expected);

        Ok(())
    }

    fn create_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]))
    }

    fn streaming_table_exec(schema: SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&schema),
            vec![Arc::new(DummyStreamPartition { schema }) as _],
            None,
            None,
            true,
            None,
        )?))
    }

    fn global_limit_exec(
        input: Arc<dyn ExecutionPlan>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(input, skip, fetch))
    }

    fn local_limit_exec(
        input: Arc<dyn ExecutionPlan>,
        fetch: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(LocalLimitExec::new(input, fetch))
    }

    fn sort_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortExec::new(sort_exprs, input))
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn projection_exec(
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(
            vec![
                (col("c1", schema.as_ref()).unwrap(), "c1".to_string()),
                (col("c2", schema.as_ref()).unwrap(), "c2".to_string()),
                (col("c3", schema.as_ref()).unwrap(), "c3".to_string()),
            ],
            input,
        )?))
    }

    fn filter_exec(
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                col("c3", schema.as_ref()).unwrap(),
                Operator::Gt,
                lit(0),
            )),
            input,
        )?))
    }

    fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalesceBatchesExec::new(input, 8192))
    }

    fn coalesce_partitions_exec(
        local_limit: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(local_limit))
    }

    fn repartition_exec(
        streaming_table: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RepartitionExec::try_new(
            streaming_table,
            Partitioning::RoundRobinBatch(8),
        )?))
    }

    fn empty_exec(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema))
    }
}
