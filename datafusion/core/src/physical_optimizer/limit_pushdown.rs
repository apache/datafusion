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

use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::ExecutionPlan;

use datafusion_common::config::ConfigOptions;
use datafusion_common::plan_datafusion_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_optimizer::push_down_limit::combine_limit;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

/// This rule inspects [`ExecutionPlan`]'s and pushes down the fetch limit from
/// the parent to the child if applicable.
#[derive(Default)]
pub struct LimitPushdown {}

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
        plan.transform_down(push_down_limits).data()
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
enum LimitExec {
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

    fn with_child(&self, child: Arc<dyn ExecutionPlan>) -> Self {
        match self {
            Self::Global(global) => {
                Self::Global(GlobalLimitExec::new(child, global.skip(), global.fetch()))
            }
            Self::Local(local) => Self::Local(LocalLimitExec::new(child, local.fetch())),
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

/// Pushes down the limit through the plan.
pub fn push_down_limits(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let maybe_modified = if let Some(limit_exec) = extract_limit(&plan) {
        let child = limit_exec.input();
        if let Some(child_limit) = extract_limit(child) {
            let merged = merge_limits(&limit_exec, &child_limit);
            // Revisit current node in case of consecutive pushdowns
            Some(push_down_limits(merged)?.data)
        } else if child.supports_limit_pushdown() {
            try_push_down_limit(&limit_exec, child.clone())?
        } else {
            add_fetch_to_child(&limit_exec, child.clone())
        }
    } else {
        None
    };

    Ok(maybe_modified.map_or(Transformed::no(plan), Transformed::yes))
}

/// Transforms the [`ExecutionPlan`] into a [`LimitExec`] if it is a
/// [`GlobalLimitExec`] or a [`LocalLimitExec`].
fn extract_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<LimitExec> {
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        Some(LimitExec::Global(GlobalLimitExec::new(
            global_limit.input().clone(),
            global_limit.skip(),
            global_limit.fetch(),
        )))
    } else {
        plan.as_any()
            .downcast_ref::<LocalLimitExec>()
            .map(|local_limit| {
                LimitExec::Local(LocalLimitExec::new(
                    local_limit.input().clone(),
                    local_limit.fetch(),
                ))
            })
    }
}

/// Merge the limits of the parent and the child. If at least one of them is a
/// [`GlobalLimitExec`], the result is also a [`GlobalLimitExec`]. Otherwise,
/// the result is a [`LocalLimitExec`].
fn merge_limits(
    parent_limit_exec: &LimitExec,
    child_limit_exec: &LimitExec,
) -> Arc<dyn ExecutionPlan> {
    // We can use the logic in `combine_limit` from the logical optimizer:
    let (skip, fetch) = combine_limit(
        parent_limit_exec.skip(),
        parent_limit_exec.fetch(),
        child_limit_exec.skip(),
        child_limit_exec.fetch(),
    );
    match (parent_limit_exec, child_limit_exec) {
        (LimitExec::Local(_), LimitExec::Local(_)) => {
            // The fetch is present in this case, can unwrap.
            Arc::new(LocalLimitExec::new(
                child_limit_exec.input().clone(),
                fetch.unwrap(),
            ))
        }
        _ => Arc::new(GlobalLimitExec::new(
            child_limit_exec.input().clone(),
            skip,
            fetch,
        )),
    }
}

/// Pushes down the limit through the child. If the child has a single input
/// partition, simply swaps the parent and the child. Otherwise, adds a
/// [`LocalLimitExec`] after in between in addition to swapping, because of
/// multiple input partitions.
fn try_push_down_limit(
    limit_exec: &LimitExec,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let grandchildren = child.children();
    if let Some(&grandchild) = grandchildren.first() {
        // GlobalLimitExec and LocalLimitExec must have an input after pushdown
        if combines_input_partitions(&child) {
            // We still need a LocalLimitExec after the child
            if let Some(fetch) = limit_exec.fetch() {
                let new_local_limit = Arc::new(LocalLimitExec::new(
                    grandchild.clone(),
                    fetch + limit_exec.skip(),
                ));
                let new_child = child.clone().with_new_children(vec![new_local_limit])?;
                Ok(Some(limit_exec.with_child(new_child).into()))
            } else {
                Ok(None)
            }
        } else {
            // Swap current with child
            let new_limit = limit_exec.with_child(grandchild.clone());
            let new_child = child.clone().with_new_children(vec![new_limit.into()])?;
            Ok(Some(new_child))
        }
    } else {
        // Operators supporting limit push down must have a child.
        Err(plan_datafusion_err!(
            "{:#?} must have a child to push down limit",
            child
        ))
    }
}

fn combines_input_partitions(exec: &Arc<dyn ExecutionPlan>) -> bool {
    let exec = exec.as_any();
    exec.is::<CoalescePartitionsExec>() || exec.is::<SortPreservingMergeExec>()
}

/// Transforms child to the fetching version if supported. Removes the parent if
/// skip is zero. Otherwise, keeps the parent.
fn add_fetch_to_child(
    limit_exec: &LimitExec,
    child: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let fetch = limit_exec.fetch();
    let skip = limit_exec.skip();

    let child_fetch = fetch.map(|f| f + skip);

    if let Some(child_with_fetch) = child.with_fetch(child_fetch) {
        if skip > 0 {
            Some(limit_exec.with_child(child_with_fetch).into())
        } else {
            Some(child_with_fetch)
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr_common::expressions::column::col;
    use datafusion_physical_expr_common::expressions::lit;
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::get_plan_string;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};

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
        let streaming_table = streaming_table_exec(schema.clone())?;
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
        let streaming_table = streaming_table_exec(schema.clone())?;
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
        let streaming_table = streaming_table_exec(schema.clone())?;
        let repartition = repartition_exec(streaming_table)?;
        let filter = filter_exec(schema.clone(), repartition)?;
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
        let streaming_table = streaming_table_exec(schema.clone())?;
        let filter = filter_exec(schema.clone(), streaming_table)?;
        let projection = projection_exec(schema.clone(), filter)?;
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
        let streaming_table = streaming_table_exec(schema.clone()).unwrap();
        let coalesce_batches = coalesce_batches_exec(streaming_table);
        let projection = projection_exec(schema.clone(), coalesce_batches)?;
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
    fn keeps_pushed_local_limit_exec_when_there_are_multiple_input_partitions(
    ) -> Result<()> {
        let schema = create_schema();
        let streaming_table = streaming_table_exec(schema.clone())?;
        let repartition = repartition_exec(streaming_table)?;
        let filter = filter_exec(schema.clone(), repartition)?;
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
            "    LocalLimitExec: fetch=5",
            "      FilterExec: c3@2 > 0",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
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

        let expected = ["LocalLimitExec: fetch=10", "  EmptyExec"];
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
            schema.clone(),
            vec![Arc::new(DummyStreamPartition {
                schema: schema.clone(),
            }) as _],
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
