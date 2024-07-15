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

//! This rule optimizes the amount of data transferred by pushing down limits

use std::sync::Arc;
use datafusion_common::config::ConfigOptions;
use datafusion_common::plan_datafusion_err;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::ExecutionPlan;

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

pub fn push_down_limits(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // TODO: Clean up and refactor
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let child = global_limit.input();
        if let Some(child_limit) = child.as_any().downcast_ref::<LocalLimitExec>() {
            // Merge limits
            let parent_skip = global_limit.skip();
            let parent_fetch = global_limit.fetch();
            let parent_max = parent_fetch.and_then(|f| Some(f + parent_skip));
            let child_fetch = child_limit.fetch();

            if let Some(parent_max) = parent_max {
                if child_fetch >= parent_max {
                    // Child fetch is larger than or equal to parent max, so we can remove the child
                    let merged = Arc::new(GlobalLimitExec::new(
                        child_limit.input().clone(),
                        parent_skip,
                        parent_fetch,
                    ));
                    Ok(Transformed::yes(push_down_limits(merged)?.data))
                } else if child_fetch > parent_skip {
                    // Child fetch is larger than parent skip, so we can trim the parent fetch
                    let merged = Arc::new(GlobalLimitExec::new(
                        child_limit.input().clone(),
                        parent_skip,
                        Some(child_fetch - parent_skip),
                    ));
                    Ok(Transformed::yes(push_down_limits(merged)?.data))
                } else {
                    // This would return an empty result
                    Err(plan_datafusion_err!("Child fetch is less than parent skip"))
                }
            } else {
                // Parent's fetch is infinite, use child's
                let new_global_limit = Arc::new(GlobalLimitExec::new(
                    child_limit.input().clone(),
                    parent_skip,
                    Some(child_fetch),
                ));
                Ok(Transformed::yes(new_global_limit))
            }
        } else if child.supports_limit_pushdown() {
            let grandchildren = child.children();
            if let Some(&grandchild) = grandchildren.first() {
                if grandchild.output_partitioning().partition_count() > 1 {
                    // Insert a LocalLimitExec after the child
                    if let Some(fetch) = global_limit.fetch() {
                        let new_local_limit = Arc::new(LocalLimitExec::new(
                            grandchild.clone(),
                            fetch + global_limit.skip(),
                        ));
                        let new_child = child.clone().with_new_children(vec![new_local_limit])?;
                        Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                            new_child,
                            global_limit.skip(),
                            global_limit.fetch(),
                        ))))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                } else {
                    // Swap current with child
                    let new_global_limit = Arc::new(GlobalLimitExec::new(
                        grandchild.clone(),
                        global_limit.skip(),
                        global_limit.fetch(),
                    ));
                    let new_child = child.clone().with_new_children(vec![new_global_limit])?;
                    Ok(Transformed::yes(new_child))
                }
            } else {
                // GlobalLimitExec would have no input, which should be impossible
                Err(plan_datafusion_err!("GlobalLimitExec has no input"))
            }
        } else {
            let fetch = global_limit.fetch();
            let skip = global_limit.skip();

            let child_fetch = fetch.and_then(|f| Some(f + skip));

            if let Some(child_with_fetch) = child.with_fetch(child_fetch) {
                if skip > 0 {
                    Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                        child_with_fetch,
                        skip,
                        fetch,
                    ))))
                } else {
                    Ok(Transformed::yes(child_with_fetch))
                }
            } else {
                Ok(Transformed::no(plan))
            }
        }
    } else if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        let child = local_limit.input();
        if let Some(child) = child.as_any().downcast_ref::<LocalLimitExec>() {
            // Keep the smaller limit
            let parent_fetch = local_limit.fetch();
            let child_fetch = child.fetch();

            let merged = Arc::new(LocalLimitExec::new(
                child.input().clone(),
                std::cmp::min(parent_fetch, child_fetch),
            ));
            Ok(Transformed::yes(push_down_limits(merged)?.data))
        } else if child.supports_limit_pushdown() {
            let grandchildren = child.children();
            if let Some(&grandchild) = grandchildren.first() {
                if grandchild.output_partitioning().partition_count() > 1 {
                    // Insert a LocalLimitExec after the child
                    let new_local_limit = Arc::new(LocalLimitExec::new(
                        grandchild.clone(),
                        local_limit.fetch(),
                    ));
                    let new_child = child.clone().with_new_children(vec![new_local_limit])?;
                    Ok(Transformed::yes(Arc::new(LocalLimitExec::new(
                        new_child,
                        local_limit.fetch(),
                    ))))
                } else {
                    // Swap current with child
                    let new_global_limit = Arc::new(LocalLimitExec::new(
                        grandchild.clone(),
                        local_limit.fetch(),
                    ));
                    let new_child = child.clone().with_new_children(vec![new_global_limit])?;
                    Ok(Transformed::yes(new_child))
                }
            } else {
                // LocalLimitExec would have no input, which should be impossible
                Err(plan_datafusion_err!("LocalLimitExec has no input"))
            }
        } else {
            let fetch = local_limit.fetch();

            if let Some(child_with_fetch) = child.with_fetch(Some(fetch)) {
                Ok(Transformed::yes(child_with_fetch))
            } else {
                Ok(Transformed::no(plan))
            }
        }
    } else {
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::BinaryExpr;
    use datafusion_physical_expr::Partitioning;
    use datafusion_physical_expr_common::expressions::column::col;
    use datafusion_physical_expr_common::expressions::lit;
    use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
    use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::get_plan_string;
    use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use datafusion_physical_plan::projection::ProjectionExec;
    use datafusion_physical_plan::repartition::RepartitionExec;
    use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use super::*;
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
    fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_zero() -> Result<()> {
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
    fn transforms_streaming_table_exec_into_fetching_version_and_keeps_the_global_limit_when_skip_is_nonzero() -> Result<()> {
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
    fn transforms_coalesce_batches_exec_into_fetching_version_and_removes_local_limit() -> Result<()> {
        let schema = create_schema();

        let streaming_table = streaming_table_exec(schema.clone())?;

        let repartition = Arc::new(RepartitionExec::try_new(
            streaming_table,
            Partitioning::RoundRobinBatch(8),
        )?);

        let coalesce_batches = Arc::new(CoalesceBatchesExec::new(
            filter_exec(schema.clone(), repartition)?,
            8192,
        ));

        let local_limit = local_limit_exec(coalesce_batches, 5);

        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(
            local_limit
        ));

        let global_limit= global_limit_exec(coalesce_partitions, 0, Some(5));

        let initial = get_plan_string(&global_limit);
        let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    LocalLimitExec: fetch=5",
            "      CoalesceBatchesExec: target_batch_size=8192, fetch=None",
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

        let projection = Arc::new(ProjectionExec::try_new(
            vec![
                (col("c1", schema.as_ref()).unwrap(), "c1".to_string()),
                (col("c2", schema.as_ref()).unwrap(), "c2".to_string()),
                (col("c3", schema.as_ref()).unwrap(), "c3".to_string()),
            ],
            filter,
        )?);

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

    fn global_limit_exec(input: Arc<dyn ExecutionPlan>, skip: usize, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(input, skip, fetch))
    }

    fn local_limit_exec(input: Arc<dyn ExecutionPlan>, fetch: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(LocalLimitExec::new(input, fetch))
    }

    fn filter_exec(schema: SchemaRef, input: Arc<dyn ExecutionPlan>) -> Result<Arc<FilterExec>> {
        Ok(Arc::new(FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                col("c3", schema.as_ref()).unwrap(),
                Operator::Gt,
                lit(0))),
            input,
        )?))
    }
}