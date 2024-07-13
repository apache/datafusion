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
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
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
    if let Some(global_limit) = plan.as_any().downcast_ref::<GlobalLimitExec>() {
        let child = global_limit.input();
        let fetch = global_limit.fetch();
        let skip = global_limit.skip();

        let child_fetch = fetch.map_or(None, |f| Some(f + skip));

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
    } else if let Some(local_limit) = plan.as_any().downcast_ref::<LocalLimitExec>() {
        let child = local_limit.input();
        let fetch = local_limit.fetch();

        if let Some(child_with_fetch) = child.with_fetch(Some(fetch)) {
            Ok(Transformed::yes(child_with_fetch))
        } else {
            Ok(Transformed::no(plan))
        }
    }
    else {
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
    fn transforms_streaming_table_exec_into_fetching_version() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Float64, true),
        ]));

        let streaming_table = StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(DummyStreamPartition {
                schema: schema.clone(),
            }) as _],
            None,
            None,
            true,
            None,
        )?;

        let global_limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(streaming_table),
            0,
            Some(5),
        ));

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
    fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_nonzero() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Utf8, true),
            Field::new("c3", DataType::Float64, true),
        ]));

        let streaming_table = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(DummyStreamPartition {
                schema: schema.clone(),
            }) as _],
            None,
            None,
            true,
            None,
        )?);

        let global_limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            streaming_table,
            2,
            Some(5),
        ));

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
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));

        let streaming_table = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(DummyStreamPartition {
                schema: schema.clone(),
            }) as _],
            None,
            None,
            true,
            None,
        )?);

        let repartition = Arc::new(RepartitionExec::try_new(
            streaming_table,
            Partitioning::RoundRobinBatch(8)
        )?);

        let coalesce_batches = Arc::new(CoalesceBatchesExec::new(
            Arc::new(FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    col("c3", schema.as_ref()).unwrap(),
                    Operator::Gt,
                    lit(0))),
                repartition)?
            ),
            8192,
        ));

        let local_limit = Arc::new(LocalLimitExec::new(
            coalesce_batches,
            5,
        ));

        let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(
            local_limit
        ));

        let global_limit: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            coalesce_partitions,
            0,
            Some(5),
        ));

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
}