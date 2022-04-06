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

//! Repartition optimizer that introduces repartition nodes to increase the level of parallism available
use std::sync::Arc;

use super::optimizer::PhysicalOptimizerRule;
use crate::physical_plan::Partitioning::*;
use crate::physical_plan::{
    repartition::RepartitionExec, with_new_children_if_necessary, ExecutionPlan,
};
use crate::{error::Result, execution::context::SessionConfig};

/// Optimizer that introduces repartition to introduce more
/// parallelism in the plan
///
/// For example, given an input such as:
///
///
/// ```text
/// ┌─────────────────────────────────┐
/// │                                 │
/// │          ExecutionPlan          │
/// │                                 │
/// └─────────────────────────────────┘
///             ▲         ▲
///             │         │
///       ┌─────┘         └─────┐
///       │                     │
///       │                     │
///       │                     │
/// ┌───────────┐         ┌───────────┐
/// │           │         │           │
/// │ batch A1  │         │ batch B1  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A2  │         │ batch B2  │
/// │           │         │           │
/// ├───────────┤         ├───────────┤
/// │           │         │           │
/// │ batch A3  │         │ batch B3  │
/// │           │         │           │
/// └───────────┘         └───────────┘
///
///      Input                 Input
///        A                     B
/// ```
///
/// This optimizer will attempt to add a `RepartitionExec` to increase
/// the parallism (to 3 in this case)
///
/// ```text
///     ┌─────────────────────────────────┐
///     │                                 │
///     │          ExecutionPlan          │
///     │                                 │
///     └─────────────────────────────────┘
///               ▲      ▲       ▲            Input now has 3
///               │      │       │             partitions
///       ┌───────┘      │       └───────┐
///       │              │               │
///       │              │               │
/// ┌───────────┐  ┌───────────┐   ┌───────────┐
/// │           │  │           │   │           │
/// │ batch A1  │  │ batch A3  │   │ batch B3  │
/// │           │  │           │   │           │
/// ├───────────┤  ├───────────┤   ├───────────┤
/// │           │  │           │   │           │
/// │ batch B2  │  │ batch B1  │   │ batch A2  │
/// │           │  │           │   │           │
/// └───────────┘  └───────────┘   └───────────┘
///       ▲              ▲               ▲
///       │              │               │
///       └─────────┐    │    ┌──────────┘
///                 │    │    │
///                 │    │    │
///     ┌─────────────────────────────────┐   batches are
///     │       RepartitionExec(3)        │   repartitioned
///     │           RoundRobin            │
///     │                                 │
///     └─────────────────────────────────┘
///                 ▲         ▲
///                 │         │
///           ┌─────┘         └─────┐
///           │                     │
///           │                     │
///           │                     │
///     ┌───────────┐         ┌───────────┐
///     │           │         │           │
///     │ batch A1  │         │ batch B1  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A2  │         │ batch B2  │
///     │           │         │           │
///     ├───────────┤         ├───────────┤
///     │           │         │           │
///     │ batch A3  │         │ batch B3  │
///     │           │         │           │
///     └───────────┘         └───────────┘
///
///
///      Input                 Input
///        A                     B
/// ```
#[derive(Default)]
pub struct Repartition {}

impl Repartition {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Recursively visits all `plan`s puts and then optionally adds a
/// `RepartitionExec` at the output of `plan` to match
/// `target_partitions` in an attempt to increase the overall parallelism.
///
/// It does so using depth first scan of the tree, and repartitions
/// any plan that:
///
/// 1. Has fewer partitions than `target_partitions`
///
/// 2. Has a direct parent that `benefits_from_input_partitioning`
///
/// 3. Does not have a parent that `relies_on_input_order` unless there
/// is an intervening node that does not `maintain_input_order`
///
/// if `can_reorder` is false, means that the output of this node
/// can not be reordered as as something upstream is relying on that order
///
/// If 'would_benefit` is false, the upstream operator doesn't
///  benefit from additional repartition
///
fn optimize_partitions(
    target_partitions: usize,
    plan: Arc<dyn ExecutionPlan>,
    can_reorder: bool,
    would_benefit: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Recurse into children bottom-up (attempt to repartition as
    // early as possible)

    let new_plan = if plan.children().is_empty() {
        // leaf node - don't replace children
        plan
    } else {
        let can_reorder_children =
            match (plan.relies_on_input_order(), plan.maintains_input_order()) {
                (true, _) => {
                    // `plan` itself relies on the order of its
                    // children, so don't reorder them!
                    false
                }
                (false, false) => {
                    // `plan` may reorder the input itself, so no need
                    // to preserve the order of any children
                    true
                }
                (false, true) => {
                    // `plan` will maintain the order, so we can only
                    // repartition children if it is ok to reorder the
                    // output of this node
                    can_reorder
                }
            };

        let children = plan
            .children()
            .iter()
            .map(|child| {
                optimize_partitions(
                    target_partitions,
                    child.clone(),
                    can_reorder_children,
                    plan.benefits_from_input_partitioning(),
                )
            })
            .collect::<Result<_>>()?;
        with_new_children_if_necessary(plan, children)?
    };

    // decide if we should bother trying to repartition the output of this plan
    let could_repartition = match new_plan.output_partitioning() {
        // Apply when underlying node has less than `self.target_partitions` amount of concurrency
        RoundRobinBatch(x) => x < target_partitions,
        UnknownPartitioning(x) => x < target_partitions,
        // we don't want to introduce partitioning after hash partitioning
        // as the plan will likely depend on this
        Hash(_, _) => false,
    };

    if would_benefit && could_repartition && can_reorder {
        Ok(Arc::new(RepartitionExec::try_new(
            new_plan,
            RoundRobinBatch(target_partitions),
        )?))
    } else {
        Ok(new_plan)
    }
}

impl PhysicalOptimizerRule for Repartition {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Don't run optimizer if target_partitions == 1
        if config.target_partitions == 1 {
            Ok(plan)
        } else {
            optimize_partitions(config.target_partitions, plan, false, false)
        }
    }

    fn name(&self) -> &str {
        "repartition"
    }
}
#[cfg(test)]
mod tests {
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::physical_plan::expressions::{col, PhysicalSortExpr};
    use crate::physical_plan::file_format::{FileScanConfig, ParquetExec};
    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::hash_aggregate::{AggregateMode, HashAggregateExec};
    use crate::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
    use crate::physical_plan::projection::ProjectionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::union::UnionExec;
    use crate::physical_plan::{displayable, Statistics};
    use crate::test::object_store::TestObjectStore;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("c1", DataType::Boolean, true)]))
    }

    fn parquet_exec() -> Arc<ParquetExec> {
        Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store: TestObjectStore::new_arc(&[("x", 100)]),
                file_schema: schema(),
                file_groups: vec![vec![PartitionedFile::new("x".to_string(), 100)]],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            None,
        ))
    }

    fn sort_preserving_merge_exec(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let expr = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: arrow::compute::SortOptions::default(),
        }];

        Arc::new(SortPreservingMergeExec::new(expr, input))
    }

    fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(FilterExec::try_new(col("c1", &schema()).unwrap(), input).unwrap())
    }

    fn sort_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = vec![PhysicalSortExpr {
            expr: col("c1", &schema()).unwrap(),
            options: SortOptions::default(),
        }];
        Arc::new(SortExec::try_new(sort_exprs, input).unwrap())
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let exprs = vec![(col("c1", &schema()).unwrap(), "c1".to_string())];
        Arc::new(ProjectionExec::try_new(exprs, input).unwrap())
    }

    fn hash_aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let schema = schema();
        Arc::new(
            HashAggregateExec::try_new(
                AggregateMode::Final,
                vec![],
                vec![],
                Arc::new(
                    HashAggregateExec::try_new(
                        AggregateMode::Partial,
                        vec![],
                        vec![],
                        input,
                        schema.clone(),
                    )
                    .unwrap(),
                ),
                schema,
            )
            .unwrap(),
        )
    }

    fn limit_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(GlobalLimitExec::new(
            Arc::new(LocalLimitExec::new(input, 100)),
            100,
        ))
    }

    fn trim_plan_display(plan: &str) -> Vec<&str> {
        plan.split('\n')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Runs the repartition optimizer and asserts the plan against the expected
    macro_rules! assert_optimized {
        ($EXPECTED_LINES: expr, $PLAN: expr) => {
            let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

            // run optimizer
            let optimizer = Repartition {};
            let optimized = optimizer
                .optimize($PLAN, &SessionConfig::new().with_target_partitions(10))?;

            // Now format correctly
            let plan = displayable(optimized.as_ref()).indent().to_string();
            let actual_lines = trim_plan_display(&plan);

            assert_eq!(
                &expected_lines, &actual_lines,
                "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
                expected_lines, actual_lines
            );
        };
    }

    #[test]
    fn added_repartition_to_single_partition() -> Result<()> {
        let plan = hash_aggregate(parquet_exec());

        let expected = [
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_deepest_node() -> Result<()> {
        let plan = hash_aggregate(filter_exec(parquet_exec()));

        let expected = &[
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_unsorted_limit() -> Result<()> {
        let plan = limit_exec(filter_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            "FilterExec: c1@0",
            // nothing sorts the data, so the local limit doesn't require sorted data either
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit() -> Result<()> {
        let plan = limit_exec(sort_exec(parquet_exec()));

        let expected = &[
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            // data is sorted so can't repartition here
            "SortExec: [c1@0 ASC]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_sorted_limit_with_filter() -> Result<()> {
        let plan = limit_exec(filter_exec(sort_exec(parquet_exec())));

        let expected = &[
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            "FilterExec: c1@0",
            // data is sorted so can't repartition here even though
            // filter would benefit from parallelism, the answers might be wrong
            "SortExec: [c1@0 ASC]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_limit() -> Result<()> {
        let plan = hash_aggregate(limit_exec(filter_exec(limit_exec(parquet_exec()))));

        let expected = &[
            "HashAggregateExec: mode=Final, gby=[], aggr=[]",
            "HashAggregateExec: mode=Partial, gby=[], aggr=[]",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            "FilterExec: c1@0",
            // repartition should happen prior to the filter to maximize parallelism
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "GlobalLimitExec: limit=100",
            "LocalLimitExec: limit=100",
            // Expect no repartition to happen for local limit
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    // repartition works differently for limit when there is a sort below it

    #[test]
    fn repartition_ignores_union() -> Result<()> {
        let plan = Arc::new(UnionExec::new(vec![parquet_exec(); 5]));

        let expected = &[
            "UnionExec",
            // Expect no repartition of ParquetExec
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_ignores_sort_preserving_merge() -> Result<()> {
        let plan = sort_preserving_merge_exec(parquet_exec());

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect no repartition of SortPreservingMergeExec
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_does_not_repartition_transitively() -> Result<()> {
        let plan = sort_preserving_merge_exec(projection_exec(parquet_exec()));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect no repartition of SortPreservingMergeExec
            // even though there is a projection exec between it
            "ProjectionExec: expr=[c1@0 as c1]",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection() -> Result<()> {
        let plan = sort_preserving_merge_exec(sort_exec(projection_exec(parquet_exec())));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_filter() -> Result<()> {
        let plan = sort_preserving_merge_exec(sort_exec(filter_exec(parquet_exec())));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "FilterExec: c1@0",
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }

    #[test]
    fn repartition_transitively_past_sort_with_projection_and_filter() -> Result<()> {
        let plan = sort_preserving_merge_exec(sort_exec(projection_exec(filter_exec(
            parquet_exec(),
        ))));

        let expected = &[
            "SortPreservingMergeExec: [c1@0 ASC]",
            // Expect repartition on the input to the sort (as it can benefit from additional parallelism)
            "SortExec: [c1@0 ASC]",
            "ProjectionExec: expr=[c1@0 as c1]",
            "FilterExec: c1@0",
            // repartition is lowest down
            "RepartitionExec: partitioning=RoundRobinBatch(10)",
            "ParquetExec: limit=None, partitions=[x], projection=[c1]",
        ];

        assert_optimized!(expected, plan);
        Ok(())
    }
}
