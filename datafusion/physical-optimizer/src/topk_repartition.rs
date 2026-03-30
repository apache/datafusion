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

//! Push TopK (Sort with fetch) past Hash Repartition
//!
//! When a `SortExec` with a fetch limit (TopK) sits above a
//! `RepartitionExec(Hash)`, and the hash partition expressions are a prefix
//! of the sort expressions, this rule inserts a copy of the TopK below
//! the repartition to reduce the volume of data flowing through the shuffle.
//!
//! This is correct because the hash partition key being a prefix of the sort
//! key guarantees that all rows with the same partition key end up in the same
//! output partition. Therefore, rows that survive the final TopK after
//! repartitioning will always survive the pre-repartition TopK as well.
//!
//! ## Example
//!
//! Before:
//! ```text
//! SortExec: TopK(fetch=3), expr=[a ASC, b ASC]
//!   RepartitionExec: Hash([a], 4)
//!     DataSourceExec
//! ```
//!
//! After:
//! ```text
//! SortExec: TopK(fetch=3), expr=[a ASC, b ASC]
//!   RepartitionExec: Hash([a], 4)
//!     SortExec: TopK(fetch=3), expr=[a ASC, b ASC]
//!       DataSourceExec
//! ```

use crate::PhysicalOptimizerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use std::sync::Arc;
// CoalesceBatchesExec is deprecated on main (replaced by arrow-rs BatchCoalescer),
// but older DataFusion versions may still insert it between SortExec and RepartitionExec.
#[expect(deprecated)]
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::{ExecutionPlan, Partitioning};

/// A physical optimizer rule that pushes TopK (Sort with fetch) past
/// hash repartition when the partition key is a prefix of the sort key.
///
/// See module-level documentation for details.
#[derive(Debug, Clone, Default)]
pub struct TopKRepartition;

impl TopKRepartition {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for TopKRepartition {
    #[expect(deprecated)] // CoalesceBatchesExec: kept for older DataFusion versions
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_topk_repartition {
            return Ok(plan);
        }
        plan.transform_down(|node| {
            // Match SortExec with fetch (TopK)
            let Some(sort_exec) = node.as_any().downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(fetch) = sort_exec.fetch() else {
                return Ok(Transformed::no(node));
            };

            // The child might be a CoalesceBatchesExec; look through it
            let sort_input = sort_exec.input();
            let sort_any = sort_input.as_any();
            let (repart_parent, repart_exec) = if let Some(rp) =
                sort_any.downcast_ref::<RepartitionExec>()
            {
                // found a RepartitionExec, use it
                (None, rp)
            } else if let Some(cb_exec) = sort_any.downcast_ref::<CoalesceBatchesExec>() {
                // There's a CoalesceBatchesExec between TopK & RepartitionExec
                // in this case we will need to reconstruct both nodes
                let cb_input = cb_exec.input();
                let Some(rp) = cb_input.as_any().downcast_ref::<RepartitionExec>() else {
                    return Ok(Transformed::no(node));
                };
                (Some(Arc::clone(sort_input)), rp)
            } else {
                return Ok(Transformed::no(node));
            };

            // Only handle Hash partitioning
            let Partitioning::Hash(hash_exprs, num_partitions) =
                repart_exec.partitioning()
            else {
                return Ok(Transformed::no(node));
            };

            let sort_exprs = sort_exec.expr();

            // Check that hash expressions are a prefix of the sort expressions.
            // Each hash expression must match the corresponding sort expression
            // (ignoring sort options like ASC/DESC since hash doesn't care about order).
            if hash_exprs.len() > sort_exprs.len() {
                return Ok(Transformed::no(node));
            }
            for (hash_expr, sort_expr) in hash_exprs.iter().zip(sort_exprs.iter()) {
                if !hash_expr.eq(&sort_expr.expr) {
                    return Ok(Transformed::no(node));
                }
            }

            // Don't push if the input to the repartition is already bounded
            // (e.g., another TopK), as it would be redundant.
            let repart_input = repart_exec.input();
            if repart_input.as_any().downcast_ref::<SortExec>().is_some() {
                return Ok(Transformed::no(node));
            }

            // Insert a copy of the TopK below the repartition
            let new_sort: Arc<dyn ExecutionPlan> = Arc::new(
                SortExec::new(sort_exprs.clone(), Arc::clone(repart_input))
                    .with_fetch(Some(fetch))
                    .with_preserve_partitioning(sort_exec.preserve_partitioning()),
            );

            let new_partitioning =
                Partitioning::Hash(hash_exprs.clone(), *num_partitions);
            let new_repartition: Arc<dyn ExecutionPlan> =
                Arc::new(RepartitionExec::try_new(new_sort, new_partitioning)?);

            // Rebuild the tree above the repartition
            let new_sort_input = if let Some(parent) = repart_parent {
                parent.with_new_children(vec![new_repartition])?
            } else {
                new_repartition
            };

            let new_top_sort: Arc<dyn ExecutionPlan> = Arc::new(
                SortExec::new(sort_exprs.clone(), new_sort_input)
                    .with_fetch(Some(fetch))
                    .with_preserve_partitioning(sort_exec.preserve_partitioning()),
            );

            Ok(Transformed::yes(new_top_sort))
        })
        .data()
    }

    fn name(&self) -> &str {
        "TopKRepartition"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::test::scan_partitioned;
    use insta::assert_snapshot;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]))
    }

    fn sort_exprs(schema: &Schema) -> LexOrdering {
        LexOrdering::new(vec![
            PhysicalSortExpr::new_default(col("a", schema).unwrap()).asc(),
            PhysicalSortExpr::new_default(col("b", schema).unwrap()).asc(),
        ])
        .unwrap()
    }

    /// TopK above Hash(a) repartition should get pushed below it,
    /// because `a` is a prefix of the sort key `(a, b)`.
    #[test]
    fn topk_pushed_below_hash_repartition() {
        let s = schema();
        let input = scan_partitioned(1);
        let ordering = sort_exprs(&s);

        let repartition = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("a", &s).unwrap()], 4),
            )
            .unwrap(),
        );

        let sort = Arc::new(
            SortExec::new(ordering, repartition)
                .with_fetch(Some(3))
                .with_preserve_partitioning(true),
        );

        let config = ConfigOptions::new();
        let optimized = TopKRepartition::new().optimize(sort, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_snapshot!(display, @r"
        SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true], sort_prefix=[a@0 ASC]
          RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1, maintains_sort_order=true
            SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
              DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    /// TopK with no fetch (unbounded sort) should NOT be pushed.
    #[test]
    fn unbounded_sort_not_pushed() {
        let s = schema();
        let input = scan_partitioned(1);
        let ordering = sort_exprs(&s);

        let repartition = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("a", &s).unwrap()], 4),
            )
            .unwrap(),
        );

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, repartition).with_preserve_partitioning(true),
        );

        let config = ConfigOptions::new();
        let optimized = TopKRepartition::new().optimize(sort, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_snapshot!(display, @r"
        SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    /// Hash key NOT a prefix of sort key should NOT be pushed.
    #[test]
    fn non_prefix_hash_key_not_pushed() {
        let s = schema();
        let input = scan_partitioned(1);
        let ordering = sort_exprs(&s);

        // Hash by `b`, but sort by `(a, b)` - b is not a prefix
        let repartition = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("b", &s).unwrap()], 4),
            )
            .unwrap(),
        );

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, repartition)
                .with_fetch(Some(3))
                .with_preserve_partitioning(true),
        );

        let config = ConfigOptions::new();
        let optimized = TopKRepartition::new().optimize(sort, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_snapshot!(display, @r"
        SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=Hash([b@1], 4), input_partitions=1
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    /// TopK above CoalesceBatchesExec above Hash(a) repartition should
    /// push through both, inserting a new TopK below the repartition.
    #[expect(deprecated)]
    #[test]
    fn topk_pushed_through_coalesce_batches() {
        let s = schema();
        let input = scan_partitioned(1);
        let ordering = sort_exprs(&s);

        let repartition = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("a", &s).unwrap()], 4),
            )
            .unwrap(),
        );

        let coalesce: Arc<dyn ExecutionPlan> =
            Arc::new(CoalesceBatchesExec::new(repartition, 8192));

        let sort = Arc::new(
            SortExec::new(ordering, coalesce)
                .with_fetch(Some(3))
                .with_preserve_partitioning(true),
        );

        let config = ConfigOptions::new();
        let optimized = TopKRepartition::new().optimize(sort, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_snapshot!(display, @r"
        SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true], sort_prefix=[a@0 ASC]
          CoalesceBatchesExec: target_batch_size=8192
            RepartitionExec: partitioning=Hash([a@0], 4), input_partitions=1, maintains_sort_order=true
              SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
                DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    /// RoundRobin repartition should NOT be pushed.
    #[test]
    fn round_robin_not_pushed() {
        let s = schema();
        let input = scan_partitioned(1);
        let ordering = sort_exprs(&s);

        let repartition = Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4)).unwrap(),
        );

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering, repartition)
                .with_fetch(Some(3))
                .with_preserve_partitioning(true),
        );

        let config = ConfigOptions::new();
        let optimized = TopKRepartition::new().optimize(sort, &config).unwrap();

        let display = displayable(optimized.as_ref()).indent(true).to_string();
        assert_snapshot!(display, @r"
        SortExec: TopK(fetch=3), expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[true]
          RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }
}
