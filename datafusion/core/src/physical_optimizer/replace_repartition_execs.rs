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

//! Repartition optimizer that replaces `SortExec`s and their suitable `RepartitionExec` children with `SortPreservingRepartitionExec`s.
use crate::error::Result;
use crate::physical_optimizer::sort_enforcement::unbounded_output;
use crate::physical_plan::repartition::RepartitionExec;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::ExecutionPlan;

use super::utils::is_repartition;

use datafusion_common::tree_node::Transformed;
use datafusion_physical_expr::utils::ordering_satisfy;

use itertools::enumerate;
use std::sync::Arc;

/// Creates a `SortPreservingRepartitionExec` from given `RepartitionExec`
fn sort_preserving_repartition(
    repartition: &RepartitionExec,
) -> Result<Arc<RepartitionExec>> {
    Ok(Arc::new(
        RepartitionExec::try_new(
            repartition.input().clone(),
            repartition.partitioning().clone(),
        )?
        .with_preserve_order(),
    ))
}

fn does_plan_maintain_input_order(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.maintains_input_order().iter().any(|flag| *flag)
}

/// Check the children nodes of a `SortExec` until ordering is lost (e.g. until
/// another `SortExec` or a `CoalescePartitionsExec` which doesn't maintain ordering)
/// and replace `RepartitionExec`s that do not maintain ordering (e.g. those whose
/// input partition counts are larger than unity) with `SortPreservingRepartitionExec`s.
/// Note that doing this may render the `SortExec` in question unneccessary, which will
/// be removed later on.
///
/// For example, we transform the plan below
/// "FilterExec: c@2 > 3",
/// "  RepartitionExec: partitioning=Hash(\[b@0], 16), input_partitions=16",
/// "    RepartitionExec: partitioning=Hash(\[a@0], 16), input_partitions=1",
/// "      MemoryExec: partitions=1, partition_sizes=\[(<depends_on_batch_size>)], output_ordering: \[PhysicalSortExpr { expr: Column { name: \"a\", index: 0 }, options: SortOptions { descending: false, nulls_first: false } }]",
/// into
/// "FilterExec: c@2 > 3",
/// "  SortPreservingRepartitionExec: partitioning=Hash(\[b@0], 16), input_partitions=16",
/// "    RepartitionExec: partitioning=Hash(\[a@0], 16), input_partitions=1",
/// "      MemoryExec: partitions=1, partition_sizes=\[<depends_on_batch_size>], output_ordering: \[PhysicalSortExpr { expr: Column { name: \"a\", index: 0 }, options: SortOptions { descending: false, nulls_first: false } }]",
/// where the `FilterExec` in the latter has output ordering `a ASC`. This ordering will
/// potentially remove a `SortExec` at the top of `FilterExec`. If this doesn't help remove
/// a `SortExec`, the old version is used.
fn replace_sort_children(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.children().is_empty() {
        return Ok(plan.clone());
    }

    let mut children = plan.children();
    for (idx, child) in enumerate(plan.children()) {
        if !is_repartition(&child) && !does_plan_maintain_input_order(&child) {
            break;
        }

        if let Some(repartition) = child.as_any().downcast_ref::<RepartitionExec>() {
            // Replace this `RepartitionExec` with a `SortPreservingRepartitionExec`
            // if it doesn't preserve ordering and its input is unbounded. Doing
            // so avoids breaking the pipeline.
            if !repartition.maintains_input_order()[0] && unbounded_output(&child) {
                let spr = sort_preserving_repartition(repartition)?
                    .with_new_children(repartition.children())?;
                // Perform the replacement and recurse into this plan's children:
                children[idx] = replace_sort_children(&spr)?;
                continue;
            }
        }

        children[idx] = replace_sort_children(&child)?;
    }

    plan.clone().with_new_children(children)
}

/// The `replace_repartition_execs` optimizer sub-rule searches for `SortExec`s
/// and their `RepartitionExec` children with multiple input partitioning having
/// local (per-partition) ordering, so that it can replace the `RepartitionExec`
/// with a `SortPreservingRepartitionExec` and remove the pipeline-breaking `SortExec`
/// from the physical plan.
///
/// The algorithm flow is simply like this:
/// 1. Visit nodes of the physical plan top-down and look for `SortExec` nodes.
/// 2. If a `SortExec` is found, iterate over its children recursively until an
///    executor that doesn't maintain ordering is encountered (or until a leaf node).
///    `RepartitionExec`s with multiple input partitions are considered as if they
///    maintain input ordering because they are potentially replaceable with
///    `SortPreservingRepartitionExec`s which maintain ordering.
/// 3_1. Replace the `RepartitionExec`s with multiple input partitions (which doesn't
///      maintain ordering) with a `SortPreservingRepartitionExec`.
/// 3_2. Otherwise, keep the plan as is.
/// 4. Check if the `SortExec` is still necessary in the updated plan by comparing
///    its input ordering with the output ordering it imposes. We do this because
///     replacing `RepartitionExec`s with `SortPreservingRepartitionExec`s enables us
///     to preserve the previously lost ordering during `RepartitionExec`s.
/// 5_1. If the `SortExec` in question turns out to be unnecessary, remove it and use
///      updated plan. Otherwise, use the original plan.
/// 6. Continue the top-down iteration until another `SortExec` is seen, or the iterations finish.
pub fn replace_repartition_execs(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        let changed_plan = replace_sort_children(&plan)?;
        // Since we have a `SortExec` here, it's guaranteed that it has a single child.
        let input = &changed_plan.children()[0];
        // Check if any child is changed, if so remove the `SortExec`. If the ordering
        // is being satisfied with the child, then it means `SortExec` is unnecessary.
        if ordering_satisfy(
            input.output_ordering(),
            sort_exec.output_ordering(),
            || input.equivalence_properties(),
            || input.ordering_equivalence_properties(),
        ) {
            Ok(Transformed::Yes(input.clone()))
        } else {
            Ok(Transformed::No(plan))
        }
    } else {
        // We don't have anything to do until we get to the `SortExec` parent.
        Ok(Transformed::No(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datasource::file_format::file_type::FileCompressionType;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::physical_plan::{CsvExec, FileScanConfig};
    use crate::physical_plan::coalesce_batches::CoalesceBatchesExec;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;

    use crate::physical_plan::filter::FilterExec;
    use crate::physical_plan::joins::{HashJoinExec, PartitionMode};
    use crate::physical_plan::repartition::RepartitionExec;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use crate::physical_plan::{displayable, Partitioning};

    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::{Result, Statistics};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::{JoinType, Operator};
    use datafusion_physical_expr::expressions::{self, col, Column};
    use datafusion_physical_expr::PhysicalSortExpr;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    /// Runs the `replace_repartition_execs` sub-rule and asserts the plan
    /// against the original and expected plans.
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
    /// `$PLAN`: the plan to optimized
    macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr) => {
            let physical_plan = $PLAN;
            let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
            let actual: Vec<&str> = formatted.trim().lines().collect();

            let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
                .iter().map(|s| *s).collect();

            assert_eq!(
                expected_plan_lines, actual,
                "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

            let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES.iter().map(|s| *s).collect();

            // Run the rule top-down
            let optimized_physical_plan = physical_plan.transform_down(&replace_repartition_execs)?;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );
        };
    }

    #[tokio::test]
    // Searches for a simple sort and a repartition just after it, the second repartition with 1 input partition should not be affected
    async fn test_replace_multiple_input_repartition_1() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_inter_children_change_only() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr_default("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let sort = sort_exec(
            vec![sort_expr_default("a", &schema)],
            coalesce_partitions,
            false,
        );
        let repartition_rr2 = repartition_exec_round_robin(sort);
        let repartition_hash2 = repartition_exec_hash(repartition_rr2);
        let filter = filter_exec(repartition_hash2, &schema);
        let sort2 = sort_exec(vec![sort_expr_default("a", &schema)], filter, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr_default("a", &schema)], sort2);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    FilterExec: c@2 > 3",
            "      RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@2 > 3",
            "    SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[a@0 ASC]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_2() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let filter = filter_exec(repartition_rr, &schema);
        let repartition_hash = repartition_exec_hash(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      FilterExec: c@2 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "    FilterExec: c@2 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash, &schema);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);
        let sort = sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@2 > 3",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@2 > 3",
            "      SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_replace_multiple_input_repartition_with_extra_steps_2() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let coalesce_batches_exec_1 = coalesce_batches_exec(repartition_rr);
        let repartition_hash = repartition_exec_hash(coalesce_batches_exec_1);
        let filter = filter_exec(repartition_hash, &schema);
        let coalesce_batches_exec_2 = coalesce_batches_exec(filter);
        let sort =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@2 > 3",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@2 > 3",
            "      SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replacing_when_no_need_to_preserve_sorting() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash, &schema);
        let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);

        let physical_plan: Arc<dyn ExecutionPlan> =
            coalesce_partitions_exec(coalesce_batches_exec);

        let expected_input = vec![
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@2 > 3",
            "      RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@2 > 3",
            "      RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_multiple_replacable_repartitions() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let filter = filter_exec(repartition_hash, &schema);
        let coalesce_batches = coalesce_batches_exec(filter);
        let repartition_hash_2 = repartition_exec_hash(coalesce_batches);
        let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash_2, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST]",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@2 > 3",
            "          RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@2 > 3",
            "        SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_not_replace_with_different_orderings() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let sort = sort_exec(
            vec![sort_expr_default("c", &schema)],
            repartition_hash,
            true,
        );

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr_default("c", &schema)], sort);

        let expected_input = vec![
            "SortPreservingMergeExec: [c@2 ASC]",
            "  SortExec: expr=[c@2 ASC]",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortPreservingMergeExec: [c@2 ASC]",
            "  SortExec: expr=[c@2 ASC]",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let physical_plan =
            sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions, false);

        let expected_input = vec![
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        let expected_optimized = vec![
            "SortExec: expr=[a@0 ASC NULLS LAST]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_lost_and_kept_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let sort_exprs = vec![sort_expr("a", &schema)];
        let source = csv_exec_sorted(&schema, sort_exprs, true);
        let repartition_rr = repartition_exec_round_robin(source);
        let repartition_hash = repartition_exec_hash(repartition_rr);
        let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
        let sort = sort_exec(
            vec![sort_expr_default("c", &schema)],
            coalesce_partitions,
            false,
        );
        let repartition_rr2 = repartition_exec_round_robin(sort);
        let repartition_hash2 = repartition_exec_hash(repartition_rr2);
        let filter = filter_exec(repartition_hash2, &schema);
        let sort2 = sort_exec(vec![sort_expr_default("c", &schema)], filter, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr_default("c", &schema)], sort2);

        let expected_input = [
            "SortPreservingMergeExec: [c@2 ASC]",
            "  SortExec: expr=[c@2 ASC]",
            "    FilterExec: c@2 > 3",
            "      RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@2 ASC]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [c@2 ASC]",
            "  FilterExec: c@2 > 3",
            "    SortPreservingRepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@2 ASC]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_multiple_child_trees() -> Result<()> {
        let schema = create_test_schema()?;

        let left_sort_exprs = vec![sort_expr("a", &schema)];
        let left_source = csv_exec_sorted(&schema, left_sort_exprs, true);
        let left_repartition_rr = repartition_exec_round_robin(left_source);
        let left_repartition_hash = repartition_exec_hash(left_repartition_rr);
        let left_coalesce_partitions =
            Arc::new(CoalesceBatchesExec::new(left_repartition_hash, 4096));

        let right_sort_exprs = vec![sort_expr("a", &schema)];
        let right_source = csv_exec_sorted(&schema, right_sort_exprs, true);
        let right_repartition_rr = repartition_exec_round_robin(right_source);
        let right_repartition_hash = repartition_exec_hash(right_repartition_rr);
        let right_coalesce_partitions =
            Arc::new(CoalesceBatchesExec::new(right_repartition_hash, 4096));

        let hash_join_exec =
            hash_join_exec(left_coalesce_partitions, right_coalesce_partitions);
        let sort = sort_exec(vec![sort_expr_default("a", &schema)], hash_join_exec, true);

        let physical_plan =
            sort_preserving_merge_exec(vec![sort_expr_default("a", &schema)], sort);

        let expected_input = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];

        let expected_optimized = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c1@0], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST], has_header=true",
        ];
        assert_optimized!(expected_input, expected_optimized, physical_plan);
        Ok(())
    }

    // End test cases
    // Start test helpers

    fn sort_expr(name: &str, schema: &Schema) -> PhysicalSortExpr {
        let sort_opts = SortOptions {
            nulls_first: false,
            descending: false,
        };
        sort_expr_options(name, schema, sort_opts)
    }

    fn sort_expr_default(name: &str, schema: &Schema) -> PhysicalSortExpr {
        let sort_opts = SortOptions::default();
        sort_expr_options(name, schema, sort_opts)
    }

    fn sort_expr_options(
        name: &str,
        schema: &Schema,
        options: SortOptions,
    ) -> PhysicalSortExpr {
        PhysicalSortExpr {
            expr: col(name, schema).unwrap(),
            options,
        }
    }

    fn sort_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(
            SortExec::new(sort_exprs, input)
                .with_preserve_partitioning(preserve_partitioning),
        )
    }

    fn sort_preserving_merge_exec(
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        Arc::new(SortPreservingMergeExec::new(sort_exprs, input))
    }

    fn repartition_exec_round_robin(
        input: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(8)).unwrap(),
        )
    }

    fn repartition_exec_hash(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![Arc::new(Column::new("c1", 0))], 8),
            )
            .unwrap(),
        )
    }

    fn filter_exec(
        input: Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
    ) -> Arc<dyn ExecutionPlan> {
        let predicate = expressions::binary(
            col("c", schema).unwrap(),
            Operator::Gt,
            expressions::lit(3i32),
            schema,
        )
        .unwrap();
        Arc::new(FilterExec::try_new(predicate, input).unwrap())
    }

    fn coalesce_batches_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalesceBatchesExec::new(input, 8192))
    }

    fn coalesce_partitions_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(input))
    }

    fn hash_join_exec(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                vec![(Column::new("c", 1), Column::new("c", 1))],
                None,
                &JoinType::Inner,
                PartitionMode::Partitioned,
                false,
            )
            .unwrap(),
        )
    }

    fn create_test_schema() -> Result<SchemaRef> {
        let column_a = Field::new("a", DataType::Int32, false);
        let column_b = Field::new("b", DataType::Int32, false);
        let column_c = Field::new("c", DataType::Int32, false);
        let column_d = Field::new("d", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![column_a, column_b, column_c, column_d]));

        Ok(schema)
    }

    // creates a csv exec source for the test purposes
    // projection and has_header parameters are given static due to testing needs
    pub fn csv_exec_sorted(
        schema: &SchemaRef,
        sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
        infinite_source: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let sort_exprs = sort_exprs.into_iter().collect();
        let projection: Vec<usize> = vec![0, 2, 3];

        Arc::new(CsvExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test:///").unwrap(),
                file_schema: schema.clone(),
                file_groups: vec![vec![PartitionedFile::new(
                    "file_path".to_string(),
                    100,
                )]],
                statistics: Statistics::default(),
                projection: Some(projection),
                limit: None,
                table_partition_cols: vec![],
                output_ordering: vec![sort_exprs],
                infinite_source,
            },
            true,
            0,
            FileCompressionType::UNCOMPRESSED,
        ))
    }

    // Util function to get string representation of a physical plan
    fn get_plan_string(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
        let formatted = displayable(plan.as_ref()).indent(true).to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();
        actual.iter().map(|elem| elem.to_string()).collect()
    }
}
