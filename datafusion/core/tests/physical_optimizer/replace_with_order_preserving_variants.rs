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

use crate::physical_optimizer::test_utils::{
    check_integrity, stream_exec_ordered_with_projection,
};

use datafusion::prelude::SessionContext;
use arrow::array::{ArrayRef, Int32Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::memory::MemoryExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{
    displayable, get_plan_string, ExecutionPlan, Partitioning,
};
use datafusion_common::tree_node::{TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::expressions::{self, col, Column};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{replace_with_order_preserving_variants, OrderPreservationContext};
use datafusion_common::config::ConfigOptions;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use rstest::rstest;
use url::Url;

/// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
/// the plan against the original and expected plans.
///
/// # Parameters
///
/// * `$EXPECTED_PLAN_LINES`: Expected input plan.
/// * `EXPECTED_OPTIMIZED_PLAN_LINES`: Optimized plan when the flag
///   `prefer_existing_sort` is `false`.
/// * `EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES`: Optimized plan when
///   the flag `prefer_existing_sort` is `true`.
/// * `$PLAN`: The plan to optimize.
macro_rules! assert_optimized_prefer_sort_on_off {
    ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $PREFER_EXISTING_SORT: expr, $SOURCE_UNBOUNDED: expr) => {
        if $PREFER_EXISTING_SORT {
            assert_optimized!(
                $EXPECTED_PLAN_LINES,
                $EXPECTED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES,
                $PLAN,
                $PREFER_EXISTING_SORT,
                $SOURCE_UNBOUNDED
            );
        } else {
            assert_optimized!(
                $EXPECTED_PLAN_LINES,
                $EXPECTED_OPTIMIZED_PLAN_LINES,
                $PLAN,
                $PREFER_EXISTING_SORT,
                $SOURCE_UNBOUNDED
            );
        }
    };
}

/// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
/// the plan against the original and expected plans for both bounded and
/// unbounded cases.
///
/// # Parameters
///
/// * `EXPECTED_UNBOUNDED_PLAN_LINES`: Expected input unbounded plan.
/// * `EXPECTED_BOUNDED_PLAN_LINES`: Expected input bounded plan.
/// * `EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES`: Optimized plan, which is
///   the same regardless of the value of the `prefer_existing_sort` flag.
/// * `EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES`: Optimized plan when the flag
///   `prefer_existing_sort` is `false` for bounded cases.
/// * `EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES`: Optimized plan
///   when the flag `prefer_existing_sort` is `true` for bounded cases.
/// * `$PLAN`: The plan to optimize.
/// * `$SOURCE_UNBOUNDED`: Whether the given plan contains an unbounded source.
macro_rules! assert_optimized_in_all_boundedness_situations {
    ($EXPECTED_UNBOUNDED_PLAN_LINES: expr,  $EXPECTED_BOUNDED_PLAN_LINES: expr, $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES: expr, $EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $SOURCE_UNBOUNDED: expr, $PREFER_EXISTING_SORT: expr) => {
        if $SOURCE_UNBOUNDED {
            assert_optimized_prefer_sort_on_off!(
                $EXPECTED_UNBOUNDED_PLAN_LINES,
                $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES,
                $EXPECTED_UNBOUNDED_OPTIMIZED_PLAN_LINES,
                $PLAN,
                $PREFER_EXISTING_SORT,
                $SOURCE_UNBOUNDED
            );
        } else {
            assert_optimized_prefer_sort_on_off!(
                $EXPECTED_BOUNDED_PLAN_LINES,
                $EXPECTED_BOUNDED_OPTIMIZED_PLAN_LINES,
                $EXPECTED_BOUNDED_PREFER_SORT_ON_OPTIMIZED_PLAN_LINES,
                $PLAN,
                $PREFER_EXISTING_SORT,
                $SOURCE_UNBOUNDED
            );
        }
    };
}

/// Runs the `replace_with_order_preserving_variants` sub-rule and asserts
/// the plan against the original and expected plans.
///
/// # Parameters
///
/// * `$EXPECTED_PLAN_LINES`: Expected input plan.
/// * `$EXPECTED_OPTIMIZED_PLAN_LINES`: Expected optimized plan.
/// * `$PLAN`: The plan to optimize.
/// * `$PREFER_EXISTING_SORT`: Value of the `prefer_existing_sort` flag.
#[macro_export]
macro_rules! assert_optimized {
        ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $PREFER_EXISTING_SORT: expr, $SOURCE_UNBOUNDED: expr) => {
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
            let mut config = ConfigOptions::new();
            config.optimizer.prefer_existing_sort=$PREFER_EXISTING_SORT;
            let plan_with_pipeline_fixer = OrderPreservationContext::new_default(physical_plan);
            let parallel = plan_with_pipeline_fixer.transform_up(|plan_with_pipeline_fixer| replace_with_order_preserving_variants(plan_with_pipeline_fixer, false, false, &config)).data().and_then(check_integrity)?;
            let optimized_physical_plan = parallel.plan;

            // Get string representation of the plan
            let actual = get_plan_string(&optimized_physical_plan);
            assert_eq!(
                expected_optimized_lines, actual,
                "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
            );

            if !$SOURCE_UNBOUNDED {
                let ctx = SessionContext::new();
                let object_store = InMemory::new();
                object_store.put(&object_store::path::Path::from("file_path"), bytes::Bytes::from("").into()).await?;
                ctx.register_object_store(&Url::parse("test://").unwrap(), Arc::new(object_store));
                let task_ctx = Arc::new(TaskContext::from(&ctx));
                let res = collect(optimized_physical_plan, task_ctx).await;
                assert!(
                    res.is_ok(),
                    "Some errors occurred while executing the optimized physical plan: {:?}", res.unwrap_err()
                );
            }
        };
    }

#[rstest]
#[tokio::test]
// Searches for a simple sort and a repartition just after it, the second repartition with 1 input partition should not be affected
async fn test_replace_multiple_input_repartition_1(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition = repartition_exec_hash(repartition_exec_round_robin(source));
    let sort = sort_exec(vec![sort_expr("a", &schema)], repartition, true);

    let physical_plan = sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_inter_children_change_only(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr_default("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let sort = sort_exec(
        vec![sort_expr_default("a", &coalesce_partitions.schema())],
        coalesce_partitions,
        false,
    );
    let repartition_rr2 = repartition_exec_round_robin(sort);
    let repartition_hash2 = repartition_exec_hash(repartition_rr2);
    let filter = filter_exec(repartition_hash2);
    let sort2 = sort_exec(vec![sort_expr_default("a", &filter.schema())], filter, true);

    let physical_plan =
        sort_preserving_merge_exec(vec![sort_expr_default("a", &sort2.schema())], sort2);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortPreservingMergeExec: [a@0 ASC]",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortPreservingMergeExec: [a@0 ASC]",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_2(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let filter = filter_exec(repartition_rr);
    let repartition_hash = repartition_exec_hash(filter);
    let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash, true);

    let physical_plan = sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded =  [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_with_extra_steps(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);
    let sort = sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec, true);

    let physical_plan = sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_replace_multiple_input_repartition_with_extra_steps_2(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let coalesce_batches_exec_1 = coalesce_batches_exec(repartition_rr);
    let repartition_hash = repartition_exec_hash(coalesce_batches_exec_1);
    let filter = filter_exec(repartition_hash);
    let coalesce_batches_exec_2 = coalesce_batches_exec(filter);
    let sort = sort_exec(vec![sort_expr("a", &schema)], coalesce_batches_exec_2, true);

    let physical_plan = sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_not_replacing_when_no_need_to_preserve_sorting(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let coalesce_batches_exec: Arc<dyn ExecutionPlan> = coalesce_batches_exec(filter);

    let physical_plan: Arc<dyn ExecutionPlan> =
        coalesce_partitions_exec(coalesce_batches_exec);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results same with and without flag, because there is no executor  with ordering requirement
    let expected_optimized_bounded = [
            "CoalescePartitionsExec",
            "  CoalesceBatchesExec: target_batch_size=8192",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_multiple_replacable_repartitions(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let filter = filter_exec(repartition_hash);
    let coalesce_batches = coalesce_batches_exec(filter);
    let repartition_hash_2 = repartition_exec_hash(coalesce_batches);
    let sort = sort_exec(vec![sort_expr("a", &schema)], repartition_hash_2, true);

    let physical_plan = sort_preserving_merge_exec(vec![sort_expr("a", &schema)], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c@1 > 3",
            "          RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "            RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "              MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      FilterExec: c@1 > 3",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_not_replace_with_different_orderings(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let sort = sort_exec(
        vec![sort_expr_default("c", &repartition_hash.schema())],
        repartition_hash,
        true,
    );

    let physical_plan =
        sort_preserving_merge_exec(vec![sort_expr_default("c", &sort.schema())], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results same with and without flag, because ordering requirement of the executor is different than the existing ordering.
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_ordering(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan =
        sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions, false);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [a@0 ASC NULLS LAST]",
            "  RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=a@0 ASC NULLS LAST",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_and_kept_ordering(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, sort_exprs)
    } else {
        memory_exec_sorted(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec_round_robin(source);
    let repartition_hash = repartition_exec_hash(repartition_rr);
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let sort = sort_exec(
        vec![sort_expr_default("c", &coalesce_partitions.schema())],
        coalesce_partitions,
        false,
    );
    let repartition_rr2 = repartition_exec_round_robin(sort);
    let repartition_hash2 = repartition_exec_hash(repartition_rr2);
    let filter = filter_exec(repartition_hash2);
    let sort2 = sort_exec(vec![sort_expr_default("c", &filter.schema())], filter, true);

    let physical_plan =
        sort_preserving_merge_exec(vec![sort_expr_default("c", &sort2.schema())], sort2);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@1 ASC], preserve_partitioning=[false]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  SortExec: expr=[c@1 ASC], preserve_partitioning=[true]",
            "    FilterExec: c@1 > 3",
            "      RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "          SortExec: expr=[c@1 ASC], preserve_partitioning=[false]",
            "            CoalescePartitionsExec",
            "              RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                  MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = [
            "SortPreservingMergeExec: [c@1 ASC]",
            "  FilterExec: c@1 > 3",
            "    RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8, preserve_order=true, sort_exprs=c@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        SortExec: expr=[c@1 ASC], preserve_partitioning=[false]",
            "          CoalescePartitionsExec",
            "            RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "                MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_multiple_child_trees(
    #[values(false, true)] source_unbounded: bool,
    #[values(false, true)] prefer_existing_sort: bool,
) -> Result<()> {
    let schema = create_test_schema()?;

    let left_sort_exprs = vec![sort_expr("a", &schema)];
    let left_source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, left_sort_exprs)
    } else {
        memory_exec_sorted(&schema, left_sort_exprs)
    };
    let left_repartition_rr = repartition_exec_round_robin(left_source);
    let left_repartition_hash = repartition_exec_hash(left_repartition_rr);
    let left_coalesce_partitions =
        Arc::new(CoalesceBatchesExec::new(left_repartition_hash, 4096));

    let right_sort_exprs = vec![sort_expr("a", &schema)];
    let right_source = if source_unbounded {
        stream_exec_ordered_with_projection(&schema, right_sort_exprs)
    } else {
        memory_exec_sorted(&schema, right_sort_exprs)
    };
    let right_repartition_rr = repartition_exec_round_robin(right_source);
    let right_repartition_hash = repartition_exec_hash(right_repartition_rr);
    let right_coalesce_partitions =
        Arc::new(CoalesceBatchesExec::new(right_repartition_hash, 4096));

    let hash_join_exec =
        hash_join_exec(left_coalesce_partitions, right_coalesce_partitions);
    let sort = sort_exec(
        vec![sort_expr_default("a", &hash_join_exec.schema())],
        hash_join_exec,
        true,
    );

    let physical_plan =
        sort_preserving_merge_exec(vec![sort_expr_default("a", &sort.schema())], sort);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];
    let expected_input_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[a, c, d], infinite_source=true, output_ordering=[a@0 ASC NULLS LAST]",
        ];

    // Expected bounded results same with and without flag, because ordering get lost during intermediate executor anyway. Hence no need to preserve
    // existing ordering.
    let expected_optimized_bounded = [
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c@1, c@1)]",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
            "      CoalesceBatchesExec: target_batch_size=4096",
            "        RepartitionExec: partitioning=Hash([c@1], 8), input_partitions=8",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[1], output_ordering=a@0 ASC NULLS LAST",
        ];
    let expected_optimized_bounded_sort_preserve = expected_optimized_bounded;

    assert_optimized_in_all_boundedness_situations!(
        expected_input_unbounded,
        expected_input_bounded,
        expected_optimized_unbounded,
        expected_optimized_bounded,
        expected_optimized_bounded_sort_preserve,
        physical_plan,
        source_unbounded,
        prefer_existing_sort
    );
    Ok(())
}

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

fn repartition_exec_round_robin(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(8)).unwrap())
}

fn repartition_exec_hash(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let input_schema = input.schema();
    Arc::new(
        RepartitionExec::try_new(
            input,
            Partitioning::Hash(vec![col("c", &input_schema).unwrap()], 8),
        )
        .unwrap(),
    )
}

fn filter_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let input_schema = input.schema();
    let predicate = expressions::binary(
        col("c", &input_schema).unwrap(),
        Operator::Gt,
        expressions::lit(3i32),
        &input_schema,
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
    let left_on = col("c", &left.schema()).unwrap();
    let right_on = col("c", &right.schema()).unwrap();
    let left_col = left_on.as_any().downcast_ref::<Column>().unwrap();
    let right_col = right_on.as_any().downcast_ref::<Column>().unwrap();
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            vec![(Arc::new(left_col.clone()), Arc::new(right_col.clone()))],
            None,
            &JoinType::Inner,
            None,
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

// creates a memory exec source for the test purposes
// projection parameter is given static due to testing needs
fn memory_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    pub fn make_partition(schema: &SchemaRef, sz: i32) -> RecordBatch {
        let values = (0..sz).collect::<Vec<_>>();
        let arr = Arc::new(Int32Array::from(values));
        let arr = arr as ArrayRef;

        RecordBatch::try_new(
            schema.clone(),
            vec![arr.clone(), arr.clone(), arr.clone(), arr],
        )
        .unwrap()
    }

    let rows = 5;
    let partitions = 1;
    let sort_exprs = sort_exprs.into_iter().collect();
    Arc::new({
        let data: Vec<Vec<_>> = (0..partitions)
            .map(|_| vec![make_partition(schema, rows)])
            .collect();
        let projection: Vec<usize> = vec![0, 2, 3];
        MemoryExec::try_new(&data, schema.clone(), Some(projection))
            .unwrap()
            .try_with_sort_information(vec![sort_exprs])
            .unwrap()
    })
}
