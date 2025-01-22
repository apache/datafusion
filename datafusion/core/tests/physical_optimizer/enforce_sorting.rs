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

use datafusion_physical_plan::displayable;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::{col, Column, NotExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};

use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::enforce_sorting::{EnforceSorting,PlanWithCorrespondingCoalescePartitions,PlanWithCorrespondingSort,parallelize_sorts,ensure_sorting};
use datafusion_physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{replace_with_order_preserving_variants,OrderPreservationContext};
use datafusion_physical_optimizer::enforce_sorting::sort_pushdown::{SortPushDown, assign_initial_requirements, pushdown_sorts};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{get_plan_string, ExecutionPlan};
use rstest::rstest;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TransformedResult};

#[tokio::test]
async fn test_remove_unnecessary_sort5() -> Result<()> {
    let left_schema = create_test_schema2()?;
    let right_schema = create_test_schema3()?;
    let left_input = memory_exec(&left_schema);
    let parquet_sort_exprs = vec![sort_expr("a", &right_schema)];
    let right_input = parquet_exec_sorted(&right_schema, parquet_sort_exprs);

    let on = vec![(
        Arc::new(Column::new_with_schema("col_a", &left_schema)?) as _,
        Arc::new(Column::new_with_schema("c", &right_schema)?) as _,
    )];
    let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
    let physical_plan = sort_exec(vec![sort_expr("a", &join.schema())], join);

    let expected_input = ["SortExec: expr=[a@2 ASC], preserve_partitioning=[false]",
        "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
        "    MemoryExec: partitions=1, partition_sizes=[0]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]"];

    let expected_optimized = ["HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
        "  ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_do_not_remove_sort_with_limit() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort = sort_exec(sort_exprs.clone(), source1);
    let limit = limit_exec(sort);

    let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

    let union = union_exec(vec![source2, limit]);
    let repartition = repartition_exec(union);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, repartition);

    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "    UnionExec",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "      GlobalLimitExec: skip=0, fetch=100",
        "        LocalLimitExec: fetch=100",
        "          SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "            ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];

    // We should keep the bottom `SortExec`.
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "      UnionExec",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "        GlobalLimitExec: skip=0, fetch=100",
        "          LocalLimitExec: fetch=100",
        "            SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "              ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_sorted() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), source1);

    let source2 = parquet_exec_sorted(&schema, sort_exprs.clone());

    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

    // one input to the union is already sorted, one is not.
    let expected_input = vec![
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
    ];
    // should not add a sort at the output of the union, input plan should not be changed
    let expected_optimized = expected_input.clone();
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), source1);

    let parquet_sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

    // one input to the union is already sorted, one is not.
    let expected_input = vec![
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
    ];
    // should not add a sort at the output of the union, input plan should not be changed
    let expected_optimized = expected_input.clone();
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted2() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort = sort_exec(sort_exprs.clone(), source1);

    let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs);

    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

    // Input is an invalid plan. In this case rule should add required sorting in appropriate places.
    // First ParquetExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy the
    // required ordering of SortPreservingMergeExec.
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];

    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted3() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort1 = sort_exec(sort_exprs1, source1.clone());
    let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
    let sort2 = sort_exec(sort_exprs2, source1);

    let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

    let union = union_exec(vec![sort1, source2, sort2]);
    let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

    // First input to the union is not Sorted (SortExec is finer than required ordering by the SortPreservingMergeExec above).
    // Second input to the union is already Sorted (matches with the required ordering by the SortPreservingMergeExec above).
    // Third input to the union is not Sorted (SortExec is matches required ordering by the SortPreservingMergeExec above).
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    // should adjust sorting in the first input of the union such that it is not unnecessarily fine
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted4() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(sort_exprs2.clone(), source1.clone());
    let sort2 = sort_exec(sort_exprs2.clone(), source1);

    let source2 = parquet_exec_sorted(&schema, sort_exprs2);

    let union = union_exec(vec![sort1, source2, sort2]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs1, union);

    // Ordering requirement of the `SortPreservingMergeExec` is not met.
    // Should modify the plan to ensure that all three inputs to the
    // `UnionExec` satisfy the ordering, OR add a single sort after
    // the `UnionExec` (both of which are equally good for this example).
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted5() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs2 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr_options(
            "non_nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ];
    let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(sort_exprs1, source1.clone());
    let sort2 = sort_exec(sort_exprs2, source1);

    let union = union_exec(vec![sort1, sort2]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

    // The `UnionExec` doesn't preserve any of the inputs ordering in the
    // example below. However, we should be able to change the unnecessarily
    // fine `SortExec`s below with required `SortExec`s that are absolutely necessary.
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted6() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(sort_exprs1, source1.clone());
    let sort_exprs2 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let repartition = repartition_exec(source1);
    let spm = sort_preserving_merge_exec(sort_exprs2, repartition);

    let parquet_sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let source2 = parquet_exec_sorted(&schema, parquet_sort_exprs.clone());

    let union = union_exec(vec![sort1, source2, spm]);
    let physical_plan = sort_preserving_merge_exec(parquet_sort_exprs, union);

    // The plan is not valid as it is -- the input ordering requirement
    // of the `SortPreservingMergeExec` under the third child of the
    // `UnionExec` is not met. We should add a `SortExec` below it.
    // At the same time, this ordering requirement is unnecessarily fine.
    // The final plan should be valid AND the ordering of the third child
    // shouldn't be finer than necessary.
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    // Should adjust the requirement in the third input of the union so
    // that it is not unnecessarily fine.
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted7() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(sort_exprs1.clone(), source1.clone());
    let sort2 = sort_exec(sort_exprs1, source1);

    let union = union_exec(vec![sort1, sort2]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

    // Union has unnecessarily fine ordering below it. We should be able to replace them with absolutely necessary ordering.
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
    let expected_output = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_output, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted8() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs2 = vec![
        sort_expr_options(
            "nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
        sort_expr_options(
            "non_nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ];
    let sort1 = sort_exec(sort_exprs1, source1.clone());
    let sort2 = sort_exec(sort_exprs2, source1);

    let physical_plan = union_exec(vec![sort1, sort2]);

    // The `UnionExec` doesn't preserve any of the inputs ordering in the
    // example below.
    let expected_input = ["UnionExec",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    // Since `UnionExec` doesn't preserve ordering in the plan above.
    // We shouldn't keep SortExecs in the plan.
    let expected_optimized = ["UnionExec",
        "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "  ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_window_multi_path_sort() -> Result<()> {
    let schema = create_test_schema()?;

    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
    // reverse sorting of sort_exprs2
    let sort_exprs3 = vec![sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )];
    let source1 = parquet_exec_sorted(&schema, sort_exprs1);
    let source2 = parquet_exec_sorted(&schema, sort_exprs2);
    let sort1 = sort_exec(sort_exprs3.clone(), source1);
    let sort2 = sort_exec(sort_exprs3.clone(), source2);

    let union = union_exec(vec![sort1, sort2]);
    let spm = sort_preserving_merge_exec(sort_exprs3.clone(), union);
    let physical_plan = bounded_window_exec("nullable_col", sort_exprs3, spm);

    // The `WindowAggExec` gets its sorting from multiple children jointly.
    // During the removal of `SortExec`s, it should be able to remove the
    // corresponding SortExecs together. Also, the inputs of these `SortExec`s
    // are not necessarily the same to be able to remove them.
    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 DESC NULLS LAST]",
        "    UnionExec",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
    let expected_optimized = [
        "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_window_multi_path_sort2() -> Result<()> {
    let schema = create_test_schema()?;

    let sort_exprs1 = LexOrdering::new(vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]);
    let sort_exprs2 = vec![sort_expr("nullable_col", &schema)];
    let source1 = parquet_exec_sorted(&schema, sort_exprs2.clone());
    let source2 = parquet_exec_sorted(&schema, sort_exprs2.clone());
    let sort1 = sort_exec(sort_exprs1.clone(), source1);
    let sort2 = sort_exec(sort_exprs1.clone(), source2);

    let union = union_exec(vec![sort1, sort2]);
    let spm = Arc::new(SortPreservingMergeExec::new(sort_exprs1, union)) as _;
    let physical_plan = bounded_window_exec("nullable_col", sort_exprs2, spm);

    // The `WindowAggExec` can get its required sorting from the leaf nodes directly.
    // The unnecessary SortExecs should be removed
    let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "    UnionExec",
        "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
    let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted_with_limit() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let sort_exprs1 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort_exprs2 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr_options(
            "non_nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ];
    let sort_exprs3 = vec![sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(sort_exprs1, source1.clone());

    let sort2 = sort_exec(sort_exprs2, source1);
    let limit = local_limit_exec(sort2);
    let limit = global_limit_exec(limit);

    let union = union_exec(vec![sort1, limit]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs3, union);

    // Should not change the unnecessarily fine `SortExec`s because there is `LimitExec`
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_order_by_left() -> Result<()> {
    let left_schema = create_test_schema()?;
    let right_schema = create_test_schema2()?;

    let left = parquet_exec(&left_schema);
    let right = parquet_exec(&right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
    )];

    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
    ];
    for join_type in join_types {
        let join =
            sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
        let sort_exprs = vec![
            sort_expr("nullable_col", &join.schema()),
            sort_expr("non_nullable_col", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs.clone(), join);

        let join_plan = format!(
            "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let join_plan2 = format!(
            "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
            join_plan2.as_str(),
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
        let expected_optimized = match join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti => {
                // can push down the sort requirements and save 1 SortExec
                vec![
                    join_plan.as_str(),
                    "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
                    "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                    "  SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                ]
            }
            _ => {
                // can not push down the sort requirements
                vec![
                    "SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
                    join_plan2.as_str(),
                    "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                    "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                ]
            }
        };
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    }
    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_order_by_right() -> Result<()> {
    let left_schema = create_test_schema()?;
    let right_schema = create_test_schema2()?;

    let left = parquet_exec(&left_schema);
    let right = parquet_exec(&right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
    )];

    let join_types = vec![
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::RightAnti,
    ];
    for join_type in join_types {
        let join =
            sort_merge_join_exec(left.clone(), right.clone(), &join_on, &join_type);
        let sort_exprs = vec![
            sort_expr("col_a", &join.schema()),
            sort_expr("col_b", &join.schema()),
        ];
        let physical_plan = sort_preserving_merge_exec(sort_exprs, join);

        let join_plan = format!(
            "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let spm_plan = match join_type {
            JoinType::RightAnti => "SortPreservingMergeExec: [col_a@0 ASC, col_b@1 ASC]",
            _ => "SortPreservingMergeExec: [col_a@2 ASC, col_b@3 ASC]",
        };
        let join_plan2 = format!(
            "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let expected_input = [spm_plan,
            join_plan2.as_str(),
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
            "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
        let expected_optimized = match join_type {
            JoinType::Inner | JoinType::Right | JoinType::RightAnti => {
                // can push down the sort requirements and save 1 SortExec
                vec![
                    join_plan.as_str(),
                    "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                    "  SortExec: expr=[col_a@0 ASC, col_b@1 ASC], preserve_partitioning=[false]",
                    "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                ]
            }
            _ => {
                // can not push down the sort requirements for Left and Full join.
                vec![
                    "SortExec: expr=[col_a@2 ASC, col_b@3 ASC], preserve_partitioning=[false]",
                    join_plan2.as_str(),
                    "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
                    "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]",
                ]
            }
        };
        assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    }
    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_complex_order_by() -> Result<()> {
    let left_schema = create_test_schema()?;
    let right_schema = create_test_schema2()?;

    let left = parquet_exec(&left_schema);
    let right = parquet_exec(&right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema()).unwrap()) as _,
    )];

    let join = sort_merge_join_exec(left, right, &join_on, &JoinType::Inner);

    // order by (col_b, col_a)
    let sort_exprs1 = vec![
        sort_expr("col_b", &join.schema()),
        sort_expr("col_a", &join.schema()),
    ];
    let physical_plan = sort_preserving_merge_exec(sort_exprs1, join.clone());

    let expected_input = ["SortPreservingMergeExec: [col_b@3 ASC, col_a@2 ASC]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];

    // can not push down the sort requirements, need to add SortExec
    let expected_optimized = ["SortExec: expr=[col_b@3 ASC, col_a@2 ASC], preserve_partitioning=[false]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    // order by (nullable_col, col_b, col_a)
    let sort_exprs2 = vec![
        sort_expr("nullable_col", &join.schema()),
        sort_expr("col_b", &join.schema()),
        sort_expr("col_a", &join.schema()),
    ];
    let physical_plan = sort_preserving_merge_exec(sort_exprs2, join);

    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, col_b@3 ASC, col_a@2 ASC]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];

    // can not push down the sort requirements, need to add SortExec
    let expected_optimized = ["SortExec: expr=[nullable_col@0 ASC, col_b@3 ASC, col_a@2 ASC], preserve_partitioning=[false]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]",
        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
        "      ParquetExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}
