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

use crate::assert_optimized;
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
use crate::test_utils::{aggregate_exec, bounded_window_exec, coalesce_batches_exec, filter_exec, hash_join_exec, memory_exec, repartition_exec, sort_exec, sort_expr, sort_expr_options, sort_preserving_merge_exec, union_exec};

fn create_test_schema() -> Result<SchemaRef> {
    let nullable_column = Field::new("nullable_col", DataType::Int32, true);
    let non_nullable_column = Field::new("non_nullable_col", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![nullable_column, non_nullable_column]));
    Ok(schema)
}

fn create_test_schema2() -> Result<SchemaRef> {
    let col_a = Field::new("col_a", DataType::Int32, true);
    let col_b = Field::new("col_b", DataType::Int32, true);
    let schema = Arc::new(Schema::new(vec![col_a, col_b]));
    Ok(schema)
}

// Generate a schema which consists of 5 columns (a, b, c, d, e)
fn create_test_schema3() -> Result<SchemaRef> {
    let a = Field::new("a", DataType::Int32, true);
    let b = Field::new("b", DataType::Int32, false);
    let c = Field::new("c", DataType::Int32, true);
    let d = Field::new("d", DataType::Int32, false);
    let e = Field::new("e", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![a, b, c, d, e]));
    Ok(schema)
}

/// Runs the sort enforcement optimizer and asserts the plan
/// against the original and expected plans
///
/// `$EXPECTED_PLAN_LINES`: input plan
/// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
/// `$PLAN`: the plan to optimized
/// `REPARTITION_SORTS`: Flag to set `config.options.optimizer.repartition_sorts` option.
///
macro_rules! assert_optimized {
    ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $REPARTITION_SORTS: expr) => {
        let mut config = ConfigOptions::new();
        config.optimizer.repartition_sorts = $REPARTITION_SORTS;

        // This file has 4 rules that use tree node, apply these rules as in the
        // EnforceSorting::optimize implementation
        // After these operations tree nodes should be in a consistent state.
        // This code block makes sure that these rules doesn't violate tree node integrity.
        {
            let plan_requirements = PlanWithCorrespondingSort::new_default($PLAN.clone());
            let adjusted = plan_requirements
                .transform_up(ensure_sorting)
                .data()
                .and_then(check_integrity)?;
            // TODO: End state payloads will be checked here.

            let new_plan = if repartition_sorts.optimizer.repartition_sorts {
                let plan_with_coalesce_partitions =
                    PlanWithCorrespondingCoalescePartitions::new_default(adjusted.plan);
                let parallel = plan_with_coalesce_partitions
                    .transform_up(parallelize_sorts)
                    .data()
                    .and_then(check_integrity)?;
                // TODO: End state payloads will be checked here.
                parallel.plan
            } else {
                adjusted.plan
            };

            let plan_with_pipeline_fixer = OrderPreservationContext::new_default(new_plan);
            let updated_plan = plan_with_pipeline_fixer
                .transform_up(|plan_with_pipeline_fixer| {
                    replace_with_order_preserving_variants(
                        plan_with_pipeline_fixer,
                        false,
                        true,
                        repartition_sorts,
                    )
                })
                .data()
                .and_then(check_integrity)?;
            // TODO: End state payloads will be checked here.

            let mut sort_pushdown = SortPushDown::new_default(updated_plan.plan);
            assign_initial_requirements(&mut sort_pushdown);
            check_integrity(pushdown_sorts(sort_pushdown)?)?;
            // TODO: End state payloads will be checked here.
        }

        let physical_plan = $PLAN;
        let formatted = displayable(physical_plan.as_ref()).indent(true).to_string();
        let actual: Vec<&str> = formatted.trim().lines().collect();

        let expected_plan_lines: Vec<&str> = $EXPECTED_PLAN_LINES
            .iter().map(|s| *s).collect();

        assert_eq!(
            expected_plan_lines, actual,
            "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_plan_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );

        let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES
            .iter().map(|s| *s).collect();

        // Run the actual optimizer
        let optimized_physical_plan =
            EnforceSorting::new().optimize(physical_plan, repartition_sorts)?;

        // Get string representation of the plan
        let actual = get_plan_string(&optimized_physical_plan);
        assert_eq!(
            expected_optimized_lines, actual,
            "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );

    };
}

#[tokio::test]
async fn test_remove_unnecessary_sort() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
    let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort_window_multilayer() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);

    let sort_exprs = vec![sort_expr_options(
        "non_nullable_col",
        &source.schema(),
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    )];
    let sort = sort_exec(sort_exprs.clone(), source);
    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let coalesce_batches = coalesce_batches_exec(sort);

    let window_agg =
        bounded_window_exec("non_nullable_col", sort_exprs, coalesce_batches);

    let sort_exprs = vec![sort_expr_options(
        "non_nullable_col",
        &window_agg.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )];

    let sort = sort_exec(sort_exprs.clone(), window_agg);

    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let filter = filter_exec(
        Arc::new(NotExpr::new(
            col("non_nullable_col", schema.as_ref()).unwrap(),
        )),
        sort,
    );

    let physical_plan = bounded_window_exec("non_nullable_col", sort_exprs, filter);

    let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
            "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "        CoalesceBatchesExec: target_batch_size=128",
            "          SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
            "            MemoryExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
            "  FilterExec: NOT non_nullable_col@1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      CoalesceBatchesExec: target_batch_size=128",
            "        SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
            "          MemoryExec: partitions=1, partition_sizes=[0]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_add_required_sort() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);

    let sort_exprs = vec![sort_expr("nullable_col", &schema)];

    let physical_plan = sort_preserving_merge_exec(sort_exprs, source);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort1() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), source);
    let spm = sort_preserving_merge_exec(sort_exprs, sort);

    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), spm);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    SortPreservingMergeExec: [nullable_col@0 ASC]",
        "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "        MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort2() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), source);
    let spm = sort_preserving_merge_exec(sort_exprs, sort);

    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort2 = sort_exec(sort_exprs.clone(), spm);
    let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let sort3 = sort_exec(sort_exprs, spm2);
    let physical_plan = repartition_exec(repartition_exec(sort3));

    let expected_input = [
            "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "      SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "            SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "              MemoryExec: partitions=1, partition_sizes=[0]",
        ];

    let expected_optimized = [
        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort3() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr("non_nullable_col", &schema)];
    let sort = sort_exec(sort_exprs.clone(), source);
    let spm = sort_preserving_merge_exec(sort_exprs, sort);

    let sort_exprs = LexOrdering::new(vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]);
    let repartition_exec = repartition_exec(spm);
    let sort2 = Arc::new(
        SortExec::new(sort_exprs.clone(), repartition_exec)
            .with_preserve_partitioning(true),
    ) as _;
    let spm2 = sort_preserving_merge_exec(sort_exprs, sort2);

    let physical_plan = aggregate_exec(spm2);

    // When removing a `SortPreservingMergeExec`, make sure that partitioning
    // requirements are not violated. In some cases, we may need to replace
    // it with a `CoalescePartitionsExec` instead of directly removing it.
    let expected_input = [
            "AggregateExec: mode=Final, gby=[], aggr=[]",
            "  SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "          SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
        ];

    let expected_optimized = [
        "AggregateExec: mode=Final, gby=[], aggr=[]",
        "  CoalescePartitionsExec",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort4() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = repartition_exec(memory_exec(&schema));

    let source2 = repartition_exec(memory_exec(&schema));
    let union = union_exec(vec![source1, source2]);

    let sort_exprs = LexOrdering::new(vec![sort_expr("non_nullable_col", &schema)]);
    // let sort = sort_exec(sort_exprs.clone(), union);
    let sort = Arc::new(
        SortExec::new(sort_exprs.clone(), union).with_preserve_partitioning(true),
    ) as _;
    let spm = sort_preserving_merge_exec(sort_exprs, sort);

    let filter = filter_exec(
        Arc::new(NotExpr::new(
            col("non_nullable_col", schema.as_ref()).unwrap(),
        )),
        spm,
    );

    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let physical_plan = sort_exec(sort_exprs, filter);

    // When removing a `SortPreservingMergeExec`, make sure that partitioning
    // requirements are not violated. In some cases, we may need to replace
    // it with a `CoalescePartitionsExec` instead of directly removing it.
    let expected_input = ["SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  FilterExec: NOT non_nullable_col@1",
            "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
            "      SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "        UnionExec",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]",
            "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "            MemoryExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
            "    FilterExec: NOT non_nullable_col@1",
            "      UnionExec",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          MemoryExec: partitions=1, partition_sizes=[0]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort6() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![sort_expr("non_nullable_col", &schema)]),
            source,
        )
        .with_fetch(Some(2)),
    );
    let physical_plan = sort_exec(
        vec![
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ],
        input,
    );

    let expected_input = [
            "SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "  SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    let expected_optimized = [
            "SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort7() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = Arc::new(SortExec::new(
        LexOrdering::new(vec![
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ]),
        source,
    ));

    let physical_plan = Arc::new(
        SortExec::new(
            LexOrdering::new(vec![sort_expr("non_nullable_col", &schema)]),
            input,
        )
        .with_fetch(Some(2)),
    ) as Arc<dyn ExecutionPlan>;

    let expected_input = [
            "SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    let expected_optimized = [
            "GlobalLimitExec: skip=0, fetch=2",
            "  SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort8() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = Arc::new(SortExec::new(
        LexOrdering::new(vec![sort_expr("non_nullable_col", &schema)]),
        source,
    ));
    let limit = Arc::new(LocalLimitExec::new(input, 2));
    let physical_plan = sort_exec(
        vec![
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ],
        limit,
    );

    let expected_input = [
            "SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "  LocalLimitExec: fetch=2",
            "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    let expected_optimized = [
            "LocalLimitExec: fetch=2",
            "  SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_do_not_pushdown_through_limit() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    // let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
    let input = Arc::new(SortExec::new(
        LexOrdering::new(vec![sort_expr("non_nullable_col", &schema)]),
        source,
    ));
    let limit = Arc::new(GlobalLimitExec::new(input, 0, Some(5))) as _;
    let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], limit);

    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  GlobalLimitExec: skip=0, fetch=5",
        "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  GlobalLimitExec: skip=0, fetch=5",
        "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_spm1() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input =
        sort_preserving_merge_exec(vec![sort_expr("non_nullable_col", &schema)], source);
    let input2 =
        sort_preserving_merge_exec(vec![sort_expr("non_nullable_col", &schema)], input);
    let physical_plan =
        sort_preserving_merge_exec(vec![sort_expr("nullable_col", &schema)], input2);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_change_wrong_sorting() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort = sort_exec(vec![sort_exprs[0].clone()], source);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
            "SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_change_wrong_sorting2() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let spm1 = sort_preserving_merge_exec(sort_exprs.clone(), source);
    let sort2 = sort_exec(vec![sort_exprs[0].clone()], spm1);
    let physical_plan = sort_preserving_merge_exec(vec![sort_exprs[1].clone()], sort2);

    let expected_input = [
        "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "  MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_multiple_sort_window_exec() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);

    let sort_exprs1 = vec![sort_expr("nullable_col", &schema)];
    let sort_exprs2 = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];

    let sort1 = sort_exec(sort_exprs1.clone(), source);
    let window_agg1 = bounded_window_exec("non_nullable_col", sort_exprs1.clone(), sort1);
    let window_agg2 = bounded_window_exec("non_nullable_col", sort_exprs2, window_agg1);
    // let filter_exec = sort_exec;
    let physical_plan = bounded_window_exec("non_nullable_col", sort_exprs1, window_agg2);

    let expected_input = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "        MemoryExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
            "        MemoryExec: partitions=1, partition_sizes=[0]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_multilayer_coalesce_partitions() -> Result<()> {
    let schema = create_test_schema()?;

    let source1 = parquet_exec(&schema);
    let repartition = repartition_exec(source1);
    let coalesce = Arc::new(CoalescePartitionsExec::new(repartition)) as _;
    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let filter = filter_exec(
        Arc::new(NotExpr::new(
            col("non_nullable_col", schema.as_ref()).unwrap(),
        )),
        coalesce,
    );
    let sort_exprs = vec![sort_expr("nullable_col", &schema)];
    let physical_plan = sort_exec(sort_exprs, filter);

    // CoalescePartitionsExec and SortExec are not directly consecutive. In this case
    // we should be able to parallelize Sorting also (given that executors in between don't require)
    // single partition.
    let expected_input = ["SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  FilterExec: NOT non_nullable_col@1",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
            "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
            "    FilterExec: NOT non_nullable_col@1",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        ParquetExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col]"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
// With new change in SortEnforcement EnforceSorting->EnforceDistribution->EnforceSorting
// should produce same result with EnforceDistribution+EnforceSorting
// This enables us to use EnforceSorting possibly before EnforceDistribution
// Given that it will be called at least once after last EnforceDistribution. The reason is that
// EnforceDistribution may invalidate ordering invariant.
async fn test_commutativity() -> Result<()> {
    let schema = create_test_schema()?;
    let config = ConfigOptions::new();

    let memory_exec = memory_exec(&schema);
    let sort_exprs = LexOrdering::new(vec![sort_expr("nullable_col", &schema)]);
    let window = bounded_window_exec("nullable_col", sort_exprs.clone(), memory_exec);
    let repartition = repartition_exec(window);

    let orig_plan =
        Arc::new(SortExec::new(sort_exprs, repartition)) as Arc<dyn ExecutionPlan>;
    let actual = get_plan_string(&orig_plan);
    let expected_input = vec![
            "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ];
    assert_eq!(
            expected_input, actual,
            "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_input:#?}\nactual:\n\n{actual:#?}\n\n"
        );

    let mut plan = orig_plan.clone();
    let rules = vec![
        Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
    ];
    for rule in rules {
        plan = rule.optimize(plan, config)?;
    }
    let first_plan = plan.clone();

    let mut plan = orig_plan.clone();
    let rules = vec![
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
    ];
    for rule in rules {
        plan = rule.optimize(plan, config)?;
    }
    let second_plan = plan.clone();

    assert_eq!(get_plan_string(&first_plan), get_plan_string(&second_plan));
    Ok(())
}

#[tokio::test]
async fn test_coalesce_propagate() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let repartition = repartition_exec(source);
    let coalesce_partitions = Arc::new(CoalescePartitionsExec::new(repartition));
    let repartition = repartition_exec(coalesce_partitions);
    let sort_exprs = LexOrdering::new(vec![sort_expr("nullable_col", &schema)]);
    // Add local sort
    let sort = Arc::new(
        SortExec::new(sort_exprs.clone(), repartition).with_preserve_partitioning(true),
    ) as _;
    let spm = sort_preserving_merge_exec(sort_exprs.clone(), sort);
    let sort = sort_exec(sort_exprs, spm);

    let physical_plan = sort.clone();
    // Sort Parallelize rule should end Coalesce + Sort linkage when Sort is Global Sort
    // Also input plan is not valid as it is. We need to add SortExec before SortPreservingMergeExec.
    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        CoalescePartitionsExec",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_with_lost_ordering_bounded() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs);
    let repartition_rr = repartition_exec(source);
    let repartition_hash = Arc::new(RepartitionExec::try_new(
        repartition_rr,
        Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
    )?) as _;
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

    let expected_input = ["SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false"];
    let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=false"];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_ordering_unbounded_bounded(
    #[values(false, true)] source_unbounded: bool,
) -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = vec![sort_expr("a", &schema)];
    // create either bounded or unbounded source
    let source = if source_unbounded {
        stream_exec_ordered(&schema, sort_exprs)
    } else {
        csv_exec_ordered(&schema, sort_exprs)
    };
    let repartition_rr = repartition_exec(source);
    let repartition_hash = Arc::new(RepartitionExec::try_new(
        repartition_rr,
        Partitioning::Hash(vec![col("c", &schema).unwrap()], 10),
    )?) as _;
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan = sort_exec(vec![sort_expr("a", &schema)], coalesce_partitions);

    // Expected inputs unbounded and bounded
    let expected_input_unbounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
    let expected_input_bounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];

    // Expected unbounded result (same for with and without flag)
    let expected_optimized_unbounded = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];

    // Expected bounded results with and without flag
    let expected_optimized_bounded = vec![
            "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "  CoalescePartitionsExec",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];
    let expected_optimized_bounded_parallelize_sort = vec![
            "SortPreservingMergeExec: [a@0 ASC]",
            "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        CsvExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], has_header=true",
        ];
    let (expected_input, expected_optimized, expected_optimized_sort_parallelize) =
        if source_unbounded {
            (
                expected_input_unbounded,
                expected_optimized_unbounded.clone(),
                expected_optimized_unbounded,
            )
        } else {
            (
                expected_input_bounded,
                expected_optimized_bounded,
                expected_optimized_bounded_parallelize_sort,
            )
        };
    assert_optimized!(
        expected_input,
        expected_optimized,
        physical_plan.clone(),
        false
    );
    assert_optimized!(
        expected_input,
        expected_optimized_sort_parallelize,
        physical_plan,
        true
    );

    Ok(())
}

#[tokio::test]
async fn test_do_not_pushdown_through_spm() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs.clone());
    let repartition_rr = repartition_exec(source);
    let spm = sort_preserving_merge_exec(sort_exprs, repartition_rr);
    let physical_plan = sort_exec(vec![sort_expr("b", &schema)], spm);

    let expected_input = ["SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
    let expected_optimized = ["SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}

#[tokio::test]
async fn test_pushdown_through_spm() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs.clone());
    let repartition_rr = repartition_exec(source);
    let spm = sort_preserving_merge_exec(sort_exprs, repartition_rr);
    let physical_plan = sort_exec(
        vec![
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ],
        spm,
    );

    let expected_input = ["SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
            "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
    let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
            "  SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[true]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "      CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], has_header=false",];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}

#[tokio::test]
async fn test_window_multi_layer_requirement() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = vec![sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, vec![]);
    let sort = sort_exec(sort_exprs.clone(), source);
    let repartition = repartition_exec(sort);
    let repartition = spr_repartition_exec(repartition);
    let spm = sort_preserving_merge_exec(sort_exprs.clone(), repartition);

    let physical_plan = bounded_window_exec("a", sort_exprs, spm);

    let expected_input = [
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
            "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC, b@1 ASC",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "        SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
            "          CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
    let expected_optimized = [
            "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
            "  SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
            "    CoalescePartitionsExec",
            "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
            "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "          CsvExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], has_header=false",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}

#[tokio::test]
async fn test_replace_with_partial_sort() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_sort_exprs = vec![sort_expr("a", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

    let physical_plan = sort_exec(
        vec![sort_expr("a", &schema), sort_expr("c", &schema)],
        unbounded_input,
    );

    let expected_input = [
            "SortExec: expr=[a@0 ASC, c@2 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]"
        ];
    let expected_optimized = [
            "PartialSortExec: expr=[a@0 ASC, c@2 ASC], common_prefix_length=[1]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_replace_with_partial_sort2() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_sort_exprs = vec![sort_expr("a", &schema), sort_expr("c", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

    let physical_plan = sort_exec(
        vec![
            sort_expr("a", &schema),
            sort_expr("c", &schema),
            sort_expr("d", &schema),
        ],
        unbounded_input,
    );

    let expected_input = [
            "SortExec: expr=[a@0 ASC, c@2 ASC, d@3 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC, c@2 ASC]"
        ];
    // let optimized
    let expected_optimized = [
            "PartialSortExec: expr=[a@0 ASC, c@2 ASC, d@3 ASC], common_prefix_length=[2]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[a@0 ASC, c@2 ASC]",
        ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_not_replaced_with_partial_sort_for_bounded_input() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
    let parquet_input = parquet_exec_sorted(&schema, input_sort_exprs);

    let physical_plan = sort_exec(
        vec![
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ],
        parquet_input,
    );
    let expected_input = [
            "SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
            "  ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[b@1 ASC, c@2 ASC]"
        ];
    let expected_no_change = expected_input;
    assert_optimized!(expected_input, expected_no_change, physical_plan, false);
    Ok(())
}

#[tokio::test]
async fn test_not_replaced_with_partial_sort_for_unbounded_input() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs);

    let physical_plan = sort_exec(
        vec![
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ],
        unbounded_input,
    );
    let expected_input = [
            "SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
            "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
        ];
    let expected_no_change = expected_input;
    assert_optimized!(expected_input, expected_no_change, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_push_with_required_input_ordering_prohibited() -> Result<()> {
    // SortExec: expr=[b]            <-- can't push this down
    //  RequiredInputOrder expr=[a]  <-- this requires input sorted by a, and preserves the input order
    //    SortExec: expr=[a]
    //      MemoryExec
    let schema = create_test_schema3()?;
    let sort_exprs_a = LexOrdering::new(vec![sort_expr("a", &schema)]);
    let sort_exprs_b = LexOrdering::new(vec![sort_expr("b", &schema)]);
    let plan = memory_exec(&schema);
    let plan = sort_exec(sort_exprs_a.clone(), plan);
    let plan = RequirementsTestExec::new(plan)
        .with_required_input_ordering(sort_exprs_a)
        .with_maintains_input_order(true)
        .into_arc();
    let plan = sort_exec(sort_exprs_b, plan);

    let expected_input = [
        "SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
        "  RequiredInputOrderingExec",
        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    // should not be able to push shorts
    let expected_no_change = expected_input;
    assert_optimized!(expected_input, expected_no_change, plan, true);
    Ok(())
}

// test when the required input ordering is satisfied so could push through
#[tokio::test]
async fn test_push_with_required_input_ordering_allowed() -> Result<()> {
    // SortExec: expr=[a,b]          <-- can push this down (as it is compatible with the required input ordering)
    //  RequiredInputOrder expr=[a]  <-- this requires input sorted by a, and preserves the input order
    //    SortExec: expr=[a]
    //      MemoryExec
    let schema = create_test_schema3()?;
    let sort_exprs_a = LexOrdering::new(vec![sort_expr("a", &schema)]);
    let sort_exprs_ab =
        LexOrdering::new(vec![sort_expr("a", &schema), sort_expr("b", &schema)]);
    let plan = memory_exec(&schema);
    let plan = sort_exec(sort_exprs_a.clone(), plan);
    let plan = RequirementsTestExec::new(plan)
        .with_required_input_ordering(sort_exprs_a)
        .with_maintains_input_order(true)
        .into_arc();
    let plan = sort_exec(sort_exprs_ab, plan);

    let expected_input = [
        "SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "  RequiredInputOrderingExec",
        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "      MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    // should able to push shorts
    let expected = [
        "RequiredInputOrderingExec",
        "  SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "    MemoryExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected, plan, true);
    Ok(())
}
