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

use crate::memory_limit::DummyStreamPartition;
use crate::physical_optimizer::test_utils::{
    aggregate_exec, bounded_window_exec, bounded_window_exec_with_partition,
    check_integrity, coalesce_batches_exec, coalesce_partitions_exec, create_test_schema,
    create_test_schema2, create_test_schema3, filter_exec, global_limit_exec,
    hash_join_exec, local_limit_exec, memory_exec, parquet_exec, parquet_exec_with_sort,
    projection_exec, repartition_exec, sort_exec, sort_exec_with_fetch, sort_expr,
    sort_expr_options, sort_merge_join_exec, sort_preserving_merge_exec,
    sort_preserving_merge_exec_with_fetch, spr_repartition_exec, stream_exec_ordered,
    union_exec, RequirementsTestExec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TransformedResult};
use datafusion_common::{Result, ScalarValue, TableReference};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr_common::operator::Operator;
use datafusion_expr::{JoinType, SortExpr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    LexOrdering, PhysicalSortExpr, PhysicalSortRequirement, OrderingRequirements
};
use datafusion_physical_expr::{Distribution, Partitioning};
use datafusion_physical_expr::expressions::{col, BinaryExpr, Column, NotExpr};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{create_window_expr, BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{displayable, get_plan_string, ExecutionPlan, InputOrderMode};
use datafusion::datasource::physical_plan::CsvSource;
use datafusion::datasource::listing::PartitionedFile;
use datafusion_physical_optimizer::enforce_sorting::{EnforceSorting, PlanWithCorrespondingCoalescePartitions, PlanWithCorrespondingSort, parallelize_sorts, ensure_sorting};
use datafusion_physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{replace_with_order_preserving_variants, OrderPreservationContext};
use datafusion_physical_optimizer::enforce_sorting::sort_pushdown::{SortPushDown, assign_initial_requirements, pushdown_sorts};
use datafusion_physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion_physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::*;
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{Field};
use arrow_schema::Schema;
use datafusion_execution::TaskContext;
use datafusion_catalog::streaming::StreamingTable;

use futures::StreamExt;
use rstest::rstest;

/// Create a sorted Csv exec
fn csv_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let mut builder = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema.clone(),
        Arc::new(CsvSource::new(false, 0, 0)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100));
    if let Some(ordering) = LexOrdering::new(sort_exprs) {
        builder = builder.with_output_ordering(vec![ordering]);
    }

    let config = builder.build();
    DataSourceExec::from_data_source(config)
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

            let new_plan = if config.optimizer.repartition_sorts {
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
                        &config,
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
            EnforceSorting::new().optimize(physical_plan,&config)?;

        // Get string representation of the plan
        let actual = get_plan_string(&optimized_physical_plan);
        assert_eq!(
            expected_optimized_lines, actual,
            "\n**Optimized Plan Mismatch\n\nexpected:\n\n{expected_optimized_lines:#?}\nactual:\n\n{actual:#?}\n\n"
        );

    };
}

#[tokio::test]
async fn test_remove_unnecessary_sort5() -> Result<()> {
    let left_schema = create_test_schema2()?;
    let right_schema = create_test_schema3()?;
    let left_input = memory_exec(&left_schema);
    let parquet_ordering = [sort_expr("a", &right_schema)].into();
    let right_input =
        parquet_exec_with_sort(right_schema.clone(), vec![parquet_ordering]);
    let on = vec![(
        Arc::new(Column::new_with_schema("col_a", &left_schema)?) as _,
        Arc::new(Column::new_with_schema("c", &right_schema)?) as _,
    )];
    let join = hash_join_exec(left_input, right_input, on, None, &JoinType::Inner)?;
    let physical_plan = sort_exec([sort_expr("a", &join.schema())].into(), join);

    let expected_input = [
        "SortExec: expr=[a@2 ASC], preserve_partitioning=[false]",
        "  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    let expected_optimized = [
        "HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_do_not_remove_sort_with_limit() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort = sort_exec(ordering.clone(), source1);
    let limit = local_limit_exec(sort, 100);
    let parquet_ordering = [sort_expr("nullable_col", &schema)].into();
    let source2 = parquet_exec_with_sort(schema, vec![parquet_ordering]);
    let union = union_exec(vec![source2, limit]);
    let repartition = repartition_exec(union);
    let physical_plan = sort_preserving_merge_exec(ordering, repartition);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // We should keep the bottom `SortExec`.
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "      UnionExec",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "        LocalLimitExec: fetch=100",
        "          SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_sorted() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let sort = sort_exec(ordering.clone(), source1);
    let source2 = parquet_exec_with_sort(schema, vec![ordering.clone()]);
    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(ordering, union);

    // one input to the union is already sorted, one is not.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // should not add a sort at the output of the union, input plan should not be changed
    assert_optimized!(expected_input, expected_input, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let sort = sort_exec(ordering.clone(), source1);
    let parquet_ordering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let source2 = parquet_exec_with_sort(schema, vec![parquet_ordering]);
    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(ordering, union);

    // one input to the union is already sorted, one is not.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // should not add a sort at the output of the union, input plan should not be changed
    assert_optimized!(expected_input, expected_input, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted2() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let sort_exprs: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort = sort_exec(sort_exprs.clone(), source1);
    let parquet_ordering = [sort_expr("nullable_col", &schema)].into();
    let source2 = parquet_exec_with_sort(schema, vec![parquet_ordering]);
    let union = union_exec(vec![source2, sort]);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, union);

    // Input is an invalid plan. In this case rule should add required sorting in appropriate places.
    // First DataSourceExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy the
    // required ordering of SortPreservingMergeExec.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted3() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort1 = sort_exec(ordering1, source1.clone());
    let ordering2 = [sort_expr("nullable_col", &schema)].into();
    let sort2 = sort_exec(ordering2, source1);
    let parquet_ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let source2 = parquet_exec_with_sort(schema, vec![parquet_ordering.clone()]);
    let union = union_exec(vec![sort1, source2, sort2]);
    let physical_plan = sort_preserving_merge_exec(parquet_ordering, union);

    // First input to the union is not Sorted (SortExec is finer than required ordering by the SortPreservingMergeExec above).
    // Second input to the union is already Sorted (matches with the required ordering by the SortPreservingMergeExec above).
    // Third input to the union is not Sorted (SortExec is matches required ordering by the SortPreservingMergeExec above).
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // should adjust sorting in the first input of the union such that it is not unnecessarily fine
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted4() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let sort1 = sort_exec(ordering2.clone(), source1.clone());
    let sort2 = sort_exec(ordering2.clone(), source1);
    let source2 = parquet_exec_with_sort(schema, vec![ordering2]);
    let union = union_exec(vec![sort1, source2, sort2]);
    let physical_plan = sort_preserving_merge_exec(ordering1, union);

    // Ordering requirement of the `SortPreservingMergeExec` is not met.
    // Should modify the plan to ensure that all three inputs to the
    // `UnionExec` satisfy the ordering, OR add a single sort after
    // the `UnionExec` (both of which are equally good for this example).
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted5() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2 = [
        sort_expr("nullable_col", &schema),
        sort_expr_options(
            "non_nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ]
    .into();
    let ordering3 = [sort_expr("nullable_col", &schema)].into();
    let sort1 = sort_exec(ordering1, source1.clone());
    let sort2 = sort_exec(ordering2, source1);
    let union = union_exec(vec![sort1, sort2]);
    let physical_plan = sort_preserving_merge_exec(ordering3, union);

    // The `UnionExec` doesn't preserve any of the inputs ordering in the
    // example below. However, we should be able to change the unnecessarily
    // fine `SortExec`s below with required `SortExec`s that are absolutely necessary.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted6() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [sort_expr("nullable_col", &schema)].into();
    let sort1 = sort_exec(ordering1, source1.clone());
    let ordering2 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let repartition = repartition_exec(source1);
    let spm = sort_preserving_merge_exec(ordering2, repartition);
    let parquet_ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let source2 = parquet_exec_with_sort(schema, vec![parquet_ordering.clone()]);
    let union = union_exec(vec![sort1, source2, spm]);
    let physical_plan = sort_preserving_merge_exec(parquet_ordering, union);

    // The plan is not valid as it is -- the input ordering requirement
    // of the `SortPreservingMergeExec` under the third child of the
    // `UnionExec` is not met. We should add a `SortExec` below it.
    // At the same time, this ordering requirement is unnecessarily fine.
    // The final plan should be valid AND the ordering of the third child
    // shouldn't be finer than necessary.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // Should adjust the requirement in the third input of the union so
    // that it is not unnecessarily fine.
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted7() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort1 = sort_exec(ordering1.clone(), source1.clone());
    let sort2 = sort_exec(ordering1, source1);
    let union = union_exec(vec![sort1, sort2]);
    let ordering2 = [sort_expr("nullable_col", &schema)].into();
    let physical_plan = sort_preserving_merge_exec(ordering2, union);

    // Union has unnecessarily fine ordering below it. We should be able to replace them with absolutely necessary ordering.
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
    let expected_output = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_output, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted8() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2 = [
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
    ]
    .into();
    let sort1 = sort_exec(ordering1, source1.clone());
    let sort2 = sort_exec(ordering2, source1);
    let physical_plan = union_exec(vec![sort1, sort2]);

    // The `UnionExec` doesn't preserve any of the inputs ordering in the
    // example below.
    let expected_input = [
        "UnionExec",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // Since `UnionExec` doesn't preserve ordering in the plan above.
    // We shouldn't keep SortExecs in the plan.
    let expected_optimized = [
        "UnionExec",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_soft_hard_requirements_remove_soft_requirement() -> Result<()> {
    let schema = create_test_schema()?;
    let source = parquet_exec(schema.clone());
    let sort_exprs = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(sort_exprs, source);
    let partition_bys = &[col("nullable_col", &schema)?];
    let physical_plan =
        bounded_window_exec_with_partition("nullable_col", vec![], partition_bys, sort);

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_soft_hard_requirements_remove_soft_requirement_without_pushdowns(
) -> Result<()> {
    let schema = create_test_schema()?;
    let source = parquet_exec(schema.clone());
    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source.clone());
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("nullable_col", &schema)?,
            Operator::Plus,
            col("non_nullable_col", &schema)?,
        )) as _,
        "count".to_string(),
    )];
    let partition_bys = &[col("nullable_col", &schema)?];
    let bounded_window =
        bounded_window_exec_with_partition("nullable_col", vec![], partition_bys, sort);
    let physical_plan = projection_exec(proj_exprs, bounded_window)?;

    let expected_input = [
        "ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as count]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as count]",
    //     "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as count]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source);
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("nullable_col", &schema)?,
            Operator::Plus,
            col("non_nullable_col", &schema)?,
        )) as _,
        "nullable_col".to_string(),
    )];
    let partition_bys = &[col("nullable_col", &schema)?];
    let projection = projection_exec(proj_exprs, sort)?;
    let physical_plan = bounded_window_exec_with_partition(
        "nullable_col",
        vec![],
        partition_bys,
        projection,
    );

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "    SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "  ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
    //     "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "    ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_soft_hard_requirements_multiple_soft_requirements() -> Result<()> {
    let schema = create_test_schema()?;
    let source = parquet_exec(schema.clone());
    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source.clone());
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("nullable_col", &schema)?,
            Operator::Plus,
            col("non_nullable_col", &schema)?,
        )) as _,
        "nullable_col".to_string(),
    )];
    let partition_bys = &[col("nullable_col", &schema)?];
    let projection = projection_exec(proj_exprs, sort)?;
    let bounded_window = bounded_window_exec_with_partition(
        "nullable_col",
        vec![],
        partition_bys,
        projection,
    );
    let physical_plan = bounded_window_exec_with_partition(
        "count",
        vec![],
        partition_bys,
        bounded_window,
    );

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "    ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
    //     "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "      ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "        SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source);
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("nullable_col", &schema)?,
            Operator::Plus,
            col("non_nullable_col", &schema)?,
        )) as _,
        "nullable_col".to_string(),
    )];
    let partition_bys = &[col("nullable_col", &schema)?];
    let projection = projection_exec(proj_exprs, sort)?;
    let bounded_window = bounded_window_exec_with_partition(
        "nullable_col",
        vec![],
        partition_bys,
        projection,
    );

    let ordering2: LexOrdering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort2 = sort_exec(ordering2.clone(), bounded_window);
    let sort3 = sort_exec(ordering2, sort2);
    let physical_plan =
        bounded_window_exec_with_partition("count", vec![], partition_bys, sort3);

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "    SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "      BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "        ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "          SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "    ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
    //     "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "      ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "        SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_soft_hard_requirements_multiple_sorts() -> Result<()> {
    let schema = create_test_schema()?;
    let source = parquet_exec(schema.clone());
    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source);
    let proj_exprs = vec![(
        Arc::new(BinaryExpr::new(
            col("nullable_col", &schema)?,
            Operator::Plus,
            col("non_nullable_col", &schema)?,
        )) as _,
        "nullable_col".to_string(),
    )];
    let partition_bys = &[col("nullable_col", &schema)?];
    let projection = projection_exec(proj_exprs, sort)?;
    let bounded_window = bounded_window_exec_with_partition(
        "nullable_col",
        vec![],
        partition_bys,
        projection,
    );
    let ordering2: LexOrdering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort2 = sort_exec(ordering2.clone(), bounded_window);
    let physical_plan = sort_exec(ordering2, sort2);

    let expected_input = [
        "SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "    BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",        "      ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "        SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "  ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
    //     "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "      ProjectionExec: expr=[nullable_col@0 + non_nullable_col@1 as nullable_col]",
        "        SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_soft_hard_requirements_with_multiple_soft_requirements_and_output_requirement(
) -> Result<()> {
    let schema = create_test_schema()?;
    let source = parquet_exec(schema.clone());
    let ordering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering, source);
    let partition_bys1 = &[col("nullable_col", &schema)?];
    let bounded_window =
        bounded_window_exec_with_partition("nullable_col", vec![], partition_bys1, sort);
    let partition_bys2 = &[col("non_nullable_col", &schema)?];
    let bounded_window2 = bounded_window_exec_with_partition(
        "non_nullable_col",
        vec![],
        partition_bys2,
        bounded_window,
    );
    let requirement = [PhysicalSortRequirement::new(
        col("non_nullable_col", &schema)?,
        Some(SortOptions::new(false, true)),
    )]
    .into();
    let physical_plan = Arc::new(OutputRequirementExec::new(
        bounded_window2,
        Some(OrderingRequirements::new(requirement)),
        Distribution::SinglePartition,
        None,
    ));

    let expected_input = [
        "OutputRequirementExec: order_by=[(non_nullable_col@1, asc)], dist_by=SinglePartition",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    // TODO When sort pushdown respects to the alternatives, and removes soft SortExecs this should be changed
    // let expected_optimized = [
    //     "OutputRequirementExec",
    //     "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
    //     "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
    //     "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(UInt64(NULL)), end_bound: CurrentRow, is_causal: false }], mode=[Linear]",
    //     "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    // ];
    let expected_optimized = [
        "OutputRequirementExec: order_by=[(non_nullable_col@1, asc)], dist_by=SinglePartition",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
        "      BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "        SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_window_multi_path_sort() -> Result<()> {
    let schema = create_test_schema()?;
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2 = [sort_expr("nullable_col", &schema)].into();
    // Reverse of the above
    let ordering3: LexOrdering = [sort_expr_options(
        "nullable_col",
        &schema,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )]
    .into();
    let source1 = parquet_exec_with_sort(schema.clone(), vec![ordering1]);
    let source2 = parquet_exec_with_sort(schema, vec![ordering2]);
    let sort1 = sort_exec(ordering3.clone(), source1);
    let sort2 = sort_exec(ordering3.clone(), source2);
    let union = union_exec(vec![sort1, sort2]);
    let spm = sort_preserving_merge_exec(ordering3.clone(), union);
    let physical_plan = bounded_window_exec("nullable_col", ordering3, spm);

    // The `WindowAggExec` gets its sorting from multiple children jointly.
    // During the removal of `SortExec`s, it should be able to remove the
    // corresponding SortExecs together. Also, the inputs of these `SortExec`s
    // are not necessarily the same to be able to remove them.
    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 DESC NULLS LAST]",
        "    UnionExec",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
    ];
    let expected_optimized = [
        "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_window_multi_path_sort2() -> Result<()> {
    let schema = create_test_schema()?;
    let ordering1: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let source1 = parquet_exec_with_sort(schema.clone(), vec![ordering2.clone()]);
    let source2 = parquet_exec_with_sort(schema, vec![ordering2.clone()]);
    let sort1 = sort_exec(ordering1.clone(), source1);
    let sort2 = sort_exec(ordering1.clone(), source2);
    let union = union_exec(vec![sort1, sort2]);
    let spm = Arc::new(SortPreservingMergeExec::new(ordering1, union)) as _;
    let physical_plan = bounded_window_exec("nullable_col", ordering2, spm);

    // The `WindowAggExec` can get its required sorting from the leaf nodes directly.
    // The unnecessary SortExecs should be removed
    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "    UnionExec",
        "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
    ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_union_inputs_different_sorted_with_limit() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let ordering1 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let ordering2 = [
        sort_expr("nullable_col", &schema),
        sort_expr_options(
            "non_nullable_col",
            &schema,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ),
    ]
    .into();
    let sort1 = sort_exec(ordering1, source1.clone());
    let sort2 = sort_exec(ordering2, source1);
    let limit = local_limit_exec(sort2, 100);
    let limit = global_limit_exec(limit, 0, Some(100));
    let union = union_exec(vec![sort1, limit]);
    let ordering3 = [sort_expr("nullable_col", &schema)].into();
    let physical_plan = sort_preserving_merge_exec(ordering3, union);

    // Should not change the unnecessarily fine `SortExec`s because there is `LimitExec`
    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_sort_merge_join_order_by_left() -> Result<()> {
    let left_schema = create_test_schema()?;
    let right_schema = create_test_schema2()?;

    let left = parquet_exec(left_schema);
    let right = parquet_exec(right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema())?) as _,
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
        let ordering = [
            sort_expr("nullable_col", &join.schema()),
            sort_expr("non_nullable_col", &join.schema()),
        ]
        .into();
        let physical_plan = sort_preserving_merge_exec(ordering, join);

        let join_plan = format!(
            "SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let join_plan2 = format!(
            "  SortMergeJoin: join_type={join_type}, on=[(nullable_col@0, col_a@0)]"
        );
        let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
            join_plan2.as_str(),
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];
        let expected_optimized = match join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::LeftSemi
            | JoinType::LeftAnti => {
                // can push down the sort requirements and save 1 SortExec
                vec![
                    join_plan.as_str(),
                    "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
                    "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
                    "  SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
                ]
            }
            _ => {
                // can not push down the sort requirements
                vec![
                    "SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
                    join_plan2.as_str(),
                    "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
                    "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
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

    let left = parquet_exec(left_schema);
    let right = parquet_exec(right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema())?) as _,
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
        let ordering = [
            sort_expr("col_a", &join.schema()),
            sort_expr("col_b", &join.schema()),
        ]
        .into();
        let physical_plan = sort_preserving_merge_exec(ordering, join);

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
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
            "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];
        let expected_optimized = match join_type {
            JoinType::Inner | JoinType::Right | JoinType::RightAnti => {
                // can push down the sort requirements and save 1 SortExec
                vec![
                    join_plan.as_str(),
                    "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
                    "  SortExec: expr=[col_a@0 ASC, col_b@1 ASC], preserve_partitioning=[false]",
                    "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
                ]
            }
            _ => {
                // can not push down the sort requirements for Left and Full join.
                vec![
                    "SortExec: expr=[col_a@2 ASC, col_b@3 ASC], preserve_partitioning=[false]",
                    join_plan2.as_str(),
                    "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
                    "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
                    "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
                    "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
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

    let left = parquet_exec(left_schema);
    let right = parquet_exec(right_schema);

    // Join on (nullable_col == col_a)
    let join_on = vec![(
        Arc::new(Column::new_with_schema("nullable_col", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("col_a", &right.schema())?) as _,
    )];

    let join = sort_merge_join_exec(left, right, &join_on, &JoinType::Inner);

    // order by (col_b, col_a)
    let ordering = [
        sort_expr("col_b", &join.schema()),
        sort_expr("col_a", &join.schema()),
    ]
    .into();
    let physical_plan = sort_preserving_merge_exec(ordering, join.clone());

    let expected_input = [
        "SortPreservingMergeExec: [col_b@3 ASC, col_a@2 ASC]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
    ];
    // can not push down the sort requirements, need to add SortExec
    let expected_optimized = [
        "SortExec: expr=[col_b@3 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    // order by (nullable_col, col_b, col_a)
    let ordering2 = [
        sort_expr("nullable_col", &join.schema()),
        sort_expr("col_b", &join.schema()),
        sort_expr("col_a", &join.schema()),
    ]
    .into();
    let physical_plan = sort_preserving_merge_exec(ordering2, join);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, col_b@3 ASC, col_a@2 ASC]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
    ];
    // Can push down the sort requirements since col_a = nullable_col
    let expected_optimized = [
        "SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "  SortExec: expr=[col_a@0 ASC, col_b@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_multilayer_coalesce_partitions() -> Result<()> {
    let schema = create_test_schema()?;
    let source1 = parquet_exec(schema.clone());
    let repartition = repartition_exec(source1);
    let coalesce = coalesce_partitions_exec(repartition) as _;
    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let filter = filter_exec(
        Arc::new(NotExpr::new(col("non_nullable_col", schema.as_ref())?)),
        coalesce,
    );
    let ordering = [sort_expr("nullable_col", &schema)].into();
    let physical_plan = sort_exec(ordering, filter);

    // CoalescePartitionsExec and SortExec are not directly consecutive. In this case
    // we should be able to parallelize Sorting also (given that executors in between don't require)
    // single partition.
    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  FilterExec: NOT non_nullable_col@1",
        "    CoalescePartitionsExec",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "    FilterExec: NOT non_nullable_col@1",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_with_lost_ordering_bounded() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = [sort_expr("a", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs);
    let repartition_rr = repartition_exec(source);
    let repartition_hash = Arc::new(RepartitionExec::try_new(
        repartition_rr,
        Partitioning::Hash(vec![col("c", &schema)?], 10),
    )?) as _;
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan = sort_exec([sort_expr("a", &schema)].into(), coalesce_partitions);

    let expected_input = [
        "SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "  CoalescePartitionsExec",
        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_with_lost_ordering_unbounded_bounded(
    #[values(false, true)] source_unbounded: bool,
) -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = [sort_expr("a", &schema)];
    // create either bounded or unbounded source
    let source = if source_unbounded {
        stream_exec_ordered(&schema, sort_exprs.clone().into())
    } else {
        csv_exec_sorted(&schema, sort_exprs.clone())
    };
    let repartition_rr = repartition_exec(source);
    let repartition_hash = Arc::new(RepartitionExec::try_new(
        repartition_rr,
        Partitioning::Hash(vec![col("c", &schema)?], 10),
    )?) as _;
    let coalesce_partitions = coalesce_partitions_exec(repartition_hash);
    let physical_plan = sort_exec(sort_exprs.into(), coalesce_partitions);

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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false",
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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false",
    ];
    let expected_optimized_bounded_parallelize_sort = vec![
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false",
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
    let sort_exprs = [sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs.clone());
    let repartition_rr = repartition_exec(source);
    let spm = sort_preserving_merge_exec(sort_exprs.into(), repartition_rr);
    let physical_plan = sort_exec([sort_expr("b", &schema)].into(), spm);

    let expected_input = [
        "SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
        "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",
    ];
    let expected_optimized = [
        "SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
        "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}

#[tokio::test]
async fn test_pushdown_through_spm() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = [sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, sort_exprs.clone());
    let repartition_rr = repartition_exec(source);
    let spm = sort_preserving_merge_exec(sort_exprs.into(), repartition_rr);
    let physical_plan = sort_exec(
        [
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ]
        .into(),
        spm,
    );

    let expected_input = [
        "SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",
    ];
    let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "  SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}

#[tokio::test]
async fn test_window_multi_layer_requirement() -> Result<()> {
    let schema = create_test_schema3()?;
    let sort_exprs = [sort_expr("a", &schema), sort_expr("b", &schema)];
    let source = csv_exec_sorted(&schema, vec![]);
    let sort = sort_exec(sort_exprs.clone().into(), source);
    let repartition = repartition_exec(sort);
    let repartition = spr_repartition_exec(repartition);
    let spm = sort_preserving_merge_exec(sort_exprs.clone().into(), repartition);
    let physical_plan = bounded_window_exec("a", sort_exprs, spm);

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10, preserve_order=true, sort_exprs=a@0 ASC, b@1 ASC",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "    CoalescePartitionsExec",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, false);

    Ok(())
}
#[tokio::test]
async fn test_not_replaced_with_partial_sort_for_bounded_input() -> Result<()> {
    let schema = create_test_schema3()?;
    let parquet_ordering = [sort_expr("b", &schema), sort_expr("c", &schema)].into();
    let parquet_input = parquet_exec_with_sort(schema.clone(), vec![parquet_ordering]);
    let physical_plan = sort_exec(
        [
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ]
        .into(),
        parquet_input,
    );
    let expected_input = [
        "SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[b@1 ASC, c@2 ASC], file_type=parquet"
    ];
    let expected_no_change = expected_input;
    assert_optimized!(expected_input, expected_no_change, physical_plan, false);
    Ok(())
}

/// Runs the sort enforcement optimizer and asserts the plan
/// against the original and expected plans
///
/// `$EXPECTED_PLAN_LINES`: input plan
/// `$EXPECTED_OPTIMIZED_PLAN_LINES`: optimized plan
/// `$PLAN`: the plan to optimized
/// `REPARTITION_SORTS`: Flag to set `config.options.optimizer.repartition_sorts` option.
/// `$CASE_NUMBER` (optional): The test case number to print on failure.
macro_rules! assert_optimized {
    ($EXPECTED_PLAN_LINES: expr, $EXPECTED_OPTIMIZED_PLAN_LINES: expr, $PLAN: expr, $REPARTITION_SORTS: expr $(, $CASE_NUMBER: expr)?) => {
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

            let new_plan = if config.optimizer.repartition_sorts {
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
                        &config,
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

        if expected_plan_lines != actual {
            $(println!("\n**Original Plan Mismatch in case {}**", $CASE_NUMBER);)?
            println!("\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n", expected_plan_lines, actual);
            assert_eq!(expected_plan_lines, actual);
        }

        let expected_optimized_lines: Vec<&str> = $EXPECTED_OPTIMIZED_PLAN_LINES
            .iter().map(|s| *s).collect();

        // Run the actual optimizer
        let optimized_physical_plan =
            EnforceSorting::new().optimize(physical_plan, &config)?;

        // Get string representation of the plan
        let actual = get_plan_string(&optimized_physical_plan);
        if expected_optimized_lines != actual {
            $(println!("\n**Optimized Plan Mismatch in case {}**", $CASE_NUMBER);)?
            println!("\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n", expected_optimized_lines, actual);
            assert_eq!(expected_optimized_lines, actual);
        }
    };
}

#[tokio::test]
async fn test_remove_unnecessary_sort() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec([sort_expr("non_nullable_col", &schema)].into(), source);
    let physical_plan = sort_exec([sort_expr("nullable_col", &schema)].into(), input);

    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort_window_multilayer() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr_options(
        "non_nullable_col",
        &source.schema(),
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    )]
    .into();
    let sort = sort_exec(ordering.clone(), source);
    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let coalesce_batches = coalesce_batches_exec(sort, 128);
    let window_agg = bounded_window_exec("non_nullable_col", ordering, coalesce_batches);
    let ordering2: LexOrdering = [sort_expr_options(
        "non_nullable_col",
        &window_agg.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )]
    .into();
    let sort = sort_exec(ordering2.clone(), window_agg);
    // Add dummy layer propagating Sort above, to test whether sort can be removed from multi layer before
    let filter = filter_exec(
        Arc::new(NotExpr::new(col("non_nullable_col", schema.as_ref())?)),
        sort,
    );
    let physical_plan = bounded_window_exec("non_nullable_col", ordering2, filter);

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  FilterExec: NOT non_nullable_col@1",
        "    SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
        "      BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "        CoalesceBatchesExec: target_batch_size=128",
        "          SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
        "            DataSourceExec: partitions=1, partition_sizes=[0]"
    ];

    let expected_optimized = [
        "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
        "  FilterExec: NOT non_nullable_col@1",
        "    BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "      CoalesceBatchesExec: target_batch_size=128",
        "        SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
        "          DataSourceExec: partitions=1, partition_sizes=[0]"
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_add_required_sort() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering = [sort_expr("nullable_col", &schema)].into();
    let physical_plan = sort_preserving_merge_exec(ordering, source);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort1() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    let sort = sort_exec(ordering.clone(), source);
    let spm = sort_preserving_merge_exec(ordering.clone(), sort);
    let sort = sort_exec(ordering.clone(), spm);
    let physical_plan = sort_preserving_merge_exec(ordering, sort);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    SortPreservingMergeExec: [nullable_col@0 ASC]",
        "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort2() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr("non_nullable_col", &schema)].into();
    let sort = sort_exec(ordering.clone(), source);
    let spm = sort_preserving_merge_exec(ordering, sort);
    let ordering2: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort2 = sort_exec(ordering2.clone(), spm);
    let spm2 = sort_preserving_merge_exec(ordering2, sort2);
    let ordering3 = [sort_expr("nullable_col", &schema)].into();
    let sort3 = sort_exec(ordering3, spm2);
    let physical_plan = repartition_exec(repartition_exec(sort3));

    let expected_input = [
        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "          SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "            SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "              DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=10",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort3() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr("non_nullable_col", &schema)].into();
    let sort = sort_exec(ordering.clone(), source);
    let spm = sort_preserving_merge_exec(ordering, sort);
    let repartition_exec = repartition_exec(spm);
    let ordering2: LexOrdering = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let sort2 = Arc::new(
        SortExec::new(ordering2.clone(), repartition_exec)
            .with_preserve_partitioning(true),
    ) as _;
    let spm2 = sort_preserving_merge_exec(ordering2, sort2);
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
        "            DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "AggregateExec: mode=Final, gby=[], aggr=[]",
        "  CoalescePartitionsExec",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
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
    let ordering: LexOrdering = [sort_expr("non_nullable_col", &schema)].into();
    let sort =
        Arc::new(SortExec::new(ordering.clone(), union).with_preserve_partitioning(true))
            as _;
    let spm = sort_preserving_merge_exec(ordering, sort);
    let filter = filter_exec(
        Arc::new(NotExpr::new(col("non_nullable_col", schema.as_ref())?)),
        spm,
    );
    let ordering2 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ]
    .into();
    let physical_plan = sort_exec(ordering2, filter);

    // When removing a `SortPreservingMergeExec`, make sure that partitioning
    // requirements are not violated. In some cases, we may need to replace
    // it with a `CoalescePartitionsExec` instead of directly removing it.
    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "  FilterExec: NOT non_nullable_col@1",
        "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "      SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "        UnionExec",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            DataSourceExec: partitions=1, partition_sizes=[0]",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "    FilterExec: NOT non_nullable_col@1",
        "      UnionExec",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: partitions=1, partition_sizes=[0]",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort6() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec_with_fetch(
        [sort_expr("non_nullable_col", &schema)].into(),
        Some(2),
        source,
    );
    let physical_plan = sort_exec(
        [
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ]
        .into(),
        input,
    );

    let expected_input = [
        "SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "  SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort7() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec(
        [
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ]
        .into(),
        source,
    );
    let physical_plan = sort_exec_with_fetch(
        [sort_expr("non_nullable_col", &schema)].into(),
        Some(2),
        input,
    );

    let expected_input = [
        "SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC], preserve_partitioning=[false], sort_prefix=[non_nullable_col@1 ASC]",
        "  SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "GlobalLimitExec: skip=0, fetch=2",
        "  SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_sort8() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec([sort_expr("non_nullable_col", &schema)].into(), source);
    let limit = Arc::new(LocalLimitExec::new(input, 2));
    let physical_plan = sort_exec(
        [
            sort_expr("non_nullable_col", &schema),
            sort_expr("nullable_col", &schema),
        ]
        .into(),
        limit,
    );

    let expected_input = [
        "SortExec: expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "  LocalLimitExec: fetch=2",
        "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "LocalLimitExec: fetch=2",
        "  SortExec: TopK(fetch=2), expr=[non_nullable_col@1 ASC, nullable_col@0 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_do_not_pushdown_through_limit() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_exec([sort_expr("non_nullable_col", &schema)].into(), source);
    let limit = Arc::new(GlobalLimitExec::new(input, 0, Some(5))) as _;
    let physical_plan = sort_exec([sort_expr("nullable_col", &schema)].into(), limit);

    let expected_input = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  GlobalLimitExec: skip=0, fetch=5",
        "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  GlobalLimitExec: skip=0, fetch=5",
        "    SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_spm1() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering: LexOrdering = [sort_expr("non_nullable_col", &schema)].into();
    let input = sort_preserving_merge_exec(ordering.clone(), source);
    let input2 = sort_preserving_merge_exec(ordering, input);
    let physical_plan =
        sort_preserving_merge_exec([sort_expr("nullable_col", &schema)].into(), input2);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "    SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_remove_unnecessary_spm2() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let input = sort_preserving_merge_exec_with_fetch(
        [sort_expr("non_nullable_col", &schema)].into(),
        source,
        100,
    );

    let expected_input = [
        "SortPreservingMergeExec: [non_nullable_col@1 ASC], fetch=100",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "LocalLimitExec: fetch=100",
        "  SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, input, true);

    Ok(())
}

#[tokio::test]
async fn test_change_wrong_sorting() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort = sort_exec([sort_exprs[0].clone()].into(), source);
    let physical_plan = sort_preserving_merge_exec(sort_exprs.into(), sort);

    let expected_input = [
        "SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_change_wrong_sorting2() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let sort_exprs = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let spm1 = sort_preserving_merge_exec(sort_exprs.clone().into(), source);
    let sort2 = sort_exec([sort_exprs[0].clone()].into(), spm1);
    let physical_plan = sort_preserving_merge_exec([sort_exprs[1].clone()].into(), sort2);

    let expected_input = [
        "SortPreservingMergeExec: [non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "    SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortExec: expr=[non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_multiple_sort_window_exec() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let ordering1 = [sort_expr("nullable_col", &schema)];
    let sort1 = sort_exec(ordering1.clone().into(), source);
    let window_agg1 = bounded_window_exec("non_nullable_col", ordering1.clone(), sort1);
    let ordering2 = [
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let window_agg2 = bounded_window_exec("non_nullable_col", ordering2, window_agg1);
    let physical_plan = bounded_window_exec("non_nullable_col", ordering1, window_agg2);

    let expected_input = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "      SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "        SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "          DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
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
    let memory_exec = memory_exec(&schema);
    let sort_exprs = [sort_expr("nullable_col", &schema)];
    let window = bounded_window_exec("nullable_col", sort_exprs.clone(), memory_exec);
    let repartition = repartition_exec(window);
    let orig_plan = sort_exec(sort_exprs.into(), repartition);

    let actual = get_plan_string(&orig_plan);
    let expected_input = vec![
        "SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "    BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_eq!(
        expected_input, actual,
        "\n**Original Plan Mismatch\n\nexpected:\n\n{expected_input:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let config = ConfigOptions::new();
    let rules = vec![
        Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
    ];
    let mut first_plan = orig_plan.clone();
    for rule in rules {
        first_plan = rule.optimize(first_plan, &config)?;
    }

    let rules = vec![
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
    ];
    let mut second_plan = orig_plan.clone();
    for rule in rules {
        second_plan = rule.optimize(second_plan, &config)?;
    }

    assert_eq!(get_plan_string(&first_plan), get_plan_string(&second_plan));
    Ok(())
}

#[tokio::test]
async fn test_coalesce_propagate() -> Result<()> {
    let schema = create_test_schema()?;
    let source = memory_exec(&schema);
    let repartition = repartition_exec(source);
    let coalesce_partitions = coalesce_partitions_exec(repartition);
    let repartition = repartition_exec(coalesce_partitions);
    let ordering: LexOrdering = [sort_expr("nullable_col", &schema)].into();
    // Add local sort
    let sort = Arc::new(
        SortExec::new(ordering.clone(), repartition).with_preserve_partitioning(true),
    ) as _;
    let spm = sort_preserving_merge_exec(ordering.clone(), sort);
    let sort = sort_exec(ordering, spm);

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
        "            DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    let expected_optimized = [
        "SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, physical_plan, true);

    Ok(())
}

#[tokio::test]
async fn test_replace_with_partial_sort2() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering = [sort_expr("a", &schema), sort_expr("c", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering);
    let physical_plan = sort_exec(
        [
            sort_expr("a", &schema),
            sort_expr("c", &schema),
            sort_expr("d", &schema),
        ]
        .into(),
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
async fn test_push_with_required_input_ordering_prohibited() -> Result<()> {
    let schema = create_test_schema3()?;
    let ordering_a: LexOrdering = [sort_expr("a", &schema)].into();
    let ordering_b: LexOrdering = [sort_expr("b", &schema)].into();
    let plan = memory_exec(&schema);
    let plan = sort_exec(ordering_a.clone(), plan);
    let plan = RequirementsTestExec::new(plan)
        .with_required_input_ordering(Some(ordering_a))
        .with_maintains_input_order(true)
        .into_arc();
    let plan = sort_exec(ordering_b, plan);

    let expected_input = [
        "SortExec: expr=[b@1 ASC], preserve_partitioning=[false]", // <-- can't push this down
        "  RequiredInputOrderingExec", // <-- this requires input sorted by a, and preserves the input order
        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    // should not be able to push shorts
    assert_optimized!(expected_input, expected_input, plan, true);
    Ok(())
}

// test when the required input ordering is satisfied so could push through
#[tokio::test]
async fn test_push_with_required_input_ordering_allowed() -> Result<()> {
    let schema = create_test_schema3()?;
    let ordering_a: LexOrdering = [sort_expr("a", &schema)].into();
    let ordering_ab = [sort_expr("a", &schema), sort_expr("b", &schema)].into();
    let plan = memory_exec(&schema);
    let plan = sort_exec(ordering_a.clone(), plan);
    let plan = RequirementsTestExec::new(plan)
        .with_required_input_ordering(Some(ordering_a))
        .with_maintains_input_order(true)
        .into_arc();
    let plan = sort_exec(ordering_ab, plan);

    let expected_input = [
        "SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]", // <-- can push this down (as it is compatible with the required input ordering)
        "  RequiredInputOrderingExec", // <-- this requires input sorted by a, and preserves the input order
        "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    // Should be able to push down
    let expected_optimized = [
        "RequiredInputOrderingExec",
        "  SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected_optimized, plan, true);
    Ok(())
}

#[tokio::test]
async fn test_replace_with_partial_sort() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering = [sort_expr("a", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering);
    let physical_plan = sort_exec(
        [sort_expr("a", &schema), sort_expr("c", &schema)].into(),
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
async fn test_not_replaced_with_partial_sort_for_unbounded_input() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering = [sort_expr("b", &schema), sort_expr("c", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering);
    let physical_plan = sort_exec(
        [
            sort_expr("a", &schema),
            sort_expr("b", &schema),
            sort_expr("c", &schema),
        ]
        .into(),
        unbounded_input,
    );
    let expected_input = [
        "SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_optimized!(expected_input, expected_input, physical_plan, true);
    Ok(())
}

#[tokio::test]
async fn test_window_partial_constant_and_set_monotonicity() -> Result<()> {
    let input_schema = create_test_schema()?;
    let ordering = [sort_expr_options(
        "nullable_col",
        &input_schema,
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )]
    .into();
    let source = parquet_exec_with_sort(input_schema.clone(), vec![ordering]) as _;

    // Function definition - Alias of the resulting column - Arguments of the function
    #[derive(Clone)]
    struct WindowFuncParam(WindowFunctionDefinition, String, Vec<Arc<dyn PhysicalExpr>>);
    let function_arg_ordered = vec![col("nullable_col", &input_schema)?];
    let function_arg_unordered = vec![col("non_nullable_col", &input_schema)?];
    let fn_count_on_ordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count".to_string(),
        function_arg_ordered.clone(),
    );
    let fn_max_on_ordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(max_udaf()),
        "max".to_string(),
        function_arg_ordered.clone(),
    );
    let fn_min_on_ordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(min_udaf()),
        "min".to_string(),
        function_arg_ordered.clone(),
    );
    let fn_avg_on_ordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(avg_udaf()),
        "avg".to_string(),
        function_arg_ordered,
    );
    let fn_count_on_unordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "count".to_string(),
        function_arg_unordered.clone(),
    );
    let fn_max_on_unordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(max_udaf()),
        "max".to_string(),
        function_arg_unordered.clone(),
    );
    let fn_min_on_unordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(min_udaf()),
        "min".to_string(),
        function_arg_unordered.clone(),
    );
    let fn_avg_on_unordered = WindowFuncParam(
        WindowFunctionDefinition::AggregateUDF(avg_udaf()),
        "avg".to_string(),
        function_arg_unordered,
    );
    struct TestCase<'a> {
        // Whether window expression has a partition_by expression or not.
        // If it does, it will be on the ordered column -- `nullable_col`.
        partition_by: bool,
        // Whether the frame is unbounded in both directions, or unbounded in
        // only one direction (when set-monotonicity has a meaning), or it is
        // a sliding window.
        window_frame: Arc<WindowFrame>,
        // Function definition - Alias of the resulting column - Arguments of the function
        func: WindowFuncParam,
        // Global sort requirement at the root and its direction,
        // which is required to be removed or preserved -- (asc, nulls_first)
        required_sort_columns: Vec<(&'a str, bool, bool)>,
        initial_plan: Vec<&'a str>,
        expected_plan: Vec<&'a str>,
    }
    let test_cases = vec![
        // ============================================REGION STARTS============================================
        // WindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on ordered column
        // Case 0:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 1:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 2:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 3:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on unordered column
        // Case 4:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("non_nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 5:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("non_nullable_col", false, false), ("max", false, false)],
            initial_plan: vec![
                "SortExec: expr=[non_nullable_col@1 DESC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 6:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", true, false), ("non_nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST, non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 7:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("avg", false, false), ("nullable_col", false, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on ordered column
        // Case 8:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 9:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 10:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 11:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on unordered column
        // Case 12:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("non_nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 13:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("non_nullable_col", true, false), ("max", false, false)],
            initial_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[non_nullable_col@1 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 14:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", false, false), ("non_nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, non_nullable_col@1 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 15:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(None)),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("avg", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(NULL)), end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Sliding(current row, unbounded following) + no partition_by + on ordered column
        // Case 16:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 17:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("max", false, true), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[max@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 18:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", true, true), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 ASC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 19:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Sliding(current row, unbounded following) + no partition_by + on unordered column
        // Case 20:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 21:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", false, true)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 22:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 23:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("avg", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Sliding(current row, unbounded following) + partition_by + on ordered column
        // Case 24:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 25:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 26:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 27:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", false, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // WindowAggExec + Sliding(current row, unbounded following) + partition_by + on unordered column
        // Case 28:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("count", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[count@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
        },
        // Case 29:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", false, true)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 30:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", false, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 31:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true)).reverse()),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(UInt64(NULL)), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on ordered column
        // Case 32:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 33:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("max", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[max@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
        },
        // Case 34:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 35:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + no partition_by + on unordered column
        // Case 36:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, true)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 37:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("max", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 38:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", false, true), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 39:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on ordered column
        // Case 40:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 41:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("max", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
        },
        // Case 42:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 43:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Plain(unbounded preceding, unbounded following) + partition_by + on unordered column
        // Case 44:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![ ("count", true, true)],
            initial_plan: vec![
                "SortExec: expr=[count@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",  ],
        },
        // Case 45:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 46:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("min", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 47:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new(Some(true))),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + no partition_by + on ordered column
        // Case 48:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("count", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 49:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32)?))),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("max", true, false)],
            initial_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 50:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("min", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 51:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + no partition_by + on unordered column
        // Case 52:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32)?))),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("count", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
        },
        // Case 53:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 54:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("min", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 55:
        TestCase {
            partition_by: false,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32)?))),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + partition_by + on ordered column
        // Case 56:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_count_on_ordered.clone(),
            required_sort_columns: vec![("count", true, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 57:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::Following(ScalarValue::new_one(&DataType::UInt32)?))),
            func: fn_max_on_ordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 58:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_min_on_ordered.clone(),
            required_sort_columns: vec![("min", false, false), ("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 59:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_avg_on_ordered.clone(),
            required_sort_columns: vec![("avg", true, false)],
            initial_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // ============================================REGION STARTS============================================
        // BoundedWindowAggExec + Sliding(bounded preceding, bounded following) + partition_by + on unordered column
        // Case 60:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_count_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("count", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 61:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_max_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("max", true, true)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 62:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_min_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false), ("min", false, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // Case 63:
        TestCase {
            partition_by: true,
            window_frame: Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Rows, WindowFrameBound::Preceding(ScalarValue::new_one(&DataType::UInt32)?), WindowFrameBound::CurrentRow)),
            func: fn_avg_on_unordered.clone(),
            required_sort_columns: vec![("nullable_col", true, false)],
            initial_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, frame: ROWS BETWEEN 1 PRECEDING AND CURRENT ROW], mode=[Sorted]",
                "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
        },
        // =============================================REGION ENDS=============================================
    ];

    for (case_idx, case) in test_cases.into_iter().enumerate() {
        let partition_by = if case.partition_by {
            vec![col("nullable_col", &input_schema)?]
        } else {
            vec![]
        };
        let window_expr = create_window_expr(
            &case.func.0,
            case.func.1,
            &case.func.2,
            &partition_by,
            &[],
            case.window_frame,
            input_schema.as_ref(),
            false,
            false,
        )?;
        let window_exec = if window_expr.uses_bounded_memory() {
            Arc::new(BoundedWindowAggExec::try_new(
                vec![window_expr],
                Arc::clone(&source),
                InputOrderMode::Sorted,
                case.partition_by,
            )?) as Arc<dyn ExecutionPlan>
        } else {
            Arc::new(WindowAggExec::try_new(
                vec![window_expr],
                Arc::clone(&source),
                case.partition_by,
            )?) as _
        };
        let output_schema = window_exec.schema();
        let sort_expr = case
            .required_sort_columns
            .iter()
            .map(|(col_name, asc, nf)| {
                sort_expr_options(
                    col_name,
                    &output_schema,
                    SortOptions {
                        descending: !asc,
                        nulls_first: *nf,
                    },
                )
            })
            .collect::<Vec<_>>();
        let ordering = LexOrdering::new(sort_expr).unwrap();
        let physical_plan = sort_exec(ordering, window_exec);

        assert_optimized!(
            case.initial_plan,
            case.expected_plan,
            physical_plan,
            true,
            case_idx
        );
    }

    Ok(())
}

#[test]
fn test_removes_unused_orthogonal_sort() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering: LexOrdering =
        [sort_expr("b", &schema), sort_expr("c", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering.clone());
    let orthogonal_sort = sort_exec([sort_expr("a", &schema)].into(), unbounded_input);
    let output_sort = sort_exec(input_ordering, orthogonal_sort); // same sort as data source

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input);

    // Test: should remove orthogonal sort, and the uppermost (unneeded) sort:
    let expected_optimized = [
        "StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_optimized!(expected_input, expected_optimized, output_sort, true);

    Ok(())
}

#[test]
fn test_keeps_used_orthogonal_sort() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering: LexOrdering =
        [sort_expr("b", &schema), sort_expr("c", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering.clone());
    let orthogonal_sort =
        sort_exec_with_fetch([sort_expr("a", &schema)].into(), Some(3), unbounded_input); // has fetch, so this orthogonal sort changes the output
    let output_sort = sort_exec(input_ordering, orthogonal_sort);

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input);

    // Test: should keep the orthogonal sort, since it modifies the output:
    let expected_optimized = expected_input;
    assert_optimized!(expected_input, expected_optimized, output_sort, true);

    Ok(())
}

#[test]
fn test_handles_multiple_orthogonal_sorts() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_ordering: LexOrdering =
        [sort_expr("b", &schema), sort_expr("c", &schema)].into();
    let unbounded_input = stream_exec_ordered(&schema, input_ordering.clone());
    let ordering0: LexOrdering = [sort_expr("c", &schema)].into();
    let orthogonal_sort_0 = sort_exec(ordering0.clone(), unbounded_input); // has no fetch, so can be removed
    let ordering1: LexOrdering = [sort_expr("a", &schema)].into();
    let orthogonal_sort_1 =
        sort_exec_with_fetch(ordering1.clone(), Some(3), orthogonal_sort_0); // has fetch, so this orthogonal sort changes the output
    let orthogonal_sort_2 = sort_exec(ordering0, orthogonal_sort_1); // has no fetch, so can be removed
    let orthogonal_sort_3 = sort_exec(ordering1, orthogonal_sort_2); // has no fetch, so can be removed
    let output_sort = sort_exec(input_ordering, orthogonal_sort_3); // final sort

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "      SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "        SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "          StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]",
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input);

    // Test: should keep only the needed orthogonal sort, and remove the unneeded ones:
    let expected_optimized = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]",
    ];
    assert_optimized!(expected_input, expected_optimized, output_sort, true);

    Ok(())
}

#[test]
fn test_parallelize_sort_preserves_fetch() -> Result<()> {
    // Create a schema
    let schema = create_test_schema3()?;
    let parquet_exec = parquet_exec(schema);
    let coalesced = coalesce_partitions_exec(parquet_exec.clone());
    let top_coalesced = coalesce_partitions_exec(coalesced.clone())
        .with_fetch(Some(10))
        .unwrap();

    let requirements = PlanWithCorrespondingCoalescePartitions::new(
        top_coalesced,
        true,
        vec![PlanWithCorrespondingCoalescePartitions::new(
            coalesced,
            true,
            vec![PlanWithCorrespondingCoalescePartitions::new(
                parquet_exec,
                false,
                vec![],
            )],
        )],
    );

    let res = parallelize_sorts(requirements)?;

    // Verify fetch was preserved
    assert_eq!(
        res.data.plan.fetch(),
        Some(10),
        "Fetch value was not preserved after transformation"
    );
    Ok(())
}

#[tokio::test]
async fn test_partial_sort_with_homogeneous_batches() -> Result<()> {
    // Create schema for the table
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    // Create homogeneous batches - each batch has the same values for columns a and b
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(Int32Array::from(vec![1, 1, 1])),
            Arc::new(Int32Array::from(vec![3, 2, 1])),
        ],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2, 2, 2])),
            Arc::new(Int32Array::from(vec![2, 2, 2])),
            Arc::new(Int32Array::from(vec![4, 6, 5])),
        ],
    )?;
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 3, 3])),
            Arc::new(Int32Array::from(vec![3, 3, 3])),
            Arc::new(Int32Array::from(vec![9, 7, 8])),
        ],
    )?;

    // Create session with batch size of 3 to match our homogeneous batch pattern
    let session_config = SessionConfig::new()
        .with_batch_size(3)
        .with_target_partitions(1);
    let ctx = SessionContext::new_with_config(session_config);

    let sort_order = vec![
        SortExpr::new(
            Expr::Column(datafusion_common::Column::new(
                Option::<TableReference>::None,
                "a",
            )),
            true,
            false,
        ),
        SortExpr::new(
            Expr::Column(datafusion_common::Column::new(
                Option::<TableReference>::None,
                "b",
            )),
            true,
            false,
        ),
    ];
    let batches = Arc::new(DummyStreamPartition {
        schema: schema.clone(),
        batches: vec![batch1, batch2, batch3],
    }) as _;
    let provider = StreamingTable::try_new(schema.clone(), vec![batches])?
        .with_sort_order(sort_order)
        .with_infinite_table(true);
    ctx.register_table("test_table", Arc::new(provider))?;

    let sql = "SELECT * FROM test_table ORDER BY a ASC, c ASC";
    let df = ctx.sql(sql).await?;

    let physical_plan = df.create_physical_plan().await?;

    // Verify that PartialSortExec is used
    let plan_str = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert!(
        plan_str.contains("PartialSortExec"),
        "Expected PartialSortExec in plan:\n{plan_str}",
    );

    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = physical_plan.execute(0, task_ctx.clone())?;

    let mut collected_batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() > 0 {
            collected_batches.push(batch);
        }
    }

    // Assert we got 3 separate batches (not concatenated into fewer)
    assert_eq!(
        collected_batches.len(),
        3,
        "Expected 3 separate batches, got {}",
        collected_batches.len()
    );

    // Verify each batch has been sorted within itself
    let expected_values = [vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];

    for (i, batch) in collected_batches.iter().enumerate() {
        let c_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual = c_array.values().iter().copied().collect::<Vec<i32>>();
        assert_eq!(actual, expected_values[i], "Batch {i} not sorted correctly",);
    }

    assert_eq!(
        task_ctx.runtime_env().memory_pool.reserved(),
        0,
        "Memory should be released after execution"
    );

    Ok(())
}
