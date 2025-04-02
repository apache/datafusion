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
    aggregate_exec, bounded_window_exec, check_integrity, coalesce_batches_exec,
    coalesce_partitions_exec, create_test_schema, create_test_schema2,
    create_test_schema3, filter_exec, global_limit_exec, hash_join_exec, limit_exec,
    local_limit_exec, memory_exec, parquet_exec, repartition_exec, sort_exec,
    sort_exec_with_fetch, sort_expr, sort_expr_options, sort_merge_join_exec,
    sort_preserving_merge_exec, sort_preserving_merge_exec_with_fetch,
    spr_repartition_exec, stream_exec_ordered, union_exec, RequirementsTestExec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TransformedResult};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{JoinType, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_expr::expressions::{col, Column, NotExpr};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{create_window_expr, BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{displayable, get_plan_string, ExecutionPlan, InputOrderMode};
use datafusion::datasource::physical_plan::{CsvSource, ParquetSource};
use datafusion::datasource::listing::PartitionedFile;
use datafusion_physical_optimizer::enforce_sorting::{EnforceSorting, PlanWithCorrespondingCoalescePartitions, PlanWithCorrespondingSort, parallelize_sorts, ensure_sorting};
use datafusion_physical_optimizer::enforce_sorting::replace_with_order_preserving_variants::{replace_with_order_preserving_variants, OrderPreservationContext};
use datafusion_physical_optimizer::enforce_sorting::sort_pushdown::{SortPushDown, assign_initial_requirements, pushdown_sorts};
use datafusion_physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_functions_aggregate::average::avg_udaf;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use rstest::rstest;

/// Create a csv exec for tests
fn csv_exec_ordered(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema.clone(),
        Arc::new(CsvSource::new(true, 0, b'"')),
    )
    .with_file(PartitionedFile::new("file_path".to_string(), 100))
    .with_output_ordering(vec![sort_exprs])
    .build();

    DataSourceExec::from_data_source(config)
}

/// Created a sorted parquet exec
pub fn parquet_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    let source = Arc::new(ParquetSource::default());
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema.clone(),
        source,
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_output_ordering(vec![sort_exprs])
    .build();

    DataSourceExec::from_data_source(config)
}

/// Create a sorted Csv exec
fn csv_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("test:///").unwrap(),
        schema.clone(),
        Arc::new(CsvSource::new(false, 0, 0)),
    )
    .with_file(PartitionedFile::new("x".to_string(), 100))
    .with_output_ordering(vec![sort_exprs])
    .build();

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
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet"];

    let expected_optimized = ["HashJoinExec: mode=Partitioned, join_type=Inner, on=[(col_a@0, c@2)]",
        "  DataSourceExec: partitions=1, partition_sizes=[0]",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      GlobalLimitExec: skip=0, fetch=100",
        "        LocalLimitExec: fetch=100",
        "          SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "            DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];

    // We should keep the bottom `SortExec`.
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2",
        "      UnionExec",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "        GlobalLimitExec: skip=0, fetch=100",
        "          LocalLimitExec: fetch=100",
        "            SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "              DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
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
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
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
    // First DataSourceExec has output ordering(nullable_col@0 ASC). However, it doesn't satisfy the
    // required ordering of SortPreservingMergeExec.
    let expected_input = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];

    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    // should adjust sorting in the first input of the union such that it is not unnecessarily fine
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    // Should adjust the requirement in the third input of the union so
    // that it is not unnecessarily fine.
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    // Union preserves the inputs ordering and we should not change any of the SortExecs under UnionExec
    let expected_output = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "  SortExec: expr=[nullable_col@0 DESC NULLS LAST, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    // Since `UnionExec` doesn't preserve ordering in the plan above.
    // We shouldn't keep SortExecs in the plan.
    let expected_optimized = ["UnionExec",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "  DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "      SortExec: expr=[nullable_col@0 DESC NULLS LAST], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet"];
    let expected_optimized = [
        "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC, non_nullable_col@1 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet"];
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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet"];
    let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortPreservingMergeExec: [nullable_col@0 ASC]",
        "    UnionExec",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC], file_type=parquet"];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  UnionExec",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    GlobalLimitExec: skip=0, fetch=100",
        "      LocalLimitExec: fetch=100",
        "        SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 DESC NULLS LAST], preserve_partitioning=[false]",
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];

    // can not push down the sort requirements, need to add SortExec
    let expected_optimized = ["SortExec: expr=[col_b@3 ASC, col_a@2 ASC], preserve_partitioning=[false]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];
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
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];

    // can not push down the sort requirements, need to add SortExec
    let expected_optimized = ["SortExec: expr=[nullable_col@0 ASC, col_b@3 ASC, col_a@2 ASC], preserve_partitioning=[false]",
        "  SortMergeJoin: join_type=Inner, on=[(nullable_col@0, col_a@0)]",
        "    SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet",
        "    SortExec: expr=[col_a@0 ASC], preserve_partitioning=[false]",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[col_a, col_b], file_type=parquet"];
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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[true]",
        "    FilterExec: NOT non_nullable_col@1",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], file_type=parquet"];
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
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false"];
    let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=false"];
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
        "        DataSourceExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=true",
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
        "        DataSourceExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=true",
    ];
    let expected_optimized_bounded_parallelize_sort = vec![
        "SortPreservingMergeExec: [a@0 ASC]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=Hash([c@2], 10), input_partitions=10",
        "      RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "        DataSourceExec: file_groups={1 group: [[file_path]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC], file_type=csv, has_header=true",
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",];
    let expected_optimized = ["SortExec: expr=[b@1 ASC], preserve_partitioning=[false]",
        "  SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",];
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
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",];
    let expected_optimized = ["SortPreservingMergeExec: [a@0 ASC, b@1 ASC]",
        "  SortExec: expr=[a@0 ASC, b@1 ASC, c@2 ASC], preserve_partitioning=[true]",
        "    RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "      DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], output_ordering=[a@0 ASC, b@1 ASC], file_type=csv, has_header=false",];
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
        "          DataSourceExec: file_groups={1 group: [[x]]}, projection=[a, b, c, d, e], file_type=csv, has_header=false",
    ];
    let expected_optimized = [
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
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
    let input = sort_exec(vec![sort_expr("non_nullable_col", &schema)], source);
    let physical_plan = sort_exec(vec![sort_expr("nullable_col", &schema)], input);

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
        "            DataSourceExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
        "  FilterExec: NOT non_nullable_col@1",
        "    BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "      CoalesceBatchesExec: target_batch_size=128",
        "        SortExec: expr=[non_nullable_col@1 DESC], preserve_partitioning=[false]",
        "          DataSourceExec: partitions=1, partition_sizes=[0]"];
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
        "            DataSourceExec: partitions=1, partition_sizes=[0]",
        "          RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "            DataSourceExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["SortPreservingMergeExec: [nullable_col@0 ASC, non_nullable_col@1 ASC]",
        "  SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[true]",
        "    FilterExec: NOT non_nullable_col@1",
        "      UnionExec",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: partitions=1, partition_sizes=[0]",
        "        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "          DataSourceExec: partitions=1, partition_sizes=[0]"];
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
        vec![sort_expr("non_nullable_col", &schema)],
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
    let sort_exprs = vec![
        sort_expr("nullable_col", &schema),
        sort_expr("non_nullable_col", &schema),
    ];
    let sort = sort_exec(vec![sort_exprs[0].clone()], source);
    let physical_plan = sort_preserving_merge_exec(sort_exprs, sort);
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
        "        DataSourceExec: partitions=1, partition_sizes=[0]"];

    let expected_optimized = ["BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "    SortExec: expr=[nullable_col@0 ASC, non_nullable_col@1 ASC], preserve_partitioning=[false]",
        "      BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "        SortExec: expr=[nullable_col@0 ASC], preserve_partitioning=[false]",
        "          DataSourceExec: partitions=1, partition_sizes=[0]"];
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
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
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
        plan = rule.optimize(plan, &config)?;
    }
    let first_plan = plan.clone();

    let mut plan = orig_plan.clone();
    let rules = vec![
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
        Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
    ];
    for rule in rules {
        plan = rule.optimize(plan, &config)?;
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
async fn test_push_with_required_input_ordering_prohibited() -> Result<()> {
    // SortExec: expr=[b]            <-- can't push this down
    //  RequiredInputOrder expr=[a]  <-- this requires input sorted by a, and preserves the input order
    //    SortExec: expr=[a]
    //      DataSourceExec
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
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
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
    //      DataSourceExec
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
        "      DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    // should able to push shorts
    let expected = [
        "RequiredInputOrderingExec",
        "  SortExec: expr=[a@0 ASC, b@1 ASC], preserve_partitioning=[false]",
        "    DataSourceExec: partitions=1, partition_sizes=[0]",
    ];
    assert_optimized!(expected_input, expected, plan, true);
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
async fn test_window_partial_constant_and_set_monotonicity() -> Result<()> {
    let input_schema = create_test_schema()?;
    let sort_exprs = vec![sort_expr_options(
        "nullable_col",
        &input_schema,
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )];
    let source = parquet_exec_sorted(&input_schema, sort_exprs);

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
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "WindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  WindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: CurrentRow, end_bound: Following(NULL), is_causal: false }]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet"
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[count@2 ASC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: Following(UInt32(1)), is_causal: false }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[min@2 DESC NULLS LAST, nullable_col@0 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[avg@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, count@2 ASC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, max@2 ASC], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[max: Ok(Field { name: \"max\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "SortExec: expr=[nullable_col@0 ASC NULLS LAST, min@2 DESC NULLS LAST], preserve_partitioning=[false]",
                "  BoundedWindowAggExec: wdw=[min: Ok(Field { name: \"min\", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
                "  BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
                "    DataSourceExec: file_groups={1 group: [[x]]}, projection=[nullable_col, non_nullable_col], output_ordering=[nullable_col@0 ASC NULLS LAST], file_type=parquet",
            ],
            expected_plan: vec![
                "BoundedWindowAggExec: wdw=[avg: Ok(Field { name: \"avg\", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt32(1)), end_bound: CurrentRow, is_causal: true }], mode=[Sorted]",
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
            &LexOrdering::default(),
            case.window_frame,
            input_schema.as_ref(),
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
        let physical_plan = sort_exec(sort_expr, window_exec);

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
    let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs.clone());

    let orthogonal_sort = sort_exec(vec![sort_expr("a", &schema)], unbounded_input);
    let output_sort = sort_exec(input_sort_exprs, orthogonal_sort); // same sort as data source

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input,);

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
    let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs.clone());

    let orthogonal_sort =
        sort_exec_with_fetch(vec![sort_expr("a", &schema)], Some(3), unbounded_input); // has fetch, so this orthogonal sort changes the output
    let output_sort = sort_exec(input_sort_exprs, orthogonal_sort);

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]"
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input,);

    // Test: should keep the orthogonal sort, since it modifies the output:
    let expected_optimized = expected_input;
    assert_optimized!(expected_input, expected_optimized, output_sort, true);

    Ok(())
}

#[test]
fn test_handles_multiple_orthogonal_sorts() -> Result<()> {
    let schema = create_test_schema3()?;
    let input_sort_exprs = vec![sort_expr("b", &schema), sort_expr("c", &schema)];
    let unbounded_input = stream_exec_ordered(&schema, input_sort_exprs.clone());

    let orthogonal_sort_0 = sort_exec(vec![sort_expr("c", &schema)], unbounded_input); // has no fetch, so can be removed
    let orthogonal_sort_1 =
        sort_exec_with_fetch(vec![sort_expr("a", &schema)], Some(3), orthogonal_sort_0); // has fetch, so this orthogonal sort changes the output
    let orthogonal_sort_2 = sort_exec(vec![sort_expr("c", &schema)], orthogonal_sort_1); // has no fetch, so can be removed
    let orthogonal_sort_3 = sort_exec(vec![sort_expr("a", &schema)], orthogonal_sort_2); // has no fetch, so can be removed
    let output_sort = sort_exec(input_sort_exprs, orthogonal_sort_3); // final sort

    // Test scenario/input has an orthogonal sort:
    let expected_input = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
        "    SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "      SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "        SortExec: expr=[c@2 ASC], preserve_partitioning=[false]",
        "          StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]",
    ];
    assert_eq!(get_plan_string(&output_sort), expected_input,);

    // Test: should keep only the needed orthogonal sort, and remove the unneeded ones:
    let expected_optimized = [
        "SortExec: expr=[b@1 ASC, c@2 ASC], preserve_partitioning=[false]",
        "  SortExec: TopK(fetch=3), expr=[a@0 ASC], preserve_partitioning=[false]",
        "    StreamingTableExec: partition_sizes=1, projection=[a, b, c, d, e], infinite_source=true, output_ordering=[b@1 ASC, c@2 ASC]",
    ];
    assert_optimized!(expected_input, expected_optimized, output_sort, true);

    Ok(())
}
