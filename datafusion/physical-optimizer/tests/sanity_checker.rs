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

//! Tests for [`SanityCheckPlan`] physical optimizer rule

use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_expr::JoinType;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::Partitioning;
use datafusion_physical_optimizer::test_utils::{
    bounded_window_exec, global_limit_exec, local_limit_exec, memory_exec,
    repartition_exec, sort_exec, sort_expr_options, sort_merge_join_exec,
};
use datafusion_physical_optimizer::{sanity_checker::*, PhysicalOptimizerRule};
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::ExecutionPlan;

fn create_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("c9", DataType::Int32, true)]))
}

fn create_test_schema2() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]))
}

/// Check if sanity checker should accept or reject plans.
fn assert_sanity_check(plan: &Arc<dyn ExecutionPlan>, is_sane: bool) {
    let sanity_checker = SanityCheckPlan::new();
    let opts = ConfigOptions::default();
    assert_eq!(
        sanity_checker.optimize(plan.clone(), &opts).is_ok(),
        is_sane
    );
}

/// Check if the plan we created is as expected by comparing the plan
/// formatted as a string.
fn assert_plan(plan: &dyn ExecutionPlan, expected_lines: Vec<&str>) {
    let plan_str = displayable(plan).indent(true).to_string();
    let actual_lines: Vec<&str> = plan_str.trim().lines().collect();
    assert_eq!(actual_lines, expected_lines);
}

#[tokio::test]
/// Tests that plan is valid when the sort requirements are satisfied.
async fn test_bounded_window_agg_sort_requirement() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr_options(
        "c9",
        &source.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )];
    let sort = sort_exec(sort_exprs.clone(), source);
    let bw = bounded_window_exec("c9", sort_exprs, sort);
    assert_plan(bw.as_ref(), vec![
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  SortExec: expr=[c9@0 ASC NULLS LAST], preserve_partitioning=[false]",
        "    MemoryExec: partitions=1, partition_sizes=[0]"
    ]);
    assert_sanity_check(&bw, true);
    Ok(())
}

#[tokio::test]
/// Tests that plan is invalid when the sort requirements are not satisfied.
async fn test_bounded_window_agg_no_sort_requirement() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let sort_exprs = vec![sort_expr_options(
        "c9",
        &source.schema(),
        SortOptions {
            descending: false,
            nulls_first: false,
        },
    )];
    let bw = bounded_window_exec("c9", sort_exprs, source);
    assert_plan(bw.as_ref(), vec![
        "BoundedWindowAggExec: wdw=[count: Ok(Field { name: \"count\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Range, start_bound: Preceding(NULL), end_bound: CurrentRow, is_causal: false }], mode=[Sorted]",
        "  MemoryExec: partitions=1, partition_sizes=[0]"
    ]);
    // Order requirement of the `BoundedWindowAggExec` is not satisfied. We expect to receive error during sanity check.
    assert_sanity_check(&bw, false);
    Ok(())
}

#[tokio::test]
/// A valid when a single partition requirement
/// is satisfied.
async fn test_global_limit_single_partition() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = global_limit_exec(source);

    assert_plan(
        limit.as_ref(),
        vec![
            "GlobalLimitExec: skip=0, fetch=100",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    assert_sanity_check(&limit, true);
    Ok(())
}

#[tokio::test]
/// An invalid plan when a single partition requirement
/// is not satisfied.
async fn test_global_limit_multi_partition() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = global_limit_exec(repartition_exec(source));

    assert_plan(
        limit.as_ref(),
        vec![
            "GlobalLimitExec: skip=0, fetch=100",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    // Distribution requirement of the `GlobalLimitExec` is not satisfied. We expect to receive error during sanity check.
    assert_sanity_check(&limit, false);
    Ok(())
}

#[tokio::test]
/// A plan with no requirements should satisfy.
async fn test_local_limit() -> Result<()> {
    let schema = create_test_schema();
    let source = memory_exec(&schema);
    let limit = local_limit_exec(source);

    assert_plan(
        limit.as_ref(),
        vec![
            "LocalLimitExec: fetch=100",
            "  MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    assert_sanity_check(&limit, true);
    Ok(())
}

#[tokio::test]
/// Valid plan with multiple children satisfy both order and distribution.
async fn test_sort_merge_join_satisfied() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let source2 = memory_exec(&schema2);
    let sort_opts = SortOptions::default();
    let sort_exprs1 = vec![sort_expr_options("c9", &source1.schema(), sort_opts)];
    let sort_exprs2 = vec![sort_expr_options("a", &source2.schema(), sort_opts)];
    let left = sort_exec(sort_exprs1, source1);
    let right = sort_exec(sort_exprs2, source2);
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(vec![right_jcol.clone()], 10),
    )?);

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1",
            "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    assert_sanity_check(&smj, true);
    Ok(())
}

#[tokio::test]
/// Invalid case when the order is not satisfied by the 2nd
/// child.
async fn test_sort_merge_join_order_missing() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let right = memory_exec(&schema2);
    let sort_exprs1 = vec![sort_expr_options(
        "c9",
        &source1.schema(),
        SortOptions::default(),
    )];
    let left = sort_exec(sort_exprs1, source1);
    // Missing sort of the right child here..
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(vec![right_jcol.clone()], 10),
    )?);

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=Hash([a@0], 10), input_partitions=1",
            "    MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    // Order requirement for the `SortMergeJoin` is not satisfied for right child. We expect to receive error during sanity check.
    assert_sanity_check(&smj, false);
    Ok(())
}

#[tokio::test]
/// Invalid case when the distribution is not satisfied by the 2nd
/// child.
async fn test_sort_merge_join_dist_missing() -> Result<()> {
    let schema1 = create_test_schema();
    let schema2 = create_test_schema2();
    let source1 = memory_exec(&schema1);
    let source2 = memory_exec(&schema2);
    let sort_opts = SortOptions::default();
    let sort_exprs1 = vec![sort_expr_options("c9", &source1.schema(), sort_opts)];
    let sort_exprs2 = vec![sort_expr_options("a", &source2.schema(), sort_opts)];
    let left = sort_exec(sort_exprs1, source1);
    let right = sort_exec(sort_exprs2, source2);
    let right = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::RoundRobinBatch(10),
    )?);
    let left_jcol = col("c9", &left.schema()).unwrap();
    let right_jcol = col("a", &right.schema()).unwrap();
    let left = Arc::new(RepartitionExec::try_new(
        left,
        Partitioning::Hash(vec![left_jcol.clone()], 10),
    )?);

    // Missing hash partitioning on right child.

    let join_on = vec![(left_jcol as _, right_jcol as _)];
    let join_ty = JoinType::Inner;
    let smj = sort_merge_join_exec(left, right, &join_on, &join_ty);

    assert_plan(
        smj.as_ref(),
        vec![
            "SortMergeJoin: join_type=Inner, on=[(c9@0, a@0)]",
            "  RepartitionExec: partitioning=Hash([c9@0], 10), input_partitions=1",
            "    SortExec: expr=[c9@0 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
            "  RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
            "    SortExec: expr=[a@0 ASC], preserve_partitioning=[false]",
            "      MemoryExec: partitions=1, partition_sizes=[0]",
        ],
    );
    // Distribution requirement for the `SortMergeJoin` is not satisfied for right child (has round-robin partitioning). We expect to receive error during sanity check.
    assert_sanity_check(&smj, false);
    Ok(())
}
