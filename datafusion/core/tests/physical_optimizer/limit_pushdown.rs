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
    coalesce_partitions_exec, global_limit_exec, hash_join_exec, local_limit_exec,
    sort_exec, sort_preserving_merge_exec, stream_exec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{ExecutionPlan, get_plan_string};

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Int32, true),
    ]))
}

fn projection_exec(
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(ProjectionExec::try_new(
        vec![
            (col("c1", schema.as_ref()).unwrap(), "c1".to_string()),
            (col("c2", schema.as_ref()).unwrap(), "c2".to_string()),
            (col("c3", schema.as_ref()).unwrap(), "c3".to_string()),
        ],
        input,
    )?))
}

fn filter_exec(
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(FilterExec::try_new(
        Arc::new(BinaryExpr::new(
            col("c3", schema.as_ref()).unwrap(),
            Operator::Gt,
            lit(0),
        )),
        input,
    )?))
}

fn repartition_exec(
    streaming_table: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(RepartitionExec::try_new(
        streaming_table,
        Partitioning::RoundRobinBatch(8),
    )?))
}

fn empty_exec(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema))
}

fn nested_loop_join_exec(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(NestedLoopJoinExec::try_new(
        left, right, None, &join_type, None,
    )?))
}

fn format_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
    get_plan_string(plan).join("\n")
}

#[test]
fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_zero() -> Result<()>
{
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @"StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=5"
    );

    Ok(())
}

#[test]
fn transforms_streaming_table_exec_into_fetching_version_and_keeps_the_global_limit_when_skip_is_nonzero()
-> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 2, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=2, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=2, fetch=5
      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=7
    "
    );

    Ok(())
}

fn join_on_columns(
    left_col: &str,
    right_col: &str,
) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
    vec![(
        Arc::new(datafusion_physical_expr::expressions::Column::new(
            left_col, 0,
        )) as _,
        Arc::new(datafusion_physical_expr::expressions::Column::new(
            right_col, 0,
        )) as _,
    )]
}

#[test]
fn absorbs_limit_into_hash_join_inner() -> Result<()> {
    // HashJoinExec with Inner join should absorb limit via with_fetch
    let schema = create_schema();
    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Inner)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // The limit should be absorbed by the hash join (not pushed to children)
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)], fetch=5
      EmptyExec
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_into_hash_join_right() -> Result<()> {
    // HashJoinExec with Right join should absorb limit via with_fetch
    let schema = create_schema();
    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Right)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(10));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=10
      HashJoinExec: mode=Partitioned, join_type=Right, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // The limit should be absorbed by the hash join
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Right, on=[(c1@0, c1@0)], fetch=10
      EmptyExec
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_into_hash_join_left() -> Result<()> {
    // during probing, then unmatched rows at the end, stopping when limit is reached
    let schema = create_schema();
    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Left)?;
    let global_limit = global_limit_exec(hash_join, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Left, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // Left join now absorbs the limit
    insta::assert_snapshot!(
        optimized,
        @r"
    HashJoinExec: mode=Partitioned, join_type=Left, on=[(c1@0, c1@0)], fetch=5
      EmptyExec
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn absorbs_limit_with_skip_into_hash_join() -> Result<()> {
    let schema = create_schema();
    let left = empty_exec(Arc::clone(&schema));
    let right = empty_exec(Arc::clone(&schema));
    let on = join_on_columns("c1", "c1");
    let hash_join = hash_join_exec(left, right, on, None, &JoinType::Inner)?;
    let global_limit = global_limit_exec(hash_join, 3, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=3, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)]
        EmptyExec
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    // With skip, GlobalLimit is kept but fetch (skip + limit = 8) is absorbed by the join
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=3, fetch=5
      HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c1@0)], fetch=8
        EmptyExec
        EmptyExec
    "
    );

    Ok(())
}

#[test]
fn pushes_global_limit_exec_through_projection_exec() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let filter = filter_exec(Arc::clone(&schema), streaming_table)?;
    let projection = projection_exec(schema, filter)?;
    let global_limit = global_limit_exec(projection, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
        FilterExec: c3@2 > 0
          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
      FilterExec: c3@2 > 0, fetch=5
        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn pushes_global_limit_into_multiple_fetch_plans() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let projection = projection_exec(Arc::clone(&schema), streaming_table)?;
    let repartition = repartition_exec(projection)?;
    let ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let sort = sort_exec(ordering.clone(), repartition);
    let spm = sort_preserving_merge_exec(ordering, sort);
    let global_limit = global_limit_exec(spm, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      SortPreservingMergeExec: [c1@0 ASC]
        SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
              StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    SortPreservingMergeExec: [c1@0 ASC], fetch=5
      SortExec: TopK(fetch=5), expr=[c1@0 ASC], preserve_partitioning=[false]
        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
          ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]
            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn keeps_pushed_local_limit_exec_when_there_are_multiple_input_partitions() -> Result<()>
{
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let repartition = repartition_exec(streaming_table)?;
    let filter = filter_exec(schema, repartition)?;
    let coalesce_partitions = coalesce_partitions_exec(filter);
    let global_limit = global_limit_exec(coalesce_partitions, 0, Some(5));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=0, fetch=5
      CoalescePartitionsExec
        FilterExec: c3@2 > 0
          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    CoalescePartitionsExec: fetch=5
      FilterExec: c3@2 > 0, fetch=5
        RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1
          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true
    "
    );

    Ok(())
}

#[test]
fn merges_local_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_local_limit = local_limit_exec(empty_exec, 10);
    let parent_local_limit = local_limit_exec(child_local_limit, 20);

    let initial = format_plan(&parent_local_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    LocalLimitExec: fetch=20
      LocalLimitExec: fetch=10
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(parent_local_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=0, fetch=10
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn merges_global_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_global_limit = global_limit_exec(empty_exec, 10, Some(30));
    let parent_global_limit = global_limit_exec(child_global_limit, 10, Some(20));

    let initial = format_plan(&parent_global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=10, fetch=20
      GlobalLimitExec: skip=10, fetch=30
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(parent_global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn merges_global_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let local_limit = local_limit_exec(empty_exec, 40);
    let global_limit = global_limit_exec(local_limit, 20, Some(30));

    let initial = format_plan(&global_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=20, fetch=30
      LocalLimitExec: fetch=40
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(global_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn merges_local_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let global_limit = global_limit_exec(empty_exec, 20, Some(30));
    let local_limit = local_limit_exec(global_limit, 20);

    let initial = format_plan(&local_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    LocalLimitExec: fetch=20
      GlobalLimitExec: skip=20, fetch=30
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(local_limit, &ConfigOptions::new())?;

    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=20, fetch=20
      EmptyExec
    "
    );

    Ok(())
}

#[test]
fn preserves_nested_global_limit() -> Result<()> {
    // If there are multiple limits in an execution plan, they all need to be
    // preserved in the optimized plan.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=1
    //   NestedLoopJoinExec (Left)
    //     EmptyExec (left side)
    //     GlobalLimitExec: skip=2, fetch=1
    //       NestedLoopJoinExec (Right)
    //         EmptyExec (left side)
    //         EmptyExec (right side)
    let schema = create_schema();

    // Build inner join: NestedLoopJoin(Empty, Empty)
    let inner_left = empty_exec(Arc::clone(&schema));
    let inner_right = empty_exec(Arc::clone(&schema));
    let inner_join = nested_loop_join_exec(inner_left, inner_right, JoinType::Right)?;

    // Add inner limit: GlobalLimitExec: skip=2, fetch=1
    let inner_limit = global_limit_exec(inner_join, 2, Some(1));

    // Build outer join: NestedLoopJoin(Empty, GlobalLimit)
    let outer_left = empty_exec(Arc::clone(&schema));
    let outer_join = nested_loop_join_exec(outer_left, inner_limit, JoinType::Left)?;

    // Add outer limit: GlobalLimitExec: skip=1, fetch=1
    let outer_limit = global_limit_exec(outer_join, 1, Some(1));

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=1
      NestedLoopJoinExec: join_type=Left
        EmptyExec
        GlobalLimitExec: skip=2, fetch=1
          NestedLoopJoinExec: join_type=Right
            EmptyExec
            EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=1
      NestedLoopJoinExec: join_type=Left
        EmptyExec
        GlobalLimitExec: skip=2, fetch=1
          NestedLoopJoinExec: join_type=Right
            EmptyExec
            EmptyExec
    "
    );

    Ok(())
}

#[test]
fn preserves_skip_before_sort() -> Result<()> {
    // If there's a limit with skip before a node that (1) supports fetch but
    // (2) does not support limit pushdown, that limit should not be removed.
    //
    // Plan structure:
    // GlobalLimitExec: skip=1, fetch=None
    //   SortExec: TopK(fetch=4)
    //     EmptyExec
    let schema = create_schema();

    let empty = empty_exec(Arc::clone(&schema));

    let ordering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }];
    let sort = sort_exec(ordering.into(), empty)
        .with_fetch(Some(4))
        .unwrap();

    let outer_limit = global_limit_exec(sort, 1, None);

    let initial = format_plan(&outer_limit);
    insta::assert_snapshot!(
        initial,
        @r"
    GlobalLimitExec: skip=1, fetch=None
      SortExec: TopK(fetch=4), expr=[c1@0 ASC], preserve_partitioning=[false]
        EmptyExec
    "
    );

    let after_optimize =
        LimitPushdown::new().optimize(outer_limit, &ConfigOptions::new())?;
    let optimized = format_plan(&after_optimize);
    insta::assert_snapshot!(
        optimized,
        @r"
    GlobalLimitExec: skip=1, fetch=3
      SortExec: TopK(fetch=4), expr=[c1@0 ASC], preserve_partitioning=[false]
        EmptyExec
    "
    );

    Ok(())
}
