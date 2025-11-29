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
    coalesce_batches_exec, coalesce_partitions_exec, global_limit_exec, local_limit_exec,
    sort_exec, sort_preserving_merge_exec, stream_exec,
};

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::error::Result;
use datafusion_execution::config::SessionConfig;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{col, lit, BinaryExpr};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion_physical_optimizer::OptimizerContext;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{get_plan_string, ExecutionPlan};

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

#[test]
fn transforms_streaming_table_exec_into_fetching_version_when_skip_is_zero() -> Result<()>
{
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 0, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=5"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn transforms_streaming_table_exec_into_fetching_version_and_keeps_the_global_limit_when_skip_is_nonzero(
) -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let global_limit = global_limit_exec(streaming_table, 2, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=2, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "GlobalLimitExec: skip=2, fetch=5",
            "  StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true, fetch=7"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn transforms_coalesce_batches_exec_into_fetching_version_and_removes_local_limit(
) -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let repartition = repartition_exec(streaming_table)?;
    let filter = filter_exec(schema, repartition)?;
    let coalesce_batches = coalesce_batches_exec(filter, 8192);
    let local_limit = local_limit_exec(coalesce_batches, 5);
    let coalesce_partitions = coalesce_partitions_exec(local_limit);
    let global_limit = global_limit_exec(coalesce_partitions, 0, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    LocalLimitExec: fetch=5",
            "      CoalesceBatchesExec: target_batch_size=8192",
            "        FilterExec: c3@2 > 0",
            "          RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
        "CoalescePartitionsExec: fetch=5",
        "  CoalesceBatchesExec: target_batch_size=8192, fetch=5",
        "    FilterExec: c3@2 > 0",
        "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        "        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn pushes_global_limit_exec_through_projection_exec() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let filter = filter_exec(Arc::clone(&schema), streaming_table)?;
    let projection = projection_exec(schema, filter)?;
    let global_limit = global_limit_exec(projection, 0, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "    FilterExec: c3@2 > 0",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "  FilterExec: c3@2 > 0, fetch=5",
            "    StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn pushes_global_limit_exec_through_projection_exec_and_transforms_coalesce_batches_exec_into_fetching_version(
) -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let coalesce_batches = coalesce_batches_exec(streaming_table, 8192);
    let projection = projection_exec(schema, coalesce_batches)?;
    let global_limit = global_limit_exec(projection, 0, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "    CoalesceBatchesExec: target_batch_size=8192",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "  CoalesceBatchesExec: target_batch_size=8192, fetch=5",
            "    StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn pushes_global_limit_into_multiple_fetch_plans() -> Result<()> {
    let schema = create_schema();
    let streaming_table = stream_exec(&schema);
    let coalesce_batches = coalesce_batches_exec(streaming_table, 8192);
    let projection = projection_exec(Arc::clone(&schema), coalesce_batches)?;
    let repartition = repartition_exec(projection)?;
    let ordering: LexOrdering = [PhysicalSortExpr {
        expr: col("c1", &schema)?,
        options: SortOptions::default(),
    }]
    .into();
    let sort = sort_exec(ordering.clone(), repartition);
    let spm = sort_preserving_merge_exec(ordering, sort);
    let global_limit = global_limit_exec(spm, 0, Some(5));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  SortPreservingMergeExec: [c1@0 ASC]",
            "    SortExec: expr=[c1@0 ASC], preserve_partitioning=[false]",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "          CoalesceBatchesExec: target_batch_size=8192",
            "            StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "SortPreservingMergeExec: [c1@0 ASC], fetch=5",
            "  SortExec: TopK(fetch=5), expr=[c1@0 ASC], preserve_partitioning=[false]",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      ProjectionExec: expr=[c1@0 as c1, c2@1 as c2, c3@2 as c3]",
            "        CoalesceBatchesExec: target_batch_size=8192",
            "          StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

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

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
            "GlobalLimitExec: skip=0, fetch=5",
            "  CoalescePartitionsExec",
            "    FilterExec: c3@2 > 0",
            "      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "        StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = [
            "CoalescePartitionsExec: fetch=5",
            "  FilterExec: c3@2 > 0, fetch=5",
            "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            "      StreamingTableExec: partition_sizes=1, projection=[c1, c2, c3], infinite_source=true"
        ];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn merges_local_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_local_limit = local_limit_exec(empty_exec, 10);
    let parent_local_limit = local_limit_exec(child_local_limit, 20);

    let initial = get_plan_string(&parent_local_limit);
    let expected_initial = [
        "LocalLimitExec: fetch=20",
        "  LocalLimitExec: fetch=10",
        "    EmptyExec",
    ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(parent_local_limit, &optimizer_context)?;

    let expected = ["GlobalLimitExec: skip=0, fetch=10", "  EmptyExec"];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn merges_global_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let child_global_limit = global_limit_exec(empty_exec, 10, Some(30));
    let parent_global_limit = global_limit_exec(child_global_limit, 10, Some(20));

    let initial = get_plan_string(&parent_global_limit);
    let expected_initial = [
        "GlobalLimitExec: skip=10, fetch=20",
        "  GlobalLimitExec: skip=10, fetch=30",
        "    EmptyExec",
    ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(parent_global_limit, &optimizer_context)?;

    let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn merges_global_limit_with_local_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let local_limit = local_limit_exec(empty_exec, 40);
    let global_limit = global_limit_exec(local_limit, 20, Some(30));

    let initial = get_plan_string(&global_limit);
    let expected_initial = [
        "GlobalLimitExec: skip=20, fetch=30",
        "  LocalLimitExec: fetch=40",
        "    EmptyExec",
    ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(global_limit, &optimizer_context)?;

    let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}

#[test]
fn merges_local_limit_with_global_limit() -> Result<()> {
    let schema = create_schema();
    let empty_exec = empty_exec(schema);
    let global_limit = global_limit_exec(empty_exec, 20, Some(30));
    let local_limit = local_limit_exec(global_limit, 20);

    let initial = get_plan_string(&local_limit);
    let expected_initial = [
        "LocalLimitExec: fetch=20",
        "  GlobalLimitExec: skip=20, fetch=30",
        "    EmptyExec",
    ];

    assert_eq!(initial, expected_initial);

    let session_config = SessionConfig::new();
    let optimizer_context = OptimizerContext::new(session_config.clone());
    let after_optimize =
        LimitPushdown::new().optimize_plan(local_limit, &optimizer_context)?;

    let expected = ["GlobalLimitExec: skip=20, fetch=20", "  EmptyExec"];
    assert_eq!(get_plan_string(&after_optimize), expected);

    Ok(())
}
