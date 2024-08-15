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

//! Tests for the physical optimizer

use datafusion_common::config::ConfigOptions;
use datafusion_physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::logical_expr::Operator;
use datafusion::prelude::SessionContext;
use datafusion::test_util::TestAggregate;
use datafusion_physical_plan::aggregates::PhysicalGroupBy;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::common;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::memory::MemoryExec;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_int64_array;
use datafusion_physical_expr::expressions::{self, cast};
use datafusion_physical_plan::aggregates::AggregateMode;

/// Mock data using a MemoryExec which has an exact count statistic
fn mock_data() -> Result<Arc<MemoryExec>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
            Arc::new(Int32Array::from(vec![Some(4), None, Some(6)])),
        ],
    )?;

    Ok(Arc::new(MemoryExec::try_new(
        &[vec![batch]],
        Arc::clone(&schema),
        None,
    )?))
}

/// Checks that the count optimization was applied and we still get the right result
async fn assert_count_optim_success(
    plan: AggregateExec,
    agg: TestAggregate,
) -> Result<()> {
    let session_ctx = SessionContext::new();
    let state = session_ctx.state();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(plan);

    let optimized =
        AggregateStatistics::new().optimize(Arc::clone(&plan), state.config_options())?;

    // A ProjectionExec is a sign that the count optimization was applied
    assert!(optimized.as_any().is::<ProjectionExec>());

    // run both the optimized and nonoptimized plan
    let optimized_result =
        common::collect(optimized.execute(0, session_ctx.task_ctx())?).await?;
    let nonoptimized_result =
        common::collect(plan.execute(0, session_ctx.task_ctx())?).await?;
    assert_eq!(optimized_result.len(), nonoptimized_result.len());

    //  and validate the results are the same and expected
    assert_eq!(optimized_result.len(), 1);
    check_batch(optimized_result.into_iter().next().unwrap(), &agg);
    // check the non optimized one too to ensure types and names remain the same
    assert_eq!(nonoptimized_result.len(), 1);
    check_batch(nonoptimized_result.into_iter().next().unwrap(), &agg);

    Ok(())
}

fn check_batch(batch: RecordBatch, agg: &TestAggregate) {
    let schema = batch.schema();
    let fields = schema.fields();
    assert_eq!(fields.len(), 1);

    let field = &fields[0];
    assert_eq!(field.name(), agg.column_name());
    assert_eq!(field.data_type(), &DataType::Int64);
    // note that nullabiolity differs

    assert_eq!(
        as_int64_array(batch.column(0)).unwrap().values(),
        &[agg.expected_count()]
    );
}

#[tokio::test]
async fn test_count_partial_direct_child() -> Result<()> {
    // basic test case with the aggregation applied on a source with exact statistics
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_star();

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(partial_agg),
        Arc::clone(&schema),
    )?;

    assert_count_optim_success(final_agg, agg).await?;

    Ok(())
}

#[tokio::test]
async fn test_count_partial_with_nulls_direct_child() -> Result<()> {
    // basic test case with the aggregation applied on a source with exact statistics
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_column(&schema);

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(partial_agg),
        Arc::clone(&schema),
    )?;

    assert_count_optim_success(final_agg, agg).await?;

    Ok(())
}

#[tokio::test]
async fn test_count_partial_indirect_child() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_star();

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    // We introduce an intermediate optimization step between the partial and final aggregtator
    let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(coalesce),
        Arc::clone(&schema),
    )?;

    assert_count_optim_success(final_agg, agg).await?;

    Ok(())
}

#[tokio::test]
async fn test_count_partial_with_nulls_indirect_child() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_column(&schema);

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    // We introduce an intermediate optimization step between the partial and final aggregtator
    let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(coalesce),
        Arc::clone(&schema),
    )?;

    assert_count_optim_success(final_agg, agg).await?;

    Ok(())
}

#[tokio::test]
async fn test_count_inexact_stat() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_star();

    // adding a filter makes the statistics inexact
    let filter = Arc::new(FilterExec::try_new(
        expressions::binary(
            expressions::col("a", &schema)?,
            Operator::Gt,
            cast(expressions::lit(1u32), &schema, DataType::Int32)?,
            &schema,
        )?,
        source,
    )?);

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        filter,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(partial_agg),
        Arc::clone(&schema),
    )?;

    let conf = ConfigOptions::new();
    let optimized = AggregateStatistics::new().optimize(Arc::new(final_agg), &conf)?;

    // check that the original ExecutionPlan was not replaced
    assert!(optimized.as_any().is::<AggregateExec>());

    Ok(())
}

#[tokio::test]
async fn test_count_with_nulls_inexact_stat() -> Result<()> {
    let source = mock_data()?;
    let schema = source.schema();
    let agg = TestAggregate::new_count_column(&schema);

    // adding a filter makes the statistics inexact
    let filter = Arc::new(FilterExec::try_new(
        expressions::binary(
            expressions::col("a", &schema)?,
            Operator::Gt,
            cast(expressions::lit(1u32), &schema, DataType::Int32)?,
            &schema,
        )?,
        source,
    )?);

    let partial_agg = AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        filter,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![agg.count_expr(&schema)],
        vec![None],
        Arc::new(partial_agg),
        Arc::clone(&schema),
    )?;

    let conf = ConfigOptions::new();
    let optimized = AggregateStatistics::new().optimize(Arc::new(final_agg), &conf)?;

    // check that the original ExecutionPlan was not replaced
    assert!(optimized.as_any().is::<AggregateExec>());

    Ok(())
}
