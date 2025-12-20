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

use crate::physical_optimizer::test_utils::TestAggregate;

use arrow::array::Int32Array;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::assert_batches_eq;
use datafusion_common::cast::as_int64_array;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{self, cast};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::aggregates::AggregateMode;
use datafusion_physical_plan::aggregates::PhysicalGroupBy;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::common;
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;

/// Mock data using a MemorySourceConfig which has an exact count statistic
fn mock_data() -> Result<Arc<DataSourceExec>> {
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

    MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None)
}

/// Checks that the count optimization was applied and we still get the right result
async fn assert_count_optim_success(
    plan: AggregateExec,
    agg: TestAggregate,
) -> Result<()> {
    let task_ctx = Arc::new(TaskContext::default());
    let plan: Arc<dyn ExecutionPlan> = Arc::new(plan);

    let config = ConfigOptions::new();
    let optimized = AggregateStatistics::new().optimize(Arc::clone(&plan), &config)?;

    // A ProjectionExec is a sign that the count optimization was applied
    assert!(optimized.as_any().is::<ProjectionExec>());

    // run both the optimized and nonoptimized plan
    let optimized_result =
        common::collect(optimized.execute(0, Arc::clone(&task_ctx))?).await?;
    let nonoptimized_result = common::collect(plan.execute(0, task_ctx)?).await?;
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
    // note that nullability differs

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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    // We introduce an intermediate optimization step between the partial and final aggregator
    let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        source,
        Arc::clone(&schema),
    )?;

    // We introduce an intermediate optimization step between the partial and final aggregator
    let coalesce = CoalescePartitionsExec::new(Arc::new(partial_agg));

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        filter,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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
        vec![Arc::new(agg.count_expr(&schema))],
        vec![None],
        filter,
        Arc::clone(&schema),
    )?;

    let final_agg = AggregateExec::try_new(
        AggregateMode::Final,
        PhysicalGroupBy::default(),
        vec![Arc::new(agg.count_expr(&schema))],
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

/// Tests that TopK aggregation correctly handles UTF-8 (string) types in both grouping keys and aggregate values.
///
/// The TopK optimization is designed to efficiently handle `GROUP BY ... ORDER BY aggregate LIMIT n` queries
/// by maintaining only the top K groups during aggregation. However, not all type combinations are supported.
///
/// This test verifies two scenarios:
/// 1. **Supported case**: UTF-8 grouping key with numeric aggregate (max/min) - should use TopK optimization
/// 2. **Unsupported case**: UTF-8 grouping key with UTF-8 aggregate value - must gracefully fall back to
///    standard aggregation without panicking
///
/// The fallback behavior is critical because attempting to use TopK with unsupported types could cause
/// runtime panics. This test ensures the optimizer correctly detects incompatible types and chooses
/// the appropriate execution path.
#[tokio::test]
async fn utf8_grouping_min_max_limit_fallbacks() -> Result<()> {
    let mut config = SessionConfig::new();
    config.options_mut().optimizer.enable_topk_aggregation = true;
    let ctx = SessionContext::new_with_config(config);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("val_str", DataType::Utf8, false),
            Field::new("val_num", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a"])),
            Arc::new(StringArray::from(vec!["alpha", "bravo", "charlie"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )?;
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    // Supported path: numeric min/max with UTF-8 grouping should still use TopK aggregation
    // and return correct results.
    let supported_df = ctx
        .sql("SELECT g, max(val_num) AS m FROM t GROUP BY g ORDER BY m DESC LIMIT 1")
        .await?;
    let supported_batches = supported_df.collect().await?;
    assert_batches_eq!(
        &[
            "+---+---+",
            "| g | m |",
            "+---+---+",
            "| a | 3 |",
            "+---+---+"
        ],
        &supported_batches
    );

    // Unsupported TopK value type: string min/max should fall back without panicking.
    let unsupported_df = ctx
        .sql("SELECT g, max(val_str) AS s FROM t GROUP BY g ORDER BY s DESC LIMIT 1")
        .await?;
    let unsupported_plan = unsupported_df.clone().create_physical_plan().await?;
    let unsupported_batches = unsupported_df.collect().await?;

    // Ensure the plan avoided the TopK-specific stream implementation.
    let plan_display = displayable(unsupported_plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        !plan_display.contains("GroupedTopKAggregateStream"),
        "Unsupported UTF-8 aggregate value should not use TopK: {plan_display}"
    );

    assert_batches_eq!(
        &[
            "+---+---------+",
            "| g | s       |",
            "+---+---------+",
            "| a | charlie |",
            "+---+---------+"
        ],
        &unsupported_batches
    );

    Ok(())
}
