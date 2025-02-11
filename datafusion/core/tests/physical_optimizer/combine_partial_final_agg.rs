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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion_common::config::ConfigOptions;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_functions_aggregate::count::count_udaf;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::{col, lit};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::ExecutionPlan;

/// Runs the CombinePartialFinalAggregate optimizer and asserts the plan against the expected
macro_rules! assert_optimized {
    ($EXPECTED_LINES: expr, $PLAN: expr) => {
        let expected_lines: Vec<&str> = $EXPECTED_LINES.iter().map(|s| *s).collect();

        // run optimizer
        let optimizer = CombinePartialFinalAggregate {};
        let config = ConfigOptions::new();
        let optimized = optimizer.optimize($PLAN, &config)?;
        // Now format correctly
        let plan = displayable(optimized.as_ref()).indent(true).to_string();
        let actual_lines = trim_plan_display(&plan);

        assert_eq!(
            &expected_lines, &actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

fn trim_plan_display(plan: &str) -> Vec<&str> {
    plan.split('\n')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]))
}

fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("x".to_string(), 100)),
    )
    .build_arc()
}

fn partial_aggregate_exec(
    input: Arc<dyn ExecutionPlan>,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let n_aggr = aggr_expr.len();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            group_by,
            aggr_expr,
            vec![None; n_aggr],
            input,
            schema,
        )
        .unwrap(),
    )
}

fn final_aggregate_exec(
    input: Arc<dyn ExecutionPlan>,
    group_by: PhysicalGroupBy,
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let n_aggr = aggr_expr.len();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggr_expr,
            vec![None; n_aggr],
            input,
            schema,
        )
        .unwrap(),
    )
}

fn repartition_exec(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(10)).unwrap())
}

// Return appropriate expr depending if COUNT is for col or table (*)
fn count_expr(
    expr: Arc<dyn PhysicalExpr>,
    name: &str,
    schema: &Schema,
) -> Arc<AggregateFunctionExpr> {
    AggregateExprBuilder::new(count_udaf(), vec![expr])
        .schema(Arc::new(schema.clone()))
        .alias(name)
        .build()
        .map(Arc::new)
        .unwrap()
}

#[test]
fn aggregations_not_combined() -> datafusion_common::Result<()> {
    let schema = schema();

    let aggr_expr = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];

    let plan = final_aggregate_exec(
        repartition_exec(partial_aggregate_exec(
            parquet_exec(&schema),
            PhysicalGroupBy::default(),
            aggr_expr.clone(),
        )),
        PhysicalGroupBy::default(),
        aggr_expr,
    );
    // should not combine the Partial/Final AggregateExecs
    let expected = &[
        "AggregateExec: mode=Final, gby=[], aggr=[COUNT(1)]",
        "RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1",
        "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
    ];
    assert_optimized!(expected, plan);

    let aggr_expr1 = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];
    let aggr_expr2 = vec![count_expr(lit(1i8), "COUNT(2)", &schema)];

    let plan = final_aggregate_exec(
        partial_aggregate_exec(
            parquet_exec(&schema),
            PhysicalGroupBy::default(),
            aggr_expr1,
        ),
        PhysicalGroupBy::default(),
        aggr_expr2,
    );
    // should not combine the Partial/Final AggregateExecs
    let expected = &[
        "AggregateExec: mode=Final, gby=[], aggr=[COUNT(2)]",
        "AggregateExec: mode=Partial, gby=[], aggr=[COUNT(1)]",
        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
    ];

    assert_optimized!(expected, plan);

    Ok(())
}

#[test]
fn aggregations_combined() -> datafusion_common::Result<()> {
    let schema = schema();
    let aggr_expr = vec![count_expr(lit(1i8), "COUNT(1)", &schema)];

    let plan = final_aggregate_exec(
        partial_aggregate_exec(
            parquet_exec(&schema),
            PhysicalGroupBy::default(),
            aggr_expr.clone(),
        ),
        PhysicalGroupBy::default(),
        aggr_expr,
    );
    // should combine the Partial/Final AggregateExecs to the Single AggregateExec
    let expected = &[
        "AggregateExec: mode=Single, gby=[], aggr=[COUNT(1)]",
        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
    ];

    assert_optimized!(expected, plan);
    Ok(())
}

#[test]
fn aggregations_with_group_combined() -> datafusion_common::Result<()> {
    let schema = schema();
    let aggr_expr = vec![
        AggregateExprBuilder::new(sum_udaf(), vec![col("b", &schema)?])
            .schema(Arc::clone(&schema))
            .alias("Sum(b)")
            .build()
            .map(Arc::new)
            .unwrap(),
    ];
    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("c", &schema)?, "c".to_string())];

    let partial_group_by = PhysicalGroupBy::new_single(groups);
    let partial_agg = partial_aggregate_exec(
        parquet_exec(&schema),
        partial_group_by,
        aggr_expr.clone(),
    );

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("c", &partial_agg.schema())?, "c".to_string())];
    let final_group_by = PhysicalGroupBy::new_single(groups);

    let plan = final_aggregate_exec(partial_agg, final_group_by, aggr_expr);
    // should combine the Partial/Final AggregateExecs to the Single AggregateExec
    let expected = &[
        "AggregateExec: mode=Single, gby=[c@2 as c], aggr=[Sum(b)]",
        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
    ];

    assert_optimized!(expected, plan);
    Ok(())
}

#[test]
fn aggregations_with_limit_combined() -> datafusion_common::Result<()> {
    let schema = schema();
    let aggr_expr = vec![];

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("c", &schema)?, "c".to_string())];

    let partial_group_by = PhysicalGroupBy::new_single(groups);
    let partial_agg = partial_aggregate_exec(
        parquet_exec(&schema),
        partial_group_by,
        aggr_expr.clone(),
    );

    let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
        vec![(col("c", &partial_agg.schema())?, "c".to_string())];
    let final_group_by = PhysicalGroupBy::new_single(groups);

    let schema = partial_agg.schema();
    let final_agg = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Final,
            final_group_by,
            aggr_expr,
            vec![],
            partial_agg,
            schema,
        )
        .unwrap()
        .with_limit(Some(5)),
    );
    let plan: Arc<dyn ExecutionPlan> = final_agg;
    // should combine the Partial/Final AggregateExecs to a Single AggregateExec
    // with the final limit preserved
    let expected = &[
        "AggregateExec: mode=Single, gby=[c@2 as c], aggr=[], lim=[5]",
        "ParquetExec: file_groups={1 group: [[x]]}, projection=[a, b, c]",
    ];

    assert_optimized!(expected, plan);
    Ok(())
}
