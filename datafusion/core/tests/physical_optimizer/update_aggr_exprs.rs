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

//! Tests for the [`OptimizeAggregateOrder`] rule.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_functions_aggregate::first_last::first_value_udaf;
use datafusion_physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::update_aggr_exprs::OptimizeAggregateOrder;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};

use crate::physical_optimizer::test_utils::parquet_exec_with_sort;

fn abc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
    ]))
}

/// An input whose declared ordering is `(a, b, c)`.
fn ordered_input(schema: &SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
    let ordering: LexOrdering = [
        PhysicalSortExpr::new_default(col("a", schema)?),
        PhysicalSortExpr::new_default(col("b", schema)?),
        PhysicalSortExpr::new_default(col("c", schema)?),
    ]
    .into();
    Ok(parquet_exec_with_sort(Arc::clone(schema), vec![ordering]) as _)
}

/// `FIRST_VALUE(d ORDER BY c)`.
fn first_value_expr(schema: &SchemaRef) -> Result<Arc<AggregateFunctionExpr>> {
    let order = PhysicalSortExpr::new_default(col("c", schema)?);
    AggregateExprBuilder::new(first_value_udaf(), vec![col("d", schema)?])
        .schema(Arc::clone(schema))
        .alias("first_value(d)")
        .order_by(vec![order])
        .build()
        .map(Arc::new)
}

fn optimized_flag(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
    let optimized =
        OptimizeAggregateOrder::new().optimize(plan, &ConfigOptions::default())?;
    let agg = optimized
        .downcast_ref::<AggregateExec>()
        .expect("optimizer keeps the AggregateExec at the root");
    Ok(format!("{:?}", agg.aggr_expr()[0].fun().inner()))
}

/// With a plain GROUP BY, the input ordering `(a, b, c)` proves the group-by
/// prefix plus the aggregate's `ORDER BY c`, so the rule marks the aggregate
/// pre-ordered.
#[test]
fn single_group_by_gets_the_group_by_prefix() -> Result<()> {
    let schema = abc_schema();
    let group_by = PhysicalGroupBy::new_single(vec![
        (col("a", &schema)?, "a".to_string()),
        (col("b", &schema)?, "b".to_string()),
    ]);
    let agg = AggregateExec::try_new(
        AggregateMode::Single,
        group_by,
        vec![first_value_expr(&schema)?],
        vec![None],
        ordered_input(&schema)?,
        Arc::clone(&schema),
    )?;
    let dbg = optimized_flag(Arc::new(agg))?;
    assert!(
        dbg.contains("is_input_pre_ordered: true"),
        "expected the flag set for a single grouping set, got: {dbg}"
    );
    Ok(())
}

/// With grouping sets the same rows are fed once per grouping set, and within
/// a coarser set's group the rows follow the full group-by prefix rather than
/// the aggregate's own ORDER BY. The rule must not use the group-by prefix to
/// prove the requirement there.
#[test]
fn grouping_sets_do_not_get_the_group_by_prefix() -> Result<()> {
    let schema = abc_schema();
    // ROLLUP(a, b): grouping sets (a, b), (a), ().
    let group_by = PhysicalGroupBy::new(
        vec![
            (col("a", &schema)?, "a".to_string()),
            (col("b", &schema)?, "b".to_string()),
        ],
        vec![
            (
                datafusion_physical_expr::expressions::lit(1i32),
                "a".to_string(),
            ),
            (
                datafusion_physical_expr::expressions::lit(1i32),
                "b".to_string(),
            ),
        ],
        vec![vec![false, false], vec![false, true], vec![true, true]],
        true,
    );
    let agg = AggregateExec::try_new(
        AggregateMode::Single,
        group_by,
        vec![first_value_expr(&schema)?],
        vec![None],
        ordered_input(&schema)?,
        Arc::clone(&schema),
    )?;
    let dbg = optimized_flag(Arc::new(agg))?;
    assert!(
        dbg.contains("is_input_pre_ordered: false"),
        "the flag must stay unset under grouping sets, got: {dbg}"
    );
    Ok(())
}
