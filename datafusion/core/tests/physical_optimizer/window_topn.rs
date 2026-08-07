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

//! Tests for the WindowTopN physical optimizer rule.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::Operator;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_functions_window::row_number::row_number_udwf;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, col, lit};
use datafusion_physical_expr::window::StandardWindowExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::window_topn::WindowTopN;
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, create_udwf_window_expr};
use datafusion_physical_plan::{ExecutionPlan, InputOrderMode};
use insta::assert_snapshot;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn plan_str(plan: &dyn ExecutionPlan) -> String {
    displayable(plan).indent(true).to_string()
}

fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = ConfigOptions::new();
    config.optimizer.enable_window_topn = true;
    WindowTopN::new().optimize(plan, &config)
}

fn optimize_disabled(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = ConfigOptions::new();
    config.optimizer.enable_window_topn = false;
    WindowTopN::new().optimize(plan, &config)
}

/// Build: FilterExec(rn <= limit) → BoundedWindowAggExec(ROW_NUMBER PBY pk OBY val) → SortExec(pk, val)
fn build_window_topn_plan(
    limit_value: i64,
    op: Operator,
) -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    // Sort by pk ASC, val ASC
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("pk", &s)?).asc(),
        PhysicalSortExpr::new_default(col("val", &s)?).asc(),
    ])
    .unwrap();

    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));

    // ROW_NUMBER() OVER (PARTITION BY pk ORDER BY val)
    let partition_by = vec![col("pk", &s)?];
    let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &s,
            "row_number".to_string(),
            false,
        )?,
        &partition_by,
        &order_by,
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )),
    ));

    let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![window_expr],
        sort,
        InputOrderMode::Sorted,
        true,
    )?);

    // FilterExec: rn op limit_value
    // The ROW_NUMBER column is at index 2 (after pk=0, val=1)
    let rn_col = Arc::new(Column::new("row_number", 2));
    let limit_lit = lit(ScalarValue::UInt64(Some(limit_value as u64)));
    let predicate = Arc::new(BinaryExpr::new(rn_col, op, limit_lit));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, window)?);

    Ok(filter)
}

/// Build a plan with no partition-by: ROW_NUMBER() OVER (ORDER BY val)
fn build_window_topn_no_partition(limit_value: i64) -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    // Sort by val ASC only (no partition key)
    let ordering =
        LexOrdering::new(vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()])
            .unwrap();

    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));

    // ROW_NUMBER() OVER (ORDER BY val) — no partition by
    let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &s,
            "row_number".to_string(),
            false,
        )?,
        &[], // empty partition_by
        &order_by,
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )),
    ));

    let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![window_expr],
        sort,
        InputOrderMode::Sorted,
        true,
    )?);

    let rn_col = Arc::new(Column::new("row_number", 2));
    let limit_lit = lit(ScalarValue::UInt64(Some(limit_value as u64)));
    let predicate = Arc::new(BinaryExpr::new(rn_col, Operator::LtEq, limit_lit));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, window)?);

    Ok(filter)
}

/// Build a plan where filter is on a data column (not window output)
fn build_non_window_filter_plan() -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("pk", &s)?).asc(),
        PhysicalSortExpr::new_default(col("val", &s)?).asc(),
    ])
    .unwrap();

    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));

    let partition_by = vec![col("pk", &s)?];
    let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &s,
            "row_number".to_string(),
            false,
        )?,
        &partition_by,
        &order_by,
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )),
    ));

    let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![window_expr],
        sort,
        InputOrderMode::Sorted,
        true,
    )?);

    // Filter on data column val (index 1), NOT on window output
    let val_col = Arc::new(Column::new("val", 1));
    let limit_lit = lit(ScalarValue::Int64(Some(3)));
    let predicate = Arc::new(BinaryExpr::new(val_col, Operator::LtEq, limit_lit));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, window)?);

    Ok(filter)
}

#[test]
fn basic_row_number_rn_lteq_3() -> Result<()> {
    let plan = build_window_topn_plan(3, Operator::LtEq)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn rn_lt_3_becomes_fetch_2() -> Result<()> {
    let plan = build_window_topn_plan(3, Operator::Lt)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fetch=2, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn flipped_3_gteq_rn() -> Result<()> {
    let plan = {
        let s = schema();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(col("pk", &s)?).asc(),
            PhysicalSortExpr::new_default(col("val", &s)?).asc(),
        ])
        .unwrap();

        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
        );

        let partition_by = vec![col("pk", &s)?];
        let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

        let window_expr = Arc::new(StandardWindowExpr::new(
            create_udwf_window_expr(
                &row_number_udwf(),
                &[],
                &s,
                "row_number".to_string(),
                false,
            )?,
            &partition_by,
            &order_by,
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
        ));

        let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
            vec![window_expr],
            sort,
            InputOrderMode::Sorted,
            true,
        )?);

        // Flipped: 3 >= rn  (Literal GtEq Column)
        let rn_col = Arc::new(Column::new("row_number", 2));
        let limit_lit = lit(ScalarValue::UInt64(Some(3)));
        let predicate = Arc::new(BinaryExpr::new(limit_lit, Operator::GtEq, rn_col));
        let filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(predicate, window)?);
        filter
    };

    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn non_window_column_filter_no_change() -> Result<()> {
    let plan = build_non_window_filter_plan()?;
    let before = plan_str(plan.as_ref());
    let optimized = optimize(plan)?;
    let after = plan_str(optimized.as_ref());
    assert_eq!(
        before, after,
        "Plan should not change when filter is on data column"
    );
    Ok(())
}

#[test]
fn config_disabled_no_change() -> Result<()> {
    let plan = build_window_topn_plan(3, Operator::LtEq)?;
    let before = plan_str(plan.as_ref());
    let optimized = optimize_disabled(plan)?;
    let after = plan_str(optimized.as_ref());
    assert_eq!(
        before, after,
        "Plan should not change when config is disabled"
    );
    Ok(())
}

#[test]
fn no_partition_by_no_change() -> Result<()> {
    // Without PARTITION BY, this is a global top-K which SortExec with
    // fetch already handles — the rule should not fire.
    let plan = build_window_topn_no_partition(5)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    FilterExec: row_number@2 <= 5
      BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        SortExec: expr=[val@1 ASC], preserve_partitioning=[true]
          PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn with_projection_between() -> Result<()> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("pk", &s)?).asc(),
        PhysicalSortExpr::new_default(col("val", &s)?).asc(),
    ])
    .unwrap();

    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));

    let partition_by = vec![col("pk", &s)?];
    let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(
            &row_number_udwf(),
            &[],
            &s,
            "row_number".to_string(),
            false,
        )?,
        &partition_by,
        &order_by,
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        )),
    ));

    let window: Arc<dyn ExecutionPlan> = Arc::new(BoundedWindowAggExec::try_new(
        vec![window_expr],
        sort,
        InputOrderMode::Sorted,
        true,
    )?);

    // Add a ProjectionExec between Filter and Window
    let window_schema = window.schema();
    let proj_exprs: Vec<(Arc<dyn datafusion_physical_expr::PhysicalExpr>, String)> =
        window_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                (
                    Arc::new(Column::new(f.name(), i))
                        as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                    f.name().to_string(),
                )
            })
            .collect();

    let projection: Arc<dyn ExecutionPlan> =
        Arc::new(ProjectionExec::try_new(proj_exprs, window)?);

    // rn column is still at index 2 in the projected schema
    let rn_col = Arc::new(Column::new("row_number", 2));
    let limit_lit = lit(ScalarValue::UInt64(Some(3)));
    let predicate = Arc::new(BinaryExpr::new(rn_col, Operator::LtEq, limit_lit));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, projection)?);

    let optimized = optimize(filter)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    ProjectionExec: expr=[pk@0 as pk, val@1 as val, row_number@2 as row_number]
      BoundedWindowAggExec: wdw=[row_number: Field { "row_number": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
        PartitionedTopKExec: fetch=3, partition=[pk@0], order=[val@1 ASC]
          PlaceholderRowExec
    "#);
    Ok(())
}
