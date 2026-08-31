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
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::Operator;
use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion_functions_window::rank::{dense_rank_udwf, rank_udwf};
use datafusion_functions_window::row_number::row_number_udwf;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, col, lit};
use datafusion_physical_expr::window::StandardWindowExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::window_topn::WindowTopN;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::metrics::MetricValue;
use datafusion_physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::partitioned_topk::PartitionedTopKExec;
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

/// Build: FilterExec(rn <= limit) → BoundedWindowAggExec(ROW_NUMBER PBY pk OBY val)
///
/// Matches the pre-`EnsureRequirements` plan shape (no `SortExec` under the window).
fn build_window_topn_plan(
    limit_value: i64,
    op: Operator,
) -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

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
        input,
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
      PartitionedTopKExec: fn=row_number, fetch=3, partition=[pk@0], order=[val@1 ASC]
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
      PartitionedTopKExec: fn=row_number, fetch=2, partition=[pk@0], order=[val@1 ASC]
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
            input,
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
      PartitionedTopKExec: fn=row_number, fetch=3, partition=[pk@0], order=[val@1 ASC]
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
        input,
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
        PartitionedTopKExec: fn=row_number, fetch=3, partition=[pk@0], order=[val@1 ASC]
          PlaceholderRowExec
    "#);
    Ok(())
}

// ----------------------------------------------------------------------
// RANK rule tests
// ----------------------------------------------------------------------

/// Build: FilterExec(rk op limit) → BoundedWindowAggExec(<udwf> PBY pk OBY val)
///
/// Matches the pre-`EnsureRequirements` plan shape (no `SortExec` under the window).
///
/// `udwf_factory` selects the window UDWF (rank, dense_rank, ...) and
/// `udwf_name` is the column name produced by that UDWF (matters because
/// the rule resolves the filter column by index, but the snapshot prints
/// the name).
fn build_ranking_topn_plan(
    udwf_factory: fn() -> Arc<datafusion_expr::WindowUDF>,
    udwf_name: &str,
    limit_value: i64,
    op: Operator,
) -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    let partition_by = vec![col("pk", &s)?];
    let order_by = vec![PhysicalSortExpr::new_default(col("val", &s)?).asc()];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(&udwf_factory(), &[], &s, udwf_name.to_string(), false)?,
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
        input,
        InputOrderMode::Sorted,
        true,
    )?);

    let rk_col = Arc::new(Column::new(udwf_name, 2));
    let limit_lit = lit(ScalarValue::UInt64(Some(limit_value as u64)));
    // Place column on whichever side matches the operator's expectation.
    let predicate: Arc<dyn datafusion_physical_expr::PhysicalExpr> = match op {
        Operator::LtEq | Operator::Lt => Arc::new(BinaryExpr::new(rk_col, op, limit_lit)),
        Operator::GtEq | Operator::Gt => Arc::new(BinaryExpr::new(limit_lit, op, rk_col)),
        _ => unreachable!("only </<=/>=/> are supported by the rule"),
    };
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, window)?);

    Ok(filter)
}

/// Build a RANK / DENSE_RANK plan with NO ORDER BY: every row ties at rank 1 — degenerate.
fn build_no_order_by_plan(
    udwf_factory: fn() -> Arc<datafusion_expr::WindowUDF>,
    udwf_name: &str,
    limit_value: i64,
) -> Result<Arc<dyn ExecutionPlan>> {
    let s = schema();
    let input: Arc<dyn ExecutionPlan> = Arc::new(PlaceholderRowExec::new(Arc::clone(&s)));

    let ordering =
        LexOrdering::new(vec![PhysicalSortExpr::new_default(col("pk", &s)?).asc()])
            .unwrap();

    let sort: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));

    let partition_by = vec![col("pk", &s)?];

    let window_expr = Arc::new(StandardWindowExpr::new(
        create_udwf_window_expr(&udwf_factory(), &[], &s, udwf_name.to_string(), false)?,
        &partition_by,
        &[], // empty ORDER BY
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

    let rk_col = Arc::new(Column::new(udwf_name, 2));
    let limit_lit = lit(ScalarValue::UInt64(Some(limit_value as u64)));
    let predicate = Arc::new(BinaryExpr::new(rk_col, Operator::LtEq, limit_lit));
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate, window)?);

    Ok(filter)
}

#[test]
fn basic_rank_rk_lteq_3() -> Result<()> {
    let plan = build_ranking_topn_plan(rank_udwf, "rank", 3, Operator::LtEq)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[rank: Field { "rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn rank_rk_lt_4_becomes_fetch_3() -> Result<()> {
    let plan = build_ranking_topn_plan(rank_udwf, "rank", 4, Operator::Lt)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[rank: Field { "rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn rank_flipped_3_gteq_rk() -> Result<()> {
    let plan = build_ranking_topn_plan(rank_udwf, "rank", 3, Operator::GtEq)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[rank: Field { "rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn rank_flipped_4_gt_rk_becomes_fetch_3() -> Result<()> {
    let plan = build_ranking_topn_plan(rank_udwf, "rank", 4, Operator::Gt)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[rank: Field { "rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn rank_no_order_by_no_change() -> Result<()> {
    // Without ORDER BY, every row ties at rank 1 — the optimization is
    // degenerate (entire input would be retained, ties storage unbounded).
    // The rule must skip.
    let plan = build_no_order_by_plan(rank_udwf, "rank", 3)?;
    let before = plan_str(plan.as_ref());
    let optimized = optimize(plan)?;
    let after = plan_str(optimized.as_ref());
    assert_eq!(
        before, after,
        "RANK with empty ORDER BY must not be rewritten"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// DENSE_RANK rule tests
// ----------------------------------------------------------------------

#[test]
fn basic_dense_rank_dr_lteq_3() -> Result<()> {
    let plan = build_ranking_topn_plan(dense_rank_udwf, "dense_rank", 3, Operator::LtEq)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[dense_rank: Field { "dense_rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=dense_rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn dense_rank_dr_lt_4_becomes_fetch_3() -> Result<()> {
    let plan = build_ranking_topn_plan(dense_rank_udwf, "dense_rank", 4, Operator::Lt)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[dense_rank: Field { "dense_rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=dense_rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn dense_rank_flipped_3_gteq_dr() -> Result<()> {
    let plan = build_ranking_topn_plan(dense_rank_udwf, "dense_rank", 3, Operator::GtEq)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[dense_rank: Field { "dense_rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=dense_rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn dense_rank_flipped_4_gt_dr_becomes_fetch_3() -> Result<()> {
    let plan = build_ranking_topn_plan(dense_rank_udwf, "dense_rank", 4, Operator::Gt)?;
    let optimized = optimize(plan)?;
    assert_snapshot!(plan_str(optimized.as_ref()), @r#"
    BoundedWindowAggExec: wdw=[dense_rank: Field { "dense_rank": UInt64 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      PartitionedTopKExec: fn=dense_rank, fetch=3, partition=[pk@0], order=[val@1 ASC]
        PlaceholderRowExec
    "#);
    Ok(())
}

#[test]
fn dense_rank_no_order_by_no_change() -> Result<()> {
    // Without ORDER BY, every row ties at dense_rank 1 — the optimization
    // is degenerate (entire input would be retained). The rule must skip.
    let plan = build_no_order_by_plan(dense_rank_udwf, "dense_rank", 3)?;
    let before = plan_str(plan.as_ref());
    let optimized = optimize(plan)?;
    let after = plan_str(optimized.as_ref());
    assert_eq!(
        before, after,
        "DENSE_RANK with empty ORDER BY must not be rewritten"
    );
    Ok(())
}

// ----------------------------------------------------------------------
// Shared guard: `fn < 1` keeps nothing
// ----------------------------------------------------------------------

#[test]
fn predicate_lt_1_no_change() -> Result<()> {
    // `fn < 1` (and the flipped `1 > fn`) yields limit_n = 0. Since
    // ROW_NUMBER / RANK / DENSE_RANK are always >= 1, the predicate keeps
    // nothing and the rule must skip — a fetch=0 PartitionedTopK* would
    // otherwise panic on its `k > 0` assertion at execution time.
    type UdwfFactory = fn() -> Arc<datafusion_expr::WindowUDF>;
    let cases: [(UdwfFactory, &str); 3] = [
        (row_number_udwf, "row_number"),
        (rank_udwf, "rank"),
        (dense_rank_udwf, "dense_rank"),
    ];
    for (factory, name) in cases {
        let plan = build_ranking_topn_plan(factory, name, 1, Operator::Lt)?;
        let before = plan_str(plan.as_ref());
        let optimized = optimize(plan)?;
        let after = plan_str(optimized.as_ref());
        assert_eq!(
            before, after,
            "`{name} < 1` (limit 0) must not be rewritten"
        );
    }
    Ok(())
}

// ----------------------------------------------------------------------
// Execution: metrics exposure
// ----------------------------------------------------------------------

/// Recursively finds the `PartitionedTopKExec` node in `plan`.
fn find_partitioned_topk(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.is::<PartitionedTopKExec>() {
        return Some(Arc::clone(plan));
    }
    plan.children().into_iter().find_map(find_partitioned_topk)
}

/// Regression test for #24470: `PartitionedTopKExec` builds an
/// `ExecutionPlanMetricsSet` and hands it to the top-K state, but used to omit
/// `ExecutionPlan::metrics()`, so nothing it recorded was ever observable (and
/// `EXPLAIN ANALYZE` showed no metrics for the operator).
///
/// The `output_batches` assertion also covers the emit-side counting fixed for
/// #24468: the per-partition heaps are coalesced into `batch_size` batches, so
/// the metric must count the coalesced output, not the per-partition inputs.
#[tokio::test]
async fn partitioned_topk_exec_exposes_metrics() -> Result<()> {
    let mut config = SessionConfig::new()
        .with_batch_size(10)
        .with_target_partitions(1);
    config.options_mut().optimizer.enable_window_topn = true;
    let ctx = SessionContext::new_with_config(config);

    // 10 partition keys x 5 rows each; top-5 per key keeps all 50 rows, which
    // at batch_size 10 must be emitted as 5 batches.
    ctx.sql(
        "CREATE TABLE t AS
         SELECT v % 10 AS pk, v AS val FROM (SELECT unnest(range(0, 50)) AS v)",
    )
    .await?
    .collect()
    .await?;

    let df = ctx
        .sql(
            "SELECT pk, val FROM (
               SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY val) AS rn FROM t
             ) WHERE rn <= 5",
        )
        .await?;
    let plan = df.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 50);

    let topk = find_partitioned_topk(&plan).expect("plan has a PartitionedTopKExec");
    let metrics = topk
        .metrics()
        .expect("PartitionedTopKExec should expose metrics");
    assert_eq!(metrics.output_rows(), Some(50));
    let output_batches = metrics
        .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
        .expect("output_batches metric")
        .as_usize();
    assert_eq!(output_batches, 5);

    Ok(())
}
