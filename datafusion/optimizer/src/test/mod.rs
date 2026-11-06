// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::analyzer::{Analyzer, AnalyzerRule};
use crate::optimizer::Optimizer;
use crate::{OptimizerContext, OptimizerRule};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, assert_contains};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, logical_plan::table_scan};
use std::sync::Arc;

pub mod udfs;
pub mod user_defined;

/// A projection computing `exprs` over `input`, whose outputs get ids as with
/// `LogicalPlanBuilder::project`, from a generator advanced past the ids of
/// `input` (for tests that build projections by hand)
pub fn new_projection(
    exprs: Vec<datafusion_expr::Expr>,
    input: Arc<LogicalPlan>,
) -> Result<datafusion_expr::Projection> {
    let exprs = named_exprs(exprs, &input);
    datafusion_expr::Projection::try_new(exprs, input)
}

/// An aggregate computing `group_expr` and `aggr_expr` over `input`, whose
/// outputs get ids from a generator advanced past the ids of `input` (bare
/// grouping columns pass through)
pub fn new_aggregate(
    input: Arc<LogicalPlan>,
    group_expr: Vec<datafusion_expr::Expr>,
    aggr_expr: Vec<datafusion_expr::Expr>,
) -> Result<datafusion_expr::Aggregate> {
    let generator =
        datafusion_common::ColumnIdGenerator::new_after(input.max_column_id());
    let (group_expr, grouping_sets) = datafusion_expr::Aggregate::allocate_group_expr(
        group_expr,
        input.schema(),
        &generator,
    )?;
    let aggr_expr = aggr_expr
        .into_iter()
        .map(|e| datafusion_expr::NamedExpr::new(e, generator.next()))
        .collect();
    datafusion_expr::Aggregate::try_new(input, group_expr, grouping_sets, aggr_expr)
}

/// The outputs computing `exprs` over `input`, see [`new_projection`]
pub fn named_exprs(
    exprs: Vec<datafusion_expr::Expr>,
    input: &LogicalPlan,
) -> Vec<datafusion_expr::NamedExpr> {
    let generator =
        datafusion_common::ColumnIdGenerator::new_after(input.max_column_id());
    exprs
        .into_iter()
        .map(|e| datafusion_expr::NamedExpr::allocate(e, input.schema(), &generator))
        .collect()
}

pub fn test_table_scan_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]
}

pub fn test_table_scan_with_struct_fields() -> Vec<Field> {
    vec![
        Field::new("id", DataType::UInt32, false),
        Field::new(
            "user",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("status", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
    ]
}

pub fn test_table_scan_with_struct() -> Result<LogicalPlan> {
    let schema = Schema::new(test_table_scan_with_struct_fields());
    table_scan(Some("test"), &schema, None)?.build()
}

/// some tests share a common table with different names
pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
    let schema = Schema::new(test_table_scan_fields());
    table_scan(Some(name), &schema, None)?.build()
}

/// some tests share a common table
pub fn test_table_scan() -> Result<LogicalPlan> {
    test_table_scan_with_name("test")
}

/// Scan an empty data source, mainly used in tests
pub fn scan_empty(
    name: Option<&str>,
    table_schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Result<LogicalPlanBuilder> {
    table_scan(name, table_schema, projection)
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}

pub fn scan_tpch_table(table: &str) -> LogicalPlan {
    let schema = Arc::new(get_tpch_table_schema(table));
    table_scan(Some(table), &schema, None)
        .unwrap()
        .build()
        .unwrap()
}

pub fn get_tpch_table_schema(table: &str) -> Schema {
    match table {
        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, true),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
        ]),

        _ => unimplemented!("Table: {}", table),
    }
}

#[macro_export]
macro_rules! assert_analyzed_plan_with_config_eq_snapshot {
    (
        $options:expr,
        $rule:expr,
        $plan:expr,
        @ $expected:literal $(,)?
    ) => {{
    let analyzed_plan = $crate::Analyzer::with_rules(vec![$rule]).execute_and_check($plan, &$options, |_, _| {})?;

    insta::assert_snapshot!(analyzed_plan, @ $expected);

    Ok::<(), datafusion_common::DataFusionError>(())
    }};
}

pub fn assert_analyzer_check_err(
    rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    plan: LogicalPlan,
    expected: &str,
) {
    let options = ConfigOptions::default();
    let analyzed_plan =
        Analyzer::with_rules(rules).execute_and_check(plan, &options, |_, _| {});
    match analyzed_plan {
        Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "An error"),
        Err(e) => {
            assert_contains!(e.to_string(), expected);
        }
    }
}

/// Asserts that `after`, the output of rules applied to `before`, has no
/// more column id issues of any kind than `before`: the check of
/// `column_id_check`, for the tests of a rule applied to a plan that has
/// issues already
///
/// A plan without any id is not checked, as by the verifier.
pub fn assert_no_new_column_id_issues(before: &LogicalPlan, after: &LogicalPlan) {
    if let Err(e) = crate::column_id_check::check_column_ids_kept(before, after) {
        panic!("{e}\nin\n{}", after.display_indent());
    }
}

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

#[macro_export]
macro_rules! assert_optimized_plan_eq_snapshot {
    (
        $optimizer_context:expr,
        $rules:expr,
        $plan:expr,
        @ $expected:literal $(,)?
    ) => {{
    let optimizer = $crate::Optimizer::with_rules($rules);
    let optimized_plan = optimizer.optimize($plan, &$optimizer_context, |_, _| {})?;
    insta::assert_snapshot!(optimized_plan, @ $expected);

    Ok::<(), datafusion_common::DataFusionError>(())
    }};
}

fn generate_optimized_plan_with_rules(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    plan: LogicalPlan,
) -> LogicalPlan {
    let config = &mut OptimizerContext::new()
        .with_max_passes(1)
        .with_skip_failing_rules(false);
    let optimizer = Optimizer::with_rules(rules);
    optimizer
        .optimize(plan, config, observe)
        .expect("failed to optimize plan")
}

pub fn assert_optimized_plan_with_rules(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    plan: LogicalPlan,
    expected: &str,
    eq: bool,
) -> Result<()> {
    let optimized_plan = generate_optimized_plan_with_rules(rules, plan);
    let formatted_plan = format!("{optimized_plan}");
    if eq {
        assert_eq!(formatted_plan, expected);
    } else {
        assert_ne!(formatted_plan, expected);
    }
    Ok(())
}

#[macro_export]
macro_rules! assert_optimized_plan_eq_display_indent_snapshot {
    (
        $rule:expr,
        $plan:expr,
        @ $expected:literal $(,)?
    ) => {{
        let optimizer = $crate::Optimizer::with_rules(vec![$rule]);
        let optimized_plan = optimizer
            .optimize($plan, &$crate::OptimizerContext::new(), |_, _| {})
            .expect("failed to optimize plan");
        let formatted_plan = optimized_plan.display_indent_schema();
        insta::assert_snapshot!(formatted_plan, @ $expected);

        Ok::<(), datafusion_common::DataFusionError>(())
    }};
}
