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

use arrow::datatypes::{DataType, Field, Schema};

use datafusion::config::ConfigOptions;
use datafusion_common::{assert_contains, Result};
use datafusion_expr::{table_scan, LogicalPlan};
use datafusion_optimizer::{
    Analyzer, AnalyzerRule, Optimizer, OptimizerContext, OptimizerRule,
};

mod common_subexpr_eliminate;
mod push_down_filter;
mod scalar_subquery_to_join;
mod user_defined;

pub(super) fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
    let schema = Schema::new(test_table_scan_fields());
    table_scan(Some(name), &schema, None)?.build()
}

fn test_table_scan_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]
}

pub(super) fn scan_tpch_table(table: &str) -> LogicalPlan {
    let schema = Arc::new(get_tpch_table_schema(table));
    table_scan(Some(table), &schema, None)
        .unwrap()
        .build()
        .unwrap()
}

fn get_tpch_table_schema(table: &str) -> Schema {
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

fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

pub(super) fn assert_optimized_plan_eq(
    rule: Arc<dyn OptimizerRule + Send + Sync>,
    plan: LogicalPlan,
    expected: &str,
) -> Result<()> {
    // Apply the rule once
    let opt_context = OptimizerContext::new().with_max_passes(1);

    let optimizer = Optimizer::with_rules(vec![rule.clone()]);
    let optimized_plan = optimizer.optimize(plan, &opt_context, observe)?;
    let formatted_plan = format!("{optimized_plan:?}");
    assert_eq!(formatted_plan, expected);

    Ok(())
}

pub(super) fn assert_multi_rules_optimized_plan_eq_display_indent(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    plan: LogicalPlan,
    expected: &str,
) {
    let optimizer = Optimizer::with_rules(rules);
    let optimized_plan = optimizer
        .optimize(plan, &OptimizerContext::new(), observe)
        .expect("failed to optimize plan");
    let formatted_plan = optimized_plan.display_indent_schema().to_string();
    assert_eq!(formatted_plan, expected);
}

pub(super) fn assert_analyzer_check_err(
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
