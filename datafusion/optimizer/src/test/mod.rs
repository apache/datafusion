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

use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_expr::{col, logical_plan::table_scan, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;

pub mod user_defined;

/// some tests share a common table with different names
pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]);
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

pub fn test_subquery_with_name(name: &str) -> Result<Arc<LogicalPlan>> {
    let table_scan = test_table_scan_with_name(name)?;
    Ok(Arc::new(
        LogicalPlanBuilder::from(table_scan)
            .project(vec![col("c")])?
            .build()?,
    ))
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

pub fn assert_optimized_plan_eq(
    rule: &dyn OptimizerRule,
    plan: &LogicalPlan,
    expected: &str,
) {
    let optimized_plan = rule
        .optimize(plan, &mut OptimizerConfig::new())
        .expect("failed to optimize plan");
    let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
    assert_eq!(formatted_plan, expected);
}

pub fn assert_optimizer_err(
    rule: &dyn OptimizerRule,
    plan: &LogicalPlan,
    expected: &str,
) {
    let res = rule.optimize(plan, &mut OptimizerConfig::new());
    match res {
        Ok(plan) => assert_eq!(format!("{}", plan.display_indent()), "An error"),
        Err(ref e) => {
            let actual = format!("{}", e);
            if expected.is_empty() || !actual.contains(expected) {
                assert_eq!(actual, expected)
            }
        }
    }
}
