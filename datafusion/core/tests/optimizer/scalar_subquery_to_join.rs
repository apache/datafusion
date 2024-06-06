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

use std::{ops::Add, sync::Arc};

use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    col, lit, max, min, out_ref_col, scalar_subquery, Between, Expr, LogicalPlanBuilder,
};
use datafusion_functions_aggregate::expr_fn::sum;
use datafusion_optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;

use super::{
    assert_analyzer_check_err, assert_multi_rules_optimized_plan_eq_display_indent,
    scan_tpch_table, test_table_scan_with_name,
};

/// Test multiple correlated subqueries
#[test]
fn multiple_subqueries() -> Result<()> {
    let orders = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                col("orders.o_custkey")
                    .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(
            lit(1)
                .lt(scalar_subquery(orders.clone()))
                .and(lit(1).lt(scalar_subquery(orders))),
        )?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: Int32(1) < __scalar_sq_1.MAX(orders.o_custkey) AND Int32(1) < __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: __scalar_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n            Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test recursive correlated subqueries
#[test]
fn recursive_subqueries() -> Result<()> {
    let lineitem = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
            .filter(
                col("lineitem.l_orderkey")
                    .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
            )?
            .aggregate(
                Vec::<Expr>::new(),
                vec![sum(col("lineitem.l_extendedprice"))],
            )?
            .project(vec![sum(col("lineitem.l_extendedprice"))])?
            .build()?,
    );

    let orders = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                col("orders.o_custkey")
                    .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                    .and(col("orders.o_totalprice").lt(scalar_subquery(lineitem))),
            )?
            .aggregate(Vec::<Expr>::new(), vec![sum(col("orders.o_totalprice"))])?
            .project(vec![sum(col("orders.o_totalprice"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_acctbal").lt(scalar_subquery(orders)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_acctbal < __scalar_sq_1.SUM(orders.o_totalprice) [c_custkey:Int64, c_name:Utf8, SUM(orders.o_totalprice):Float64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, SUM(orders.o_totalprice):Float64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [SUM(orders.o_totalprice):Float64;N, o_custkey:Int64]\
        \n        Projection: SUM(orders.o_totalprice), orders.o_custkey [SUM(orders.o_totalprice):Float64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[SUM(orders.o_totalprice)]] [o_custkey:Int64, SUM(orders.o_totalprice):Float64;N]\
        \n            Filter: orders.o_totalprice < __scalar_sq_2.SUM(lineitem.l_extendedprice) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N]\
        \n              Left Join:  Filter: __scalar_sq_2.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N]\
        \n                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n                SubqueryAlias: __scalar_sq_2 [SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64]\
        \n                  Projection: SUM(lineitem.l_extendedprice), lineitem.l_orderkey [SUM(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64]\
        \n                    Aggregate: groupBy=[[lineitem.l_orderkey]], aggr=[[SUM(lineitem.l_extendedprice)]] [l_orderkey:Int64, SUM(lineitem.l_extendedprice):Float64;N]\
        \n                      TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";
    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery filter with additional subquery filters
#[test]
fn scalar_subquery_with_subquery_filters() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey"))
                    .and(col("o_orderkey").eq(lit(1))),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery with no columns in schema
#[test]
fn scalar_subquery_no_cols() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    // it will optimize, but fail for the same reason the unoptimized query would
    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for scalar subquery with both columns in schema
#[test]
fn scalar_subquery_with_no_correlated_cols() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery not equal
#[test]
fn scalar_subquery_where_not_eq() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .not_eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) != orders.o_custkey";

    assert_analyzer_check_err(vec![], plan, expected);
    Ok(())
}

/// Test for correlated scalar subquery less than
#[test]
fn scalar_subquery_where_less_than() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .lt(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) < orders.o_custkey";

    assert_analyzer_check_err(vec![], plan, expected);
    Ok(())
}

/// Test for correlated scalar subquery filter with subquery disjunction
#[test]
fn scalar_subquery_with_subquery_disjunction() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey"))
                    .or(col("o_orderkey").eq(lit(1))),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Correlated column is not allowed in predicate: outer_ref(customer.c_custkey) = orders.o_custkey OR orders.o_orderkey = Int32(1)";

    assert_analyzer_check_err(vec![], plan, expected);
    Ok(())
}

/// Test for correlated scalar without projection
#[test]
fn scalar_subquery_no_projection() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Scalar subquery should only return one column";
    assert_analyzer_check_err(vec![], plan, expected);
    Ok(())
}

/// Test for correlated scalar expressions
#[test]
fn scalar_subquery_project_expr() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![col("MAX(orders.o_custkey)").add(lit(1))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) + Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey) + Int32(1), orders.o_custkey [MAX(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery multiple projected columns
#[test]
fn scalar_subquery_multi_col() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
            .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(
            col("customer.c_custkey")
                .eq(scalar_subquery(sq))
                .and(col("c_custkey").eq(lit(1))),
        )?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "check_analyzed_plan\
        \ncaused by\
        \nError during planning: Scalar subquery should only return one column";
    assert_analyzer_check_err(vec![], plan, expected);
    Ok(())
}

/// Test for correlated scalar subquery filter with additional filters
#[test]
fn scalar_subquery_additional_filters_with_non_equal_clause() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(
            col("customer.c_custkey")
                .gt_eq(scalar_subquery(sq))
                .and(col("c_custkey").eq(lit(1))),
        )?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey >= __scalar_sq_1.MAX(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

#[test]
fn scalar_subquery_additional_filters_with_equal_clause() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(
            col("customer.c_custkey")
                .eq(scalar_subquery(sq))
                .and(col("c_custkey").eq(lit(1))),
        )?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery filter with disjustions
#[test]
fn scalar_subquery_disjunction() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(
            col("customer.c_custkey")
                .eq(scalar_subquery(sq))
                .or(col("customer.c_custkey").eq(lit(1))),
        )?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for correlated scalar subquery filter
#[test]
fn exists_subquery_correlated() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
            .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
            .aggregate(Vec::<Expr>::new(), vec![min(col("c"))])?
            .project(vec![min(col("c"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
        .filter(col("test.c").lt(scalar_subquery(sq)))?
        .project(vec![col("test.c")])?
        .build()?;

    let expected = "Projection: test.c [c:UInt32]\
        \n  Filter: test.c < __scalar_sq_1.MIN(sq.c) [a:UInt32, b:UInt32, c:UInt32, MIN(sq.c):UInt32;N, a:UInt32;N]\
        \n    Left Join:  Filter: test.a = __scalar_sq_1.a [a:UInt32, b:UInt32, c:UInt32, MIN(sq.c):UInt32;N, a:UInt32;N]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __scalar_sq_1 [MIN(sq.c):UInt32;N, a:UInt32]\
        \n        Projection: MIN(sq.c), sq.a [MIN(sq.c):UInt32;N, a:UInt32]\
        \n          Aggregate: groupBy=[[sq.a]], aggr=[[MIN(sq.c)]] [a:UInt32, MIN(sq.c):UInt32;N]\
        \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

/// Test for non-correlated scalar subquery with no filters
#[test]
fn scalar_subquery_non_correlated_no_filters_with_non_equal_clause() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").lt(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey < __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

#[test]
fn scalar_subquery_non_correlated_no_filters_with_equal_clause() -> Result<()> {
    let sq = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MAX(orders.o_custkey):Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

#[test]
fn correlated_scalar_subquery_in_between_clause() -> Result<()> {
    let sq1 = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
            .project(vec![min(col("orders.o_custkey"))])?
            .build()?,
    );
    let sq2 = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                out_ref_col(DataType::Int64, "customer.c_custkey")
                    .eq(col("orders.o_custkey")),
            )?
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let between_expr = Expr::Between(Between {
        expr: Box::new(col("customer.c_custkey")),
        negated: false,
        low: Box::new(scalar_subquery(sq1)),
        high: Box::new(scalar_subquery(sq2)),
    });

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(between_expr)?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey BETWEEN __scalar_sq_1.MIN(orders.o_custkey) AND __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n    Left Join:  Filter: customer.c_custkey = __scalar_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N, MAX(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n      Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, o_custkey:Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MIN(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Projection: MIN(orders.o_custkey), orders.o_custkey [MIN(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n            Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MIN(orders.o_custkey)]] [o_custkey:Int64, MIN(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n        Projection: MAX(orders.o_custkey), orders.o_custkey [MAX(orders.o_custkey):Int64;N, o_custkey:Int64]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}

#[test]
fn uncorrelated_scalar_subquery_in_between_clause() -> Result<()> {
    let sq1 = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
            .project(vec![min(col("orders.o_custkey"))])?
            .build()?,
    );
    let sq2 = Arc::new(
        LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
            .project(vec![max(col("orders.o_custkey"))])?
            .build()?,
    );

    let between_expr = Expr::Between(Between {
        expr: Box::new(col("customer.c_custkey")),
        negated: false,
        low: Box::new(scalar_subquery(sq1)),
        high: Box::new(scalar_subquery(sq2)),
    });

    let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
        .filter(between_expr)?
        .project(vec![col("customer.c_custkey")])?
        .build()?;

    let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey BETWEEN __scalar_sq_1.MIN(orders.o_custkey) AND __scalar_sq_2.MAX(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, MAX(orders.o_custkey):Int64;N]\
        \n    Left Join:  [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N, MAX(orders.o_custkey):Int64;N]\
        \n      Left Join:  [c_custkey:Int64, c_name:Utf8, MIN(orders.o_custkey):Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [MIN(orders.o_custkey):Int64;N]\
        \n          Projection: MIN(orders.o_custkey) [MIN(orders.o_custkey):Int64;N]\
        \n            Aggregate: groupBy=[[]], aggr=[[MIN(orders.o_custkey)]] [MIN(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [MAX(orders.o_custkey):Int64;N]\
        \n        Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

    assert_multi_rules_optimized_plan_eq_display_indent(
        vec![Arc::new(ScalarSubqueryToJoin::new())],
        plan,
        expected,
    );
    Ok(())
}
