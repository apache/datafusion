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

use std::any::Any;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use datafusion_common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    and, col, in_list, in_subquery, lit, or, sum, BinaryExpr, ColumnarValue, Expr,
    Extension, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF,
    ScalarUDFImpl, Signature, TableProviderFilterPushDown, TableScan, TableSource,
    TableType, UserDefinedLogicalNodeCore, Volatility,
};

use datafusion_expr::logical_plan::table_scan;

use datafusion_optimizer::{
    optimizer::Optimizer, push_down_filter::PushDownFilter,
    rewrite_disjunctive_predicate::RewriteDisjunctivePredicate, OptimizerContext,
    OptimizerRule,
};

use crate::optimizer::observe;

use super::test_table_scan_with_name;

// Test push down filter
fn test_table_scan_fields() -> Vec<Field> {
    vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]
}

/// some tests share a common table
fn test_table_scan() -> Result<LogicalPlan> {
    test_table_scan_with_name("test")
}

fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
    super::assert_optimized_plan_eq(Arc::new(PushDownFilter::new()), plan, expected)
}

fn assert_optimized_plan_eq_with_rewrite_predicate(
    plan: LogicalPlan,
    expected: &str,
) -> Result<()> {
    let optimizer = Optimizer::with_rules(vec![
        Arc::new(RewriteDisjunctivePredicate::new()),
        Arc::new(PushDownFilter::new()),
    ]);
    let optimized_plan = optimizer.optimize(plan, &OptimizerContext::new(), observe)?;

    let formatted_plan = format!("{optimized_plan:?}");
    assert_eq!(expected, formatted_plan);
    Ok(())
}

#[test]
fn filter_before_projection() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;
    // filter is before projection
    let expected = "\
            Projection: test.a, test.b\
            \n  TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_after_limit() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .limit(0, Some(10))?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;
    // filter is before single projection
    let expected = "\
            Filter: test.a = Int64(1)\
            \n  Limit: skip=0, fetch=10\
            \n    Projection: test.a, test.b\
            \n      TableScan: test";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_no_columns() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(lit(0i64).eq(lit(1i64)))?
        .build()?;
    let expected = "TableScan: test, full_filters=[Int64(0) = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_jump_2_plans() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .project(vec![col("c"), col("b")])?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;
    // filter is before double projection
    let expected = "\
            Projection: test.c, test.b\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_move_agg() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .aggregate(vec![col("a")], vec![sum(col("b")).alias("total_salary")])?
        .filter(col("a").gt(lit(10i64)))?
        .build()?;
    // filter of key aggregation is commutative
    let expected = "\
            Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b) AS total_salary]]\
            \n  TableScan: test, full_filters=[test.a > Int64(10)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_complex_group_by() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
        .filter(col("b").gt(lit(10i64)))?
        .build()?;
    let expected = "Filter: test.b > Int64(10)\
        \n  Aggregate: groupBy=[[test.b + test.a]], aggr=[[SUM(test.a), test.b]]\
        \n    TableScan: test";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn push_agg_need_replace_expr() -> Result<()> {
    let plan = LogicalPlanBuilder::from(test_table_scan()?)
        .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
        .filter(col("test.b + test.a").gt(lit(10i64)))?
        .build()?;
    let expected = "Aggregate: groupBy=[[test.b + test.a]], aggr=[[SUM(test.a), test.b]]\
        \n  TableScan: test, full_filters=[test.b + test.a > Int64(10)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_keep_agg() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .aggregate(vec![col("a")], vec![sum(col("b")).alias("b")])?
        .filter(col("b").gt(lit(10i64)))?
        .build()?;
    // filter of aggregate is after aggregation since they are non-commutative
    let expected = "\
            Filter: b > Int64(10)\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b) AS b]]\
            \n    TableScan: test";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that a filter is pushed to before a projection, the filter expression is correctly re-written
#[test]
fn alias() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .filter(col("b").eq(lit(1i64)))?
        .build()?;
    // filter is before projection
    let expected = "\
            Projection: test.a AS b, test.c\
            \n  TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

fn add(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Plus,
        Box::new(right),
    ))
}

fn multiply(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        Operator::Multiply,
        Box::new(right),
    ))
}

/// verifies that a filter is pushed to before a projection with a complex expression, the filter expression is correctly re-written
#[test]
fn complex_expression() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![
            add(multiply(col("a"), lit(2)), col("c")).alias("b"),
            col("c"),
        ])?
        .filter(col("b").eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "\
            Filter: b = Int64(1)\
            \n  Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n    TableScan: test"
    );

    // filter is before projection
    let expected = "\
            Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n  TableScan: test, full_filters=[test.a * Int32(2) + test.c = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that when a filter is pushed to after 2 projections, the filter expression is correctly re-written
#[test]
fn complex_plan() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![
            add(multiply(col("a"), lit(2)), col("c")).alias("b"),
            col("c"),
        ])?
        // second projection where we rename columns, just to make it difficult
        .project(vec![multiply(col("b"), lit(3)).alias("a"), col("c")])?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "\
            Filter: a = Int64(1)\
            \n  Projection: b * Int32(3) AS a, test.c\
            \n    Projection: test.a * Int32(2) + test.c AS b, test.c\
            \n      TableScan: test"
    );

    // filter is before the projections
    let expected = "\
        Projection: b * Int32(3) AS a, test.c\
        \n  Projection: test.a * Int32(2) + test.c AS b, test.c\
        \n    TableScan: test, full_filters=[(test.a * Int32(2) + test.c) * Int32(3) = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NoopPlan {
    input: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}

impl UserDefinedLogicalNodeCore for NoopPlan {
    fn name(&self) -> &str {
        "NoopPlan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.input.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.input
            .iter()
            .flat_map(|child| child.expressions())
            .collect()
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        HashSet::from_iter(vec!["c".to_string()])
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NoopPlan")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs,
            schema: Arc::clone(&self.schema),
        })
    }
}

#[test]
fn user_defined_plan() -> Result<()> {
    let table_scan = test_table_scan()?;

    let custom_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(NoopPlan {
            input: vec![table_scan.clone()],
            schema: table_scan.schema().clone(),
        }),
    });
    let plan = LogicalPlanBuilder::from(custom_plan)
        .filter(col("a").eq(lit(1i64)))?
        .build()?;

    // Push filter below NoopPlan
    let expected = "\
            NoopPlan\
            \n  TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)?;

    let custom_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(NoopPlan {
            input: vec![table_scan.clone()],
            schema: table_scan.schema().clone(),
        }),
    });
    let plan = LogicalPlanBuilder::from(custom_plan)
        .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
        .build()?;

    // Push only predicate on `a` below NoopPlan
    let expected = "\
            Filter: test.c = Int64(2)\
            \n  NoopPlan\
            \n    TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)?;

    let custom_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(NoopPlan {
            input: vec![table_scan.clone(), table_scan.clone()],
            schema: table_scan.schema().clone(),
        }),
    });
    let plan = LogicalPlanBuilder::from(custom_plan)
        .filter(col("a").eq(lit(1i64)))?
        .build()?;

    // Push filter below NoopPlan for each child branch
    let expected = "\
            NoopPlan\
            \n  TableScan: test, full_filters=[test.a = Int64(1)]\
            \n  TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)?;

    let custom_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(NoopPlan {
            input: vec![table_scan.clone(), table_scan.clone()],
            schema: table_scan.schema().clone(),
        }),
    });
    let plan = LogicalPlanBuilder::from(custom_plan)
        .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
        .build()?;

    // Push only predicate on `a` below NoopPlan
    let expected = "\
            Filter: test.c = Int64(2)\
            \n  NoopPlan\
            \n    TableScan: test, full_filters=[test.a = Int64(1)]\
            \n    TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that when two filters apply after an aggregation that only allows one to be pushed, one is pushed
/// and the other not.
#[test]
fn multi_filter() -> Result<()> {
    // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .aggregate(vec![col("b")], vec![sum(col("c"))])?
        .filter(col("b").gt(lit(10i64)))?
        .filter(col("SUM(test.c)").gt(lit(10i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "\
            Filter: SUM(test.c) > Int64(10)\
            \n  Filter: b > Int64(10)\
            \n    Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
            \n      Projection: test.a AS b, test.c\
            \n        TableScan: test"
    );

    // filter is before the projections
    let expected = "\
        Filter: SUM(test.c) > Int64(10)\
        \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
        \n    Projection: test.a AS b, test.c\
        \n      TableScan: test, full_filters=[test.a > Int64(10)]";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that when a filter with two predicates is applied after an aggregation that only allows one to be pushed, one is pushed
/// and the other not.
#[test]
fn split_filter() -> Result<()> {
    // the aggregation allows one filter to pass (b), and the other one to not pass (SUM(c))
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .aggregate(vec![col("b")], vec![sum(col("c"))])?
        .filter(and(
            col("SUM(test.c)").gt(lit(10i64)),
            and(col("b").gt(lit(10i64)), col("SUM(test.c)").lt(lit(20i64))),
        ))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "\
            Filter: SUM(test.c) > Int64(10) AND b > Int64(10) AND SUM(test.c) < Int64(20)\
            \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test"
    );

    // filter is before the projections
    let expected = "\
        Filter: SUM(test.c) > Int64(10) AND SUM(test.c) < Int64(20)\
        \n  Aggregate: groupBy=[[b]], aggr=[[SUM(test.c)]]\
        \n    Projection: test.a AS b, test.c\
        \n      TableScan: test, full_filters=[test.a > Int64(10)]";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that when two limits are in place, we jump neither
#[test]
fn double_limit() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .limit(0, Some(20))?
        .limit(0, Some(10))?
        .project(vec![col("a"), col("b")])?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;
    // filter does not just any of the limits
    let expected = "\
            Projection: test.a, test.b\
            \n  Filter: test.a = Int64(1)\
            \n    Limit: skip=0, fetch=10\
            \n      Limit: skip=0, fetch=20\
            \n        Projection: test.a, test.b\
            \n          TableScan: test";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn union_all() -> Result<()> {
    let table_scan = test_table_scan()?;
    let table_scan2 = test_table_scan_with_name("test2")?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .union(LogicalPlanBuilder::from(table_scan2).build()?)?
        .filter(col("a").eq(lit(1i64)))?
        .build()?;
    // filter appears below Union
    let expected = "Union\
        \n  TableScan: test, full_filters=[test.a = Int64(1)]\
        \n  TableScan: test2, full_filters=[test2.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn union_all_on_projection() -> Result<()> {
    let table_scan = test_table_scan()?;
    let table = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b")])?
        .alias("test2")?;

    let plan = table
        .clone()
        .union(table.build()?)?
        .filter(col("b").eq(lit(1i64)))?
        .build()?;

    // filter appears below Union
    let expected = "Union\n  SubqueryAlias: test2\
        \n    Projection: test.a AS b\
        \n      TableScan: test, full_filters=[test.a = Int64(1)]\
        \n  SubqueryAlias: test2\
        \n    Projection: test.a AS b\
        \n      TableScan: test, full_filters=[test.a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_union_different_schema() -> Result<()> {
    let left = LogicalPlanBuilder::from(test_table_scan()?)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;

    let schema = Schema::new(vec![
        Field::new("d", DataType::UInt32, false),
        Field::new("e", DataType::UInt32, false),
        Field::new("f", DataType::UInt32, false),
    ]);
    let right = table_scan(Some("test1"), &schema, None)?
        .project(vec![col("d"), col("e"), col("f")])?
        .build()?;
    let filter = and(col("test.a").eq(lit(1)), col("test1.d").gt(lit(2)));
    let plan = LogicalPlanBuilder::from(left)
        .cross_join(right)?
        .project(vec![col("test.a"), col("test1.d")])?
        .filter(filter)?
        .build()?;

    let expected = "Projection: test.a, test1.d\
        \n  CrossJoin:\
        \n    Projection: test.a, test.b, test.c\
        \n      TableScan: test, full_filters=[test.a = Int32(1)]\
        \n    Projection: test1.d, test1.e, test1.f\
        \n      TableScan: test1, full_filters=[test1.d > Int32(2)]";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_project_same_name_different_qualifier() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test1")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = and(col("test.a").eq(lit(1)), col("test1.a").gt(lit(2)));
    let plan = LogicalPlanBuilder::from(left)
        .cross_join(right)?
        .project(vec![col("test.a"), col("test1.a")])?
        .filter(filter)?
        .build()?;

    let expected = "Projection: test.a, test1.a\
        \n  CrossJoin:\
        \n    Projection: test.a, test.b, test.c\
        \n      TableScan: test, full_filters=[test.a = Int32(1)]\
        \n    Projection: test1.a, test1.b, test1.c\
        \n      TableScan: test1, full_filters=[test1.a > Int32(2)]";
    assert_optimized_plan_eq(plan, expected)
}

/// verifies that filters with the same columns are correctly placed
#[test]
fn filter_2_breaks_limits() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a")])?
        .filter(col("a").lt_eq(lit(1i64)))?
        .limit(0, Some(1))?
        .project(vec![col("a")])?
        .filter(col("a").gt_eq(lit(1i64)))?
        .build()?;
    // Should be able to move both filters below the projections

    // not part of the test
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.a >= Int64(1)\
             \n  Projection: test.a\
             \n    Limit: skip=0, fetch=1\
             \n      Filter: test.a <= Int64(1)\
             \n        Projection: test.a\
             \n          TableScan: test"
    );

    let expected = "\
        Projection: test.a\
        \n  Filter: test.a >= Int64(1)\
        \n    Limit: skip=0, fetch=1\
        \n      Projection: test.a\
        \n        TableScan: test, full_filters=[test.a <= Int64(1)]";

    assert_optimized_plan_eq(plan, expected)
}

/// verifies that filters to be placed on the same depth are ANDed
#[test]
fn two_filters_on_same_depth() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .limit(0, Some(1))?
        .filter(col("a").lt_eq(lit(1i64)))?
        .filter(col("a").gt_eq(lit(1i64)))?
        .project(vec![col("a")])?
        .build()?;

    // not part of the test
    assert_eq!(
        format!("{plan:?}"),
        "Projection: test.a\
            \n  Filter: test.a >= Int64(1)\
            \n    Filter: test.a <= Int64(1)\
            \n      Limit: skip=0, fetch=1\
            \n        TableScan: test"
    );

    let expected = "\
        Projection: test.a\
        \n  Filter: test.a >= Int64(1) AND test.a <= Int64(1)\
        \n    Limit: skip=0, fetch=1\
        \n      TableScan: test";

    assert_optimized_plan_eq(plan, expected)
}

/// verifies that filters on a plan with user nodes are not lost
/// (ARROW-10547)
#[test]
fn filters_user_defined_node() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(col("a").lt_eq(lit(1i64)))?
        .build()?;

    let plan = super::user_defined::new(plan);

    let expected = "\
            TestUserDefined\
             \n  Filter: test.a <= Int64(1)\
             \n    TableScan: test";

    // not part of the test
    assert_eq!(format!("{plan:?}"), expected);

    let expected = "\
        TestUserDefined\
         \n  TableScan: test, full_filters=[test.a <= Int64(1)]";

    assert_optimized_plan_eq(plan, expected)
}

/// post-on-join predicates on a column common to both sides is pushed to both sides
#[test]
fn filter_on_join_on_common_independent() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            None,
        )?
        .filter(col("test.a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.a <= Int64(1)\
            \n  Inner Join: test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter sent to side before the join
    let expected = "\
        Inner Join: test.a = test2.a\
        \n  TableScan: test, full_filters=[test.a <= Int64(1)]\
        \n  Projection: test2.a\
        \n    TableScan: test2, full_filters=[test2.a <= Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// post-using-join predicates on a column common to both sides is pushed to both sides
#[test]
fn filter_using_join_on_common_independent() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join_using(
            right,
            JoinType::Inner,
            vec![Column::from_name("a".to_string())],
        )?
        .filter(col("a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.a <= Int64(1)\
            \n  Inner Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter sent to side before the join
    let expected = "\
        Inner Join: Using test.a = test2.a\
        \n  TableScan: test, full_filters=[test.a <= Int64(1)]\
        \n  Projection: test2.a\
        \n    TableScan: test2, full_filters=[test2.a <= Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// post-join predicates with columns from both sides are converted to join filterss
#[test]
fn filter_join_on_common_dependent() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            None,
        )?
        .filter(col("c").lt_eq(col("b")))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.c <= test2.b\
            \n  Inner Join: test.a = test2.a\
            \n    Projection: test.a, test.c\
            \n      TableScan: test\
            \n    Projection: test2.a, test2.b\
            \n      TableScan: test2"
    );

    // Filter is converted to Join Filter
    let expected = "\
        Inner Join: test.a = test2.a Filter: test.c <= test2.b\
        \n  Projection: test.a, test.c\
        \n    TableScan: test\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// post-join predicates with columns from one side of a join are pushed only to that side
#[test]
fn filter_join_on_one_side() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let table_scan_right = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(table_scan_right)
        .project(vec![col("a"), col("c")])?
        .build()?;

    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            None,
        )?
        .filter(col("b").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.b <= Int64(1)\
            \n  Inner Join: test.a = test2.a\
            \n    Projection: test.a, test.b\
            \n      TableScan: test\
            \n    Projection: test2.a, test2.c\
            \n      TableScan: test2"
    );

    let expected = "\
        Inner Join: test.a = test2.a\
        \n  Projection: test.a, test.b\
        \n    TableScan: test, full_filters=[test.b <= Int64(1)]\
        \n  Projection: test2.a, test2.c\
        \n    TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// post-join predicates on the right side of a left join are not duplicated
/// TODO: In this case we can sometimes convert the join to an INNER join
#[test]
fn filter_using_left_join() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join_using(
            right,
            JoinType::Left,
            vec![Column::from_name("a".to_string())],
        )?
        .filter(col("test2.a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test2.a <= Int64(1)\
            \n  Left Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter not duplicated nor pushed down - i.e. noop
    let expected = "\
        Filter: test2.a <= Int64(1)\
        \n  Left Join: Using test.a = test2.a\
        \n    TableScan: test\
        \n    Projection: test2.a\
        \n      TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// post-join predicates on the left side of a right join are not duplicated
#[test]
fn filter_using_right_join() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join_using(
            right,
            JoinType::Right,
            vec![Column::from_name("a".to_string())],
        )?
        .filter(col("test.a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.a <= Int64(1)\
            \n  Right Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter not duplicated nor pushed down - i.e. noop
    let expected = "\
        Filter: test.a <= Int64(1)\
        \n  Right Join: Using test.a = test2.a\
        \n    TableScan: test\
        \n    Projection: test2.a\
        \n      TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// post-left-join predicate on a column common to both sides is only pushed to the left side
/// i.e. - not duplicated to the right side
#[test]
fn filter_using_left_join_on_common() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join_using(
            right,
            JoinType::Left,
            vec![Column::from_name("a".to_string())],
        )?
        .filter(col("a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test.a <= Int64(1)\
            \n  Left Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter sent to left side of the join, not the right
    let expected = "\
        Left Join: Using test.a = test2.a\
        \n  TableScan: test, full_filters=[test.a <= Int64(1)]\
        \n  Projection: test2.a\
        \n    TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// post-right-join predicate on a column common to both sides is only pushed to the right side
/// i.e. - not duplicated to the left side.
#[test]
fn filter_using_right_join_on_common() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join_using(
            right,
            JoinType::Right,
            vec![Column::from_name("a".to_string())],
        )?
        .filter(col("test2.a").lt_eq(lit(1i64)))?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Filter: test2.a <= Int64(1)\
            \n  Right Join: Using test.a = test2.a\
            \n    TableScan: test\
            \n    Projection: test2.a\
            \n      TableScan: test2"
    );

    // filter sent to right side of join, not duplicated to the left
    let expected = "\
        Right Join: Using test.a = test2.a\
        \n  TableScan: test\
        \n  Projection: test2.a\
        \n    TableScan: test2, full_filters=[test2.a <= Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// single table predicate parts of ON condition should be pushed to both inputs
#[test]
fn join_on_with_filter() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = col("test.c")
        .gt(lit(1u32))
        .and(col("test.b").lt(col("test2.b")))
        .and(col("test2.c").gt(lit(4u32)));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "Inner Join: test.a = test2.a Filter: test.c > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

    let expected = "\
        Inner Join: test.a = test2.a Filter: test.b < test2.b\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test, full_filters=[test.c > UInt32(1)]\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    TableScan: test2, full_filters=[test2.c > UInt32(4)]";
    assert_optimized_plan_eq(plan, expected)
}

/// join filter should be completely removed after pushdown
#[test]
fn join_filter_removed() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = col("test.b")
        .gt(lit(1u32))
        .and(col("test2.c").gt(lit(4u32)));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Inner Join: test.a = test2.a Filter: test.b > UInt32(1) AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
    );

    let expected = "\
        Inner Join: test.a = test2.a\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test, full_filters=[test.b > UInt32(1)]\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    TableScan: test2, full_filters=[test2.c > UInt32(4)]";
    assert_optimized_plan_eq(plan, expected)
}

/// predicate on join key in filter expression should be pushed down to both inputs
#[test]
fn join_filter_on_common() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("b")])?
        .build()?;
    let filter = col("test.a").gt(lit(1u32));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("a")], vec![Column::from_name("b")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
        format!("{plan:?}"),
        "Inner Join: test.a = test2.b Filter: test.a > UInt32(1)\
            \n  Projection: test.a\
            \n    TableScan: test\
            \n  Projection: test2.b\
            \n    TableScan: test2"
    );

    let expected = "\
        Inner Join: test.a = test2.b\
        \n  Projection: test.a\
        \n    TableScan: test, full_filters=[test.a > UInt32(1)]\
        \n  Projection: test2.b\
        \n    TableScan: test2, full_filters=[test2.b > UInt32(1)]";
    assert_optimized_plan_eq(plan, expected)
}

/// single table predicate parts of ON condition should be pushed to right input
#[test]
fn left_join_on_with_filter() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = col("test.a")
        .gt(lit(1u32))
        .and(col("test.b").lt(col("test2.b")))
        .and(col("test2.c").gt(lit(4u32)));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Left,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

    let expected = "\
        Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    TableScan: test2, full_filters=[test2.c > UInt32(4)]";
    assert_optimized_plan_eq(plan, expected)
}

/// single table predicate parts of ON condition should be pushed to left input
#[test]
fn right_join_on_with_filter() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = col("test.a")
        .gt(lit(1u32))
        .and(col("test.b").lt(col("test2.b")))
        .and(col("test2.c").gt(lit(4u32)));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Right,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "Right Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

    let expected = "\
        Right Join: test.a = test2.a Filter: test.b < test2.b AND test2.c > UInt32(4)\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test, full_filters=[test.a > UInt32(1)]\
        \n  Projection: test2.a, test2.b, test2.c\
        \n    TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

/// single table predicate parts of ON condition should not be pushed
#[test]
fn full_join_on_with_filter() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let filter = col("test.a")
        .gt(lit(1u32))
        .and(col("test.b").lt(col("test2.b")))
        .and(col("test2.c").gt(lit(4u32)));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Full,
            (vec![Column::from_name("a")], vec![Column::from_name("a")]),
            Some(filter),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "Full Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)\
            \n  Projection: test.a, test.b, test.c\
            \n    TableScan: test\
            \n  Projection: test2.a, test2.b, test2.c\
            \n    TableScan: test2"
        );

    let expected = &format!("{plan:?}");
    assert_optimized_plan_eq(plan, expected)
}

struct PushDownProvider {
    pub filter_support: TableProviderFilterPushDown,
}

#[async_trait]
impl TableSource for PushDownProvider {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(&self, _e: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(self.filter_support.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn table_scan_with_pushdown_provider(
    filter_support: TableProviderFilterPushDown,
) -> Result<LogicalPlan> {
    let test_provider = PushDownProvider { filter_support };

    let table_scan = LogicalPlan::TableScan(TableScan {
        table_name: "test".into(),
        filters: vec![],
        projected_schema: Arc::new(DFSchema::try_from(
            (*test_provider.schema()).clone(),
        )?),
        projection: None,
        source: Arc::new(test_provider),
        fetch: None,
    });

    LogicalPlanBuilder::from(table_scan)
        .filter(col("a").eq(lit(1i64)))?
        .build()
}

#[test]
fn filter_with_table_provider_exact() -> Result<()> {
    let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Exact)?;

    let expected = "\
        TableScan: test, full_filters=[a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_with_table_provider_inexact() -> Result<()> {
    let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

    let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test, partial_filters=[a = Int64(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn filter_with_table_provider_multiple_invocations() -> Result<()> {
    let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

    let optimized_plan = PushDownFilter::new()
        .rewrite(plan, &OptimizerContext::new())
        .expect("failed to optimize plan")
        .data;

    let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test, partial_filters=[a = Int64(1)]";

    // Optimizing the same plan multiple times should produce the same plan
    // each time.
    assert_optimized_plan_eq(optimized_plan, expected)
}

#[test]
fn filter_with_table_provider_unsupported() -> Result<()> {
    let plan =
        table_scan_with_pushdown_provider(TableProviderFilterPushDown::Unsupported)?;

    let expected = "\
        Filter: a = Int64(1)\
        \n  TableScan: test";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn multi_combined_filter() -> Result<()> {
    let test_provider = PushDownProvider {
        filter_support: TableProviderFilterPushDown::Inexact,
    };

    let table_scan = LogicalPlan::TableScan(TableScan {
        table_name: "test".into(),
        filters: vec![col("a").eq(lit(10i64)), col("b").gt(lit(11i64))],
        projected_schema: Arc::new(DFSchema::try_from(
            (*test_provider.schema()).clone(),
        )?),
        projection: Some(vec![0]),
        source: Arc::new(test_provider),
        fetch: None,
    });

    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
        .project(vec![col("a"), col("b")])?
        .build()?;

    let expected = "Projection: a, b\
            \n  Filter: a = Int64(10) AND b > Int64(11)\
            \n    TableScan: test projection=[a], partial_filters=[a = Int64(10), b > Int64(11)]";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn multi_combined_filter_exact() -> Result<()> {
    let test_provider = PushDownProvider {
        filter_support: TableProviderFilterPushDown::Exact,
    };

    let table_scan = LogicalPlan::TableScan(TableScan {
        table_name: "test".into(),
        filters: vec![],
        projected_schema: Arc::new(DFSchema::try_from(
            (*test_provider.schema()).clone(),
        )?),
        projection: Some(vec![0]),
        source: Arc::new(test_provider),
        fetch: None,
    });

    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
        .project(vec![col("a"), col("b")])?
        .build()?;

    let expected = r#"
Projection: a, b
  TableScan: test projection=[a], full_filters=[a = Int64(10), b > Int64(11)]
        "#
    .trim();

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_filter_with_alias() -> Result<()> {
    // in table scan the true col name is 'test.a',
    // but we rename it as 'b', and use col 'b' in filter
    // we need rewrite filter col before push down.
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
        .build()?;

    // filter on col b
    assert_eq!(
        format!("{plan:?}"),
        "Filter: b > Int64(10) AND test.c > Int64(10)\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test"
    );

    // rewrite filter col b to test.a
    let expected = "\
            Projection: test.a AS b, test.c\
            \n  TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]\
            ";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_filter_with_alias_2() -> Result<()> {
    // in table scan the true col name is 'test.a',
    // but we rename it as 'b', and use col 'b' in filter
    // we need rewrite filter col before push down.
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .project(vec![col("b"), col("c")])?
        .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
        .build()?;

    // filter on col b
    assert_eq!(
        format!("{plan:?}"),
        "Filter: b > Int64(10) AND test.c > Int64(10)\
            \n  Projection: b, test.c\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test\
            "
    );

    // rewrite filter col b to test.a
    let expected = "\
            Projection: b, test.c\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]\
            ";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_filter_with_multi_alias() -> Result<()> {
    let table_scan = test_table_scan()?;
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c").alias("d")])?
        .filter(and(col("b").gt(lit(10i64)), col("d").gt(lit(10i64))))?
        .build()?;

    // filter on col b and d
    assert_eq!(
        format!("{plan:?}"),
        "Filter: b > Int64(10) AND d > Int64(10)\
            \n  Projection: test.a AS b, test.c AS d\
            \n    TableScan: test\
            "
    );

    // rewrite filter col b to test.a, col d to test.c
    let expected = "\
            Projection: test.a AS b, test.c AS d\
            \n  TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]";

    assert_optimized_plan_eq(plan, expected)
}

/// predicate on join key in filter expression should be pushed down to both inputs
#[test]
fn join_filter_with_alias() -> Result<()> {
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("b").alias("d")])?
        .build()?;
    let filter = col("c").gt(lit(1u32));
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec![Column::from_name("c")], vec![Column::from_name("d")]),
            Some(filter),
        )?
        .build()?;

    assert_eq!(
        format!("{plan:?}"),
        "Inner Join: c = d Filter: c > UInt32(1)\
            \n  Projection: test.a AS c\
            \n    TableScan: test\
            \n  Projection: test2.b AS d\
            \n    TableScan: test2"
    );

    // Change filter on col `c`, 'd' to `test.a`, 'test.b'
    let expected = "\
        Inner Join: c = d\
        \n  Projection: test.a AS c\
        \n    TableScan: test, full_filters=[test.a > UInt32(1)]\
        \n  Projection: test2.b AS d\
        \n    TableScan: test2, full_filters=[test2.b > UInt32(1)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_in_filter_with_alias() -> Result<()> {
    // in table scan the true col name is 'test.a',
    // but we rename it as 'b', and use col 'b' in filter
    // we need rewrite filter col before push down.
    let table_scan = test_table_scan()?;
    let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .filter(in_list(col("b"), filter_value, false))?
        .build()?;

    // filter on col b
    assert_eq!(
        format!("{plan:?}"),
        "Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test\
            "
    );

    // rewrite filter col b to test.a
    let expected = "\
            Projection: test.a AS b, test.c\
            \n  TableScan: test, full_filters=[test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])]";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_in_filter_with_alias_2() -> Result<()> {
    // in table scan the true col name is 'test.a',
    // but we rename it as 'b', and use col 'b' in filter
    // we need rewrite filter col before push down.
    let table_scan = test_table_scan()?;
    let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .project(vec![col("b"), col("c")])?
        .filter(in_list(col("b"), filter_value, false))?
        .build()?;

    // filter on col b
    assert_eq!(
        format!("{plan:?}"),
        "Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])\
            \n  Projection: b, test.c\
            \n    Projection: test.a AS b, test.c\
            \n      TableScan: test\
            "
    );

    // rewrite filter col b to test.a
    let expected = "\
            Projection: b, test.c\
            \n  Projection: test.a AS b, test.c\
            \n    TableScan: test, full_filters=[test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])]";

    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn test_in_subquery_with_alias() -> Result<()> {
    // in table scan the true col name is 'test.a',
    // but we rename it as 'b', and use col 'b' in subquery filter
    let table_scan = test_table_scan()?;
    let table_scan_sq = test_table_scan_with_name("sq")?;
    let subplan = Arc::new(
        LogicalPlanBuilder::from(table_scan_sq)
            .project(vec![col("c")])?
            .build()?,
    );
    let plan = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a").alias("b"), col("c")])?
        .filter(in_subquery(col("b"), subplan))?
        .build()?;

    // filter on col b in subquery
    let expected_before = "\
        Filter: b IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: sq.c\
        \n      TableScan: sq\
        \n  Projection: test.a AS b, test.c\
        \n    TableScan: test";
    assert_eq!(format!("{plan:?}"), expected_before);

    // rewrite filter col b to test.a
    let expected_after = "\
        Projection: test.a AS b, test.c\
        \n  TableScan: test, full_filters=[test.a IN (<subquery>)]\
        \n    Subquery:\
        \n      Projection: sq.c\
        \n        TableScan: sq";
    assert_optimized_plan_eq(plan, expected_after)
}

#[test]
fn test_propagation_of_optimized_inner_filters_with_projections() -> Result<()> {
    // SELECT a FROM (SELECT 1 AS a) b WHERE b.a = 1
    let plan = LogicalPlanBuilder::empty(true)
        .project(vec![lit(0i64).alias("a")])?
        .alias("b")?
        .project(vec![col("b.a")])?
        .alias("b")?
        .filter(col("b.a").eq(lit(1i64)))?
        .project(vec![col("b.a")])?
        .build()?;

    let expected_before = "Projection: b.a\
        \n  Filter: b.a = Int64(1)\
        \n    SubqueryAlias: b\
        \n      Projection: b.a\
        \n        SubqueryAlias: b\
        \n          Projection: Int64(0) AS a\
        \n            EmptyRelation";
    assert_eq!(format!("{plan:?}"), expected_before);

    // Ensure that the predicate without any columns (0 = 1) is
    // still there.
    let expected_after = "Projection: b.a\
        \n  SubqueryAlias: b\
        \n    Projection: b.a\
        \n      SubqueryAlias: b\
        \n        Projection: Int64(0) AS a\
        \n          Filter: Int64(0) = Int64(1)\
        \n            EmptyRelation";
    assert_optimized_plan_eq(plan, expected_after)
}

#[test]
fn test_crossjoin_with_or_clause() -> Result<()> {
    // select * from test,test1 where (test.a = test1.a and test.b > 1) or (test.b = test1.b and test.c < 10);
    let table_scan = test_table_scan()?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b"), col("c")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test1")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a").alias("d"), col("a").alias("e")])?
        .build()?;
    let filter = or(
        and(col("a").eq(col("d")), col("b").gt(lit(1u32))),
        and(col("b").eq(col("e")), col("c").lt(lit(10u32))),
    );
    let plan = LogicalPlanBuilder::from(left)
        .cross_join(right)?
        .filter(filter)?
        .build()?;
    let expected = "\
        Inner Join:  Filter: test.a = d AND test.b > UInt32(1) OR test.b = e AND test.c < UInt32(10)\
        \n  Projection: test.a, test.b, test.c\
        \n    TableScan: test, full_filters=[test.b > UInt32(1) OR test.c < UInt32(10)]\
        \n  Projection: test1.a AS d, test1.a AS e\
        \n    TableScan: test1";
    assert_optimized_plan_eq_with_rewrite_predicate(plan.clone(), expected)?;

    // Originally global state which can help to avoid duplicate Filters been generated and pushed down.
    // Now the global state is removed. Need to double confirm that avoid duplicate Filters.
    let optimized_plan = PushDownFilter::new()
        .rewrite(plan, &OptimizerContext::new())
        .expect("failed to optimize plan")
        .data;
    assert_optimized_plan_eq(optimized_plan, expected)
}

#[test]
fn left_semi_join_with_filters() -> Result<()> {
    let left = test_table_scan_with_name("test1")?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::LeftSemi,
            (
                vec![Column::from_qualified_name("test1.a")],
                vec![Column::from_qualified_name("test2.a")],
            ),
            Some(
                col("test1.b")
                    .gt(lit(1u32))
                    .and(col("test2.b").gt(lit(2u32))),
            ),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "LeftSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

    // Both side will be pushed down.
    let expected = "\
        LeftSemi Join: test1.a = test2.a\
        \n  TableScan: test1, full_filters=[test1.b > UInt32(1)]\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2, full_filters=[test2.b > UInt32(2)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn right_semi_join_with_filters() -> Result<()> {
    let left = test_table_scan_with_name("test1")?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::RightSemi,
            (
                vec![Column::from_qualified_name("test1.a")],
                vec![Column::from_qualified_name("test2.a")],
            ),
            Some(
                col("test1.b")
                    .gt(lit(1u32))
                    .and(col("test2.b").gt(lit(2u32))),
            ),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "RightSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

    // Both side will be pushed down.
    let expected = "\
        RightSemi Join: test1.a = test2.a\
        \n  TableScan: test1, full_filters=[test1.b > UInt32(1)]\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2, full_filters=[test2.b > UInt32(2)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn left_anti_join_with_filters() -> Result<()> {
    let table_scan = test_table_scan_with_name("test1")?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::LeftAnti,
            (
                vec![Column::from_qualified_name("test1.a")],
                vec![Column::from_qualified_name("test2.a")],
            ),
            Some(
                col("test1.b")
                    .gt(lit(1u32))
                    .and(col("test2.b").gt(lit(2u32))),
            ),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  Projection: test1.a, test1.b\
            \n    TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

    // For left anti, filter of the right side filter can be pushed down.
    let expected = "\
        LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1)\
        \n  Projection: test1.a, test1.b\
        \n    TableScan: test1\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2, full_filters=[test2.b > UInt32(2)]";
    assert_optimized_plan_eq(plan, expected)
}

#[test]
fn right_anti_join_with_filters() -> Result<()> {
    let table_scan = test_table_scan_with_name("test1")?;
    let left = LogicalPlanBuilder::from(table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan)
        .project(vec![col("a"), col("b")])?
        .build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::RightAnti,
            (
                vec![Column::from_qualified_name("test1.a")],
                vec![Column::from_qualified_name("test2.a")],
            ),
            Some(
                col("test1.b")
                    .gt(lit(1u32))
                    .and(col("test2.b").gt(lit(2u32))),
            ),
        )?
        .build()?;

    // not part of the test, just good to know:
    assert_eq!(
            format!("{plan:?}"),
            "RightAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)\
            \n  Projection: test1.a, test1.b\
            \n    TableScan: test1\
            \n  Projection: test2.a, test2.b\
            \n    TableScan: test2",
        );

    // For right anti, filter of the left side can be pushed down.
    let expected = "RightAnti Join: test1.a = test2.a Filter: test2.b > UInt32(2)\
        \n  Projection: test1.a, test1.b\
        \n    TableScan: test1, full_filters=[test1.b > UInt32(1)]\
        \n  Projection: test2.a, test2.b\
        \n    TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}

#[derive(Debug)]
struct TestScalarUDF {
    signature: Signature,
}

impl ScalarUDFImpl for TestScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "TestScalarUDF"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::from(1)))
    }
}

#[test]
fn test_push_down_volatile_function_in_aggregate() -> Result<()> {
    // SELECT t.a, t.r FROM (SELECT a, SUM(b),  TestScalarUDF()+1 AS r FROM test1 GROUP BY a) AS t WHERE t.a > 5 AND t.r > 0.5;
    let table_scan = test_table_scan_with_name("test1")?;
    let fun = ScalarUDF::new_from_impl(TestScalarUDF {
        signature: Signature::exact(vec![], Volatility::Volatile),
    });
    let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));

    let plan = LogicalPlanBuilder::from(table_scan)
        .aggregate(vec![col("a")], vec![sum(col("b"))])?
        .project(vec![col("a"), sum(col("b")), add(expr, lit(1)).alias("r")])?
        .alias("t")?
        .filter(col("t.a").gt(lit(5)).and(col("t.r").gt(lit(0.5))))?
        .project(vec![col("t.a"), col("t.r")])?
        .build()?;

    let expected_before = "Projection: t.a, t.r\
        \n  Filter: t.a > Int32(5) AND t.r > Float64(0.5)\
        \n    SubqueryAlias: t\
        \n      Projection: test1.a, SUM(test1.b), TestScalarUDF() + Int32(1) AS r\
        \n        Aggregate: groupBy=[[test1.a]], aggr=[[SUM(test1.b)]]\
        \n          TableScan: test1";
    assert_eq!(format!("{plan:?}"), expected_before);

    let expected_after = "Projection: t.a, t.r\
        \n  SubqueryAlias: t\
        \n    Filter: r > Float64(0.5)\
        \n      Projection: test1.a, SUM(test1.b), TestScalarUDF() + Int32(1) AS r\
        \n        Aggregate: groupBy=[[test1.a]], aggr=[[SUM(test1.b)]]\
        \n          TableScan: test1, full_filters=[test1.a > Int32(5)]";
    assert_optimized_plan_eq(plan, expected_after)
}

#[test]
fn test_push_down_volatile_function_in_join() -> Result<()> {
    // SELECT t.a, t.r FROM (SELECT test1.a AS a, TestScalarUDF() AS r FROM test1 join test2 ON test1.a = test2.a) AS t WHERE t.r > 0.5;
    let table_scan = test_table_scan_with_name("test1")?;
    let fun = ScalarUDF::new_from_impl(TestScalarUDF {
        signature: Signature::exact(vec![], Volatility::Volatile),
    });
    let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));
    let left = LogicalPlanBuilder::from(table_scan).build()?;
    let right_table_scan = test_table_scan_with_name("test2")?;
    let right = LogicalPlanBuilder::from(right_table_scan).build()?;
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (
                vec![Column::from_qualified_name("test1.a")],
                vec![Column::from_qualified_name("test2.a")],
            ),
            None,
        )?
        .project(vec![col("test1.a").alias("a"), expr.alias("r")])?
        .alias("t")?
        .filter(col("t.r").gt(lit(0.8)))?
        .project(vec![col("t.a"), col("t.r")])?
        .build()?;

    let expected_before = "Projection: t.a, t.r\
        \n  Filter: t.r > Float64(0.8)\
        \n    SubqueryAlias: t\
        \n      Projection: test1.a AS a, TestScalarUDF() AS r\
        \n        Inner Join: test1.a = test2.a\
        \n          TableScan: test1\
        \n          TableScan: test2";
    assert_eq!(format!("{plan:?}"), expected_before);

    let expected = "Projection: t.a, t.r\
        \n  SubqueryAlias: t\
        \n    Filter: r > Float64(0.8)\
        \n      Projection: test1.a AS a, TestScalarUDF() AS r\
        \n        Inner Join: test1.a = test2.a\
        \n          TableScan: test1\
        \n          TableScan: test2";
    assert_optimized_plan_eq(plan, expected)
}
