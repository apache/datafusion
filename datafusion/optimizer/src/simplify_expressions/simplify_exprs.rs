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

//! Simplify expressions optimizer rule and implementation

use std::sync::Arc;

use datafusion_common::{DFSchema, DFSchemaRef, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::utils::merge_schema;
use datafusion_physical_expr::execution_props::ExecutionProps;

use crate::{OptimizerConfig, OptimizerRule};

use super::{ExprSimplifier, SimplifyContext};

/// Optimizer Pass that simplifies [`LogicalPlan`]s by rewriting
/// [`Expr`]`s evaluating constants and applying algebraic
/// simplifications
///
/// # Introduction
/// It uses boolean algebra laws to simplify or reduce the number of terms in expressions.
///
/// # Example:
/// `Filter: b > 2 AND b > 2`
/// is optimized to
/// `Filter: b > 2`
///
/// [`Expr`]: datafusion_expr::Expr
#[derive(Default)]
pub struct SimplifyExpressions {}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time = config.query_execution_start_time();
        Ok(Some(Self::optimize_internal(plan, &execution_props)?))
    }
}

impl SimplifyExpressions {
    fn optimize_internal(
        plan: &LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<LogicalPlan> {
        let schema = if !plan.inputs().is_empty() {
            DFSchemaRef::new(merge_schema(plan.inputs()))
        } else if let LogicalPlan::TableScan(scan) = plan {
            // When predicates are pushed into a table scan, there is no input
            // schema to resolve predicates against, so it must be handled specially
            //
            // Note that this is not `plan.schema()` which is the *output*
            // schema, and reflects any pushed down projection. The output schema
            // will not contain columns that *only* appear in pushed down predicates
            // (and no where else) in the plan.
            //
            // Thus, use the full schema of the inner provider without any
            // projection applied for simplification
            Arc::new(DFSchema::try_from_qualified_schema(
                &scan.table_name,
                &scan.source.schema(),
            )?)
        } else {
            Arc::new(DFSchema::empty())
        };
        let info = SimplifyContext::new(execution_props).with_schema(schema);

        let new_inputs = plan
            .inputs()
            .iter()
            .map(|input| Self::optimize_internal(input, execution_props))
            .collect::<Result<Vec<_>>>()?;

        let simplifier = ExprSimplifier::new(info);

        // The left and right expressions in a Join on clause are not
        // commutative, for reasons that are not entirely clear. Thus, do not
        // reorder expressions in Join while simplifying.
        //
        // This is likely related to the fact that order of the columns must
        // match the order of the children. see
        // https://github.com/apache/arrow-datafusion/pull/8780 for more details
        let simplifier = if let LogicalPlan::Join(_) = plan {
            simplifier.with_canonicalize(false)
        } else {
            simplifier
        };

        let exprs = plan
            .expressions()
            .into_iter()
            .map(|e| {
                // TODO: unify with `rewrite_preserving_name`
                let original_name = e.name_for_alias()?;
                let new_e = simplifier.simplify(e)?;
                new_e.alias_if_changed(original_name)
            })
            .collect::<Result<Vec<_>>>()?;

        plan.with_new_exprs(exprs, new_inputs)
    }
}

impl SimplifyExpressions {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::{DateTime, Utc};

    use datafusion_expr::logical_plan::builder::table_scan_with_filters;
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        and, binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, Expr,
        ExprSchemable, JoinType,
    };
    use datafusion_expr::{call_fn, or, BinaryExpr, Cast, Operator};

    use crate::simplify_expressions::utils::for_test::now_expr;
    use crate::test::{assert_fields_eq, test_table_scan_with_name};
    use crate::OptimizerContext;

    use super::*;

    fn test_table_scan() -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Boolean, false),
            Field::new("d", DataType::UInt32, false),
            Field::new("e", DataType::UInt32, true),
        ]);
        table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan")
    }

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let rule = SimplifyExpressions::new();
        let optimized_plan = rule
            .try_optimize(plan, &OptimizerContext::new())
            .unwrap()
            .expect("failed to optimize plan");
        let formatted_plan = format!("{optimized_plan:?}");
        assert_eq!(formatted_plan, expected);
        Ok(())
    }

    #[test]
    fn test_simplify_table_full_filter_in_scan() -> Result<()> {
        let fields = vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ];

        let schema = Schema::new(fields);

        let table_scan = table_scan_with_filters(
            Some("test"),
            &schema,
            Some(vec![0]),
            vec![col("b").is_not_null()],
        )?
        .build()?;
        assert_eq!(1, table_scan.schema().fields().len());
        assert_fields_eq(&table_scan, vec!["a"]);

        let expected = "TableScan: test projection=[a], full_filters=[Boolean(true) AS b IS NOT NULL]";

        assert_optimized_plan_eq(&table_scan, expected)
    }

    #[test]
    fn test_simplify_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
	        Filter: test.b > Int32(1)\
            \n  Projection: test.a\
            \n    TableScan: test",
        )
    }

    #[test]
    fn test_simplify_optimized_plan() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
	        Filter: test.b > Int32(1)\
            \n  Projection: test.a\
            \n    TableScan: test",
        )
    }

    #[test]
    fn test_simplify_optimized_plan_with_or() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(or(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: test.b > Int32(1)\
            \n  Projection: test.a\
            \n    TableScan: test",
        )
    }

    #[test]
    fn test_simplify_optimized_plan_with_composed_and() -> Result<()> {
        let table_scan = test_table_scan();
        // ((c > 5) AND (d < 6)) AND (c > 5) --> (c > 5) AND (d < 6)
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(and(
                and(col("a").gt(lit(5)), col("b").lt(lit(6))),
                col("a").gt(lit(5)),
            ))?
            .build()?;

        assert_optimized_plan_eq(
            &plan,
            "\
            Filter: test.a > Int32(5) AND test.b < Int32(6)\
            \n  Projection: test.a, test.b\
	        \n    TableScan: test",
        )
    }

    #[test]
    fn test_simplify_optimized_plan_eq_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(true)))?
            .filter(col("c").eq(lit(false)))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.c\
        \n    Filter: test.b\
        \n      TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_not_eq_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)))?
            .filter(col("c").not_eq(lit(false)))?
            .limit(0, Some(1))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: test.a\
        \n  Limit: skip=0, fetch=1\
        \n    Filter: test.c\
        \n      Filter: NOT test.b\
        \n        TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_and_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.b AND test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_or_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: test.a\
        \n  Filter: NOT test.b OR NOT test.c\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_not_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(false)).not())?
            .project(vec![col("a")])?
            .build()?;

        let expected = "\
        Projection: test.a\
        \n  Filter: test.b\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_support_projection() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])?
            .build()?;

        let expected = "\
        Projection: test.a, test.d, NOT test.b AS test.b = Boolean(false)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_support_aggregate() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c"), col("b")])?
            .aggregate(
                vec![col("a"), col("c")],
                vec![
                    datafusion_expr::max(col("b").eq(lit(true))),
                    datafusion_expr::min(col("b")),
                ],
            )?
            .build()?;

        let expected = "\
        Aggregate: groupBy=[[test.a, test.c]], aggr=[[MAX(test.b) AS MAX(test.b = Boolean(true)), MIN(test.b)]]\
        \n  Projection: test.a, test.c, test.b\
        \n    TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn test_simplify_optimized_plan_support_values() -> Result<()> {
        let expr1 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(1)),
            Operator::Plus,
            Box::new(lit(2)),
        ));
        let expr2 = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(2)),
            Operator::Minus,
            Box::new(lit(1)),
        ));
        let values = vec![vec![expr1, expr2]];
        let plan = LogicalPlanBuilder::values(values)?.build()?;

        let expected = "\
        Values: (Int32(3) AS Int32(1) + Int32(2), Int32(1) AS Int32(2) - Int32(1))";

        assert_optimized_plan_eq(&plan, expected)
    }

    fn get_optimized_plan_formatted(
        plan: &LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> String {
        let config = OptimizerContext::new().with_query_execution_start_time(*date_time);
        let rule = SimplifyExpressions::new();

        let optimized_plan = rule
            .try_optimize(plan, &config)
            .unwrap()
            .expect("failed to optimize plan");
        format!("{optimized_plan:?}")
    }

    #[test]
    fn cast_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let proj = vec![Expr::Cast(Cast::new(Box::new(lit("0")), DataType::Int32))];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)?
            .build()?;

        let expected = "Projection: Int32(0) AS Utf8(\"0\")\
            \n  TableScan: test";
        let actual = get_optimized_plan_formatted(&plan, &Utc::now());
        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn multiple_now_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let time = Utc::now();
        let proj = vec![now_expr(), now_expr().alias("t2")];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)?
            .build()?;

        // expect the same timestamp appears in both exprs
        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected = format!(
            "Projection: TimestampNanosecond({}, Some(\"+00:00\")) AS now(), TimestampNanosecond({}, Some(\"+00:00\")) AS t2\
            \n  TableScan: test",
            time.timestamp_nanos_opt().unwrap(),
            time.timestamp_nanos_opt().unwrap()
        );

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn simplify_and_eval() -> Result<()> {
        // demonstrate a case where the evaluation needs to run prior
        // to the simplifier for it to work
        let table_scan = test_table_scan();
        let time = Utc::now();
        // (true or false) != col --> !col
        let proj = vec![lit(true).or(lit(false)).not_eq(col("a"))];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(proj)?
            .build()?;

        let actual = get_optimized_plan_formatted(&plan, &time);
        let expected =
            "Projection: NOT test.a AS Boolean(true) OR Boolean(false) != test.a\
                        \n  TableScan: test";

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn simplify_not_binary() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not())?
            .build()?;
        let expected = "Filter: test.d <= Int32(10)\
            \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_bool_and() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).and(col("d").lt(lit(100))).not())?
            .build()?;
        let expected = "Filter: test.d <= Int32(10) OR test.d >= Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_bool_or() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).or(col("d").lt(lit(100))).not())?
            .build()?;
        let expected = "Filter: test.d <= Int32(10) AND test.d >= Int32(100)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not().not())?
            .build()?;
        let expected = "Filter: test.d > Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("e").is_null().not())?
            .build()?;
        let expected = "Filter: test.e IS NOT NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("e").is_not_null().not())?
            .build()?;
        let expected = "Filter: test.e IS NULL\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_in() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], false).not())?
            .build()?;
        let expected =
            "Filter: test.d != Int32(1) AND test.d != Int32(2) AND test.d != Int32(3)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not_in() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], true).not())?
            .build()?;
        let expected =
            "Filter: test.d = Int32(1) OR test.d = Int32(2) OR test.d = Int32(3)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_between() -> Result<()> {
        let table_scan = test_table_scan();
        let qual = col("d").between(lit(1), lit(10));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())?
            .build()?;
        let expected = "Filter: test.d < Int32(1) OR test.d > Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not_between() -> Result<()> {
        let table_scan = test_table_scan();
        let qual = col("d").not_between(lit(1), lit(10));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())?
            .build()?;
        let expected = "Filter: test.d >= Int32(1) AND test.d <= Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_like() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").like(col("b")).not())?
            .build()?;
        let expected = "Filter: test.a NOT LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not_like() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").not_like(col("b")).not())?
            .build()?;
        let expected = "Filter: test.a LIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_ilike() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)
            .expect("creating scan")
            .build()
            .expect("building plan");

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").ilike(col("b")).not())?
            .build()?;
        let expected = "Filter: test.a NOT ILIKE test.b\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_distinct_from() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsDistinctFrom, lit(10)).not())?
            .build()?;
        let expected = "Filter: test.d IS NOT DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_not_not_distinct_from() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsNotDistinctFrom, lit(10)).not())?
            .build()?;
        let expected = "Filter: test.d IS DISTINCT FROM Int32(10)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_equijoin_predicate() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        let left_key = col("t1.a") + lit(1i64).cast_to(&DataType::UInt32, t1.schema())?;
        let right_key =
            col("t2.a") + lit(2i64).cast_to(&DataType::UInt32, t2.schema())?;
        let plan = LogicalPlanBuilder::from(t1)
            .join_with_expr_keys(
                t2,
                JoinType::Inner,
                (vec![left_key], vec![right_key]),
                None,
            )?
            .build()?;

        // before simplify: t1.a + CAST(Int64(1), UInt32) = t2.a + CAST(Int64(2), UInt32)
        // after simplify: t1.a + UInt32(1) = t2.a + UInt32(2) AS t1.a + Int64(1) = t2.a + Int64(2)
        let expected = "Inner Join: t1.a + UInt32(1) = t2.a + UInt32(2)\
            \n  TableScan: t1\
            \n  TableScan: t2";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_project_scalar_fn() -> Result<()> {
        // Issue https://github.com/apache/arrow-datafusion/issues/5996
        let schema = Schema::new(vec![Field::new("f", DataType::Float64, false)]);
        let plan = table_scan(Some("test"), &schema, None)?
            .project(vec![call_fn("power", vec![col("f"), lit(1.0)])?])?
            .build()?;

        // before simplify: power(t.f, 1.0)
        // after simplify:  t.f as "power(t.f, 1.0)"
        let expected = "Projection: test.f AS power(test.f,Float64(1))\
                      \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_scan_predicate() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("f", DataType::Float64, false),
            Field::new("g", DataType::Float64, false),
        ]);
        let plan = table_scan_with_filters(
            Some("test"),
            &schema,
            None,
            vec![col("g").eq(call_fn("power", vec![col("f"), lit(1.0)])?)],
        )?
        .build()?;

        // before simplify: t.g = power(t.f, 1.0)
        // after simplify:  (t.g = t.f) as "t.g = power(t.f, 1.0)"
        let expected = "TableScan: test, full_filters=[g = f AS g = power(f,Float64(1))]";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_is_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_not_null())?
            .build()?;
        let expected = "Filter: Boolean(true)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn simplify_is_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_null())?
            .build()?;
        let expected = "Filter: Boolean(false)\
        \n  TableScan: test";

        assert_optimized_plan_eq(&plan, expected)
    }
}
