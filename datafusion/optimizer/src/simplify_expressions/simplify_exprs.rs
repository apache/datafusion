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

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::Expr;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::merge_schema;

use crate::optimizer::ApplyOrder;
use crate::utils::NamePreserver;
use crate::{OptimizerConfig, OptimizerRule};

use super::ExprSimplifier;

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
#[derive(Default, Debug)]
pub struct SimplifyExpressions {}

impl OptimizerRule for SimplifyExpressions {
    fn name(&self) -> &str {
        "simplify_expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let mut execution_props = ExecutionProps::new();
        execution_props.query_execution_start_time = config.query_execution_start_time();
        execution_props.config_options = Some(config.options());
        Self::optimize_internal(plan, &execution_props)
    }
}

impl SimplifyExpressions {
    fn optimize_internal(
        plan: LogicalPlan,
        execution_props: &ExecutionProps,
    ) -> Result<Transformed<LogicalPlan>> {
        let schema = if !plan.inputs().is_empty() {
            DFSchemaRef::new(merge_schema(&plan.inputs()))
        } else if let LogicalPlan::TableScan(scan) = &plan {
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
                scan.table_name.clone(),
                &scan.source.schema(),
            )?)
        } else {
            Arc::new(DFSchema::empty())
        };

        let info = SimplifyContext::new(execution_props).with_schema(schema);

        // Inputs have already been rewritten (due to bottom-up traversal handled by Optimizer)
        // Just need to rewrite our own expressions

        let simplifier = ExprSimplifier::new(info);

        // The left and right expressions in a Join on clause are not
        // commutative, for reasons that are not entirely clear. Thus, do not
        // reorder expressions in Join while simplifying.
        //
        // This is likely related to the fact that order of the columns must
        // match the order of the children. see
        // https://github.com/apache/datafusion/pull/8780 for more details
        let simplifier = if let LogicalPlan::Join(_) = plan {
            simplifier.with_canonicalize(false)
        } else {
            simplifier
        };

        // Preserve expression names to avoid changing the schema of the plan.
        let name_preserver = NamePreserver::new(&plan);
        let mut rewrite_expr = |expr: Expr| {
            let name = name_preserver.save(&expr);
            let expr = simplifier.simplify_with_cycle_count_transformed(expr)?.0;
            Ok(Transformed::new_transformed(
                name.restore(expr.data),
                expr.transformed,
            ))
        };

        plan.map_expressions(|expr| {
            // Preserve the aliasing of grouping sets.
            if let Expr::GroupingSet(_) = &expr {
                expr.map_children(&mut rewrite_expr)
            } else {
                rewrite_expr(expr)
            }
        })
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
    use datafusion_expr::*;
    use datafusion_functions_aggregate::expr_fn::{max, min};

    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::{assert_fields_eq, test_table_scan_with_name};

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

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(SimplifyExpressions::new())];
            let optimizer_ctx = OptimizerContext::new();
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
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

        assert_optimized_plan_equal!(
            table_scan,
            @ r"TableScan: test projection=[a], full_filters=[Boolean(true)]"
        )
    }

    #[test]
    fn test_simplify_filter_pushdown() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.b > Int32(1)
          Projection: test.a
            TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(and(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
            Filter: test.b > Int32(1)
              Projection: test.a
                TableScan: test
            "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_with_or() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(or(col("b").gt(lit(1)), col("b").gt(lit(1))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
            Filter: test.b > Int32(1)
              Projection: test.a
                TableScan: test
            "
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a > Int32(5) AND test.b < Int32(6)
          Projection: test.a, test.b
            TableScan: test
        "
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a
          Filter: NOT test.c
            Filter: test.b
              TableScan: test
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a
          Limit: skip=0, fetch=1
            Filter: test.c
              Filter: NOT test.b
                TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_and_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).and(col("c").eq(lit(true))))?
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a
          Filter: NOT test.b AND test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_or_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").not_eq(lit(true)).or(col("c").eq(lit(false))))?
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a
          Filter: NOT test.b OR NOT test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_not_expr() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").eq(lit(false)).not())?
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a
          Filter: test.b
            TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_support_projection() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("d"), col("b").eq(lit(false))])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Projection: test.a, test.d, NOT test.b AS test.b = Boolean(false)
          TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_optimized_plan_support_aggregate() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c"), col("b")])?
            .aggregate(
                vec![col("a"), col("c")],
                vec![max(col("b").eq(lit(true))), min(col("b"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Aggregate: groupBy=[[test.a, test.c]], aggr=[[max(test.b) AS max(test.b = Boolean(true)), min(test.b)]]
          Projection: test.a, test.c, test.b
            TableScan: test
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @ "Values: (Int32(3) AS Int32(1) + Int32(2), Int32(1) AS Int32(2) - Int32(1))"
        )
    }

    fn get_optimized_plan_formatted(
        plan: LogicalPlan,
        date_time: &DateTime<Utc>,
    ) -> String {
        let config = OptimizerContext::new().with_query_execution_start_time(*date_time);
        let rule = SimplifyExpressions::new();

        let optimized_plan = rule.rewrite(plan, &config).unwrap().data;
        format!("{optimized_plan}")
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
        let actual = get_optimized_plan_formatted(plan, &Utc::now());
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

        let actual = get_optimized_plan_formatted(plan, &time);
        let expected = "Projection: NOT test.a AS Boolean(true) OR Boolean(false) != test.a\
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d <= Int32(10)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_bool_and() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).and(col("d").lt(lit(100))).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d <= Int32(10) OR test.d >= Int32(100)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_bool_or() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).or(col("d").lt(lit(100))).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d <= Int32(10) AND test.d >= Int32(100)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").gt(lit(10)).not().not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d > Int32(10)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("e").is_null().not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.e IS NOT NULL
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("e").is_not_null().not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.e IS NULL
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_in() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], false).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d != Int32(1) AND test.d != Int32(2) AND test.d != Int32(3)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_in() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").in_list(vec![lit(1), lit(2), lit(3)], true).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d = Int32(1) OR test.d = Int32(2) OR test.d = Int32(3)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_between() -> Result<()> {
        let table_scan = test_table_scan();
        let qual = col("d").between(lit(1), lit(10));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d < Int32(1) OR test.d > Int32(10)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_between() -> Result<()> {
        let table_scan = test_table_scan();
        let qual = col("d").not_between(lit(1), lit(10));

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(qual.not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d >= Int32(1) AND test.d <= Int32(10)
          TableScan: test
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a NOT LIKE test.b
          TableScan: test
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a LIKE test.b
          TableScan: test
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a NOT ILIKE test.b
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_distinct_from() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsDistinctFrom, lit(10)).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d IS NOT DISTINCT FROM Int32(10)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_distinct_from() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(binary_expr(col("d"), Operator::IsNotDistinctFrom, lit(10)).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.d IS DISTINCT FROM Int32(10)
          TableScan: test
        "
        )
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
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Inner Join: t1.a + UInt32(1) = t2.a + UInt32(2)
          TableScan: t1
          TableScan: t2
        "
        )
    }

    #[test]
    fn simplify_is_not_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_not_null())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: Boolean(true)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_is_null() -> Result<()> {
        let table_scan = test_table_scan();

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("d").is_null())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: Boolean(false)
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_grouping_sets() -> Result<()> {
        let table_scan = test_table_scan();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                [grouping_set(vec![
                    vec![(lit(42).alias("prev") + lit(1)).alias("age"), col("a")],
                    vec![col("a").or(col("b")).and(lit(1).lt(lit(0))).alias("cond")],
                    vec![col("d").alias("e"), (lit(1) + lit(2))],
                ])],
                [] as [Expr; 0],
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Aggregate: groupBy=[[GROUPING SETS ((Int32(43) AS age, test.a), (Boolean(false) AS cond), (test.d AS e, Int32(3) AS Int32(1) + Int32(2)))]], aggr=[[]]
          TableScan: test
        "
        )
    }

    #[test]
    fn test_simplify_regex_special_cases() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        // Test `= ".*"` transforms to true (except for empty strings)
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(binary_expr(col("a"), Operator::RegexMatch, lit(".*")))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a IS NOT NULL
          TableScan: test
        "
        )?;

        // Test `!= ".*"` transforms to checking if the column is empty
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(binary_expr(col("a"), Operator::RegexNotMatch, lit(".*")))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r#"
        Filter: test.a = Utf8("")
          TableScan: test
        "#
        )?;

        // Test case-insensitive versions

        // Test `=~ ".*"` (case-insensitive) transforms to true (except for empty strings)
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(binary_expr(col("b"), Operator::RegexIMatch, lit(".*")))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: Boolean(true)
          TableScan: test
        "
        )?;

        // Test `!~ ".*"` (case-insensitive) transforms to checking if the column is empty
        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .filter(binary_expr(col("a"), Operator::RegexNotIMatch, lit(".*")))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r#"
        Filter: test.a = Utf8("")
          TableScan: test
        "#
        )
    }

    #[test]
    fn simplify_not_in_list() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").in_list(vec![lit("a"), lit("b")], false).not())?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r#"
        Filter: test.a != Utf8("a") AND test.a != Utf8("b")
          TableScan: test
        "#
        )
    }

    #[test]
    fn simplify_not_not_in_list() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                col("a")
                    .in_list(vec![lit("a"), lit("b")], false)
                    .not()
                    .not(),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r#"
        Filter: test.a = Utf8("a") OR test.a = Utf8("b")
          TableScan: test
        "#
        )
    }

    #[test]
    fn simplify_not_exists() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;
        let table_scan2 =
            datafusion_expr::table_scan(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                exists(Arc::new(LogicalPlanBuilder::from(table_scan2).build()?)).not(),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: NOT EXISTS (<subquery>)
          Subquery:
            TableScan: test2
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_exists() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;
        let table_scan2 =
            datafusion_expr::table_scan(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                exists(Arc::new(LogicalPlanBuilder::from(table_scan2).build()?))
                    .not()
                    .not(),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: EXISTS (<subquery>)
          Subquery:
            TableScan: test2
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_in_subquery() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;
        let table_scan2 =
            datafusion_expr::table_scan(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                in_subquery(
                    col("a"),
                    Arc::new(LogicalPlanBuilder::from(table_scan2).build()?),
                )
                .not(),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a NOT IN (<subquery>)
          Subquery:
            TableScan: test2
          TableScan: test
        "
        )
    }

    #[test]
    fn simplify_not_not_in_subquery() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;
        let table_scan2 =
            datafusion_expr::table_scan(Some("test2"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                in_subquery(
                    col("a"),
                    Arc::new(LogicalPlanBuilder::from(table_scan2).build()?),
                )
                .not()
                .not(),
            )?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Filter: test.a IN (<subquery>)
          Subquery:
            TableScan: test2
          TableScan: test
        "
        )
    }
}
