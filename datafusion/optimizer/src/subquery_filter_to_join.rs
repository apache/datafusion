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

//! Optimizer rule for rewriting subquery filters to joins
//!
//! It handles standalone parts of logical conjunction expressions, i.e.
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) AND t2.f = 'x'
//! ```
//! will be rewritten, but
//! ```text
//!   WHERE t1.f IN (SELECT f FROM t2) OR t2.f = 'x'
//! ```
//! won't
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{
    expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
    logical_plan::{
        builder::build_join_schema, Filter, Join, JoinConstraint, JoinType, LogicalPlan,
    },
    Expr,
};
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct SubqueryFilterToJoin {}

impl SubqueryFilterToJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for SubqueryFilterToJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                // Splitting filter expression into components by AND
                let mut filters = vec![];
                utils::split_conjunction(predicate, &mut filters);

                // Searching for subquery-based filters
                let (subquery_filters, regular_filters): (Vec<&Expr>, Vec<&Expr>) =
                    filters
                        .into_iter()
                        .partition(|&e| matches!(e, Expr::InSubquery { .. }));

                // Check all subquery filters could be rewritten
                //
                // In case of expressions which could not be rewritten
                // return original filter with optimized input
                let mut subqueries_in_regular = vec![];
                regular_filters.iter().try_for_each(|&e| {
                    extract_subquery_filters(e, &mut subqueries_in_regular)
                })?;

                if !subqueries_in_regular.is_empty() {
                    return Ok(LogicalPlan::Filter(Filter {
                        predicate: predicate.clone(),
                        input: Arc::new(optimized_input),
                    }));
                };

                // Add subquery joins to new_input
                // optimized_input value should retain for possible optimization rollback
                let opt_result = subquery_filters.iter().try_fold(
                    optimized_input.clone(),
                    |input, &e| match e {
                        Expr::InSubquery {
                            expr,
                            subquery,
                            negated,
                        } => {
                            let right_input = self.optimize(
                                &subquery.subquery,
                                optimizer_config
                            )?;
                            let right_schema = right_input.schema();
                            if right_schema.fields().len() != 1 {
                                return Err(DataFusionError::Plan(
                                    "Only single column allowed in InSubquery"
                                        .to_string(),
                                ));
                            };

                            let right_key = right_schema.field(0).qualified_column();
                            let left_key = match *expr.clone() {
                                Expr::Column(col) => col,
                                _ => return Err(DataFusionError::NotImplemented(
                                    "Filtering by expression not implemented for InSubquery"
                                        .to_string(),
                                )),
                            };

                            let join_type = if *negated {
                                JoinType::Anti
                            } else {
                                JoinType::Semi
                            };

                            let schema = build_join_schema(
                                optimized_input.schema(),
                                right_schema,
                                &join_type,
                            )?;

                            Ok(LogicalPlan::Join(Join {
                                left: Arc::new(input),
                                right: Arc::new(right_input),
                                on: vec![(left_key, right_key)],
                                filter: None,
                                join_type,
                                join_constraint: JoinConstraint::On,
                                schema: Arc::new(schema),
                                null_equals_null: false,
                            }))
                        }
                        _ => Err(DataFusionError::Plan(
                            "Unknown expression while rewriting subquery to joins"
                                .to_string(),
                        )),
                    }
                );

                // In case of expressions which could not be rewritten
                // return original filter with optimized input
                let new_input = match opt_result {
                    Ok(plan) => plan,
                    Err(_) => {
                        return Ok(LogicalPlan::Filter(Filter {
                            predicate: predicate.clone(),
                            input: Arc::new(optimized_input),
                        }))
                    }
                };

                // Apply regular filters to join output if some or just return join
                if regular_filters.is_empty() {
                    Ok(new_input)
                } else {
                    Ok(utils::add_filter(new_input, &regular_filters))
                }
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "subquery_filter_to_join"
    }
}

fn extract_subquery_filters(expression: &Expr, extracted: &mut Vec<Expr>) -> Result<()> {
    struct InSubqueryVisitor<'a> {
        accum: &'a mut Vec<Expr>,
    }

    impl ExpressionVisitor for InSubqueryVisitor<'_> {
        fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
            if let Expr::InSubquery { .. } = expr {
                self.accum.push(expr.to_owned());
            }
            Ok(Recursion::Continue(self))
        }
    }

    expression.accept(InSubqueryVisitor { accum: extracted })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        and, binary_expr, col, in_subquery, lit, logical_plan::LogicalPlanBuilder,
        not_in_subquery, or, Operator,
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = SubqueryFilterToJoin::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

    fn test_subquery_with_name(name: &str) -> Result<Arc<LogicalPlan>> {
        let table_scan = test_table_scan_with_name(name)?;
        Ok(Arc::new(
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("c")])?
                .build()?,
        ))
    }

    /// Test for single IN subquery filter
    #[test]
    fn in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.c [c:UInt32]\
        \n      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for single NOT IN subquery filter
    #[test]
    fn not_in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Anti Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.c [c:UInt32]\
        \n      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for several IN subquery expressions
    #[test]
    fn in_subquery_multiple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                in_subquery(col("c"), test_subquery_with_name("sq_1")?),
                in_subquery(col("b"), test_subquery_with_name("sq_2")?),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.b = #sq_2.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    Semi Join: #test.c = #sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      Projection: #sq_1.c [c:UInt32]\
        \n        TableScan: sq_1 [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq_2.c [c:UInt32]\
        \n      TableScan: sq_2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with additional AND filter
    #[test]
    fn in_subquery_with_and_filters() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                in_subquery(col("c"), test_subquery_with_name("sq")?),
                and(
                    binary_expr(col("a"), Operator::Eq, lit(1_u32)),
                    binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                ),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Filter: #test.a = UInt32(1) AND #test.b < UInt32(30) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Semi Join: #test.c = #sq.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      Projection: #sq.c [c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with additional OR filter
    /// filter expression not modified
    #[test]
    fn in_subquery_with_or_filters() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(or(
                and(
                    binary_expr(col("a"), Operator::Eq, lit(1_u32)),
                    binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                ),
                in_subquery(col("c"), test_subquery_with_name("sq")?),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Filter: #test.a = UInt32(1) AND #test.b < UInt32(30) OR #test.c IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: #sq.c [c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    #[test]
    fn in_subquery_with_and_or_filters() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(and(
                or(
                    binary_expr(col("a"), Operator::Eq, lit(1_u32)),
                    in_subquery(col("b"), test_subquery_with_name("sq1")?),
                ),
                in_subquery(col("c"), test_subquery_with_name("sq2")?),
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Filter: #test.a = UInt32(1) OR #test.b IN (<subquery>) AND #test.c IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: #sq1.c [c:UInt32]\
        \n        TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: #sq2.c [c:UInt32]\
        \n        TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for nested IN subqueries
    #[test]
    fn in_subquery_nested() -> Result<()> {
        let table_scan = test_table_scan()?;

        let subquery = LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
            .filter(in_subquery(col("a"), test_subquery_with_name("sq_nested")?))?
            .project(vec![col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("b"), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: #test.b [b:UInt32]\
        \n  Semi Join: #test.b = #sq.a [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #sq.a [a:UInt32]\
        \n      Semi Join: #sq.a = #sq_nested.c [a:UInt32, b:UInt32, c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]\
        \n        Projection: #sq_nested.c [c:UInt32]\
        \n          TableScan: sq_nested [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for filter input modification in case filter not supported
    /// Outer filter expression not modified while inner converted to join
    #[test]
    fn in_subquery_input_modified() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq_inner")?))?
            .project_with_alias(vec![col("b"), col("c")], Some("wrapped".to_string()))?
            .filter(or(
                binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                in_subquery(col("c"), test_subquery_with_name("sq_outer")?),
            ))?
            .project(vec![col("b")])?
            .build()?;

        let expected = "Projection: #wrapped.b [b:UInt32]\
        \n  Filter: #wrapped.b < UInt32(30) OR #wrapped.c IN (<subquery>) [b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\n      Projection: #sq_outer.c [c:UInt32]\
        \n        TableScan: sq_outer [a:UInt32, b:UInt32, c:UInt32]\
        \n    Projection: #test.b, #test.c, alias=wrapped [b:UInt32, c:UInt32]\
        \n      Semi Join: #test.c = #sq_inner.c [a:UInt32, b:UInt32, c:UInt32]\
        \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n        Projection: #sq_inner.c [c:UInt32]\
        \n          TableScan: sq_inner [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }
}
