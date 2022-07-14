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

use crate::utils::{exprs_to_join_cols, find_join_exprs, split_conjunction};
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::Column;
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use log::debug;
use std::sync::Arc;

#[derive(Default)]
pub struct DecorrelateWhereIn {}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have a where in subquery
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    ///
    /// Returns a tuple of tuples ((expressions, subqueries, negated), remaining expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let mut filters = vec![];
        split_conjunction(predicate, &mut filters); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let subquery =
                        self.optimize(&*subquery.subquery, optimizer_config)?;
                    let subquery = Arc::new(subquery);
                    let subquery = Subquery { subquery };
                    let subquery =
                        SubqueryInfo::new(subquery.clone(), (**expr).clone(), *negated);
                    subqueries.push(subquery);
                    // TODO: if subquery doesn't get optimized, optimized children are lost
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateWhereIn {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate,
                input: filter_input,
            }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(filter_input, optimizer_config)?;

                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(predicate, optimizer_config)?;
                let optimized_plan = LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(optimized_input),
                });
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(optimized_plan);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = (**filter_input).clone();
                for subquery in subqueries {
                    let res = optimize_where_in(&subquery, &cur_input, &other_exprs)?;
                    if let Some(res) = res {
                        cur_input = res
                    }
                }
                Ok(cur_input)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }
}

fn optimize_where_in(
    query_info: &SubqueryInfo,
    outer_input: &LogicalPlan,
    outer_other_exprs: &[Expr],
) -> datafusion_common::Result<Option<LogicalPlan>> {
    // where in queries should always project a single expression
    let proj = match &*query_info.query.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(None),
    };
    let mut subqry_input = proj.input.clone();
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(None), // in subquery means only 1 expr
    };
    let subquery_col = match proj {
        Expr::Column(it) => Column::from(it.flat_name().as_str()),
        _ => return Ok(None), // only operate on columns for now, not arbitrary expressions
    };

    // Grab column names to join on
    let outer_col = match &query_info.where_in_expr {
        Expr::Column(it) => Column::from(it.flat_name().as_str()),
        _ => return Ok(None), // only operate on columns for now, not arbitrary expressions
    };

    // If subquery is correlated, grab necessary information
    let mut subqry_cols = vec![];
    let mut outer_cols = vec![];
    let mut join_filters = None;
    let mut other_subqry_exprs = vec![];
    if let LogicalPlan::Filter(subqry_filter) = (*subqry_input).clone() {
        subqry_input = subqry_filter.input.clone();

        // split into filters
        let mut subqry_filter_exprs = vec![];
        split_conjunction(&subqry_filter.predicate, &mut subqry_filter_exprs);

        // Grab column names to join on
        let (col_exprs, other_exprs) =
            find_join_exprs(subqry_filter_exprs, subqry_filter.input.schema());
        (outer_cols, subqry_cols, join_filters) =
            exprs_to_join_cols(&col_exprs, subqry_filter.input.schema(), false)?;
        other_subqry_exprs = other_exprs;
    }

    let subqry_cols: Vec<_> = vec![subquery_col]
        .iter()
        .cloned()
        .chain(subqry_cols)
        .collect();
    let outer_cols: Vec<_> = vec![outer_col].iter().cloned().chain(outer_cols).collect();

    // build subquery side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*subqry_input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };
    let projection: Vec<_> = subqry_cols
        .iter()
        .map(|it| Expr::Column(it.clone()))
        .collect();
    let subqry_plan = subqry_plan.project(projection)?.build()?;

    let join_keys = (outer_cols, subqry_cols);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(outer_input.clone());
    let new_plan = if query_info.negated {
        new_plan.join(&subqry_plan, JoinType::Anti, join_keys, join_filters)?
    } else {
        new_plan.join(&subqry_plan, JoinType::Semi, join_keys, join_filters)?
    };
    let new_plan = if let Some(expr) = combine_filters(outer_other_exprs) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    let result = new_plan.build()?;
    debug!("where in optimized: {}", result.display_indent());
    Ok(Some(result))
}

struct SubqueryInfo {
    query: Subquery,
    where_in_expr: Expr,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, expr: Expr, negated: bool) -> Self {
        Self {
            query,
            where_in_expr: expr,
            negated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        col, in_subquery, lit, logical_plan::LogicalPlanBuilder, not_in_subquery,
    };
    use std::ops::Add;

    /// Test multiple correlated subqueries
    /// See subqueries.rs where_in_multiple()
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("customer.c_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), orders.clone())
                    .and(in_subquery(col("customer.c_custkey"), orders.clone())),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"unknown"#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test recursive correlated subqueries
    /// See subqueries.rs where_in_recursive()
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(col("lineitem.l_orderkey").eq(col("orders.o_orderkey")))?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    in_subquery(col("orders.o_orderkey"), lineitem)
                        .and(col("orders.o_custkey").eq(col("customer.c_custkey"))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"unknown"#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with additional subquery filters
    #[test]
    fn in_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery with no columns in schema
    #[test]
    fn in_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("customer.c_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for IN subquery with both columns in schema
    #[test]
    fn in_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery not equal
    #[test]
    fn in_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").not_eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery less than
    #[test]
    fn in_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").lt(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with subquery disjunction
    #[test]
    fn in_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN without projection
    #[test]
    fn in_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery join on expression
    #[test]
    fn in_subquery_join_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey").add(lit(1)), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN expressions
    #[test]
    fn in_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery multiple projected columns
    #[test]
    fn in_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with additional filters
    #[test]
    fn in_subquery_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter with disjustions
    #[test]
    fn in_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), sq)
                    .or(col("customer.c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
    }

    /// Test for correlated IN subquery filter
    #[test]
    fn in_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(in_subquery(col("c"), sq))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = r#"Projection: #test.b [b:UInt32]
  Semi Join: #test.c = #sq.c, #test.a = #sq.a [a:UInt32, b:UInt32, c:UInt32]
    TableScan: test [a:UInt32, b:UInt32, c:UInt32]
    Projection: #sq.c, #sq.a [c:UInt32, a:UInt32]
      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&plan, expected);
        Ok(())
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

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = DecorrelateWhereIn::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{}", optimized_plan.display_indent_schema());
        assert_eq!(formatted_plan, expected);
    }

}
