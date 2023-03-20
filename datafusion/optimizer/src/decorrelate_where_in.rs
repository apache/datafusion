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

use crate::alias::AliasGenerator;
use crate::optimizer::ApplyOrder;
use crate::utils::{
    collect_subquery_cols, conjunction, extract_join_filters, only_or_err,
    split_conjunction,
};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{context, Column, Result};
use datafusion_expr::expr_rewriter::{replace_col, unnormalize_col};
use datafusion_expr::logical_plan::{JoinType, Projection, Subquery};
use datafusion_expr::{Expr, Filter, LogicalPlan, LogicalPlanBuilder};
use log::debug;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Default)]
pub struct DecorrelateWhereIn {
    alias: AliasGenerator,
}

impl DecorrelateWhereIn {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that have a where in subquery (and recurses when found)
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    /// * `optimizer_config` - For generating unique subquery aliases
    ///
    /// Returns a tuple (subqueries, non-subquery expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        config: &dyn OptimizerConfig,
    ) -> datafusion_common::Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let filters = split_conjunction(predicate); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let subquery_plan = self
                        .try_optimize(&subquery.subquery, config)?
                        .map(Arc::new)
                        .unwrap_or_else(|| subquery.subquery.clone());
                    let subquery = Subquery {
                        subquery: subquery_plan,
                        outer_ref_columns: subquery.outer_ref_columns.clone(),
                    };
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
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let (subqueries, other_exprs) =
                    self.extract_subquery_exprs(&filter.predicate, config)?;
                if subqueries.is_empty() {
                    // regular filter, no subquery exists clause here
                    return Ok(None);
                }

                // iterate through all exists clauses in predicate, turning each into a join
                let mut cur_input = filter.input.as_ref().clone();
                for subquery in subqueries {
                    cur_input = optimize_where_in(&subquery, &cur_input, &self.alias)?;
                }

                let expr = conjunction(other_exprs);
                if let Some(expr) = expr {
                    let new_filter = Filter::try_new(expr, Arc::new(cur_input))?;
                    cur_input = LogicalPlan::Filter(new_filter);
                }

                Ok(Some(cur_input))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "decorrelate_where_in"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Optimize the where in subquery to left-anti/left-semi join.
/// If the subquery is a correlated subquery, we need extract the join predicate from the subquery.
///
/// For example, given a query like:
/// `select t1.a, t1.b from t1 where t1 in (select t2.a from t2 where t1.b = t2.b and t1.c > t2.c)`
///
/// The optimized plan will be:
///
/// ```text
/// Projection: t1.a, t1.b
///   LeftSemi Join:  Filter: t1.a = __correlated_sq_1.a AND t1.b = __correlated_sq_1.b AND t1.c > __correlated_sq_1.c
///     TableScan: t1
///     SubqueryAlias: __correlated_sq_1
///       Projection: t2.a AS a, t2.b, t2.c
///         TableScan: t2
/// ```
fn optimize_where_in(
    query_info: &SubqueryInfo,
    left: &LogicalPlan,
    alias: &AliasGenerator,
) -> Result<LogicalPlan> {
    let projection = Projection::try_from_plan(&query_info.query.subquery)
        .map_err(|e| context!("a projection is required", e))?;
    let subquery_input = projection.input.clone();
    // TODO add the validate logic to Analyzer
    let subquery_expr = only_or_err(projection.expr.as_slice())
        .map_err(|e| context!("single expression projection required", e))?;

    // extract join filters
    let (join_filters, subquery_input) = extract_join_filters(subquery_input.as_ref())?;

    // in_predicate may be also include in the join filters, remove it from the join filters.
    let in_predicate = Expr::eq(query_info.where_in_expr.clone(), subquery_expr.clone());
    let join_filters = remove_duplicated_filter(join_filters, in_predicate);

    // replace qualified name with subquery alias.
    let subquery_alias = alias.next("__correlated_sq");
    let input_schema = subquery_input.schema();
    let mut subquery_cols = collect_subquery_cols(&join_filters, input_schema.clone())?;
    let join_filter = conjunction(join_filters).map_or(Ok(None), |filter| {
        replace_qualified_name(filter, &subquery_cols, &subquery_alias).map(Option::Some)
    })?;

    // add projection
    if let Expr::Column(col) = subquery_expr {
        subquery_cols.remove(col);
    }
    let subquery_expr_name = format!("{:?}", unnormalize_col(subquery_expr.clone()));
    let first_expr = subquery_expr.clone().alias(subquery_expr_name.clone());
    let projection_exprs: Vec<Expr> = [first_expr]
        .into_iter()
        .chain(subquery_cols.into_iter().map(Expr::Column))
        .collect();

    let right = LogicalPlanBuilder::from(subquery_input)
        .project(projection_exprs)?
        .alias(&subquery_alias)?
        .build()?;

    // join our sub query into the main plan
    let join_type = match query_info.negated {
        true => JoinType::LeftAnti,
        false => JoinType::LeftSemi,
    };
    let right_join_col = Column::new(Some(subquery_alias), subquery_expr_name);
    let in_predicate = Expr::eq(
        query_info.where_in_expr.clone(),
        Expr::Column(right_join_col),
    );
    let join_filter = join_filter
        .map(|filter| in_predicate.clone().and(filter))
        .unwrap_or_else(|| in_predicate);

    let new_plan = LogicalPlanBuilder::from(left.clone())
        .join(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            Some(join_filter),
        )?
        .build()?;

    debug!("where in optimized:\n{}", new_plan.display_indent());
    Ok(new_plan)
}

fn remove_duplicated_filter(filters: Vec<Expr>, in_predicate: Expr) -> Vec<Expr> {
    filters
        .into_iter()
        .filter(|filter| {
            if filter == &in_predicate {
                return false;
            }

            // ignore the binary order
            !match (filter, &in_predicate) {
                (Expr::BinaryExpr(a_expr), Expr::BinaryExpr(b_expr)) => {
                    (a_expr.op == b_expr.op)
                        && (a_expr.left == b_expr.left && a_expr.right == b_expr.right)
                        || (a_expr.left == b_expr.right && a_expr.right == b_expr.left)
                }
                _ => false,
            }
        })
        .collect::<Vec<_>>()
}

fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| {
            Column::from_qualified_name(format!("{}.{}", subquery_alias, col.name))
        })
        .collect();
    let replace_map: HashMap<&Column, &Column> =
        cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
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
    use datafusion_common::Result;
    use datafusion_expr::{
        and, binary_expr, col, in_subquery, lit, logical_plan::LogicalPlanBuilder,
        not_in_subquery, or, Operator,
    };
    use std::ops::Add;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            plan,
            expected,
        );
        Ok(())
    }

    fn test_subquery_with_name(name: &str) -> Result<Arc<LogicalPlan>> {
        let table_scan = test_table_scan_with_name(name)?;
        Ok(Arc::new(
            LogicalPlanBuilder::from(table_scan)
                .project(vec![col("c")])?
                .build()?,
        ))
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.b = __correlated_sq_2.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n        Projection: sq_1.c AS c [c:UInt32]\
        \n          TableScan: sq_1 [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_2 [c:UInt32]\
        \n      Projection: sq_2.c AS c [c:UInt32]\
        \n        TableScan: sq_2 [a:UInt32, b:UInt32, c:UInt32]";
        assert_optimized_plan_equal(&plan, expected)
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  Filter: test.a = UInt32(1) AND test.b < UInt32(30) [a:UInt32, b:UInt32, c:UInt32]\
        \n    LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n        Projection: sq.c AS c [c:UInt32]\
        \n          TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  Filter: test.a = UInt32(1) AND test.b < UInt32(30) OR test.c IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: sq.c [c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  Filter: test.a = UInt32(1) OR test.b IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: sq1.c [c:UInt32]\
        \n        TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]\
        \n    LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n        Projection: sq2.c AS c [c:UInt32]\
        \n          TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.b = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [a:UInt32]\
        \n      Projection: sq.a AS a [a:UInt32]\
        \n        LeftSemi Join:  Filter: sq.a = __correlated_sq_2.c [a:UInt32, b:UInt32, c:UInt32]\
        \n          TableScan: sq [a:UInt32, b:UInt32, c:UInt32]\
        \n          SubqueryAlias: __correlated_sq_2 [c:UInt32]\
        \n            Projection: sq_nested.c AS c [c:UInt32]\
        \n              TableScan: sq_nested [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    /// Test for filter input modification in case filter not supported
    /// Outer filter expression not modified while inner converted to join
    #[test]
    fn in_subquery_input_modified() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq_inner")?))?
            .project(vec![col("b"), col("c")])?
            .alias("wrapped")?
            .filter(or(
                binary_expr(col("b"), Operator::Lt, lit(30_u32)),
                in_subquery(col("c"), test_subquery_with_name("sq_outer")?),
            ))?
            .project(vec![col("b")])?
            .build()?;

        let expected = "Projection: wrapped.b [b:UInt32]\
        \n  Filter: wrapped.b < UInt32(30) OR wrapped.c IN (<subquery>) [b:UInt32, c:UInt32]\
        \n    Subquery: [c:UInt32]\
        \n      Projection: sq_outer.c [c:UInt32]\
        \n        TableScan: sq_outer [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: wrapped [b:UInt32, c:UInt32]\
        \n      Projection: test.b, test.c [b:UInt32, c:UInt32]\
        \n        LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n          TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n          SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n            Projection: sq_inner.c AS c [c:UInt32]\
        \n              TableScan: sq_inner [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_equal(&plan, expected)
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        let _ = env_logger::try_init();
    }

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
                    .and(in_subquery(col("customer.c_custkey"), orders)),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent());

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n        Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n    SubqueryAlias: __correlated_sq_2 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        LeftSemi Join:  Filter: orders.o_orderkey = __correlated_sq_2.l_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          SubqueryAlias: __correlated_sq_2 [l_orderkey:Int64]\
        \n            Projection: lineitem.l_orderkey AS l_orderkey [l_orderkey:Int64]\
        \n              TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND customer.c_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND customer.c_custkey != __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND customer.c_custkey < __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND (customer.c_custkey = __correlated_sq_1.o_custkey OR __correlated_sq_1.o_orderkey = Int32(1)) [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64, o_orderkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey, orders.o_orderkey [o_custkey:Int64, o_orderkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );

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

        // Maybe okay if the table only has a single column?
        assert_optimizer_err(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            "a projection is required",
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey + Int32(1) = __correlated_sq_1.o_custkey AND customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n      Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey + Int32(1) AND customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __correlated_sq_1 [o_custkey + Int32(1):Int64, o_custkey:Int64]\
        \n      Projection: orders.o_custkey + Int32(1) AS o_custkey + Int32(1), orders.o_custkey [o_custkey + Int32(1):Int64, o_custkey:Int64]\
        \n        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        assert_optimizer_err(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            "single expression projection required",
        );
        Ok(())
    }

    /// Test for correlated IN subquery filter with additional filters
    #[test]
    fn should_support_additional_filters() -> Result<()> {
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]\
        \n    LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]\
        \n        Projection: orders.o_custkey AS o_custkey [o_custkey:Int64]\
        \n          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        // TODO: support disjunction - for now expect unaltered plan
        let expected = r#"Projection: customer.c_custkey [c_custkey:Int64]
  Filter: customer.c_custkey IN (<subquery>) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
    Subquery: [o_custkey:Int64]
      Projection: orders.o_custkey [o_custkey:Int64]
        Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]"#;

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.c = __correlated_sq_1.c AND test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c:UInt32, a:UInt32]\
        \n      Projection: sq.c AS c, sq.a [c:UInt32, a:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n      Projection: sq.c AS c [c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftAnti Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c:UInt32]\
        \n      Projection: sq.c AS c [c:UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn in_subquery_both_side_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c") + lit(1u32), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.c * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c * UInt32(2):UInt32]\
        \n      Projection: sq.c * UInt32(2) AS c * UInt32(2) [c * UInt32(2):UInt32]\
        \n        TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn in_subquery_join_filter_and_inner_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                col("test.a")
                    .eq(col("sq.a"))
                    .and(col("sq.a").add(lit(1u32)).eq(col("sq.b"))),
            )?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c") + lit(1u32), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.c * UInt32(2) AND test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c * UInt32(2):UInt32, a:UInt32]\
        \n      Projection: sq.c * UInt32(2) AS c * UInt32(2), sq.a [c * UInt32(2):UInt32, a:UInt32]\
        \n        Filter: sq.a + UInt32(1) = sq.b [a:UInt32, b:UInt32, c:UInt32]\
        \n          TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn in_subquery_muti_project_subquery_cols() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                col("test.a")
                    .add(col("test.b"))
                    .eq(col("sq.a").add(col("sq.b")))
                    .and(col("sq.a").add(lit(1u32)).eq(col("sq.b"))),
            )?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c") + lit(1u32), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
        \n  LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.c * UInt32(2) AND test.a + test.b = __correlated_sq_1.a + __correlated_sq_1.b [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    SubqueryAlias: __correlated_sq_1 [c * UInt32(2):UInt32, a:UInt32, b:UInt32]\
        \n      Projection: sq.c * UInt32(2) AS c * UInt32(2), sq.a, sq.b [c * UInt32(2):UInt32, a:UInt32, b:UInt32]\
        \n        Filter: sq.a + UInt32(1) = sq.b [a:UInt32, b:UInt32, c:UInt32]\
        \n          TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn two_in_subquery_with_outer_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .filter(col("test.a").gt(col("sq1.a")))?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .filter(col("test.a").gt(col("sq2.a")))?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                in_subquery(col("c") + lit(1u32), Arc::new(subquery1)).and(
                    in_subquery(col("c") * lit(2u32), Arc::new(subquery2))
                        .and(col("test.c").gt(lit(1u32))),
                ),
            )?
            .project(vec![col("test.b")])?
            .build()?;

        let expected = "Projection: test.b [b:UInt32]\
        \n  Filter: test.c > UInt32(1) [a:UInt32, b:UInt32, c:UInt32]\
        \n    LeftSemi Join:  Filter: test.c * UInt32(2) = __correlated_sq_2.c * UInt32(2) AND test.a > __correlated_sq_2.a [a:UInt32, b:UInt32, c:UInt32]\
        \n      LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.c * UInt32(2) AND test.a > __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]\
        \n        TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n        SubqueryAlias: __correlated_sq_1 [c * UInt32(2):UInt32, a:UInt32]\
        \n          Projection: sq1.c * UInt32(2) AS c * UInt32(2), sq1.a [c * UInt32(2):UInt32, a:UInt32]\
        \n            TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __correlated_sq_2 [c * UInt32(2):UInt32, a:UInt32]\
        \n        Projection: sq2.c * UInt32(2) AS c * UInt32(2), sq2.a [c * UInt32(2):UInt32, a:UInt32]\
        \n          TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn in_subquery_with_same_table() -> Result<()> {
        let outer_scan = test_table_scan()?;
        let subquery_scan = test_table_scan()?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").gt(col("test.b")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(in_subquery(col("test.a"), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        // Subquery and outer query refer to the same table.
        let expected = "Projection: test.b [b:UInt32]\
                      \n  LeftSemi Join:  Filter: test.a = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]\
                      \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
                      \n    SubqueryAlias: __correlated_sq_1 [c:UInt32]\
                      \n      Projection: test.c AS c [c:UInt32]\
                      \n        Filter: test.a > test.b [a:UInt32, b:UInt32, c:UInt32]\
                      \n          TableScan: test [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(DecorrelateWhereIn::new()),
            &plan,
            expected,
        );
        Ok(())
    }
}
