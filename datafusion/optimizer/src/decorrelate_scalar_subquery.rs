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
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateScalarSubquery {}

impl DecorrelateScalarSubquery {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Finds expressions that have a scalar subquery in them
    ///
    /// # Arguments
    ///
    /// * `predicate` - A conjunction to split and search
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let mut filters = vec![];
        split_conjunction(predicate, &mut filters); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::BinaryExpr {
                    left: l_expr,
                    op,
                    right: r_expr,
                } => {
                    let l_query = match &**l_expr {
                        Expr::ScalarSubquery(subquery) => Some(subquery.clone()),
                        _ => None,
                    };
                    let r_query = match &**r_expr {
                        Expr::ScalarSubquery(subquery) => Some(subquery.clone()),
                        _ => None,
                    };
                    if l_query.is_none() && r_query.is_none() {
                        others.push((*it).clone());
                        continue;
                    }
                    let mut recurse =
                        |q: Option<Subquery>, expr: Expr, lhs: bool| -> Result<()> {
                            let subquery = match q {
                                Some(subquery) => subquery,
                                _ => return Ok(()),
                            };
                            let subquery =
                                self.optimize(&*subquery.subquery, optimizer_config)?;
                            let subquery = Arc::new(subquery);
                            let subquery = Subquery { subquery };
                            let res = SubqueryInfo::new(subquery, expr, *op, lhs);
                            subqueries.push(res);
                            Ok(())
                        };
                    recurse(l_query, (**r_expr).clone(), false)?;
                    recurse(r_query, (**l_expr).clone(), true)?;
                    // TODO: if subquery doesn't get optimized, optimized children are lost
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for DecorrelateScalarSubquery {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

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
                let mut cur_input = (**input).clone();
                for subquery in subqueries {
                    let res = optimize_scalar(
                        &subquery,
                        &cur_input,
                        &other_exprs,
                        optimizer_config,
                    )?;
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
        "decorrelate_scalar_subquery"
    }
}

/// Takes a query like:
///
/// ```select id from customers where balance > (select avg(total) from orders)```
///
/// and optimizes it into:
///
/// ```select c.id from customers c
/// inner join (select c_id, avg(total) as val from orders group by c_id) o on o.c_id = c.c_id
/// where c.balance > o.val```
///
/// # Arguments
///
/// * `subqry` - The subquery portion of the `where exists` (select * from orders)
/// * `negated` - True if the subquery is a `where not exists`
/// * `filter_input` - The non-subquery portion (from customers)
/// * `other_filter_exprs` - Any additional parts to the `where` expression (and c.x = y)
/// * `optimizer_config` - Used to generate unique subquery aliases
fn optimize_scalar(
    query_info: &SubqueryInfo,
    filter_input: &LogicalPlan,
    outer_others: &[Expr],
    optimizer_config: &mut OptimizerConfig,
) -> datafusion_common::Result<Option<LogicalPlan>> {
    // Scalar subqueries should be projecting a single value, grab and alias it
    let proj = match &*query_info.query.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(None), // should be projecting something
    };
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(None), // scalar subquery means only 1 expr
    };
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());

    // Only operate if there is one input
    let sub_inputs = query_info.query.subquery.inputs();
    let sub_input = match sub_inputs.as_slice() {
        [it] => it,
        _ => return Ok(None), // shouldn't be a join (>1 input)
    };

    // Scalar subqueries should be aggregating a value
    let aggr = match sub_input {
        LogicalPlan::Aggregate(a) => a,
        _ => return Ok(None),
    };
    let filter = match &*aggr.input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(None), // Not correlated - TODO: also handle this case
    };

    // split into filters
    let mut subqry_filter_exprs = vec![];
    split_conjunction(&filter.predicate, &mut subqry_filter_exprs);

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) =
        find_join_exprs(subqry_filter_exprs, filter.input.schema())?;
    let (outer_cols, subqry_cols, join_filters) =
        exprs_to_join_cols(&col_exprs, filter.input.schema(), false)?;
    if join_filters.is_some() {
        return Ok(None); // non-column join expressions not yet supported
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let subqry_alias = format!("__sq_{}", optimizer_config.next_id());
    let group_by: Vec<_> = subqry_cols
        .iter()
        .map(|it| Expr::Column(it.clone()))
        .collect();

    // build subquery side of join - the thing the subquery was querying
    let subqry_plan = LogicalPlanBuilder::from((*filter.input).clone());
    let subqry_plan = if let Some(expr) = combine_filters(&other_subqry_exprs) {
        subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        subqry_plan
    };

    // project the prior projection + any correlated (and now grouped) columns
    let proj: Vec<_> = group_by
        .iter()
        .cloned()
        .chain(vec![proj].iter().cloned())
        .collect();
    let subqry_plan = subqry_plan
        .aggregate(group_by, aggr.aggr_expr.clone())?
        .project_with_alias(proj, Some(subqry_alias.clone()))?
        .build()?;

    // qualify the join columns for outside the subquery
    let subqry_cols: Vec<_> = subqry_cols
        .iter()
        .map(|it| Column {
            relation: Some(subqry_alias.clone()),
            name: it.name.clone(),
        })
        .collect();
    let join_keys = (outer_cols, subqry_cols);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let new_plan = if join_keys.0.is_empty() {
        // if not correlated, group down to 1 row and cross join on that (preserving row count)
        new_plan.cross_join(&subqry_plan)?
    } else {
        // inner join if correlated, grouping by the join keys so we don't change row count
        new_plan.join(&subqry_plan, JoinType::Inner, join_keys, None)?
    };

    // if the main query had additional expressions, restore them
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)?
    } else {
        new_plan
    };

    // restore conditions
    let qry_expr = Box::new(Expr::Column(Column {
        relation: Some(subqry_alias),
        name: "__value".to_string(),
    }));
    let filter_expr = if query_info.expr_on_left {
        Expr::BinaryExpr {
            left: Box::new(query_info.expr.clone()),
            op: query_info.op,
            right: qry_expr,
        }
    } else {
        Expr::BinaryExpr {
            left: qry_expr,
            op: query_info.op,
            right: Box::new(query_info.expr.clone()),
        }
    };
    let new_plan = new_plan.filter(filter_expr)?;

    let new_plan = new_plan.build()?;
    Ok(Some(new_plan))
}

struct SubqueryInfo {
    query: Subquery,
    expr: Expr,
    op: Operator,
    expr_on_left: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, expr: Expr, op: Operator, expr_on_left: bool) -> Self {
        Self {
            query,
            expr,
            op,
            expr_on_left,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::{
        col, lit, logical_plan::LogicalPlanBuilder, max, min, scalar_subquery,
    };
    use std::ops::Add;
    use crate::utils::{assert_optimized_plan_eq, assert_optimizer_err};

    /// Test multiple correlated subqueries
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("customer.c_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                lit(1)
                    .lt(scalar_subquery(orders.clone()))
                    .and(lit(1).lt(scalar_subquery(orders.clone()))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Filter: Int32(1) < #__sq_2.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N, o_custkey:Int64, __value:Int64;N]
    Inner Join: #customer.c_custkey = #__sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N, o_custkey:Int64, __value:Int64;N]
      Filter: Int32(1) < #__sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
        Inner Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
          TableScan: customer [c_custkey:Int64, c_name:Utf8]
          Projection: #orders.o_custkey, #MAX(orders.o_custkey) AS __value, alias=__sq_1 [o_custkey:Int64, __value:Int64;N]
            Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[MAX(#orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]
              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8]
      Projection: #orders.o_custkey, #MAX(orders.o_custkey) AS __value, alias=__sq_2 [o_custkey:Int64, __value:Int64;N]
        Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[MAX(#orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]
          TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8]"#;
        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(col("lineitem.l_orderkey").eq(col("orders.o_orderkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("lineitem.l_orderkey"))])?
                .project(vec![max(col("lineitem.l_orderkey"))])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_orderkey")
                        .eq(scalar_subquery(lineitem))
                        .and(col("orders.o_custkey").eq(col("customer.c_custkey"))),
                )?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(max(col("customer.c_custkey")).eq(scalar_subquery(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"unknown"#;
        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional subquery filters
    #[test]
    fn scalar_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
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

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Filter: #customer.c_custkey = #__sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
    Inner Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
      TableScan: customer [c_custkey:Int64, c_name:Utf8]
      Projection: #orders.o_custkey, #MAX(orders.o_custkey) AS __value, alias=__sq_1 [o_custkey:Int64, __value:Int64;N]
        Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[MAX(#orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]
          Filter: #orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8]
            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8]"#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery with no columns in schema
    #[test]
    fn scalar_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("customer.c_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
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

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery not equal
    #[test]
    fn scalar_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").not_eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery less than
    #[test]
    fn scalar_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").lt(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#"can't optimize < column comparison"#;
        assert_optimizer_err(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with subquery disjunction
    #[test]
    fn scalar_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("customer.c_custkey")
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

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
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

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar expressions
    #[test]
    fn scalar_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey")).add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
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

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional filters
    #[test]
    fn scalar_subquery_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
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

        let expected = r#"Projection: #customer.c_custkey [c_custkey:Int64]
  Filter: #customer.c_custkey = #__sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
    Filter: #customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
      Inner Join: #customer.c_custkey = #__sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]
        TableScan: customer [c_custkey:Int64, c_name:Utf8]
        Projection: #orders.o_custkey, #MAX(orders.o_custkey) AS __value, alias=__sq_1 [o_custkey:Int64, __value:Int64;N]
          Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[MAX(#orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]
            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8]"#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with disjustions
    #[test]
    fn scalar_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
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

        let expected = r#""#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("test.a").eq(col("sq.a")))?
                .aggregate(Vec::<Expr>::new(), vec![min(col("c"))])?
                .project(vec![min(col("c"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(col("test.c").lt(scalar_subquery(sq)))?
            .project(vec![col("test.c")])?
            .build()?;

        let expected = r#"Projection: #test.c [c:UInt32]
  Filter: #test.c < #__sq_1.__value [a:UInt32, b:UInt32, c:UInt32, a:UInt32, __value:UInt32;N]
    Inner Join: #test.a = #__sq_1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, __value:UInt32;N]
      TableScan: test [a:UInt32, b:UInt32, c:UInt32]
      Projection: #sq.a, #MIN(sq.c) AS __value, alias=__sq_1 [a:UInt32, __value:UInt32;N]
        Aggregate: groupBy=[[#sq.a]], aggr=[[MIN(#sq.c)]] [a:UInt32, MIN(sq.c):UInt32;N]
          TableScan: sq [a:UInt32, b:UInt32, c:UInt32]"#;

        assert_optimized_plan_eq(&DecorrelateScalarSubquery::new(), &plan, expected);
        Ok(())
    }
}
