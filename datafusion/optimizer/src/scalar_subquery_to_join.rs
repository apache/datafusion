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
    conjunction, exprs_to_join_cols, find_join_exprs, only_or_err, split_conjunction,
    verify_not_disjunction,
};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{context, plan_err, Column, Result};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::logical_plan::{Filter, JoinType, Limit, Subquery};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator};
use log::debug;
use std::sync::Arc;

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct ScalarSubqueryToJoin {
    alias: Arc<AliasGenerator>,
}

impl ScalarSubqueryToJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that have a scalar subquery in them (and recurses when found)
    ///
    /// # Arguments
    /// * `predicate` - A conjunction to split and search
    /// * `optimizer_config` - For generating unique subquery aliases
    ///
    /// Returns a tuple (subqueries, non-subquery expressions)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        config: &dyn OptimizerConfig,
    ) -> Result<(Vec<SubqueryInfo>, Vec<Expr>)> {
        let filters = split_conjunction(predicate); // TODO: disjunctions

        let mut subqueries = vec![];
        let mut others = vec![];
        for it in filters.iter() {
            match it {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    let l_query = Subquery::try_from_expr(left);
                    let r_query = Subquery::try_from_expr(right);
                    if l_query.is_err() && r_query.is_err() {
                        others.push((*it).clone());
                        continue;
                    }
                    let mut recurse =
                        |q: Result<&Subquery>, expr: Expr, lhs: bool| -> Result<()> {
                            let subquery = match q {
                                Ok(subquery) => subquery,
                                _ => return Ok(()),
                            };
                            let subquery = self
                                .try_optimize(&subquery.subquery, config)?
                                .map(Arc::new)
                                .unwrap_or_else(|| subquery.subquery.clone());
                            let subquery = Subquery { subquery };
                            let res = SubqueryInfo::new(subquery, expr, *op, lhs);
                            subqueries.push(res);
                            Ok(())
                        };
                    recurse(l_query, (**right).clone(), false)?;
                    recurse(r_query, (**left).clone(), true)?;
                    // TODO: if subquery doesn't get optimized, optimized children are lost
                }
                _ => others.push((*it).clone()),
            }
        }

        Ok((subqueries, others))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
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

                // iterate through all subqueries in predicate, turning each into a join
                let mut cur_input = filter.input.as_ref().clone();
                for subquery in subqueries {
                    if let Some(optimized_subquery) =
                        optimize_scalar(&subquery, &cur_input, &other_exprs, &self.alias)?
                    {
                        cur_input = optimized_subquery;
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(None);
                    }
                }
                Ok(Some(cur_input))
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Takes a query like:
///
/// ```text
/// select id from customers where balance >
///     (select avg(total) from orders where orders.c_id = customers.id)
/// ```
///
/// and optimizes it into:
///
/// ```text
/// select c.id from customers c
/// inner join (select c_id, avg(total) as val from orders group by c_id) o on o.c_id = c.c_id
/// where c.balance > o.val
/// ```
///
/// Or a query like:
///
/// ```text
/// select id from customers where balance >
///     (select avg(total) from orders)
/// ```
///
/// and optimizes it into:
///
/// ```text
/// select c.id from customers c
/// cross join (select avg(total) as val from orders) a
/// where c.balance > a.val
/// ```
///
/// # Arguments
///
/// * `query_info` - The subquery portion of the `where` (select avg(total) from orders)
/// * `filter_input` - The non-subquery portion (from customers)
/// * `outer_others` - Any additional parts to the `where` expression (and c.x = y)
/// * `optimizer_config` - Used to generate unique subquery aliases
fn optimize_scalar(
    query_info: &SubqueryInfo,
    filter_input: &LogicalPlan,
    outer_others: &[Expr],
    alias: &AliasGenerator,
) -> Result<Option<LogicalPlan>> {
    let subquery = query_info.query.subquery.as_ref();
    debug!(
        "optimizing:
{}",
        subquery.display_indent()
    );
    let proj = match &subquery {
        LogicalPlan::Projection(proj) => proj,
        LogicalPlan::Limit(Limit {
            skip: 0,
            fetch: Some(1),
            ..
        }) => return plan_err!("Scalar subqueries with LIMIT 1 are not yet supported"),
        _ => {
            // this rule does not support this type of scalar subquery
            debug!(
                "cannot translate this type of scalar subquery to a join: {}",
                subquery.display_indent()
            );
            return Ok(None);
        }
    };
    let proj = only_or_err(proj.expr.as_slice())
        .map_err(|e| context!("exactly one expression should be projected", e))?;
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());
    let sub_inputs = subquery.inputs();
    let sub_input = only_or_err(sub_inputs.as_slice())
        .map_err(|e| context!("Exactly one input is expected. Is this a join?", e))?;

    let aggr = match sub_input {
        LogicalPlan::Aggregate(aggr) => aggr,
        _ => {
            // this rule does not support this type of scalar subquery
            debug!(
                "cannot translate this type of scalar subquery to a join: {}",
                subquery.display_indent()
            );
            return Ok(None);
        }
    };
    let filter = Filter::try_from_plan(&aggr.input).ok();

    // if there were filters, we use that logical plan, otherwise the plan from the aggregate
    let input = if let Some(filter) = filter {
        &filter.input
    } else {
        &aggr.input
    };

    // if there were filters, split and capture them
    let subqry_filter_exprs = if let Some(filter) = filter {
        split_conjunction(&filter.predicate)
    } else {
        vec![]
    };
    verify_not_disjunction(&subqry_filter_exprs)?;

    // Grab column names to join on
    let (col_exprs, other_subqry_exprs) =
        find_join_exprs(subqry_filter_exprs, input.schema())?;
    let (mut outer_cols, subqry_cols, join_filters) =
        exprs_to_join_cols(&col_exprs, input.schema(), false)?;
    if join_filters.is_some() {
        plan_err!("only joins on column equality are presently supported")?;
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let subqry_alias = alias.next("__scalar_sq");
    let group_by: Vec<_> = subqry_cols
        .iter()
        .map(|it| Expr::Column(it.clone()))
        .collect();

    // build subquery side of join - the thing the subquery was querying
    let mut subqry_plan = LogicalPlanBuilder::from((**input).clone());
    if let Some(expr) = conjunction(other_subqry_exprs) {
        subqry_plan = subqry_plan.filter(expr)? // if the subquery had additional expressions, restore them
    }

    // project the prior projection + any correlated (and now grouped) columns
    let proj: Vec<_> = group_by
        .iter()
        .cloned()
        .chain(vec![proj].iter().cloned())
        .collect();
    let subqry_plan = subqry_plan
        .aggregate(group_by, aggr.aggr_expr.clone())?
        .project(proj)?
        .alias(&subqry_alias)?
        .build()?;

    // qualify the join columns for outside the subquery
    let mut subqry_cols: Vec<_> = subqry_cols
        .iter()
        .map(|it| Column {
            relation: Some(subqry_alias.clone()),
            name: it.name.clone(),
        })
        .collect();

    let qry_expr = Expr::Column(Column {
        relation: Some(subqry_alias),
        name: "__value".to_string(),
    });

    // if correlated subquery's operation is column equality, put the clause into join on clause.
    let mut restore_where_clause = true;

    if let (Operator::Eq, Expr::Column(column)) = (query_info.op, &query_info.expr) {
        // only do this optimization for correlated subquery
        if !outer_cols.is_empty() {
            outer_cols.push(column.clone());
            subqry_cols.push(qry_expr.try_into_col().unwrap());
            restore_where_clause = false;
        }
    }

    let join_keys = (outer_cols, subqry_cols);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(filter_input.clone());
    let mut new_plan = if join_keys.0.is_empty() {
        // if not correlated, group down to 1 row and cross join on that (preserving row count)
        new_plan.cross_join(subqry_plan)?
    } else {
        // inner join if correlated, grouping by the join keys so we don't change row count
        new_plan.join(subqry_plan, JoinType::Inner, join_keys, None)?
    };

    // restore where in condition
    if restore_where_clause {
        let filter_expr = if query_info.expr_on_left {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(query_info.expr.clone()),
                query_info.op,
                Box::new(qry_expr),
            ))
        } else {
            Expr::BinaryExpr(BinaryExpr::new(
                Box::new(qry_expr),
                query_info.op,
                Box::new(query_info.expr.clone()),
            ))
        };
        new_plan = new_plan.filter(filter_expr)?;
    }

    // if the main query had additional expressions, restore them
    if let Some(expr) = conjunction(outer_others.to_vec()) {
        new_plan = new_plan.filter(expr)?
    }
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
        col, lit, logical_plan::LogicalPlanBuilder, max, min, scalar_subquery, sum,
    };
    use std::ops::Add;

    #[cfg(test)]
    #[ctor::ctor]
    fn init() {
        let _ = env_logger::try_init();
    }

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
                    .and(lit(1).lt(scalar_subquery(orders))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: Int32(1) < __scalar_sq_2.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N, o_custkey:Int64, __value:Int64;N]\
        \n    Inner Join: customer.c_custkey = __scalar_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N, o_custkey:Int64, __value:Int64;N]\
        \n      Filter: Int32(1) < __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n        Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n          TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n          SubqueryAlias: __scalar_sq_1 [o_custkey:Int64, __value:Int64;N]\
        \n            Projection: orders.o_custkey, MAX(orders.o_custkey) AS __value [o_custkey:Int64, __value:Int64;N]\
        \n              Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n      SubqueryAlias: __scalar_sq_2 [o_custkey:Int64, __value:Int64;N]\
        \n        Projection: orders.o_custkey, MAX(orders.o_custkey) AS __value [o_custkey:Int64, __value:Int64;N]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(col("lineitem.l_orderkey").eq(col("orders.o_orderkey")))?
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
                        .eq(col("customer.c_custkey"))
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
        \n  Filter: customer.c_acctbal < __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Float64;N]\
        \n    Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Float64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [o_custkey:Int64, __value:Float64;N]\
        \n        Projection: orders.o_custkey, SUM(orders.o_totalprice) AS __value [o_custkey:Int64, __value:Float64;N]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[SUM(orders.o_totalprice)]] [o_custkey:Int64, SUM(orders.o_totalprice):Float64;N]\
        \n            Filter: orders.o_totalprice < __scalar_sq_2.__value [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, l_orderkey:Int64, __value:Float64;N]\
        \n              Inner Join: orders.o_orderkey = __scalar_sq_2.l_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, l_orderkey:Int64, __value:Float64;N]\
        \n                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n                SubqueryAlias: __scalar_sq_2 [l_orderkey:Int64, __value:Float64;N]\
        \n                  Projection: lineitem.l_orderkey, SUM(lineitem.l_extendedprice) AS __value [l_orderkey:Int64, __value:Float64;N]\
        \n                    Aggregate: groupBy=[[lineitem.l_orderkey]], aggr=[[SUM(lineitem.l_extendedprice)]] [l_orderkey:Int64, SUM(lineitem.l_extendedprice):Float64;N]\
        \n                      TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey, customer.c_custkey = __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n    TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n    SubqueryAlias: __scalar_sq_1 [o_custkey:Int64, __value:Int64;N]\
        \n      Projection: orders.o_custkey, MAX(orders.o_custkey) AS __value [o_custkey:Int64, __value:Int64;N]\
        \n        Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n          Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
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

        // it will optimize, but fail for the same reason the unoptimized query would
        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n    CrossJoin: [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [__value:Int64;N]\
        \n        Projection: MAX(orders.o_custkey) AS __value [__value:Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            Filter: customer.c_custkey = customer.c_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";
        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
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
        \n  Filter: customer.c_custkey = __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n    CrossJoin: [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [__value:Int64;N]\
        \n        Projection: MAX(orders.o_custkey) AS __value [__value:Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
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

        let expected = r#"only joins on column equality are presently supported"#;

        assert_optimizer_err(Arc::new(ScalarSubqueryToJoin::new()), &plan, expected);
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
        assert_optimizer_err(Arc::new(ScalarSubqueryToJoin::new()), &plan, expected);
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

        let expected = r#"Optimizing disjunctions not supported!"#;
        assert_optimizer_err(Arc::new(ScalarSubqueryToJoin::new()), &plan, expected);
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

        // we expect the plan to be unchanged because this subquery is not supported by this rule
        let expected = r#"Projection: customer.c_custkey [c_custkey:Int64]
  Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
    Subquery: [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
      Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]"#;

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    /// Test for correlated scalar expressions
    #[test]
    #[ignore]
    fn scalar_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey")).add(lit(1))])?
                .build()?,
        );
        /*
        Error: SchemaError(FieldNotFound { qualifier: Some("orders"), name: "o_custkey", valid_fields: Some(["MAX(orders.o_custkey)"]) })
         */

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = r#""#;

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
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

        let expected = r#"exactly one expression should be projected"#;
        assert_optimizer_err(Arc::new(ScalarSubqueryToJoin::new()), &plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional filters
    #[test]
    fn scalar_subquery_additional_filters_with_non_equal_clause() -> Result<()> {
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
                    .gt_eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n    Filter: customer.c_custkey >= __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n      Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n        TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n        SubqueryAlias: __scalar_sq_1 [o_custkey:Int64, __value:Int64;N]\
        \n          Projection: orders.o_custkey, MAX(orders.o_custkey) AS __value [o_custkey:Int64, __value:Int64;N]\
        \n            Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
        Ok(())
    }

    #[test]
    fn scalar_subquery_additional_filters_with_equal_clause() -> Result<()> {
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

        let expected = "Projection: customer.c_custkey [c_custkey:Int64]\
        \n  Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n    Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey, customer.c_custkey = __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, o_custkey:Int64, __value:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [o_custkey:Int64, __value:Int64;N]\
        \n        Projection: orders.o_custkey, MAX(orders.o_custkey) AS __value [o_custkey:Int64, __value:Int64;N]\
        \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[MAX(orders.o_custkey)]] [o_custkey:Int64, MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
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

        // unoptimized plan because we don't support disjunctions yet
        let expected = r#"Projection: customer.c_custkey [c_custkey:Int64]
  Filter: customer.c_custkey = (<subquery>) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
    Subquery: [MAX(orders.o_custkey):Int64;N]
      Projection: MAX(orders.o_custkey) [MAX(orders.o_custkey):Int64;N]
        Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]
          Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
    TableScan: customer [c_custkey:Int64, c_name:Utf8]"#;
        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
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

        let expected = "Projection: test.c [c:UInt32]\
        \n  Filter: test.c < __scalar_sq_1.__value [a:UInt32, b:UInt32, c:UInt32, a:UInt32, __value:UInt32;N]\
        \n    Inner Join: test.a = __scalar_sq_1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, __value:UInt32;N]\
        \n      TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n      SubqueryAlias: __scalar_sq_1 [a:UInt32, __value:UInt32;N]\
        \n        Projection: sq.a, MIN(sq.c) AS __value [a:UInt32, __value:UInt32;N]\
        \n          Aggregate: groupBy=[[sq.a]], aggr=[[MIN(sq.c)]] [a:UInt32, MIN(sq.c):UInt32;N]\
        \n            TableScan: sq [a:UInt32, b:UInt32, c:UInt32]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
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
        \n  Filter: customer.c_custkey < __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n    CrossJoin: [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [__value:Int64;N]\
        \n        Projection: MAX(orders.o_custkey) AS __value [__value:Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
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
        \n  Filter: customer.c_custkey = __scalar_sq_1.__value [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n    CrossJoin: [c_custkey:Int64, c_name:Utf8, __value:Int64;N]\
        \n      TableScan: customer [c_custkey:Int64, c_name:Utf8]\
        \n      SubqueryAlias: __scalar_sq_1 [__value:Int64;N]\
        \n        Projection: MAX(orders.o_custkey) AS __value [__value:Int64;N]\
        \n          Aggregate: groupBy=[[]], aggr=[[MAX(orders.o_custkey)]] [MAX(orders.o_custkey):Int64;N]\
        \n            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]";

        assert_optimized_plan_eq_display_indent(
            Arc::new(ScalarSubqueryToJoin::new()),
            &plan,
            expected,
        );
        Ok(())
    }
}
