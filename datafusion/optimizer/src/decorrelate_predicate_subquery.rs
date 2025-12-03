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

//! [`DecorrelatePredicateSubquery`] converts `IN`/`EXISTS` subquery predicates to `SEMI`/`ANTI` joins
use std::collections::BTreeSet;
use std::ops::Deref;
use std::sync::Arc;

use crate::decorrelate::PullUpCorrelatedExpr;
use crate::optimizer::ApplyOrder;
use crate::utils::replace_qualified_name;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{assert_or_internal_err, plan_err, Column, Result};
use datafusion_expr::expr::{Exists, InSubquery};
use datafusion_expr::expr_rewriter::create_col_from_scalar_expr;
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::utils::{conjunction, expr_to_columns, split_conjunction_owned};
use datafusion_expr::{
    exists, in_subquery, lit, not, not_exists, not_in_subquery, BinaryExpr, Expr, Filter,
    LogicalPlan, LogicalPlanBuilder, Operator,
};

use log::debug;

/// Optimizer rule for rewriting predicate(IN/EXISTS) subquery to left semi/anti joins
#[derive(Default, Debug)]
pub struct DecorrelatePredicateSubquery {}

impl DecorrelatePredicateSubquery {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for DecorrelatePredicateSubquery {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let plan = plan
            .map_subqueries(|subquery| {
                subquery.transform_down(|p| self.rewrite(p, config))
            })?
            .data;

        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        if !has_subquery(&filter.predicate) {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        let (with_subqueries, mut other_exprs): (Vec<_>, Vec<_>) =
            split_conjunction_owned(filter.predicate)
                .into_iter()
                .partition(has_subquery);

        assert_or_internal_err!(
            !with_subqueries.is_empty(),
            "can not find expected subqueries in DecorrelatePredicateSubquery"
        );

        // iterate through all exists clauses in predicate, turning each into a join
        let mut cur_input = Arc::unwrap_or_clone(filter.input);
        for subquery_expr in with_subqueries {
            match extract_subquery_info(subquery_expr) {
                // The subquery expression is at the top level of the filter
                SubqueryPredicate::Top(subquery) => {
                    match build_join_top(&subquery, &cur_input, config.alias_generator())?
                    {
                        Some(plan) => cur_input = plan,
                        // If the subquery can not be converted to a Join, reconstruct the subquery expression and add it to the Filter
                        None => other_exprs.push(subquery.expr()),
                    }
                }
                // The subquery expression is embedded within another expression
                SubqueryPredicate::Embedded(expr) => {
                    let (plan, expr_without_subqueries) =
                        rewrite_inner_subqueries(cur_input, expr, config)?;
                    cur_input = plan;
                    other_exprs.push(expr_without_subqueries);
                }
            }
        }

        let expr = conjunction(other_exprs);
        if let Some(expr) = expr {
            let new_filter = Filter::try_new(expr, Arc::new(cur_input))?;
            cur_input = LogicalPlan::Filter(new_filter);
        }
        Ok(Transformed::yes(cur_input))
    }

    fn name(&self) -> &str {
        "decorrelate_predicate_subquery"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn rewrite_inner_subqueries(
    outer: LogicalPlan,
    expr: Expr,
    config: &dyn OptimizerConfig,
) -> Result<(LogicalPlan, Expr)> {
    let mut cur_input = outer;
    let alias = config.alias_generator();
    let expr_without_subqueries = expr.transform(|e| match e {
        Expr::Exists(Exists {
            subquery: Subquery { subquery, .. },
            negated,
        }) => match mark_join(&cur_input, &subquery, None, negated, alias)? {
            Some((plan, exists_expr)) => {
                cur_input = plan;
                Ok(Transformed::yes(exists_expr))
            }
            None if negated => Ok(Transformed::no(not_exists(subquery))),
            None => Ok(Transformed::no(exists(subquery))),
        },
        Expr::InSubquery(InSubquery {
            expr,
            subquery: Subquery { subquery, .. },
            negated,
        }) => {
            let in_predicate = subquery
                .head_output_expr()?
                .map_or(plan_err!("single expression required."), |output_expr| {
                    Ok(Expr::eq(*expr.clone(), output_expr))
                })?;
            match mark_join(&cur_input, &subquery, Some(&in_predicate), negated, alias)? {
                Some((plan, exists_expr)) => {
                    cur_input = plan;
                    Ok(Transformed::yes(exists_expr))
                }
                None if negated => Ok(Transformed::no(not_in_subquery(*expr, subquery))),
                None => Ok(Transformed::no(in_subquery(*expr, subquery))),
            }
        }
        _ => Ok(Transformed::no(e)),
    })?;
    Ok((cur_input, expr_without_subqueries.data))
}

enum SubqueryPredicate {
    // The subquery expression is at the top level of the filter and can be fully replaced by a
    // semi/anti join
    Top(SubqueryInfo),
    // The subquery expression is embedded within another expression and is replaced using an
    // existence join
    Embedded(Expr),
}

fn extract_subquery_info(expr: Expr) -> SubqueryPredicate {
    match expr {
        Expr::Not(not_expr) => match *not_expr {
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => SubqueryPredicate::Top(SubqueryInfo::new_with_in_expr(
                subquery, *expr, !negated,
            )),
            Expr::Exists(Exists { subquery, negated }) => {
                SubqueryPredicate::Top(SubqueryInfo::new(subquery, !negated))
            }
            expr => SubqueryPredicate::Embedded(not(expr)),
        },
        Expr::InSubquery(InSubquery {
            expr,
            subquery,
            negated,
        }) => SubqueryPredicate::Top(SubqueryInfo::new_with_in_expr(
            subquery, *expr, negated,
        )),
        Expr::Exists(Exists { subquery, negated }) => {
            SubqueryPredicate::Top(SubqueryInfo::new(subquery, negated))
        }
        expr => SubqueryPredicate::Embedded(expr),
    }
}

fn has_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::InSubquery(_) | Expr::Exists(_) => Ok(true),
        _ => Ok(false),
    })
    .unwrap()
}

/// Optimize the subquery to left-anti/left-semi join.
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
///       Projection: t2.a, t2.b, t2.c
///         TableScan: t2
/// ```
///
/// Given another query like:
/// `select t1.id from t1 where exists(SELECT t2.id FROM t2 WHERE t1.id = t2.id)`
///
/// The optimized plan will be:
///
/// ```text
/// Projection: t1.id
///   LeftSemi Join:  Filter: t1.id = __correlated_sq_1.id
///     TableScan: t1
///     SubqueryAlias: __correlated_sq_1
///       Projection: t2.id
///         TableScan: t2
/// ```
fn build_join_top(
    query_info: &SubqueryInfo,
    left: &LogicalPlan,
    alias: &Arc<AliasGenerator>,
) -> Result<Option<LogicalPlan>> {
    let where_in_expr_opt = &query_info.where_in_expr;
    let in_predicate_opt = where_in_expr_opt
        .clone()
        .map(|where_in_expr| {
            query_info
                .query
                .subquery
                .head_output_expr()?
                .map_or(plan_err!("single expression required."), |expr| {
                    Ok(Expr::eq(where_in_expr, expr))
                })
        })
        .map_or(Ok(None), |v| v.map(Some))?;

    let join_type = match query_info.negated {
        true => JoinType::LeftAnti,
        false => JoinType::LeftSemi,
    };
    let subquery = query_info.query.subquery.as_ref();
    let subquery_alias = alias.next("__correlated_sq");
    build_join(
        left,
        subquery,
        in_predicate_opt.as_ref(),
        join_type,
        subquery_alias,
    )
}

/// This is used to handle the case when the subquery is embedded in a more complex boolean
/// expression like and OR. For example
///
/// `select t1.id from t1 where t1.id < 0 OR exists(SELECT t2.id FROM t2 WHERE t1.id = t2.id)`
///
/// The optimized plan will be:
///
/// ```text
/// Projection: t1.id
///   Filter: t1.id < 0 OR __correlated_sq_1.mark
///     LeftMark Join:  Filter: t1.id = __correlated_sq_1.id
///       TableScan: t1
///       SubqueryAlias: __correlated_sq_1
///         Projection: t2.id
///           TableScan: t2
fn mark_join(
    left: &LogicalPlan,
    subquery: &LogicalPlan,
    in_predicate_opt: Option<&Expr>,
    negated: bool,
    alias_generator: &Arc<AliasGenerator>,
) -> Result<Option<(LogicalPlan, Expr)>> {
    let alias = alias_generator.next("__correlated_sq");

    let exists_col = Expr::Column(Column::new(Some(alias.clone()), "mark"));
    let exists_expr = if negated { !exists_col } else { exists_col };

    Ok(
        build_join(left, subquery, in_predicate_opt, JoinType::LeftMark, alias)?
            .map(|plan| (plan, exists_expr)),
    )
}

fn build_join(
    left: &LogicalPlan,
    subquery: &LogicalPlan,
    in_predicate_opt: Option<&Expr>,
    join_type: JoinType,
    alias: String,
) -> Result<Option<LogicalPlan>> {
    let mut pull_up = PullUpCorrelatedExpr::new()
        .with_in_predicate_opt(in_predicate_opt.cloned())
        .with_exists_sub_query(in_predicate_opt.is_none());

    let new_plan = subquery.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let sub_query_alias = LogicalPlanBuilder::from(new_plan)
        .alias(alias.to_string())?
        .build()?;
    let mut all_correlated_cols = BTreeSet::new();
    pull_up
        .correlated_subquery_cols_map
        .values()
        .for_each(|cols| all_correlated_cols.extend(cols.clone()));

    // alias the join filter
    let join_filter_opt = conjunction(pull_up.join_filters)
        .map_or(Ok(None), |filter| {
            replace_qualified_name(filter, &all_correlated_cols, &alias).map(Some)
        })?;

    let join_filter = match (join_filter_opt, in_predicate_opt.cloned()) {
        (
            Some(join_filter),
            Some(Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            })),
        ) => {
            let right_col = create_col_from_scalar_expr(right.deref(), alias)?;
            let in_predicate = Expr::eq(left.deref().clone(), Expr::Column(right_col));
            in_predicate.and(join_filter)
        }
        (Some(join_filter), _) => join_filter,
        (
            _,
            Some(Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Eq,
                right,
            })),
        ) => {
            let right_col = create_col_from_scalar_expr(right.deref(), alias)?;
            let in_predicate = Expr::eq(left.deref().clone(), Expr::Column(right_col));
            in_predicate
        }
        (None, None) => lit(true),
        _ => return Ok(None),
    };

    if matches!(join_type, JoinType::LeftMark | JoinType::RightMark) {
        let right_schema = sub_query_alias.schema();

        // Gather all columns needed for the join filter + predicates
        let mut needed = std::collections::HashSet::new();
        expr_to_columns(&join_filter, &mut needed)?;
        if let Some(in_pred) = in_predicate_opt {
            expr_to_columns(in_pred, &mut needed)?;
        }

        // Keep only columns that actually belong to the RIGHT child, and sort by their
        // position in the right schema for deterministic order.
        let mut right_cols_idx_and_col: Vec<(usize, Column)> = needed
            .into_iter()
            .filter_map(|c| right_schema.index_of_column(&c).ok().map(|idx| (idx, c)))
            .collect();

        right_cols_idx_and_col.sort_by_key(|(idx, _)| *idx);

        let right_proj_exprs: Vec<Expr> = right_cols_idx_and_col
            .into_iter()
            .map(|(_, c)| Expr::Column(c))
            .collect();

        let right_projected = if !right_proj_exprs.is_empty() {
            LogicalPlanBuilder::from(sub_query_alias.clone())
                .project(right_proj_exprs)?
                .build()?
        } else {
            // Degenerate case: no right columns referenced by the predicate(s)
            sub_query_alias.clone()
        };
        let new_plan = LogicalPlanBuilder::from(left.clone())
            .join_on(right_projected, join_type, Some(join_filter))?
            .build()?;

        debug!(
            "predicate subquery optimized:\n{}",
            new_plan.display_indent()
        );

        return Ok(Some(new_plan));
    }

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(left.clone())
        .join_on(sub_query_alias, join_type, Some(join_filter))?
        .build()?;
    debug!(
        "predicate subquery optimized:\n{}",
        new_plan.display_indent()
    );
    Ok(Some(new_plan))
}

#[derive(Debug)]
struct SubqueryInfo {
    query: Subquery,
    where_in_expr: Option<Expr>,
    negated: bool,
}

impl SubqueryInfo {
    pub fn new(query: Subquery, negated: bool) -> Self {
        Self {
            query,
            where_in_expr: None,
            negated,
        }
    }

    pub fn new_with_in_expr(query: Subquery, expr: Expr, negated: bool) -> Self {
        Self {
            query,
            where_in_expr: Some(expr),
            negated,
        }
    }

    pub fn expr(self) -> Expr {
        match self.where_in_expr {
            Some(expr) => match self.negated {
                true => not_in_subquery(expr, self.query.subquery),
                false => in_subquery(expr, self.query.subquery),
            },
            None => match self.negated {
                true => not_exists(self.query.subquery),
                false => exists(self.query.subquery),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use super::*;
    use crate::test::*;

    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_expr::builder::table_source;
    use datafusion_expr::{and, binary_expr, col, lit, not, out_ref_col, table_scan};

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(DecorrelatePredicateSubquery::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.b = __correlated_sq_2.c [a:UInt32, b:UInt32, c:UInt32]
            LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
              SubqueryAlias: __correlated_sq_1 [c:UInt32]
                Projection: sq_1.c [c:UInt32]
                  TableScan: sq_1 [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_2 [c:UInt32]
              Projection: sq_2.c [c:UInt32]
                TableScan: sq_2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: test.a = UInt32(1) AND test.b < UInt32(30) [a:UInt32, b:UInt32, c:UInt32]
            LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
              TableScan: test [a:UInt32, b:UInt32, c:UInt32]
              SubqueryAlias: __correlated_sq_1 [c:UInt32]
                Projection: sq.c [c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.b = __correlated_sq_2.a [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_2 [a:UInt32]
              Projection: sq.a [a:UInt32]
                LeftSemi Join:  Filter: sq.a = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
                  SubqueryAlias: __correlated_sq_1 [c:UInt32]
                    Projection: sq_nested.c [c:UInt32]
                      TableScan: sq_nested [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test multiple correlated subqueries
    /// See subqueries.rs where_in_multiple()
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                in_subquery(col("customer.c_custkey"), Arc::clone(&orders))
                    .and(in_subquery(col("customer.c_custkey"), orders)),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;
        debug!("plan to optimize:\n{}", plan.display_indent());

        assert_optimized_plan_equal!(
                plan,
                @r###"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]
            LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
              TableScan: customer [c_custkey:Int64, c_name:Utf8]
              SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                Projection: orders.o_custkey [o_custkey:Int64]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            SubqueryAlias: __correlated_sq_2 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "###    
        )
    }

    /// Test recursive correlated subqueries
    /// See subqueries.rs where_in_recursive()
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    in_subquery(col("orders.o_orderkey"), lineitem).and(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                    ),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_2 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                LeftSemi Join:  Filter: orders.o_orderkey = __correlated_sq_1.l_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  SubqueryAlias: __correlated_sq_1 [l_orderkey:Int64]
                    Projection: lineitem.l_orderkey [l_orderkey:Int64]
                      TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]
        "
        )
    }

    /// Test for correlated IN subquery filter with additional subquery filters
    #[test]
    fn in_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery with no columns in schema
    #[test]
    fn in_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery not equal
    #[test]
    fn in_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .not_eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND customer.c_custkey != __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery less than
    #[test]
    fn in_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .lt(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND customer.c_custkey < __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery filter with subquery disjunction
    #[test]
    fn in_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND (customer.c_custkey = __correlated_sq_1.o_custkey OR __correlated_sq_1.o_orderkey = Int32(1)) [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64, o_orderkey:Int64]
              Projection: orders.o_custkey, orders.o_orderkey [o_custkey:Int64, o_orderkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
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
        let expected = "Invalid (non-executable) plan after Analyzer\
        \ncaused by\
        \nError during planning: InSubquery should only return one column, but found 4";
        assert_analyzer_check_err(vec![], plan, expected);

        Ok(())
    }

    /// Test for correlated IN subquery join on expression
    #[test]
    fn in_subquery_join_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey").add(lit(1)), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey + Int32(1) = __correlated_sq_1.o_custkey AND customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN expressions
    #[test]
    fn in_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(in_subquery(col("customer.c_custkey"), sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.orders.o_custkey + Int32(1) AND customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [orders.o_custkey + Int32(1):Int64, o_custkey:Int64]
              Projection: orders.o_custkey + Int32(1), orders.o_custkey [orders.o_custkey + Int32(1):Int64, o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery multiple projected columns
    #[test]
    fn in_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
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

        let expected = "Invalid (non-executable) plan after Analyzer\
        \ncaused by\
        \nError during planning: InSubquery should only return one column";
        assert_analyzer_check_err(vec![], plan, expected);

        Ok(())
    }

    /// Test for correlated IN subquery filter with additional filters
    #[test]
    fn should_support_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
            LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
              TableScan: customer [c_custkey:Int64, c_name:Utf8]
              SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                Projection: orders.o_custkey [o_custkey:Int64]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated IN subquery filter
    #[test]
    fn in_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(in_subquery(col("c"), sq))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c = __correlated_sq_1.c AND test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32, a:UInt32]
              Projection: sq.c, sq.a [c:UInt32, a:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for single IN subquery filter
    #[test]
    fn in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for single NOT IN subquery filter
    #[test]
    fn not_in_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_in_subquery(col("c"), test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftAnti Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn wrapped_not_in_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not(in_subquery(col("c"), test_subquery_with_name("sq")?)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftAnti Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn wrapped_not_not_in_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not(not_in_subquery(
                col("c"),
                test_subquery_with_name("sq")?,
            )))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.sq.c * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [sq.c * UInt32(2):UInt32]
              Projection: sq.c * UInt32(2) [sq.c * UInt32(2):UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn in_subquery_join_filter_and_inner_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                out_ref_col(DataType::UInt32, "test.a")
                    .eq(col("sq.a"))
                    .and(col("sq.a").add(lit(1u32)).eq(col("sq.b"))),
            )?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c") + lit(1u32), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.sq.c * UInt32(2) AND test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [sq.c * UInt32(2):UInt32, a:UInt32]
              Projection: sq.c * UInt32(2), sq.a [sq.c * UInt32(2):UInt32, a:UInt32]
                Filter: sq.a + UInt32(1) = sq.b [a:UInt32, b:UInt32, c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn in_subquery_multi_project_subquery_cols() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;

        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                out_ref_col(DataType::UInt32, "test.a")
                    .add(out_ref_col(DataType::UInt32, "test.b"))
                    .eq(col("sq.a").add(col("sq.b")))
                    .and(col("sq.a").add(lit(1u32)).eq(col("sq.b"))),
            )?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(in_subquery(col("c") + lit(1u32), Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.sq.c * UInt32(2) AND test.a + test.b = __correlated_sq_1.a + __correlated_sq_1.b [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [sq.c * UInt32(2):UInt32, a:UInt32, b:UInt32]
              Projection: sq.c * UInt32(2), sq.a, sq.b [sq.c * UInt32(2):UInt32, a:UInt32, b:UInt32]
                Filter: sq.a + UInt32(1) = sq.b [a:UInt32, b:UInt32, c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn two_in_subquery_with_outer_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .filter(out_ref_col(DataType::UInt32, "test.a").gt(col("sq1.a")))?
            .project(vec![col("c") * lit(2u32)])?
            .build()?;

        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .filter(out_ref_col(DataType::UInt32, "test.a").gt(col("sq2.a")))?
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

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: test.c > UInt32(1) [a:UInt32, b:UInt32, c:UInt32]
            LeftSemi Join:  Filter: test.c * UInt32(2) = __correlated_sq_2.sq2.c * UInt32(2) AND test.a > __correlated_sq_2.a [a:UInt32, b:UInt32, c:UInt32]
              LeftSemi Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.sq1.c * UInt32(2) AND test.a > __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]
                TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                SubqueryAlias: __correlated_sq_1 [sq1.c * UInt32(2):UInt32, a:UInt32]
                  Projection: sq1.c * UInt32(2), sq1.a [sq1.c * UInt32(2):UInt32, a:UInt32]
                    TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]
              SubqueryAlias: __correlated_sq_2 [sq2.c * UInt32(2):UInt32, a:UInt32]
                Projection: sq2.c * UInt32(2), sq2.a [sq2.c * UInt32(2):UInt32, a:UInt32]
                  TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
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
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: test.a = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: test.c [c:UInt32]
                Filter: test.a > test.b [a:UInt32, b:UInt32, c:UInt32]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for multiple exists subqueries in the same filter expression
    #[test]
    fn multiple_exists_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(Arc::clone(&orders)).and(exists(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: __correlated_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]
            LeftSemi Join:  Filter: __correlated_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]
              TableScan: customer [c_custkey:Int64, c_name:Utf8]
              SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                Projection: orders.o_custkey [o_custkey:Int64]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            SubqueryAlias: __correlated_sq_2 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_exists_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .project(vec![col("lineitem.l_orderkey")])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    exists(lineitem).and(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                    ),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(orders))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: __correlated_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_2 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                LeftSemi Join:  Filter: __correlated_sq_1.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  SubqueryAlias: __correlated_sq_1 [l_orderkey:Int64]
                    Projection: lineitem.l_orderkey [l_orderkey:Int64]
                      TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]
        "
        )
    }

    /// Test for correlated exists subquery filter with additional subquery filters
    #[test]
    fn exists_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    #[test]
    fn exists_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(out_ref_col(DataType::Int64, "customer.c_custkey").eq(lit(1u32)))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Other rule will pushdown `customer.c_custkey = 1`,
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = UInt32(1) [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for exists subquery with both columns in schema
    #[test]
    fn exists_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery not equal
    #[test]
    fn exists_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .not_eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey != __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery less than
    #[test]
    fn exists_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .lt(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey < __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
              Projection: orders.o_custkey [o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery filter with subquery disjunction
    #[test]
    fn exists_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey OR __correlated_sq_1.o_orderkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64, o_orderkey:Int64]
              Projection: orders.o_custkey, orders.o_orderkey [o_custkey:Int64, o_orderkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists without projection
    #[test]
    fn exists_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
              TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists expressions
    #[test]
    fn exists_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [orders.o_custkey + Int32(1):Int64, o_custkey:Int64]
              Projection: orders.o_custkey + Int32(1), orders.o_custkey [orders.o_custkey + Int32(1):Int64, o_custkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery filter with additional filters
    #[test]
    fn exists_subquery_should_support_additional_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq).and(col("c_custkey").eq(lit(1))))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8]
            LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8]
              TableScan: customer [c_custkey:Int64, c_name:Utf8]
              SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                Projection: orders.o_custkey [o_custkey:Int64]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery filter with disjunctions
    #[test]
    fn exists_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(exists(sq).or(col("customer.c_custkey").eq(lit(1))))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: __correlated_sq_1.mark OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, mark:Boolean]
            LeftMark Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, mark:Boolean]
              TableScan: customer [c_custkey:Int64, c_name:Utf8]
              SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                Projection: orders.o_custkey [o_custkey:Int64]
                  Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated EXISTS subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .project(vec![col("c")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(exists(sq))?
            .project(vec![col("test.c")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c [c:UInt32]
          LeftSemi Join:  Filter: test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32, a:UInt32]
              Projection: sq.c, sq.a [c:UInt32, a:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for single exists subquery filter
    #[test]
    fn exists_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for single NOT exists subquery filter
    #[test]
    fn not_exists_subquery_simple() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(not_exists(test_subquery_with_name("sq")?))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftAnti Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn two_exists_subquery_with_outer_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan1 = test_table_scan_with_name("sq1")?;
        let subquery_scan2 = test_table_scan_with_name("sq2")?;

        let subquery1 = LogicalPlanBuilder::from(subquery_scan1)
            .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq1.a")))?
            .project(vec![col("c")])?
            .build()?;

        let subquery2 = LogicalPlanBuilder::from(subquery_scan2)
            .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq2.a")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(
                exists(Arc::new(subquery1))
                    .and(exists(Arc::new(subquery2)).and(col("test.c").gt(lit(1u32)))),
            )?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: test.c > UInt32(1) [a:UInt32, b:UInt32, c:UInt32]
            LeftSemi Join:  Filter: test.a = __correlated_sq_2.a [a:UInt32, b:UInt32, c:UInt32]
              LeftSemi Join:  Filter: test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32]
                TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                SubqueryAlias: __correlated_sq_1 [c:UInt32, a:UInt32]
                  Projection: sq1.c, sq1.a [c:UInt32, a:UInt32]
                    TableScan: sq1 [a:UInt32, b:UInt32, c:UInt32]
              SubqueryAlias: __correlated_sq_2 [c:UInt32, a:UInt32]
                Projection: sq2.c, sq2.a [c:UInt32, a:UInt32]
                  TableScan: sq2 [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_subquery_expr_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                (lit(1u32) + col("sq.a"))
                    .gt(out_ref_col(DataType::UInt32, "test.a") * lit(2u32)),
            )?
            .project(vec![lit(1u32)])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: UInt32(1) + __correlated_sq_1.a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [UInt32(1):UInt32, a:UInt32]
              Projection: UInt32(1), sq.a [UInt32(1):UInt32, a:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_subquery_with_same_table() -> Result<()> {
        let outer_scan = test_table_scan()?;
        let subquery_scan = test_table_scan()?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(col("test.a").gt(col("test.b")))?
            .project(vec![col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        // Subquery and outer query refer to the same table.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: test.c [c:UInt32]
                Filter: test.a > test.b [a:UInt32, b:UInt32, c:UInt32]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_distinct_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                (lit(1u32) + col("sq.a"))
                    .gt(out_ref_col(DataType::UInt32, "test.a") * lit(2u32)),
            )?
            .project(vec![col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: UInt32(1) + __correlated_sq_1.a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32, a:UInt32]
              Distinct: [c:UInt32, a:UInt32]
                Projection: sq.c, sq.a [c:UInt32, a:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_distinct_expr_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                (lit(1u32) + col("sq.a"))
                    .gt(out_ref_col(DataType::UInt32, "test.a") * lit(2u32)),
            )?
            .project(vec![col("sq.b") + col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: UInt32(1) + __correlated_sq_1.a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [sq.b + sq.c:UInt32, a:UInt32]
              Distinct: [sq.b + sq.c:UInt32, a:UInt32]
                Projection: sq.b + sq.c, sq.a [sq.b + sq.c:UInt32, a:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_distinct_subquery_with_literal() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_scan = test_table_scan_with_name("sq")?;
        let subquery = LogicalPlanBuilder::from(subquery_scan)
            .filter(
                (lit(1u32) + col("sq.a"))
                    .gt(out_ref_col(DataType::UInt32, "test.a") * lit(2u32)),
            )?
            .project(vec![lit(1u32), col("sq.c")])?
            .distinct()?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: UInt32(1) + __correlated_sq_1.a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [UInt32(1):UInt32, c:UInt32, a:UInt32]
              Distinct: [UInt32(1):UInt32, c:UInt32, a:UInt32]
                Projection: UInt32(1), sq.c, sq.a [UInt32(1):UInt32, c:UInt32, a:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn exists_uncorrelated_unnest() -> Result<()> {
        let subquery_table_source = table_source(&Schema::new(vec![Field::new(
            "arr",
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
            true,
        )]));
        let subquery = LogicalPlanBuilder::scan_with_filters(
            "sq",
            subquery_table_source,
            None,
            vec![],
        )?
        .unnest_column("arr")?
        .build()?;
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [arr:Int32;N]
              Unnest: lists[sq.arr|depth=1] structs[] [arr:Int32;N]
                TableScan: sq [arr:List(Field { data_type: Int32, nullable: true });N]
        "
        )
    }

    #[test]
    fn exists_correlated_unnest() -> Result<()> {
        let table_scan = test_table_scan()?;
        let subquery_table_source = table_source(&Schema::new(vec![Field::new(
            "a",
            DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
            true,
        )]));
        let subquery = LogicalPlanBuilder::scan_with_filters(
            "sq",
            subquery_table_source,
            None,
            vec![],
        )?
        .unnest_column("a")?
        .filter(col("a").eq(out_ref_col(DataType::UInt32, "test.b")))?
        .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: __correlated_sq_1.a = test.b [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [a:UInt32;N]
              Unnest: lists[sq.a|depth=1] structs[] [a:UInt32;N]
                TableScan: sq [a:List(Field { data_type: UInt32, nullable: true });N]
        "
        )
    }

    #[test]
    fn upper_case_ident() -> Result<()> {
        let fields = vec![
            Field::new("A", DataType::UInt32, false),
            Field::new("B", DataType::UInt32, false),
        ];

        let schema = Schema::new(fields);
        let table_scan_a = table_scan(Some("\"TEST_A\""), &schema, None)?.build()?;
        let table_scan_b = table_scan(Some("\"TEST_B\""), &schema, None)?.build()?;

        let subquery = LogicalPlanBuilder::from(table_scan_b)
            .filter(col("\"A\"").eq(out_ref_col(DataType::UInt32, "\"TEST_A\".\"A\"")))?
            .project(vec![lit(1)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(table_scan_a)
            .filter(exists(Arc::new(subquery)))?
            .project(vec![col("\"TEST_A\".\"B\"")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: TEST_A.B [B:UInt32]
          LeftSemi Join:  Filter: __correlated_sq_1.A = TEST_A.A [A:UInt32, B:UInt32]
            TableScan: TEST_A [A:UInt32, B:UInt32]
            SubqueryAlias: __correlated_sq_1 [Int32(1):Int32, A:UInt32]
              Projection: Int32(1), TEST_B.A [Int32(1):Int32, A:UInt32]
                TableScan: TEST_B [A:UInt32, B:UInt32]
        "
        )
    }
}
