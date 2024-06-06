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

//! [`ScalarSubqueryToJoin`] rewriting scalar subquery filters to `JOIN`s

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use crate::optimizer::ApplyOrder;
use crate::utils::replace_qualified_name;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{internal_err, plan_err, Column, Result, ScalarValue};
use datafusion_expr::expr_rewriter::create_col_from_scalar_expr;
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{expr, EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder};

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct ScalarSubqueryToJoin {}

impl ScalarSubqueryToJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that have a scalar subquery in them (and recurses when found)
    ///
    /// # Arguments
    /// * `predicate` - A conjunction to split and search
    ///
    /// Returns a tuple (subqueries, alias)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        alias_gen: Arc<AliasGenerator>,
    ) -> Result<(Vec<(Subquery, String)>, Expr)> {
        let mut extract = ExtractScalarSubQuery {
            sub_query_info: vec![],
            alias_gen,
        };
        predicate
            .clone()
            .rewrite(&mut extract)
            .data()
            .map(|new_expr| (extract.sub_query_info, new_expr))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called ScalarSubqueryToJoin::rewrite")
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                // Optimization: skip the rest of the rule and its copies if
                // there are no scalar subqueries
                if !contains_scalar_subquery(&filter.predicate) {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                let (subqueries, mut rewrite_expr) = self.extract_subquery_exprs(
                    &filter.predicate,
                    config.alias_generator(),
                )?;

                if subqueries.is_empty() {
                    return internal_err!("Expected subqueries not found in filter");
                }

                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = filter.input.as_ref().clone();
                for (subquery, alias) in subqueries {
                    if let Some((optimized_subquery, expr_check_map)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        if !expr_check_map.is_empty() {
                            rewrite_expr = rewrite_expr
                                .transform_up(|expr| {
                                    // replace column references with entry in map, if it exists
                                    if let Some(map_expr) = expr
                                        .try_as_col()
                                        .and_then(|col| expr_check_map.get(&col.name))
                                    {
                                        Ok(Transformed::yes(map_expr.clone()))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .data()?;
                        }
                        cur_input = optimized_subquery;
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                    }
                }
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .filter(rewrite_expr)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::Projection(projection) => {
                // Optimization: skip the rest of the rule and its copies if
                // there are no scalar subqueries
                if !projection.expr.iter().any(contains_scalar_subquery) {
                    return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                }

                let mut all_subqueryies = vec![];
                let mut expr_to_rewrite_expr_map = HashMap::new();
                let mut subquery_to_expr_map = HashMap::new();
                for expr in projection.expr.iter() {
                    let (subqueries, rewrite_exprs) =
                        self.extract_subquery_exprs(expr, config.alias_generator())?;
                    for (subquery, _) in &subqueries {
                        subquery_to_expr_map.insert(subquery.clone(), expr.clone());
                    }
                    all_subqueryies.extend(subqueries);
                    expr_to_rewrite_expr_map.insert(expr, rewrite_exprs);
                }
                if all_subqueryies.is_empty() {
                    return internal_err!("Expected subqueries not found in projection");
                }
                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = projection.input.as_ref().clone();
                for (subquery, alias) in all_subqueryies {
                    if let Some((optimized_subquery, expr_check_map)) =
                        build_join(&subquery, &cur_input, &alias)?
                    {
                        cur_input = optimized_subquery;
                        if !expr_check_map.is_empty() {
                            if let Some(expr) = subquery_to_expr_map.get(&subquery) {
                                if let Some(rewrite_expr) =
                                    expr_to_rewrite_expr_map.get(expr)
                                {
                                    let new_expr = rewrite_expr
                                        .clone()
                                        .transform_up(|expr| {
                                            // replace column references with entry in map, if it exists
                                            if let Some(map_expr) =
                                                expr.try_as_col().and_then(|col| {
                                                    expr_check_map.get(&col.name)
                                                })
                                            {
                                                Ok(Transformed::yes(map_expr.clone()))
                                            } else {
                                                Ok(Transformed::no(expr))
                                            }
                                        })
                                        .data()?;
                                    expr_to_rewrite_expr_map.insert(expr, new_expr);
                                }
                            }
                        }
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                    }
                }

                let mut proj_exprs = vec![];
                for expr in projection.expr.iter() {
                    let old_expr_name = expr.display_name()?;
                    let new_expr = expr_to_rewrite_expr_map.get(expr).unwrap();
                    let new_expr_name = new_expr.display_name()?;
                    if new_expr_name != old_expr_name {
                        proj_exprs.push(new_expr.clone().alias(old_expr_name))
                    } else {
                        proj_exprs.push(new_expr.clone());
                    }
                }
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .project(proj_exprs)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }

            plan => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Returns true if the expression has a scalar subquery somewhere in it
/// false otherwise
fn contains_scalar_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| Ok(matches!(expr, Expr::ScalarSubquery(_))))
        .expect("Inner is always Ok")
}

struct ExtractScalarSubQuery {
    sub_query_info: Vec<(Subquery, String)>,
    alias_gen: Arc<AliasGenerator>,
}

impl TreeNodeRewriter for ExtractScalarSubQuery {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::ScalarSubquery(subquery) => {
                let subqry_alias = self.alias_gen.next("__scalar_sq");
                self.sub_query_info
                    .push((subquery.clone(), subqry_alias.clone()));
                let scalar_expr = subquery
                    .subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), Ok)?;
                Ok(Transformed::new(
                    Expr::Column(create_col_from_scalar_expr(
                        &scalar_expr,
                        subqry_alias,
                    )?),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
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
/// left join (select c_id, avg(total) as val from orders group by c_id) o on o.c_id = c.c_id
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
/// left join (select avg(total) as val from orders) a
/// where c.balance > a.val
/// ```
///
/// # Arguments
///
/// * `query_info` - The subquery portion of the `where` (select avg(total) from orders)
/// * `filter_input` - The non-subquery portion (from customers)
/// * `outer_others` - Any additional parts to the `where` expression (and c.x = y)
/// * `subquery_alias` - Subquery aliases
fn build_join(
    subquery: &Subquery,
    filter_input: &LogicalPlan,
    subquery_alias: &str,
) -> Result<Option<(LogicalPlan, HashMap<String, Expr>)>> {
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let new_plan = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let collected_count_expr_map =
        pull_up.collected_count_expr_map.get(&new_plan).cloned();
    let sub_query_alias = LogicalPlanBuilder::from(new_plan)
        .alias(subquery_alias.to_string())?
        .build()?;

    let mut all_correlated_cols = BTreeSet::new();
    pull_up
        .correlated_subquery_cols_map
        .values()
        .for_each(|cols| all_correlated_cols.extend(cols.clone()));

    // alias the join filter
    let join_filter_opt =
        conjunction(pull_up.join_filters).map_or(Ok(None), |filter| {
            replace_qualified_name(filter, &all_correlated_cols, subquery_alias)
                .map(Option::Some)
        })?;

    // join our sub query into the main plan
    let new_plan = if join_filter_opt.is_none() {
        match filter_input {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: _,
            }) => sub_query_alias,
            _ => {
                // if not correlated, group down to 1 row and left join on that (preserving row count)
                LogicalPlanBuilder::from(filter_input.clone())
                    .join_on(sub_query_alias, JoinType::Left, None)?
                    .build()?
            }
        }
    } else {
        // left join if correlated, grouping by the join keys so we don't change row count
        LogicalPlanBuilder::from(filter_input.clone())
            .join_on(sub_query_alias, JoinType::Left, join_filter_opt)?
            .build()?
    };
    let mut computation_project_expr = HashMap::new();
    if let Some(expr_map) = collected_count_expr_map {
        for (name, result) in expr_map {
            let computer_expr = if let Some(filter) = &pull_up.pull_up_having_expr {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![
                        (
                            Box::new(Expr::IsNull(Box::new(Expr::Column(
                                Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                            )))),
                            Box::new(result),
                        ),
                        (
                            Box::new(Expr::Not(Box::new(filter.clone()))),
                            Box::new(Expr::Literal(ScalarValue::Null)),
                        ),
                    ],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            } else {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::IsNull(Box::new(Expr::Column(
                            Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                        )))),
                        Box::new(result),
                    )],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            };
            computation_project_expr.insert(name, computer_expr);
        }
    }

    Ok(Some((new_plan, computation_project_expr)))
}

// Add test to datafusion/core/tests/optimizer/scalar_subquery_to_join.rs
