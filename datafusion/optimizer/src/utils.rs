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

//! Collection of utility functions that are leveraged by the query optimizer rules

use crate::simplify_expressions::{ExprSimplifier, SimplifyContext};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter,
};
use datafusion_common::{plan_err, Column, DFSchemaRef, ScalarValue};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::{BinaryExpr, Sort};
use datafusion_expr::expr_rewriter::{
    replace_col, strip_outer_reference, unnormalize_col,
};
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::utils::from_plan;
use datafusion_expr::{
    and,
    logical_plan::{Filter, LogicalPlan},
    AggregateFunction, EmptyRelation, Expr, Operator,
};
use datafusion_physical_expr::execution_props::ExecutionProps;
use log::{debug, trace};
use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::Arc;

/// Convenience rule for writing optimizers: recursively invoke
/// optimize on plan's children and then return a node of the same
/// type. Useful for optimizer rules which want to leave the type
/// of plan unchanged but still apply to the children.
/// This also handles the case when the `plan` is a [`LogicalPlan::Explain`].
///
/// Returning `Ok(None)` indicates that the plan can't be optimized by the `optimizer`.
pub fn optimize_children(
    optimizer: &impl OptimizerRule,
    plan: &LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let new_exprs = plan.expressions();
    let mut new_inputs = Vec::with_capacity(plan.inputs().len());
    let mut plan_is_changed = false;
    for input in plan.inputs() {
        let new_input = optimizer.try_optimize(input, config)?;
        plan_is_changed = plan_is_changed || new_input.is_some();
        new_inputs.push(new_input.unwrap_or_else(|| input.clone()))
    }
    if plan_is_changed {
        Ok(Some(from_plan(plan, &new_exprs, &new_inputs)?))
    } else {
        Ok(None)
    }
}

/// Splits a conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// See [`split_conjunction_owned`] for more details and an example.
pub fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    split_conjunction_impl(expr, vec![])
}

fn split_conjunction_impl<'a>(expr: &'a Expr, mut exprs: Vec<&'a Expr>) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            right,
            op: Operator::And,
            left,
        }) => {
            let exprs = split_conjunction_impl(left, exprs);
            split_conjunction_impl(right, exprs)
        }
        Expr::Alias(expr, _) => split_conjunction_impl(expr, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Splits an owned conjunctive [`Expr`] such as `A AND B AND C` => `[A, B, C]`
///
/// This is often used to "split" filter expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::split_conjunction_owned;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_conjunction_owned to split them
/// assert_eq!(split_conjunction_owned(expr), split);
/// ```
pub fn split_conjunction_owned(expr: Expr) -> Vec<Expr> {
    split_binary_owned(expr, Operator::And)
}

/// Splits an owned binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// This is often used to "split" expressions such as `col1 = 5
/// AND col2 = 10` into [`col1 = 5`, `col2 = 10`];
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit, Operator};
/// # use datafusion_optimizer::utils::split_binary_owned;
/// # use std::ops::Add;
/// // a=1 + b=2
/// let expr = col("a").eq(lit(1)).add(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use split_binary_owned to split them
/// assert_eq!(split_binary_owned(expr, Operator::Plus), split);
/// ```
pub fn split_binary_owned(expr: Expr, op: Operator) -> Vec<Expr> {
    split_binary_owned_impl(expr, op, vec![])
}

fn split_binary_owned_impl(
    expr: Expr,
    operator: Operator,
    mut exprs: Vec<Expr>,
) -> Vec<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if op == operator => {
            let exprs = split_binary_owned_impl(*left, operator, exprs);
            split_binary_owned_impl(*right, operator, exprs)
        }
        Expr::Alias(expr, _) => split_binary_owned_impl(*expr, operator, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Splits an binary operator tree [`Expr`] such as `A <OP> B <OP> C` => `[A, B, C]`
///
/// See [`split_binary_owned`] for more details and an example.
pub fn split_binary(expr: &Expr, op: Operator) -> Vec<&Expr> {
    split_binary_impl(expr, op, vec![])
}

fn split_binary_impl<'a>(
    expr: &'a Expr,
    operator: Operator,
    mut exprs: Vec<&'a Expr>,
) -> Vec<&'a Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { right, op, left }) if *op == operator => {
            let exprs = split_binary_impl(left, operator, exprs);
            split_binary_impl(right, operator, exprs)
        }
        Expr::Alias(expr, _) => split_binary_impl(expr, operator, exprs),
        other => {
            exprs.push(other);
            exprs
        }
    }
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical AND.
///
/// Returns None if the filters array is empty.
///
/// # Example
/// ```
/// # use datafusion_expr::{col, lit};
/// # use datafusion_optimizer::utils::conjunction;
/// // a=1 AND b=2
/// let expr = col("a").eq(lit(1)).and(col("b").eq(lit(2)));
///
/// // [a=1, b=2]
/// let split = vec![
///   col("a").eq(lit(1)),
///   col("b").eq(lit(2)),
/// ];
///
/// // use conjunction to join them together with `AND`
/// assert_eq!(conjunction(split), Some(expr));
/// ```
pub fn conjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| accum.and(expr))
}

/// Combines an array of filter expressions into a single filter
/// expression consisting of the input filter expressions joined with
/// logical OR.
///
/// Returns None if the filters array is empty.
pub fn disjunction(filters: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    filters.into_iter().reduce(|accum, expr| accum.or(expr))
}

/// Recursively un-alias an expressions
#[inline]
pub fn unalias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(sub_expr, _) => unalias(*sub_expr),
        _ => expr,
    }
}

/// returns a new [LogicalPlan] that wraps `plan` in a [LogicalPlan::Filter] with
/// its predicate be all `predicates` ANDed.
pub fn add_filter(plan: LogicalPlan, predicates: &[&Expr]) -> Result<LogicalPlan> {
    // reduce filters to a single filter with an AND
    let predicate = predicates
        .iter()
        .skip(1)
        .fold(predicates[0].clone(), |acc, predicate| {
            and(acc, (*predicate).to_owned())
        });

    Ok(LogicalPlan::Filter(Filter::try_new(
        predicate,
        Arc::new(plan),
    )?))
}

/// Looks for correlating expressions: for example, a binary expression with one field from the subquery, and
/// one not in the subquery (closed upon from outer scope)
///
/// # Arguments
///
/// * `exprs` - List of expressions that may or may not be joins
///
/// # Return value
///
/// Tuple of (expressions containing joins, remaining non-join expressions)
pub fn find_join_exprs(exprs: Vec<&Expr>) -> Result<(Vec<Expr>, Vec<Expr>)> {
    let mut joins = vec![];
    let mut others = vec![];
    for filter in exprs.into_iter() {
        // If the expression contains correlated predicates, add it to join filters
        if filter.contains_outer() {
            if !matches!(filter, Expr::BinaryExpr(BinaryExpr{ left, op: Operator::Eq, right }) if left.eq(right))
            {
                joins.push(strip_outer_reference((*filter).clone()));
            }
        } else {
            others.push((*filter).clone());
        }
    }

    Ok((joins, others))
}

/// Returns the first (and only) element in a slice, or an error
///
/// # Arguments
///
/// * `slice` - The slice to extract from
///
/// # Return value
///
/// The first element, or an error
pub fn only_or_err<T>(slice: &[T]) -> Result<&T> {
    match slice {
        [it] => Ok(it),
        [] => plan_err!("No items found!"),
        _ => plan_err!("More than one item found!"),
    }
}

/// Rewrites `expr` using `rewriter`, ensuring that the output has the
/// same name as `expr` prior to rewrite, adding an alias if necessary.
///
/// This is important when optimizing plans to ensure the output
/// schema of plan nodes don't change after optimization
pub fn rewrite_preserving_name<R>(expr: Expr, rewriter: &mut R) -> Result<Expr>
where
    R: TreeNodeRewriter<N = Expr>,
{
    let original_name = name_for_alias(&expr)?;
    let expr = expr.rewrite(rewriter)?;
    add_alias_if_changed(original_name, expr)
}

/// Return the name to use for the specific Expr, recursing into
/// `Expr::Sort` as appropriate
fn name_for_alias(expr: &Expr) -> Result<String> {
    match expr {
        Expr::Sort(Sort { expr, .. }) => name_for_alias(expr),
        expr => expr.display_name(),
    }
}

/// Ensure `expr` has the name as `original_name` by adding an
/// alias if necessary.
fn add_alias_if_changed(original_name: String, expr: Expr) -> Result<Expr> {
    let new_name = name_for_alias(&expr)?;

    if new_name == original_name {
        return Ok(expr);
    }

    Ok(match expr {
        Expr::Sort(Sort {
            expr,
            asc,
            nulls_first,
        }) => {
            let expr = add_alias_if_changed(original_name, *expr)?;
            Expr::Sort(Sort::new(Box::new(expr), asc, nulls_first))
        }
        expr => expr.alias(original_name),
    })
}

/// merge inputs schema into a single schema.
pub fn merge_schema(inputs: Vec<&LogicalPlan>) -> DFSchema {
    if inputs.len() == 1 {
        inputs[0].schema().clone().as_ref().clone()
    } else {
        inputs.iter().map(|input| input.schema()).fold(
            DFSchema::empty(),
            |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            },
        )
    }
}

pub(crate) fn collect_subquery_cols(
    exprs: &[Expr],
    subquery_schema: DFSchemaRef,
) -> Result<BTreeSet<Column>> {
    exprs.iter().try_fold(BTreeSet::new(), |mut cols, expr| {
        let mut using_cols: Vec<Column> = vec![];
        for col in expr.to_columns()?.into_iter() {
            if subquery_schema.has_column(&col) {
                using_cols.push(col);
            }
        }

        cols.extend(using_cols);
        Result::<_>::Ok(cols)
    })
}

pub(crate) fn replace_qualified_name(
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

/// Log the plan in debug/tracing mode after some part of the optimizer runs
pub fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
}

/// This struct rewrite the sub query plan by pull up the correlated expressions(contains outer reference columns) from the inner subquery's [Filter].
/// It adds the inner reference columns to the 'Projection' or 'Aggregate' of the subquery if they are missing, so that they can be evaluated by the parent operator as the join condition.
pub struct PullUpCorrelatedExpr {
    pub join_filters: Vec<Expr>,
    // mapping from the plan to its holding correlated columns
    pub correlated_subquery_cols_map: HashMap<LogicalPlan, BTreeSet<Column>>,
    pub in_predicate_opt: Option<Expr>,
    // indicate whether it is Exists(Not Exists) SubQuery
    pub exists_sub_query: bool,
    // indicate whether the correlated expressions can pull up or not
    pub can_pull_up: bool,
    // indicate whether the subquery need to collect count expr mapping
    pub need_collect_count_expr_map: bool,
    // mapping from expr name to the pair of agg expr and its evaluation result on empty record batch
    pub collected_count_expr_map: HashMap<String, (Expr, Expr)>,
    pub expr_check_map: ExprCheckMap,
}

pub type ExprCheckMap = HashMap<String, (Expr, Expr)>;

impl TreeNodeRewriter for PullUpCorrelatedExpr {
    type N = LogicalPlan;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<RewriteRecursion> {
        match plan {
            LogicalPlan::Filter(_) => Ok(RewriteRecursion::Continue),
            LogicalPlan::Union(_) | LogicalPlan::Sort(_) | LogicalPlan::Extension(_) => {
                let plan_hold_outer = !plan.all_out_ref_exprs().is_empty();
                if plan_hold_outer {
                    // the unsupported case
                    self.can_pull_up = false;
                    Ok(RewriteRecursion::Stop)
                } else {
                    Ok(RewriteRecursion::Continue)
                }
            }
            LogicalPlan::Limit(_) => {
                let plan_hold_outer = !plan.all_out_ref_exprs().is_empty();
                match (self.exists_sub_query, plan_hold_outer) {
                    (false, true) => {
                        // the unsupported case
                        self.can_pull_up = false;
                        Ok(RewriteRecursion::Stop)
                    }
                    _ => Ok(RewriteRecursion::Continue),
                }
            }
            _ if plan.expressions().iter().any(|expr| expr.contains_outer()) => {
                // the unsupported cases, the plan expressions contain out reference columns(like window expressions)
                self.can_pull_up = false;
                Ok(RewriteRecursion::Stop)
            }
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let subquery_schema = plan.schema().clone();
        match &plan {
            LogicalPlan::Filter(plan_filter) => {
                let subquery_filter_exprs = split_conjunction(&plan_filter.predicate);
                let (mut join_filters, subquery_filters) =
                    find_join_exprs(subquery_filter_exprs)?;
                if let Some(in_predicate) = &self.in_predicate_opt {
                    // in_predicate may be already included in the join filters, remove it from the join filters first.
                    join_filters = remove_duplicated_filter(join_filters, in_predicate);
                }
                let correlated_subquery_cols =
                    collect_subquery_cols(&join_filters, subquery_schema)?;
                for expr in join_filters {
                    if !self.join_filters.contains(&expr) {
                        self.join_filters.push(expr)
                    }
                }
                // if the subquery still has filter expressions, restore them.
                let mut plan = LogicalPlanBuilder::from((*plan_filter.input).clone());
                if let Some(expr) = conjunction(subquery_filters) {
                    plan = plan.filter(expr)?
                }
                let new_plan = plan.build()?;
                self.correlated_subquery_cols_map
                    .insert(new_plan.clone(), correlated_subquery_cols);
                Ok(new_plan)
            }
            LogicalPlan::Projection(projection)
                if self.in_predicate_opt.is_some() || !self.join_filters.is_empty() =>
            {
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                // add missing columns to Projection
                let mut missing_exprs =
                    self.collect_missing_exprs(&projection.expr, &local_correlated_cols)?;
                if !self.collected_count_expr_map.is_empty() {
                    let head_expr = missing_exprs.get(0);
                    if let Some(expr) = head_expr {
                        let result_expr = expr.clone().transform_up(&|expr| {
                            if let Expr::Column(Column { name, .. }) = &expr {
                                if let Some((_, result_expr)) =
                                    self.collected_count_expr_map.get(name)
                                {
                                    Ok(Transformed::Yes(result_expr.clone()))
                                } else {
                                    Ok(Transformed::No(expr))
                                }
                            } else {
                                Ok(Transformed::No(expr))
                            }
                        })?;
                        let scalar_expr = match expr {
                            Expr::Alias(_, alias) => (
                                alias.to_string(),
                                Expr::Column(Column::new_unqualified(alias)),
                            ),
                            Expr::Column(Column { relation: _, name }) => {
                                (name.to_string(), expr.clone())
                            }
                            _ => {
                                let scalar_column = expr.display_name()?;
                                (
                                    scalar_column.clone(),
                                    Expr::Column(Column::new_unqualified(scalar_column)),
                                )
                            }
                        };
                        self.expr_check_map
                            .insert(scalar_expr.0, (scalar_expr.1, result_expr));
                        missing_exprs.push(Expr::Column(Column::new_unqualified(
                            "__always_true".to_string(),
                        )));
                    }
                }

                let new_plan = LogicalPlanBuilder::from((*projection.input).clone())
                    .project(missing_exprs)?
                    .build()?;
                Ok(new_plan)
            }
            LogicalPlan::Aggregate(aggregate)
                if self.in_predicate_opt.is_some() || !self.join_filters.is_empty() =>
            {
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                // add missing columns to Aggregation's group expression
                let mut missing_exprs = self.collect_missing_exprs(
                    &aggregate.group_expr,
                    &local_correlated_cols,
                )?;

                if self.need_collect_count_expr_map && aggregate.group_expr.is_empty() {
                    let agg_result_exprs = agg_exprs_eva_result_on_empty_batch(
                        &aggregate.aggr_expr,
                        subquery_schema,
                    )?;
                    if !missing_exprs.is_empty() {
                        let scalar_agg = !agg_result_exprs.values().any(|result_expr| {
                            matches!(result_expr, Expr::Literal(ScalarValue::Null))
                        });
                        if scalar_agg {
                            let internal_always_true_col = Expr::Alias(
                                Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)))),
                                "__always_true".to_string(),
                            );
                            missing_exprs.push(internal_always_true_col);
                            for (agg_expr, result_expr_on_empty) in agg_result_exprs {
                                let agg_expr_name = agg_expr.display_name()?;
                                self.collected_count_expr_map.insert(
                                    agg_expr_name,
                                    (agg_expr, result_expr_on_empty),
                                );
                            }
                        }
                    }
                }

                let new_plan = LogicalPlanBuilder::from((*aggregate.input).clone())
                    .aggregate(missing_exprs, aggregate.aggr_expr.to_vec())?
                    .build()?;
                Ok(new_plan)
            }
            LogicalPlan::SubqueryAlias(alias) => {
                let mut local_correlated_cols = BTreeSet::new();
                collect_local_correlated_cols(
                    &plan,
                    &self.correlated_subquery_cols_map,
                    &mut local_correlated_cols,
                );
                let mut new_correlated_cols = BTreeSet::new();
                for col in local_correlated_cols.iter() {
                    new_correlated_cols
                        .insert(Column::new(Some(alias.alias.clone()), col.name.clone()));
                }
                self.correlated_subquery_cols_map
                    .insert(plan.clone(), new_correlated_cols);
                Ok(plan)
            }
            LogicalPlan::Limit(limit) => {
                // handling the limit clause in the subquery
                match (self.exists_sub_query, self.join_filters.is_empty()) {
                    // un-correlated exist subquery, keep the limit
                    (true, true) => Ok(plan),
                    // Correlated exist subquery, remove the limit(so that correlated expressions can pull up)
                    (true, false) => {
                        if limit.fetch.filter(|limit_row| *limit_row == 0).is_some() {
                            Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                                produce_one_row: false,
                                schema: limit.input.schema().clone(),
                            }))
                        } else {
                            LogicalPlanBuilder::from((*limit.input).clone()).build()
                        }
                    }
                    _ => Ok(plan),
                }
            }
            _ => Ok(plan),
        }
    }
}

impl PullUpCorrelatedExpr {
    fn collect_missing_exprs(
        &self,
        exprs: &[Expr],
        correlated_subquery_cols: &BTreeSet<Column>,
    ) -> Result<Vec<Expr>> {
        let mut missing_exprs = vec![];
        if let Some(Expr::BinaryExpr(BinaryExpr {
            left: _,
            op: Operator::Eq,
            right,
        })) = &self.in_predicate_opt
        {
            if !matches!(right.deref(), Expr::Column(_))
                && !matches!(right.deref(), Expr::Literal(_))
                && !matches!(right.deref(), Expr::Alias(_, _))
            {
                let alias_expr = right
                    .deref()
                    .clone()
                    .alias(format!("{:?}", unnormalize_col(right.deref().clone())));
                missing_exprs.push(alias_expr)
            }
        }
        for expr in exprs {
            if !missing_exprs.contains(expr) {
                missing_exprs.push(expr.clone())
            }
        }
        for col in correlated_subquery_cols.iter() {
            let col_expr = Expr::Column(col.clone());
            if !missing_exprs.contains(&col_expr) {
                missing_exprs.push(col_expr)
            }
        }
        Ok(missing_exprs)
    }
}

fn collect_local_correlated_cols(
    plan: &LogicalPlan,
    all_cols_map: &HashMap<LogicalPlan, BTreeSet<Column>>,
    local_cols: &mut BTreeSet<Column>,
) {
    for child in plan.inputs() {
        if let Some(cols) = all_cols_map.get(child) {
            local_cols.extend(cols.clone());
        }
        // SubqueryAlias is treated as the leaf node
        if !matches!(child, LogicalPlan::SubqueryAlias(_)) {
            collect_local_correlated_cols(child, all_cols_map, local_cols);
        }
    }
}

fn remove_duplicated_filter(filters: Vec<Expr>, in_predicate: &Expr) -> Vec<Expr> {
    filters
        .into_iter()
        .filter(|filter| {
            if filter == in_predicate {
                return false;
            }

            // ignore the binary order
            !match (filter, in_predicate) {
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

fn agg_exprs_eva_result_on_empty_batch(
    agg_expr: &[Expr],
    schema: DFSchemaRef,
) -> Result<HashMap<Expr, Expr>> {
    let mut result_expr_map = HashMap::new();
    for e in agg_expr.iter() {
        let new_expr = e.clone().transform_up(&|expr| {
            let new_expr = match expr {
                Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
                    fun,
                    ..
                }) => {
                    if matches!(fun, AggregateFunction::Count) {
                        Transformed::Yes(Expr::Literal(ScalarValue::Int64(Some(0))))
                    } else {
                        Transformed::Yes(Expr::Literal(ScalarValue::Null))
                    }
                }
                Expr::AggregateUDF(_) => {
                    Transformed::Yes(Expr::Literal(ScalarValue::Null))
                }
                _ => Transformed::No(expr),
            };
            Ok(new_expr)
        })?;

        let props = ExecutionProps::new();
        let info = SimplifyContext::new(&props).with_schema(schema.clone());
        let simplifier = ExprSimplifier::new(info);
        result_expr_map.insert(e.clone(), simplifier.simplify(new_expr)?);
    }
    Ok(result_expr_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{col, lit, utils::expr_to_columns};
    use std::collections::HashSet;
    use std::ops::Add;

    #[test]
    fn test_split_conjunction() {
        let expr = col("a");
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_conjunction_two() {
        let expr = col("a").eq(lit(5)).and(col("b"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b");

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_alias() {
        let expr = col("a").eq(lit(5)).and(col("b").alias("the_alias"));
        let expr1 = col("a").eq(lit(5));
        let expr2 = col("b"); // has no alias

        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr1, &expr2]);
    }

    #[test]
    fn test_split_conjunction_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        let result = split_conjunction(&expr);
        assert_eq!(result, vec![&expr]);
    }

    #[test]
    fn test_split_binary_owned() {
        let expr = col("a");
        assert_eq!(split_binary_owned(expr.clone(), Operator::And), vec![expr]);
    }

    #[test]
    fn test_split_binary_owned_two() {
        assert_eq!(
            split_binary_owned(col("a").eq(lit(5)).and(col("b")), Operator::And),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_binary_owned_different_op() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(
            // expr is connected by OR, but pass in AND
            split_binary_owned(expr.clone(), Operator::And),
            vec![expr]
        );
    }

    #[test]
    fn test_split_conjunction_owned() {
        let expr = col("a");
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_split_conjunction_owned_two() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b"))),
            vec![col("a").eq(lit(5)), col("b")]
        );
    }

    #[test]
    fn test_split_conjunction_owned_alias() {
        assert_eq!(
            split_conjunction_owned(col("a").eq(lit(5)).and(col("b").alias("the_alias"))),
            vec![
                col("a").eq(lit(5)),
                // no alias on b
                col("b"),
            ]
        );
    }

    #[test]
    fn test_conjunction_empty() {
        assert_eq!(conjunction(vec![]), None);
    }

    #[test]
    fn test_conjunction() {
        // `[A, B, C]`
        let expr = conjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A AND B) AND C`
        assert_eq!(expr, Some(col("a").and(col("b")).and(col("c"))));

        // which is different than `A AND (B AND C)`
        assert_ne!(expr, Some(col("a").and(col("b").and(col("c")))));
    }

    #[test]
    fn test_disjunction_empty() {
        assert_eq!(disjunction(vec![]), None);
    }

    #[test]
    fn test_disjunction() {
        // `[A, B, C]`
        let expr = disjunction(vec![col("a"), col("b"), col("c")]);

        // --> `(A OR B) OR C`
        assert_eq!(expr, Some(col("a").or(col("b")).or(col("c"))));

        // which is different than `A OR (B OR C)`
        assert_ne!(expr, Some(col("a").or(col("b").or(col("c")))));
    }

    #[test]
    fn test_split_conjunction_owned_or() {
        let expr = col("a").eq(lit(5)).or(col("b"));
        assert_eq!(split_conjunction_owned(expr.clone()), vec![expr]);
    }

    #[test]
    fn test_collect_expr() -> Result<()> {
        let mut accum: HashSet<Column> = HashSet::new();
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        expr_to_columns(
            &Expr::Cast(Cast::new(Box::new(col("a")), DataType::Float64)),
            &mut accum,
        )?;
        assert_eq!(1, accum.len());
        assert!(accum.contains(&Column::from_name("a")));
        Ok(())
    }

    #[test]
    fn test_rewrite_preserving_name() {
        test_rewrite(col("a"), col("a"));

        test_rewrite(col("a"), col("b"));

        // cast data types
        test_rewrite(
            col("a"),
            Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int32)),
        );

        // change literal type from i32 to i64
        test_rewrite(col("a").add(lit(1i32)), col("a").add(lit(1i64)));

        // SortExpr a+1 ==> b + 2
        test_rewrite(
            Expr::Sort(Sort::new(Box::new(col("a").add(lit(1i32))), true, false)),
            Expr::Sort(Sort::new(Box::new(col("b").add(lit(2i64))), true, false)),
        );
    }

    /// rewrites `expr_from` to `rewrite_to` using
    /// `rewrite_preserving_name` verifying the result is `expected_expr`
    fn test_rewrite(expr_from: Expr, rewrite_to: Expr) {
        struct TestRewriter {
            rewrite_to: Expr,
        }

        impl TreeNodeRewriter for TestRewriter {
            type N = Expr;

            fn mutate(&mut self, _: Expr) -> Result<Expr> {
                Ok(self.rewrite_to.clone())
            }
        }

        let mut rewriter = TestRewriter {
            rewrite_to: rewrite_to.clone(),
        };
        let expr = rewrite_preserving_name(expr_from.clone(), &mut rewriter).unwrap();

        let original_name = match &expr_from {
            Expr::Sort(Sort { expr, .. }) => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        let new_name = match &expr {
            Expr::Sort(Sort { expr, .. }) => expr.display_name(),
            expr => expr.display_name(),
        }
        .unwrap();

        assert_eq!(
            original_name, new_name,
            "mismatch rewriting expr_from: {expr_from} to {rewrite_to}"
        )
    }
}
