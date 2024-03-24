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

use super::plan::*;
use crate::expr::{Exists, InSubquery};
use crate::{Expr, UserDefinedLogicalNode};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{internal_err, Result};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use std::sync::{Arc, OnceLock};

impl LogicalPlan {
    /// applies `f` to each expression of this node, potentially rewriting it in
    /// place
    ///
    /// If `f` returns an error, the error is returned and the expressions are
    /// left in a partially modified state
    pub fn rewrite_exprs<F>(&mut self, mut f: F) -> Result<Transformed<()>>
    where
        F: FnMut(&mut Expr) -> Result<Transformed<()>>,
    {
        match self {
            LogicalPlan::Projection(Projection { expr, .. }) => {
                rewrite_expr_iter_mut(expr.iter_mut(), f)
            }
            LogicalPlan::Values(Values { values, .. }) => {
                rewrite_expr_iter_mut(values.iter_mut().flatten(), f)
            }
            LogicalPlan::Filter(Filter { predicate, .. }) => f(predicate),
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                ..
            }) => match partitioning_scheme {
                Partitioning::Hash(expr, _) => rewrite_expr_iter_mut(expr.iter_mut(), f),
                Partitioning::DistributeBy(expr) => {
                    rewrite_expr_iter_mut(expr.iter_mut(), f)
                }
                Partitioning::RoundRobinBatch(_) => Ok(Transformed::no(())),
            },
            LogicalPlan::Window(Window { window_expr, .. }) => {
                rewrite_expr_iter_mut(window_expr.iter_mut(), f)
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                ..
            }) => {
                let exprs = group_expr.iter_mut().chain(aggr_expr.iter_mut());
                rewrite_expr_iter_mut(exprs, f)
            }
            // There are two part of expression for join, equijoin(on) and non-equijoin(filter).
            // 1. the first part is `on.len()` equijoin expressions, and the struct of each expr is `left-on = right-on`.
            // 2. the second part is non-equijoin(filter).
            LogicalPlan::Join(Join { on, filter, .. }) => {
                let exprs = on
                    .iter_mut()
                    .flat_map(|(e1, e2)| std::iter::once(e1).chain(std::iter::once(e2)));

                let result = rewrite_expr_iter_mut(exprs, &mut f)?;

                if let Some(filter) = filter.as_mut() {
                    result.and_then(|| f(filter))
                } else {
                    Ok(result)
                }
            }
            LogicalPlan::Sort(Sort { expr, .. }) => {
                rewrite_expr_iter_mut(expr.iter_mut(), f)
            }
            LogicalPlan::Extension(extension) => {
                rewrite_extension_exprs(&mut extension.node, f)
            }
            LogicalPlan::TableScan(TableScan { filters, .. }) => {
                rewrite_expr_iter_mut(filters.iter_mut(), f)
            }
            LogicalPlan::Unnest(Unnest { column, .. }) => rewrite_column(column, f),
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                ..
            })) => {
                let exprs = on_expr
                    .iter_mut()
                    .chain(select_expr.iter_mut())
                    .chain(sort_expr.iter_mut().flat_map(|x| x.iter_mut()));

                rewrite_expr_iter_mut(exprs, f)
            }
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::RecursiveQuery(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(Distinct::All(_))
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Prepare(_) => Ok(Transformed::no(())),
        }
    }

    /// applies `f` to each input of this node, rewriting them in place.
    ///
    /// # Notes
    /// Inputs include both direct children as well as any embedded subquery
    /// `LogicalPlan`s, for example such as are in [`Expr::Exists`].
    ///
    /// If `f` returns an `Err`, that Err is returned, and the inputs are left
    /// in a partially modified state
    pub fn rewrite_inputs<F>(&mut self, mut f: F) -> Result<Transformed<()>>
    where
        F: FnMut(&mut LogicalPlan) -> Result<Transformed<()>>,
    {
        let children_result = match self {
            LogicalPlan::Projection(Projection { input, .. }) => {
                rewrite_arc(input, &mut f)
            }
            LogicalPlan::Filter(Filter { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Repartition(Repartition { input, .. }) => {
                rewrite_arc(input, &mut f)
            }
            LogicalPlan::Window(Window { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Aggregate(Aggregate { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Sort(Sort { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Join(Join { left, right, .. }) => {
                rewrite_arc(left, &mut f)?.and_then(|| rewrite_arc(right, &mut f))
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                rewrite_arc(left, &mut f)?.and_then(|| rewrite_arc(right, &mut f))
            }
            LogicalPlan::Limit(Limit { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Subquery(Subquery { subquery, .. }) => {
                rewrite_arc(subquery, &mut f)
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                rewrite_arc(input, &mut f)
            }
            LogicalPlan::Extension(extension) => {
                rewrite_extension_inputs(&mut extension.node, &mut f)
            }
            LogicalPlan::Union(Union { inputs, .. }) => inputs
                .iter_mut()
                .try_fold(Transformed::no(()), |acc, input| {
                    acc.and_then(|| rewrite_arc(input, &mut f))
                }),
            LogicalPlan::Distinct(
                Distinct::All(input) | Distinct::On(DistinctOn { input, .. }),
            ) => rewrite_arc(input, &mut f),
            LogicalPlan::Explain(explain) => rewrite_arc(&mut explain.plan, &mut f),
            LogicalPlan::Analyze(analyze) => rewrite_arc(&mut analyze.input, &mut f),
            LogicalPlan::Dml(write) => rewrite_arc(&mut write.input, &mut f),
            LogicalPlan::Copy(copy) => rewrite_arc(&mut copy.input, &mut f),
            LogicalPlan::Ddl(ddl) => {
                if let Some(input) = ddl.input_mut() {
                    rewrite_arc(input, &mut f)
                } else {
                    Ok(Transformed::no(()))
                }
            }
            LogicalPlan::Unnest(Unnest { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::Prepare(Prepare { input, .. }) => rewrite_arc(input, &mut f),
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                static_term,
                recursive_term,
                ..
            }) => rewrite_arc(static_term, &mut f)?
                .and_then(|| rewrite_arc(recursive_term, &mut f)),
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::Statement { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::DescribeTable(_) => Ok(Transformed::no(())),
        }?;

        // after visiting the actual children we we need to visit any subqueries
        // that are inside the expressions
        children_result.and_then(|| self.rewrite_subqueries(&mut f))
    }

    /// applies `f` to LogicalPlans in any subquery expressions
    ///
    /// If Err is returned, the plan may be left in a partially modified state
    fn rewrite_subqueries<F>(&mut self, mut f: F) -> Result<Transformed<()>>
    where
        F: FnMut(&mut LogicalPlan) -> Result<Transformed<()>>,
    {
        self.rewrite_exprs(|expr| match expr {
            Expr::Exists(Exists { subquery, .. })
            | Expr::InSubquery(InSubquery { subquery, .. })
            | Expr::ScalarSubquery(subquery) => {
                rewrite_arc(&mut subquery.subquery, &mut f)
            }
            _ => Ok(Transformed::no(())),
        })
    }
}

/// writes each `&mut Expr` in the iterator using `f`
fn rewrite_expr_iter_mut<'a, F>(
    i: impl IntoIterator<Item = &'a mut Expr>,
    mut f: F,
) -> Result<Transformed<()>>
where
    F: FnMut(&mut Expr) -> Result<Transformed<()>>,
{
    i.into_iter()
        .try_fold(Transformed::no(()), |acc, expr| acc.and_then(|| f(expr)))
}

/// A temporary node that is left in place while rewriting the children of a
/// [`LogicalPlan`]. This is necessary to ensure that the `LogicalPlan` is
/// always in a valid state (from the Rust perspective)
static PLACEHOLDER: OnceLock<Arc<LogicalPlan>> = OnceLock::new();

/// Applies `f` to rewrite the existing node, while avoiding `clone`'ing as much
/// as possiblw.
///
/// TODO eventually remove `Arc<LogicalPlan>` from `LogicalPlan` and have it own
/// its inputs, so this code would not be needed. However, for now we try and
/// unwrap the `Arc` which avoids `clone`ing in most cases.
///
/// On error, node be left with a placeholder logical plan
fn rewrite_arc<F>(node: &mut Arc<LogicalPlan>, mut f: F) -> Result<Transformed<()>>
where
    F: FnMut(&mut LogicalPlan) -> Result<Transformed<()>>,
{
    // We need to leave a valid node in the Arc, while we rewrite the existing
    // one, so use a single global static placeholder node
    let mut new_node = PLACEHOLDER
        .get_or_init(|| {
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: DFSchemaRef::new(DFSchema::empty()),
            }))
        })
        .clone();

    // take the old value out of the Arc
    std::mem::swap(node, &mut new_node);

    // try to update existing node, if it isn't shared with others
    let mut new_node = Arc::try_unwrap(new_node)
        // if None is returned, there is another reference to this
        // LogicalPlan, so we must clone instead
        .unwrap_or_else(|node| node.as_ref().clone());

    // apply the actual transform
    let result = f(&mut new_node)?;

    // put the new value back into the Arc
    let mut new_node = Arc::new(new_node);
    std::mem::swap(node, &mut new_node);

    Ok(result)
}

/// Rewrites a [`Column`] in place using the provided closure
fn rewrite_column<F>(column: &mut Column, mut f: F) -> Result<Transformed<()>>
where
    F: FnMut(&mut Expr) -> Result<Transformed<()>>,
{
    // Since `Column`'s isn't an `Expr`, but the closure in terms of Exprs,
    // we make a temporary Expr to rewrite and then put it back

    let mut swap_column = Column::new_unqualified("TEMP_unnest_column");
    std::mem::swap(column, &mut swap_column);

    let mut expr = Expr::Column(swap_column);
    let result = f(&mut expr)?;
    // Get the rewritten column
    let Expr::Column(mut swap_column) = expr else {
        return internal_err!(
            "Rewrite of Column Expr must return Column, returned {expr:?}"
        );
    };
    // put the rewritten column back
    std::mem::swap(column, &mut swap_column);
    Ok(result)
}

/// Rewrites all expressions for an Extension node "in place"
/// (it currently has to copy values because there are no APIs for in place modification)
/// TODO file ticket for inplace modificiation of Extension nodes
///
/// Should be removed when we have an API for in place modifications of the
/// extension to avoid these copies
fn rewrite_extension_exprs<F>(
    node: &mut Arc<dyn UserDefinedLogicalNode>,
    f: F,
) -> Result<Transformed<()>>
where
    F: FnMut(&mut Expr) -> Result<Transformed<()>>,
{
    let mut exprs = node.expressions();
    let result = rewrite_expr_iter_mut(exprs.iter_mut(), f)?;
    let inputs: Vec<_> = node.inputs().into_iter().cloned().collect();
    let mut new_node = node.from_template(&exprs, &inputs);
    std::mem::swap(node, &mut new_node);
    Ok(result)
}

/// Rewrties all inputs for an Extension node "in place"
/// (it currently has to copy values because there are no APIs for in place modification)
///
/// Should be removed when we have an API for in place modifications of the
/// extension to avoid these copies
fn rewrite_extension_inputs<F>(
    node: &mut Arc<dyn UserDefinedLogicalNode>,
    mut f: F,
) -> Result<Transformed<()>>
where
    F: FnMut(&mut LogicalPlan) -> Result<Transformed<()>>,
{
    let mut inputs: Vec<_> = node.inputs().into_iter().cloned().collect();

    let result = inputs
        .iter_mut()
        .try_fold(Transformed::no(()), |acc, input| acc.and_then(|| f(input)))?;
    let exprs = node.expressions();
    let mut new_node = node.from_template(&exprs, &inputs);
    std::mem::swap(node, &mut new_node);
    Ok(result)
}
