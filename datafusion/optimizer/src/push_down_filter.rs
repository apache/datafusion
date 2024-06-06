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

//! [`PushDownFilter`] applies filters as early as possible

use indexmap::IndexSet;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    internal_err, plan_err, qualified_name, Column, DFSchema, DFSchemaRef,
    JoinConstraint, Result,
};
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::logical_plan::{
    CrossJoin, Join, JoinType, LogicalPlan, TableScan, Union,
};
use datafusion_expr::utils::{conjunction, split_conjunction, split_conjunction_owned};
use datafusion_expr::{
    and, build_join_schema, or, BinaryExpr, Expr, Filter, LogicalPlanBuilder, Operator,
    TableProviderFilterPushDown,
};

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimizer rule for pushing (moving) filter expressions down in a plan so
/// they are applied as early as possible.
///
/// # Introduction
///
/// The goal of this rule is to improve query performance by eliminating
/// redundant work.
///
/// For example, given a plan that sorts all values where `a > 10`:
///
/// ```text
///  Filter (a > 10)
///    Sort (a, b)
/// ```
///
/// A better plan is to  filter the data *before* the Sort, which sorts fewer
/// rows and therefore does less work overall:
///
/// ```text
///  Sort (a, b)
///    Filter (a > 10)  <-- Filter is moved before the sort
/// ```
///
/// However it is not always possible to push filters down. For example, given a
/// plan that finds the top 3 values and then keeps only those that are greater
/// than 10, if the filter is pushed below the limit it would produce a
/// different result.
///
/// ```text
///  Filter (a > 10)   <-- can not move this Filter before the limit
///    Limit (fetch=3)
///      Sort (a, b)
/// ```
///
///
/// More formally, a filter-commutative operation is an operation `op` that
/// satisfies `filter(op(data)) = op(filter(data))`.
///
/// The filter-commutative property is plan and column-specific. A filter on `a`
/// can be pushed through a `Aggregate(group_by = [a], agg=[SUM(b))`. However, a
/// filter on  `SUM(b)` can not be pushed through the same aggregate.
///
/// # Handling Conjunctions
///
/// It is possible to only push down **part** of a filter expression if is
/// connected with `AND`s (more formally if it is a "conjunction").
///
/// For example, given the following plan:
///
/// ```text
/// Filter(a > 10 AND SUM(b) < 5)
///   Aggregate(group_by = [a], agg = [SUM(b))
/// ```
///
/// The `a > 10` is commutative with the `Aggregate` but  `SUM(b) < 5` is not.
/// Therefore it is possible to only push part of the expression, resulting in:
///
/// ```text
/// Filter(SUM(b) < 5)
///   Aggregate(group_by = [a], agg = [SUM(b))
///     Filter(a > 10)
/// ```
///
/// # Handling Column Aliases
///
/// This optimizer must sometimes handle re-writing filter expressions when they
/// pushed, for example if there is a projection that aliases `a+1` to `"b"`:
///
/// ```text
/// Filter (b > 10)
///     Projection: [a+1 AS "b"]  <-- changes the name of `a+1` to `b`
/// ```
///
/// To apply the filter prior to the `Projection`, all references to `b` must be
/// rewritten to `a+1`:
///
/// ```text
/// Projection: a AS "b"
///     Filter: (a + 1 > 10)  <--- changed from b to a + 1
/// ```
/// # Implementation Notes
///
/// This implementation performs a single pass through the plan, "pushing" down
/// filters. When it passes through a filter, it stores that filter, and when it
/// reaches a plan node that does not commute with that filter, it adds the
/// filter to that place. When it passes through a projection, it re-writes the
/// filter's expression taking into account that projection.
#[derive(Default)]
pub struct PushDownFilter {}

/// For a given JOIN type, determine whether each side of the join is preserved.
///
/// We say a join side is preserved if the join returns all or a subset of the rows from
/// the relevant side, such that each row of the output table directly maps to a row of
/// the preserved input table. If a table is not preserved, it can provide extra null rows.
/// That is, there may be rows in the output table that don't directly map to a row in the
/// input table.
///
/// For example:
///   - In an inner join, both sides are preserved, because each row of the output
///     maps directly to a row from each side.
///   - In a left join, the left side is preserved and the right is not, because
///     there may be rows in the output that don't directly map to a row in the
///     right input (due to nulls filling where there is no match on the right).
///
/// This is important because we can always push down post-join filters to a preserved
/// side of the join, assuming the filter only references columns from that side. For the
/// non-preserved side it can be more tricky.
///
/// Returns a tuple of booleans - (left_preserved, right_preserved).
fn lr_is_preserved(join_type: JoinType) -> Result<(bool, bool)> {
    match join_type {
        JoinType::Inner => Ok((true, true)),
        JoinType::Left => Ok((true, false)),
        JoinType::Right => Ok((false, true)),
        JoinType::Full => Ok((false, false)),
        // No columns from the right side of the join can be referenced in output
        // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
        JoinType::LeftSemi | JoinType::LeftAnti => Ok((true, false)),
        // No columns from the left side of the join can be referenced in output
        // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
        JoinType::RightSemi | JoinType::RightAnti => Ok((false, true)),
    }
}

/// For a given JOIN logical plan, determine whether each side of the join is preserved
/// in terms on join filtering.
/// Predicates from join filter can only be pushed to preserved join side.
fn on_lr_is_preserved(join_type: JoinType) -> Result<(bool, bool)> {
    match join_type {
        JoinType::Inner => Ok((true, true)),
        JoinType::Left => Ok((false, true)),
        JoinType::Right => Ok((true, false)),
        JoinType::Full => Ok((false, false)),
        JoinType::LeftSemi | JoinType::RightSemi => Ok((true, true)),
        JoinType::LeftAnti => Ok((false, true)),
        JoinType::RightAnti => Ok((true, false)),
    }
}

/// Determine which predicates in state can be pushed down to a given side of a join.
/// To determine this, we need to know the schema of the relevant join side and whether
/// or not the side's rows are preserved when joining. If the side is not preserved, we
/// do not push down anything. Otherwise we can push down predicates where all of the
/// relevant columns are contained on the relevant join side's schema.
fn can_pushdown_join_predicate(predicate: &Expr, schema: &DFSchema) -> Result<bool> {
    let schema_columns = schema
        .iter()
        .flat_map(|(qualifier, field)| {
            [
                Column::new(qualifier.cloned(), field.name()),
                // we need to push down filter using unqualified column as well
                Column::new_unqualified(field.name()),
            ]
        })
        .collect::<HashSet<_>>();
    let columns = predicate.to_columns()?;

    Ok(schema_columns
        .intersection(&columns)
        .collect::<HashSet<_>>()
        .len()
        == columns.len())
}

/// Determine whether the predicate can evaluate as the join conditions
fn can_evaluate_as_join_condition(predicate: &Expr) -> Result<bool> {
    let mut is_evaluate = true;
    predicate.apply(|expr| match expr {
        Expr::Column(_)
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _) => Ok(TreeNodeRecursion::Jump),
        Expr::Exists { .. }
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Unnest(_)
        | Expr::ScalarFunction(_) => {
            is_evaluate = false;
            Ok(TreeNodeRecursion::Stop)
        }
        Expr::Alias(_)
        | Expr::BinaryExpr(_)
        | Expr::Like(_)
        | Expr::SimilarTo(_)
        | Expr::Not(_)
        | Expr::IsNotNull(_)
        | Expr::IsNull(_)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsUnknown(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::IsNotUnknown(_)
        | Expr::Negative(_)
        | Expr::Between(_)
        | Expr::Case(_)
        | Expr::Cast(_)
        | Expr::TryCast(_)
        | Expr::InList { .. } => Ok(TreeNodeRecursion::Continue),
        Expr::Sort(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::Wildcard { .. }
        | Expr::GroupingSet(_) => internal_err!("Unsupported predicate type"),
    })?;
    Ok(is_evaluate)
}

/// examine OR clause to see if any useful clauses can be extracted and push down.
/// extract at least one qual from each sub clauses of OR clause, then form the quals
/// to new OR clause as predicate.
///
/// # Example
/// ```text
/// Filter: (a = c and a < 20) or (b = d and b > 10)
///     join/crossjoin:
///          TableScan: projection=[a, b]
///          TableScan: projection=[c, d]
/// ```
///
/// is optimized to
///
/// ```text
/// Filter: (a = c and a < 20) or (b = d and b > 10)
///     join/crossjoin:
///          Filter: (a < 20) or (b > 10)
///              TableScan: projection=[a, b]
///          TableScan: projection=[c, d]
/// ```
///
/// In general, predicates of this form:
///
/// ```sql
/// (A AND B) OR (C AND D)
/// ```
///
/// will be transformed to one of:
///
/// * `((A AND B) OR (C AND D)) AND (A OR C)`
/// * `((A AND B) OR (C AND D)) AND ((A AND B) OR C)`
/// * do nothing.
fn extract_or_clauses_for_join<'a>(
    filters: &'a [Expr],
    schema: &'a DFSchema,
) -> impl Iterator<Item = Expr> + 'a {
    let schema_columns = schema
        .iter()
        .flat_map(|(qualifier, field)| {
            [
                Column::new(qualifier.cloned(), field.name()),
                // we need to push down filter using unqualified column as well
                Column::new_unqualified(field.name()),
            ]
        })
        .collect::<HashSet<_>>();

    // new formed OR clauses and their column references
    filters.iter().filter_map(move |expr| {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) = expr
        {
            let left_expr = extract_or_clause(left.as_ref(), &schema_columns);
            let right_expr = extract_or_clause(right.as_ref(), &schema_columns);

            // If nothing can be extracted from any sub clauses, do nothing for this OR clause.
            if let (Some(left_expr), Some(right_expr)) = (left_expr, right_expr) {
                return Some(or(left_expr, right_expr));
            }
        }
        None
    })
}

/// extract qual from OR sub-clause.
///
/// A qual is extracted if it only contains set of column references in schema_columns.
///
/// For AND clause, we extract from both sub-clauses, then make new AND clause by extracted
/// clauses if both extracted; Otherwise, use the extracted clause from any sub-clauses or None.
///
/// For OR clause, we extract from both sub-clauses, then make new OR clause by extracted clauses if both extracted;
/// Otherwise, return None.
///
/// For other clause, apply the rule above to extract clause.
fn extract_or_clause(expr: &Expr, schema_columns: &HashSet<Column>) -> Option<Expr> {
    let mut predicate = None;

    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left: l_expr,
            op: Operator::Or,
            right: r_expr,
        }) => {
            let l_expr = extract_or_clause(l_expr, schema_columns);
            let r_expr = extract_or_clause(r_expr, schema_columns);

            if let (Some(l_expr), Some(r_expr)) = (l_expr, r_expr) {
                predicate = Some(or(l_expr, r_expr));
            }
        }
        Expr::BinaryExpr(BinaryExpr {
            left: l_expr,
            op: Operator::And,
            right: r_expr,
        }) => {
            let l_expr = extract_or_clause(l_expr, schema_columns);
            let r_expr = extract_or_clause(r_expr, schema_columns);

            match (l_expr, r_expr) {
                (Some(l_expr), Some(r_expr)) => {
                    predicate = Some(and(l_expr, r_expr));
                }
                (Some(l_expr), None) => {
                    predicate = Some(l_expr);
                }
                (None, Some(r_expr)) => {
                    predicate = Some(r_expr);
                }
                (None, None) => {
                    predicate = None;
                }
            }
        }
        _ => {
            let columns = expr.to_columns().ok().unwrap();

            if schema_columns
                .intersection(&columns)
                .collect::<HashSet<_>>()
                .len()
                == columns.len()
            {
                predicate = Some(expr.clone());
            }
        }
    }

    predicate
}

/// push down join/cross-join
fn push_down_all_join(
    predicates: Vec<Expr>,
    inferred_join_predicates: Vec<Expr>,
    mut join: Join,
    on_filter: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    let is_inner_join = join.join_type == JoinType::Inner;
    // Get pushable predicates from current optimizer state
    let (left_preserved, right_preserved) = lr_is_preserved(join.join_type)?;

    // The predicates can be divided to three categories:
    // 1) can push through join to its children(left or right)
    // 2) can be converted to join conditions if the join type is Inner
    // 3) should be kept as filter conditions
    let left_schema = join.left.schema();
    let right_schema = join.right.schema();
    let mut left_push = vec![];
    let mut right_push = vec![];
    let mut keep_predicates = vec![];
    let mut join_conditions = vec![];
    for predicate in predicates {
        if left_preserved && can_pushdown_join_predicate(&predicate, left_schema)? {
            left_push.push(predicate);
        } else if right_preserved
            && can_pushdown_join_predicate(&predicate, right_schema)?
        {
            right_push.push(predicate);
        } else if is_inner_join && can_evaluate_as_join_condition(&predicate)? {
            // Here we do not differ it is eq or non-eq predicate, ExtractEquijoinPredicate will extract the eq predicate
            // and convert to the join on condition
            join_conditions.push(predicate);
        } else {
            keep_predicates.push(predicate);
        }
    }

    // For infer predicates, if they can not push through join, just drop them
    for predicate in inferred_join_predicates {
        if left_preserved && can_pushdown_join_predicate(&predicate, left_schema)? {
            left_push.push(predicate);
        } else if right_preserved
            && can_pushdown_join_predicate(&predicate, right_schema)?
        {
            right_push.push(predicate);
        }
    }

    if !on_filter.is_empty() {
        let (on_left_preserved, on_right_preserved) = on_lr_is_preserved(join.join_type)?;
        for on in on_filter {
            if on_left_preserved && can_pushdown_join_predicate(&on, left_schema)? {
                left_push.push(on)
            } else if on_right_preserved
                && can_pushdown_join_predicate(&on, right_schema)?
            {
                right_push.push(on)
            } else {
                join_conditions.push(on)
            }
        }
    }

    // Extract from OR clause, generate new predicates for both side of join if possible.
    // We only track the unpushable predicates above.
    if left_preserved {
        left_push.extend(extract_or_clauses_for_join(&keep_predicates, left_schema));
        left_push.extend(extract_or_clauses_for_join(&join_conditions, left_schema));
    }
    if right_preserved {
        right_push.extend(extract_or_clauses_for_join(&keep_predicates, right_schema));
        right_push.extend(extract_or_clauses_for_join(&join_conditions, right_schema));
    }

    if let Some(predicate) = conjunction(left_push) {
        join.left = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.left)?));
    }
    if let Some(predicate) = conjunction(right_push) {
        join.right =
            Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.right)?));
    }

    // Add any new join conditions as the non join predicates
    join.filter = conjunction(join_conditions);

    // wrap the join on the filter whose predicates must be kept, if any
    let plan = LogicalPlan::Join(join);
    let plan = if let Some(predicate) = conjunction(keep_predicates) {
        LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(plan))?)
    } else {
        plan
    };
    Ok(Transformed::yes(plan))
}

fn push_down_join(
    join: Join,
    parent_predicate: Option<&Expr>,
) -> Result<Transformed<LogicalPlan>> {
    // Split the parent predicate into individual conjunctive parts.
    let predicates = parent_predicate
        .map_or_else(Vec::new, |pred| split_conjunction_owned(pred.clone()));

    // Extract conjunctions from the JOIN's ON filter, if present.
    let on_filters = join
        .filter
        .as_ref()
        .map_or_else(Vec::new, |filter| split_conjunction_owned(filter.clone()));

    // Are there any new join predicates that can be inferred from the filter expressions?
    let inferred_join_predicates =
        infer_join_predicates(&join, &predicates, &on_filters)?;

    if on_filters.is_empty()
        && predicates.is_empty()
        && inferred_join_predicates.is_empty()
    {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    push_down_all_join(predicates, inferred_join_predicates, join, on_filters)
}

/// Extracts any equi-join join predicates from the given filter expressions.
///
/// Parameters
/// * `join` the join in question
///
/// * `predicates` the pushed down filter expression
///
/// * `on_filters` filters from the join ON clause that have not already been
/// identified as join predicates
///
fn infer_join_predicates(
    join: &Join,
    predicates: &[Expr],
    on_filters: &[Expr],
) -> Result<Vec<Expr>> {
    if join.join_type != JoinType::Inner {
        return Ok(vec![]);
    }

    // Only allow both side key is column.
    let join_col_keys = join
        .on
        .iter()
        .filter_map(|(l, r)| {
            let left_col = l.try_as_col()?;
            let right_col = r.try_as_col()?;
            Some((left_col, right_col))
        })
        .collect::<Vec<_>>();

    // TODO refine the logic, introduce EquivalenceProperties to logical plan and infer additional filters to push down
    // For inner joins, duplicate filters for joined columns so filters can be pushed down
    // to both sides. Take the following query as an example:
    //
    // ```sql
    // SELECT * FROM t1 JOIN t2 on t1.id = t2.uid WHERE t1.id > 1
    // ```
    //
    // `t1.id > 1` predicate needs to be pushed down to t1 table scan, while
    // `t2.uid > 1` predicate needs to be pushed down to t2 table scan.
    //
    // Join clauses with `Using` constraints also take advantage of this logic to make sure
    // predicates reference the shared join columns are pushed to both sides.
    // This logic should also been applied to conditions in JOIN ON clause
    predicates
        .iter()
        .chain(on_filters.iter())
        .filter_map(|predicate| {
            let mut join_cols_to_replace = HashMap::new();

            let columns = match predicate.to_columns() {
                Ok(columns) => columns,
                Err(e) => return Some(Err(e)),
            };

            for col in columns.iter() {
                for (l, r) in join_col_keys.iter() {
                    if col == *l {
                        join_cols_to_replace.insert(col, *r);
                        break;
                    } else if col == *r {
                        join_cols_to_replace.insert(col, *l);
                        break;
                    }
                }
            }

            if join_cols_to_replace.is_empty() {
                return None;
            }

            let join_side_predicate =
                match replace_col(predicate.clone(), &join_cols_to_replace) {
                    Ok(p) => p,
                    Err(e) => {
                        return Some(Err(e));
                    }
                };

            Some(Ok(join_side_predicate))
        })
        .collect::<Result<Vec<_>>>()
}

impl OptimizerRule for PushDownFilter {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called PushDownFilter::rewrite")
    }

    fn name(&self) -> &str {
        "push_down_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Join(join) = plan {
            return push_down_join(join, None);
        };

        let plan_schema = plan.schema().clone();

        let LogicalPlan::Filter(mut filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        match unwrap_arc(filter.input) {
            LogicalPlan::Filter(child_filter) => {
                let parents_predicates = split_conjunction_owned(filter.predicate);

                // remove duplicated filters
                let child_predicates = split_conjunction_owned(child_filter.predicate);
                let new_predicates = parents_predicates
                    .into_iter()
                    .chain(child_predicates)
                    // use IndexSet to remove dupes while preserving predicate order
                    .collect::<IndexSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();

                let Some(new_predicate) = conjunction(new_predicates) else {
                    return plan_err!("at least one expression exists");
                };
                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    new_predicate,
                    child_filter.input,
                )?);
                self.rewrite(new_filter, _config)
            }
            LogicalPlan::Repartition(repartition) => {
                let new_filter =
                    Filter::try_new(filter.predicate, Arc::clone(&repartition.input))
                        .map(LogicalPlan::Filter)?;
                insert_below(LogicalPlan::Repartition(repartition), new_filter)
            }
            LogicalPlan::Distinct(distinct) => {
                let new_filter =
                    Filter::try_new(filter.predicate, Arc::clone(distinct.input()))
                        .map(LogicalPlan::Filter)?;
                insert_below(LogicalPlan::Distinct(distinct), new_filter)
            }
            LogicalPlan::Sort(sort) => {
                let new_filter =
                    Filter::try_new(filter.predicate, Arc::clone(&sort.input))
                        .map(LogicalPlan::Filter)?;
                insert_below(LogicalPlan::Sort(sort), new_filter)
            }
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                let mut replace_map = HashMap::new();
                for (i, (qualifier, field)) in
                    subquery_alias.input.schema().iter().enumerate()
                {
                    let (sub_qualifier, sub_field) =
                        subquery_alias.schema.qualified_field(i);
                    replace_map.insert(
                        qualified_name(sub_qualifier, sub_field.name()),
                        Expr::Column(Column::new(qualifier.cloned(), field.name())),
                    );
                }
                let new_predicate = replace_cols_by_name(filter.predicate, &replace_map)?;

                let new_filter = LogicalPlan::Filter(Filter::try_new(
                    new_predicate,
                    Arc::clone(&subquery_alias.input),
                )?);
                insert_below(LogicalPlan::SubqueryAlias(subquery_alias), new_filter)
            }
            LogicalPlan::Projection(projection) => {
                // A projection is filter-commutable if it do not contain volatile predicates or contain volatile
                // predicates that are not used in the filter. However, we should re-writes all predicate expressions.
                // collect projection.
                let (volatile_map, non_volatile_map): (HashMap<_, _>, HashMap<_, _>) =
                    projection
                        .schema
                        .iter()
                        .zip(projection.expr.iter())
                        .map(|((qualifier, field), expr)| {
                            // strip alias, as they should not be part of filters
                            let expr = expr.clone().unalias();

                            (qualified_name(qualifier, field.name()), expr)
                        })
                        .partition(|(_, value)| value.is_volatile().unwrap_or(true));

                let mut push_predicates = vec![];
                let mut keep_predicates = vec![];
                for expr in split_conjunction_owned(filter.predicate.clone()) {
                    if contain(&expr, &volatile_map) {
                        keep_predicates.push(expr);
                    } else {
                        push_predicates.push(expr);
                    }
                }

                match conjunction(push_predicates) {
                    Some(expr) => {
                        // re-write all filters based on this projection
                        // E.g. in `Filter: b\n  Projection: a > 1 as b`, we can swap them, but the filter must be "a > 1"
                        let new_filter = LogicalPlan::Filter(Filter::try_new(
                            replace_cols_by_name(expr, &non_volatile_map)?,
                            Arc::clone(&projection.input),
                        )?);

                        match conjunction(keep_predicates) {
                            None => insert_below(
                                LogicalPlan::Projection(projection),
                                new_filter,
                            ),
                            Some(keep_predicate) => insert_below(
                                LogicalPlan::Projection(projection),
                                new_filter,
                            )?
                            .map_data(|child_plan| {
                                Filter::try_new(keep_predicate, Arc::new(child_plan))
                                    .map(LogicalPlan::Filter)
                            }),
                        }
                    }
                    None => {
                        filter.input = Arc::new(LogicalPlan::Projection(projection));
                        Ok(Transformed::no(LogicalPlan::Filter(filter)))
                    }
                }
            }
            LogicalPlan::Union(ref union) => {
                let mut inputs = Vec::with_capacity(union.inputs.len());
                for input in &union.inputs {
                    let mut replace_map = HashMap::new();
                    for (i, (qualifier, field)) in input.schema().iter().enumerate() {
                        let (union_qualifier, union_field) =
                            union.schema.qualified_field(i);
                        replace_map.insert(
                            qualified_name(union_qualifier, union_field.name()),
                            Expr::Column(Column::new(qualifier.cloned(), field.name())),
                        );
                    }

                    let push_predicate =
                        replace_cols_by_name(filter.predicate.clone(), &replace_map)?;
                    inputs.push(Arc::new(LogicalPlan::Filter(Filter::try_new(
                        push_predicate,
                        Arc::clone(input),
                    )?)))
                }
                Ok(Transformed::yes(LogicalPlan::Union(Union {
                    inputs,
                    schema: Arc::clone(&plan_schema),
                })))
            }
            LogicalPlan::Aggregate(agg) => {
                // We can push down Predicate which in groupby_expr.
                let group_expr_columns = agg
                    .group_expr
                    .iter()
                    .map(|e| Ok(Column::from_qualified_name(e.display_name()?)))
                    .collect::<Result<HashSet<_>>>()?;

                let predicates = split_conjunction_owned(filter.predicate.clone());

                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for expr in predicates {
                    let cols = expr.to_columns()?;
                    if cols.iter().all(|c| group_expr_columns.contains(c)) {
                        push_predicates.push(expr);
                    } else {
                        keep_predicates.push(expr);
                    }
                }

                // As for plan Filter: Column(a+b) > 0 -- Agg: groupby:[Column(a)+Column(b)]
                // After push, we need to replace `a+b` with Column(a)+Column(b)
                // So we need create a replace_map, add {`a+b` --> Expr(Column(a)+Column(b))}
                let mut replace_map = HashMap::new();
                for expr in &agg.group_expr {
                    replace_map.insert(expr.display_name()?, expr.clone());
                }
                let replaced_push_predicates = push_predicates
                    .into_iter()
                    .map(|expr| replace_cols_by_name(expr, &replace_map))
                    .collect::<Result<Vec<_>>>()?;

                let agg_input = Arc::clone(&agg.input);
                Transformed::yes(LogicalPlan::Aggregate(agg))
                    .transform_data(|new_plan| {
                        // If we have a filter to push, we push it down to the input of the aggregate
                        if let Some(predicate) = conjunction(replaced_push_predicates) {
                            let new_filter = make_filter(predicate, agg_input)?;
                            insert_below(new_plan, new_filter)
                        } else {
                            Ok(Transformed::no(new_plan))
                        }
                    })?
                    .map_data(|child_plan| {
                        // if there are any remaining predicates we can't push, add them
                        // back as a filter
                        if let Some(predicate) = conjunction(keep_predicates) {
                            make_filter(predicate, Arc::new(child_plan))
                        } else {
                            Ok(child_plan)
                        }
                    })
            }
            LogicalPlan::Join(join) => push_down_join(join, Some(&filter.predicate)),
            LogicalPlan::CrossJoin(cross_join) => {
                let predicates = split_conjunction_owned(filter.predicate);
                let join = convert_cross_join_to_inner_join(cross_join)?;
                let plan = push_down_all_join(predicates, vec![], join, vec![])?;
                convert_to_cross_join_if_beneficial(plan.data)
            }
            LogicalPlan::TableScan(scan) => {
                let filter_predicates = split_conjunction(&filter.predicate);
                let results = scan
                    .source
                    .supports_filters_pushdown(filter_predicates.as_slice())?;
                if filter_predicates.len() != results.len() {
                    return internal_err!(
                        "Vec returned length: {} from supports_filters_pushdown is not the same size as the filters passed, which length is: {}",
                        results.len(),
                        filter_predicates.len());
                }

                let zip = filter_predicates.into_iter().zip(results);

                let new_scan_filters = zip
                    .clone()
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
                    .map(|(pred, _)| pred);
                let new_scan_filters: Vec<Expr> = scan
                    .filters
                    .iter()
                    .chain(new_scan_filters)
                    .unique()
                    .cloned()
                    .collect();
                let new_predicate: Vec<Expr> = zip
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Exact)
                    .map(|(pred, _)| pred.clone())
                    .collect();

                let new_scan = LogicalPlan::TableScan(TableScan {
                    filters: new_scan_filters,
                    ..scan
                });

                Transformed::yes(new_scan).transform_data(|new_scan| {
                    if let Some(predicate) = conjunction(new_predicate) {
                        make_filter(predicate, Arc::new(new_scan)).map(Transformed::yes)
                    } else {
                        Ok(Transformed::no(new_scan))
                    }
                })
            }
            LogicalPlan::Extension(extension_plan) => {
                let prevent_cols =
                    extension_plan.node.prevent_predicate_push_down_columns();

                // determine if we can push any predicates down past the extension node

                // each element is true for push, false to keep
                let predicate_push_or_keep = split_conjunction(&filter.predicate)
                    .iter()
                    .map(|expr| {
                        let cols = expr.to_columns()?;
                        if cols.iter().any(|c| prevent_cols.contains(&c.name)) {
                            Ok(false) // No push (keep)
                        } else {
                            Ok(true) // push
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                // all predicates are kept, no changes needed
                if predicate_push_or_keep.iter().all(|&x| !x) {
                    filter.input = Arc::new(LogicalPlan::Extension(extension_plan));
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                // going to push some predicates down, so split the predicates
                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for (push, expr) in predicate_push_or_keep
                    .into_iter()
                    .zip(split_conjunction_owned(filter.predicate).into_iter())
                {
                    if !push {
                        keep_predicates.push(expr);
                    } else {
                        push_predicates.push(expr);
                    }
                }

                let new_children = match conjunction(push_predicates) {
                    Some(predicate) => extension_plan
                        .node
                        .inputs()
                        .into_iter()
                        .map(|child| {
                            Ok(LogicalPlan::Filter(Filter::try_new(
                                predicate.clone(),
                                Arc::new(child.clone()),
                            )?))
                        })
                        .collect::<Result<Vec<_>>>()?,
                    None => extension_plan.node.inputs().into_iter().cloned().collect(),
                };
                // extension with new inputs.
                let child_plan = LogicalPlan::Extension(extension_plan);
                let new_extension =
                    child_plan.with_new_exprs(child_plan.expressions(), new_children)?;

                let new_plan = match conjunction(keep_predicates) {
                    Some(predicate) => LogicalPlan::Filter(Filter::try_new(
                        predicate,
                        Arc::new(new_extension),
                    )?),
                    None => new_extension,
                };
                Ok(Transformed::yes(new_plan))
            }
            child => {
                filter.input = Arc::new(child);
                Ok(Transformed::no(LogicalPlan::Filter(filter)))
            }
        }
    }
}

/// Creates a new LogicalPlan::Filter node.
pub fn make_filter(predicate: Expr, input: Arc<LogicalPlan>) -> Result<LogicalPlan> {
    Filter::try_new(predicate, input).map(LogicalPlan::Filter)
}

/// Replace the existing child of the single input node with `new_child`.
///
/// Starting:
/// ```text
/// plan
///   child
/// ```
///
/// Ending:
/// ```text
/// plan
///   new_child
/// ```
fn insert_below(
    plan: LogicalPlan,
    new_child: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    let mut new_child = Some(new_child);
    let transformed_plan = plan.map_children(|_child| {
        if let Some(new_child) = new_child.take() {
            Ok(Transformed::yes(new_child))
        } else {
            // already took the new child
            internal_err!("node had more than one input")
        }
    })?;

    // make sure we did the actual replacement
    if new_child.is_some() {
        return internal_err!("node had no  inputs");
    }

    Ok(transformed_plan)
}

impl PushDownFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Converts the given cross join to an inner join with an empty equality
/// predicate and an empty filter condition.
fn convert_cross_join_to_inner_join(cross_join: CrossJoin) -> Result<Join> {
    let CrossJoin { left, right, .. } = cross_join;
    let join_schema = build_join_schema(left.schema(), right.schema(), &JoinType::Inner)?;
    Ok(Join {
        left,
        right,
        join_type: JoinType::Inner,
        join_constraint: JoinConstraint::On,
        on: vec![],
        filter: None,
        schema: DFSchemaRef::new(join_schema),
        null_equals_null: true,
    })
}

/// Converts the given inner join with an empty equality predicate and an
/// empty filter condition to a cross join.
fn convert_to_cross_join_if_beneficial(
    plan: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    match plan {
        // Can be converted back to cross join
        LogicalPlan::Join(join) if join.on.is_empty() && join.filter.is_none() => {
            LogicalPlanBuilder::from(unwrap_arc(join.left))
                .cross_join(unwrap_arc(join.right))?
                .build()
                .map(Transformed::yes)
        }
        LogicalPlan::Filter(filter) => convert_to_cross_join_if_beneficial(unwrap_arc(
            filter.input,
        ))?
        .transform_data(|child_plan| {
            Filter::try_new(filter.predicate, Arc::new(child_plan))
                .map(LogicalPlan::Filter)
                .map(Transformed::yes)
        }),
        plan => Ok(Transformed::no(plan)),
    }
}

/// replaces columns by its name on the projection.
pub fn replace_cols_by_name(
    e: Expr,
    replace_map: &HashMap<String, Expr>,
) -> Result<Expr> {
    e.transform_up(|expr| {
        Ok(if let Expr::Column(c) = &expr {
            match replace_map.get(&c.flat_name()) {
                Some(new_c) => Transformed::yes(new_c.clone()),
                None => Transformed::no(expr),
            }
        } else {
            Transformed::no(expr)
        })
    })
    .data()
}

/// check whether the expression uses the columns in `check_map`.
fn contain(e: &Expr, check_map: &HashMap<String, Expr>) -> bool {
    let mut is_contain = false;
    e.apply(|expr| {
        Ok(if let Expr::Column(c) = &expr {
            match check_map.get(&c.flat_name()) {
                Some(_) => {
                    is_contain = true;
                    TreeNodeRecursion::Stop
                }
                None => TreeNodeRecursion::Continue,
            }
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .unwrap();
    is_contain
}

// Add test to datafusion/core/tests/optimizer/push_down_filter.rs
