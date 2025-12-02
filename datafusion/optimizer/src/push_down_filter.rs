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

//! [`PushDownFilter`] applies filters as early as possible

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::DataType;
use indexmap::IndexSet;
use itertools::Itertools;

use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    assert_eq_or_internal_err, assert_or_internal_err, internal_err, plan_err,
    qualified_name, Column, DFSchema, Result,
};
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::logical_plan::{Join, JoinType, LogicalPlan, TableScan, Union};
use datafusion_expr::utils::{
    conjunction, expr_to_columns, split_conjunction, split_conjunction_owned,
};
use datafusion_expr::{
    and, or, BinaryExpr, Expr, Filter, Operator, Projection, TableProviderFilterPushDown,
};

use crate::optimizer::ApplyOrder;
use crate::simplify_expressions::simplify_predicates;
use crate::utils::{has_all_column_refs, is_restrict_null_predicate};
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
/// can be pushed through a `Aggregate(group_by = [a], agg=[sum(b))`. However, a
/// filter on  `sum(b)` can not be pushed through the same aggregate.
///
/// # Handling Conjunctions
///
/// It is possible to only push down **part** of a filter expression if is
/// connected with `AND`s (more formally if it is a "conjunction").
///
/// For example, given the following plan:
///
/// ```text
/// Filter(a > 10 AND sum(b) < 5)
///   Aggregate(group_by = [a], agg = [sum(b))
/// ```
///
/// The `a > 10` is commutative with the `Aggregate` but  `sum(b) < 5` is not.
/// Therefore it is possible to only push part of the expression, resulting in:
///
/// ```text
/// Filter(sum(b) < 5)
///   Aggregate(group_by = [a], agg = [sum(b))
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
#[derive(Default, Debug)]
pub struct PushDownFilter {}

/// For a given JOIN type, determine whether each input of the join is preserved
/// for post-join (`WHERE` clause) filters.
///
/// It is only correct to push filters below a join for preserved inputs.
///
/// # Return Value
/// A tuple of booleans - (left_preserved, right_preserved).
///
/// # "Preserved" input definition
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
///
///   - In a left join, the left side is preserved (we can push predicates) but
///     the right is not, because there may be rows in the output that don't
///     directly map to a row in the right input (due to nulls filling where there
///     is no match on the right).
pub(crate) fn lr_is_preserved(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left => (true, false),
        JoinType::Right => (false, true),
        JoinType::Full => (false, false),
        // No columns from the right side of the join can be referenced in output
        // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => (true, false),
        // No columns from the left side of the join can be referenced in output
        // predicates for semi/anti joins, so whether we specify t/f doesn't matter.
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => (false, true),
    }
}

/// For a given JOIN type, determine whether each input of the join is preserved
/// for the join condition (`ON` clause filters).
///
/// It is only correct to push filters below a join for preserved inputs.
///
/// # Return Value
/// A tuple of booleans - (left_preserved, right_preserved).
///
/// See [`lr_is_preserved`] for a definition of "preserved".
pub(crate) fn on_lr_is_preserved(join_type: JoinType) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (true, true),
        JoinType::Left => (false, true),
        JoinType::Right => (true, false),
        JoinType::Full => (false, false),
        JoinType::LeftSemi | JoinType::RightSemi => (true, true),
        JoinType::LeftAnti => (false, true),
        JoinType::RightAnti => (true, false),
        JoinType::LeftMark => (false, true),
        JoinType::RightMark => (true, false),
    }
}

/// Evaluates the columns referenced in the given expression to see if they refer
/// only to the left or right columns
#[derive(Debug)]
struct ColumnChecker<'a> {
    /// schema of left join input
    left_schema: &'a DFSchema,
    /// columns in left_schema, computed on demand
    left_columns: Option<HashSet<Column>>,
    /// schema of right join input
    right_schema: &'a DFSchema,
    /// columns in left_schema, computed on demand
    right_columns: Option<HashSet<Column>>,
}

impl<'a> ColumnChecker<'a> {
    fn new(left_schema: &'a DFSchema, right_schema: &'a DFSchema) -> Self {
        Self {
            left_schema,
            left_columns: None,
            right_schema,
            right_columns: None,
        }
    }

    /// Return true if the expression references only columns from the left side of the join
    fn is_left_only(&mut self, predicate: &Expr) -> bool {
        if self.left_columns.is_none() {
            self.left_columns = Some(schema_columns(self.left_schema));
        }
        has_all_column_refs(predicate, self.left_columns.as_ref().unwrap())
    }

    /// Return true if the expression references only columns from the right side of the join
    fn is_right_only(&mut self, predicate: &Expr) -> bool {
        if self.right_columns.is_none() {
            self.right_columns = Some(schema_columns(self.right_schema));
        }
        has_all_column_refs(predicate, self.right_columns.as_ref().unwrap())
    }
}

/// Returns all columns in the schema
fn schema_columns(schema: &DFSchema) -> HashSet<Column> {
    schema
        .iter()
        .flat_map(|(qualifier, field)| {
            [
                Column::new(qualifier.cloned(), field.name()),
                // we need to push down filter using unqualified column as well
                Column::new_unqualified(field.name()),
            ]
        })
        .collect::<HashSet<_>>()
}

/// Determine whether the predicate can evaluate as the join conditions
fn can_evaluate_as_join_condition(predicate: &Expr) -> Result<bool> {
    let mut is_evaluate = true;
    predicate.apply(|expr| match expr {
        Expr::Column(_)
        | Expr::Literal(_, _)
        | Expr::Placeholder(_)
        | Expr::ScalarVariable(_, _) => Ok(TreeNodeRecursion::Jump),
        Expr::Exists { .. }
        | Expr::InSubquery(_)
        | Expr::ScalarSubquery(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::Unnest(_) => {
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
        | Expr::InList { .. }
        | Expr::ScalarFunction(_) => Ok(TreeNodeRecursion::Continue),
        // TODO: remove the next line after `Expr::Wildcard` is removed
        #[expect(deprecated)]
        Expr::AggregateFunction(_)
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
    let schema_columns = schema_columns(schema);

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
            if has_all_column_refs(expr, schema_columns) {
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
    let (left_preserved, right_preserved) = lr_is_preserved(join.join_type);

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
    let mut checker = ColumnChecker::new(left_schema, right_schema);
    for predicate in predicates {
        if left_preserved && checker.is_left_only(&predicate) {
            left_push.push(predicate);
        } else if right_preserved && checker.is_right_only(&predicate) {
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
        if left_preserved && checker.is_left_only(&predicate) {
            left_push.push(predicate);
        } else if right_preserved && checker.is_right_only(&predicate) {
            right_push.push(predicate);
        }
    }

    let mut on_filter_join_conditions = vec![];
    let (on_left_preserved, on_right_preserved) = on_lr_is_preserved(join.join_type);

    if !on_filter.is_empty() {
        for on in on_filter {
            if on_left_preserved && checker.is_left_only(&on) {
                left_push.push(on)
            } else if on_right_preserved && checker.is_right_only(&on) {
                right_push.push(on)
            } else {
                on_filter_join_conditions.push(on)
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

    // For predicates from join filter, we should check with if a join side is preserved
    // in term of join filtering.
    if on_left_preserved {
        left_push.extend(extract_or_clauses_for_join(
            &on_filter_join_conditions,
            left_schema,
        ));
    }
    if on_right_preserved {
        right_push.extend(extract_or_clauses_for_join(
            &on_filter_join_conditions,
            right_schema,
        ));
    }

    if let Some(predicate) = conjunction(left_push) {
        join.left = Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.left)?));
    }
    if let Some(predicate) = conjunction(right_push) {
        join.right =
            Arc::new(LogicalPlan::Filter(Filter::try_new(predicate, join.right)?));
    }

    // Add any new join conditions as the non join predicates
    join_conditions.extend(on_filter_join_conditions);
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
///   identified as join predicates
fn infer_join_predicates(
    join: &Join,
    predicates: &[Expr],
    on_filters: &[Expr],
) -> Result<Vec<Expr>> {
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

    let join_type = join.join_type;

    let mut inferred_predicates = InferredPredicates::new(join_type);

    infer_join_predicates_from_predicates(
        &join_col_keys,
        predicates,
        &mut inferred_predicates,
    )?;

    infer_join_predicates_from_on_filters(
        &join_col_keys,
        join_type,
        on_filters,
        &mut inferred_predicates,
    )?;

    Ok(inferred_predicates.predicates)
}

/// Inferred predicates collector.
/// When the JoinType is not Inner, we need to detect whether the inferred predicate can strictly
/// filter out NULL, otherwise ignore it. e.g.
/// ```text
/// SELECT * FROM t1 LEFT JOIN t2 ON t1.c0 = t2.c0 WHERE t2.c0 IS NULL;
/// ```
/// We cannot infer the predicate `t1.c0 IS NULL`, otherwise the predicate will be pushed down to
/// the left side, resulting in the wrong result.
struct InferredPredicates {
    predicates: Vec<Expr>,
    is_inner_join: bool,
}

impl InferredPredicates {
    fn new(join_type: JoinType) -> Self {
        Self {
            predicates: vec![],
            is_inner_join: matches!(join_type, JoinType::Inner),
        }
    }

    fn try_build_predicate(
        &mut self,
        predicate: Expr,
        replace_map: &HashMap<&Column, &Column>,
    ) -> Result<()> {
        if self.is_inner_join
            || matches!(
                is_restrict_null_predicate(
                    predicate.clone(),
                    replace_map.keys().cloned()
                ),
                Ok(true)
            )
        {
            self.predicates.push(replace_col(predicate, replace_map)?);
        }

        Ok(())
    }
}

/// Infer predicates from the pushed down predicates.
///
/// Parameters
/// * `join_col_keys` column pairs from the join ON clause
///
/// * `predicates` the pushed down predicates
///
/// * `inferred_predicates` the inferred results
fn infer_join_predicates_from_predicates(
    join_col_keys: &[(&Column, &Column)],
    predicates: &[Expr],
    inferred_predicates: &mut InferredPredicates,
) -> Result<()> {
    infer_join_predicates_impl::<true, true>(
        join_col_keys,
        predicates,
        inferred_predicates,
    )
}

/// Infer predicates from the join filter.
///
/// Parameters
/// * `join_col_keys` column pairs from the join ON clause
///
/// * `join_type` the JoinType of Join
///
/// * `on_filters` filters from the join ON clause that have not already been
///   identified as join predicates
///
/// * `inferred_predicates` the inferred results
fn infer_join_predicates_from_on_filters(
    join_col_keys: &[(&Column, &Column)],
    join_type: JoinType,
    on_filters: &[Expr],
    inferred_predicates: &mut InferredPredicates,
) -> Result<()> {
    match join_type {
        JoinType::Full | JoinType::LeftAnti | JoinType::RightAnti => Ok(()),
        JoinType::Inner => infer_join_predicates_impl::<true, true>(
            join_col_keys,
            on_filters,
            inferred_predicates,
        ),
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftMark => {
            infer_join_predicates_impl::<true, false>(
                join_col_keys,
                on_filters,
                inferred_predicates,
            )
        }
        JoinType::Right | JoinType::RightSemi | JoinType::RightMark => {
            infer_join_predicates_impl::<false, true>(
                join_col_keys,
                on_filters,
                inferred_predicates,
            )
        }
    }
}

/// Infer predicates from the given predicates.
///
/// Parameters
/// * `join_col_keys` column pairs from the join ON clause
///
/// * `input_predicates` the given predicates. It can be the pushed down predicates,
///   or it can be the filters of the Join
///
/// * `inferred_predicates` the inferred results
///
/// * `ENABLE_LEFT_TO_RIGHT` indicates that the right table related predicate can
///   be inferred from the left table related predicate
///
/// * `ENABLE_RIGHT_TO_LEFT` indicates that the left table related predicate can
///   be inferred from the right table related predicate
fn infer_join_predicates_impl<
    const ENABLE_LEFT_TO_RIGHT: bool,
    const ENABLE_RIGHT_TO_LEFT: bool,
>(
    join_col_keys: &[(&Column, &Column)],
    input_predicates: &[Expr],
    inferred_predicates: &mut InferredPredicates,
) -> Result<()> {
    for predicate in input_predicates {
        let mut join_cols_to_replace = HashMap::new();

        for &col in &predicate.column_refs() {
            for (l, r) in join_col_keys.iter() {
                if ENABLE_LEFT_TO_RIGHT && col == *l {
                    join_cols_to_replace.insert(col, *r);
                    break;
                }
                if ENABLE_RIGHT_TO_LEFT && col == *r {
                    join_cols_to_replace.insert(col, *l);
                    break;
                }
            }
        }
        if join_cols_to_replace.is_empty() {
            continue;
        }

        inferred_predicates
            .try_build_predicate(predicate.clone(), &join_cols_to_replace)?;
    }
    Ok(())
}

impl OptimizerRule for PushDownFilter {
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

        let plan_schema = Arc::clone(plan.schema());

        let LogicalPlan::Filter(mut filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        let predicate = split_conjunction_owned(filter.predicate.clone());
        let old_predicate_len = predicate.len();
        let new_predicates = simplify_predicates(predicate)?;
        if old_predicate_len != new_predicates.len() {
            let Some(new_predicate) = conjunction(new_predicates) else {
                // new_predicates is empty - remove the filter entirely
                // Return the child plan without the filter
                return Ok(Transformed::yes(Arc::unwrap_or_clone(filter.input)));
            };
            filter.predicate = new_predicate;
        }

        match Arc::unwrap_or_clone(filter.input) {
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
                #[allow(clippy::used_underscore_binding)]
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
                let predicates = split_conjunction_owned(filter.predicate.clone());
                let (new_projection, keep_predicate) =
                    rewrite_projection(predicates, projection)?;
                if new_projection.transformed {
                    match keep_predicate {
                        None => Ok(new_projection),
                        Some(keep_predicate) => new_projection.map_data(|child_plan| {
                            Filter::try_new(keep_predicate, Arc::new(child_plan))
                                .map(LogicalPlan::Filter)
                        }),
                    }
                } else {
                    filter.input = Arc::new(new_projection.data);
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            }
            LogicalPlan::Unnest(mut unnest) => {
                let predicates = split_conjunction_owned(filter.predicate.clone());
                let mut non_unnest_predicates = vec![];
                let mut unnest_predicates = vec![];
                let mut unnest_struct_columns = vec![];

                for idx in &unnest.struct_type_columns {
                    let (sub_qualifier, field) =
                        unnest.input.schema().qualified_field(*idx);
                    let field_name = field.name().clone();

                    if let DataType::Struct(children) = field.data_type() {
                        for child in children {
                            let child_name = child.name().clone();
                            unnest_struct_columns.push(Column::new(
                                sub_qualifier.cloned(),
                                format!("{field_name}.{child_name}"),
                            ));
                        }
                    }
                }

                for predicate in predicates {
                    // collect all the Expr::Column in predicate recursively
                    let mut accum: HashSet<Column> = HashSet::new();
                    expr_to_columns(&predicate, &mut accum)?;

                    let contains_list_columns =
                        unnest.list_type_columns.iter().any(|(_, unnest_list)| {
                            accum.contains(&unnest_list.output_column)
                        });
                    let contains_struct_columns =
                        unnest_struct_columns.iter().any(|c| accum.contains(c));

                    if contains_list_columns || contains_struct_columns {
                        unnest_predicates.push(predicate);
                    } else {
                        non_unnest_predicates.push(predicate);
                    }
                }

                // Unnest predicates should not be pushed down.
                // If no non-unnest predicates exist, early return
                if non_unnest_predicates.is_empty() {
                    filter.input = Arc::new(LogicalPlan::Unnest(unnest));
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                // Push down non-unnest filter predicate
                // Unnest
                //   Unnest Input (Projection)
                // -> rewritten to
                // Unnest
                //   Filter
                //     Unnest Input (Projection)

                let unnest_input = std::mem::take(&mut unnest.input);

                let filter_with_unnest_input = LogicalPlan::Filter(Filter::try_new(
                    conjunction(non_unnest_predicates).unwrap(), // Safe to unwrap since non_unnest_predicates is not empty.
                    unnest_input,
                )?);

                // Directly assign new filter plan as the new unnest's input.
                // The new filter plan will go through another rewrite pass since the rule itself
                // is applied recursively to all the child from top to down
                let unnest_plan =
                    insert_below(LogicalPlan::Unnest(unnest), filter_with_unnest_input)?;

                match conjunction(unnest_predicates) {
                    None => Ok(unnest_plan),
                    Some(predicate) => Ok(Transformed::yes(LogicalPlan::Filter(
                        Filter::try_new(predicate, Arc::new(unnest_plan.data))?,
                    ))),
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
                    .map(|e| {
                        let (relation, name) = e.qualified_name();
                        Column::new(relation, name)
                    })
                    .collect::<HashSet<_>>();

                let predicates = split_conjunction_owned(filter.predicate);

                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for expr in predicates {
                    let cols = expr.column_refs();
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
                    replace_map.insert(expr.schema_name().to_string(), expr.clone());
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
            // Tries to push filters based on the partition key(s) of the window function(s) used.
            // Example:
            //   Before:
            //     Filter: (a > 1) and (b > 1) and (c > 1)
            //      Window: func() PARTITION BY [a] ...
            //   ---
            //   After:
            //     Filter: (b > 1) and (c > 1)
            //      Window: func() PARTITION BY [a] ...
            //        Filter: (a > 1)
            LogicalPlan::Window(window) => {
                // Retrieve the set of potential partition keys where we can push filters by.
                // Unlike aggregations, where there is only one statement per SELECT, there can be
                // multiple window functions, each with potentially different partition keys.
                // Therefore, we need to ensure that any potential partition key returned is used in
                // ALL window functions. Otherwise, filters cannot be pushed by through that column.
                let extract_partition_keys = |func: &WindowFunction| {
                    func.params
                        .partition_by
                        .iter()
                        .map(|c| {
                            let (relation, name) = c.qualified_name();
                            Column::new(relation, name)
                        })
                        .collect::<HashSet<_>>()
                };
                let potential_partition_keys = window
                    .window_expr
                    .iter()
                    .map(|e| {
                        match e {
                            Expr::WindowFunction(window_func) => {
                                extract_partition_keys(window_func)
                            }
                            Expr::Alias(alias) => {
                                if let Expr::WindowFunction(window_func) =
                                    alias.expr.as_ref()
                                {
                                    extract_partition_keys(window_func)
                                } else {
                                    // window functions expressions are only Expr::WindowFunction
                                    unreachable!()
                                }
                            }
                            _ => {
                                // window functions expressions are only Expr::WindowFunction
                                unreachable!()
                            }
                        }
                    })
                    // performs the set intersection of the partition keys of all window functions,
                    // returning only the common ones
                    .reduce(|a, b| &a & &b)
                    .unwrap_or_default();

                let predicates = split_conjunction_owned(filter.predicate);
                let mut keep_predicates = vec![];
                let mut push_predicates = vec![];
                for expr in predicates {
                    let cols = expr.column_refs();
                    if cols.iter().all(|c| potential_partition_keys.contains(c)) {
                        push_predicates.push(expr);
                    } else {
                        keep_predicates.push(expr);
                    }
                }

                // Unlike with aggregations, there are no cases where we have to replace, e.g.,
                // `a+b` with Column(a)+Column(b). This is because partition expressions are not
                // available as standalone columns to the user. For example, while an aggregation on
                // `a+b` becomes Column(a + b), in a window partition it becomes
                // `func() PARTITION BY [a + b] ...`. Thus, filters on expressions always remain in
                // place, so we can use `push_predicates` directly. This is consistent with other
                // optimizers, such as the one used by Postgres.

                let window_input = Arc::clone(&window.input);
                Transformed::yes(LogicalPlan::Window(window))
                    .transform_data(|new_plan| {
                        // If we have a filter to push, we push it down to the input of the window
                        if let Some(predicate) = conjunction(push_predicates) {
                            let new_filter = make_filter(predicate, window_input)?;
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
            LogicalPlan::TableScan(scan) => {
                let filter_predicates = split_conjunction(&filter.predicate);

                let (volatile_filters, non_volatile_filters): (Vec<&Expr>, Vec<&Expr>) =
                    filter_predicates
                        .into_iter()
                        .partition(|pred| pred.is_volatile());

                // Check which non-volatile filters are supported by source
                let supported_filters = scan
                    .source
                    .supports_filters_pushdown(non_volatile_filters.as_slice())?;
                assert_eq_or_internal_err!(
                    non_volatile_filters.len(),
                    supported_filters.len(),
                    "Vec returned length: {} from supports_filters_pushdown is not the same size as the filters passed, which length is: {}",
                    supported_filters.len(),
                    non_volatile_filters.len()
                );

                // Compose scan filters from non-volatile filters of `Exact` or `Inexact` pushdown type
                let zip = non_volatile_filters.into_iter().zip(supported_filters);

                let new_scan_filters = zip
                    .clone()
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
                    .map(|(pred, _)| pred);

                // Add new scan filters
                let new_scan_filters: Vec<Expr> = scan
                    .filters
                    .iter()
                    .chain(new_scan_filters)
                    .unique()
                    .cloned()
                    .collect();

                // Compose predicates to be of `Unsupported` or `Inexact` pushdown type, and also include volatile filters
                let new_predicate: Vec<Expr> = zip
                    .filter(|(_, res)| res != &TableProviderFilterPushDown::Exact)
                    .map(|(pred, _)| pred)
                    .chain(volatile_filters)
                    .cloned()
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
                // This check prevents the Filter from being removed when the extension node has no children,
                // so we return the original Filter unchanged.
                if extension_plan.node.inputs().is_empty() {
                    filter.input = Arc::new(LogicalPlan::Extension(extension_plan));
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }
                let prevent_cols =
                    extension_plan.node.prevent_predicate_push_down_columns();

                // determine if we can push any predicates down past the extension node

                // each element is true for push, false to keep
                let predicate_push_or_keep = split_conjunction(&filter.predicate)
                    .iter()
                    .map(|expr| {
                        let cols = expr.column_refs();
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

/// Attempts to push `predicate` into a `FilterExec` below `projection
///
/// # Returns
/// (plan, remaining_predicate)
///
/// `plan` is a LogicalPlan for `projection` with possibly a new FilterExec below it.
/// `remaining_predicate` is any part of the predicate that could not be pushed down
///
/// # Args
/// - predicates: Split predicates like `[foo=5, bar=6]`
/// - projection: The target projection plan to push down the predicates
///
/// # Example
///
/// Pushing a predicate like `foo=5 AND bar=6` with an input plan like this:
///
/// ```text
/// Projection(foo, c+d as bar)
/// ```
///
/// Might result in returning `remaining_predicate` of `bar=6` and a plan like
///
/// ```text
/// Projection(foo, c+d as bar)
///  Filter(foo=5)
///   ...
/// ```
fn rewrite_projection(
    predicates: Vec<Expr>,
    mut projection: Projection,
) -> Result<(Transformed<LogicalPlan>, Option<Expr>)> {
    // A projection is filter-commutable if it do not contain volatile predicates or contain volatile
    // predicates that are not used in the filter. However, we should re-writes all predicate expressions.
    // collect projection.
    let (volatile_map, non_volatile_map): (HashMap<_, _>, HashMap<_, _>) = projection
        .schema
        .iter()
        .zip(projection.expr.iter())
        .map(|((qualifier, field), expr)| {
            // strip alias, as they should not be part of filters
            let expr = expr.clone().unalias();

            (qualified_name(qualifier, field.name()), expr)
        })
        .partition(|(_, value)| value.is_volatile());

    let mut push_predicates = vec![];
    let mut keep_predicates = vec![];
    for expr in predicates {
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
                std::mem::take(&mut projection.input),
            )?);

            projection.input = Arc::new(new_filter);

            Ok((
                Transformed::yes(LogicalPlan::Projection(projection)),
                conjunction(keep_predicates),
            ))
        }
        None => Ok((Transformed::no(LogicalPlan::Projection(projection)), None)),
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
    assert_or_internal_err!(new_child.is_none(), "node had no inputs");

    Ok(transformed_plan)
}

impl PushDownFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::cmp::Ordering;
    use std::fmt::{Debug, Formatter};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;

    use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
    use datafusion_expr::expr::{ScalarFunction, WindowFunction};
    use datafusion_expr::logical_plan::table_scan;
    use datafusion_expr::{
        col, in_list, in_subquery, lit, ColumnarValue, ExprFunctionExt, Extension,
        LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TableSource, TableType, UserDefinedLogicalNodeCore, Volatility,
        WindowFunctionDefinition,
    };

    use crate::assert_optimized_plan_eq_snapshot;
    use crate::optimizer::Optimizer;
    use crate::simplify_expressions::SimplifyExpressions;
    use crate::test::*;
    use crate::OptimizerContext;
    use datafusion_expr::test::function_stub::sum;
    use insta::assert_snapshot;

    use super::*;

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(PushDownFilter::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    macro_rules! assert_optimized_plan_eq_with_rewrite_predicate {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer = Optimizer::with_rules(vec![
                Arc::new(SimplifyExpressions::new()),
                Arc::new(PushDownFilter::new()),
            ]);
            let optimized_plan = optimizer.optimize($plan, &OptimizerContext::new(), observe)?;
            assert_snapshot!(optimized_plan, @ $expected);
            Ok::<(), DataFusionError>(())
        }};
    }

    #[test]
    fn filter_before_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.b
          TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )
    }

    #[test]
    fn filter_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(0, Some(10))?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before single projection
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.a = Int64(1)
          Limit: skip=0, fetch=10
            Projection: test.a, test.b
              TableScan: test
        "
        )
    }

    #[test]
    fn filter_no_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(lit(0i64).eq(lit(1i64)))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test, full_filters=[Int64(0) = Int64(1)]"
        )
    }

    #[test]
    fn filter_jump_2_plans() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("c"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter is before double projection
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c, test.b
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )
    }

    #[test]
    fn filter_move_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("total_salary")])?
            .filter(col("a").gt(lit(10i64)))?
            .build()?;
        // filter of key aggregation is commutative
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS total_salary]]
          TableScan: test, full_filters=[test.a > Int64(10)]
        "
        )
    }

    /// verifies that filters with unusual column names are pushed down through aggregate operators
    #[test]
    fn filter_move_agg_special() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("$a", DataType::UInt32, false),
            Field::new("$b", DataType::UInt32, false),
            Field::new("$c", DataType::UInt32, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("$a")], vec![sum(col("$b")).alias("total_salary")])?
            .filter(col("$a").gt(lit(10i64)))?
            .build()?;
        // filter of key aggregation is commutative
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.$a]], aggr=[[sum(test.$b) AS total_salary]]
          TableScan: test, full_filters=[test.$a > Int64(10)]
        "
        )
    }

    #[test]
    fn filter_complex_group_by() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.b > Int64(10)
          Aggregate: groupBy=[[test.b + test.a]], aggr=[[sum(test.a), test.b]]
            TableScan: test
        "
        )
    }

    #[test]
    fn push_agg_need_replace_expr() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .aggregate(vec![add(col("b"), col("a"))], vec![sum(col("a")), col("b")])?
            .filter(col("test.b + test.a").gt(lit(10i64)))?
            .build()?;
        assert_optimized_plan_equal!(
            plan,
            @r"
        Aggregate: groupBy=[[test.b + test.a]], aggr=[[sum(test.a), test.b]]
          TableScan: test, full_filters=[test.b + test.a > Int64(10)]
        "
        )
    }

    #[test]
    fn filter_keep_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).alias("b")])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;
        // filter of aggregate is after aggregation since they are non-commutative
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: b > Int64(10)
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b) AS b]]
            TableScan: test
        "
        )
    }

    /// verifies that when partitioning by 'a' and 'b', and filtering by 'b', 'b' is pushed
    #[test]
    fn filter_move_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a"), col("b")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .filter(col("b").gt(lit(10i64)))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        WindowAggr: windowExpr=[[rank() PARTITION BY [test.a, test.b] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          TableScan: test, full_filters=[test.b > Int64(10)]
        "
        )
    }

    /// verifies that filters with unusual identifier names are pushed down through window functions
    #[test]
    fn filter_window_special_identifier() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("$a", DataType::UInt32, false),
            Field::new("$b", DataType::UInt32, false),
            Field::new("$c", DataType::UInt32, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("$a"), col("$b")])
        .order_by(vec![col("$c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .filter(col("$b").gt(lit(10i64)))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        WindowAggr: windowExpr=[[rank() PARTITION BY [test.$a, test.$b] ORDER BY [test.$c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          TableScan: test, full_filters=[test.$b > Int64(10)]
        "
        )
    }

    /// verifies that when partitioning by 'a' and 'b', and filtering by 'a' and 'b', both 'a' and
    /// 'b' are pushed
    #[test]
    fn filter_move_complex_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a"), col("b")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .filter(and(col("a").gt(lit(10i64)), col("b").eq(lit(1i64))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        WindowAggr: windowExpr=[[rank() PARTITION BY [test.a, test.b] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          TableScan: test, full_filters=[test.a > Int64(10), test.b = Int64(1)]
        "
        )
    }

    /// verifies that when partitioning by 'a' and filtering by 'a' and 'b', only 'a' is pushed
    #[test]
    fn filter_move_partial_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .filter(and(col("a").gt(lit(10i64)), col("b").eq(lit(1i64))))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.b = Int64(1)
          WindowAggr: windowExpr=[[rank() PARTITION BY [test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: test, full_filters=[test.a > Int64(10)]
        "
        )
    }

    /// verifies that filters on partition expressions are not pushed, as the single expression
    /// column is not available to the user, unlike with aggregations
    #[test]
    fn filter_expression_keep_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![add(col("a"), col("b"))]) // PARTITION BY a + b
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            // unlike with aggregations, single partition column "test.a + test.b" is not available
            // to the plan, so we use multiple columns when filtering
            .filter(add(col("a"), col("b")).gt(lit(10i64)))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.a + test.b > Int64(10)
          WindowAggr: windowExpr=[[rank() PARTITION BY [test.a + test.b] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: test
        "
        )
    }

    /// verifies that filters are not pushed on order by columns (that are not used in partitioning)
    #[test]
    fn filter_order_keep_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .filter(col("c").gt(lit(10i64)))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.c > Int64(10)
          WindowAggr: windowExpr=[[rank() PARTITION BY [test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: test
        "
        )
    }

    /// verifies that when we use multiple window functions with a common partition key, the filter
    /// on that key is pushed
    #[test]
    fn filter_multiple_windows_common_partitions() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window1 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let window2 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("b"), col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window1, window2])?
            .filter(col("a").gt(lit(10i64)))? // a appears in both window functions
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        WindowAggr: windowExpr=[[rank() PARTITION BY [test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, rank() PARTITION BY [test.b, test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          TableScan: test, full_filters=[test.a > Int64(10)]
        "
        )
    }

    /// verifies that when we use multiple window functions with different partitions keys, the
    /// filter cannot be pushed
    #[test]
    fn filter_multiple_windows_disjoint_partitions() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window1 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let window2 = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("b"), col("a")])
        .order_by(vec![col("c").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window1, window2])?
            .filter(col("b").gt(lit(10i64)))? // b only appears in one window function
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.b > Int64(10)
          WindowAggr: windowExpr=[[rank() PARTITION BY [test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, rank() PARTITION BY [test.b, test.a] ORDER BY [test.c ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: test
        "
        )
    }

    /// verifies that a filter is pushed to before a projection, the filter expression is correctly re-written
    #[test]
    fn alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;
        // filter is before projection
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS b, test.c
          TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )
    }

    fn add(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Plus,
            Box::new(right),
        ))
    }

    fn multiply(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Multiply,
            Box::new(right),
        ))
    }

    /// verifies that a filter is pushed to before a projection with a complex expression, the filter expression is correctly re-written
    #[test]
    fn complex_expression() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: b = Int64(1)
          Projection: test.a * Int32(2) + test.c AS b, test.c
            TableScan: test
        ",
        );
        // filter is before projection
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a * Int32(2) + test.c AS b, test.c
          TableScan: test, full_filters=[test.a * Int32(2) + test.c = Int64(1)]
        "
        )
    }

    /// verifies that when a filter is pushed to after 2 projections, the filter expression is correctly re-written
    #[test]
    fn complex_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                add(multiply(col("a"), lit(2)), col("c")).alias("b"),
                col("c"),
            ])?
            // second projection where we rename columns, just to make it difficult
            .project(vec![multiply(col("b"), lit(3)).alias("a"), col("c")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: a = Int64(1)
          Projection: b * Int32(3) AS a, test.c
            Projection: test.a * Int32(2) + test.c AS b, test.c
              TableScan: test
        ",
        );
        // filter is before the projections
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: b * Int32(3) AS a, test.c
          Projection: test.a * Int32(2) + test.c AS b, test.c
            TableScan: test, full_filters=[(test.a * Int32(2) + test.c) * Int32(3) = Int64(1)]
        "
        )
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct NoopPlan {
        input: Vec<LogicalPlan>,
        schema: DFSchemaRef,
    }

    // Manual implementation needed because of `schema` field. Comparison excludes this field.
    impl PartialOrd for NoopPlan {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.input
                .partial_cmp(&other.input)
                // TODO (https://github.com/apache/datafusion/issues/17477) avoid recomparing all fields
                .filter(|cmp| *cmp != Ordering::Equal || self == other)
        }
    }

    impl UserDefinedLogicalNodeCore for NoopPlan {
        fn name(&self) -> &str {
            "NoopPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            self.input.iter().collect()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.input
                .iter()
                .flat_map(|child| child.expressions())
                .collect()
        }

        fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
            HashSet::from_iter(vec!["c".to_string()])
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoopPlan")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                input: inputs,
                schema: Arc::clone(&self.schema),
            })
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }

    #[test]
    fn user_defined_plan() -> Result<()> {
        let table_scan = test_table_scan()?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // Push filter below NoopPlan
        assert_optimized_plan_equal!(
            plan,
            @r"
        NoopPlan
          TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
            .build()?;

        // Push only predicate on `a` below NoopPlan
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.c = Int64(2)
          NoopPlan
            TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone(), table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)))?
            .build()?;

        // Push filter below NoopPlan for each child branch
        assert_optimized_plan_equal!(
            plan,
            @r"
        NoopPlan
          TableScan: test, full_filters=[test.a = Int64(1)]
          TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )?;

        let custom_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone(), table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });
        let plan = LogicalPlanBuilder::from(custom_plan)
            .filter(col("a").eq(lit(1i64)).and(col("c").eq(lit(2i64))))?
            .build()?;

        // Push only predicate on `a` below NoopPlan
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.c = Int64(2)
          NoopPlan
            TableScan: test, full_filters=[test.a = Int64(1)]
            TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )
    }

    /// verifies that when two filters apply after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn multi_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (sum(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(col("b").gt(lit(10i64)))?
            .filter(col("sum(test.c)").gt(lit(10i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: sum(test.c) > Int64(10)
          Filter: b > Int64(10)
            Aggregate: groupBy=[[b]], aggr=[[sum(test.c)]]
              Projection: test.a AS b, test.c
                TableScan: test
        ",
        );
        // filter is before the projections
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: sum(test.c) > Int64(10)
          Aggregate: groupBy=[[b]], aggr=[[sum(test.c)]]
            Projection: test.a AS b, test.c
              TableScan: test, full_filters=[test.a > Int64(10)]
        "
        )
    }

    /// verifies that when a filter with two predicates is applied after an aggregation that only allows one to be pushed, one is pushed
    /// and the other not.
    #[test]
    fn split_filter() -> Result<()> {
        // the aggregation allows one filter to pass (b), and the other one to not pass (sum(c))
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .aggregate(vec![col("b")], vec![sum(col("c"))])?
            .filter(and(
                col("sum(test.c)").gt(lit(10i64)),
                and(col("b").gt(lit(10i64)), col("sum(test.c)").lt(lit(20i64))),
            ))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: sum(test.c) > Int64(10) AND b > Int64(10) AND sum(test.c) < Int64(20)
          Aggregate: groupBy=[[b]], aggr=[[sum(test.c)]]
            Projection: test.a AS b, test.c
              TableScan: test
        ",
        );
        // filter is before the projections
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: sum(test.c) > Int64(10) AND sum(test.c) < Int64(20)
          Aggregate: groupBy=[[b]], aggr=[[sum(test.c)]]
            Projection: test.a AS b, test.c
              TableScan: test, full_filters=[test.a > Int64(10)]
        "
        )
    }

    /// verifies that when two limits are in place, we jump neither
    #[test]
    fn double_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .limit(0, Some(20))?
            .limit(0, Some(10))?
            .project(vec![col("a"), col("b")])?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter does not just any of the limits
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.b
          Filter: test.a = Int64(1)
            Limit: skip=0, fetch=10
              Limit: skip=0, fetch=20
                Projection: test.a, test.b
                  TableScan: test
        "
        )
    }

    #[test]
    fn union_all() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table_scan2 = test_table_scan_with_name("test2")?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .union(LogicalPlanBuilder::from(table_scan2).build()?)?
            .filter(col("a").eq(lit(1i64)))?
            .build()?;
        // filter appears below Union
        assert_optimized_plan_equal!(
            plan,
            @r"
        Union
          TableScan: test, full_filters=[test.a = Int64(1)]
          TableScan: test2, full_filters=[test2.a = Int64(1)]
        "
        )
    }

    #[test]
    fn union_all_on_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b")])?
            .alias("test2")?;

        let plan = table
            .clone()
            .union(table.build()?)?
            .filter(col("b").eq(lit(1i64)))?
            .build()?;

        // filter appears below Union
        assert_optimized_plan_equal!(
            plan,
            @r"
        Union
          SubqueryAlias: test2
            Projection: test.a AS b
              TableScan: test, full_filters=[test.a = Int64(1)]
          SubqueryAlias: test2
            Projection: test.a AS b
              TableScan: test, full_filters=[test.a = Int64(1)]
        "
        )
    }

    #[test]
    fn test_union_different_schema() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;

        let schema = Schema::new(vec![
            Field::new("d", DataType::UInt32, false),
            Field::new("e", DataType::UInt32, false),
            Field::new("f", DataType::UInt32, false),
        ]);
        let right = table_scan(Some("test1"), &schema, None)?
            .project(vec![col("d"), col("e"), col("f")])?
            .build()?;
        let filter = and(col("test.a").eq(lit(1)), col("test1.d").gt(lit(2)));
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .project(vec![col("test.a"), col("test1.d")])?
            .filter(filter)?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test1.d
          Cross Join: 
            Projection: test.a, test.b, test.c
              TableScan: test, full_filters=[test.a = Int32(1)]
            Projection: test1.d, test1.e, test1.f
              TableScan: test1, full_filters=[test1.d > Int32(2)]
        "
        )
    }

    #[test]
    fn test_project_same_name_different_qualifier() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test1")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = and(col("test.a").eq(lit(1)), col("test1.a").gt(lit(2)));
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .project(vec![col("test.a"), col("test1.a")])?
            .filter(filter)?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test1.a
          Cross Join: 
            Projection: test.a, test.b, test.c
              TableScan: test, full_filters=[test.a = Int32(1)]
            Projection: test1.a, test1.b, test1.c
              TableScan: test1, full_filters=[test1.a > Int32(2)]
        "
        )
    }

    /// verifies that filters with the same columns are correctly placed
    #[test]
    fn filter_2_breaks_limits() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(col("a").lt_eq(lit(1i64)))?
            .limit(0, Some(1))?
            .project(vec![col("a")])?
            .filter(col("a").gt_eq(lit(1i64)))?
            .build()?;
        // Should be able to move both filters below the projections

        // not part of the test
        assert_snapshot!(plan,
        @r"
        Filter: test.a >= Int64(1)
          Projection: test.a
            Limit: skip=0, fetch=1
              Filter: test.a <= Int64(1)
                Projection: test.a
                  TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a
          Filter: test.a >= Int64(1)
            Limit: skip=0, fetch=1
              Projection: test.a
                TableScan: test, full_filters=[test.a <= Int64(1)]
        "
        )
    }

    /// verifies that filters to be placed on the same depth are ANDed
    #[test]
    fn two_filters_on_same_depth() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1))?
            .filter(col("a").lt_eq(lit(1i64)))?
            .filter(col("a").gt_eq(lit(1i64)))?
            .project(vec![col("a")])?
            .build()?;

        // not part of the test
        assert_snapshot!(plan,
        @r"
        Projection: test.a
          Filter: test.a >= Int64(1)
            Filter: test.a <= Int64(1)
              Limit: skip=0, fetch=1
                TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a
          Filter: test.a >= Int64(1) AND test.a <= Int64(1)
            Limit: skip=0, fetch=1
              TableScan: test
        "
        )
    }

    /// verifies that filters on a plan with user nodes are not lost
    /// (ARROW-10547)
    #[test]
    fn filters_user_defined_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let plan = user_defined::new(plan);

        // not part of the test
        assert_snapshot!(plan,
        @r"
        TestUserDefined
          Filter: test.a <= Int64(1)
            TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        TestUserDefined
          TableScan: test, full_filters=[test.a <= Int64(1)]
        "
        )
    }

    /// post-on-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_on_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("test.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.a <= Int64(1)
          Inner Join: test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter sent to side before the join
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.a
          TableScan: test, full_filters=[test.a <= Int64(1)]
          Projection: test2.a
            TableScan: test2, full_filters=[test2.a <= Int64(1)]
        "
        )
    }

    /// post-using-join predicates on a column common to both sides is pushed to both sides
    #[test]
    fn filter_using_join_on_common_independent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.a <= Int64(1)
          Inner Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter sent to side before the join
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: Using test.a = test2.a
          TableScan: test, full_filters=[test.a <= Int64(1)]
          Projection: test2.a
            TableScan: test2, full_filters=[test2.a <= Int64(1)]
        "
        )
    }

    /// post-join predicates with columns from both sides are converted to join filters
    #[test]
    fn filter_join_on_common_dependent() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("c").lt_eq(col("b")))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.c <= test2.b
          Inner Join: test.a = test2.a
            Projection: test.a, test.c
              TableScan: test
            Projection: test2.a, test2.b
              TableScan: test2
        ",
        );
        // Filter is converted to Join Filter
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.a Filter: test.c <= test2.b
          Projection: test.a, test.c
            TableScan: test
          Projection: test2.a, test2.b
            TableScan: test2
        "
        )
    }

    /// post-join predicates with columns from one side of a join are pushed only to that side
    #[test]
    fn filter_join_on_one_side() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let table_scan_right = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(table_scan_right)
            .project(vec![col("a"), col("c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("b").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.b <= Int64(1)
          Inner Join: test.a = test2.a
            Projection: test.a, test.b
              TableScan: test
            Projection: test2.a, test2.c
              TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.a
          Projection: test.a, test.b
            TableScan: test, full_filters=[test.b <= Int64(1)]
          Projection: test2.a, test2.c
            TableScan: test2
        "
        )
    }

    /// post-join predicates on the right side of a left join are not duplicated
    /// TODO: In this case we can sometimes convert the join to an INNER join
    #[test]
    fn filter_using_left_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test2.a <= Int64(1)
          Left Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter not duplicated nor pushed down - i.e. noop
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test2.a <= Int64(1)
          Left Join: Using test.a = test2.a
            TableScan: test, full_filters=[test.a <= Int64(1)]
            Projection: test2.a
              TableScan: test2
        "
        )
    }

    /// post-join predicates on the left side of a right join are not duplicated
    #[test]
    fn filter_using_right_join() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.a <= Int64(1)
          Right Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter not duplicated nor pushed down - i.e. noop
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test.a <= Int64(1)
          Right Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2, full_filters=[test2.a <= Int64(1)]
        "
        )
    }

    /// post-left-join predicate on a column common to both sides is only pushed to the left side
    /// i.e. - not duplicated to the right side
    #[test]
    fn filter_using_left_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Left,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test.a <= Int64(1)
          Left Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter sent to left side of the join, not the right
        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: Using test.a = test2.a
          TableScan: test, full_filters=[test.a <= Int64(1)]
          Projection: test2.a
            TableScan: test2
        "
        )
    }

    /// post-right-join predicate on a column common to both sides is only pushed to the right side
    /// i.e. - not duplicated to the left side.
    #[test]
    fn filter_using_right_join_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                right,
                JoinType::Right,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test2.a <= Int64(1)
          Right Join: Using test.a = test2.a
            TableScan: test
            Projection: test2.a
              TableScan: test2
        ",
        );
        // filter sent to right side of join, not duplicated to the left
        assert_optimized_plan_equal!(
            plan,
            @r"
        Right Join: Using test.a = test2.a
          TableScan: test
          Projection: test2.a
            TableScan: test2, full_filters=[test2.a <= Int64(1)]
        "
        )
    }

    /// single table predicate parts of ON condition should be pushed to both inputs
    #[test]
    fn join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.c")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Inner Join: test.a = test2.a Filter: test.c > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.a Filter: test.b < test2.b
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.c > UInt32(1)]
          Projection: test2.a, test2.b, test2.c
            TableScan: test2, full_filters=[test2.c > UInt32(4)]
        "
        )
    }

    /// join filter should be completely removed after pushdown
    #[test]
    fn join_filter_removed() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.b")
            .gt(lit(1u32))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Inner Join: test.a = test2.a Filter: test.b > UInt32(1) AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.a
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.b > UInt32(1)]
          Projection: test2.a, test2.b, test2.c
            TableScan: test2, full_filters=[test2.c > UInt32(4)]
        "
        )
    }

    /// predicate on join key in filter expression should be pushed down to both inputs
    #[test]
    fn join_filter_on_common() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("b")])?
            .build()?;
        let filter = col("test.a").gt(lit(1u32));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("b")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Inner Join: test.a = test2.b Filter: test.a > UInt32(1)
          Projection: test.a
            TableScan: test
          Projection: test2.b
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: test.a = test2.b
          Projection: test.a
            TableScan: test, full_filters=[test.a > UInt32(1)]
          Projection: test2.b
            TableScan: test2, full_filters=[test2.b > UInt32(1)]
        "
        )
    }

    /// single table predicate parts of ON condition should be pushed to right input
    #[test]
    fn left_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Left Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2, full_filters=[test2.c > UInt32(4)]
        "
        )
    }

    /// single table predicate parts of ON condition should be pushed to left input
    #[test]
    fn right_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Right Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Right Join: test.a = test2.a Filter: test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.a > UInt32(1)]
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        "
        )
    }

    /// single table predicate parts of ON condition should not be pushed
    #[test]
    fn full_join_on_with_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let filter = col("test.a")
            .gt(lit(1u32))
            .and(col("test.b").lt(col("test2.b")))
            .and(col("test2.c").gt(lit(4u32)));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                Some(filter),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Full Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Full Join: test.a = test2.a Filter: test.a > UInt32(1) AND test.b < test2.b AND test2.c > UInt32(4)
          Projection: test.a, test.b, test.c
            TableScan: test
          Projection: test2.a, test2.b, test2.c
            TableScan: test2
        "
        )
    }

    struct PushDownProvider {
        pub filter_support: TableProviderFilterPushDown,
    }

    #[async_trait]
    impl TableSource for PushDownProvider {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ]))
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            Ok((0..filters.len())
                .map(|_| self.filter_support.clone())
                .collect())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    fn table_scan_with_pushdown_provider_builder(
        filter_support: TableProviderFilterPushDown,
        filters: Vec<Expr>,
        projection: Option<Vec<usize>>,
    ) -> Result<LogicalPlanBuilder> {
        let test_provider = PushDownProvider { filter_support };

        let table_scan = LogicalPlan::TableScan(TableScan {
            table_name: "test".into(),
            filters,
            projected_schema: Arc::new(DFSchema::try_from(test_provider.schema())?),
            projection,
            source: Arc::new(test_provider),
            fetch: None,
        });

        Ok(LogicalPlanBuilder::from(table_scan))
    }

    fn table_scan_with_pushdown_provider(
        filter_support: TableProviderFilterPushDown,
    ) -> Result<LogicalPlan> {
        table_scan_with_pushdown_provider_builder(filter_support, vec![], None)?
            .filter(col("a").eq(lit(1i64)))?
            .build()
    }

    #[test]
    fn filter_with_table_provider_exact() -> Result<()> {
        let plan = table_scan_with_pushdown_provider(TableProviderFilterPushDown::Exact)?;

        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test, full_filters=[a = Int64(1)]"
        )
    }

    #[test]
    fn filter_with_table_provider_inexact() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: a = Int64(1)
          TableScan: test, partial_filters=[a = Int64(1)]
        "
        )
    }

    #[test]
    fn filter_with_table_provider_multiple_invocations() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Inexact)?;

        let optimized_plan = PushDownFilter::new()
            .rewrite(plan, &OptimizerContext::new())
            .expect("failed to optimize plan")
            .data;

        // Optimizing the same plan multiple times should produce the same plan
        // each time.
        assert_optimized_plan_equal!(
            optimized_plan,
            @r"
        Filter: a = Int64(1)
          TableScan: test, partial_filters=[a = Int64(1)]
        "
        )
    }

    #[test]
    fn filter_with_table_provider_unsupported() -> Result<()> {
        let plan =
            table_scan_with_pushdown_provider(TableProviderFilterPushDown::Unsupported)?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: a = Int64(1)
          TableScan: test
        "
        )
    }

    #[test]
    fn multi_combined_filter() -> Result<()> {
        let plan = table_scan_with_pushdown_provider_builder(
            TableProviderFilterPushDown::Inexact,
            vec![col("a").eq(lit(10i64)), col("b").gt(lit(11i64))],
            Some(vec![0]),
        )?
        .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
        .project(vec![col("a"), col("b")])?
        .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: a, b
          Filter: a = Int64(10) AND b > Int64(11)
            TableScan: test projection=[a], partial_filters=[a = Int64(10), b > Int64(11)]
        "
        )
    }

    #[test]
    fn multi_combined_filter_exact() -> Result<()> {
        let plan = table_scan_with_pushdown_provider_builder(
            TableProviderFilterPushDown::Exact,
            vec![],
            Some(vec![0]),
        )?
        .filter(and(col("a").eq(lit(10i64)), col("b").gt(lit(11i64))))?
        .project(vec![col("a"), col("b")])?
        .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: a, b
          TableScan: test projection=[a], full_filters=[a = Int64(10), b > Int64(11)]
        "
        )
    }

    #[test]
    fn test_filter_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
            .build()?;

        // filter on col b
        assert_snapshot!(plan,
        @r"
        Filter: b > Int64(10) AND test.c > Int64(10)
          Projection: test.a AS b, test.c
            TableScan: test
        ",
        );
        // rewrite filter col b to test.a
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS b, test.c
          TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]
        "
        )
    }

    #[test]
    fn test_filter_with_alias_2() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .project(vec![col("b"), col("c")])?
            .filter(and(col("b").gt(lit(10i64)), col("c").gt(lit(10i64))))?
            .build()?;

        // filter on col b
        assert_snapshot!(plan,
        @r"
        Filter: b > Int64(10) AND test.c > Int64(10)
          Projection: b, test.c
            Projection: test.a AS b, test.c
              TableScan: test
        ",
        );
        // rewrite filter col b to test.a
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: b, test.c
          Projection: test.a AS b, test.c
            TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]
        "
        )
    }

    #[test]
    fn test_filter_with_multi_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c").alias("d")])?
            .filter(and(col("b").gt(lit(10i64)), col("d").gt(lit(10i64))))?
            .build()?;

        // filter on col b and d
        assert_snapshot!(plan,
        @r"
        Filter: b > Int64(10) AND d > Int64(10)
          Projection: test.a AS b, test.c AS d
            TableScan: test
        ",
        );
        // rewrite filter col b to test.a, col d to test.c
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS b, test.c AS d
          TableScan: test, full_filters=[test.a > Int64(10), test.c > Int64(10)]
        "
        )
    }

    /// predicate on join key in filter expression should be pushed down to both inputs
    #[test]
    fn join_filter_with_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("b").alias("d")])?
            .build()?;
        let filter = col("c").gt(lit(1u32));
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec![Column::from_name("c")], vec![Column::from_name("d")]),
                Some(filter),
            )?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Inner Join: c = d Filter: c > UInt32(1)
          Projection: test.a AS c
            TableScan: test
          Projection: test2.b AS d
            TableScan: test2
        ",
        );
        // Change filter on col `c`, 'd' to `test.a`, 'test.b'
        assert_optimized_plan_equal!(
            plan,
            @r"
        Inner Join: c = d
          Projection: test.a AS c
            TableScan: test, full_filters=[test.a > UInt32(1)]
          Projection: test2.b AS d
            TableScan: test2, full_filters=[test2.b > UInt32(1)]
        "
        )
    }

    #[test]
    fn test_in_filter_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(in_list(col("b"), filter_value, false))?
            .build()?;

        // filter on col b
        assert_snapshot!(plan,
        @r"
        Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])
          Projection: test.a AS b, test.c
            TableScan: test
        ",
        );
        // rewrite filter col b to test.a
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS b, test.c
          TableScan: test, full_filters=[test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])]
        "
        )
    }

    #[test]
    fn test_in_filter_with_alias_2() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in filter
        // we need rewrite filter col before push down.
        let table_scan = test_table_scan()?;
        let filter_value = vec![lit(1u32), lit(2u32), lit(3u32), lit(4u32)];
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .project(vec![col("b"), col("c")])?
            .filter(in_list(col("b"), filter_value, false))?
            .build()?;

        // filter on col b
        assert_snapshot!(plan,
        @r"
        Filter: b IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])
          Projection: b, test.c
            Projection: test.a AS b, test.c
              TableScan: test
        ",
        );
        // rewrite filter col b to test.a
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: b, test.c
          Projection: test.a AS b, test.c
            TableScan: test, full_filters=[test.a IN ([UInt32(1), UInt32(2), UInt32(3), UInt32(4)])]
        "
        )
    }

    #[test]
    fn test_in_subquery_with_alias() -> Result<()> {
        // in table scan the true col name is 'test.a',
        // but we rename it as 'b', and use col 'b' in subquery filter
        let table_scan = test_table_scan()?;
        let table_scan_sq = test_table_scan_with_name("sq")?;
        let subplan = Arc::new(
            LogicalPlanBuilder::from(table_scan_sq)
                .project(vec![col("c")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .filter(in_subquery(col("b"), subplan))?
            .build()?;

        // filter on col b in subquery
        assert_snapshot!(plan,
        @r"
        Filter: b IN (<subquery>)
          Subquery:
            Projection: sq.c
              TableScan: sq
          Projection: test.a AS b, test.c
            TableScan: test
        ",
        );
        // rewrite filter col b to test.a
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a AS b, test.c
          TableScan: test, full_filters=[test.a IN (<subquery>)]
            Subquery:
              Projection: sq.c
                TableScan: sq
        "
        )
    }

    #[test]
    fn test_propagation_of_optimized_inner_filters_with_projections() -> Result<()> {
        // SELECT a FROM (SELECT 1 AS a) b WHERE b.a = 1
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![lit(0i64).alias("a")])?
            .alias("b")?
            .project(vec![col("b.a")])?
            .alias("b")?
            .filter(col("b.a").eq(lit(1i64)))?
            .project(vec![col("b.a")])?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Projection: b.a
          Filter: b.a = Int64(1)
            SubqueryAlias: b
              Projection: b.a
                SubqueryAlias: b
                  Projection: Int64(0) AS a
                    EmptyRelation: rows=1
        ",
        );
        // Ensure that the predicate without any columns (0 = 1) is
        // still there.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: b.a
          SubqueryAlias: b
            Projection: b.a
              SubqueryAlias: b
                Projection: Int64(0) AS a
                  Filter: Int64(0) = Int64(1)
                    EmptyRelation: rows=1
        "
        )
    }

    #[test]
    fn test_crossjoin_with_or_clause() -> Result<()> {
        // select * from test,test1 where (test.a = test1.a and test.b > 1) or (test.b = test1.b and test.c < 10);
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test1")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a").alias("d"), col("a").alias("e")])?
            .build()?;
        let filter = or(
            and(col("a").eq(col("d")), col("b").gt(lit(1u32))),
            and(col("b").eq(col("e")), col("c").lt(lit(10u32))),
        );
        let plan = LogicalPlanBuilder::from(left)
            .cross_join(right)?
            .filter(filter)?
            .build()?;

        assert_optimized_plan_eq_with_rewrite_predicate!(plan.clone(), @r"
        Inner Join:  Filter: test.a = d AND test.b > UInt32(1) OR test.b = e AND test.c < UInt32(10)
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.b > UInt32(1) OR test.c < UInt32(10)]
          Projection: test1.a AS d, test1.a AS e
            TableScan: test1
        ")?;

        // Originally global state which can help to avoid duplicate Filters been generated and pushed down.
        // Now the global state is removed. Need to double confirm that avoid duplicate Filters.
        let optimized_plan = PushDownFilter::new()
            .rewrite(plan, &OptimizerContext::new())
            .expect("failed to optimize plan")
            .data;
        assert_optimized_plan_equal!(
            optimized_plan,
            @r"
        Inner Join:  Filter: test.a = d AND test.b > UInt32(1) OR test.b = e AND test.c < UInt32(10)
          Projection: test.a, test.b, test.c
            TableScan: test, full_filters=[test.b > UInt32(1) OR test.c < UInt32(10)]
          Projection: test1.a AS d, test1.a AS e
            TableScan: test1
        "
        )
    }

    #[test]
    fn left_semi_join() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                None,
            )?
            .filter(col("test2.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test2.a <= Int64(1)
          LeftSemi Join: test1.a = test2.a
            TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2
        ",
        );
        // Inferred the predicate `test1.a <= Int64(1)` and push it down to the left side.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test2.a <= Int64(1)
          LeftSemi Join: test1.a = test2.a
            TableScan: test1, full_filters=[test1.a <= Int64(1)]
            Projection: test2.a, test2.b
              TableScan: test2
        "
        )
    }

    #[test]
    fn left_semi_join_with_filters() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        LeftSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)
          TableScan: test1
          Projection: test2.a, test2.b
            TableScan: test2
        ",
        );
        // Both side will be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        LeftSemi Join: test1.a = test2.a
          TableScan: test1, full_filters=[test1.b > UInt32(1)]
          Projection: test2.a, test2.b
            TableScan: test2, full_filters=[test2.b > UInt32(2)]
        "
        )
    }

    #[test]
    fn right_semi_join() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                None,
            )?
            .filter(col("test1.a").lt_eq(lit(1i64)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test1.a <= Int64(1)
          RightSemi Join: test1.a = test2.a
            TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2
        ",
        );
        // Inferred the predicate `test2.a <= Int64(1)` and push it down to the right side.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test1.a <= Int64(1)
          RightSemi Join: test1.a = test2.a
            TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2, full_filters=[test2.a <= Int64(1)]
        "
        )
    }

    #[test]
    fn right_semi_join_with_filters() -> Result<()> {
        let left = test_table_scan_with_name("test1")?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightSemi,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        RightSemi Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)
          TableScan: test1
          Projection: test2.a, test2.b
            TableScan: test2
        ",
        );
        // Both side will be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        RightSemi Join: test1.a = test2.a
          TableScan: test1, full_filters=[test1.b > UInt32(1)]
          Projection: test2.a, test2.b
            TableScan: test2, full_filters=[test2.b > UInt32(2)]
        "
        )
    }

    #[test]
    fn left_anti_join() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                None,
            )?
            .filter(col("test2.a").gt(lit(2u32)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test2.a > UInt32(2)
          LeftAnti Join: test1.a = test2.a
            Projection: test1.a, test1.b
              TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2
        ",
        );
        // For left anti, filter of the right side filter can be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test2.a > UInt32(2)
          LeftAnti Join: test1.a = test2.a
            Projection: test1.a, test1.b
              TableScan: test1, full_filters=[test1.a > UInt32(2)]
            Projection: test2.a, test2.b
              TableScan: test2
        "
        )
    }

    #[test]
    fn left_anti_join_with_filters() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)
          Projection: test1.a, test1.b
            TableScan: test1
          Projection: test2.a, test2.b
            TableScan: test2
        ",
        );
        // For left anti, filter of the right side filter can be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        LeftAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1)
          Projection: test1.a, test1.b
            TableScan: test1
          Projection: test2.a, test2.b
            TableScan: test2, full_filters=[test2.b > UInt32(2)]
        "
        )
    }

    #[test]
    fn right_anti_join() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                None,
            )?
            .filter(col("test1.a").gt(lit(2u32)))?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        Filter: test1.a > UInt32(2)
          RightAnti Join: test1.a = test2.a
            Projection: test1.a, test1.b
              TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2
        ",
        );
        // For right anti, filter of the left side can be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: test1.a > UInt32(2)
          RightAnti Join: test1.a = test2.a
            Projection: test1.a, test1.b
              TableScan: test1
            Projection: test2.a, test2.b
              TableScan: test2, full_filters=[test2.a > UInt32(2)]
        "
        )
    }

    #[test]
    fn right_anti_join_with_filters() -> Result<()> {
        let table_scan = test_table_scan_with_name("test1")?;
        let left = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a"), col("b")])?
            .build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::RightAnti,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                Some(
                    col("test1.b")
                        .gt(lit(1u32))
                        .and(col("test2.b").gt(lit(2u32))),
                ),
            )?
            .build()?;

        // not part of the test, just good to know:
        assert_snapshot!(plan,
        @r"
        RightAnti Join: test1.a = test2.a Filter: test1.b > UInt32(1) AND test2.b > UInt32(2)
          Projection: test1.a, test1.b
            TableScan: test1
          Projection: test2.a, test2.b
            TableScan: test2
        ",
        );
        // For right anti, filter of the left side can be pushed down.
        assert_optimized_plan_equal!(
            plan,
            @r"
        RightAnti Join: test1.a = test2.a Filter: test2.b > UInt32(2)
          Projection: test1.a, test1.b
            TableScan: test1, full_filters=[test1.b > UInt32(1)]
          Projection: test2.a, test2.b
            TableScan: test2
        "
        )
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct TestScalarUDF {
        signature: Signature,
    }

    impl ScalarUDFImpl for TestScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "TestScalarUDF"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::from(1)))
        }
    }

    #[test]
    fn test_push_down_volatile_function_in_aggregate() -> Result<()> {
        // SELECT t.a, t.r FROM (SELECT a, sum(b),  TestScalarUDF()+1 AS r FROM test1 GROUP BY a) AS t WHERE t.a > 5 AND t.r > 0.5;
        let table_scan = test_table_scan_with_name("test1")?;
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::exact(vec![], Volatility::Volatile),
        });
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .project(vec![col("a"), sum(col("b")), add(expr, lit(1)).alias("r")])?
            .alias("t")?
            .filter(col("t.a").gt(lit(5)).and(col("t.r").gt(lit(0.5))))?
            .project(vec![col("t.a"), col("t.r")])?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Projection: t.a, t.r
          Filter: t.a > Int32(5) AND t.r > Float64(0.5)
            SubqueryAlias: t
              Projection: test1.a, sum(test1.b), TestScalarUDF() + Int32(1) AS r
                Aggregate: groupBy=[[test1.a]], aggr=[[sum(test1.b)]]
                  TableScan: test1
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: t.a, t.r
          SubqueryAlias: t
            Filter: r > Float64(0.5)
              Projection: test1.a, sum(test1.b), TestScalarUDF() + Int32(1) AS r
                Aggregate: groupBy=[[test1.a]], aggr=[[sum(test1.b)]]
                  TableScan: test1, full_filters=[test1.a > Int32(5)]
        "
        )
    }

    #[test]
    fn test_push_down_volatile_function_in_join() -> Result<()> {
        // SELECT t.a, t.r FROM (SELECT test1.a AS a, TestScalarUDF() AS r FROM test1 join test2 ON test1.a = test2.a) AS t WHERE t.r > 0.5;
        let table_scan = test_table_scan_with_name("test1")?;
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::exact(vec![], Volatility::Volatile),
        });
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan).build()?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (
                    vec![Column::from_qualified_name("test1.a")],
                    vec![Column::from_qualified_name("test2.a")],
                ),
                None,
            )?
            .project(vec![col("test1.a").alias("a"), expr.alias("r")])?
            .alias("t")?
            .filter(col("t.r").gt(lit(0.8)))?
            .project(vec![col("t.a"), col("t.r")])?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Projection: t.a, t.r
          Filter: t.r > Float64(0.8)
            SubqueryAlias: t
              Projection: test1.a AS a, TestScalarUDF() AS r
                Inner Join: test1.a = test2.a
                  TableScan: test1
                  TableScan: test2
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: t.a, t.r
          SubqueryAlias: t
            Filter: r > Float64(0.8)
              Projection: test1.a AS a, TestScalarUDF() AS r
                Inner Join: test1.a = test2.a
                  TableScan: test1
                  TableScan: test2
        "
        )
    }

    #[test]
    fn test_push_down_volatile_table_scan() -> Result<()> {
        // SELECT test.a, test.b FROM test as t WHERE TestScalarUDF() > 0.1;
        let table_scan = test_table_scan()?;
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::exact(vec![], Volatility::Volatile),
        });
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(expr.gt(lit(0.1)))?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Filter: TestScalarUDF() > Float64(0.1)
          Projection: test.a, test.b
            TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.b
          Filter: TestScalarUDF() > Float64(0.1)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_push_down_volatile_mixed_table_scan() -> Result<()> {
        // SELECT test.a, test.b FROM test as t WHERE TestScalarUDF() > 0.1 and test.a > 5 and test.b > 10;
        let table_scan = test_table_scan()?;
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::exact(vec![], Volatility::Volatile),
        });
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(
                expr.gt(lit(0.1))
                    .and(col("t.a").gt(lit(5)))
                    .and(col("t.b").gt(lit(10))),
            )?
            .build()?;

        assert_snapshot!(plan,
        @r"
        Filter: TestScalarUDF() > Float64(0.1) AND t.a > Int32(5) AND t.b > Int32(10)
          Projection: test.a, test.b
            TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.a, test.b
          Filter: TestScalarUDF() > Float64(0.1)
            TableScan: test, full_filters=[t.a > Int32(5), t.b > Int32(10)]
        "
        )
    }

    #[test]
    fn test_push_down_volatile_mixed_unsupported_table_scan() -> Result<()> {
        // SELECT test.a, test.b FROM test as t WHERE TestScalarUDF() > 0.1 and test.a > 5 and test.b > 10;
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::exact(vec![], Volatility::Volatile),
        });
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![]));
        let plan = table_scan_with_pushdown_provider_builder(
            TableProviderFilterPushDown::Unsupported,
            vec![],
            None,
        )?
        .project(vec![col("a"), col("b")])?
        .filter(
            expr.gt(lit(0.1))
                .and(col("t.a").gt(lit(5)))
                .and(col("t.b").gt(lit(10))),
        )?
        .build()?;

        assert_snapshot!(plan,
        @r"
        Filter: TestScalarUDF() > Float64(0.1) AND t.a > Int32(5) AND t.b > Int32(10)
          Projection: a, b
            TableScan: test
        ",
        );
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: a, b
          Filter: t.a > Int32(5) AND t.b > Int32(10) AND TestScalarUDF() > Float64(0.1)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_push_down_filter_to_user_defined_node() -> Result<()> {
        // Define a custom user-defined logical node
        #[derive(Debug, Hash, Eq, PartialEq)]
        struct TestUserNode {
            schema: DFSchemaRef,
        }

        impl PartialOrd for TestUserNode {
            fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
                None
            }
        }

        impl TestUserNode {
            fn new() -> Self {
                let schema = Arc::new(
                    DFSchema::new_with_metadata(
                        vec![(None, Field::new("a", DataType::Int64, false).into())],
                        Default::default(),
                    )
                    .unwrap(),
                );

                Self { schema }
            }
        }

        impl UserDefinedLogicalNodeCore for TestUserNode {
            fn name(&self) -> &str {
                "test_node"
            }

            fn inputs(&self) -> Vec<&LogicalPlan> {
                vec![]
            }

            fn schema(&self) -> &DFSchemaRef {
                &self.schema
            }

            fn expressions(&self) -> Vec<Expr> {
                vec![]
            }

            fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
                write!(f, "TestUserNode")
            }

            fn with_exprs_and_inputs(
                &self,
                exprs: Vec<Expr>,
                inputs: Vec<LogicalPlan>,
            ) -> Result<Self> {
                assert!(exprs.is_empty());
                assert!(inputs.is_empty());
                Ok(Self {
                    schema: Arc::clone(&self.schema),
                })
            }
        }

        // Create a node and build a plan with a filter
        let node = LogicalPlan::Extension(Extension {
            node: Arc::new(TestUserNode::new()),
        });

        let plan = LogicalPlanBuilder::from(node).filter(lit(false))?.build()?;

        // Check the original plan format (not part of the test assertions)
        assert_snapshot!(plan,
        @r"
        Filter: Boolean(false)
          TestUserNode
        ",
        );
        // Check that the filter is pushed down to the user-defined node
        assert_optimized_plan_equal!(
            plan,
            @r"
        Filter: Boolean(false)
          TestUserNode
        "
        )
    }
}
