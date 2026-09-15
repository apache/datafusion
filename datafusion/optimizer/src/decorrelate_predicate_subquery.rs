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

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use crate::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use crate::extract_equijoin_predicate::split_eq_and_noneq_join_predicate;
use crate::optimizer::ApplyOrder;
use crate::utils::replace_qualified_name;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Column, DFSchema, NullEquality, Result, ScalarValue, assert_or_internal_err,
    internal_datafusion_err, internal_err, not_impl_err, plan_err,
};
use datafusion_expr::expr::{Exists, InSubquery, in_subquery_tuple_values};
use datafusion_expr::expr_rewriter::{
    create_col_from_scalar_expr, strip_outer_reference,
};
use datafusion_expr::logical_plan::{JoinType, Subquery};
use datafusion_expr::utils::{conjunction, expr_to_columns, split_conjunction_owned};
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, LogicalPlanBuilder, Operator,
    exists, in_subquery, lit, not, not_exists, not_in_subquery, when,
};

use log::debug;

/// Optimizer rule for rewriting predicate(IN/EXISTS) subquery to left semi/anti joins
#[derive(Default, Debug)]
pub struct DecorrelatePredicateSubquery {}

impl DecorrelatePredicateSubquery {
    #[expect(missing_docs)]
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

        if let LogicalPlan::Projection(projection) = plan {
            if !projection.expr.iter().any(has_subquery) {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }

            let original_projection = projection.clone();
            let mut cur_input = Arc::unwrap_or_clone(projection.input);
            let mut rewritten_exprs = Vec::with_capacity(projection.expr.len());
            for expr in projection.expr {
                let original_name = expr.schema_name().to_string();
                let (new_input, mut rewritten_expr) =
                    rewrite_inner_subqueries(cur_input, expr, config, true, true)?;
                if has_subquery(&rewritten_expr) {
                    return Ok(Transformed::no(LogicalPlan::Projection(
                        original_projection,
                    )));
                }
                cur_input = new_input;

                if rewritten_expr.schema_name().to_string() != original_name {
                    rewritten_expr = rewritten_expr.alias(original_name);
                }
                rewritten_exprs.push(rewritten_expr);
            }

            let new_plan = LogicalPlanBuilder::from(cur_input)
                .project(rewritten_exprs)?
                .build()?;
            return Ok(Transformed::yes(new_plan));
        }

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
        let original_schema = cur_input.schema().columns();
        for subquery_expr in with_subqueries {
            match extract_subquery_info(subquery_expr) {
                // The subquery expression is at the top level of the filter
                SubqueryPredicate::Top(subquery) => {
                    match build_join_top(&subquery, &cur_input, config.alias_generator())?
                    {
                        Some(plan) => cur_input = plan,
                        // The subquery cannot become a semi or anti join. A
                        // `NOT IN` may still become mark joins that
                        // materialize its three-valued result, see
                        // `in_subquery_value_mark_join`. Any other subquery
                        // expression goes back into the filter as it is.
                        None => match subquery.expr() {
                            expr @ Expr::InSubquery(InSubquery {
                                negated: true, ..
                            }) => {
                                let (plan, expr) = rewrite_inner_subqueries(
                                    cur_input, expr, config, true, true,
                                )?;
                                cur_input = plan;
                                other_exprs.push(expr);
                            }
                            expr => other_exprs.push(expr),
                        },
                    }
                }
                // The subquery expression is embedded within another expression
                SubqueryPredicate::Embedded(expr) => {
                    let (plan, expr_without_subqueries) =
                        rewrite_filter_subqueries(cur_input, expr, config)?;
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

        if cur_input.schema().fields().len() != original_schema.len() {
            cur_input = LogicalPlanBuilder::from(cur_input)
                .project(original_schema.into_iter().map(Expr::from))?
                .build()?;
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

/// Rewrites the subqueries of one embedded `Filter` conjunct, deciding for each
/// occurrence whether its mark join has to be null-aware.
///
/// A `Filter` keeps a row only when the predicate is TRUE, and `AND`/`OR` make
/// TRUE only out of TRUE. A NULL mark therefore acts exactly like a FALSE mark
/// under them, so a non-negated `IN` reached through nothing but `AND`/`OR`
/// does not need the more expensive null-aware join. Every other context can
/// tell NULL from FALSE and gets one.
///
/// The decision is taken at the occurrence rather than once for the whole
/// conjunct, so a sibling's shape cannot change it, and any expression this
/// function does not name falls through to the null-aware branch.
fn rewrite_filter_subqueries(
    outer: LogicalPlan,
    expr: Expr,
    config: &dyn OptimizerConfig,
) -> Result<(LogicalPlan, Expr)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::And | Operator::Or),
            right,
        }) => {
            let (outer, left) = rewrite_filter_subqueries(outer, *left, config)?;
            let (outer, right) = rewrite_filter_subqueries(outer, *right, config)?;
            Ok((
                outer,
                Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right))),
            ))
        }
        Expr::InSubquery(in_subquery) => {
            // A subquery in the `IN` value is compared, not used as a filter
            // truth value, so its own mark is observable there.
            let needs_null_aware_mark =
                in_subquery.negated || has_subquery(&in_subquery.expr);
            rewrite_inner_subqueries(
                outer,
                Expr::InSubquery(in_subquery),
                config,
                false,
                needs_null_aware_mark,
            )
        }
        other => rewrite_inner_subqueries(outer, other, config, false, true),
    }
}

/// `needs_null_aware_mark` is `false` only when the caller can prove that a NULL
/// mark and a FALSE mark give the same answer. See [`rewrite_filter_subqueries`].
fn rewrite_inner_subqueries(
    outer: LogicalPlan,
    expr: Expr,
    config: &dyn OptimizerConfig,
    materialize_in_value: bool,
    needs_null_aware_mark: bool,
) -> Result<(LogicalPlan, Expr)> {
    let mut cur_input = outer;
    let alias = config.alias_generator();
    let expr_without_subqueries = expr.transform(|e| match e {
        Expr::Exists(Exists {
            subquery: Subquery { subquery, .. },
            negated,
        }) => match mark_join(
            &cur_input,
            &subquery,
            None,
            negated,
            alias,
            needs_null_aware_mark,
        )? {
            Some(MarkJoin {
                plan,
                mark: exists_expr,
                ..
            }) => {
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
            let rewritten = if materialize_in_value {
                in_subquery_value_mark_join(
                    &cur_input,
                    &subquery,
                    *expr.clone(),
                    negated,
                    alias,
                )?
            } else {
                let in_predicate = subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), |output_expr| {
                        Ok(Expr::eq(*expr.clone(), output_expr))
                    })?;
                mark_join(
                    &cur_input,
                    &subquery,
                    Some(&in_predicate),
                    negated,
                    alias,
                    needs_null_aware_mark,
                )?
                .map(|join| (join.plan, join.mark))
            };
            match rewritten {
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

/// Rewrites an `IN` subquery that gives a value, for example in a SELECT list.
/// The value follows SQL three-valued logic: TRUE for a match, FALSE for a miss
/// and NULL (UNKNOWN) when the answer depends on a NULL.
///
/// There are two paths:
///
/// * One mark join. When the mark column is already exact under three-valued
///   logic (see [`MarkJoin::three_valued_exact`]), the mark column is the
///   answer and this single join is the full rewrite. This is the usual case.
/// * Three mark joins. In the other case the mark column only tells TRUE from
///   not-TRUE, so the UNKNOWN cases must be materialized: one more join tells
///   if the subquery gives a NULL, and one more tells if the subquery gives
///   any row. A `CASE` expression puts the three marks together. The two extra
///   joins have no join predicate, so use them only when the first path
///   cannot apply.
fn in_subquery_value_mark_join(
    left: &LogicalPlan,
    subquery: &LogicalPlan,
    expr: Expr,
    negated: bool,
    alias: &Arc<AliasGenerator>,
) -> Result<Option<(LogicalPlan, Expr)>> {
    let output_expr = subquery
        .head_output_expr()?
        .map_or(plan_err!("single expression required."), Ok)?;
    let in_predicate = Expr::eq(expr.clone(), output_expr.clone());
    let Some(MarkJoin {
        plan: matched_plan,
        mark: matched,
        three_valued_exact,
    }) = mark_join(left, subquery, Some(&in_predicate), false, alias, true)?
    else {
        return Ok(None);
    };

    // The mark column is the full answer when it is exact. Negation does not
    // change that, because NOT UNKNOWN is UNKNOWN.
    if three_valued_exact {
        return Ok(Some((
            matched_plan,
            if negated { not(matched) } else { matched },
        )));
    }

    // The fallback below compares the `IN` value as a single value, which a
    // multi-column `(a, b) IN (SELECT x, y ...)` is not. `build_join` rejects
    // the tuples whose mark cannot be exact, so this is not reached for them.
    if in_subquery_tuple_values(&expr, subquery)?.is_some() {
        return internal_err!(
            "a multi-column IN subquery needs a three-valued exact mark join"
        );
    }
    // SQL IN needs three facts per outer row to distinguish FALSE from UNKNOWN.
    let null_subquery = LogicalPlanBuilder::from(subquery.clone())
        .filter(output_expr.is_null())?
        .build()?;
    let Some(MarkJoin {
        plan: null_plan,
        mark: subquery_has_null,
        ..
    }) = mark_join(&matched_plan, &null_subquery, None, false, alias, true)?
    else {
        return Ok(None);
    };
    let Some(MarkJoin {
        plan: final_plan,
        mark: subquery_non_empty,
        ..
    }) = mark_join(&null_plan, subquery, None, false, alias, true)?
    else {
        return Ok(None);
    };

    let unknown = subquery_has_null.or(expr.is_null().and(subquery_non_empty));
    let result = when(matched, lit(true))
        .when(unknown, lit(ScalarValue::Boolean(None)))
        .otherwise(lit(false))?;
    Ok(Some((
        final_plan,
        if negated { not(result) } else { result },
    )))
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
    Ok(build_join(
        left,
        subquery,
        in_predicate_opt.as_ref(),
        join_type,
        &subquery_alias,
        true,
    )?
    .map(|join| join.plan))
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
    needs_null_aware_mark: bool,
) -> Result<Option<MarkJoin>> {
    let alias = alias_generator.next("__correlated_sq");

    let exists_col = Expr::Column(Column::new(Some(alias.clone()), "mark"));
    let exists_expr = if negated { !exists_col } else { exists_col };

    Ok(build_join(
        left,
        subquery,
        in_predicate_opt,
        JoinType::LeftMark,
        &alias,
        needs_null_aware_mark,
    )?
    .map(|join| MarkJoin {
        plan: join.plan,
        mark: exists_expr,
        three_valued_exact: join.mark_is_three_valued_exact,
    }))
}

/// A [`JoinType::LeftMark`] join that replaces a subquery predicate.
struct MarkJoin {
    /// The outer plan with the subquery joined into it.
    plan: LogicalPlan,
    /// Reads the mark column of the join, negated if the caller asked for it.
    mark: Expr,
    /// True when the mark column already gives SQL three-valued `IN`
    /// semantics: TRUE for a match, FALSE for a miss and NULL for UNKNOWN.
    ///
    /// A plain mark is exact when neither side of the `IN` predicate can be
    /// NULL in scope. Otherwise the join must be null-aware, and the `IN`
    /// equality must be its first key, which is where the null-aware hash join
    /// reads the `IN` value. That join also applies a residual non-equality
    /// filter when it decides whether a NULL makes the mark UNKNOWN.
    ///
    /// In any other case the caller materializes the UNKNOWN rows with more
    /// joins, see [`in_subquery_value_mark_join`].
    three_valued_exact: bool,
}

/// Are the `IN` equalities, in order, the first equalities of `join_filter`
/// that the hash join can use as keys? A null-aware hash join reads its first
/// `V` keys as the `IN` values (one per tuple element of a multi-column
/// `IN`) and the other keys as the correlation.
fn values_are_first_keys(
    in_values: &[InValue],
    join_filter: &Expr,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<bool> {
    let (equijoin_keys, _) = split_eq_and_noneq_join_predicate(
        join_filter.clone(),
        left_schema,
        right_schema,
    )?;
    Ok(equijoin_keys.len() >= in_values.len()
        && equijoin_keys
            .iter()
            .zip(in_values)
            .all(|((value, column), in_value)| {
                value == &in_value.value && column == &in_value.subquery_column
            }))
}

/// The two sides of an `IN` predicate, `value IN (SELECT output_expr ..)`, or
/// of one tuple element of a multi-column `(a, b) IN (SELECT x, y ..)`.
struct InValue {
    /// The value from the outer plan, as the join filter refers to it. This is
    /// a projected column when `value_as_written` is a constant.
    value: Expr,
    /// The subquery output, as the join filter refers to it: a column of the
    /// aliased subquery.
    subquery_column: Expr,
    /// The value as the query writes it.
    value_as_written: Expr,
    /// The subquery output expression as the query writes it.
    output_expr: Expr,
}

impl InValue {
    /// Can either side be NULL for a row inside the scope of an outer row?
    /// See [`key_may_be_null_in_scope`]. The correlated filters name the
    /// expressions as the query writes them, so the test matches on those.
    ///
    /// Only then can `IN` be UNKNOWN, so only then does the join need
    /// null-aware semantics. A correlation key that is NULL just empties the
    /// scope, which makes `IN` FALSE.
    fn may_be_null_in_scope(
        &self,
        left_schema: &DFSchema,
        right_schema: &DFSchema,
        scope_filters: &[Expr],
    ) -> Result<bool> {
        Ok(key_may_be_null_in_scope(
            self.value.nullable(left_schema)?,
            &self.value_as_written,
            true,
            scope_filters,
        ) || key_may_be_null_in_scope(
            self.subquery_column.nullable(right_schema)?,
            &self.output_expr,
            false,
            scope_filters,
        ))
    }
}

/// Can this join key be NULL for a row inside the scope of an outer row?
///
/// `nullable` is what the schema says about the key. The key is a full
/// expression, not only a column. An expression can be NULL although none of
/// its columns is nullable: `NULLIF(id, 1)`, `TRY_CAST(s AS INT)`, a `CASE`
/// with no `ELSE` branch, or a scalar function that does not declare its
/// nullability. So the caller asks the key expression itself, against the
/// schema of its own side.
///
/// The subquery can still keep every NULL out of its result. A correlated
/// conjunct such as `y = x`, `y > x` or `y IS NOT NULL` is never TRUE for a
/// NULL `y`, so no row with a NULL `y` is in the scope of any outer row, and
/// an outer row with a NULL `x` has an empty scope. Neither can make `IN`
/// UNKNOWN, so such a key does not need null-aware semantics. The usual shape
/// is a correlation that repeats the `IN` predicate, `x IN (SELECT y FROM t
/// WHERE y = x)`: the pull up drops that conjunct from the join filter, but
/// it still bounds the subquery result
/// (<https://github.com/apache/datafusion/issues/25480>).
///
/// This is a sufficient test, not an exact one. A conjunct counts only if it
/// is a comparison or an `IS NOT NULL` on the key expression itself, casts
/// aside. Any other conjunct is assumed to let a NULL through, which keeps
/// the join null-aware.
///
/// `key_is_outer` tells on which side of the join the key is. The scope
/// filters keep their outer references, and a side of a conjunct matches an
/// outer key only if it names outer columns alone. A subquery column can have
/// the same qualified name as an outer column (`FROM t AS a` inside a query
/// over `a`), and it must not be read as the outer key.
fn key_may_be_null_in_scope(
    nullable: bool,
    key: &Expr,
    key_is_outer: bool,
    scope_filters: &[Expr],
) -> bool {
    if !nullable {
        return false;
    }
    let key = strip_casts(key);
    !scope_filters
        .iter()
        .any(|filter| filter_rejects_null(filter, key, key_is_outer))
}

/// Is `filter` never TRUE when `key` is NULL? See
/// [`key_may_be_null_in_scope`] for `key_is_outer`.
fn filter_rejects_null(filter: &Expr, key: &Expr, key_is_outer: bool) -> bool {
    let is_key = |side: &Expr| {
        let side = strip_casts(side);
        if key_is_outer {
            side.contains_outer()
                && side.column_refs().is_empty()
                && &strip_outer_reference(side.clone()) == key
        } else {
            !side.contains_outer() && side == key
        }
    };
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            matches!(
                op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
            ) && (is_key(left) || is_key(right))
        }
        Expr::IsNotNull(expr) => is_key(expr),
        _ => false,
    }
}

/// `CAST(NULL)` is NULL and `CAST(x)` is not NULL for a non-null `x`, so a
/// conjunct on `x` says the same about `CAST(x)`, and the other way round.
/// Type coercion adds such casts on one side only. `TRY_CAST` can make a NULL
/// from a value, so it is not unwrapped.
fn strip_casts(expr: &Expr) -> &Expr {
    let mut expr = expr;
    while let Expr::Cast(cast) = expr {
        expr = cast.expr.as_ref();
    }
    expr
}

/// Combines the `IN`/`NOT IN` equalities, one per value, with the correlation
/// predicates, if any.
///
/// The equalities stay the leading conjuncts, in order, so that they become
/// `on[..V]`, the key positions a null-aware hash join reads as the `NOT IN`
/// value keys (see `HashJoinExec::null_aware`).
fn in_join_filter(in_values: &[InValue], correlation: Option<Expr>) -> Result<Expr> {
    conjunction(
        in_values
            .iter()
            .map(|in_value| {
                Expr::eq(in_value.value.clone(), in_value.subquery_column.clone())
            })
            .chain(correlation),
    )
    .ok_or_else(|| internal_datafusion_err!("an `IN` predicate has at least one value"))
}

/// The `i`-th output expression of `subquery`, as the query writes it.
fn subquery_output_expr(subquery: &LogicalPlan, i: usize) -> Result<Expr> {
    match subquery {
        _ if i == 0 => subquery
            .head_output_expr()?
            .ok_or_else(|| internal_datafusion_err!("subquery has no output expression")),
        LogicalPlan::Projection(projection) => Ok(projection.expr[i].clone()),
        _ => Ok(Expr::Column(Column::from(
            subquery.schema().qualified_field(i),
        ))),
    }
}

/// Sets [`Join::null_aware_value_keys`](datafusion_expr::Join::null_aware_value_keys)
/// on a join built by [`build_join`].
fn with_null_aware_value_keys(
    plan: LogicalPlan,
    null_aware_value_keys: usize,
) -> Result<LogicalPlan> {
    let LogicalPlan::Join(join) = plan else {
        return internal_err!("expected a join, got {}", plan.display());
    };
    Ok(LogicalPlan::Join(
        join.with_null_aware_value_keys(null_aware_value_keys),
    ))
}

/// The outcome of [`build_join`].
struct BuiltJoin {
    /// The outer plan with the subquery joined into it.
    plan: LogicalPlan,
    /// See [`MarkJoin::three_valued_exact`]. This is always false unless the
    /// join is a [`JoinType::LeftMark`] join built for an `IN` predicate.
    mark_is_three_valued_exact: bool,
}

fn build_join(
    left: &LogicalPlan,
    subquery: &LogicalPlan,
    in_predicate_opt: Option<&Expr>,
    join_type: JoinType,
    alias: &str,
    needs_null_aware_mark: bool,
) -> Result<Option<BuiltJoin>> {
    let mut pull_up = PullUpCorrelatedExpr::new()
        .with_in_predicate_opt(in_predicate_opt.cloned())
        .with_exists_sub_query(in_predicate_opt.is_none())
        .with_need_handle_count_bug(true);

    let new_plan = subquery.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let count_bug_compensation = pull_up.collected_count_expr_map.get(&new_plan).cloned();

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
            replace_qualified_name(filter, &all_correlated_cols, alias).map(Some)
        })?;

    if let Some(expr_map) = count_bug_compensation
        && !expr_map.is_empty()
    {
        return build_join_with_count_bug(
            left,
            sub_query_alias,
            join_filter_opt,
            in_predicate_opt,
            join_type,
            alias,
            &expr_map,
            pull_up.pull_up_having_expr.as_ref(),
        )
        .map(|plan| {
            Some(BuiltJoin {
                plan,
                mark_is_three_valued_exact: false,
            })
        });
    }

    // The two sides of the `IN` predicate: the value from the outer plan and
    // the subquery output it is compared with, renamed to the alias. A
    // multi-column `(a, b) IN (SELECT x, y ...)` has one pair per tuple
    // element; its `in_predicate` compares the whole tuple with the first
    // subquery output, so each element is paired with its own output here.
    // `EXISTS` has none.
    let in_values = match in_predicate_opt {
        Some(Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        })) => {
            let values = match in_subquery_tuple_values(left, subquery)? {
                Some(values) => values.into_owned(),
                None => vec![left.deref().clone()],
            };
            values
                .into_iter()
                .enumerate()
                .map(|(i, value)| {
                    let output_expr = if i == 0 {
                        right.deref().clone()
                    } else {
                        subquery_output_expr(subquery, i)?
                    };
                    let right_col =
                        create_col_from_scalar_expr(&output_expr, alias.to_string())?;
                    Ok(InValue {
                        value: value.clone(),
                        subquery_column: Expr::Column(right_col),
                        value_as_written: value,
                        output_expr,
                    })
                })
                .collect::<Result<Vec<_>>>()?
        }
        Some(_) => return Ok(None),
        None => vec![],
    };

    // True if an `IN` value or the subquery column it is compared with can be
    // NULL inside the scope of an outer row.
    let mut value_may_be_null = false;
    for in_value in &in_values {
        value_may_be_null |= in_value.may_be_null_in_scope(
            left.schema(),
            sub_query_alias.schema(),
            &pull_up.correlated_filters,
        )?;
    }

    // Whether this join needs null-aware (`NOT IN` three-valued) semantics.
    // Decided once: the constant projection below, the mark branch and the anti
    // branch all depend on the same answer. `NOT EXISTS` is two-valued and has
    // no `IN` value, so it never qualifies.
    //
    // The left projection below only adds a column for a constant value and
    // the right projection only drops unreferenced columns, so neither changes
    // the nullability this looks at.
    let null_aware = value_may_be_null
        && match join_type {
            JoinType::LeftAnti => true,
            JoinType::LeftMark => needs_null_aware_mark,
            _ => false,
        };

    // `<constant> IN/NOT IN (<subquery>)`: the outer value expression holds no
    // column reference, so `<constant> = __correlated_sq.col` is not a valid
    // equi-join key (see `find_valid_equijoin_key_pair`) and stays in the join
    // filter. Two things then go wrong for a null-aware join: the filter is
    // right-only, so `push_down_filter` moves it into the subquery and drops the
    // very NULLs that make `NOT IN` UNKNOWN, and a join without equi-join keys
    // is planned as a nested loop join, which has no null-aware implementation.
    // A correlated subquery hits a third problem: its correlation predicate is a
    // valid equi-join key, so it takes `on[0]`, the position the null-aware hash
    // join reads as the `NOT IN` value key.
    // Projecting the constant as a column of the outer side turns the predicate
    // into a real equi-join key, which fixes all three. It only pays for itself
    // on a join that ends up null-aware.
    //
    // Every constant element of a tuple is projected the same way, so that all
    // value keys lead the equi-join keys in order.
    let mut projected_left = None;
    let mut in_values = in_values;
    if null_aware
        && in_values
            .iter()
            .any(|in_value| in_value.value.column_refs().is_empty())
    {
        let left_schema = left.schema();
        let mut projections = left_schema
            .columns()
            .into_iter()
            .map(Expr::from)
            .collect::<Vec<_>>();
        let is_tuple = in_values.len() > 1;
        for (i, in_value) in in_values.iter_mut().enumerate() {
            if !in_value.value.column_refs().is_empty() {
                continue;
            }
            // The projected column is unqualified, so a left field that already
            // has this name — however unlikely — would make the reference
            // ambiguous.
            let mut value_name = if is_tuple {
                format!("{alias}_value_{i}")
            } else {
                format!("{alias}_value")
            };
            while left_schema.fields().iter().any(|f| f.name() == &value_name) {
                value_name.push('_');
            }
            let value_col = Column::new_unqualified(value_name);
            let constant =
                std::mem::replace(&mut in_value.value, Expr::Column(value_col.clone()));
            projections.push(constant.alias(value_col.name()));
        }
        projected_left = Some(
            LogicalPlanBuilder::from(left.clone())
                .project(projections)?
                .build()?,
        );
    }
    let left = projected_left.as_ref().unwrap_or(left);

    let join_filter = match (in_values.is_empty(), join_filter_opt) {
        (false, correlation_opt) => in_join_filter(&in_values, correlation_opt)?,
        (true, Some(correlation)) => correlation,
        (true, None) => lit(true),
    };

    // Only a null-aware join reads the key order.
    let value_is_first_key = null_aware
        && values_are_first_keys(
            &in_values,
            &join_filter,
            left.schema(),
            sub_query_alias.schema(),
        )?;

    // A multi-column `IN` has no fallback that materializes the UNKNOWN rows
    // (see below), so a tuple element that does not become its key, for
    // example because its type is not hashable, is rejected instead of being
    // read in the wrong key position.
    if null_aware && !value_is_first_key && in_values.len() > 1 {
        return not_impl_err!(
            "Multi-column NOT IN subquery is not supported when a tuple element is not a hashable equi-join key: {join_filter}"
        );
    }
    // The first `V` keys of a null-aware join are its `NOT IN` value keys.
    let null_aware_value_keys = in_values.len().max(1);

    // A `NOT IN` in a filter builds a `LeftAnti` join. The null-aware hash
    // join reads its first key as the `NOT IN` value and the other keys as the
    // scope of the outer row (see `HashJoinExec::null_aware`). If the `IN`
    // equality did not become that first key, give up here. The caller then
    // materializes the UNKNOWN rows with more joins, see
    // `in_subquery_value_mark_join`.
    if join_type == JoinType::LeftAnti && null_aware && !value_is_first_key {
        return Ok(None);
    }

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
        let mut right_col_indices: Vec<usize> = needed
            .into_iter()
            .filter_map(|column| right_schema.index_of_column(&column).ok())
            .collect();

        right_col_indices.sort_unstable();
        right_col_indices.dedup();

        let right_proj_exprs: Vec<Expr> = right_col_indices
            .into_iter()
            .map(|index| Expr::Column(Column::from(right_schema.qualified_field(index))))
            .collect();

        let right_projected = if !right_proj_exprs.is_empty() {
            LogicalPlanBuilder::from(sub_query_alias.clone())
                .project(right_proj_exprs)?
                .build()?
        } else {
            // Degenerate case: no right columns referenced by the predicate(s)
            sub_query_alias.clone()
        };

        // For scalar NOT IN mark joins, propagate null-aware semantics into the
        // nullable mark column. A non-equality correlation stays behind as a
        // join filter, which the hash join also applies when it decides
        // whether a NULL makes the mark UNKNOWN.
        let mark_is_three_valued_exact = !in_values.is_empty()
            && (!value_may_be_null || (null_aware && value_is_first_key));

        let new_plan = with_null_aware_value_keys(
            LogicalPlanBuilder::from(left.clone())
                .join_detailed_with_options(
                    right_projected,
                    join_type,
                    (Vec::<Column>::new(), Vec::<Column>::new()),
                    Some(join_filter),
                    NullEquality::NullEqualsNothing,
                    null_aware,
                )?
                .build()?,
            null_aware_value_keys,
        )?;

        debug!(
            "predicate subquery optimized:\n{}",
            new_plan.display_indent()
        );

        return Ok(Some(BuiltJoin {
            plan: new_plan,
            mark_is_three_valued_exact,
        }));
    }

    // join our sub query into the main plan
    let new_plan = if null_aware {
        // Use join_detailed_with_options to set null_aware flag
        with_null_aware_value_keys(
            LogicalPlanBuilder::from(left.clone())
                .join_detailed_with_options(
                    sub_query_alias,
                    join_type,
                    (Vec::<Column>::new(), Vec::<Column>::new()), // No equijoin keys, filter-based join
                    Some(join_filter),
                    NullEquality::NullEqualsNothing,
                    true, // null_aware
                )?
                .build()?,
            null_aware_value_keys,
        )?
    } else {
        LogicalPlanBuilder::from(left.clone())
            .join_on(sub_query_alias, join_type, Some(join_filter))?
            .build()?
    };
    debug!(
        "predicate subquery optimized:\n{}",
        new_plan.display_indent()
    );
    Ok(Some(BuiltJoin {
        plan: new_plan,
        mark_is_three_valued_exact: false,
    }))
}

/// Builds the join for a correlated `EXISTS` subquery whose groupless
/// aggregate requires count-bug compensation.
#[expect(clippy::too_many_arguments)]
fn build_join_with_count_bug(
    left: &LogicalPlan,
    sub_query_alias: LogicalPlan,
    join_filter_opt: Option<Expr>,
    in_predicate_opt: Option<&Expr>,
    join_type: JoinType,
    alias: &str,
    expr_map: &crate::decorrelate::ExprResultMap,
    pull_up_having_expr: Option<&Expr>,
) -> Result<LogicalPlan> {
    if in_predicate_opt.is_some() {
        return not_impl_err!(
            "build_join_with_count_bug: `IN`/`NOT IN` count-bug compensation is not implemented"
        );
    }

    let joined = LogicalPlanBuilder::from(left.clone())
        .join_on(sub_query_alias, JoinType::Left, join_filter_opt)?
        .build()?;

    let left_projection: Vec<Expr> = left
        .schema()
        .columns()
        .into_iter()
        .map(Expr::from)
        .collect();

    let indicator_col = Expr::Column(Column::new(Some(alias), UN_MATCHED_ROW_INDICATOR));

    let having_arm = pull_up_having_expr.map(|f| f.clone().is_not_true());

    let mut expr_rewrite = TypeCoercionRewriter::new(joined.schema());

    // The subquery's HAVING clause, evaluated against an unmatched row's
    // default aggregate values (e.g. count(*) defaults to 0, sum(x) to
    // NULL), is usually `true`, but not once a second filter is combined
    // into it, so it cannot be assumed.
    let unmatched_having_default = match pull_up_having_expr {
        Some(f) => f
            .clone()
            .transform_up(|e| {
                if let Expr::Column(Column { name, .. }) = &e
                    && let Some(default_value) = expr_map.get(name)
                {
                    return Ok(Transformed::yes(default_value.clone()));
                }
                Ok(Transformed::no(e))
            })
            .data()
            .and_then(|simplified| simplified.rewrite(&mut expr_rewrite).data())?,
        None => lit(true),
    };

    // EXISTS is true by default (the groupless aggregate always
    // produces a row), unless either the row joined against a group
    // whose HAVING predicate failed, or an unmatched row's own
    // default aggregate values fail that same HAVING clause.
    let exists_expr = match having_arm {
        Some(when_expr) => {
            when(indicator_col.clone().is_null(), unmatched_having_default)
                .when(when_expr, lit(false))
                .otherwise(lit(true))?
                .rewrite(&mut expr_rewrite)
                .data()?
        }
        None => lit(true),
    };

    let new_plan = match join_type {
        JoinType::LeftMark | JoinType::RightMark => {
            let mut proj_exprs = left_projection;
            proj_exprs.push(exists_expr.alias_qualified(Some(alias), "mark"));
            LogicalPlanBuilder::from(joined)
                .project(proj_exprs)?
                .build()?
        }
        JoinType::LeftAnti => LogicalPlanBuilder::from(joined)
            .filter(not(exists_expr))?
            .project(left_projection)?
            .build()?,
        JoinType::LeftSemi => LogicalPlanBuilder::from(joined)
            .filter(exists_expr)?
            .project(left_projection)?
            .build()?,
        _ => {
            return internal_err!(
                "build_join_with_count_bug: unsupported join type {join_type:?}"
            );
        }
    };

    debug!(
        "predicate subquery (count bug) optimized:\n{}",
        new_plan.display_indent()
    );

    Ok(new_plan)
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
    use datafusion_expr::{
        TableScanBuilder, and, binary_expr, col, cube, grouping_set, out_ref_col, rollup,
        table_scan,
    };
    use datafusion_functions_aggregate::count::count_udaf;

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

    fn nullable_scalar_mark_scan(name: &str) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("grp", DataType::Int32, true),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    /// `CASE WHEN test.c = 1 THEN NULL ELSE test.c END`: an expression that can
    /// be NULL although `test.c` is not nullable. `NULLIF(c, 1)` and
    /// `TRY_CAST(c AS INT)` have the same shape, but the optimizer crate cannot
    /// depend on the function crates.
    fn nullable_key_expr() -> Result<Expr> {
        when(col("test.c").eq(lit(1u32)), lit(ScalarValue::UInt32(None)))
            .otherwise(col("test.c"))
    }

    fn has_null_aware_left_mark_join(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Join(join) = plan
            && join.join_type == JoinType::LeftMark
        {
            return join.null_aware;
        }

        plan.inputs().into_iter().any(has_null_aware_left_mark_join)
    }

    fn has_non_null_aware_left_mark_join(plan: &LogicalPlan) -> bool {
        if let LogicalPlan::Join(join) = plan
            && join.join_type == JoinType::LeftMark
        {
            return !join.null_aware;
        }

        plan.inputs()
            .into_iter()
            .any(has_non_null_aware_left_mark_join)
    }

    fn optimize_with_decorrelate(plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = crate::Optimizer::with_rules(vec![Arc::new(
            DecorrelatePredicateSubquery::new(),
        )]);
        optimizer.optimize(plan, &crate::OptimizerContext::new(), |_, _| {})
    }

    /// A grouping set subquery for the tests below: `SELECT c FROM <name> WHERE
    /// c = test.c GROUP BY <group_expr>`.
    fn correlated_grouping_set_subquery(
        name: &str,
        group_expr: Expr,
    ) -> Result<Arc<LogicalPlan>> {
        Ok(Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name(name)?)
                .filter(
                    col(format!("{name}.c")).eq(out_ref_col(DataType::UInt32, "test.c")),
                )?
                .aggregate(vec![group_expr], Vec::<Expr>::new())?
                .project(vec![col(format!("{name}.c"))])?
                .build()?,
        ))
    }

    /// `ROLLUP(c)` is `GROUPING SETS ((c), ())`. Adding the correlated column to
    /// every set drops the empty one, so the subquery is left correlated.
    /// <https://github.com/apache/datafusion/issues/25519>
    #[test]
    fn exists_subquery_with_rollup_is_not_decorrelated() -> Result<()> {
        let subquery = correlated_grouping_set_subquery("sq", rollup(vec![col("sq.c")]))?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(exists(subquery))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: EXISTS (<subquery>) [a:UInt32, b:UInt32, c:UInt32]
            Subquery: [c:UInt32;N]
              Projection: sq.c [c:UInt32;N]
                Aggregate: groupBy=[[ROLLUP (sq.c)]], aggr=[[]] [c:UInt32;N, __grouping_id:UInt8]
                  Filter: sq.c = outer_ref(test.c) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// `CUBE(c)` holds the empty set for the same reason. The correlation is on
    /// `a` rather than on the `IN` key, so it stays a filter of its own instead
    /// of being folded into the `IN` predicate.
    /// <https://github.com/apache/datafusion/issues/25519>
    #[test]
    fn in_subquery_with_cube_is_not_decorrelated() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(col("sq.a").eq(out_ref_col(DataType::UInt32, "test.a")))?
                .aggregate(vec![cube(vec![col("sq.c")])], Vec::<Expr>::new())?
                .project(vec![col("sq.c")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(in_subquery(col("test.c"), subquery))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: test.c IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]
            Subquery: [c:UInt32;N]
              Projection: sq.c [c:UInt32;N]
                Aggregate: groupBy=[[CUBE (sq.c)]], aggr=[[]] [c:UInt32;N, __grouping_id:UInt8]
                  Filter: sq.a = outer_ref(test.a) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// A set that groups by another column does not carry the correlated one.
    /// <https://github.com/apache/datafusion/issues/25519>
    #[test]
    fn exists_subquery_with_partial_grouping_set_is_not_decorrelated() -> Result<()> {
        let subquery = correlated_grouping_set_subquery(
            "sq",
            grouping_set(vec![vec![col("sq.c")], vec![col("sq.b")]]),
        )?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(exists(subquery))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Filter: EXISTS (<subquery>) [a:UInt32, b:UInt32, c:UInt32]
            Subquery: [c:UInt32;N]
              Projection: sq.c [c:UInt32;N]
                Aggregate: groupBy=[[GROUPING SETS ((sq.c), (sq.b))]], aggr=[[]] [c:UInt32;N, b:UInt32;N, __grouping_id:UInt8]
                  Filter: sq.c = outer_ref(test.c) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Every set already groups by the correlated column, so the pull up adds
    /// nothing and the subquery decorrelates as it did before.
    /// <https://github.com/apache/datafusion/issues/25519>
    #[test]
    fn exists_subquery_with_covering_grouping_set_is_decorrelated() -> Result<()> {
        let subquery = correlated_grouping_set_subquery(
            "sq",
            grouping_set(vec![vec![col("sq.c")], vec![col("sq.c"), col("sq.b")]]),
        )?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(exists(subquery))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftSemi Join:  Filter: __correlated_sq_1.c = test.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32;N]
              Projection: sq.c [c:UInt32;N]
                Aggregate: groupBy=[[GROUPING SETS ((sq.c), (sq.c, sq.b))]], aggr=[[]] [c:UInt32;N, b:UInt32;N, __grouping_id:UInt8]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
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
                @r"
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
        "
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
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey AND (customer.c_custkey = __correlated_sq_1.o_custkey OR __correlated_sq_1.__correlated_group_expr_o_orderkey) [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64, __correlated_group_expr_o_orderkey:Boolean]
              Projection: orders.o_custkey, orders.o_orderkey = Int32(1) AS __correlated_group_expr_o_orderkey [o_custkey:Int64, __correlated_group_expr_o_orderkey:Boolean]
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

    #[test]
    fn in_subquery_in_projection() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![
                in_subquery(col("c"), test_subquery_with_name("sq")?).alias("is_present"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: __correlated_sq_1.mark AS is_present [is_present:Boolean;N]
          LeftMark Join:  Filter: test.c = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32, mark:Boolean;N]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            Projection: __correlated_sq_1.c [c:UInt32]
              SubqueryAlias: __correlated_sq_1 [c:UInt32]
                Projection: sq.c [c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// A residual non-equality correlation stays on the one mark join. The
    /// null-aware hash join applies it when it decides whether a NULL makes
    /// the mark UNKNOWN, so the mark is still exact.
    #[test]
    fn in_subquery_in_projection_with_residual_filter() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(nullable_scalar_mark_scan("inner_t")?)
                .filter(
                    out_ref_col(DataType::Int32, "outer_t.grp").gt(col("inner_t.grp")),
                )?
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(nullable_scalar_mark_scan("outer_t")?)
            .project(vec![
                in_subquery(col("outer_t.id"), subquery).alias("is_present"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: __correlated_sq_1.mark AS is_present [is_present:Boolean;N]
          LeftMark Join:  Filter: outer_t.id = __correlated_sq_1.id AND outer_t.grp > __correlated_sq_1.grp null_aware [id:Int32;N, grp:Int32;N, mark:Boolean;N]
            TableScan: outer_t [id:Int32;N, grp:Int32;N]
            Projection: __correlated_sq_1.id, __correlated_sq_1.grp [id:Int32;N, grp:Int32;N]
              SubqueryAlias: __correlated_sq_1 [id:Int32;N, grp:Int32;N]
                Projection: inner_t.id, inner_t.grp [id:Int32;N, grp:Int32;N]
                  TableScan: inner_t [id:Int32;N, grp:Int32;N]
        "
        )
    }

    /// `NOT IN` reads the same mark column, negated. The keys are nullable here,
    /// so the join is null-aware and the mark is NULL for the UNKNOWN rows.
    #[test]
    fn not_in_subquery_in_projection() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(nullable_scalar_mark_scan("inner_t")?)
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(nullable_scalar_mark_scan("outer_t")?)
            .project(vec![
                not_in_subquery(col("outer_t.id"), subquery).alias("is_absent"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: NOT __correlated_sq_1.mark AS is_absent [is_absent:Boolean;N]
          LeftMark Join:  Filter: outer_t.id = __correlated_sq_1.id null_aware [id:Int32;N, grp:Int32;N, mark:Boolean;N]
            TableScan: outer_t [id:Int32;N, grp:Int32;N]
            Projection: __correlated_sq_1.id [id:Int32;N]
              SubqueryAlias: __correlated_sq_1 [id:Int32;N]
                Projection: inner_t.id [id:Int32;N]
                  TableScan: inner_t [id:Int32;N, grp:Int32;N]
        "
        )
    }

    /// A key expression can be NULL although none of its columns is nullable.
    /// The mark join must then be null-aware, so the mark is NULL for the rows
    /// that give UNKNOWN.
    #[test]
    fn in_subquery_in_projection_with_nullable_key_expr() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![
                in_subquery(nullable_key_expr()?, test_subquery_with_name("sq")?)
                    .alias("is_present"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: __correlated_sq_1.mark AS is_present [is_present:Boolean;N]
          LeftMark Join:  Filter: CASE WHEN test.c = UInt32(1) THEN UInt32(NULL) ELSE test.c END = __correlated_sq_1.c null_aware [a:UInt32, b:UInt32, c:UInt32, mark:Boolean;N]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            Projection: __correlated_sq_1.c [c:UInt32]
              SubqueryAlias: __correlated_sq_1 [c:UInt32]
                Projection: sq.c [c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// The `NOT IN` filter path builds a `LeftAnti` join. It reads the key
    /// nullability the same way, so a nullable key expression over columns that
    /// are not nullable also makes that join null-aware.
    #[test]
    fn not_in_subquery_filter_with_nullable_key_expr() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(not_in_subquery(
                nullable_key_expr()?,
                test_subquery_with_name("sq")?,
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftAnti Join:  Filter: CASE WHEN test.c = UInt32(1) THEN UInt32(NULL) ELSE test.c END = __correlated_sq_1.c null_aware [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn unsupported_correlated_in_projection_is_left_unchanged() -> Result<()> {
        let subquery = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .limit(0, Some(1))?
                .project(vec![col("orders.o_custkey")])?
                .build()?,
        );
        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .project(vec![
                in_subquery(col("customer.c_custkey"), subquery).alias("is_present"),
            ])?
            .build()?;

        let result = DecorrelatePredicateSubquery::new()
            .rewrite(plan.clone(), &crate::OptimizerContext::new())?;

        assert!(!result.transformed);
        assert_eq!(result.data, plan);
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

    /// A constant value expression has no column, so `Int32(3) = inner_t.id`
    /// cannot be an equi-join key on its own. The rule projects the constant as
    /// a column of the outer side; `ExtractEquijoinPredicate` (not run here)
    /// then turns the filter into a real key for the null-aware hash join.
    #[test]
    fn constant_not_in_subquery_projects_value_as_join_key() -> Result<()> {
        let outer_scan = nullable_scalar_mark_scan("outer_t")?;
        let inner_scan = nullable_scalar_mark_scan("inner_t")?;

        let subquery = Arc::new(
            LogicalPlanBuilder::from(inner_scan)
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(not_in_subquery(lit(3i32), subquery))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: outer_t.id, outer_t.grp [id:Int32;N, grp:Int32;N]
          LeftAnti Join:  Filter: __correlated_sq_1_value = __correlated_sq_1.id null_aware [id:Int32;N, grp:Int32;N, __correlated_sq_1_value:Int32]
            Projection: outer_t.id, outer_t.grp, Int32(3) AS __correlated_sq_1_value [id:Int32;N, grp:Int32;N, __correlated_sq_1_value:Int32]
              TableScan: outer_t [id:Int32;N, grp:Int32;N]
            SubqueryAlias: __correlated_sq_1 [id:Int32;N]
              Projection: inner_t.id [id:Int32;N]
                TableScan: inner_t [id:Int32;N, grp:Int32;N]
        "
        )
    }

    /// A NULL constant has no column, but it is still nullable. The join must
    /// be null-aware although the subquery column `sq.c` is not nullable
    /// (<https://github.com/apache/datafusion/issues/25473>).
    #[test]
    fn null_constant_not_in_non_nullable_subquery_is_null_aware() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(not_in_subquery(
                lit(ScalarValue::UInt32(None)),
                test_subquery_with_name("sq")?,
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]
            LeftAnti Join:  Filter: __correlated_sq_1_value = __correlated_sq_1.c null_aware [a:UInt32, b:UInt32, c:UInt32, __correlated_sq_1_value:UInt32;N]
              Projection: test.a, test.b, test.c, UInt32(NULL) AS __correlated_sq_1_value [a:UInt32, b:UInt32, c:UInt32, __correlated_sq_1_value:UInt32;N]
                TableScan: test [a:UInt32, b:UInt32, c:UInt32]
              SubqueryAlias: __correlated_sq_1 [c:UInt32]
                Projection: sq.c [c:UInt32]
                  TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// `test.c + 1` over a non-nullable `test.c` cannot be NULL, and neither can
    /// `sq.c`, so the anti join stays plain
    /// (<https://github.com/apache/datafusion/issues/25474>).
    #[test]
    fn non_nullable_key_expr_not_in_is_not_null_aware() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(not_in_subquery(
                col("test.c") + lit(1u32),
                test_subquery_with_name("sq")?,
            ))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          LeftAnti Join:  Filter: test.c + UInt32(1) = __correlated_sq_1.c [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32]
              Projection: sq.c [c:UInt32]
                TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    #[test]
    fn correlated_not_in_mark_join_is_null_aware_for_hashable_filter() -> Result<()> {
        let outer_scan = nullable_scalar_mark_scan("outer_t")?;
        let inner_scan = nullable_scalar_mark_scan("inner_t")?;

        let subquery = Arc::new(
            LogicalPlanBuilder::from(inner_scan)
                .filter(
                    out_ref_col(DataType::Int32, "outer_t.grp").eq(col("inner_t.grp")),
                )?
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(not_in_subquery(col("outer_t.id"), subquery).is_null())?
            .build()?;

        let optimized = optimize_with_decorrelate(plan)?;
        assert!(
            has_null_aware_left_mark_join(&optimized),
            "{}",
            optimized.display_indent_schema()
        );

        Ok(())
    }

    /// A correlation that repeats the `IN` predicate keeps every NULL out of
    /// the subquery result, so the mark join must not be null-aware.
    #[test]
    fn mark_join_for_in_predicate_correlation_is_not_null_aware() -> Result<()> {
        let outer_scan = nullable_scalar_mark_scan("outer_t")?;
        let inner_scan = nullable_scalar_mark_scan("inner_t")?;

        let subquery = Arc::new(
            LogicalPlanBuilder::from(inner_scan)
                .filter(out_ref_col(DataType::Int32, "outer_t.id").eq(col("inner_t.id")))?
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(in_subquery(col("outer_t.id"), subquery).is_null())?
            .build()?;

        let optimized = optimize_with_decorrelate(plan)?;
        assert!(
            has_non_null_aware_left_mark_join(&optimized),
            "{}",
            optimized.display_indent_schema()
        );

        Ok(())
    }

    /// A grouping set above the correlation puts a NULL back into the key: the
    /// grand-total row of `ROLLUP` is a NULL for every outer row. The
    /// correlation then no longer bounds the subquery result, and the mark
    /// join stays null-aware.
    #[test]
    fn mark_join_for_in_predicate_correlation_below_rollup_is_null_aware() -> Result<()> {
        let subquery = correlated_grouping_set_subquery("sq", rollup(vec![col("sq.c")]))?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![in_subquery(col("test.c"), subquery).alias("m")])?
            .build()?;

        let optimized = optimize_with_decorrelate(plan)?;
        assert!(
            has_null_aware_left_mark_join(&optimized),
            "{}",
            optimized.display_indent_schema()
        );

        Ok(())
    }

    /// The same for the `LeftAnti` join that a `NOT IN` filter builds.
    #[test]
    fn anti_join_for_in_predicate_correlation_is_not_null_aware() -> Result<()> {
        let outer_scan = nullable_scalar_mark_scan("outer_t")?;
        let inner_scan = nullable_scalar_mark_scan("inner_t")?;

        let subquery = Arc::new(
            LogicalPlanBuilder::from(inner_scan)
                .filter(out_ref_col(DataType::Int32, "outer_t.id").eq(col("inner_t.id")))?
                .project(vec![col("inner_t.id")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_scan)
            .filter(not_in_subquery(col("outer_t.id"), subquery))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        LeftAnti Join:  Filter: outer_t.id = __correlated_sq_1.id [id:Int32;N, grp:Int32;N]
          TableScan: outer_t [id:Int32;N, grp:Int32;N]
          SubqueryAlias: __correlated_sq_1 [id:Int32;N]
            Projection: inner_t.id [id:Int32;N]
              TableScan: inner_t [id:Int32;N, grp:Int32;N]
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
          LeftSemi Join:  Filter: customer.c_custkey = __correlated_sq_1.o_custkey OR __correlated_sq_1.__correlated_group_expr_o_orderkey [c_custkey:Int64, c_name:Utf8]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __correlated_sq_1 [o_custkey:Int64, __correlated_group_expr_o_orderkey:Boolean]
              Projection: orders.o_custkey, orders.o_orderkey = Int32(1) AS __correlated_group_expr_o_orderkey [o_custkey:Int64, __correlated_group_expr_o_orderkey:Boolean]
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
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: __correlated_sq_1.mark OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, mark:Boolean;N]
              LeftMark Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, mark:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __correlated_sq_1 [o_custkey:Int64]
                  Projection: orders.o_custkey [o_custkey:Int64]
                    Filter: customer.c_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated exists subquery filter with disjunction and count bug
    #[test]
    fn exists_subquery_disjunction_with_count_bug() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .aggregate(Vec::<Expr>::new(), vec![count_udaf().call(vec![])])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(exists(sq).or(col("test.c").eq(lit(1))))?
            .project(vec![col("test.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.b [b:UInt32]
          Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]
            Filter: __correlated_sq_1.mark OR test.c = Int32(1) [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
              Projection: test.a, test.b, test.c, Boolean(true) AS mark [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                Left Join:  Filter: test.a = __correlated_sq_1.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32;N, __always_true:Boolean;N, count():Int64;N]
                  TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                  SubqueryAlias: __correlated_sq_1 [a:UInt32, __always_true:Boolean, count():Int64]
                    Aggregate: groupBy=[[sq.a, Boolean(true) AS __always_true]], aggr=[[count()]] [a:UInt32, __always_true:Boolean, count():Int64]
                      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
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
          LeftSemi Join:  Filter: __correlated_sq_1.__correlated_group_expr_a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [UInt32(1):UInt32, __correlated_group_expr_a:UInt32]
              Projection: UInt32(1), UInt32(1) + sq.a AS __correlated_group_expr_a [UInt32(1):UInt32, __correlated_group_expr_a:UInt32]
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
          LeftSemi Join:  Filter: __correlated_sq_1.__correlated_group_expr_a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [c:UInt32, __correlated_group_expr_a:UInt32]
              Distinct: [c:UInt32, __correlated_group_expr_a:UInt32]
                Projection: sq.c, UInt32(1) + sq.a AS __correlated_group_expr_a [c:UInt32, __correlated_group_expr_a:UInt32]
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
          LeftSemi Join:  Filter: __correlated_sq_1.__correlated_group_expr_a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [sq.b + sq.c:UInt32, __correlated_group_expr_a:UInt32]
              Distinct: [sq.b + sq.c:UInt32, __correlated_group_expr_a:UInt32]
                Projection: sq.b + sq.c, UInt32(1) + sq.a AS __correlated_group_expr_a [sq.b + sq.c:UInt32, __correlated_group_expr_a:UInt32]
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
          LeftSemi Join:  Filter: __correlated_sq_1.__correlated_group_expr_a > test.a * UInt32(2) [a:UInt32, b:UInt32, c:UInt32]
            TableScan: test [a:UInt32, b:UInt32, c:UInt32]
            SubqueryAlias: __correlated_sq_1 [UInt32(1):UInt32, c:UInt32, __correlated_group_expr_a:UInt32]
              Distinct: [UInt32(1):UInt32, c:UInt32, __correlated_group_expr_a:UInt32]
                Projection: UInt32(1), sq.c, UInt32(1) + sq.a AS __correlated_group_expr_a [UInt32(1):UInt32, c:UInt32, __correlated_group_expr_a:UInt32]
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
        let subquery_table_scan =
            TableScanBuilder::new("sq", subquery_table_source).build()?;
        let subquery = LogicalPlanBuilder::table_scan(subquery_table_scan)?
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
                TableScan: sq [arr:List(Int32);N]
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
        let subquery_table_scan =
            TableScanBuilder::new("sq", subquery_table_source).build()?;
        let subquery = LogicalPlanBuilder::table_scan(subquery_table_scan)?
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
                TableScan: sq [a:List(UInt32);N]
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

    /// An Unnest replaces the column it unnests with the values of that
    /// column. A correlated filter on that column below the Unnest compares
    /// the whole list, so it cannot move above the Unnest.
    #[test]
    fn exists_subquery_filter_on_unnested_column() -> Result<()> {
        let list_type =
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("arr", list_type.clone(), true),
        ]);
        let subquery = Arc::new(
            table_scan(Some("sq"), &schema, None)?
                .filter(col("sq.arr").eq(out_ref_col(list_type, "outer_t.arr")))?
                .unnest_column("arr")?
                .project(vec![col("sq.id")])?
                .build()?,
        );
        let plan = table_scan(Some("outer_t"), &schema, None)?
            .filter(exists(subquery))?
            .project(vec![col("outer_t.id")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: outer_t.id [id:Int32;N]
          Filter: EXISTS (<subquery>) [id:Int32;N, arr:List(Int32);N]
            Subquery: [id:Int32;N]
              Projection: sq.id [id:Int32;N]
                Unnest: lists[sq.arr|depth=1] structs[] [id:Int32;N, arr:Int32;N]
                  Filter: sq.arr = outer_ref(outer_t.arr) [id:Int32;N, arr:List(Int32);N]
                    TableScan: sq [id:Int32;N, arr:List(Int32);N]
            TableScan: outer_t [id:Int32;N, arr:List(Int32);N]
        "
        )
    }
}
