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

//! [`EliminateJoin`] rewrites joins to simpler forms to make them cheaper
//! to evaluate. We implement three distinct rewrites:
//!
//! * An inner join can be rewritten to an empty relation if the join condition
//!   is trivially false.
//!
//! * An inner join `L ⋈ R` can be rewritten to a left semi join `L ⋉ R`
//!   (`LeftSemi`), which keeps the rows of L that have a match in R and outputs
//!   only L's columns. The rewrite to `L ⋉ R` is valid when both of the
//!   following are true:
//!
//!     1. None of R's columns are referenced above the join.
//!     2. R does not observably multiply L's rows. This holds when either the
//!        join's ancestors are duplicate-insensitive (e.g., DISTINCT) and its
//!        conditions are repeatable, or we can use functional dependencies to
//!        prove that each L row matches at most one R row (R is provably unique
//!        on the join keys).
//!
//! * A left outer join `L ⟕ R` can be removed entirely, i.e. replaced by `L`,
//!   under the same two conditions. Unlike an inner join, a left join
//!   preserves every row of L whether or not it has a match in R, so when R's
//!   columns are unused and R cannot multiply L's rows the join has no
//!   observable effect at all. Such joins commonly appear in generated SQL
//!   and in queries over views that join in lookup tables the query does not
//!   read. A repeatable join filter does not prevent this rewrite: for a left
//!   join it only decides whether a left row is matched or null-padded, and
//!   either way the row is emitted. Symmetrically, a right outer join `L ⟖ R`
//!   can be replaced by `R` when L's columns are unused and L cannot multiply
//!   R's rows.
//!
//! # Overview
//!
//! `rewrite_subtree` walks the plan top-down, threading two pieces of context
//! down to each join:
//!
//! * `live` — which of the join's output columns are referenced above it. It is
//!   propagated top-down: each node asks its children only for the columns it
//!   needs from them, so a projection or aggregate asks for just the columns its
//!   expressions reference, dropping the rest (the narrowing); a join splits the
//!   set across its two inputs.
//! * `duplicate_insensitive` — whether emitting each row once instead of many
//!   times will not change the output. A duplicate-collapsing node (e.g.,
//!   DISTINCT, an `Aggregate` plan node whose aggregate expressions all ignore
//!   duplicate input rows, or the existence side of a semi/anti/mark join) sets
//!   it `true` for its subtree, and it propagates downward until a node that
//!   makes the row count observable again (a `LIMIT`, a top-N sort, a volatile
//!   expression, ...) clears it. It is therefore fixed by the nearest such node,
//!   not by the whole ancestor chain: a collapsing node shields its subtree,
//!   so a duplicate-sensitive node further above does not matter.
//!
//! At each join, `rewritten_join_type` combines this context with the side's
//! functional dependencies to choose `Inner`, `LeftSemi`, or `RightSemi`, or
//! to eliminate the join entirely in favor of its preserved input. Most
//! node types just forward the context to their single child via
//! `rewrite_single_input`; nodes that alter column requirements or
//! duplicate-sensitivity (projection, aggregate, sort, ...) adjust it first.
use crate::utils::{
    for_each_referenced_index, is_duplicate_insensitive_aggregate, is_repeatable,
};
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    DFSchema, Dependency, HashSet, NullEquality, Result, ScalarValue,
};
use datafusion_expr::{
    Expr, JoinType,
    logical_plan::{
        Aggregate, Distinct, DistinctOn, EmptyRelation, Filter, Join, Limit, LogicalPlan,
        Partitioning, Projection, Repartition, Sort, SubqueryAlias,
    },
};
use std::sync::Arc;

/// The columns that are "live" at a plan node, i.e., which of its output
/// columns are referenced by an ancestor node. Represented as a set of column
/// indices, relative to the node's schema.
///
/// See the module-level docs for how this set is threaded down the plan and
/// narrowed or split at each node.
#[derive(Debug, Default, Clone)]
struct LiveColumns(HashSet<usize>);

impl LiveColumns {
    fn new() -> Self {
        Self(HashSet::new())
    }

    /// Every column of `schema` is live.
    fn all(schema: &DFSchema) -> Self {
        Self((0..schema.fields().len()).collect())
    }

    /// The columns of `schema` referenced by any of `exprs`.
    fn try_new<'a>(
        exprs: impl IntoIterator<Item = &'a Expr>,
        schema: &DFSchema,
    ) -> Result<Self> {
        let mut live = Self::new();
        live.extend_from(exprs, schema)?;
        Ok(live)
    }

    /// Inserts the index, within `schema`, of every column referenced by any of
    /// `exprs`, including columns reached through correlated subquery outer
    /// references.
    fn extend_from<'a>(
        &mut self,
        exprs: impl IntoIterator<Item = &'a Expr>,
        schema: &DFSchema,
    ) -> Result<()> {
        for expr in exprs {
            for_each_referenced_index(expr, schema, |idx| {
                self.0.insert(idx);
            })?;
        }
        Ok(())
    }

    fn insert(&mut self, idx: usize) {
        self.0.insert(idx);
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Splits live columns spanning a join's combined output (the left input's
    /// columns first, then the right input's) into the per-side sets, rebasing
    /// the right side's indices to start at zero. `left_len` is the number of
    /// columns contributed by the left input.
    fn split_at(&self, left_len: usize) -> (Self, Self) {
        let mut left = Self::new();
        let mut right = Self::new();
        for &idx in &self.0 {
            if idx < left_len {
                left.insert(idx);
            } else {
                right.insert(idx - left_len);
            }
        }
        (left, right)
    }
}

/// Rewrites an inner join to a semi join when one input only filters the
/// other, removes an outer join whose non-preserved side is unused and cannot
/// multiply the preserved side's rows, and replaces an always-false inner join
/// with an empty relation.
#[derive(Default, Debug)]
pub struct EliminateJoin;

impl EliminateJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateJoin {
    fn name(&self) -> &str {
        "eliminate_join"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let live = LiveColumns::all(plan.schema());
        rewrite_subtree(plan, live, false)
    }
}

/// Rewrites `plan` and everything below it, including joins nested inside
/// subquery expressions.
///
/// [`rewrite_node`] handles the node itself and recurses into its plan
/// children; this wrapper additionally descends into the node's own subquery
/// expressions.  Each subquery is seeded as a fresh root, since its columns are
/// independent of the enclosing plan's `live` set.
fn rewrite_subtree(
    plan: LogicalPlan,
    live: LiveColumns,
    duplicate_insensitive: bool,
) -> Result<Transformed<LogicalPlan>> {
    rewrite_node(plan, live, duplicate_insensitive)?.transform_data(|plan| {
        plan.map_subqueries(|subquery| {
            let live = LiveColumns::all(subquery.schema());
            rewrite_subtree(subquery, live, false)
        })
    })
}

fn rewrite_node(
    plan: LogicalPlan,
    live: LiveColumns,
    duplicate_insensitive: bool,
) -> Result<Transformed<LogicalPlan>> {
    match plan {
        // The only arm that rewrites a join; the rest just thread context down to one.
        LogicalPlan::Join(join) => rewrite_join(join, &live, duplicate_insensitive),
        LogicalPlan::Projection(Projection {
            expr,
            input,
            schema,
            ..
        }) => {
            // Narrows `live` to the columns the projection's expressions reference.
            let child_live = LiveColumns::try_new(&expr, input.schema())?;
            let child_duplicate_insensitive =
                duplicate_insensitive && expr.iter().all(is_repeatable);
            rewrite_single_input(
                input,
                child_live,
                child_duplicate_insensitive,
                |input| {
                    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                        expr, input, schema,
                    )?))
                },
            )
        }
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            // Adds the predicate's columns to `live` (a side used only by the filter stays live).
            let mut child_live = live;
            child_live.extend_from([&predicate], input.schema())?;
            let child_duplicate_insensitive =
                duplicate_insensitive && is_repeatable(&predicate);
            rewrite_single_input(
                input,
                child_live,
                child_duplicate_insensitive,
                |input| Ok(LogicalPlan::Filter(Filter::new(predicate, input))),
            )
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
            ..
        }) => {
            // Narrows `live` to the grouping and aggregate expressions' columns.
            let child_live = LiveColumns::try_new(
                group_expr.iter().chain(&aggr_expr),
                input.schema(),
            )?;

            // The input can ignore repeated rows when grouping expressions are
            // repeatable and every aggregate ignores duplicates, either by
            // nature (`min`) or because it deduplicates its own input
            // (`count(DISTINCT x)`). This covers grouping-only and global
            // aggregates. One sensitive aggregate makes input multiplicity
            // observable, even beneath an insensitive ancestor.
            let child_duplicate_insensitive =
                aggr_expr.iter().all(is_duplicate_insensitive_aggregate)
                    && group_expr.iter().all(is_repeatable);

            rewrite_single_input(
                input,
                child_live,
                child_duplicate_insensitive,
                |input| {
                    Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                        input, group_expr, aggr_expr, schema,
                    )?))
                },
            )
        }
        LogicalPlan::Distinct(Distinct::All(input)) => {
            // `SELECT DISTINCT *` is equivalent to a no-aggregate `GROUP BY`
            // over every input column, so the input is duplicate-insensitive,
            // but every column is part of the dedup key.
            let child_live = LiveColumns::all(input.schema());
            rewrite_single_input(input, child_live, true, |input| {
                Ok(LogicalPlan::Distinct(Distinct::All(input)))
            })
        }
        LogicalPlan::Distinct(Distinct::On(DistinctOn {
            on_expr,
            select_expr,
            sort_expr,
            input,
            schema,
        })) => {
            // `DISTINCT ON (on) select [ORDER BY sort]` is a no-aggregate
            // `GROUP BY` on the columns it reads, so its input is duplicate-
            // insensitive; the live columns are exactly those of the
            // ON/SELECT/ORDER BY expressions.
            let exprs = || {
                on_expr
                    .iter()
                    .chain(&select_expr)
                    .chain(sort_expr.iter().flatten().map(|sort| &sort.expr))
            };
            let child_live = LiveColumns::try_new(exprs(), input.schema())?;
            let child_duplicate_insensitive = exprs().all(is_repeatable);
            rewrite_single_input(
                input,
                child_live,
                child_duplicate_insensitive,
                |input| {
                    Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn {
                        on_expr,
                        select_expr,
                        sort_expr,
                        input,
                        schema,
                    })))
                },
            )
        }
        LogicalPlan::Sort(Sort { expr, input, fetch }) => {
            // Adds the sort-key columns to `live`.
            let mut child_live = live;
            child_live.extend_from(expr.iter().map(|s| &s.expr), input.schema())?;

            // A `fetch` (top-N) makes the row count observable, so duplicate-
            // insensitivity does not survive past it.
            let child_duplicate_insensitive = duplicate_insensitive
                && fetch.is_none()
                && expr.iter().all(|sort| is_repeatable(&sort.expr));
            rewrite_single_input(
                input,
                child_live,
                child_duplicate_insensitive,
                |input| Ok(LogicalPlan::Sort(Sort { expr, input, fetch })),
            )
        }
        LogicalPlan::Limit(Limit { skip, fetch, input }) => {
            // LIMIT makes the row count observable, so it clears duplicate-insensitivity.
            rewrite_single_input(input, live, false, |input| {
                Ok(LogicalPlan::Limit(Limit { skip, fetch, input }))
            })
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            // Re-aliases columns 1:1, so `live` and duplicate-sensitivity pass through unchanged.
            rewrite_single_input(input, live, duplicate_insensitive, |input| {
                Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                    input, alias,
                )?))
            })
        }
        LogicalPlan::Repartition(Repartition {
            input,
            partitioning_scheme,
        }) => {
            // Adds any partitioning-key columns to `live`; duplicate-sensitivity is unchanged.
            let mut child_live = live;
            match &partitioning_scheme {
                Partitioning::Hash(exprs, _) | Partitioning::DistributeBy(exprs) => {
                    child_live.extend_from(exprs, input.schema())?;
                }
                Partitioning::Range(range) => {
                    child_live.extend_from(
                        range.ordering().iter().map(|sort_expr| &sort_expr.expr),
                        input.schema(),
                    )?;
                }
                Partitioning::RoundRobinBatch(_) => {}
            }
            rewrite_single_input(input, child_live, duplicate_insensitive, |input| {
                Ok(LogicalPlan::Repartition(Repartition {
                    input,
                    partitioning_scheme,
                }))
            })
        }
        // Conservatively treat any other plan node as a fresh root, since we are
        // not sure of its semantics with respect to duplicates or live columns.
        _ => plan.map_children(|child| {
            let live = LiveColumns::all(child.schema());
            rewrite_subtree(child, live, false)
        }),
    }
}

/// Recurses into a single-input node's child, threading `child_live` and
/// `duplicate_insensitive` down, then rebuilds the node from the (possibly
/// rewritten) child via `rebuild`. The child's `Transformed` flag is preserved,
/// so the node is reported as changed exactly when its child changed.
fn rewrite_single_input<F>(
    input: Arc<LogicalPlan>,
    child_live: LiveColumns,
    duplicate_insensitive: bool,
    rebuild: F,
) -> Result<Transformed<LogicalPlan>>
where
    F: FnOnce(Arc<LogicalPlan>) -> Result<LogicalPlan>,
{
    rewrite_subtree(
        Arc::unwrap_or_clone(input),
        child_live,
        duplicate_insensitive,
    )?
    .map_data(|input| rebuild(Arc::new(input)))
}

fn rewrite_join(
    join: Join,
    live: &LiveColumns,
    duplicate_insensitive: bool,
) -> Result<Transformed<LogicalPlan>> {
    if join.join_type == JoinType::Inner
        && join.on.is_empty()
        && matches!(
            join.filter.as_ref(),
            Some(Expr::Literal(ScalarValue::Boolean(Some(false)), _))
        )
    {
        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
            EmptyRelation {
                produce_one_row: false,
                schema: join.schema,
            },
        )));
    }

    let (visible_left, visible_right) = split_join_output_columns(&join, live);

    // A semi join may stop evaluating conditions after finding a match.
    // If the conditions are not repeatable, skipping evaluations can change
    // later matches. Removing duplicate input rows can have the same effect.
    // Require repeatable conditions for this join and rewrites of its inputs.
    let repeatable = join
        .on
        .iter()
        .all(|(left, right)| is_repeatable(left) && is_repeatable(right))
        && join.filter.iter().all(is_repeatable);
    let duplicate_insensitive = duplicate_insensitive && repeatable;

    let rewritten_join_type = match rewritten_join_type(
        &join,
        &visible_left,
        &visible_right,
        duplicate_insensitive,
    ) {
        JoinRewrite::ReplaceWithLeft => {
            let left = rewrite_subtree(
                Arc::unwrap_or_clone(join.left),
                visible_left,
                duplicate_insensitive,
            )?;
            return Ok(Transformed::yes(left.data));
        }
        JoinRewrite::ReplaceWithRight => {
            let right = rewrite_subtree(
                Arc::unwrap_or_clone(join.right),
                visible_right,
                duplicate_insensitive,
            )?;
            return Ok(Transformed::yes(right.data));
        }
        JoinRewrite::Join(join_type) => join_type,
    };

    let (mut left_live, mut right_live) = match rewritten_join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (visible_left, LiveColumns::new())
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (LiveColumns::new(), visible_right)
        }
        _ => (visible_left, visible_right),
    };

    add_join_condition_columns(&join, &mut left_live, &mut right_live)?;

    let (left_dup_insensitive, right_dup_insensitive) = child_duplicate_insensitivity(
        rewritten_join_type,
        duplicate_insensitive,
        repeatable,
    );

    let left = rewrite_subtree(
        Arc::unwrap_or_clone(join.left),
        left_live,
        left_dup_insensitive,
    )?;
    let right = rewrite_subtree(
        Arc::unwrap_or_clone(join.right),
        right_live,
        right_dup_insensitive,
    )?;

    let changed =
        left.transformed || right.transformed || rewritten_join_type != join.join_type;
    let left = Arc::new(left.data);
    let right = Arc::new(right.data);

    if changed {
        // The join type or an input changed, so the output schema may have
        // narrowed; recompute it via `try_new`.
        Ok(Transformed::yes(LogicalPlan::Join(Join::try_new(
            left,
            right,
            join.on,
            join.filter,
            rewritten_join_type,
            join.join_constraint,
            join.null_equality,
            join.null_aware,
        )?)))
    } else {
        // Nothing changed; reassemble the join reusing its existing schema rather
        // than recomputing it.
        Ok(Transformed::no(LogicalPlan::Join(Join {
            left,
            right,
            on: join.on,
            filter: join.filter,
            join_type: join.join_type,
            join_constraint: join.join_constraint,
            schema: join.schema,
            null_equality: join.null_equality,
            null_aware: join.null_aware,
        })))
    }
}

/// Returns which join inputs can safely ignore duplicate rows from their own
/// descendants. For semi/anti/mark joins, duplicates from the existence side do
/// not change the result even when the parent itself is duplicate-sensitive,
/// provided the join conditions are `repeatable`: removing duplicate rows
/// changes how often the conditions are evaluated.
fn child_duplicate_insensitivity(
    join_type: JoinType,
    duplicate_insensitive: bool,
    repeatable: bool,
) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (duplicate_insensitive, duplicate_insensitive),
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (duplicate_insensitive, repeatable)
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (repeatable, duplicate_insensitive)
        }
        JoinType::Left | JoinType::Right | JoinType::Full => (false, false),
    }
}

/// The rewrite chosen for a join by [`rewritten_join_type`].
enum JoinRewrite {
    /// Keep the join, with this (possibly rewritten) join type.
    Join(JoinType),
    /// The join has no observable effect; replace it with its left input.
    ReplaceWithLeft,
    /// The join has no observable effect; replace it with its right input.
    ReplaceWithRight,
}

/// Chooses a cheaper form for a join: removes an outer join whose non-preserved
/// side is redundant, or rewrites an inner join to a semi join when the
/// removed side has no parent-visible columns and either the parent ignores
/// duplicate output rows or the removed side is unique on the join keys.
fn rewritten_join_type(
    join: &Join,
    visible_left: &LiveColumns,
    visible_right: &LiveColumns,
    duplicate_insensitive: bool,
) -> JoinRewrite {
    // A side is redundant when nothing above the join references its columns
    // and it cannot multiply the other side's rows (the ancestors are
    // duplicate-insensitive, or the side is unique on the join keys).
    let can_remove_right = visible_right.is_empty()
        && (duplicate_insensitive
            || side_unique_on_join(
                join.right.schema(),
                join.on.iter().map(|(_, right)| right),
                join.null_equality,
            ));

    // A LEFT JOIN preserves every left row, so with a redundant right side the
    // join has no observable effect and can be replaced by its left input.
    // A filter only decides whether a row is matched or null-padded. When
    // relying on duplicate-insensitivity, the caller has already required
    // repeatable join conditions.
    if join.join_type == JoinType::Left && can_remove_right {
        return JoinRewrite::ReplaceWithLeft;
    }
    let can_remove_left = visible_left.is_empty()
        && (duplicate_insensitive
            || side_unique_on_join(
                join.left.schema(),
                join.on.iter().map(|(left, _)| left),
                join.null_equality,
            ));

    // Symmetrical rule for RIGHT JOIN removal (same explanation as above for the left-join case)
    if join.join_type == JoinType::Right && can_remove_left {
        return JoinRewrite::ReplaceWithRight;
    }

    if join.join_type != JoinType::Inner || join.on.is_empty() {
        return JoinRewrite::Join(join.join_type);
    }

    if can_remove_right {
        return JoinRewrite::Join(JoinType::LeftSemi);
    }
    if can_remove_left {
        return JoinRewrite::Join(JoinType::RightSemi);
    }

    JoinRewrite::Join(JoinType::Inner)
}

fn add_join_condition_columns(
    join: &Join,
    left_live: &mut LiveColumns,
    right_live: &mut LiveColumns,
) -> Result<()> {
    left_live.extend_from(join.on.iter().map(|(l, _)| l), join.left.schema())?;
    right_live.extend_from(join.on.iter().map(|(_, r)| r), join.right.schema())?;

    if let Some(filter) = &join.filter {
        left_live.extend_from([filter], join.left.schema())?;
        right_live.extend_from([filter], join.right.schema())?;
    }

    Ok(())
}

fn split_join_output_columns(
    join: &Join,
    live: &LiveColumns,
) -> (LiveColumns, LiveColumns) {
    let left_len = join.left.schema().fields().len();
    match join.join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            live.split_at(left_len)
        }
        // A semi/anti/mark join outputs only the surviving side's columns, with
        // the same index space, so `live` passes straight through to that side.
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (live.clone(), LiveColumns::new())
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (LiveColumns::new(), live.clone())
        }
    }
}

fn side_unique_on_join<'a>(
    schema: &DFSchema,
    join_exprs: impl Iterator<Item = &'a Expr>,
    null_equality: NullEquality,
) -> bool {
    let join_key_indices = join_exprs
        .filter_map(|expr| match expr {
            Expr::Alias(alias) => alias.expr.as_ref().try_as_col(),
            _ => expr.try_as_col(),
        })
        .filter_map(|column| schema.maybe_index_of_column(column))
        .collect::<Vec<usize>>();

    schema.functional_dependencies().iter().any(|dependency| {
        dependency.mode == Dependency::Single
            && (!dependency.nullable || null_equality == NullEquality::NullEqualsNothing)
            && dependency
                .source_indices
                .iter()
                .all(|idx| join_key_indices.contains(idx))
    })
}

#[cfg(test)]
mod tests {
    use crate::OptimizerContext;
    use crate::OptimizerRule;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::eliminate_join::EliminateJoin;
    use crate::test::udfs::{DistinctHandlingTestUDAF, PlacementTestUDF};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::tree_node::Transformed;
    use datafusion_common::{
        Constraint, Constraints, NullEquality, Result, ScalarValue, SplitPoint,
    };
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::{
        AggregateUDF, DistinctHandling, Expr, ExprFunctionExt, JoinType, LogicalPlan,
        Partitioning, RangePartitioning, ScalarUDF, Volatility, col, exists, lit,
        logical_plan::builder::{
            LogicalPlanBuilder, table_scan, table_source_with_constraints,
        },
        out_ref_col, scalar_subquery,
    };
    use datafusion_functions_aggregate::expr_fn::{
        corr, count, count_distinct, max, min, regr_count, stddev,
    };
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    fn rewrite(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        EliminateJoin::new().rewrite(plan, &OptimizerContext::new())
    }

    fn assert_not_rewritten(plan: LogicalPlan) -> Result<()> {
        assert!(!rewrite(plan)?.transformed);
        Ok(())
    }

    /// A scalar UDF with the given volatility.
    fn test_udf(volatility: Volatility) -> ScalarUDF {
        ScalarUDF::from(PlacementTestUDF::new().with_volatility(volatility))
    }

    #[test]
    fn join_on_false() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .join_on(
                LogicalPlanBuilder::empty(false).build()?,
                Inner,
                Some(lit(false)),
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

    #[test]
    fn inner_to_left_semi_when_removed_side_is_unique() -> Result<()> {
        let plan = left_join_right_with_constraints(primary_key_on_id())?
            .project(vec![col("l.x")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          LeftSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn inner_to_left_semi_when_removed_side_is_unique_with_join_filter() -> Result<()> {
        let right = scan("r", &test_schema(), primary_key_on_id())?;
        let plan =
            LogicalPlanBuilder::from(scan("l", &test_schema(), Constraints::default())?)
                .join(
                    right,
                    Inner,
                    (vec!["l.id"], vec!["r.id"]),
                    Some(col("r.y").gt(col("l.x"))),
                )?
                .project(vec![col("l.x")])?
                .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          LeftSemi Join: l.id = r.id Filter: r.y > l.x
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn inner_to_right_semi_when_removed_side_is_unique() -> Result<()> {
        let plan = left_with_constraints_join_right(primary_key_on_id())?
            .project(vec![col("r.y")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: r.y
          RightSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn inner_to_left_semi_for_duplicate_insensitive_parent() -> Result<()> {
        let plan = left_join_right()?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          LeftSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn count_is_not_duplicate_insensitive() -> Result<()> {
        // COUNT observes how many rows fall in each group. With a non-unique
        // right side the join must stay an inner join: collapsing it to a semi
        // join would drop matching duplicates and undercount `count(l.id)`.
        let plan = left_join_right()?
            .aggregate(vec![col("l.x")], vec![count(col("l.id"))])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[count(l.id)]]
          Inner Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn insensitive_aggregates_enable_semi_joins() -> Result<()> {
        for column in ["l.x", "r.x"] {
            let aggr_expr = vec![
                min(col(column)).alias("minimum"),
                max(col(column)).distinct().build()?,
            ];
            // Both global and grouped aggregates ignore duplicate input rows.
            for group_expr in [vec![], vec![col(column)]] {
                let plan = left_join_right()?
                    .aggregate(group_expr, aggr_expr.clone())?
                    .build()?;
                let result = rewrite(plan)?;
                assert!(result.transformed);
                let LogicalPlan::Aggregate(aggregate) = result.data else {
                    panic!("expected aggregate");
                };
                assert_eq!(aggregate.aggr_expr, aggr_expr);
                let LogicalPlan::Join(join) = aggregate.input.as_ref() else {
                    panic!("expected join");
                };
                assert_eq!(
                    join.join_type,
                    if column == "l.x" {
                        JoinType::LeftSemi
                    } else {
                        JoinType::RightSemi
                    }
                );
            }
        }
        Ok(())
    }

    #[test]
    fn global_min_removes_unused_outer_join() -> Result<()> {
        for (join_type, column, table) in
            [(JoinType::Left, "l.x", "l"), (JoinType::Right, "r.x", "r")]
        {
            let left = scan("l", &test_schema(), Constraints::default())?;
            let right = scan("r", &test_schema(), Constraints::default())?;
            let plan = LogicalPlanBuilder::from(left)
                .join(right, join_type, (vec!["l.id"], vec!["r.id"]), None)?
                .aggregate(Vec::<Expr>::new(), vec![min(col(column))])?
                .build()?;
            let optimized = rewrite(plan)?.data;
            let expected = LogicalPlanBuilder::from(scan(
                table,
                &test_schema(),
                Constraints::default(),
            )?)
            .aggregate(Vec::<Expr>::new(), vec![min(col(column))])?
            .build()?;
            assert_eq!(optimized, expected);
        }
        Ok(())
    }

    #[test]
    fn distinct_sensitive_aggregates_enable_semi_joins() -> Result<()> {
        // A `Sensitive` function called with DISTINCT deduplicates its own
        // input, so it cannot observe rows repeated by the join.
        for aggr_expr in [
            vec![count_distinct(col("l.x"))],
            vec![count_distinct(col("l.x")), count_distinct(col("l.y"))],
            vec![
                min(col("l.x")),
                count(col("l.x"))
                    .distinct()
                    .filter(col("l.y").gt(lit(0)))
                    .build()?,
            ],
        ] {
            let plan = left_join_right()?
                .aggregate(vec![col("l.id")], aggr_expr)?
                .build()?;
            let result = rewrite(plan)?;
            assert!(result.transformed);
            let LogicalPlan::Aggregate(aggregate) = result.data else {
                panic!("expected aggregate");
            };
            let LogicalPlan::Join(join) = aggregate.input.as_ref() else {
                panic!("expected join");
            };
            assert_eq!(join.join_type, JoinType::LeftSemi);
        }
        Ok(())
    }

    #[test]
    fn duplicate_sensitive_aggregates_block_rewrite() -> Result<()> {
        // One aggregate that observes repeated rows keeps the join, even
        // beside aggregates that do not. DISTINCT does not qualify an
        // `Unsupported` function: its accumulator does not deduplicate, and
        // may silently compute the non-distinct answer.
        for sensitive in [
            count(col("l.x")),
            stddev(col("l.x")).distinct().build()?,
            corr(col("l.x"), col("l.y")).distinct().build()?,
            regr_count(col("l.x"), col("l.y")).distinct().build()?,
        ] {
            let plan = left_join_right()?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![min(col("l.x")), count_distinct(col("l.x")), sensitive],
                )?
                .build()?;
            assert_not_rewritten(plan)?;
        }
        Ok(())
    }

    #[test]
    fn sensitive_aggregate_blocks_insensitive_ancestor() -> Result<()> {
        let plan = left_join_right()?
            .aggregate(vec![col("l.x")], vec![count(col("l.id")).alias("n")])?
            .aggregate(Vec::<Expr>::new(), vec![min(col("n"))])?
            .build()?;
        assert_not_rewritten(plan)?;
        Ok(())
    }

    #[test]
    fn subquery_aggregate_argument_blocks_rewrite() -> Result<()> {
        // Expr's usual volatility check does not descend into a subquery plan.
        let volatile = test_udf(Volatility::Volatile).call(vec![lit(1)]);
        let subquery = LogicalPlanBuilder::empty(true)
            .project(vec![volatile])?
            .build()?;
        let plan = left_join_right()?
            .aggregate(
                vec![col("l.x")],
                vec![min(scalar_subquery(Arc::new(subquery)))],
            )?
            .build()?;
        assert_not_rewritten(plan)?;
        Ok(())
    }

    #[test]
    fn aggregate_filter_and_ordering_keep_columns_live() -> Result<()> {
        for aggr in [
            min(col("l.x")).filter(col("r.y").gt(lit(0))).build()?,
            min(col("l.x"))
                .order_by(vec![col("r.y").sort(true, false)])
                .build()?,
        ] {
            let plan = left_join_right()?
                .aggregate(Vec::<Expr>::new(), vec![aggr])?
                .build()?;
            assert_not_rewritten(plan)?;
        }

        let plan = left_join_right()?
            .aggregate(
                Vec::<Expr>::new(),
                vec![min(col("l.x")).filter(col("l.y").gt(lit(0))).build()?],
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[]], aggr=[[min(l.x) FILTER (WHERE l.y > Int32(0))]]
          LeftSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    fn volatile_expr() -> Expr {
        test_udf(Volatility::Volatile).call(vec![col("l.x")])
    }

    #[test]
    fn volatile_aggregate_expressions_block_rewrite() -> Result<()> {
        for (group_expr, aggr) in [
            (vec![], min(volatile_expr())),
            (vec![volatile_expr()], min(col("l.x"))),
            (
                vec![],
                min(col("l.x"))
                    .filter(volatile_expr().gt(lit(0_u32)))
                    .build()?,
            ),
            (
                vec![],
                min(col("l.x"))
                    .order_by(vec![volatile_expr().sort(true, false)])
                    .build()?,
            ),
        ] {
            let plan = left_join_right()?
                .aggregate(group_expr, vec![aggr])?
                .build()?;
            assert_not_rewritten(plan)?;
        }
        Ok(())
    }

    #[test]
    fn volatile_intervening_expressions_block_rewrite() -> Result<()> {
        for input in [
            left_join_right()?.project(vec![col("l.x"), volatile_expr().alias("v")])?,
            left_join_right()?.filter(volatile_expr().gt(lit(0_u32)))?,
            left_join_right()?.sort(vec![volatile_expr().sort(true, false)])?,
        ] {
            let plan = input
                .aggregate(Vec::<Expr>::new(), vec![min(col("l.x"))])?
                .build()?;
            assert_not_rewritten(plan)?;
        }
        Ok(())
    }

    #[test]
    fn join_conditions_must_be_repeatable() -> Result<()> {
        for volatility in [Volatility::Stable, Volatility::Volatile] {
            let udf = test_udf(volatility);
            for (left_key, right_key, filter) in [
                (udf.call(vec![col("l.id")]), col("r.id"), None),
                (col("l.id"), udf.call(vec![col("r.id")]), None),
                (
                    col("l.id"),
                    col("r.id"),
                    Some(udf.call(vec![col("l.x")]).gt(lit(0_u32))),
                ),
            ] {
                let plan = LogicalPlanBuilder::from(scan(
                    "l",
                    &test_schema(),
                    Constraints::default(),
                )?)
                .join_with_expr_keys(
                    scan("r", &test_schema(), Constraints::default())?,
                    Inner,
                    (vec![left_key], vec![right_key]),
                    filter,
                )?
                .aggregate(Vec::<Expr>::new(), vec![min(col("l.x"))])?
                .build()?;
                let result = rewrite(plan.clone())?;
                assert_eq!(
                    result.transformed,
                    volatility != Volatility::Volatile,
                    "{volatility:?}: {}",
                    plan.display_indent(),
                );
            }
        }
        Ok(())
    }

    #[test]
    fn existence_side_rewrites_require_repeatable_join_conditions() -> Result<()> {
        // There is no duplicate-insensitive ancestor. Only the semi join's
        // existence side can make the nested inner join eligible for rewriting.
        for join_type in [JoinType::LeftSemi, JoinType::RightSemi] {
            for volatility in [Volatility::Stable, Volatility::Volatile] {
                let inner = left_join_right()?.build()?;
                let other = scan("s", &test_schema(), Constraints::default())?;
                let (left, right, keys) = if join_type == JoinType::LeftSemi {
                    (other, inner, (vec!["s.id"], vec!["l.id"]))
                } else {
                    (inner, other, (vec!["l.id"], vec!["s.id"]))
                };
                let predicate =
                    test_udf(volatility).call(vec![col("l.x")]).gt(lit(0_u32));
                let plan = LogicalPlanBuilder::from(left)
                    .join(right, join_type, keys, Some(predicate))?
                    .build()?;
                let result = rewrite(plan)?;
                let repeatable = volatility != Volatility::Volatile;
                assert_eq!(
                    result.transformed, repeatable,
                    "{join_type:?}, {volatility:?}"
                );
                let LogicalPlan::Join(join) = result.data else {
                    panic!("expected semi join");
                };
                assert_eq!(join.join_type, join_type);
                let existence_side = if join_type == JoinType::LeftSemi {
                    join.right
                } else {
                    join.left
                };
                let LogicalPlan::Join(nested) = existence_side.as_ref() else {
                    panic!("expected nested join");
                };
                assert_eq!(
                    nested.join_type,
                    if repeatable {
                        JoinType::LeftSemi
                    } else {
                        Inner
                    },
                );
            }
        }
        Ok(())
    }

    #[test]
    fn distinct_only_qualifies_sensitive_udaf() -> Result<()> {
        for (distinct_handling, distinct, expect_rewrite) in [
            (DistinctHandling::Insensitive, false, true),
            (DistinctHandling::Insensitive, true, true),
            (DistinctHandling::Sensitive, false, false),
            // The accumulator deduplicates its own input.
            (DistinctHandling::Sensitive, true, true),
            (DistinctHandling::Unsupported, false, false),
            // The accumulator does not implement DISTINCT, so the input's
            // repeated rows stay observable.
            (DistinctHandling::Unsupported, true, false),
        ] {
            let udf = AggregateUDF::from(DistinctHandlingTestUDAF::new(
                "custom_udaf",
                distinct_handling,
            ));
            let mut aggr = udf.call(vec![col("l.x")]);
            if distinct {
                aggr = aggr.distinct().build()?;
            }
            let plan = left_join_right()?
                .aggregate(Vec::<Expr>::new(), vec![aggr])?
                .build()?;
            let result = rewrite(plan)?;
            assert_eq!(
                result.transformed, expect_rewrite,
                "{distinct_handling:?}, distinct={distinct}"
            );
        }
        Ok(())
    }

    #[test]
    fn aliased_udaf_uses_declared_handling() -> Result<()> {
        for volatility in [
            Volatility::Immutable,
            Volatility::Stable,
            Volatility::Volatile,
        ] {
            let udf = AggregateUDF::from(
                DistinctHandlingTestUDAF::new(
                    "custom_udaf",
                    DistinctHandling::Insensitive,
                )
                .with_volatility(volatility),
            )
            .with_aliases(["custom_alias"]);
            let plan = left_join_right()?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![udf.call(vec![col("l.x")]).alias("result")],
                )?
                .build()?;
            let result = rewrite(plan)?;
            assert_eq!(result.transformed, volatility != Volatility::Volatile);
        }
        Ok(())
    }

    #[test]
    fn duplicate_insensitive_context_propagates_through_join_tree() -> Result<()> {
        let left = scan("l", &test_schema(), Constraints::default())?;
        let middle = scan("m", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), Constraints::default())?;

        let left_join_middle = LogicalPlanBuilder::from(left)
            .join(middle, Inner, (vec!["l.id"], vec!["m.id"]), None)?
            .build()?;

        let plan = LogicalPlanBuilder::from(left_join_middle)
            .join(right, Inner, (vec!["l.id"], vec!["r.id"]), None)?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          LeftSemi Join: l.id = r.id
            LeftSemi Join: l.id = m.id
              TableScan: l
              TableScan: m
            TableScan: r
        ")
    }

    #[test]
    fn projection_does_not_rewrite_without_uniqueness() -> Result<()> {
        let plan = left_join_right()?.project(vec![col("l.x")])?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          Inner Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn required_filter_column_prevents_duplicate_insensitive_rewrite() -> Result<()> {
        let plan = left_join_right()?
            .filter(col("r.y").gt(lit(10_i32)))?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          Filter: r.y > Int32(10)
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn distinct_star_keeps_unreferenced_side() -> Result<()> {
        // `SELECT DISTINCT *` deduplicates on every join-output column, including
        // the right side's. With a non-unique right side the inner join can
        // multiply left rows into distinct `(l, r)` combinations, so the join
        // must not be rewritten to a semi join (which would drop the right
        // columns from the DISTINCT key and undercount the result). This holds
        // even when the right side is unique on the join keys: its columns are
        // part of the DISTINCT key regardless.
        let plan = left_join_right()?
            .distinct()?
            .project(vec![col("l.x")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          Distinct:
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn distinct_drops_unreferenced_side_when_projected() -> Result<()> {
        // `SELECT DISTINCT l.x` projects the right side away below the DISTINCT,
        // leaving it outside the dedup key. Like a no-aggregate `GROUP BY l.x`,
        // the DISTINCT makes the input duplicate-insensitive, so the inner join
        // collapses to a semi join even though the right side is not unique.
        let plan = left_join_right()?
            .project(vec![col("l.x")])?
            .distinct()?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Projection: l.x
            LeftSemi Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn correlated_subquery_outer_ref_prevents_rewrite() -> Result<()> {
        // The right side is unique, so the subquery's repeatability barrier
        // alone cannot prevent a semi-join rewrite. Tracking the correlated
        // `r.y` reference must keep the right side live and the join inner.
        let subquery =
            LogicalPlanBuilder::from(scan("s", &test_schema(), Constraints::default())?)
                .filter(col("s.id").eq(out_ref_col(DataType::Int32, "r.y")))?
                .project(vec![lit(1)])?
                .build()?;

        let plan = left_join_right_with_constraints(primary_key_on_id())?
            .filter(exists(Arc::new(subquery)))?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          Filter: EXISTS (<subquery>)
            Subquery:
              Projection: Int32(1)
                Filter: s.id = outer_ref(r.y)
                  TableScan: s
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn inner_to_semi_inside_uncorrelated_subquery() -> Result<()> {
        // A join nested inside a (not-yet-decorrelated) subquery is still
        // rewritten, because `rewrite_subtree` descends into subquery plans
        // itself via `map_subqueries`. Here the subquery's projection keeps
        // only `l.x` and the removed side `r` is unique (PK), so the inner join
        // collapses to a semi join.
        let subquery = left_join_right_with_constraints(primary_key_on_id())?
            .project(vec![col("l.x")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(scan(
            "outer",
            &test_schema(),
            Constraints::default(),
        )?)
        .filter(exists(Arc::new(subquery)))?
        .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: EXISTS (<subquery>)
          Subquery:
            Projection: l.x
              LeftSemi Join: l.id = r.id
                TableScan: l
                TableScan: r
          TableScan: outer
        ")
    }

    #[test]
    fn inner_to_semi_inside_correlated_subquery() -> Result<()> {
        // `map_subqueries` descends into correlated subqueries too, not just
        // uncorrelated ones, so a join inside one is still rewritten. The
        // subquery correlates on `outer.id` (via the filter), but that reference
        // and the projection touch only `l`; `r` is unique (PK) and unreferenced,
        // so the inner join inside the subquery collapses to a semi join.
        let subquery = left_join_right_with_constraints(primary_key_on_id())?
            .filter(col("l.x").eq(out_ref_col(DataType::Int32, "outer.id")))?
            .project(vec![col("l.x")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(scan(
            "outer",
            &test_schema(),
            Constraints::default(),
        )?)
        .filter(exists(Arc::new(subquery)))?
        .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Filter: EXISTS (<subquery>)
          Subquery:
            Projection: l.x
              Filter: l.x = outer_ref(outer.id)
                LeftSemi Join: l.id = r.id
                  TableScan: l
                  TableScan: r
          TableScan: outer
        ")
    }

    #[test]
    fn nullable_unique_rewrites_under_null_equals_nothing() -> Result<()> {
        // A `UNIQUE` (rather than `PRIMARY KEY`) constraint marks the key as
        // nullable. Under the default `NullEqualsNothing` join semantics a null
        // key matches nothing, so a unique side still yields at most one match
        // per left row and the inner join can become a semi join.
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), unique_on_x())?;
        let plan = LogicalPlanBuilder::from(left)
            .join(right, Inner, (vec!["l.x"], vec!["r.x"]), None)?
            .project(vec![col("l.id")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.id
          LeftSemi Join: l.x = r.x
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn nullable_unique_does_not_rewrite_under_null_equals_null() -> Result<()> {
        // With `NullEqualsNull` semantics two null keys compare equal, so a
        // nullable `UNIQUE` key no longer guarantees at most one match per left
        // row: several null-keyed right rows could match a null-keyed left row.
        // Uniqueness on the join keys is therefore not established and the inner
        // join must be preserved.
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), unique_on_x())?;
        let plan = LogicalPlanBuilder::from(left)
            .join_detailed(
                right,
                Inner,
                (vec!["l.x"], vec!["r.x"]),
                None,
                NullEquality::NullEqualsNull,
            )?
            .project(vec![col("l.id")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.id
          Inner Join: l.x = r.x
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn composite_unique_rewrites_when_join_covers_all_key_columns() -> Result<()> {
        // The removed side is unique on the composite key `(id, x)`. The join
        // equates both key columns, so each left row matches at most one right
        // row and the inner join can become a semi join.
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), composite_primary_key_on_id_x())?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                Inner,
                (vec!["l.id", "l.x"], vec!["r.id", "r.x"]),
                None,
            )?
            .project(vec![col("l.y")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.y
          LeftSemi Join: l.id = r.id, l.x = r.x
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn composite_unique_does_not_rewrite_when_join_misses_a_key_column() -> Result<()> {
        // The removed side is unique only on the *composite* key `(id, x)`. The
        // join equates `id` but not `x`, so a left row may match many right rows
        // (those sharing its `id` but differing in `x`). Uniqueness on the join
        // keys is not established, so the inner join must be preserved. This
        // guards the requirement that the join cover *every* column of the
        // unique key, not just some.
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), composite_primary_key_on_id_x())?;
        let plan = LogicalPlanBuilder::from(left)
            .join(right, Inner, (vec!["l.id"], vec!["r.id"]), None)?
            .project(vec![col("l.y")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.y
          Inner Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn top_n_sort_blocks_duplicate_insensitive_rewrite() -> Result<()> {
        // A top-N `Sort` (one with a `fetch`) makes the row count observable, so
        // the duplicate-insensitivity established by the `GROUP BY` does not survive
        // past it. With a non-unique right side the join must stay an inner join: a
        // semi join could drop matching duplicates and change which rows fall within
        // the top N.
        let plan = left_join_right()?
            .sort_with_limit(vec![col("l.x").sort(true, false)], Some(5))?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          Sort: l.x ASC NULLS LAST, fetch=5
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn sort_without_fetch_preserves_duplicate_insensitive_rewrite() -> Result<()> {
        // A `Sort` without a `fetch` does not make the row count observable, so it
        // forwards the parent's duplicate-insensitivity to the join unchanged
        // (sorting before or after duplicate removal is equivalent). The non-unique
        // right side is unreferenced, so the inner join collapses to a semi join.
        let plan = left_join_right()?
            .sort(vec![col("l.x").sort(true, false)])?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          Sort: l.x ASC NULLS LAST
            LeftSemi Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn limit_blocks_duplicate_insensitive_rewrite() -> Result<()> {
        // `LIMIT` makes the row count observable, clearing the duplicate-
        // insensitivity established by the `GROUP BY`. With a non-unique right side
        // the join must stay an inner join, since a semi join could drop matching
        // duplicates and change which rows the limit returns.
        let plan = left_join_right()?
            .limit(0, Some(5))?
            .aggregate(vec![col("l.x")], Vec::<Expr>::new())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[l.x]], aggr=[[]]
          Limit: skip=0, fetch=5
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn repartition_hash_key_keeps_removed_side_live() -> Result<()> {
        // The projection keeps only `l.x`, and the right side is unique (PK), so
        // absent any other use of `r` the inner join would collapse to a semi join.
        // But the `Repartition` hashes on `r.y`, which keeps the right side live, so
        // the join must stay an inner join to preserve `r.y` for the partitioning.
        let plan = left_join_right_with_constraints(primary_key_on_id())?
            .repartition(Partitioning::Hash(vec![col("r.y")], 4))?
            .project(vec![col("l.x")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          Repartition: Hash(r.y) partition_count=4
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn repartition_range_key_keeps_removed_side_live() -> Result<()> {
        // The projection keeps only `l.x`, and the right side is unique (PK), so
        // absent any other use of `r` the inner join would collapse to a semi join.
        // But the `Repartition` ranges on `r.y`, which keeps the right side live, so
        // the join must stay an inner join to preserve `r.y` for the partitioning.
        let plan = left_join_right_with_constraints(primary_key_on_id())?
            .repartition(Partitioning::Range(RangePartitioning::try_new(
                vec![col("r.y").sort(true, true)],
                vec![SplitPoint::new(vec![ScalarValue::Int32(Some(10))])],
            )?))?
            .project(vec![col("l.x")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          Repartition: Range([r.y ASC NULLS FIRST], [(10)], 2)
            Inner Join: l.id = r.id
              TableScan: l
              TableScan: r
        ")
    }

    #[test]
    fn distinct_on_enables_semi_join_rewrite() -> Result<()> {
        // `DISTINCT ON (l.x)` is a no-aggregate `GROUP BY` on the columns it reads,
        // so it makes its input duplicate-insensitive. The non-unique right side is
        // unreferenced, so the inner join collapses to a semi join.
        let plan = left_join_right()?
            .distinct_on(vec![col("l.x")], vec![col("l.x")], None)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        DistinctOn: on_expr=[[l.x]], select_expr=[[l.x]], sort_expr=[[]]
          LeftSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    #[test]
    fn distinct_on_expressions_must_be_repeatable() -> Result<()> {
        for volatility in [Volatility::Stable, Volatility::Volatile] {
            let expr = test_udf(volatility).call(vec![col("l.x")]);
            for (on_expr, select_expr, sort_expr) in [
                (vec![expr.clone()], vec![col("l.x")], None),
                (vec![col("l.x")], vec![expr.clone()], None),
                (
                    vec![col("l.x")],
                    vec![col("l.x")],
                    Some(vec![col("l.x").sort(true, false), expr.sort(true, false)]),
                ),
            ] {
                let plan = left_join_right()?
                    .distinct_on(on_expr, select_expr, sort_expr)?
                    .build()?;
                let result = rewrite(plan.clone())?;
                assert_eq!(
                    result.transformed,
                    volatility != Volatility::Volatile,
                    "{volatility:?}: {}",
                    plan.display_indent(),
                );
            }
        }
        Ok(())
    }

    #[test]
    fn existing_semi_join_passes_through_unchanged() -> Result<()> {
        // A join that is already a semi join is threaded through unchanged: the rule
        // only rewrites inner joins. This exercises the context-propagation paths for
        // a non-inner join type, whose existence side contributes no live columns.
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), Constraints::default())?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::LeftSemi,
                (vec!["l.id"], vec!["r.id"]),
                None,
            )?
            .project(vec![col("l.x")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.x
          LeftSemi Join: l.id = r.id
            TableScan: l
            TableScan: r
        ")
    }

    fn left_join_right() -> Result<LogicalPlanBuilder> {
        left_join_right_with_constraints(Constraints::default())
    }

    fn left_join_right_with_constraints(
        right_constraints: Constraints,
    ) -> Result<LogicalPlanBuilder> {
        let left = scan("l", &test_schema(), Constraints::default())?;
        let right = scan("r", &test_schema(), right_constraints)?;

        LogicalPlanBuilder::from(left).join(
            right,
            Inner,
            (vec!["l.id"], vec!["r.id"]),
            None,
        )
    }

    fn left_with_constraints_join_right(
        left_constraints: Constraints,
    ) -> Result<LogicalPlanBuilder> {
        let left = scan("l", &test_schema(), left_constraints)?;
        let right = scan("r", &test_schema(), Constraints::default())?;

        LogicalPlanBuilder::from(left).join(
            right,
            Inner,
            (vec!["l.id"], vec!["r.id"]),
            None,
        )
    }

    fn scan(
        name: &str,
        schema: &Schema,
        constraints: Constraints,
    ) -> Result<LogicalPlan> {
        if constraints.is_empty() {
            table_scan(Some(name), schema, None)?.build()
        } else {
            LogicalPlanBuilder::scan(
                name,
                table_source_with_constraints(schema, constraints),
                None,
            )?
            .build()
        }
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ])
    }

    fn primary_key_on_id() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    }

    /// A nullable unique key on column `x` (index 1). `Unique` (unlike
    /// `PrimaryKey`) marks the dependency as nullable, which is what gates the
    /// rewrite on the join's `null_equality`.
    fn unique_on_x() -> Constraints {
        Constraints::new_unverified(vec![Constraint::Unique(vec![1])])
    }

    /// A composite primary key spanning columns `id` and `x` (indices 0 and 1).
    fn composite_primary_key_on_id_x() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0, 1])])
    }
}
