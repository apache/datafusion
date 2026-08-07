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

//! [`EliminateJoin`] rewrites inner joins to simpler forms to make them cheaper
//! to evaluate. We implement two distinct rewrites:
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
//!        join's ancestors are duplicate-insensitive (e.g., DISTINCT) or we can use
//!        functional dependencies to prove that each L row matches at most one R
//!        row (R is provably unique on the join keys).
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
//!   DISTINCT, GROUP BY with no aggregate functions, or the existence side of a
//!   semi/anti/mark join) sets it `true` for its subtree, and it propagates
//!   downward until a node that makes the row count observable again (a `LIMIT`,
//!   a top-N sort, ...) clears it. It is therefore fixed by the nearest such
//!   node, not by the whole ancestor chain: a collapsing node shields its subtree,
//!   so a duplicate-sensitive node further above does not matter.
//!
//! At each join, `rewritten_join_type` combines this context with the side's
//! functional dependencies to choose `Inner`, `LeftSemi`, or `RightSemi`. Most
//! node types just forward the context to their single child via
//! `rewrite_single_input`; nodes that alter column requirements or
//! duplicate-sensitivity (projection, aggregate, sort, ...) adjust it first.
use crate::utils::for_each_referenced_index;
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

/// Rewrites an inner join to a semi join when one input only filters the other,
/// and replaces an always-false inner join with an empty relation.
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
            rewrite_single_input(input, child_live, duplicate_insensitive, |input| {
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr, input, schema,
                )?))
            })
        }
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            // Adds the predicate's columns to `live` (a side used only by the filter stays live).
            let mut child_live = live;
            child_live.extend_from([&predicate], input.schema())?;
            rewrite_single_input(input, child_live, duplicate_insensitive, |input| {
                Ok(LogicalPlan::Filter(Filter::new(predicate, input)))
            })
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

            // A grouping aggregate with no aggregate functions (`GROUP BY` with
            // an empty `aggr_expr`) only observes which group-key values exist,
            // not how many rows produced them, so its input is duplicate-
            // insensitive.
            let child_duplicate_insensitive =
                !group_expr.is_empty() && aggr_expr.is_empty();

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
            let mut child_live =
                LiveColumns::try_new(on_expr.iter().chain(&select_expr), input.schema())?;
            if let Some(sort_expr) = &sort_expr {
                child_live
                    .extend_from(sort_expr.iter().map(|s| &s.expr), input.schema())?;
            }

            rewrite_single_input(input, child_live, true, |input| {
                Ok(LogicalPlan::Distinct(Distinct::On(DistinctOn {
                    on_expr,
                    select_expr,
                    sort_expr,
                    input,
                    schema,
                })))
            })
        }
        LogicalPlan::Sort(Sort { expr, input, fetch }) => {
            // Adds the sort-key columns to `live`.
            let mut child_live = live;
            child_live.extend_from(expr.iter().map(|s| &s.expr), input.schema())?;

            // A `fetch` (top-N) makes the row count observable, so duplicate-
            // insensitivity does not survive past it.
            let child_duplicate_insensitive = duplicate_insensitive && fetch.is_none();
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

    let rewritten_join_type =
        rewritten_join_type(&join, &visible_left, &visible_right, duplicate_insensitive);

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

    let (left_dup_insensitive, right_dup_insensitive) =
        child_duplicate_insensitivity(rewritten_join_type, duplicate_insensitive);

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
/// not change the result even when the parent itself is duplicate-sensitive.
fn child_duplicate_insensitivity(
    join_type: JoinType,
    duplicate_insensitive: bool,
) -> (bool, bool) {
    match join_type {
        JoinType::Inner => (duplicate_insensitive, duplicate_insensitive),
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            (duplicate_insensitive, true)
        }
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
            (true, duplicate_insensitive)
        }
        JoinType::Left | JoinType::Right | JoinType::Full => (false, false),
    }
}

/// Rewrites an inner join to a semi join when the removed side has no
/// parent-visible columns and either the parent ignores duplicate output rows or
/// the removed side is unique on the join keys.
fn rewritten_join_type(
    join: &Join,
    visible_left: &LiveColumns,
    visible_right: &LiveColumns,
    duplicate_insensitive: bool,
) -> JoinType {
    if join.join_type != JoinType::Inner || join.on.is_empty() {
        return join.join_type;
    }

    let can_remove_right = duplicate_insensitive
        || side_unique_on_join(
            join.right.schema(),
            join.on.iter().map(|(_, right)| right),
            join.null_equality,
        );
    if visible_right.is_empty() && can_remove_right {
        return JoinType::LeftSemi;
    }

    let can_remove_left = duplicate_insensitive
        || side_unique_on_join(
            join.left.schema(),
            join.on.iter().map(|(left, _)| left),
            join.null_equality,
        );
    if visible_left.is_empty() && can_remove_left {
        return JoinType::RightSemi;
    }

    JoinType::Inner
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
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::eliminate_join::EliminateJoin;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{
        Constraint, Constraints, NullEquality, Result, ScalarValue, SplitPoint,
    };
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::{
        Expr, JoinType, Partitioning, RangePartitioning, col, exists, lit,
        logical_plan::builder::{
            LogicalPlanBuilder, table_scan, table_source_with_constraints,
        },
        out_ref_col,
    };
    use datafusion_functions_aggregate::expr_fn::count;
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
    fn aggregate_with_aggregates_is_not_duplicate_insensitive() -> Result<()> {
        // A `GROUP BY` *with* aggregate functions observes how many rows fall in
        // each group, so its input is not duplicate-insensitive. With a non-unique
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
        // The aggregate makes the parent duplicate-insensitive, so absent any
        // other use of the right side the join would collapse to a semi join.
        // But the `EXISTS` subquery correlates on `r.y`, so the right side is
        // still needed and the join must stay an inner join. Otherwise the
        // semi join would drop `r`, orphaning the correlated `r.y` reference.
        let subquery =
            LogicalPlanBuilder::from(scan("s", &test_schema(), Constraints::default())?)
                .filter(col("s.id").eq(out_ref_col(DataType::Int32, "r.y")))?
                .project(vec![lit(1)])?
                .build()?;

        let plan = left_join_right()?
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
    ) -> Result<datafusion_expr::logical_plan::LogicalPlan> {
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
