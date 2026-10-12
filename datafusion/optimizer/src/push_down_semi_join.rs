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

//! [`PushDownSemiJoin`] moves semi joins closer to the data they filter.
//!
//! Two rewrites are applied to `LeftSemi` / `LeftAnti` joins:
//!
//! 1. **Push below inner joins.** If every left-side column referenced by the
//!    semi join comes from one input of an inner join, the semi join is applied
//!    to that input instead, so it filters rows *before* the (larger) inner
//!    join runs:
//!
//!    ```text
//!    LeftSemi(Inner(A, B), S)  =>  Inner(LeftSemi(A, S), B)
//!    ```
//!
//! 2. **Restrict the aggregate of a decorrelated subquery** ("magic set").
//!    For `LeftSemi(L, Aggregate(group by k))` joined on `L.x = k`, groups whose
//!    `k` never appears in `L.x` can not match, so they need not be computed.
//!    If `L.x` originates from a cheap, filtered scan `F` (reachable from `L`
//!    through inner joins, filters and projections, so `F.x ⊇ L.x`), the
//!    aggregate input is semi joined with `F` first:
//!
//!    ```text
//!    LeftSemi(L, Aggregate(I, k))  =>  LeftSemi(L, Aggregate(LeftSemi(I, F on k = F.x), k))
//!    ```

use std::collections::HashSet;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, NullEquality, Result};
use datafusion_expr::logical_plan::{Join, JoinType, Projection, SubqueryAlias};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{Expr, LogicalPlan};

const MAGIC_ALIAS_PREFIX: &str = "__semi_keys";

/// See the [module level documentation](self).
#[derive(Default, Debug)]
pub struct PushDownSemiJoin {}

impl PushDownSemiJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownSemiJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn name(&self) -> &str {
        "push_down_semi_join"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_semi_join_pushdown {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            || join.null_aware
        {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let mut transformed = false;
        let mut join = join;
        if join.join_type == JoinType::LeftSemi
            && join.null_equality == NullEquality::NullEqualsNothing
            && let Some(new_right) = restrict_aggregate(&join, config)?
        {
            join = Join::try_new(
                Arc::clone(&join.left),
                Arc::new(new_right),
                join.on,
                join.filter,
                join.join_type,
                join.join_constraint,
                join.null_equality,
                join.null_aware,
            )?;
            transformed = true;
        }

        // Rewrite 1: push the semi join below an inner join on its left side
        let left_cols = left_referenced_columns(&join);
        let right = Arc::clone(&join.right);
        let make_semi = |input: LogicalPlan| -> Result<LogicalPlan> {
            Ok(LogicalPlan::Join(Join::try_new(
                Arc::new(input),
                Arc::clone(&right),
                join.on.clone(),
                join.filter.clone(),
                join.join_type,
                join.join_constraint,
                join.null_equality,
                join.null_aware,
            )?))
        };
        if let Some(pushed) = push_through(join.left.as_ref(), &left_cols, &make_semi)? {
            return Ok(Transformed::yes(pushed));
        }

        Ok(Transformed::new_transformed(
            LogicalPlan::Join(join),
            transformed,
        ))
    }
}

/// Columns of the join's left input referenced by the join keys and filter.
fn left_referenced_columns(join: &Join) -> HashSet<Column> {
    let left_schema = join.left.schema();
    let mut cols = HashSet::new();
    for (l, _) in &join.on {
        cols.extend(l.column_refs().into_iter().cloned());
    }
    if let Some(filter) = &join.filter {
        cols.extend(
            filter
                .column_refs()
                .into_iter()
                .filter(|c| left_schema.has_column(c))
                .cloned(),
        );
    }
    cols
}

/// Try to place the semi join (built by `make_semi`) below `plan`, as deep as
/// possible below inner joins. Returns `None` if `plan` can not be rewritten,
/// in which case the caller keeps the semi join above `plan`.
fn push_through(
    plan: &LogicalPlan,
    cols: &HashSet<Column>,
    make_semi: &dyn Fn(LogicalPlan) -> Result<LogicalPlan>,
) -> Result<Option<LogicalPlan>> {
    let pushed_input = match plan {
        LogicalPlan::Projection(p) => {
            let passes_through = cols.iter().all(|c| {
                p.expr
                    .iter()
                    .any(|e| matches!(e, Expr::Column(pc) if pc == c))
            });
            // Evaluating volatile expressions on fewer rows changes results
            if !passes_through || p.expr.iter().any(|e| e.is_volatile()) {
                return Ok(None);
            }
            push_through(p.input.as_ref(), cols, make_semi)?.map(|input| vec![input])
        }
        LogicalPlan::Filter(f) if !f.predicate.is_volatile() => {
            push_through(f.input.as_ref(), cols, make_semi)?.map(|input| vec![input])
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            let in_left = cols.iter().all(|c| j.left.schema().has_column(c));
            let in_right = cols.iter().all(|c| j.right.schema().has_column(c));
            if in_left == in_right {
                return Ok(None);
            }
            let (side, other) = if in_left {
                (&j.left, &j.right)
            } else {
                (&j.right, &j.left)
            };
            // If the other input is filtered, the inner join itself likely
            // removes rows, so the semi join is cheaper to evaluate above it.
            if reduces_rows(other.as_ref()) {
                return Ok(None);
            }
            let new_side = match push_through(side.as_ref(), cols, make_semi)? {
                Some(p) => p,
                None => make_semi(side.as_ref().clone())?,
            };
            Some(if in_left {
                vec![new_side, j.right.as_ref().clone()]
            } else {
                vec![j.left.as_ref().clone(), new_side]
            })
        }
        _ => None,
    };

    match pushed_input {
        Some(inputs) => Ok(Some(plan.with_new_exprs(plan.expressions(), inputs)?)),
        // Nothing to push through: the caller places the semi join on `plan`
        None => Ok(None),
    }
}

/// True if `plan` contains a filter or a semi / anti join, i.e. joining with it
/// is likely to remove rows.
fn reduces_rows(plan: &LogicalPlan) -> bool {
    plan.exists(|p| {
        Ok(match p {
            LogicalPlan::Filter(f) => is_selective(&f.predicate),
            LogicalPlan::TableScan(scan) => scan.filters.iter().any(is_selective),
            LogicalPlan::Join(j) => {
                matches!(j.join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            }
            _ => false,
        })
    })
    .unwrap_or(true)
}

/// False for predicates that only consist of `IS NOT NULL` checks, which other
/// rules add to join inputs and which rarely remove many rows.
fn is_selective(predicate: &Expr) -> bool {
    split_conjunction(predicate)
        .into_iter()
        .any(|e| !matches!(e, Expr::IsNotNull(_)))
}

/// Rewrite 2 (see module docs). Returns the new right input of the semi join.
fn restrict_aggregate(
    join: &Join,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    for (l, r) in &join.on {
        let (Expr::Column(lcol), Expr::Column(rcol)) = (l, r) else {
            continue;
        };
        let Some(source) = find_key_source(join.left.as_ref(), lcol) else {
            continue;
        };
        if let Some(new_right) =
            add_semi_below_aggregate(join.right.as_ref(), rcol, &source, lcol, config)?
        {
            return Ok(Some(new_right));
        }
    }
    Ok(None)
}

/// Find a cheap subtree (Filter/Projection over a TableScan, containing a
/// filter) whose values of `col` are a superset of the values of `col` in
/// `plan`.
fn find_key_source(plan: &LogicalPlan, col: &Column) -> Option<LogicalPlan> {
    // Prefer the outermost cheap subtree: it includes all of its filters
    if is_cheap_filtered(plan) {
        return Some(plan.clone());
    }
    match plan {
        LogicalPlan::Projection(p) => {
            if !p
                .expr
                .iter()
                .any(|e| matches!(e, Expr::Column(pc) if pc == col))
            {
                return None;
            }
            find_key_source(p.input.as_ref(), col)
        }
        LogicalPlan::Filter(f) => find_key_source(f.input.as_ref(), col),
        LogicalPlan::Join(j)
            if matches!(
                j.join_type,
                JoinType::Inner | JoinType::LeftSemi | JoinType::LeftAnti
            ) =>
        {
            if j.left.schema().has_column(col) {
                find_key_source(j.left.as_ref(), col)
            } else if j.join_type == JoinType::Inner && j.right.schema().has_column(col) {
                find_key_source(j.right.as_ref(), col)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// True if `plan` only consists of Filter / Projection / TableScan nodes and
/// filters at least some rows.
fn is_cheap_filtered(plan: &LogicalPlan) -> bool {
    fn walk(plan: &LogicalPlan, filtered: &mut bool) -> bool {
        match plan {
            // The subtree is evaluated a second time: it must be deterministic
            LogicalPlan::Filter(f) if !f.predicate.is_volatile() => {
                *filtered |= is_selective(&f.predicate);
                walk(f.input.as_ref(), filtered)
            }
            LogicalPlan::Projection(p) if !p.expr.iter().any(|e| e.is_volatile()) => {
                walk(p.input.as_ref(), filtered)
            }
            LogicalPlan::TableScan(_) => true,
            _ => false,
        }
    }
    let mut filtered = false;
    walk(plan, &mut filtered) && filtered
}

/// Walk down `plan` (SubqueryAlias / Projection / Filter) following `col` to an
/// Aggregate where it is a plain group column, and semi join the aggregate's
/// input with `source` on that column.
fn add_semi_below_aggregate(
    plan: &LogicalPlan,
    col: &Column,
    source: &LogicalPlan,
    source_col: &Column,
    config: &dyn OptimizerConfig,
) -> Result<Option<LogicalPlan>> {
    let Some(idx) = plan.schema().maybe_index_of_column(col) else {
        return Ok(None);
    };
    let new_input = match plan {
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            let child_col = input.schema().columns()[idx].clone();
            add_semi_below_aggregate(input, &child_col, source, source_col, config)?
        }
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let child_col = match &expr[idx] {
                Expr::Column(c) => c.clone(),
                Expr::Alias(a) => match a.expr.as_ref() {
                    Expr::Column(c) => c.clone(),
                    _ => return Ok(None),
                },
                _ => return Ok(None),
            };
            add_semi_below_aggregate(input, &child_col, source, source_col, config)?
        }
        LogicalPlan::Filter(f) => {
            add_semi_below_aggregate(&f.input, col, source, source_col, config)?
        }
        LogicalPlan::Aggregate(agg) => {
            if idx >= agg.group_expr.len() {
                return Ok(None);
            }
            let Expr::Column(group_col) = &agg.group_expr[idx] else {
                return Ok(None);
            };
            if already_restricted(agg.input.as_ref()) {
                return Ok(None);
            }
            let alias = config.alias_generator().next(MAGIC_ALIAS_PREFIX);
            let keys = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(LogicalPlan::Projection(Projection::try_new(
                    vec![Expr::Column(source_col.clone())],
                    Arc::new(source.clone()),
                )?)),
                alias.as_str(),
            )?);
            let key_col = Column::new(Some(alias.as_str()), source_col.name.clone());
            let semi = LogicalPlan::Join(Join::try_new(
                Arc::clone(&agg.input),
                Arc::new(keys),
                vec![(Expr::Column(group_col.clone()), Expr::Column(key_col))],
                None,
                JoinType::LeftSemi,
                datafusion_expr::JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?);
            return Ok(Some(plan.with_new_exprs(plan.expressions(), vec![semi])?));
        }
        _ => return Ok(None),
    };
    new_input
        .map(|input| plan.with_new_exprs(plan.expressions(), vec![input]))
        .transpose()
}

fn already_restricted(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Join(Join { join_type: JoinType::LeftSemi, right, .. })
            if matches!(right.as_ref(), LogicalPlan::SubqueryAlias(a)
                if a.alias.table().starts_with(MAGIC_ALIAS_PREFIX))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_functions_aggregate::expr_fn::sum;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(PushDownSemiJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    fn scan(name: &str) -> Result<LogicalPlanBuilder> {
        Ok(LogicalPlanBuilder::from(test_table_scan_with_name(name)?))
    }

    #[test]
    fn semi_join_pushed_below_inner_join() -> Result<()> {
        let plan = scan("t1")?
            .join(
                scan("t2")?.build()?,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t2.a"]),
                None,
            )?
            .join(
                scan("t3")?.build()?,
                JoinType::LeftSemi,
                (vec!["t1.b"], vec!["t3.b"]),
                None,
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        Inner Join: t1.a = t2.a
          LeftSemi Join: t1.b = t3.b
            TableScan: t1
            TableScan: t3
          TableScan: t2
        ")
    }

    #[test]
    fn anti_join_pushed_below_inner_join() -> Result<()> {
        let plan = scan("t1")?
            .join(
                scan("t2")?.build()?,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t2.a"]),
                None,
            )?
            .join(
                scan("t3")?.build()?,
                JoinType::LeftAnti,
                (vec!["t2.b"], vec!["t3.b"]),
                None,
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        Inner Join: t1.a = t2.a
          TableScan: t1
          LeftAnti Join: t2.b = t3.b
            TableScan: t2
            TableScan: t3
        ")
    }

    #[test]
    fn semi_join_not_pushed_below_filtering_inner_join() -> Result<()> {
        let filtered = scan("t2")?.filter(col("t2.c").eq(lit(1u32)))?.build()?;
        let plan = scan("t1")?
            .join(
                filtered,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t2.a"]),
                None,
            )?
            .join(
                scan("t3")?.build()?,
                JoinType::LeftSemi,
                (vec!["t1.b"], vec!["t3.b"]),
                None,
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        LeftSemi Join: t1.b = t3.b
          Inner Join: t1.a = t2.a
            TableScan: t1
            Filter: t2.c = UInt32(1)
              TableScan: t2
          TableScan: t3
        ")
    }

    #[test]
    fn semi_join_referencing_both_sides_not_pushed() -> Result<()> {
        let plan = scan("t1")?
            .join(
                scan("t2")?.build()?,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t2.a"]),
                None,
            )?
            .join(
                scan("t3")?.build()?,
                JoinType::LeftSemi,
                (vec!["t1.b"], vec!["t3.b"]),
                Some(col("t2.c").lt(col("t3.c"))),
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        LeftSemi Join: t1.b = t3.b Filter: t2.c < t3.c
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
          TableScan: t3
        ")
    }

    #[test]
    fn aggregate_restricted_to_filtered_outer_keys() -> Result<()> {
        let filtered = scan("t2")?.filter(col("t2.c").eq(lit(1u32)))?.build()?;
        let outer = scan("t1")?
            .join(
                filtered,
                JoinType::Inner,
                (vec!["t1.a"], vec!["t2.a"]),
                None,
            )?
            .build()?;
        let subquery = scan("t3")?
            .aggregate(vec![col("t3.a")], vec![sum(col("t3.b")).alias("total")])?
            .alias("sq")?
            .build()?;
        let plan = LogicalPlanBuilder::from(outer)
            .join(
                subquery,
                JoinType::LeftSemi,
                (vec!["t2.a"], vec!["sq.a"]),
                Some(col("t1.b").lt(col("sq.total"))),
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        LeftSemi Join: t2.a = sq.a Filter: t1.b < sq.total
          Inner Join: t1.a = t2.a
            TableScan: t1
            Filter: t2.c = UInt32(1)
              TableScan: t2
          SubqueryAlias: sq
            Aggregate: groupBy=[[t3.a]], aggr=[[sum(t3.b) AS total]]
              LeftSemi Join: t3.a = __semi_keys_1.a
                TableScan: t3
                SubqueryAlias: __semi_keys_1
                  Projection: t2.a
                    Filter: t2.c = UInt32(1)
                      TableScan: t2
        ")
    }

    #[test]
    fn aggregate_not_restricted_by_unfiltered_keys() -> Result<()> {
        let subquery = scan("t3")?
            .aggregate(vec![col("t3.a")], vec![sum(col("t3.b")).alias("total")])?
            .alias("sq")?
            .build()?;
        let plan = scan("t1")?
            .join(
                subquery,
                JoinType::LeftSemi,
                (vec!["t1.a"], vec!["sq.a"]),
                Some(col("t1.b").lt(col("sq.total"))),
            )?
            .build()?;
        assert_optimized_plan_equal!(plan, @"
        LeftSemi Join: t1.a = sq.a Filter: t1.b < sq.total
          TableScan: t1
          SubqueryAlias: sq
            Aggregate: groupBy=[[t3.a]], aggr=[[sum(t3.b) AS total]]
              TableScan: t3
        ")
    }
}
