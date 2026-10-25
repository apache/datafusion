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

use std::collections::HashMap;
use std::sync::Arc;

use crate::decorrelate::{
    PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR, build_join_filter,
};
use crate::optimizer::ApplyOrder;
use crate::utils::evaluates_to_null;
use crate::{OptimizerConfig, OptimizerRule};

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result, ScalarValue, assert_or_internal_err, plan_err};
use datafusion_expr::expr::{BinaryExpr, Sort};
use datafusion_expr::expr_rewriter::{
    create_col_from_scalar_expr, strip_outer_reference,
};
use datafusion_expr::logical_plan::{
    AsOfJoin, AsOfMatch, FetchType, Filter, JoinConstraint, JoinType, Projection,
    SkipType, Subquery,
};
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, Operator, lit, not, when,
};

/// Optimizer rule that rewrites scalar subquery filters to joins and places an
/// additional projection on top of the filter to preserve the original schema.
///
/// When [`datafusion_common::config::OptimizerOptions::enable_physical_uncorrelated_scalar_subquery`] is
/// true (the default), only *correlated* scalar subqueries are rewritten here;
/// uncorrelated ones are left for physical execution via `ScalarSubqueryExec`.
/// When the option is false, all scalar subqueries — correlated and
/// uncorrelated — are rewritten to left joins by this rule.
#[derive(Default, Debug)]
pub struct ScalarSubqueryToJoin {}

impl ScalarSubqueryToJoin {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Finds expressions that contain correlated scalar subqueries (and
    /// recurses when found).
    ///
    /// # Arguments
    /// * `predicate` - A conjunction to split and search.
    /// * `alias_gen` - Generator used to produce unique aliases for each
    ///   extracted scalar subquery (e.g. `__scalar_sq_1`, `__scalar_sq_2`).
    ///   Each subquery is replaced by a column reference using the generated
    ///   alias, and the same alias is later used to construct the join.
    ///
    /// Returns a tuple (subqueries, alias)
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        alias_gen: &Arc<AliasGenerator>,
        physical_uncorrelated: bool,
    ) -> Result<(Vec<(Subquery, String)>, Expr)> {
        let mut extract = ExtractScalarSubQuery {
            sub_query_info: vec![],
            alias_gen,
            physical_uncorrelated,
        };
        predicate
            .clone()
            .rewrite(&mut extract)
            .data()
            .map(|new_expr| (extract.sub_query_info, new_expr))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let physical_uncorrelated = config
                    .options()
                    .optimizer
                    .enable_physical_uncorrelated_scalar_subquery;
                // Optimization: skip the rest of the rule and its copies if
                // there are no scalar subqueries this rule should rewrite
                if !contains_scalar_subquery_to_rewrite(
                    &filter.predicate,
                    physical_uncorrelated,
                ) {
                    return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                }

                let (subqueries, mut rewrite_expr) = self.extract_subquery_exprs(
                    &filter.predicate,
                    config.alias_generator(),
                    physical_uncorrelated,
                )?;

                assert_or_internal_err!(
                    !subqueries.is_empty(),
                    "Expected subqueries not found in filter"
                );

                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = filter.input.as_ref().clone();
                for (subquery, alias) in subqueries {
                    if let Some((optimized_subquery, compensation_exprs)) =
                        build_join(&subquery, &cur_input, &alias, config)?
                    {
                        if !compensation_exprs.is_empty() {
                            rewrite_expr = rewrite_expr
                                .transform_up(|expr| {
                                    if let Some(compensation_expr) = expr
                                        .try_as_col()
                                        .and_then(|col| compensation_exprs.get(col))
                                    {
                                        Ok(Transformed::yes(compensation_expr.clone()))
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

                // Preserve original schema as new Join might have more fields than what Filter & parents expect.
                let projection =
                    filter.input.schema().columns().into_iter().map(Expr::from);
                let new_plan = LogicalPlanBuilder::from(cur_input)
                    .filter(rewrite_expr)?
                    .project(projection)?
                    .build()?;
                Ok(Transformed::yes(new_plan))
            }
            LogicalPlan::Projection(projection) => {
                let physical_uncorrelated = config
                    .options()
                    .optimizer
                    .enable_physical_uncorrelated_scalar_subquery;
                // Optimization: skip the rest of the rule and its copies if there
                // are no scalar subqueries this rule should rewrite
                if !projection.expr.iter().any(|expr| {
                    contains_scalar_subquery_to_rewrite(expr, physical_uncorrelated)
                }) {
                    return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                }

                let mut all_subqueries = vec![];
                let mut alias_to_index: HashMap<String, usize> = HashMap::new();
                let mut rewrite_exprs: Vec<Expr> =
                    Vec::with_capacity(projection.expr.len());
                for (idx, expr) in projection.expr.iter().enumerate() {
                    let (subqueries, rewrite_expr) = self.extract_subquery_exprs(
                        expr,
                        config.alias_generator(),
                        physical_uncorrelated,
                    )?;
                    for (_, alias) in &subqueries {
                        alias_to_index.insert(alias.clone(), idx);
                    }
                    all_subqueries.extend(subqueries);
                    rewrite_exprs.push(rewrite_expr);
                }
                assert_or_internal_err!(
                    !all_subqueries.is_empty(),
                    "Expected subqueries not found in projection"
                );
                // iterate through all subqueries in predicate, turning each into a left join
                let mut cur_input = projection.input.as_ref().clone();
                for (subquery, alias) in all_subqueries {
                    if let Some((optimized_subquery, compensation_exprs)) =
                        build_join(&subquery, &cur_input, &alias, config)?
                    {
                        cur_input = optimized_subquery;
                        if !compensation_exprs.is_empty()
                            && let Some(&idx) = alias_to_index.get(&alias)
                        {
                            let new_expr = rewrite_exprs[idx]
                                .clone()
                                .transform_up(|expr| {
                                    if let Some(compensation_expr) = expr
                                        .try_as_col()
                                        .and_then(|col| compensation_exprs.get(col))
                                    {
                                        Ok(Transformed::yes(compensation_expr.clone()))
                                    } else {
                                        Ok(Transformed::no(expr))
                                    }
                                })
                                .data()?;
                            rewrite_exprs[idx] = new_expr;
                        }
                    } else {
                        // if we can't handle all of the subqueries then bail for now
                        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                    }
                }

                let mut proj_exprs = Vec::with_capacity(projection.expr.len());
                for (expr, new_expr) in projection.expr.iter().zip(rewrite_exprs) {
                    let old_expr_name = expr.schema_name().to_string();
                    let new_expr_name = new_expr.schema_name().to_string();
                    if new_expr_name != old_expr_name {
                        proj_exprs.push(new_expr.alias(old_expr_name))
                    } else {
                        proj_exprs.push(new_expr);
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

/// Returns true if the expression contains a scalar subquery that this rule
/// should rewrite to a join.
///
/// When `enable_physical_uncorrelated_scalar_subquery` is true (the default) only
/// correlated scalar subqueries are rewritten — uncorrelated ones are handled
/// by the physical planner via `ScalarSubqueryExec`. When it is false, all
/// scalar subqueries (correlated and uncorrelated) are rewritten.
fn contains_scalar_subquery_to_rewrite(expr: &Expr, physical_uncorrelated: bool) -> bool {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(sq)
                if !physical_uncorrelated || !sq.outer_ref_columns.is_empty()
        ))
    })
    .expect("Inner is always Ok")
}

struct ExtractScalarSubQuery<'a> {
    sub_query_info: Vec<(Subquery, String)>,
    alias_gen: &'a Arc<AliasGenerator>,
    physical_uncorrelated: bool,
}

impl TreeNodeRewriter for ExtractScalarSubQuery<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            // Match scalar subqueries this rule should rewrite to a join. When
            // `physical_uncorrelated` is true, only correlated subqueries are
            // rewritten — uncorrelated ones are handled later by the physical
            // planner. When false, both are rewritten.
            Expr::ScalarSubquery(ref subquery)
                if !self.physical_uncorrelated
                    || !subquery.outer_ref_columns.is_empty() =>
            {
                let subquery = subquery.clone();
                let scalar_expr = subquery
                    .subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), Ok)?;
                let subqry_alias = self.alias_gen.next("__scalar_sq");
                let col =
                    create_col_from_scalar_expr(&scalar_expr, subqry_alias.clone())?;
                self.sub_query_info.push((subquery, subqry_alias));
                Ok(Transformed::new(
                    Expr::Column(col),
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
/// left join (select c_id, avg(total) from orders group by c_id) o
/// on o.c_id = c.id
/// where c.balance > o."avg(total)"
/// ```
///
/// When [`datafusion_common::config::OptimizerOptions::enable_physical_uncorrelated_scalar_subquery`] is
/// false, this function also handles uncorrelated scalar subqueries, rewriting
/// them as a `Left Join: Filter: Boolean(true)` instead of leaving them for
/// `ScalarSubqueryExec`.
///
/// # Arguments
///
/// * `subquery` - The scalar subquery to rewrite (correlated, or uncorrelated
///   when `enable_physical_uncorrelated_scalar_subquery` is false).
/// * `outer_input` - The outer plan that the decorrelated subquery is
///   left-joined onto — the input of the `Filter` or `Projection` node
///   that contained the subquery.
/// * `subquery_alias` - The unique alias assigned to the decorrelated
///   subquery; used both to qualify the join condition and to produce
///   column references for the caller to substitute.
///
/// Returns `Ok(None)` if the subquery cannot be decorrelated. On success,
/// returns the rewritten outer plan and a map from each count-bug-affected
/// column to its `CASE WHEN __always_true IS NULL THEN ... END` compensation
/// expression, which the caller must substitute into any expression that
/// references those columns.
fn build_join(
    subquery: &Subquery,
    outer_input: &LogicalPlan,
    subquery_alias: &str,
    config: &dyn OptimizerConfig,
) -> Result<Option<(LogicalPlan, HashMap<Column, Expr>)>> {
    // `build_join` also handles uncorrelated scalar subqueries (as a left
    // join with `Boolean(true)`) when the
    // `enable_physical_uncorrelated_scalar_subquery` option is disabled.
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let decorrelated_subquery = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return build_top1_join(subquery, outer_input, subquery_alias, config);
    }

    let collected_count_expr_map = pull_up
        .collected_count_expr_map
        .get(&decorrelated_subquery)
        .cloned();
    let aliased_subquery = LogicalPlanBuilder::from(decorrelated_subquery)
        .alias(subquery_alias.to_string())?
        .build()?;

    let join_filter_opt = build_join_filter(
        pull_up.join_filters,
        pull_up.correlated_subquery_cols_map.values().flatten(),
        subquery_alias,
    )?;

    // When pull-up did not extract any usable join keys (a correlated subquery
    // whose predicate references only outer columns), fall back to `ON true`:
    // the decorrelated subquery still yields at most one row per outer row
    // because its aggregate is grouped by the (empty) set of correlated inner
    // columns.
    let join_filter = join_filter_opt.or_else(|| Some(lit(true)));

    let new_plan = LogicalPlanBuilder::from(outer_input.clone())
        .join_on(aliased_subquery, JoinType::Left, join_filter)?
        .build()?;

    // Add count-bug compensation for each of the subquery's projected
    // expressions that yield non-NULL values on empty input. We wrap each
    // such expression in a CASE that substitutes the empty-input value
    // when the LEFT JOIN produced synthetic right-side NULLs (no inner
    // row matched), and uses the actual right-side value (which may
    // itself be NULL) otherwise.
    let mut compensation_exprs = HashMap::new();
    if let Some(expr_map) = collected_count_expr_map {
        // No session timezone: this builds a *searched* `CASE WHEN`, so the
        // `CASE expr WHEN` comparison rule never applies, and the only type
        // introduced is the untyped `Null` of the HAVING arm. The expressions
        // themselves come from a plan `TypeCoercion` has already coerced.
        let mut expr_rewrite = TypeCoercionRewriter::new(new_plan.schema());
        let having_arm = pull_up
            .pull_up_having_expr
            .as_ref()
            .map(|f| (not(f.clone()), lit(ScalarValue::Null)));
        for (name, result) in expr_map {
            if evaluates_to_null(result.clone(), result.column_refs())? {
                // Aggregates whose empty-input value is NULL (max/min/sum/…)
                // need no compensation: the LEFT JOIN already produces NULL
                // for unmatched outer rows.
                continue;
            }

            let indicator_col =
                Column::new(Some(subquery_alias), UN_MATCHED_ROW_INDICATOR);
            // Qualify with the subquery alias to avoid ambiguity when the
            // outer table has a column with the same name as the aggregate.
            let value_col = Column::new(Some(subquery_alias), name);

            let mut builder = when(Expr::Column(indicator_col).is_null(), result);
            if let Some((when_expr, then_expr)) = &having_arm {
                builder = builder.when(when_expr.clone(), then_expr.clone());
            }
            let compensation_expr = builder.otherwise(Expr::Column(value_col.clone()))?;
            compensation_exprs.insert(
                value_col,
                compensation_expr.rewrite(&mut expr_rewrite).data()?,
            );
        }
    }

    Ok(Some((new_plan, compensation_exprs)))
}

/// Rewrites a correlated scalar subquery whose plan selects one row per outer
/// row via `ORDER BY ... LIMIT 1` — the shape [`PullUpCorrelatedExpr`] cannot
/// handle because a `Sort` (or `Limit`) sits between the subquery head and the
/// correlated filter.
///
/// Two rewrites, both preserving the scalar-subquery contract of at most one
/// row per outer row:
///
/// * **ASOF join**: equalities plus a single range predicate on the (only)
///   sort key, in the direction the sort picks, e.g. `ORDER BY k DESC` over
///   `inner.k <= outer.v` becomes `ASOF JOIN ... MATCH_CONDITION outer.v >=
///   inner.k`, which returns the greatest `k <= v` per outer row.
/// * **`first_value` + LEFT JOIN**: equalities only; the subquery becomes an
///   aggregate grouped by the equality inner columns computing
///   `first_value(<output column> ORDER BY <sort keys>)`, left-joined on the
///   equality keys.
///
/// Correlated predicates the rewrites cannot express (e.g. a range not on the
/// sort key, `!=`, or `OR`) yield `Ok(None)` and the subquery is left as-is.
fn build_top1_join(
    subquery: &Subquery,
    outer_input: &LogicalPlan,
    subquery_alias: &str,
    config: &dyn OptimizerConfig,
) -> Result<Option<(LogicalPlan, HashMap<Column, Expr>)>> {
    // `LIMIT 1` (skip 0) on top of the subquery.
    let LogicalPlan::Limit(limit) = subquery.subquery.as_ref() else {
        return Ok(None);
    };
    if !matches!(limit.get_skip_type()?, SkipType::Literal(0))
        || !matches!(limit.get_fetch_type()?, FetchType::Literal(Some(1)))
    {
        return Ok(None);
    }

    // Zero or more projections may sit between the Limit and the Sort (the
    // output projection is elided when the output column is the sort key).
    let mut above: Vec<&Projection> = vec![];
    let mut node = limit.input.as_ref();
    while let LogicalPlan::Projection(p) = node {
        above.push(p);
        node = p.input.as_ref();
    }
    let LogicalPlan::Sort(sort) = node else {
        return Ok(None);
    };

    // Sort keys carrying outer references (e.g. `ORDER BY outer.x`) cannot be
    // moved to the decorrelated side.
    if sort.expr.iter().any(|s| s.expr.contains_outer()) {
        return Ok(None);
    }

    // Projection(s) below the Sort (the planner projects the sort keys before
    // sorting). Resolve columns referenced from the Sort's input schema back
    // to the base subtree's columns.
    let mut projs: Vec<&Projection> = vec![];
    let mut base = sort.input.as_ref();
    while let LogicalPlan::Projection(p) = base {
        projs.push(p);
        base = p.input.as_ref();
    }

    // Resolves a column through `projs` (top to bottom); returns `None` for
    // projections that compute rather than pass the column through.
    fn resolve_col(col: &Column, projs: &[&Projection]) -> Option<Column> {
        let mut cur = col.clone();
        for p in projs {
            let idx = p.schema.index_of_column(&cur).ok()?;
            let Expr::Column(c) = &p.expr[idx] else {
                return None;
            };
            cur = c.clone();
        }
        Some(cur)
    }

    // Rewrites every column reference in `expr` through `projs`.
    let resolve_expr = |expr: &Expr| -> Option<Expr> {
        expr.clone()
            .transform(|e| match e {
                Expr::Column(c) => match resolve_col(&c, &projs) {
                    Some(rc) => Ok(Transformed::yes(Expr::Column(rc))),
                    None => plan_err!("unsupported expression in subquery projection"),
                },
                _ => Ok(Transformed::no(e)),
            })
            .map(|t| t.data)
            .ok()
    };

    // Every conjunct that mentions an outer reference must be a top-level
    // conjunct of a `Filter` inside the base subtree. Outer references
    // anywhere else (join keys, projections, nested plans) cannot be
    // re-expressed as join keys.
    let mut correlated: Vec<Expr> = vec![];
    let mut unhandled = false;
    base.apply(|plan| {
        Ok(match plan {
            // A nested subquery keeps its own correlation scope.
            LogicalPlan::Subquery(_) => TreeNodeRecursion::Jump,
            LogicalPlan::Filter(filter) => {
                for conjunct in split_conjunction(&filter.predicate) {
                    if conjunct.contains_outer() {
                        correlated.push(conjunct.clone());
                    }
                }
                TreeNodeRecursion::Continue
            }
            _ if plan.contains_outer_reference() => {
                unhandled = true;
                TreeNodeRecursion::Stop
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    if unhandled || correlated.is_empty() {
        return Ok(None);
    }

    // The scalar output column — the first field of the subquery's output
    // schema — must resolve through every projection to a plain column of the
    // base subtree.
    let Some(output_col) = subquery.subquery.schema().columns().into_iter().next() else {
        return Ok(None);
    };
    let all_projs: Vec<&Projection> =
        above.iter().copied().chain(projs.iter().copied()).collect();
    let Some(inner_output_col) = resolve_col(&output_col, &all_projs) else {
        return Ok(None);
    };

    // Classify each correlated conjunct as an equality key or a range
    // predicate, normalized so the inner side is on the left.
    let mut eqs: Vec<(Column, Expr)> = vec![];
    let mut ranges: Vec<(Column, Operator, Expr)> = vec![];
    for conjunct in &correlated {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = conjunct else {
            return Ok(None);
        };
        let (inner, op, outer) = match (left.contains_outer(), right.contains_outer()) {
            (false, true) => (left.as_ref(), *op, right.as_ref()),
            (true, false) => {
                let Some(swapped) = op.swap() else {
                    return Ok(None);
                };
                (right.as_ref(), swapped, left.as_ref())
            }
            _ => return Ok(None),
        };
        let Expr::Column(inner_col) = inner else {
            return Ok(None);
        };
        let outer_expr = strip_outer_reference(outer.clone());
        // ASOF requires deterministic keys that reference the outer input.
        if outer_expr.is_volatile() || outer_expr.column_refs().is_empty() {
            return Ok(None);
        }
        match op {
            Operator::Eq => eqs.push((inner_col.clone(), outer_expr)),
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                ranges.push((inner_col.clone(), op, outer_expr))
            }
            _ => return Ok(None),
        }
    }

    // Rebuild the base subtree with the correlated conjuncts removed; filters
    // left with no predicate are dropped.
    let inner_plan = base
        .clone()
        .transform_up(|plan| match plan {
            LogicalPlan::Filter(f) => {
                let remaining = split_conjunction(&f.predicate)
                    .into_iter()
                    .filter(|e| !e.contains_outer())
                    .cloned();
                match conjunction(remaining) {
                    Some(predicate) => Ok(Transformed::yes(LogicalPlan::Filter(
                        Filter::try_new(predicate, f.input)?,
                    ))),
                    None => Ok(Transformed::yes((*f.input).clone())),
                }
            }
            _ => Ok(Transformed::no(plan)),
        })
        .data()?;

    let aliased = |c: &Column| Column::new(Some(subquery_alias), &c.name);

    // The decorrelated side is re-qualified under `subquery_alias`, so every
    // column it must provide has to have a unique unqualified name there.
    let mut needed_cols: Vec<Column> = vec![inner_output_col.clone()];
    for inner_col in eqs
        .iter()
        .map(|(inner_col, _)| inner_col)
        .chain(ranges.iter().map(|(range_col, _, _)| range_col))
    {
        if !needed_cols.contains(inner_col) {
            needed_cols.push(inner_col.clone());
        }
    }
    let mut needed_names: Vec<&str> =
        needed_cols.iter().map(|c| c.name.as_str()).collect();
    needed_names.sort_unstable();
    needed_names.dedup();
    if needed_names.len() != needed_cols.len() {
        return Ok(None);
    }

    // ASOF path: the single range predicate must be on the only sort key and
    // bound it in the direction the sort picks (`ORDER BY k DESC` picks the
    // greatest `k <= v`; `ASC` the least `k >= v`).
    if ranges.len() == 1 && sort.expr.len() == 1 {
        let (range_col, range_op, range_outer) = &ranges[0];
        let sort_key = &sort.expr[0];
        let direction_matches = if sort_key.asc {
            matches!(range_op, Operator::Gt | Operator::GtEq)
        } else {
            matches!(range_op, Operator::Lt | Operator::LtEq)
        };
        // The physical ASOF join additionally requires hashable,
        // non-floating equality key types.
        let eq_types_ok = eqs.iter().all(|(inner_col, _)| {
            inner_plan
                .schema()
                .qualified_field_from_column(inner_col)
                .map(|(_, f)| {
                    let t = f.data_type();
                    !t.is_floating() && datafusion_expr::utils::can_hash(t)
                })
                .unwrap_or(false)
        });
        if direction_matches
            && eq_types_ok
            && let Expr::Column(sort_col) = &sort_key.expr
            && resolve_col(sort_col, &projs).as_ref() == Some(range_col)
        {
            // Project the output and key columns of the base subtree; the
            // extracted scalar expression already references
            // `subquery_alias.<name>`.
            let right_plan = LogicalPlanBuilder::from(inner_plan)
                .project(needed_cols.iter().cloned().map(Expr::Column))?
                .alias(subquery_alias.to_string())?
                .build()?;

            let on: Vec<(Expr, Expr)> = eqs
                .iter()
                .map(|(inner_col, outer)| {
                    (outer.clone(), Expr::Column(aliased(inner_col)))
                })
                .collect();
            // The stored op is normalized with the inner side on the left; the
            // match condition puts the outer (left-input) expression first.
            let match_op = range_op
                .swap()
                .expect("comparison operators always have a flipped form");
            let asof = AsOfJoin::try_new(
                Arc::new(outer_input.clone()),
                Arc::new(right_plan),
                on,
                AsOfMatch::new(
                    range_outer.clone(),
                    match_op,
                    Expr::Column(aliased(range_col)),
                ),
                JoinConstraint::On,
            )?;
            return Ok(Some((LogicalPlan::AsOfJoin(asof), HashMap::new())));
        }
    }

    // `first_value` path: equality-only correlation.
    if ranges.is_empty() && !eqs.is_empty() {
        let order_by = sort
            .expr
            .iter()
            .map(|s| {
                resolve_expr(&s.expr).map(|expr| Sort {
                    expr,
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
            })
            .collect::<Option<Vec<_>>>();

        if let Some(order_by) = order_by {
            // The aggregate output field must carry the output column's
            // (unqualified) name so the extracted `subquery_alias.<name>`
            // reference resolves; group keys keep their own names.
            let mut group_cols: Vec<Column> = vec![];
            for inner_col in eqs.iter().map(|(inner_col, _)| inner_col) {
                if !group_cols.contains(inner_col) {
                    group_cols.push(inner_col.clone());
                }
            }
            // When the output column is itself an equality key its value is
            // already the group's key, so no aggregate is needed — and
            // aliasing one to the same name would make `subquery_alias.<name>`
            // ambiguous.
            let aggr_exprs = if group_cols.contains(&inner_output_col) {
                Vec::<Expr>::new()
            } else {
                // Resolved through the session's function registry rather
                // than a direct dependency on the functions crates.
                let Some(registry) = config.function_registry() else {
                    return Ok(None);
                };
                let Ok(first_value_udaf) = registry.udaf("first_value") else {
                    return Ok(None);
                };
                vec![
                    first_value_udaf
                        .call(vec![Expr::Column(inner_output_col.clone())])
                        .order_by(order_by)
                        .build()
                        // guaranteed to be `Expr::AggregateFunction`
                        .unwrap()
                        .alias(inner_output_col.name),
                ]
            };
            let right_plan = LogicalPlanBuilder::from(inner_plan)
                .aggregate(group_cols.iter().cloned().map(Expr::Column), aggr_exprs)?
                .alias(subquery_alias.to_string())?
                .build()?;

            let on_exprs: Vec<Expr> = eqs
                .iter()
                .map(|(inner_col, outer)| {
                    outer.clone().eq(Expr::Column(aliased(inner_col)))
                })
                .collect();
            let new_plan = LogicalPlanBuilder::from(outer_input.clone())
                .join_on(right_plan, JoinType::Left, on_exprs)?
                .build()?;
            return Ok(Some((new_plan, HashMap::new())));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use super::*;
    use crate::test::*;

    use arrow::datatypes::DataType;
    use datafusion_expr::test::function_stub::sum;

    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use datafusion_expr::{Between, col, expr, out_ref_col, rollup, scalar_subquery};
    use datafusion_functions_aggregate::min_max::{max, min};

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(ScalarSubqueryToJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    /// A correlated scalar subquery whose aggregate uses `ROLLUP` keeps its
    /// correlation: the empty set yields a row for outer rows the filter matches
    /// nothing for, and the join that would replace the filter cannot produce it.
    /// <https://github.com/apache/datafusion/issues/25519>
    #[test]
    fn scalar_subquery_with_rollup_is_not_decorrelated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(
                    vec![rollup(vec![col("orders.o_custkey")])],
                    vec![max(col("orders.o_custkey"))],
                )?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[ROLLUP (orders.o_custkey)]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64;N, __grouping_id:UInt8, max(orders.o_custkey):Int64;N]
                  Filter: orders.o_custkey = outer_ref(customer.c_custkey) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test multiple correlated subqueries
    #[test]
    fn multiple_subqueries() -> Result<()> {
        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                lit(1)
                    .lt(scalar_subquery(Arc::clone(&orders)))
                    .and(lit(1).lt(scalar_subquery(orders))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: Int32(1) < __scalar_sq_1.max(orders.o_custkey) AND Int32(1) < __scalar_sq_2.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: __scalar_sq_2.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                  TableScan: customer [c_custkey:Int64, c_name:Utf8]
                  SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                      Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                SubqueryAlias: __scalar_sq_2 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test recursive correlated subqueries
    #[test]
    fn recursive_subqueries() -> Result<()> {
        let lineitem = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("lineitem"))
                .filter(
                    col("lineitem.l_orderkey")
                        .eq(out_ref_col(DataType::Int64, "orders.o_orderkey")),
                )?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![sum(col("lineitem.l_extendedprice"))],
                )?
                .project(vec![sum(col("lineitem.l_extendedprice"))])?
                .build()?,
        );

        let orders = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                        .and(col("orders.o_totalprice").lt(scalar_subquery(lineitem))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("orders.o_totalprice"))])?
                .project(vec![sum(col("orders.o_totalprice"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_acctbal").lt(scalar_subquery(orders)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_acctbal < __scalar_sq_1.sum(orders.o_totalprice) [c_custkey:Int64, c_name:Utf8, sum(orders.o_totalprice):Float64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey [c_custkey:Int64, c_name:Utf8, sum(orders.o_totalprice):Float64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [sum(orders.o_totalprice):Float64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: sum(orders.o_totalprice), orders.o_custkey, __always_true [sum(orders.o_totalprice):Float64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[sum(orders.o_totalprice)]] [o_custkey:Int64, __always_true:Boolean, sum(orders.o_totalprice):Float64;N]
                      Projection: orders.o_orderkey, orders.o_custkey, orders.o_orderstatus, orders.o_totalprice [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        Filter: orders.o_totalprice < __scalar_sq_2.sum(lineitem.l_extendedprice) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N, __always_true:Boolean;N]
                          Left Join:  Filter: __scalar_sq_2.l_orderkey = orders.o_orderkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N, sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64;N, __always_true:Boolean;N]
                            TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                            SubqueryAlias: __scalar_sq_2 [sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64, __always_true:Boolean]
                              Projection: sum(lineitem.l_extendedprice), lineitem.l_orderkey, __always_true [sum(lineitem.l_extendedprice):Float64;N, l_orderkey:Int64, __always_true:Boolean]
                                Aggregate: groupBy=[[lineitem.l_orderkey, Boolean(true) AS __always_true]], aggr=[[sum(lineitem.l_extendedprice)]] [l_orderkey:Int64, __always_true:Boolean, sum(lineitem.l_extendedprice):Float64;N]
                                  TableScan: lineitem [l_orderkey:Int64, l_partkey:Int64, l_suppkey:Int64, l_linenumber:Int32, l_quantity:Float64, l_extendedprice:Float64]
        "
        )
    }

    /// Test for correlated scalar subquery filter with additional subquery filters
    #[test]
    fn scalar_subquery_with_subquery_filters() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .and(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      Filter: orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery with no columns in schema
    #[test]
    fn scalar_subquery_no_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // it will optimize, but fail for the same reason the unoptimized query would
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
              Left Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N]
                  Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for scalar subquery with both columns in schema
    #[test]
    fn scalar_subquery_with_no_correlated_cols() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("orders.o_custkey").eq(col("orders.o_custkey")))?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: orders.o_custkey = orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery not equal
    #[test]
    fn scalar_subquery_where_not_eq() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .not_eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) != orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery less than
    #[test]
    fn scalar_subquery_where_less_than() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .lt(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) < orders.o_custkey [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar subquery filter with subquery disjunction
    #[test]
    fn scalar_subquery_with_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey"))
                        .or(col("o_orderkey").eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // Unsupported predicate, subquery should not be decorrelated
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  Filter: outer_ref(customer.c_custkey) = orders.o_custkey OR orders.o_orderkey = Int32(1) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Test for correlated scalar without projection
    #[test]
    fn scalar_subquery_no_projection() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Error during planning: Scalar subquery should only return one column, but found 4: orders.o_orderkey, orders.o_custkey, orders.o_orderstatus, orders.o_totalprice";
        assert_analyzer_check_err(vec![], plan, expected);
        Ok(())
    }

    /// Test for correlated scalar expressions
    #[test]
    fn scalar_subquery_project_expr() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![col("max(orders.o_custkey)").add(lit(1))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) + Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey) + Int32(1), orders.o_custkey, __always_true [max(orders.o_custkey) + Int32(1):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery with non-strong project
    #[test]
    fn scalar_subquery_with_non_strong_project() -> Result<()> {
        let case = Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(col("max(orders.o_totalprice)")),
                Box::new(lit("a")),
            )],
            else_expr: Some(Box::new(lit("b"))),
        });

        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_totalprice"))])?
                .project(vec![case])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .project(vec![col("customer.c_custkey"), scalar_subquery(sq)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r#"
        Projection: customer.c_custkey, CASE WHEN __scalar_sq_1.__always_true IS NULL THEN CASE WHEN CAST(Float64(NULL) AS Boolean) THEN Utf8("a") ELSE Utf8("b") END ELSE __scalar_sq_1.CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END END AS CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END [c_custkey:Int64, CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8;N]
          Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8;N, o_custkey:Int64;N, __always_true:Boolean;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __scalar_sq_1 [CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8, o_custkey:Int64, __always_true:Boolean]
              Projection: CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END, orders.o_custkey, __always_true [CASE WHEN max(orders.o_totalprice) THEN Utf8("a") ELSE Utf8("b") END:Utf8, o_custkey:Int64, __always_true:Boolean]
                Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_totalprice)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_totalprice):Float64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "#
        )
    }

    /// Test for correlated scalar subquery multiple projected columns
    #[test]
    fn scalar_subquery_multi_col() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(col("customer.c_custkey").eq(col("orders.o_custkey")))?
                .project(vec![col("orders.o_custkey"), col("orders.o_orderkey")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let expected = "Error during planning: Scalar subquery should only return one column, but found 2: orders.o_custkey, orders.o_orderkey";
        assert_analyzer_check_err(vec![], plan, expected);
        Ok(())
    }

    /// Test for correlated scalar subquery filter with additional filters
    #[test]
    fn scalar_subquery_additional_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .gt_eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey >= __scalar_sq_1.max(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    #[test]
    fn scalar_subquery_additional_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .and(col("c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) AND customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery filter with disjunctions
    #[test]
    fn scalar_subquery_disjunction() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(
                col("customer.c_custkey")
                    .eq(scalar_subquery(sq))
                    .or(col("customer.c_custkey").eq(lit(1))),
            )?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) OR customer.c_custkey = Int32(1) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Test for correlated scalar subquery filter
    #[test]
    fn exists_subquery_correlated() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(test_table_scan_with_name("sq")?)
                .filter(out_ref_col(DataType::UInt32, "test.a").eq(col("sq.a")))?
                .aggregate(Vec::<Expr>::new(), vec![min(col("c"))])?
                .project(vec![min(col("c"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("test")?)
            .filter(col("test.c").lt(scalar_subquery(sq)))?
            .project(vec![col("test.c")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: test.c [c:UInt32]
          Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]
            Filter: test.c < __scalar_sq_1.min(sq.c) [a:UInt32, b:UInt32, c:UInt32, min(sq.c):UInt32;N, a:UInt32;N, __always_true:Boolean;N]
              Left Join:  Filter: test.a = __scalar_sq_1.a [a:UInt32, b:UInt32, c:UInt32, min(sq.c):UInt32;N, a:UInt32;N, __always_true:Boolean;N]
                TableScan: test [a:UInt32, b:UInt32, c:UInt32]
                SubqueryAlias: __scalar_sq_1 [min(sq.c):UInt32;N, a:UInt32, __always_true:Boolean]
                  Projection: min(sq.c), sq.a, __always_true [min(sq.c):UInt32;N, a:UInt32, __always_true:Boolean]
                    Aggregate: groupBy=[[sq.a, Boolean(true) AS __always_true]], aggr=[[min(sq.c)]] [a:UInt32, __always_true:Boolean, min(sq.c):UInt32;N]
                      TableScan: sq [a:UInt32, b:UInt32, c:UInt32]
        "
        )
    }

    /// Test for non-correlated scalar subquery with no filters
    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_non_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").lt(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey < (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn scalar_subquery_non_correlated_no_filters_with_equal_clause() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn correlated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    out_ref_col(DataType::Int64, "customer.c_custkey")
                        .eq(col("orders.o_custkey")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey BETWEEN __scalar_sq_1.min(orders.o_custkey) AND __scalar_sq_2.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
              Left Join:  Filter: customer.c_custkey = __scalar_sq_2.o_custkey [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N, max(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                Left Join:  Filter: customer.c_custkey = __scalar_sq_1.o_custkey [c_custkey:Int64, c_name:Utf8, min(orders.o_custkey):Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
                  TableScan: customer [c_custkey:Int64, c_name:Utf8]
                  SubqueryAlias: __scalar_sq_1 [min(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Projection: min(orders.o_custkey), orders.o_custkey, __always_true [min(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                      Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[min(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, min(orders.o_custkey):Int64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                SubqueryAlias: __scalar_sq_2 [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: max(orders.o_custkey), orders.o_custkey, __always_true [max(orders.o_custkey):Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey)]] [o_custkey:Int64, __always_true:Boolean, max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    #[test]
    fn uncorrelated_scalar_subquery_in_between_clause() -> Result<()> {
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![min(col("orders.o_custkey"))])?
                .project(vec![min(col("orders.o_custkey"))])?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let between_expr = Expr::Between(Between {
            expr: Box::new(col("customer.c_custkey")),
            negated: false,
            low: Box::new(scalar_subquery(sq1)),
            high: Box::new(scalar_subquery(sq2)),
        });

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(between_expr)?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey BETWEEN (<subquery>) AND (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [min(orders.o_custkey):Int64;N]
              Projection: min(orders.o_custkey) [min(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[min(orders.o_custkey)]] [min(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            Subquery: [max(orders.o_custkey):Int64;N]
              Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                  TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    #[test]
    fn uncorrelated_scalar_subquery_rewritten_when_flag_off() -> Result<()> {
        use datafusion_common::config::ConfigOptions;

        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .aggregate(Vec::<Expr>::new(), vec![max(col("orders.o_custkey"))])?
                .project(vec![max(col("orders.o_custkey"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        let mut options = ConfigOptions::default();
        options
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;
        let context = crate::OptimizerContext::new_with_config_options(Arc::new(options));

        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(ScalarSubqueryToJoin::new());
        let optimizer = crate::Optimizer::with_rules(vec![rule]);
        let optimized_plan = optimizer
            .optimize(plan, &context, |_, _| {})
            .expect("failed to optimize plan");
        let formatted_plan = optimized_plan.display_indent_schema();

        insta::assert_snapshot!(
            formatted_plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.max(orders.o_custkey) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
              Left Join:  Filter: Boolean(true) [c_custkey:Int64, c_name:Utf8, max(orders.o_custkey):Int64;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [max(orders.o_custkey):Int64;N]
                  Projection: max(orders.o_custkey) [max(orders.o_custkey):Int64;N]
                    Aggregate: groupBy=[[]], aggr=[[max(orders.o_custkey)]] [max(orders.o_custkey):Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        );

        Ok(())
    }

    #[test]
    fn correlated_scalar_subquery_filter_above_correlated_derived_table() -> Result<()> {
        let derived = LogicalPlanBuilder::from(scan_tpch_table("orders"))
            .filter(
                col("orders.o_custkey")
                    .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
            )?
            .aggregate(
                Vec::<Expr>::new(),
                vec![max(col("orders.o_custkey")).alias("n")],
            )?
            .project(vec![col("n")])?
            .alias("t")?
            .build()?;

        let sq = Arc::new(
            LogicalPlanBuilder::from(derived)
                .filter(out_ref_col(DataType::Int64, "customer.c_custkey").gt(lit(0i64)))?
                .project(vec![col("t.n")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .project(vec![col("customer.c_custkey"), scalar_subquery(sq)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey, __scalar_sq_1.n AS n [c_custkey:Int64, n:Int64;N]
          Left Join:  Filter: __scalar_sq_1.o_custkey = customer.c_custkey AND customer.c_custkey > Int64(0) [c_custkey:Int64, c_name:Utf8, n:Int64;N, o_custkey:Int64;N, __always_true:Boolean;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __scalar_sq_1 [n:Int64;N, o_custkey:Int64, __always_true:Boolean]
              Projection: t.n, t.o_custkey, t.__always_true [n:Int64;N, o_custkey:Int64, __always_true:Boolean]
                SubqueryAlias: t [n:Int64;N, o_custkey:Int64, __always_true:Boolean]
                  Projection: n, orders.o_custkey, __always_true [n:Int64;N, o_custkey:Int64, __always_true:Boolean]
                    Aggregate: groupBy=[[orders.o_custkey, Boolean(true) AS __always_true]], aggr=[[max(orders.o_custkey) AS n]] [o_custkey:Int64, __always_true:Boolean, n:Int64;N]
                      TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// Correlated scalar subquery with `ORDER BY sort_key DESC LIMIT 1` where
    /// the range predicate is on the sort key becomes an ASOF join.
    #[test]
    fn correlated_scalar_top1_asof() -> Result<()> {
        let sq =
            Arc::new(
                LogicalPlanBuilder::from(scan_tpch_table("orders"))
                    .filter(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                            .and(col("orders.o_orderkey").lt_eq(out_ref_col(
                                DataType::Int64,
                                "customer.c_custkey",
                            ))),
                    )?
                    .project(vec![
                        col("orders.o_orderstatus"),
                        col("orders.o_custkey"),
                        col("orders.o_orderkey"),
                    ])?
                    .sort(vec![col("orders.o_orderkey").sort(false, true)])?
                    .project(vec![col("orders.o_orderstatus")])?
                    .limit(0, Some(1))?
                    .build()?,
            );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Projection: customer.c_custkey, customer.c_name [c_custkey:Int64, c_name:Utf8]
            Filter: customer.c_custkey = __scalar_sq_1.o_orderstatus [c_custkey:Int64, c_name:Utf8, o_orderstatus:Utf8;N, o_custkey:Int64;N, o_orderkey:Int64;N]
              AsOf Join: match=[customer.c_custkey >= __scalar_sq_1.o_orderkey], constraint=On, on=[customer.c_custkey = __scalar_sq_1.o_custkey] [c_custkey:Int64, c_name:Utf8, o_orderstatus:Utf8;N, o_custkey:Int64;N, o_orderkey:Int64;N]
                TableScan: customer [c_custkey:Int64, c_name:Utf8]
                SubqueryAlias: __scalar_sq_1 [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                  Projection: orders.o_orderstatus, orders.o_custkey, orders.o_orderkey [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                    TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// The same `ORDER BY ... LIMIT 1` shape in a projection position.
    #[test]
    fn correlated_scalar_top1_asof_projection() -> Result<()> {
        let sq =
            Arc::new(
                LogicalPlanBuilder::from(scan_tpch_table("orders"))
                    .filter(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                            .and(col("orders.o_orderkey").lt_eq(out_ref_col(
                                DataType::Int64,
                                "customer.c_custkey",
                            ))),
                    )?
                    .project(vec![
                        col("orders.o_orderstatus"),
                        col("orders.o_custkey"),
                        col("orders.o_orderkey"),
                    ])?
                    .sort(vec![col("orders.o_orderkey").sort(false, true)])?
                    .project(vec![col("orders.o_orderstatus")])?
                    .limit(0, Some(1))?
                    .build()?,
            );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .project(vec![col("customer.c_custkey"), scalar_subquery(sq)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey, __scalar_sq_1.o_orderstatus AS o_orderstatus [c_custkey:Int64, o_orderstatus:Utf8;N]
          AsOf Join: match=[customer.c_custkey >= __scalar_sq_1.o_orderkey], constraint=On, on=[customer.c_custkey = __scalar_sq_1.o_custkey] [c_custkey:Int64, c_name:Utf8, o_orderstatus:Utf8;N, o_custkey:Int64;N, o_orderkey:Int64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
            SubqueryAlias: __scalar_sq_1 [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
              Projection: orders.o_orderstatus, orders.o_custkey, orders.o_orderkey [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
        "
        )
    }

    /// A range predicate that does not bound the sort key in the sorted
    /// direction is left alone.
    #[test]
    fn correlated_scalar_top1_wrong_direction() -> Result<()> {
        let sq =
            Arc::new(
                LogicalPlanBuilder::from(scan_tpch_table("orders"))
                    .filter(
                        col("orders.o_custkey")
                            .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                            .and(col("orders.o_orderkey").lt_eq(out_ref_col(
                                DataType::Int64,
                                "customer.c_custkey",
                            ))),
                    )?
                    .project(vec![
                        col("orders.o_orderstatus"),
                        col("orders.o_custkey"),
                        col("orders.o_orderkey"),
                    ])?
                    .sort(vec![col("orders.o_orderkey").sort(true, false)])?
                    .project(vec![col("orders.o_orderstatus")])?
                    .limit(0, Some(1))?
                    .build()?,
            );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        // `o_orderkey <= outer` with an ASC sort would pick the globally
        // smallest key, not an ASOF match; the subquery is left in place.
        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [o_orderstatus:Utf8]
              Limit: skip=0, fetch=1 [o_orderstatus:Utf8]
                Projection: orders.o_orderstatus [o_orderstatus:Utf8]
                  Sort: orders.o_orderkey ASC NULLS LAST [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                    Projection: orders.o_orderstatus, orders.o_custkey, orders.o_orderkey [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                      Filter: orders.o_custkey = outer_ref(customer.c_custkey) AND orders.o_orderkey <= outer_ref(customer.c_custkey) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// An equality-only `ORDER BY ... LIMIT 1` needs `first_value` from the
    /// function registry; without a registry the subquery is left in place.
    #[test]
    fn correlated_scalar_top1_eq_only_no_registry() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey")),
                )?
                .project(vec![
                    col("orders.o_orderstatus"),
                    col("orders.o_custkey"),
                    col("orders.o_orderkey"),
                ])?
                .sort(vec![
                    col("orders.o_orderkey").sort(false, true),
                    col("orders.o_orderstatus").sort(false, true),
                ])?
                .project(vec![col("orders.o_orderstatus")])?
                .limit(0, Some(1))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [o_orderstatus:Utf8]
              Limit: skip=0, fetch=1 [o_orderstatus:Utf8]
                Projection: orders.o_orderstatus [o_orderstatus:Utf8]
                  Sort: orders.o_orderkey DESC NULLS FIRST, orders.o_orderstatus DESC NULLS FIRST [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                    Projection: orders.o_orderstatus, orders.o_custkey, orders.o_orderkey [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                      Filter: orders.o_custkey = outer_ref(customer.c_custkey) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }

    /// Correlated predicates that are neither equalities nor ranges on the
    /// sort key cannot become join keys; the subquery is left in place.
    #[test]
    fn correlated_scalar_top1_unhandled_predicate() -> Result<()> {
        let sq = Arc::new(
            LogicalPlanBuilder::from(scan_tpch_table("orders"))
                .filter(
                    col("orders.o_custkey")
                        .eq(out_ref_col(DataType::Int64, "customer.c_custkey"))
                        .and(
                            col("orders.o_orderstatus")
                                .not_eq(out_ref_col(DataType::Utf8, "customer.c_name")),
                        ),
                )?
                .project(vec![
                    col("orders.o_orderstatus"),
                    col("orders.o_custkey"),
                    col("orders.o_orderkey"),
                ])?
                .sort(vec![col("orders.o_orderkey").sort(false, true)])?
                .project(vec![col("orders.o_orderstatus")])?
                .limit(0, Some(1))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(scan_tpch_table("customer"))
            .filter(col("customer.c_custkey").eq(scalar_subquery(sq)))?
            .project(vec![col("customer.c_custkey")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: customer.c_custkey [c_custkey:Int64]
          Filter: customer.c_custkey = (<subquery>) [c_custkey:Int64, c_name:Utf8]
            Subquery: [o_orderstatus:Utf8]
              Limit: skip=0, fetch=1 [o_orderstatus:Utf8]
                Projection: orders.o_orderstatus [o_orderstatus:Utf8]
                  Sort: orders.o_orderkey DESC NULLS FIRST [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                    Projection: orders.o_orderstatus, orders.o_custkey, orders.o_orderkey [o_orderstatus:Utf8, o_custkey:Int64, o_orderkey:Int64]
                      Filter: orders.o_custkey = outer_ref(customer.c_custkey) AND orders.o_orderstatus != outer_ref(customer.c_name) [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
                        TableScan: orders [o_orderkey:Int64, o_custkey:Int64, o_orderstatus:Utf8, o_totalprice:Float64;N]
            TableScan: customer [c_custkey:Int64, c_name:Utf8]
        "
        )
    }
}
