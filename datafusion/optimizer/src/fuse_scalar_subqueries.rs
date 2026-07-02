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

//! [`FuseScalarSubqueries`] fuses multiple uncorrelated scalar-aggregate
//! subqueries over the same source into a single aggregate.
//!
//! A query that selects several scalar-aggregate subqueries over one table
//! scans that table once per subquery:
//!
//! ```sql
//! SELECT (SELECT count(*) FROM t WHERE a < 10),
//!        (SELECT avg(x)   FROM t WHERE a >= 10);
//! ```
//!
//! When two or more such subqueries share an identical source, this rule
//! collapses them into a single scan plus a single aggregate, pushing each
//! subquery's predicate into a `FILTER (WHERE ...)` clause:
//!
//! ```sql
//! SELECT count(*) FILTER (WHERE a < 10),
//!        avg(x)   FILTER (WHERE a >= 10)
//! FROM t;
//! ```
//!
//! The fused aggregate produces a single row that is cross-joined back into the
//! outer plan, and each scalar subquery is replaced by a reference to the
//! corresponding merged aggregate column.
//!
//! The rewrite is deliberately conservative. It only fires for uncorrelated
//! scalar aggregates over a structurally identical source and skips `DISTINCT`,
//! ordered, or volatile aggregates as well as predicates that contain
//! subqueries.

use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::{Aggregate, Expr, Filter, LogicalPlan, Projection, Subquery, lit};
use log::debug;

/// Prefix for the synthetic columns that carry the fused aggregate results.
const FUSED_AGGREGATE_PREFIX: &str = "__datafusion_fused_scalar_agg_";

/// Optimizer rule that fuses uncorrelated scalar-aggregate subqueries over the
/// same source into a single aggregate. See the [module] documentation for
/// details.
///
/// [module]: crate::fuse_scalar_subqueries
#[derive(Default, Debug)]
pub struct FuseScalarSubqueries;

impl FuseScalarSubqueries {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for FuseScalarSubqueries {
    fn name(&self) -> &str {
        "fuse_scalar_subqueries"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_fuse_scalar_subqueries {
            return Ok(Transformed::no(plan));
        }

        // Fast pre-check: the rule can only fire on a `Projection` whose
        // expressions contain at least one scalar subquery. Skip the expensive
        // bottom-up traversal otherwise.
        if !plan.exists(|p| {
            Ok(match p {
                LogicalPlan::Projection(projection) => {
                    projection.expr.iter().any(expr_contains_scalar_subquery)
                }
                _ => false,
            })
        })? {
            return Ok(Transformed::no(plan));
        }

        plan.rewrite_with_subqueries(&mut FuseScalarSubqueriesRewriter)
    }
}

struct FuseScalarSubqueriesRewriter;

impl TreeNodeRewriter for FuseScalarSubqueriesRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(_) = &plan else {
            return Ok(Transformed::no(plan));
        };
        match try_fuse_projection(plan.clone())? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// A scalar subquery candidate plus the aggregate branch extracted from it.
struct Candidate {
    /// The original `Expr::ScalarSubquery` node, used to locate replacements.
    scalar_subquery: Expr,
    branch: ScalarAggregateBranch,
}

/// The salient parts of a `Projection: ... -> Aggregate -> [Filter] -> source`
/// scalar-aggregate subquery.
struct ScalarAggregateBranch {
    /// The shared source under the (optional) filter.
    source: LogicalPlan,
    /// The branch predicate, or `true` when the branch has no filter.
    predicate: Expr,
    /// The single aggregate expression computed by the subquery.
    aggr_expr: Expr,
}

/// Attempt to fuse the scalar-aggregate subqueries in `plan` (a `Projection`).
fn try_fuse_projection(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(Projection {
        expr,
        input,
        schema,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    let candidates = collect_candidates(&expr)?;
    if candidates.len() < 2 {
        return Ok(None);
    }

    // All candidates must read from a structurally identical source.
    let first = &candidates[0];
    if !candidates
        .iter()
        .all(|candidate| candidate.branch.source == first.branch.source)
    {
        debug!("fuse_scalar_subqueries skipped: candidates do not share one source");
        return Ok(None);
    }

    // Scan the shared source once, keeping rows that satisfy any branch.
    let source = add_source_filter(
        first.branch.source.clone(),
        candidates.iter().map(|c| c.branch.predicate.clone()),
    )?;

    // Build one `FILTER (WHERE predicate)` aggregate per branch and remember
    // which synthetic column replaces each original scalar subquery.
    let mut merged_exprs = Vec::with_capacity(candidates.len());
    let mut replacements = Vec::with_capacity(candidates.len());
    for (idx, candidate) in candidates.iter().enumerate() {
        let alias = format!("{FUSED_AGGREGATE_PREFIX}{idx}");
        merged_exprs.push(
            add_aggregate_filter(
                candidate.branch.aggr_expr.clone(),
                &candidate.branch.predicate,
            )?
            .alias(&alias),
        );
        replacements.push((
            candidate.scalar_subquery.clone(),
            Expr::Column(Column::new_unqualified(alias)),
        ));
    }

    let merged = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(source),
        Vec::<Expr>::new(),
        merged_exprs,
    )?);

    // The single-row aggregate is cross-joined into the original input so the
    // outer expressions can reference the merged columns.
    let joined_input = LogicalPlanBuilder::from(Arc::unwrap_or_clone(input))
        .cross_join(merged)?
        .build()?;

    let rewritten_expr = expr
        .into_iter()
        .map(|expr| replace_scalar_subqueries(expr, &replacements))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(rewritten_expr, Arc::new(joined_input), schema)?,
    )))
}

/// Collect the uncorrelated scalar-aggregate subqueries referenced by `exprs`.
fn collect_candidates(exprs: &[Expr]) -> Result<Vec<Candidate>> {
    let mut candidates = vec![];
    for expr in exprs {
        expr.apply(|expr| {
            let Expr::ScalarSubquery(subquery) = expr else {
                return Ok(TreeNodeRecursion::Continue);
            };
            // Correlated subqueries are left to the decorrelation rules.
            if !subquery.outer_ref_columns.is_empty() {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(branch) = extract_scalar_aggregate(subquery)? {
                candidates.push(Candidate {
                    scalar_subquery: expr.clone(),
                    branch,
                });
            }
            Ok(TreeNodeRecursion::Jump)
        })?;
    }
    Ok(candidates)
}

/// Extract a single-aggregate, no-group-by branch from a scalar subquery,
/// returning `None` when the shape is unsupported.
fn extract_scalar_aggregate(
    subquery: &Subquery,
) -> Result<Option<ScalarAggregateBranch>> {
    let mut plan = (*subquery.subquery).clone();

    // A trailing projection of the lone aggregate column is safe to peel.
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = &plan {
        if expr.len() != 1 || !is_safe_passthrough_expr(&expr[0]) {
            return Ok(None);
        }
        plan = Arc::unwrap_or_clone(Arc::clone(input));
    }

    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    if !group_expr.is_empty() || aggr_expr.len() != 1 {
        return Ok(None);
    }
    if !is_mergeable_aggregate_expr(&aggr_expr[0]) {
        debug!("fuse_scalar_subqueries skipped: aggregate expression is unsafe");
        return Ok(None);
    }

    let (source, predicate) = match Arc::unwrap_or_clone(input) {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                debug!("fuse_scalar_subqueries skipped: branch predicate is unsafe");
                return Ok(None);
            }
            (Arc::unwrap_or_clone(input), predicate)
        }
        other => (other, lit(true)),
    };

    Ok(Some(ScalarAggregateBranch {
        source,
        predicate,
        aggr_expr: strip_outer_alias(aggr_expr.into_iter().next().unwrap()),
    }))
}

/// Build a single scan of `source` keeping rows that satisfy any branch
/// predicate. When every branch is unfiltered the source is returned as-is.
fn add_source_filter(
    source: LogicalPlan,
    predicates: impl IntoIterator<Item = Expr>,
) -> Result<LogicalPlan> {
    let predicate = predicates
        .into_iter()
        .filter(|expr| expr != &lit(true))
        .reduce(|left, right| left.or(right));

    match predicate {
        Some(predicate) => LogicalPlanBuilder::from(source).filter(predicate)?.build(),
        None => Ok(source),
    }
}

/// Attach `predicate` as a `FILTER (WHERE ...)` clause to every aggregate
/// function inside `aggr_expr`, conjoining with any existing filter.
fn add_aggregate_filter(aggr_expr: Expr, predicate: &Expr) -> Result<Expr> {
    if predicate == &lit(true) {
        return Ok(aggr_expr);
    }

    Ok(aggr_expr
        .transform(|expr| {
            let Expr::AggregateFunction(mut aggregate) = expr else {
                return Ok(Transformed::no(expr));
            };
            let filter = aggregate
                .params
                .filter
                .take()
                .map(|existing| existing.and(predicate.clone()))
                .unwrap_or_else(|| predicate.clone());
            aggregate.params.filter = Some(Box::new(filter));
            Ok(Transformed::yes(Expr::AggregateFunction(aggregate)))
        })?
        .data)
}

/// Replace any of the original scalar subqueries in `expr` with the merged
/// aggregate column that now carries its value.
fn replace_scalar_subqueries(expr: Expr, replacements: &[(Expr, Expr)]) -> Result<Expr> {
    Ok(expr
        .transform(|expr| {
            match replacements.iter().find(|(subquery, _)| subquery == &expr) {
                Some((_, replacement)) => Ok(Transformed::yes(replacement.clone())),
                None => Ok(Transformed::no(expr)),
            }
        })?
        .data)
}

fn strip_outer_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

/// An aggregate is mergeable when it is a plain (non-`DISTINCT`, unordered)
/// aggregate function with no volatile or subquery arguments.
fn is_mergeable_aggregate_expr(expr: &Expr) -> bool {
    if expr.is_volatile() || expr_contains_scalar_subquery(expr) {
        return false;
    }
    matches!(
        strip_outer_alias(expr.clone()),
        Expr::AggregateFunction(aggregate)
            if !aggregate.params.distinct && aggregate.params.order_by.is_empty()
    )
}

fn is_mergeable_predicate(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_scalar_subquery(expr)
}

fn is_safe_passthrough_expr(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_scalar_subquery(expr)
}

fn expr_contains_scalar_subquery(expr: &Expr) -> bool {
    expr.exists(|e| {
        Ok(matches!(
            e,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
        ))
    })
    .expect("boolean expression walk is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use crate::{OptimizerContext, OptimizerRule};
    use arrow::datatypes::DataType;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::test::function_stub::{avg, count, sum};
    use datafusion_expr::{
        ColumnarValue, ExprFunctionExt, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility, col, lit, out_ref_col, scalar_subquery,
    };

    macro_rules! assert_fused {
        ($plan:expr, @ $expected:literal $(,)?) => {{
            let mut options = ConfigOptions::default();
            options.optimizer.enable_fuse_scalar_subqueries = true;
            let optimizer_ctx =
                OptimizerContext::new_with_config_options(Arc::new(options))
                    .with_max_passes(1);
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(FuseScalarSubqueries::new())];
            assert_optimized_plan_eq_snapshot!(optimizer_ctx, rules, $plan, @ $expected,)
        }};
    }

    /// Build an uncorrelated scalar-aggregate subquery: `SELECT <agg> FROM t
    /// WHERE <predicate>`.
    fn scalar_agg(predicate: Expr, aggr: Expr) -> Result<Expr> {
        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(predicate)?
            .aggregate(Vec::<Expr>::new(), vec![aggr])?
            .build()?;
        Ok(scalar_subquery(Arc::new(plan)))
    }

    #[test]
    fn fuses_two_scalar_aggregates_over_same_source() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").lt(lit(10)), count(col("b")))?.alias("lo"),
                scalar_agg(col("a").gt_eq(lit(10)), avg(col("c")))?.alias("hi"),
            ])?
            .build()?;

        assert_fused!(plan, @r"
        Projection: __datafusion_fused_scalar_agg_0 AS lo, __datafusion_fused_scalar_agg_1 AS hi
          Cross Join:
            EmptyRelation: rows=1
            Aggregate: groupBy=[[]], aggr=[[COUNT(t.b) FILTER (WHERE t.a < Int32(10)) AS __datafusion_fused_scalar_agg_0, avg(t.c) FILTER (WHERE t.a >= Int32(10)) AS __datafusion_fused_scalar_agg_1]]
              Filter: t.a < Int32(10) OR t.a >= Int32(10)
                TableScan: t
        ")
    }

    #[test]
    fn preserves_existing_aggregate_filter() -> Result<()> {
        let agg_with_filter = sum(col("b")).filter(col("c").gt(lit(5))).build()?;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").gt(lit(1)), agg_with_filter)?.alias("x"),
                scalar_agg(col("a").lt(lit(10)), count(col("a")))?.alias("y"),
            ])?
            .build()?;

        assert_fused!(plan, @r"
        Projection: __datafusion_fused_scalar_agg_0 AS x, __datafusion_fused_scalar_agg_1 AS y
          Cross Join:
            EmptyRelation: rows=1
            Aggregate: groupBy=[[]], aggr=[[sum(t.b) FILTER (WHERE t.c > Int32(5) AND t.a > Int32(1)) AS __datafusion_fused_scalar_agg_0, COUNT(t.a) FILTER (WHERE t.a < Int32(10)) AS __datafusion_fused_scalar_agg_1]]
              Filter: t.a > Int32(1) OR t.a < Int32(10)
                TableScan: t
        ")
    }

    #[test]
    fn keeps_subqueries_over_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("b"))])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .aggregate(Vec::<Expr>::new(), vec![avg(col("c"))])?
            .build()?;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_subquery(Arc::new(left)).alias("x"),
                scalar_subquery(Arc::new(right)).alias("y"),
            ])?
            .build()?;

        // Different sources: left untouched.
        assert_fused!(plan, @r"
        Projection: (<subquery>) AS x, (<subquery>) AS y
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[sum(t1.b)]]
              TableScan: t1
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[avg(t2.c)]]
              TableScan: t2
          EmptyRelation: rows=1
        ")
    }

    #[test]
    fn keeps_single_scalar_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").lt(lit(10)), count(col("b")))?.alias("only"),
            ])?
            .build()?;

        // Fewer than two candidates: nothing to fuse.
        assert_fused!(plan, @r"
        Projection: (<subquery>) AS only
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[COUNT(t.b)]]
              Filter: t.a < Int32(10)
                TableScan: t
          EmptyRelation: rows=1
        ")
    }

    #[test]
    fn keeps_correlated_subqueries() -> Result<()> {
        let correlated = |pred: Expr, aggr: Expr| -> Result<Expr> {
            let plan = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
                .filter(pred)?
                .aggregate(Vec::<Expr>::new(), vec![aggr])?
                .build()?;
            Ok(scalar_subquery(Arc::new(plan)))
        };
        let outer = out_ref_col(DataType::UInt32, "o.a");
        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("o")?)
            .project(vec![
                correlated(col("a").eq(outer.clone()), count(col("b")))?.alias("x"),
                correlated(col("a").eq(outer), avg(col("c")))?.alias("y"),
            ])?
            .build()?;

        // Correlated: left to the decorrelation rules.
        assert_fused!(plan, @r"
        Projection: (<subquery>) AS x, (<subquery>) AS y
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[COUNT(t.b)]]
              Filter: t.a = outer_ref(o.a)
                TableScan: t
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[avg(t.c)]]
              Filter: t.a = outer_ref(o.a)
                TableScan: t
          TableScan: o
        ")
    }

    #[test]
    fn keeps_distinct_aggregates() -> Result<()> {
        let distinct = count(col("b")).distinct().build()?;
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").lt(lit(10)), distinct)?.alias("x"),
                scalar_agg(col("a").gt_eq(lit(10)), avg(col("c")))?.alias("y"),
            ])?
            .build()?;

        // A DISTINCT aggregate is not mergeable, so neither branch is fused.
        assert_fused!(plan, @r"
        Projection: (<subquery>) AS x, (<subquery>) AS y
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[COUNT(DISTINCT t.b)]]
              Filter: t.a < Int32(10)
                TableScan: t
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[avg(t.c)]]
              Filter: t.a >= Int32(10)
                TableScan: t
          EmptyRelation: rows=1
        ")
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileTestUdf;

    impl ScalarUDFImpl for VolatileTestUdf {
        fn name(&self) -> &str {
            "volatile_test"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
            &SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("VolatileTestUdf is not intended for execution")
        }
    }

    #[test]
    fn keeps_volatile_predicate() -> Result<()> {
        let volatile = ScalarUDF::new_from_impl(VolatileTestUdf).call(vec![]);
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").lt(volatile), count(col("b")))?.alias("x"),
                scalar_agg(col("a").gt_eq(lit(10)), avg(col("c")))?.alias("y"),
            ])?
            .build()?;

        // A volatile predicate is not mergeable, so the plan is left unchanged.
        assert_fused!(plan, @r"
        Projection: (<subquery>) AS x, (<subquery>) AS y
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[COUNT(t.b)]]
              Filter: t.a < volatile_test()
                TableScan: t
          Subquery:
            Aggregate: groupBy=[[]], aggr=[[avg(t.c)]]
              Filter: t.a >= Int32(10)
                TableScan: t
          EmptyRelation: rows=1
        ")
    }

    #[test]
    fn disabled_by_default() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![
                scalar_agg(col("a").lt(lit(10)), count(col("b")))?.alias("lo"),
                scalar_agg(col("a").gt_eq(lit(10)), avg(col("c")))?.alias("hi"),
            ])?
            .build()?;

        // With the default config (flag off) the rule is a no-op.
        let rule = FuseScalarSubqueries::new();
        let result = rule.rewrite(plan, &OptimizerContext::new())?;
        assert!(!result.transformed);
        Ok(())
    }
}
