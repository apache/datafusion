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

//! Rewrites `UNION DISTINCT` branches that differ only by filter predicates
//! into a single filtered branch plus `DISTINCT`.

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::utils::disjunction;
use datafusion_expr::{
    Distinct, Expr, Filter, LogicalPlan, Projection, SubqueryAlias, Union,
};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct UnionsToFilter;

impl UnionsToFilter {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for UnionsToFilter {
    fn name(&self) -> &str {
        "unions_to_filter"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_unions_to_filter {
            return Ok(Transformed::no(plan));
        }

        // Fast pre-check: if the plan tree has no Distinct::All node at all we can
        // skip the expensive bottom-up rewrite_with_subqueries traversal entirely.
        // This matters for large UNION ALL plans (e.g. TPC-DS Q4) where the rule
        // can never fire and the traversal overhead is otherwise measurable.
        if !plan.exists(|p| Ok(matches!(p, LogicalPlan::Distinct(Distinct::All(_)))))? {
            return Ok(Transformed::no(plan));
        }

        plan.rewrite_with_subqueries(&mut UnionsToFilterRewriter)
    }
}

struct UnionsToFilterRewriter;

impl TreeNodeRewriter for UnionsToFilterRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let inner = Arc::unwrap_or_clone(input);
                match try_rewrite_distinct_union(inner.clone())? {
                    Some(rewritten) => Ok(Transformed::yes(rewritten)),
                    None => Ok(Transformed::no(LogicalPlan::Distinct(Distinct::All(
                        Arc::new(inner),
                    )))),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn try_rewrite_distinct_union(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Union(Union { inputs, schema }) = plan else {
        debug!("unions_to_filter skipped: input is not a UNION");
        return Ok(None);
    };

    if inputs.len() < 2 {
        debug!(
            "unions_to_filter skipped: UNION has {} input(s), need at least 2",
            inputs.len()
        );
        return Ok(None);
    }

    let mut grouped: HashMap<GroupKey, Vec<Expr>> = HashMap::new();
    let mut input_order: Vec<GroupKey> = Vec::new();
    let mut transformed = false;

    for input in inputs {
        let Some(branch) = extract_branch(Arc::unwrap_or_clone(input))? else {
            return Ok(None);
        };

        let key = GroupKey {
            source: branch.source,
            wrappers: branch.wrappers,
        };
        if let Some(conds) = grouped.get_mut(&key) {
            conds.push(branch.predicate);
            transformed = true;
        } else {
            input_order.push(key.clone());
            grouped.insert(key, vec![branch.predicate]);
        }
    }

    if !transformed {
        debug!("unions_to_filter skipped: no branch groups could be merged");
        return Ok(None);
    }

    let mut builder: Option<LogicalPlanBuilder> = None;
    for key in input_order {
        let predicates = grouped
            .remove(&key)
            .expect("grouped predicates should exist for every source");
        let combined =
            disjunction(predicates).expect("union branches always provide predicates");
        let branch = LogicalPlanBuilder::from(key.source)
            .filter(combined)?
            .build()?;
        let branch = wrap_branch(branch, &key.wrappers)?;
        let branch = coerce_plan_expr_for_schema(branch, &schema)?;
        let branch = align_plan_to_schema(branch, Arc::clone(&schema))?;
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(builder) => builder.union(branch)?,
        });
    }

    let union = builder
        .expect("at least one branch after rewrite")
        .build()?;
    Ok(Some(LogicalPlan::Distinct(Distinct::All(Arc::new(union)))))
}

struct UnionBranch {
    source: LogicalPlan,
    predicate: Expr,
    wrappers: Vec<Wrapper>,
}

fn extract_branch(plan: LogicalPlan) -> Result<Option<UnionBranch>> {
    let (wrappers, plan) = peel_wrappers(plan);

    // Volatile or subquery expressions in the projection must not be merged:
    // they are evaluated once per branch in the original plan but would be
    // evaluated once per combined row after the rewrite, which can change the
    // output row set.
    if !wrapper_projections_are_safe(&wrappers) {
        debug!(
            "unions_to_filter skipped: projection wrapper contains volatile expression or subquery"
        );
        return Ok(None);
    }

    match plan {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                debug!(
                    "unions_to_filter skipped: branch predicate contains volatility or a subquery"
                );
                return Ok(None);
            }
            Ok(Some(UnionBranch {
                source: strip_passthrough_nodes(Arc::unwrap_or_clone(input)),
                predicate,
                wrappers,
            }))
        }
        // A Limit or Sort node changes the row-set semantics of the branch.
        // Merging two such branches into one would silently drop the per-branch
        // row restriction (LIMIT) or rely on an order guarantee that UNION does
        // not preserve (ORDER BY).  Bail out to leave the UNION unchanged.
        LogicalPlan::Limit(_) => {
            debug!("unions_to_filter skipped: branch contains LIMIT");
            Ok(None)
        }
        LogicalPlan::Sort(_) => {
            debug!("unions_to_filter skipped: branch contains ORDER BY / SORT");
            Ok(None)
        }
        other => Ok(Some(UnionBranch {
            source: strip_passthrough_nodes(other.clone()),
            predicate: Expr::Literal(
                datafusion_common::ScalarValue::Boolean(Some(true)),
                None,
            ),
            wrappers,
        })),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    source: LogicalPlan,
    wrappers: Vec<Wrapper>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Wrapper {
    Projection {
        expr: Vec<Expr>,
        schema: datafusion_common::DFSchemaRef,
    },
    SubqueryAlias {
        alias: datafusion_common::TableReference,
        schema: datafusion_common::DFSchemaRef,
    },
}

fn peel_wrappers(mut plan: LogicalPlan) -> (Vec<Wrapper>, LogicalPlan) {
    let mut wrappers = vec![];
    loop {
        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::Projection { expr, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::SubqueryAlias { alias, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            other => return (wrappers, other),
        }
    }
}

fn wrap_branch(mut plan: LogicalPlan, wrappers: &[Wrapper]) -> Result<LogicalPlan> {
    for wrapper in wrappers.iter().rev() {
        plan = match wrapper {
            Wrapper::Projection { expr, schema } => {
                LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr.clone(),
                    Arc::new(plan),
                    Arc::clone(schema),
                )?)
            }
            Wrapper::SubqueryAlias { alias, .. } => LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), alias.clone())?,
            ),
        };
    }
    Ok(plan)
}

fn strip_passthrough_nodes(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(Projection { input, .. }) => {
            strip_passthrough_nodes(Arc::unwrap_or_clone(input))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            strip_passthrough_nodes(Arc::unwrap_or_clone(input))
        }
        other => other,
    }
}

fn align_plan_to_schema(
    plan: LogicalPlan,
    schema: datafusion_common::DFSchemaRef,
) -> Result<LogicalPlan> {
    if plan.schema() == &schema {
        return Ok(plan);
    }

    let expr = plan
        .schema()
        .iter()
        .enumerate()
        .map(|(i, _)| {
            Expr::Column(datafusion_common::Column::from(
                plan.schema().qualified_field(i),
            ))
        })
        .collect::<Vec<_>>();

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expr,
        Arc::new(plan),
        schema,
    )?))
}

fn is_mergeable_predicate(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_subquery(expr)
}

/// Returns `true` when every projection expression in `wrappers` is both
/// non-volatile and subquery-free.
///
/// Volatile expressions (e.g. `random()`, `now()`) or correlated subqueries
/// in the SELECT list cannot be safely merged: the original plan evaluates
/// them once per branch execution, while the rewritten plan evaluates them
/// once per combined row, which can change the set of output rows.
fn wrapper_projections_are_safe(wrappers: &[Wrapper]) -> bool {
    wrappers.iter().all(|w| match w {
        Wrapper::Projection { expr, .. } => expr
            .iter()
            .all(|e| !e.is_volatile() && !expr_contains_subquery(e)),
        Wrapper::SubqueryAlias { .. } => true,
    })
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => Ok(true),
        _ => Ok(false),
    })
    .expect("boolean expression walk is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility, col, lit,
    };

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut options = datafusion_common::config::ConfigOptions::default();
            options.optimizer.enable_unions_to_filter = true;
            let optimizer_ctx = OptimizerContext::new_with_config_options(Arc::new(options))
                .with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(UnionsToFilter::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
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
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("VolatileTestUdf is not intended for execution")
        }
    }

    fn volatile_expr() -> Expr {
        ScalarUDF::new_from_impl(VolatileTestUdf).call(vec![])
    }

    #[test]
    fn rewrite_union_distinct_same_source_filters() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Projection: t.a, t.b, t.c
            Filter: t.a = Int32(1) OR t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_distinct_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Filter: t1.a = Int32(1)
              TableScan: t1
            Filter: t2.a = Int32(2)
              TableScan: t2
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_distinct_with_volatile_predicate() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(volatile_expr().gt(lit(0.5_f64)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Filter: volatile_test() > Float64(0.5)
              TableScan: t
            Filter: t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_distinct_with_matching_projection_prefix() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("b").eq(lit(5)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Projection: emp.a AS mgr, emp.b AS comm
            Filter: Boolean(true) OR emp.b = Int32(5)
              TableScan: emp
        ")?;
        Ok(())
    }

    /// A volatile expression in the **projection** (SELECT list) must block the
    /// rewrite.  Each original branch evaluates it independently; merging them
    /// would evaluate it once per combined row, changing the row set.
    #[test]
    fn keep_union_distinct_with_volatile_projection() -> Result<()> {
        // Both branches project volatile_test() AS v over the same source.
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Projection: volatile_test() AS v, t.a
              Filter: t.a = Int32(1)
                TableScan: t
            Projection: volatile_test() AS v, t.a
              Filter: t.a = Int32(2)
                TableScan: t
        ")?;
        Ok(())
    }

    /// A scalar subquery in the **projection** must also block the rewrite.
    #[test]
    fn keep_union_distinct_with_subquery_in_projection() -> Result<()> {
        use datafusion_expr::scalar_subquery;

        // Build a simple scalar subquery: (SELECT t2.b FROM t2 WHERE t2.a = t.a)
        let t2 = test_table_scan_with_name("t2")?;
        let subquery_plan = Arc::new(
            LogicalPlanBuilder::from(t2)
                .filter(col("t2.a").eq(col("t.a")))?
                .project(vec![col("t2.b")])?
                .build()?,
        );
        let sq = scalar_subquery(subquery_plan);

        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![sq.clone().alias("sub"), col("a")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![sq.alias("sub"), col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        // Plan should be left untouched because the projection contains a subquery.
        let optimized = {
            let mut options = datafusion_common::config::ConfigOptions::default();
            options.optimizer.enable_unions_to_filter = true;
            let optimizer_ctx =
                OptimizerContext::new_with_config_options(Arc::new(options))
                    .with_max_passes(1);
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(UnionsToFilter::new())];
            crate::Optimizer::with_rules(rules).optimize(
                plan.clone(),
                &optimizer_ctx,
                |_, _| {},
            )?
        };
        // The Distinct(Union(...)) structure must be preserved.
        assert!(
            matches!(
                &optimized,
                LogicalPlan::Distinct(Distinct::All(inner))
                if matches!(inner.as_ref(), LogicalPlan::Union(_))
            ),
            "expected Distinct(Union(...)) to be preserved, got:\n{optimized:?}"
        );
        Ok(())
    }

    /// A UNION where both branches have a LIMIT must **not** be rewritten.
    /// Each branch independently restricts the row-set; collapsing them into a
    /// single branch would lose the per-branch LIMIT semantics.
    #[test]
    fn keep_union_distinct_with_limit_branches() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .limit(0, Some(2))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .limit(0, Some(2))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Limit: skip=0, fetch=2
              Projection: emp.a AS mgr, emp.b AS comm
                TableScan: emp
            Limit: skip=0, fetch=2
              Projection: emp.a AS mgr, emp.b AS comm
                TableScan: emp
        ")?;
        Ok(())
    }

    /// A UNION where both branches have an ORDER BY (Sort) must **not** be
    /// rewritten.  ORDER BY inside a UNION subquery does not guarantee ordering
    /// in the result; merging the branches would silently discard the Sort.
    #[test]
    fn keep_union_distinct_with_sort_branches() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .sort(vec![col("a").sort(true, true)])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .sort(vec![col("a").sort(true, true)])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Projection: mgr, comm
              Sort: emp.a ASC NULLS FIRST
                Projection: emp.a AS mgr, emp.b AS comm, emp.a
                  TableScan: emp
            Projection: mgr, comm
              Sort: emp.a ASC NULLS FIRST
                Projection: emp.a AS mgr, emp.b AS comm, emp.a
                  TableScan: emp
        ")?;
        Ok(())
    }
}
