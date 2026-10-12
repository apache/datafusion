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

//! [`EliminateAggregateDistinct`] drops the `DISTINCT` modifier from aggregate
//! functions that report [`DistinctHandling::Insensitive`]

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{DistinctHandling, Expr, LogicalPlan};

/// Optimizer rule that removes a `DISTINCT` modifier that cannot change the
/// result of the aggregate it is attached to.
///
/// `min`, `max`, `bool_and`, `bit_or` and friends have an idempotent merge, so
/// `min(DISTINCT x)` and `min(x)` return the same value. Removing the flag here
/// keeps [`crate::single_distinct_to_groupby::SingleDistinctToGroupBy`] from
/// rewriting the plan into an inner group by that only exists to deduplicate.
///
/// An aggregate states how it treats duplicates through
/// [`datafusion_expr::AggregateUDFImpl::distinct_handling`].
///
/// ```text
/// Aggregate: groupBy=[[g]], aggr=[[min(DISTINCT x)]]
/// ```
///
/// becomes
///
/// ```text
/// Aggregate: groupBy=[[g]], aggr=[[min(x) AS "min(DISTINCT x)"]]
/// ```
///
/// The alias keeps the output schema unchanged so the parent projection still
/// resolves.
#[derive(Default, Debug)]
pub struct EliminateAggregateDistinct {}

impl EliminateAggregateDistinct {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateAggregateDistinct {
    fn name(&self) -> &str {
        "eliminate_aggregate_distinct"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Aggregate expressions only appear on Aggregate nodes, so every other
        // node is a cheap no-op. Window functions carry their own `distinct`
        // flag and are out of scope.
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            return Ok(Transformed::no(plan));
        };
        if !can_strip_every_distinct(&aggregate.aggr_expr)? {
            return Ok(Transformed::no(plan));
        }

        // Dropping `DISTINCT` changes `Expr::schema_name`, and with it the
        // output schema of the Aggregate, so restore the original name. The
        // aggregate may sit under an alias that type coercion added, so walk
        // the expression rather than matching only its root.
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let saved_name = name_preserver.save(&expr);
            expr.transform_down(strip_insensitive_distinct)
                .map(|t| t.update_data(|e| saved_name.restore(e)))
        })
    }
}

/// Whether the node has at least one `DISTINCT` and every one is
/// `Insensitive`.
///
/// If only some of them were stripped, a `Sensitive` `DISTINCT` could stay
/// beside a stripped aggregate. A stripped aggregate carries an alias, and
/// [`crate::single_distinct_to_groupby::SingleDistinctToGroupBy`] does not
/// rewrite a node whose `aggr_expr` contains an alias. So `min(DISTINCT x),
/// count(DISTINCT x)` would lose the rewrite that `count` needs. When every
/// `DISTINCT` is `Insensitive`, none is left after stripping, so that rule has
/// nothing to rewrite. This is conservative: `min(DISTINCT x),
/// count(DISTINCT y)` keeps the `min` flag. That flag costs nothing at run
/// time, because `min` ignores `is_distinct` when it selects its accumulator.
fn can_strip_every_distinct(aggr_expr: &[Expr]) -> Result<bool> {
    let mut found_distinct = false;
    let mut all_insensitive = true;
    for expr in aggr_expr {
        expr.apply(|e| {
            if let Expr::AggregateFunction(AggregateFunction { func, params }) = e
                && params.distinct
            {
                found_distinct = true;
                if func.distinct_handling() != DistinctHandling::Insensitive {
                    all_insensitive = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !all_insensitive {
            break;
        }
    }
    Ok(found_distinct && all_insensitive)
}

/// Drops `DISTINCT` from `expr` if it is an aggregate that ignores duplicates.
///
/// The handling is checked again here rather than trusted to
/// [`can_strip_every_distinct`], which only inspects `aggr_expr`, while
/// `map_expressions` also visits the group expressions.
///
/// An idempotent merge is not always commutative: `first_value` is
/// idempotent but order-sensitive. The rule does not need commutativity.
/// `Insensitive` means that the result does not change when duplicates are
/// removed, and stripping `DISTINCT` only stops that removal. `order_by` and
/// `filter` are carried over untouched, so the function sees the same rows in
/// the same order, plus the duplicates that it ignores.
fn strip_insensitive_distinct(expr: Expr) -> Result<Transformed<Expr>> {
    Ok(match expr {
        Expr::AggregateFunction(mut agg)
            if agg.params.distinct
                && agg.func.distinct_handling() == DistinctHandling::Insensitive =>
        {
            agg.params.distinct = false;
            Transformed::yes(Expr::AggregateFunction(agg))
        }
        _ => Transformed::no(expr),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::udfs::DistinctHandlingTestUDAF;
    use crate::test::*;

    use crate::single_distinct_to_groupby::SingleDistinctToGroupBy;
    use datafusion_expr::{AggregateUDF, ExprFunctionExt, LogicalPlanBuilder, col, lit};
    use datafusion_functions_aggregate::expr_fn::{bit_xor, max, min, sum};

    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(EliminateAggregateDistinct::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    /// A user defined aggregate that reports the given [`DistinctHandling`].
    ///
    /// The tests above read the tag off built-in functions, which only covers
    /// the variants those functions happen to carry.
    fn tagged(name: &'static str, handling: DistinctHandling) -> AggregateUDF {
        AggregateUDF::from(DistinctHandlingTestUDAF::new(name, handling))
    }

    /// `min(DISTINCT b)` loses the flag but keeps its column name.
    #[test]
    fn eliminate_distinct_from_min() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![min(col("b")).distinct().build()?])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[min(test.b) AS min(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// `sum` deduplicates for real, so the flag stays.
    #[test]
    fn keep_distinct_on_sum() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b")).distinct().build()?])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// XOR cancels duplicate pairs, unlike its `bit_and`/`bit_or` siblings.
    #[test]
    fn keep_distinct_on_bit_xor() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![bit_xor(col("b")).distinct().build()?])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[bit_xor(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// A node keeps every flag when one of them has to stay, so that this rule
    /// cannot change which plans `SingleDistinctToGroupBy` rewrites.
    #[test]
    fn mixed_node_is_left_alone() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    min(col("b")).distinct().build()?,
                    sum(col("c")).distinct().build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[min(DISTINCT test.b), sum(DISTINCT test.c)]]
          TableScan: test
        ")
    }

    /// Several duplicate-insensitive aggregates all lose the flag together.
    #[test]
    fn eliminate_distinct_from_every_insensitive_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    min(col("b")).distinct().build()?,
                    max(col("c")).distinct().build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[min(test.b) AS min(DISTINCT test.b), max(test.c) AS max(DISTINCT test.c)]]
          TableScan: test
        ")
    }

    /// A non-distinct aggregate alongside is no obstacle.
    #[test]
    fn eliminate_distinct_beside_non_distinct_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![min(col("b")).distinct().build()?, sum(col("c"))],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[min(test.b) AS min(DISTINCT test.b), sum(test.c)]]
          TableScan: test
        ")
    }

    /// The gate only inspects `aggr_expr`, so a `Sensitive` distinct aggregate
    /// in the group expressions must keep its flag.
    #[test]
    fn keep_sensitive_distinct_in_group_expr() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![sum(col("c")).distinct().build()?],
                vec![min(col("b")).distinct().build()?],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[sum(DISTINCT test.c)]], aggr=[[min(test.b) AS min(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// `FILTER` is applied before deduplication, so it rides along untouched.
    #[test]
    fn eliminate_distinct_keeps_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    min(col("b"))
                        .distinct()
                        .filter(col("c").gt(lit(0u32)))
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[min(test.b) FILTER (WHERE test.c > UInt32(0)) AS min(DISTINCT test.b) FILTER (WHERE test.c > UInt32(0))]]
          TableScan: test
        ")
    }

    /// The conservative gate exists so `SingleDistinctToGroupBy` still sees the
    /// plan exactly as it did before this rule was added.
    #[test]
    fn mixed_node_still_reaches_single_distinct_to_groupby() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    min(col("b")).distinct().build()?,
                    sum(col("b")).distinct().build()?,
                ],
            )?
            .build()?;

        let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
        let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
            Arc::new(EliminateAggregateDistinct::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
        ];
        assert_optimized_plan_eq_snapshot!(
            optimizer_ctx,
            rules,
            plan,
            @r"
        Projection: test.a, min(alias1) AS min(DISTINCT test.b), sum(alias1) AS sum(DISTINCT test.b)
          Aggregate: groupBy=[[test.a]], aggr=[[min(alias1), sum(alias1)]]
            Aggregate: groupBy=[[test.a, test.b AS alias1]], aggr=[[]]
              TableScan: test
        ",
        )
    }

    /// A user defined `Insensitive` aggregate loses the flag, keeps its output
    /// name, and carries `FILTER` and `ORDER BY` over untouched.
    #[test]
    fn eliminate_distinct_from_insensitive_udaf() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    tagged("first_seen", DistinctHandling::Insensitive)
                        .call(vec![col("b")])
                        .distinct()
                        .filter(col("c").gt(lit(0u32)))
                        .order_by(vec![col("b").sort(true, false)])
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[first_seen(test.b) FILTER (WHERE test.c > UInt32(0)) ORDER BY [test.b ASC NULLS LAST] AS first_seen(DISTINCT test.b) FILTER (WHERE test.c > UInt32(0)) ORDER BY [test.b ASC NULLS LAST]]]
          TableScan: test
        ")
    }

    /// A user defined aggregate that deduplicates for real keeps the flag.
    #[test]
    fn keep_distinct_on_sensitive_udaf() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    tagged("counts_uniques", DistinctHandling::Sensitive)
                        .call(vec![col("b")])
                        .distinct()
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[counts_uniques(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// An aggregate that does not implement `DISTINCT` keeps the flag too: the
    /// planner still has to deduplicate the input for it.
    #[test]
    fn keep_distinct_on_unsupported_udaf() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    tagged("needs_dedup", DistinctHandling::Unsupported)
                        .call(vec![col("b")])
                        .distinct()
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[needs_dedup(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// The conservative gate covers user defined functions as well: an
    /// `Insensitive` one beside a `Sensitive` one keeps both flags.
    #[test]
    fn mixed_udaf_node_is_left_alone() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    tagged("first_seen", DistinctHandling::Insensitive)
                        .call(vec![col("b")])
                        .distinct()
                        .build()?,
                    tagged("counts_uniques", DistinctHandling::Sensitive)
                        .call(vec![col("c")])
                        .distinct()
                        .build()?,
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[first_seen(DISTINCT test.b), counts_uniques(DISTINCT test.c)]]
          TableScan: test
        ")
    }

    /// An aggregate that already carries an alias keeps that one name: the
    /// rule restores the name it found, so no second alias is stacked on top.
    #[test]
    fn already_aliased_aggregate_gets_no_second_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    tagged("first_seen", DistinctHandling::Insensitive)
                        .call(vec![col("b")])
                        .distinct()
                        .build()?
                        .alias("m"),
                ],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[first_seen(test.b) AS m]]
          TableScan: test
        ")
    }

    /// `AggregateUDF::with_aliases` wraps the function in another
    /// `AggregateUDFImpl`, which has to pass the tag through.
    #[test]
    fn with_aliases_delegates_distinct_handling() -> Result<()> {
        let aliased = tagged("first_seen", DistinctHandling::Insensitive)
            .with_aliases(["first_hit"]);
        assert_eq!(aliased.distinct_handling(), DistinctHandling::Insensitive);

        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![aliased.call(vec![col("b")]).distinct().build()?],
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[first_seen(test.b) AS first_seen(DISTINCT test.b)]]
          TableScan: test
        ")
    }

    /// A plan with no Aggregate takes the no-op path.
    #[test]
    fn non_aggregate_plan_is_unchanged() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(1u32)))?
            .project(vec![col("a")])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a
          Filter: test.b > UInt32(1)
            TableScan: test
        ")
    }
}
