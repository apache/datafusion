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
//! functions that report [`DistinctHandling::Ignored`]

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
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
        if !matches!(plan, LogicalPlan::Aggregate(_)) {
            return Ok(Transformed::no(plan));
        }

        // Dropping `DISTINCT` changes `Expr::schema_name`, and with it the
        // output schema of the Aggregate, so restore the original name.
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            // The aggregate may sit under an alias that type coercion added,
            // so walk the expression rather than matching only its root.
            let saved_name = name_preserver.save(&expr);
            let rewritten = expr.transform_down(strip_ignored_distinct)?;
            if rewritten.transformed {
                Ok(Transformed::yes(saved_name.restore(rewritten.data)))
            } else {
                Ok(Transformed::no(rewritten.data))
            }
        })
    }
}

/// Drops `DISTINCT` from `expr` if it is an aggregate that ignores duplicates.
///
/// An idempotent merge is also commutative, so an `Ignored` function is
/// insensitive to input order and `order_by` needs no extra guard. `filter` is
/// applied before deduplication either way, so it is carried over untouched.
fn strip_ignored_distinct(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::AggregateFunction(AggregateFunction { func, params }) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !params.distinct || func.distinct_handling() != DistinctHandling::Ignored {
        return Ok(Transformed::no(expr));
    }

    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        unreachable!("matched Expr::AggregateFunction above")
    };
    // Destructured exhaustively so a new field cannot be dropped silently.
    let AggregateFunctionParams {
        args,
        distinct: _,
        filter,
        order_by,
        null_treatment,
    } = params;

    Ok(Transformed::yes(Expr::AggregateFunction(
        AggregateFunction {
            func,
            params: AggregateFunctionParams {
                args,
                distinct: false,
                filter,
                order_by,
                null_treatment,
            },
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;

    use datafusion_expr::{ExprFunctionExt, LogicalPlanBuilder, col, lit};
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

    /// `max(DISTINCT b)` is the other half of the same accumulator family.
    #[test]
    fn eliminate_distinct_from_max() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b")).distinct().build()?])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Aggregate: groupBy=[[test.a]], aggr=[[max(test.b) AS max(DISTINCT test.b)]]
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

    /// A plan mixing the two strips only the duplicate-insensitive one.
    #[test]
    fn eliminate_distinct_from_min_only() -> Result<()> {
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
        Aggregate: groupBy=[[test.a]], aggr=[[min(test.b) AS min(DISTINCT test.b), sum(DISTINCT test.c)]]
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
