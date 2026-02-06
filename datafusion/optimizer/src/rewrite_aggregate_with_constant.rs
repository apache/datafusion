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

//! [`RewriteAggregateWithConstant`] rewrites `SUM(column ± constant)` to `SUM(column) ± constant * COUNT(column)`

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use std::collections::HashMap;

use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::AggregateFunctionParams;
use datafusion_expr::{
    Aggregate, BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator, binary_expr,
    col, lit,
};
use datafusion_functions_aggregate::expr_fn::{count, sum};
use indexmap::IndexMap;

/// Optimizer rule that rewrites `SUM(column ± constant)` expressions
/// into `SUM(column) ± constant * COUNT(column)` when multiple such expressions
/// exist for the same base column.
///
/// This reduces computation by calculating SUM once and deriving other values.
///
/// # Example
/// ```sql
/// SELECT SUM(a), SUM(a + 1), SUM(a + 2) FROM t;
/// ```
/// is rewritten into a Projection on top of an Aggregate:
/// ```sql
/// -- New Projection Node
/// SELECT sum_a, sum_a + 1 * count_a, sum_a + 2 * count_a
/// -- New Aggregate Node
/// FROM (SELECT SUM(a) as sum_a, COUNT(a) as count_a FROM t);
/// ```
#[derive(Default, Debug)]
pub struct RewriteAggregateWithConstant {}

impl RewriteAggregateWithConstant {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for RewriteAggregateWithConstant {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            // This rule specifically targets Aggregate nodes
            LogicalPlan::Aggregate(aggregate) => {
                // Step 1: Identify which expressions can be rewritten and group them by base column
                let rewrite_info = analyze_aggregate(&aggregate)?;

                if rewrite_info.is_empty() {
                    // No groups found with 2+ matching SUM expressions, return original plan
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                // Step 2: Perform the actual transformation into Aggregate + Projection
                transform_aggregate(aggregate, &rewrite_info)
            }
            // Non-aggregate plans are passed through unchanged
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "rewrite_aggregate_with_constant"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Bottom-up ensures we optimize subqueries before the outer query
        Some(ApplyOrder::BottomUp)
    }
}

/// Internal structure to track metadata for a SUM expression that qualifies for rewrite.
#[derive(Debug, Clone)]
struct SumWithConstant {
    /// The inner expression being summed (e.g., the `a` in `SUM(a + 1)`)
    base_expr: Expr,
    /// The constant value being added/subtracted (e.g., `1` in `SUM(a + 1)`)
    constant: ScalarValue,
    /// The operator (`+` or `-`)
    operator: Operator,
    /// The index in the original Aggregate's `aggr_expr` list, used to maintain output order
    original_index: usize,
    // Note: ORDER BY inside SUM is irrelevant because SUM is commutative —
    // the order of addition doesn't change the result. If this rule is ever
    // extended to non-commutative aggregates, ORDER BY handling would need
    // to be added back.
}

/// Maps a base expression's schema name to all its SUM(base ± const) variants.
/// We use IndexMap to preserve insertion order, ensuring deterministic output
/// in the rewritten plan (important for stable EXPLAIN output in tests).
type RewriteGroups = IndexMap<String, Vec<SumWithConstant>>;

/// Scans the aggregate expressions to find candidates for the rewrite.
fn analyze_aggregate(aggregate: &Aggregate) -> Result<RewriteGroups> {
    let mut groups: RewriteGroups = IndexMap::new();

    for (idx, expr) in aggregate.aggr_expr.iter().enumerate() {
        // Try to match the pattern SUM(col ± lit)
        if let Some(sum_info) = extract_sum_with_constant(expr, idx)? {
            let key = sum_info.base_expr.schema_name().to_string();
            groups.entry(key).or_default().push(sum_info);
        }
    }

    // Optimization: Only rewrite if we have at least 2 expressions for the same column.
    // If there's only one SUM(a + 1), rewriting it to SUM(a) + 1*COUNT(a)
    // actually increases the work (1 agg -> 2 aggs).
    groups.retain(|_, v| v.len() >= 2);

    Ok(groups)
}

/// Extract SUM(base_expr ± constant) pattern from an expression.
/// Handles both `Expr::AggregateFunction(...)` and `Expr::Alias(Expr::AggregateFunction(...))`
/// so the rule works regardless of whether aggregate expressions carry aliases
/// (e.g., when plans are built via the LogicalPlanBuilder API).
fn extract_sum_with_constant(expr: &Expr, idx: usize) -> Result<Option<SumWithConstant>> {
    // Unwrap Expr::Alias if present — the SQL planner puts aliases in a
    // Projection above the Aggregate, but the builder API allows aliases
    // directly inside aggr_expr.
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };

    match inner {
        Expr::AggregateFunction(agg_fn) => {
            // Rule only applies to SUM
            if agg_fn.func.name().to_lowercase() != "sum" {
                return Ok(None);
            }

            let AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by: _,
                null_treatment: _,
            } = &agg_fn.params;

            // We cannot easily rewrite SUM(DISTINCT a + 1) or SUM(a + 1) FILTER (...)
            // as the math SUM(a) + k*COUNT(a) wouldn't hold correctly with these modifiers.
            if *distinct || filter.is_some() {
                return Ok(None);
            }

            // SUM must have exactly one argument (e.g. SUM(a + 1)).
            // This rejects invalid calls like SUM() or non-standard multi-argument variations.
            if args.len() != 1 {
                return Ok(None);
            }

            let arg = &args[0];

            // Try to match: base_expr +/- constant
            // Note: If the base_expr is complex (e.g., SUM(a + b + 1)), base_expr will be "a + b".
            // The rule will still work if multiple SUMs have the exact same complex base_expr,
            // as they will be grouped by the string representation of that expression.
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = arg
                && matches!(op, Operator::Plus | Operator::Minus)
            {
                // Check if right side is a literal constant
                // Check if right side is a literal constant (e.g., SUM(a + 1))
                if let Expr::Literal(constant, _) = right.as_ref()
                    && is_numeric_constant(constant)
                {
                    return Ok(Some(SumWithConstant {
                        base_expr: (**left).clone(),
                        constant: constant.clone(),
                        operator: *op,
                        original_index: idx,
                    }));
                }

                // Also check left side for commutative addition (e.g., SUM(1 + a))
                // Does NOT apply to subtraction: SUM(5 - a) ≠ SUM(a - 5)
                if let Expr::Literal(constant, _) = left.as_ref()
                    && is_numeric_constant(constant)
                    && *op == Operator::Plus
                {
                    return Ok(Some(SumWithConstant {
                        base_expr: (**right).clone(),
                        constant: constant.clone(),
                        operator: Operator::Plus,
                        original_index: idx,
                    }));
                }
            }

            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Check if a scalar value is a numeric constant
/// (guards against non-arithmetic types like strings, booleans, dates, etc.)
fn is_numeric_constant(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Decimal256(_, _, _)
    )
}

/// Check if an expression is a plain `SUM(base_expr)` whose base expression matches
/// one of our rewrite groups. Returns `Some(base_key)` if matched, `None` otherwise.
///
/// Handles both `Expr::AggregateFunction(...)` and `Expr::Alias(Expr::AggregateFunction(...))`
/// so that aliased plain SUMs (e.g., `SUM(a) AS total`) are correctly detected and reused
/// instead of being duplicated in the new Aggregate node.
///
/// Returning the `base_key` directly eliminates the need for the caller to re-extract it
/// from the expression (avoiding a potential panic on impossible-to-reach fallback paths).
fn check_plain_sum_in_group(
    expr: &Expr,
    known_base_keys: &IndexMap<String, (usize, usize)>,
) -> Option<String> {
    // Unwrap alias if present
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };

    if let Expr::AggregateFunction(agg_fn) = inner
        && agg_fn.func.name().to_lowercase() == "sum"
        && agg_fn.params.args.len() == 1
        && !agg_fn.params.distinct
        && agg_fn.params.filter.is_none()
    {
        let arg = &agg_fn.params.args[0];
        let base_key = arg.schema_name().to_string();
        if known_base_keys.contains_key(&base_key) {
            return Some(base_key);
        }
    }
    None
}

/// Alias `expr` to match the output column name of `orig_expr`.
///
/// If `orig_expr` is `Expr::Alias(name)`, the result is aliased to that name.
/// Otherwise, the result is aliased to `orig_expr.schema_name()` to preserve
/// the auto-generated column name (e.g., `"sum(t.a + Int64(1))"`).
fn alias_like(expr: Expr, orig_expr: &Expr) -> Expr {
    match orig_expr {
        Expr::Alias(alias) => expr.alias(alias.name.clone()),
        _ => expr.alias(orig_expr.schema_name().to_string()),
    }
}

/// Transform the aggregate plan by rewriting SUM(col ± constant) expressions.
///
/// Replaces the original `Aggregate` node with:
///   1. A new `Aggregate` containing one `SUM(base)` + one `COUNT(base)` per group,
///      plus any unrelated expressions passed through.
///   2. A `Projection` on top that derives each original output column using
///      the formula: `SUM(base) ± constant * COUNT(base)`.
///
/// Delegates to [`build_rewrite_expressions`] for the expression logic and
/// [`assemble_rewritten_plan`] for the plan construction.
fn transform_aggregate(
    aggregate: Aggregate,
    rewrite_groups: &RewriteGroups,
) -> Result<Transformed<LogicalPlan>> {
    let (new_aggr_exprs, projection_exprs) =
        build_rewrite_expressions(&aggregate, rewrite_groups);
    assemble_rewritten_plan(aggregate, new_aggr_exprs, projection_exprs)
}

/// Build the new aggregate expressions and corresponding projection expressions.
///
/// For each rewrite group (e.g., all SUMs involving column `a`), adds one
/// `SUM(a)` and one `COUNT(a)` to the new aggregate. Then maps every original
/// aggregate expression to a projection expression using one of three cases:
///
/// - **Case 1** (rewritable): `SUM(a + k)` → `SUM(a) + k * COUNT(a)`
/// - **Case 2** (plain SUM in group): `SUM(a)` → reference the already-added `SUM(a)`
/// - **Case 3** (unrelated): pass through unchanged (e.g., `AVG(b)`)
///
/// Returns `(new_aggr_exprs, projection_exprs)`.
fn build_rewrite_expressions(
    aggregate: &Aggregate,
    rewrite_groups: &RewriteGroups,
) -> (Vec<Expr>, Vec<Expr>) {
    let mut new_aggr_exprs: Vec<Expr> = Vec::new();
    let mut projection_exprs: Vec<Expr> = Vec::new();

    // O(1) lookup of rewritable expressions by their original index.
    let rewrite_lookup: HashMap<usize, &SumWithConstant> = rewrite_groups
        .values()
        .flatten()
        .map(|s| (s.original_index, s))
        .collect();

    // Maps base expression key → (sum_index, count_index) in `new_aggr_exprs`.
    // Column names are derived on-demand from `new_aggr_exprs[index].schema_name()`
    // rather than maintaining separate name maps.
    let mut base_expr_indices: IndexMap<String, (usize, usize)> = IndexMap::new();

    // For every rewrite group, add one SUM(base) and one COUNT(base)
    // to the new Aggregate.
    for (base_key, sums) in rewrite_groups {
        let base_expr = &sums[0].base_expr;

        let sum_index = new_aggr_exprs.len();
        new_aggr_exprs.push(sum(base_expr.clone()));

        // COUNT(col) rather than COUNT(*): if col is NULL, SUM(col+1) should
        // be NULL, and COUNT(col) correctly returns 0 for NULLs.
        let count_index = new_aggr_exprs.len();
        new_aggr_exprs.push(count(base_expr.clone()));

        base_expr_indices.insert(base_key.clone(), (sum_index, count_index));
    }

    // Map each original aggregate expression to a projection expression.
    for (idx, orig_expr) in aggregate.aggr_expr.iter().enumerate() {
        let projection_expr = if let Some(sum_info) = rewrite_lookup.get(&idx) {
            // ── Case 1: Rewritable SUM(col ± constant) ──
            // Derive: SUM(col) ± (constant * COUNT(col))
            build_derived_projection(
                sum_info,
                &base_expr_indices,
                &new_aggr_exprs,
                orig_expr,
            )
        } else if let Some(base_key) =
            check_plain_sum_in_group(orig_expr, &base_expr_indices)
        {
            // ── Case 2: Plain SUM(a) matching a rewrite group ──
            // Reuse the SUM(a) we already added instead of creating a duplicate.
            let (sum_idx, _) = base_expr_indices[&base_key];
            let sum_ref = col(new_aggr_exprs[sum_idx].schema_name().to_string());
            alias_like(sum_ref, orig_expr)
        } else {
            // ── Case 3: Unrelated expression (e.g., AVG(b), MAX(c)) ──
            // Pass it through to the new Aggregate node unchanged.
            new_aggr_exprs.push(orig_expr.clone());
            match orig_expr {
                Expr::Alias(alias) => col(alias.name.clone()),
                _ => col(orig_expr.schema_name().to_string()),
            }
        };

        projection_exprs.push(projection_expr);
    }

    (new_aggr_exprs, projection_exprs)
}

/// Build the projection expression for a single rewritable SUM(col ± constant):
///
///   `SUM(col) ± (constant * COUNT(col))`, aliased to match the original expression.
fn build_derived_projection(
    sum_info: &SumWithConstant,
    base_expr_indices: &IndexMap<String, (usize, usize)>,
    new_aggr_exprs: &[Expr],
    orig_expr: &Expr,
) -> Expr {
    let base_key = sum_info.base_expr.schema_name().to_string();
    let (sum_idx, count_idx) = base_expr_indices[&base_key];

    let sum_ref = col(new_aggr_exprs[sum_idx].schema_name().to_string());
    let count_ref = col(new_aggr_exprs[count_idx].schema_name().to_string());

    let multiplied = binary_expr(
        lit(sum_info.constant.clone()),
        Operator::Multiply,
        count_ref,
    );
    let result = binary_expr(sum_ref, sum_info.operator, multiplied);
    alias_like(result, orig_expr)
}

/// Assemble the final rewritten plan: `Projection → Aggregate → (original input)`.
///
/// GROUP BY columns are passed through both nodes so the output schema is preserved.
fn assemble_rewritten_plan(
    aggregate: Aggregate,
    new_aggr_exprs: Vec<Expr>,
    projection_exprs: Vec<Expr>,
) -> Result<Transformed<LogicalPlan>> {
    // GROUP BY columns: reference them in the Projection.
    let group_refs: Vec<Expr> = aggregate
        .group_expr
        .iter()
        .map(|e| match e {
            Expr::Alias(alias) => col(alias.name.clone()),
            Expr::Column(c) => Expr::Column(c.clone()),
            _ => col(e.schema_name().to_string()),
        })
        .collect();

    // Final projection: [GROUP BY columns] ++ [aggregate/rewritten columns]
    let mut final_projection = group_refs;
    final_projection.extend(projection_exprs);

    // Build: Projection → Aggregate → (original input)
    let new_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        aggregate.input,
        aggregate.group_expr,
        new_aggr_exprs,
    )?);

    let projection = LogicalPlanBuilder::from(new_aggregate)
        .project(final_projection)?
        .build()?;

    Ok(Transformed::yes(projection))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::{LogicalPlanBuilder, col, lit};
    use datafusion_functions_aggregate::expr_fn::sum;

    #[test]
    fn test_sum_with_constant_basic() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    sum(col("a")),
                    sum(col("a") + lit(1)),
                    sum(col("a") + lit(2)),
                ],
            )?
            .build()?;

        let rule = RewriteAggregateWithConstant::new();
        let config = OptimizerContext::new();
        let result = rule.rewrite(plan, &config)?;

        // Should be transformed
        assert!(result.transformed);
        Ok(())
    }

    #[test]
    fn test_no_transform_single_sum() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("a") + lit(1))])?
            .build()?;

        let rule = RewriteAggregateWithConstant::new();
        let config = OptimizerContext::new();
        let result = rule.rewrite(plan, &config)?;

        // Should NOT be transformed (only one SUM)
        assert!(!result.transformed);
        Ok(())
    }

    #[test]
    fn test_no_transform_no_constant() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("a")), sum(col("b"))])?
            .build()?;

        let rule = RewriteAggregateWithConstant::new();
        let config = OptimizerContext::new();
        let result = rule.rewrite(plan, &config)?;

        // Should NOT be transformed (no constants)
        assert!(!result.transformed);
        Ok(())
    }

    /// Test that aliased SUM(col ± constant) expressions are correctly detected
    /// and rewritten. This exercises the Expr::Alias unwrapping in
    /// `extract_sum_with_constant`.
    ///
    /// Note: The SQL planner puts aliases in a Projection above the Aggregate,
    /// so this case only arises when building plans via the LogicalPlanBuilder API.
    #[test]
    fn test_aliased_sum_with_constant() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    sum(col("a") + lit(1)).alias("sum_a_plus_1"),
                    sum(col("a") + lit(2)).alias("sum_a_plus_2"),
                ],
            )?
            .build()?;

        let rule = RewriteAggregateWithConstant::new();
        let config = OptimizerContext::new();
        let result = rule.rewrite(plan, &config)?;

        // Should be transformed: both aliased SUMs share base column "a"
        assert!(result.transformed);

        // The rewritten plan should be a Projection on top of an Aggregate
        let plan_str = format!("{}", result.data.display_indent());
        assert!(
            plan_str.contains("Projection:"),
            "Expected Projection node in rewritten plan, got:\n{plan_str}"
        );
        // The new aggregate should have SUM(a) and COUNT(a), not two separate SUMs
        assert!(
            plan_str.contains("sum(test.a)") && plan_str.contains("count(test.a)"),
            "Expected SUM(a) and COUNT(a) in rewritten plan, got:\n{plan_str}"
        );
        Ok(())
    }

    /// Test that an aliased plain SUM(a) alongside SUM(a+1) and SUM(a+2) is
    /// correctly detected as a "plain SUM in group" and reused, rather than
    /// being duplicated in the new Aggregate node.
    ///
    /// This exercises the Expr::Alias unwrapping in `check_plain_sum_in_group`.
    #[test]
    fn test_aliased_plain_sum_in_group() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    // This plain SUM(a) is aliased — it should be detected
                    // as matching the rewrite group for "a" and reused.
                    sum(col("a")).alias("total_a"),
                    sum(col("a") + lit(1)),
                    sum(col("a") + lit(2)),
                ],
            )?
            .build()?;

        let rule = RewriteAggregateWithConstant::new();
        let config = OptimizerContext::new();
        let result = rule.rewrite(plan, &config)?;

        // Should be transformed
        assert!(result.transformed);

        let plan_str = format!("{}", result.data.display_indent());

        // The rewritten plan should contain "total_a" alias in the Projection
        assert!(
            plan_str.contains("total_a"),
            "Expected alias 'total_a' in rewritten plan, got:\n{plan_str}"
        );

        // The Aggregate node should contain exactly one SUM(a) and one COUNT(a),
        // NOT a duplicate SUM(a). Count occurrences of "sum(test.a)" in the
        // Aggregate line (not the Projection line).
        let aggr_line = plan_str
            .lines()
            .find(|l| l.contains("Aggregate:"))
            .expect("should have Aggregate node");
        let sum_count = aggr_line.matches("sum(test.a)").count();
        assert_eq!(
            sum_count, 1,
            "Expected exactly 1 SUM(test.a) in Aggregate (no duplicate), got {sum_count} in:\n{aggr_line}"
        );

        Ok(())
    }
}
