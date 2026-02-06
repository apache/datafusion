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

use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunctionParams, Sort};
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

/// Internal structure to track metadata for a SUM expression that qualifies for rewrite
#[derive(Debug, Clone)]
struct SumWithConstant {
    /// The inner expression being manipulated (e.g., the 'a' in SUM(a + 1))
    base_expr: Expr,
    /// The constant value being added/subtracted
    constant: ScalarValue,
    /// The operator (+ or -)
    operator: Operator,
    /// The index in the original Aggregate's aggr_expr list, used to maintain output order
    original_index: usize,
    /// Any ORDER BY clause inside the aggregate (e.g., SUM(a+1 ORDER BY b))
    order_by: Vec<Sort>,
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
                order_by,
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
                if let Expr::Literal(constant, _) = right.as_ref()
                    && is_numeric_constant(constant)
                {
                    return Ok(Some(SumWithConstant {
                        base_expr: (**left).clone(),
                        constant: constant.clone(),
                        operator: *op,
                        original_index: idx,
                        order_by: order_by.clone(),
                    }));
                }

                // Also check left side (for patterns like: constant + base_expr)
                if let Expr::Literal(constant, _) = left.as_ref()
                    && is_numeric_constant(constant)
                    && *op == Operator::Plus
                {
                    return Ok(Some(SumWithConstant {
                        base_expr: (**right).clone(),
                        constant: constant.clone(),
                        operator: Operator::Plus,
                        original_index: idx,
                        order_by: order_by.clone(),
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

/// Check if an expression is a plain SUM(base_expr) that matches one of our rewrite groups.
/// Handles both `Expr::AggregateFunction(...)` and `Expr::Alias(Expr::AggregateFunction(...))`
/// so that aliased plain SUMs (e.g., `SUM(a) AS total`) are correctly detected and reused
/// instead of being duplicated in the new Aggregate node.
fn check_plain_sum_in_group(
    expr: &Expr,
    base_expr_indices: &IndexMap<String, (usize, usize)>,
) -> Option<(usize, usize)> {
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
        return base_expr_indices.get(&base_key).copied();
    }
    None
}

/// Transform the aggregate plan by rewriting SUM(col ± constant) expressions
fn transform_aggregate(
    aggregate: Aggregate,
    rewrite_groups: &RewriteGroups,
) -> Result<Transformed<LogicalPlan>> {
    let mut new_aggr_exprs = Vec::new();
    let mut projection_exprs = Vec::new();

    // Build a flat list of all SUMs to rewrite, sorted by original index
    let mut all_sums: Vec<SumWithConstant> = rewrite_groups
        .values()
        .flat_map(|v| v.iter().cloned())
        .collect();
    all_sums.sort_by_key(|s| s.original_index);

    // Maps base column names to the indices of their new SUM/COUNT in the new Aggregate node
    let mut base_expr_indices: IndexMap<String, (usize, usize)> = IndexMap::new();

    // Process each group to determine what to add to the aggregate
    let mut sum_names: IndexMap<String, String> = IndexMap::new();
    let mut count_names: IndexMap<String, String> = IndexMap::new();

    // For every group (e.g., all SUMs involving column 'a'), add one SUM(a) and one COUNT(a)
    for (base_key, sums) in rewrite_groups {
        // If any original SUM had an ORDER BY, we try to preserve it in our new base SUM.
        let representative = sums
            .iter()
            .find(|s| !s.order_by.is_empty())
            .unwrap_or(&sums[0]);

        // Add SUM(base_expr) with ORDER BY preserved
        let sum_expr = sum(representative.base_expr.clone());
        // Note: ORDER BY is not needed for SUM as it's commutative
        let sum_name = sum_expr.schema_name().to_string();
        let sum_index = new_aggr_exprs.len();
        new_aggr_exprs.push(sum_expr);
        sum_names.insert(base_key.clone(), sum_name);

        // Add the base COUNT(a)
        // We use COUNT(col) rather than COUNT(*) because if 'col' is NULL, 
        // SUM(col + 1) should be NULL, and COUNT(col) correctly returns 0 for NULLs,
        // whereas COUNT(*) would count the row.
        let count_expr = count(representative.base_expr.clone());
        let count_name = count_expr.schema_name().to_string();
        let count_index = new_aggr_exprs.len();
        new_aggr_exprs.push(count_expr);
        count_names.insert(base_key.clone(), count_name);

        base_expr_indices.insert(base_key.clone(), (sum_index, count_index));
    }

    // Now iterate through the ORIGINAL aggregate expressions to build the PROJECTION
    for (idx, orig_expr) in aggregate.aggr_expr.iter().enumerate() {
        // Check if this expression should be rewritten
        let rewritten = all_sums.iter().find(|s| s.original_index == idx);

        let projection_expr = if let Some(sum_info) = rewritten {
            let base_key = sum_info.base_expr.schema_name().to_string();

            // Construct the math: SUM(col) [±] (constant * COUNT(col))
            let sum_ref = col(&sum_names[&base_key]);
            let count_ref = col(&count_names[&base_key]);

            let multiplied = binary_expr(
                lit(sum_info.constant.clone()),
                Operator::Multiply,
                count_ref,
            );

            let result = binary_expr(sum_ref, sum_info.operator, multiplied);

            // Ensure the output column name matches the original (aliased or generated)
            match orig_expr {
                Expr::Alias(alias) => result.alias(alias.name.clone()),
                _ => result.alias(orig_expr.schema_name().to_string()),
            }
        } else {
            // Special case: If the user had a plain SUM(a) alongside SUM(a+1),
            // we should reuse the SUM(a) we just added instead of adding another one.
            let is_plain_sum_in_group =
                check_plain_sum_in_group(orig_expr, &base_expr_indices);

            if is_plain_sum_in_group.is_some() {
                // Use the already-computed SUM.
                // Unwrap alias to get to the AggregateFunction and extract the base key.
                let inner_expr = match orig_expr {
                    Expr::Alias(alias) => alias.expr.as_ref(),
                    other => other,
                };
                let base_key = if let Expr::AggregateFunction(agg_fn) = inner_expr {
                    agg_fn.params.args[0].schema_name().to_string()
                } else {
                    String::new()
                };
                let sum_ref = col(&sum_names[&base_key]);
                match orig_expr {
                    Expr::Alias(alias) => sum_ref.alias(alias.name.clone()),
                    _ => sum_ref.alias(orig_expr.schema_name().to_string()),
                }
            } else {
                // This expression is unrelated to our rewrites (e.g., AVG(b) or MAX(c)).
                // We just pass it through to the new Aggregate node.
                new_aggr_exprs.push(orig_expr.clone());

                // And reference it in the projection by name.
                match orig_expr {
                    Expr::Alias(alias) => col(alias.name.clone()),
                    _ => col(orig_expr.schema_name().to_string()),
                }
            }
        };

        projection_exprs.push(projection_expr);
    }

    // Handle GROUP BY columns: they must be passed through the Aggregate and Projection
    let group_exprs: Vec<Expr> = aggregate
        .group_expr
        .iter()
        .map(|e| match e {
            Expr::Alias(alias) => col(alias.name.clone()),
            Expr::Column(c) => Expr::Column(c.clone()),
            _ => col(e.schema_name().to_string()),
        })
        .collect();

    // Final projection includes [Group Columns] + [Aggregated/Rewritten Columns]
    let mut final_projection = group_exprs;
    final_projection.extend(projection_exprs);

    // Create the new Aggregate plan node
    let new_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        aggregate.input,
        aggregate.group_expr,
        new_aggr_exprs,
    )?);

    // Wrap the Aggregate with the Projection
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
