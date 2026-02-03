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

//! [`RewriteAggregateWithConstant`] rewrites `SUM(column ± constant)` to `SUM(column) ± constant * COUNT(*)`

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
use std::collections::HashMap;

/// Optimizer rule that rewrites `SUM(column ± constant)` expressions
/// into `SUM(column) ± constant * COUNT(*)` when multiple such expressions
/// exist for the same base column.
///
/// This reduces computation by calculating SUM once and deriving other values.
///
/// # Example
/// ```sql
/// SELECT SUM(a), SUM(a + 1), SUM(a + 2) FROM t;
/// ```
/// is rewritten to:
/// ```sql
/// SELECT sum_a, sum_a + 1 * count_a, sum_a + 2 * count_a
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
            LogicalPlan::Aggregate(aggregate) => {
                // Check if we can apply the transformation
                let rewrite_info = analyze_aggregate(&aggregate)?;

                if rewrite_info.is_empty() {
                    // No transformation possible
                    return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
                }

                // Build the transformed plan
                transform_aggregate(aggregate, &rewrite_info)
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "rewrite_aggregate_with_constant"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

/// Information about a SUM expression with a constant offset
#[derive(Debug, Clone)]
struct SumWithConstant {
    /// The base expression (e.g., column 'a' in SUM(a + 1))
    base_expr: Expr,
    /// The constant value being added/subtracted
    constant: ScalarValue,
    /// The operator (+ or -)
    operator: Operator,
    /// Original index in the aggregate expressions
    original_index: usize,
    /// ORDER BY clause if present
    order_by: Vec<Sort>,
}

/// Information about groups of SUMs that can be rewritten
type RewriteGroups = HashMap<String, Vec<SumWithConstant>>;

/// Analyze the aggregate to find groups of SUM(col ± constant) that can be rewritten
fn analyze_aggregate(aggregate: &Aggregate) -> Result<RewriteGroups> {
    let mut groups: RewriteGroups = HashMap::new();

    for (idx, expr) in aggregate.aggr_expr.iter().enumerate() {
        if let Some(sum_info) = extract_sum_with_constant(expr, idx)? {
            let key = sum_info.base_expr.schema_name().to_string();
            groups.entry(key).or_default().push(sum_info);
        }
    }

    // Only keep groups with 2 or more SUMs on the same base column
    groups.retain(|_, v| v.len() >= 2);

    Ok(groups)
}

/// Extract SUM(base_expr ± constant) pattern from an expression
fn extract_sum_with_constant(expr: &Expr, idx: usize) -> Result<Option<SumWithConstant>> {
    match expr {
        Expr::AggregateFunction(agg_fn) => {
            // Must be SUM function
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

            // Skip if DISTINCT or FILTER present
            if *distinct || filter.is_some() {
                return Ok(None);
            }

            // Must have exactly one argument
            if args.len() != 1 {
                return Ok(None);
            }

            let arg = &args[0];

            // Try to match: base_expr +/- constant
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

/// Check if an expression is a plain SUM(base_expr) that matches one of our rewrite groups
fn check_plain_sum_in_group(
    expr: &Expr,
    base_expr_indices: &HashMap<String, (usize, usize)>,
) -> Option<(usize, usize)> {
    if let Expr::AggregateFunction(agg_fn) = expr
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

    // Track which base expressions we've already added SUM/COUNT for
    let mut base_expr_indices: HashMap<String, (usize, usize)> = HashMap::new();

    // Process each group to determine what to add to the aggregate
    let mut sum_names: HashMap<String, String> = HashMap::new();
    let mut count_names: HashMap<String, String> = HashMap::new();

    for (base_key, sums) in rewrite_groups {
        // Find a representative SUM (prefer one with ORDER BY if any)
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

        // Add COUNT - use COUNT(col) for nullable columns
        // For nullable columns, COUNT(col) correctly excludes NULLs
        let count_expr = count(representative.base_expr.clone());
        let count_name = count_expr.schema_name().to_string();

        let count_index = new_aggr_exprs.len();
        new_aggr_exprs.push(count_expr);
        count_names.insert(base_key.clone(), count_name);

        base_expr_indices.insert(base_key.clone(), (sum_index, count_index));
    }

    // Now build projection expressions for all original aggregate expressions
    for (idx, orig_expr) in aggregate.aggr_expr.iter().enumerate() {
        // Check if this expression should be rewritten
        let rewritten = all_sums.iter().find(|s| s.original_index == idx);

        let projection_expr = if let Some(sum_info) = rewritten {
            let base_key = sum_info.base_expr.schema_name().to_string();

            // Build: SUM(col) ± constant * COUNT(...)
            let sum_ref = col(&sum_names[&base_key]);
            let count_ref = col(&count_names[&base_key]);

            let multiplied = binary_expr(
                lit(sum_info.constant.clone()),
                Operator::Multiply,
                count_ref,
            );

            let result = binary_expr(sum_ref, sum_info.operator, multiplied);

            // Preserve original alias if present
            match orig_expr {
                Expr::Alias(alias) => result.alias(alias.name.clone()),
                _ => result.alias(orig_expr.schema_name().to_string()),
            }
        } else {
            // Check if this is a plain SUM(base_expr) that we're already computing
            let is_plain_sum_in_group =
                check_plain_sum_in_group(orig_expr, &base_expr_indices);

            if is_plain_sum_in_group.is_some() {
                // Use the already-computed SUM
                let base_key = if let Expr::AggregateFunction(agg_fn) = orig_expr {
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
                // Keep non-rewritten expressions as-is
                new_aggr_exprs.push(orig_expr.clone());

                match orig_expr {
                    Expr::Alias(alias) => col(alias.name.clone()),
                    _ => col(orig_expr.schema_name().to_string()),
                }
            }
        };

        projection_exprs.push(projection_expr);
    }

    // Also add group by expressions to projection
    let group_exprs: Vec<Expr> = aggregate
        .group_expr
        .iter()
        .map(|e| match e {
            Expr::Alias(alias) => col(alias.name.clone()),
            Expr::Column(c) => Expr::Column(c.clone()),
            _ => col(e.schema_name().to_string()),
        })
        .collect();

    // Prepend group expressions to projection
    let mut final_projection = group_exprs;
    final_projection.extend(projection_exprs);

    // Create new aggregate with rewritten expressions
    let new_aggregate = LogicalPlan::Aggregate(Aggregate::try_new(
        aggregate.input,
        aggregate.group_expr,
        new_aggr_exprs,
    )?);

    // Wrap with projection
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
}
