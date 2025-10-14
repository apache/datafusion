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

//! [`PushDownSort`] pushes sort expressions into table scans to enable
//! sort pushdown optimizations by table providers

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{ConcreteTreeNode, Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanContext, ScanOrdering, SortExpr};
use log::Log;

/// Optimization rule that pushes sort expressions down to table scans
/// when the sort can potentially be optimized by the table provider.
///
/// This rule looks for `Sort -> TableScan` patterns and moves the sort
/// expressions into the `TableScan.preferred_ordering` field, allowing
/// table providers to potentially optimize the scan based on sort requirements.
///
/// # Behavior
///
/// The optimizer preserves the original `Sort` node as a fallback while passing
/// the ordering preference to the `TableScan` as an optimization hint. This ensures
/// correctness even if the table provider cannot satisfy the requested ordering.
///
/// # Supported Sort Expressions
///
/// Currently, only simple column references are supported for pushdown because
/// table providers typically cannot optimize complex expressions in sort operations.
/// Complex expressions like `col("a") + col("b")` or function calls are not pushed down.
///
/// # Examples
///
/// ```text
/// Before optimization:
/// Sort: test.a ASC NULLS LAST
///   TableScan: test
///
/// After optimization:
/// Sort: test.a ASC NULLS LAST  -- Preserved as fallback
///   TableScan: test            -- Now includes preferred_ordering hint
/// ```
#[derive(Default, Debug)]
pub struct PushDownSort {}

impl PushDownSort {
    /// Creates a new instance of the `PushDownSort` optimizer rule.
    ///
    /// # Returns
    ///
    /// A new `PushDownSort` optimizer rule that can be added to the optimization pipeline.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use datafusion_optimizer::push_down_sort::PushDownSort;
    ///
    /// let rule = PushDownSort::new();
    /// ```
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownSort {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    /// Recursively push down sort expressions through the logical plan tree.
    /// 
    /// We stop when we hit:
    /// 1. A TableScan leaf. In this case we bind the preferred ordering
    ///    to the TableScan node and return a new plan tree.
    /// 2. Any node that is not a Filter, Projection or SubqueryAlias. In this case
    ///    we clear the sort expressions and continue the recursion with no preferred
    ///    ordering.
    /// 3. A Sort node. In this case we replace the current sort expressions
    ///    with the new ones and continue the recursion.
    /// 
    /// # Arguments
    /// 
    /// * `plan` - The current logical plan node being processed.
    /// * `sort_exprs` - The current list of sort expressions to push down.
    /// 
    /// # Returns
    /// 
    /// A `Result` containing the transformed logical plan with sort expressions
    /// pushed down where possible.
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let ctx = SortPushdownContext::new_default(plan);
        ctx.transform_down(|mut ctx| {
            match &ctx.plan {
                LogicalPlan::TableScan(table_scan) => {
                    if let Some(sort_exprs) = &ctx.data {
                        // Create new TableScan with preferred ordering
                        let new_table_scan = table_scan.clone().with_ordering(
                            ScanOrdering::default()
                                .with_preferred_ordering(sort_exprs.to_vec()),
                        );
                        // Return new TableScan with preferred ordering
                        return Ok(Transformed::yes(SortPushdownContext::new_default(LogicalPlan::TableScan(new_table_scan))));
                    }
                    // No sort expressions to push down or cannot push down, return original plan
                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::Sort(ref sort) => {
                    // Update current sort expressions to the new ones
                    ctx.data = Some(sort.expr.clone());
                    // Continue recursion with updated sort expressions
                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::Projection(ref projection) => {
                    // We can only push down sort expressions through a projection if the expression we are sorting on was not created by the projection itself.
                    // We may also need to re-write sort expressions to reverse aliasing done by the projection.
                    todo!();
                }
                LogicalPlan::Filter(_) | LogicalPlan::Repartition(_) => {
                    // Continue recursion without modifying current sort expressions
                    Ok(Transformed::no(ctx))
                }
                _ => {
                    todo!()
                }
            }
        }).map(|transformed_ctx| {
            transformed_ctx.map_data(|ctx| Ok(ctx.plan))
        }).flatten()
    }

    fn name(&self) -> &str {
        "push_down_sort"
    }
}

type SortPushdownContext = LogicalPlanContext<Option<Vec<SortExpr>>>;

fn find_original_column_expression(
    sort_expr: &Expr,
    projection_exprs: &[Expr],
) -> Option<Expr> {
    match sort_expr {
        Expr::Column(_) => {
            // Direct column reference, check if it exists in projection expressions
            for expr in projection_exprs {
                if expr == sort_expr {
                    return Some(expr.clone());
                }
                if let Expr::Alias(alias_expr, _) = expr {
                    if alias_expr == sort_expr {
                        return Some(*alias_expr.clone());
                    }
                }
            }
            None
        }
        Expr::Alias(alias_expr, _) => {
            // Sort expression is an alias, find the original expression
            for expr in projection_exprs {
                if let Expr::Alias(proj_alias_expr, _) = expr {
                    if proj_alias_expr == alias_expr {
                        return Some(*proj_alias_expr.clone());
                    }
                }
            }
            None
        }
        _ => None, // Complex expressions are not supported for pushdown
    }
}
