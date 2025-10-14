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

use std::collections::HashMap;

use crate::optimizer::ApplyOrder;
use crate::utils::{build_schema_remapping, replace_cols_by_name};
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{qualified_name, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Expr, LogicalPlanContext, ScanOrdering, SortExpr};

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
                        return Ok(Transformed::yes(SortPushdownContext::new_default(
                            LogicalPlan::TableScan(new_table_scan),
                        )));
                    }
                    // No sort expressions to push down or cannot push down, return original plan
                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::Sort(ref sort) => {
                    // Update current sort expressions to the new ones
                    ctx.data = Some(sort.expr.clone());
                    // Propagate sort expressions to all children
                    let sort_exprs = ctx.data.clone();
                    for child in ctx.children.iter_mut() {
                        child.data = sort_exprs.clone();
                    }
                    // Continue recursion with updated sort expressions
                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::Projection(ref projection) => {
                    // We can only push down sort expressions through a projection if the expression we are sorting on was not created by the projection itself.
                    // We may also need to re-write sort expressions to reverse aliasing done by the projection.

                    if let Some(sort_exprs) = &ctx.data {
                        // Build projection mapping: output column name -> underlying input expression
                        let projection_map: HashMap<String, Expr> = projection
                            .schema
                            .iter()
                            .zip(projection.expr.iter())
                            .map(|((qualifier, field), expr)| {
                                // Strip alias, as they should not be part of sort expressions
                                (
                                    qualified_name(qualifier, field.name()),
                                    expr.clone().unalias(),
                                )
                            })
                            .collect();

                        // Rewrite sort expressions through the projection, stopping at first failure.
                        // We push down whatever prefix we can, but if any expression cannot be rewritten
                        // we drop them from the sort pushdown.
                        // For example, given the projection `a as a, b as b, c + d + 1 as cd1` and the sort expression `a, cd1, b`
                        // we will only be able to push down the sort expression `a,` but not `cd1` as it is not a simple column reference and not `b` as it comes after `cd1`.
                        let mut rewritten_sorts = Vec::new();
                        for sort_expr in sort_exprs {
                            match replace_cols_by_name(
                                sort_expr.expr.clone(),
                                &projection_map,
                            ) {
                                Ok(rewritten_expr) => {
                                    // Successfully rewritten, keep it in the pushdown list
                                    rewritten_sorts.push(SortExpr {
                                        expr: rewritten_expr,
                                        asc: sort_expr.asc,
                                        nulls_first: sort_expr.nulls_first,
                                    });
                                }
                                Err(_) => {
                                    // Cannot rewrite this expression, stop here (partial pushdown)
                                    break;
                                }
                            }
                        }

                        // Update context with the rewritten sort expressions (or None if empty)
                        ctx.data = if rewritten_sorts.is_empty() {
                            None
                        } else {
                            Some(rewritten_sorts.clone())
                        };

                        // Propagate rewritten sort expressions to children
                        let data_to_propagate = ctx.data.clone();
                        for child in ctx.children.iter_mut() {
                            child.data = data_to_propagate.clone();
                        }

                        return Ok(Transformed::no(ctx));
                    }

                    // Continue recursion with potentially updated sort expressions
                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::SubqueryAlias(ref subquery_alias) => {
                    // Similar to Projection, we need to rewrite sort expressions through the SubqueryAlias
                    // by replacing column references that use the alias qualifier with the underlying
                    // table qualifier.

                    if let Some(sort_exprs) = &ctx.data {
                        // Build mapping: alias.field_name -> underlying Column(qualifier, field_name)
                        let replace_map = build_schema_remapping(
                            &subquery_alias.schema,
                            subquery_alias.input.schema(),
                        );

                        // Rewrite sort expressions to use underlying qualifiers
                        let mut rewritten_sorts = Vec::new();
                        for sort_expr in sort_exprs {
                            match replace_cols_by_name(
                                sort_expr.expr.clone(),
                                &replace_map,
                            ) {
                                Ok(rewritten_expr) => {
                                    rewritten_sorts.push(SortExpr {
                                        expr: rewritten_expr,
                                        asc: sort_expr.asc,
                                        nulls_first: sort_expr.nulls_first,
                                    });
                                }
                                Err(_) => {
                                    // Cannot rewrite, stop here
                                    break;
                                }
                            }
                        }

                        // Update context with rewritten sort expressions
                        ctx.data = if rewritten_sorts.is_empty() {
                            None
                        } else {
                            Some(rewritten_sorts.clone())
                        };

                        // Propagate rewritten sort expressions to children
                        let data_to_propagate = ctx.data.clone();
                        for child in ctx.children.iter_mut() {
                            child.data = data_to_propagate.clone();
                        }
                    } else {
                        // No sort expressions to propagate
                        for child in ctx.children.iter_mut() {
                            child.data = None;
                        }
                    }

                    Ok(Transformed::no(ctx))
                }
                LogicalPlan::Filter(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::Limit(_) => {
                    // Propagate current sort expressions to children without modification
                    let current_data = ctx.data.clone();
                    for child in ctx.children.iter_mut() {
                        child.data = current_data.clone();
                    }
                    // Continue recursion without modifying current sort expressions
                    Ok(Transformed::no(ctx))
                }
                _ => {
                    // Cannot push sort expressions through this node type
                    // Clear sort expressions from both context and children
                    ctx.data = None;
                    for child in ctx.children.iter_mut() {
                        child.data = None;
                    }
                    Ok(Transformed::no(ctx))
                }
            }
        })
        .and_then(|transformed_ctx| transformed_ctx.map_data(|ctx| Ok(ctx.plan)))
    }

    fn name(&self) -> &str {
        "push_down_sort"
    }
}

type SortPushdownContext = LogicalPlanContext<Option<Vec<SortExpr>>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::*;
    use crate::OptimizerContext;
    use datafusion_common::Result;
    use datafusion_expr::test::function_stub::sum;
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, Expr, ExprFunctionExt,
    };
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(PushDownSort::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    // ===== Basic Sort Pushdown Tests =====

    #[test]
    fn sort_before_table_scan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_with_multiple_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                col("a").sort(true, false),
                col("b").sort(false, true),
                col("c").sort(true, true),
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST, test.c ASC NULLS FIRST
          TableScan: test preferred_ordering=[test.a ASC NULLS LAST, test.b DESC NULLS FIRST, test.c ASC NULLS FIRST]
        "
        )
    }

    #[test]
    fn sort_with_options() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                col("a").sort(false, true), // DESC NULLS FIRST
                col("b").sort(true, true),  // ASC NULLS FIRST
            ])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a DESC NULLS FIRST, test.b ASC NULLS FIRST
          TableScan: test preferred_ordering=[test.a DESC NULLS FIRST, test.b ASC NULLS FIRST]
        "
        )
    }

    #[test]
    fn no_sort_no_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan).build()?;

        assert_optimized_plan_equal!(
            plan,
            @"TableScan: test"
        )
    }

    // ===== Sort Through Projection Tests =====

    #[test]
    fn sort_through_simple_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_through_projection_with_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a").alias("b"), col("c")])?
            .sort(vec![col("b").sort(true, false)])?
            .build()?;

        // Sort on aliased column 'b' should be rewritten to sort on 'a'
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: b ASC NULLS LAST
          Projection: test.a AS b, test.c
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_through_complex_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), (col("b") + col("c")).alias("bc")])?
            .sort(vec![col("bc").sort(true, false)])?
            .build()?;

        // Sort on computed column can push down as complex expression
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: bc ASC NULLS LAST
          Projection: test.a, test.b + test.c AS bc
            TableScan: test preferred_ordering=[test.b + test.c ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_through_multiple_projections() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .project(vec![col("a"), col("c")])?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.c
            Projection: test.a, test.b, test.c
              TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_rewrites_expression_to_source_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![
                col("a"),
                (col("b") + col("c") + lit(1)).alias("bc1"),
                col("c"),
            ])?
            .sort(vec![
                col("bc1").sort(true, false),
                col("c").sort(true, false),
            ])?
            .build()?;

        // We can successfully rewrite the sort expression `bc1` to `b + c + 1` and push it down
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: bc1 ASC NULLS LAST, test.c ASC NULLS LAST
          Projection: test.a, test.b + test.c + Int32(1) AS bc1, test.c
            TableScan: test preferred_ordering=[test.b + test.c + Int32(1) ASC NULLS LAST, test.c ASC NULLS LAST]
        "
        )
    }

    // ===== Sort Through Filter Tests =====

    #[test]
    fn sort_through_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").gt(lit(10i64)))?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Filter: test.a > Int64(10)
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_through_filter_and_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .filter(col("a").gt(lit(10i64)))?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Filter: test.a > Int64(10)
            Projection: test.a, test.b
              TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    // ===== Sort Through Repartition Tests =====

    #[test]
    fn sort_through_repartition() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .repartition(datafusion_expr::Partitioning::RoundRobinBatch(4))?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Repartition: RoundRobinBatch partition_count=4
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    // ===== Sort Replacement Tests =====

    #[test]
    fn multiple_sorts() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a").sort(true, false)])?
            .sort(vec![col("b").sort(false, true)])?
            .build()?;

        // The innermost sort should be what gets pushed down
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.b DESC NULLS FIRST
          Sort: test.a ASC NULLS LAST
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_updates_existing_sort() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a").sort(true, false), col("b").sort(true, false)])?
            .project(vec![col("a"), col("b"), col("c")])?
            .sort(vec![col("c").sort(false, true)])?
            .build()?;

        // Innermost sort should be what gets pushed down
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.c DESC NULLS FIRST
          Projection: test.a, test.b, test.c
            Sort: test.a ASC NULLS LAST, test.b ASC NULLS LAST
              TableScan: test preferred_ordering=[test.a ASC NULLS LAST, test.b ASC NULLS LAST]
        "
        )
    }

    // ===== Boundary Case Tests =====

    #[test]
    fn sort_blocked_by_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(10))?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Sort can push through limit
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Limit: skip=0, fetch=10
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_blocked_by_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Sort cannot push through aggregate
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
            TableScan: test
        "
        )
    }

    #[test]
    fn sort_blocked_by_join() -> Result<()> {
        let left = test_table_scan()?;
        let right = test_table_scan_with_name("test2")?;
        let plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                datafusion_expr::JoinType::Inner,
                (
                    vec![datafusion_common::Column::from_name("a")],
                    vec![datafusion_common::Column::from_name("a")],
                ),
                None,
            )?
            .sort(vec![col("test.a").sort(true, false)])?
            .build()?;

        // Sort cannot push through join
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          Inner Join: test.a = test2.a
            TableScan: test
            TableScan: test2
        "
        )
    }

    #[test]
    fn sort_blocked_by_window() -> Result<()> {
        let table_scan = test_table_scan()?;

        let window = Expr::from(datafusion_expr::expr::WindowFunction::new(
            datafusion_expr::WindowFunctionDefinition::WindowUDF(
                datafusion_functions_window::rank::rank_udwf(),
            ),
            vec![],
        ))
        .partition_by(vec![col("a")])
        .order_by(vec![col("b").sort(true, true)])
        .build()
        .unwrap();

        let plan = LogicalPlanBuilder::from(table_scan)
            .window(vec![window])?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Sort cannot push through window
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.a ASC NULLS LAST
          WindowAggr: windowExpr=[[rank() PARTITION BY [test.a] ORDER BY [test.b ASC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
            TableScan: test
        "
        )
    }

    // ===== Edge Case Tests =====

    #[test]
    fn sort_with_special_column_names() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_expr::logical_plan::table_scan;

        let schema = Schema::new(vec![
            Field::new("$a", DataType::UInt32, false),
            Field::new("$b", DataType::UInt32, false),
        ]);
        let table_scan = table_scan(Some("test"), &schema, None)?.build()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("$a").sort(true, false)])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: test.$a ASC NULLS LAST
          TableScan: test preferred_ordering=[test.$a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("subquery")?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Sort should pass through SubqueryAlias
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: subquery.a ASC NULLS LAST
          SubqueryAlias: subquery
            TableScan: test preferred_ordering=[test.a ASC NULLS LAST]
        "
        )
    }

    #[test]
    fn sort_with_union() -> Result<()> {
        let left = test_table_scan()?;
        let right = test_table_scan_with_name("test2")?;
        let plan = LogicalPlanBuilder::from(left)
            .union(right)?
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Sort cannot push through union
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: a ASC NULLS LAST
          Union
            TableScan: test
            TableScan: test2
        "
        )
    }

    // ===== Integration Tests =====

    #[test]
    fn complex_plan_with_sort() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])?
            .filter(col("b").gt(lit(5i64)))?
            .project(vec![col("a").alias("x"), col("c")])?
            .sort(vec![col("x").sort(true, false), col("c").sort(false, true)])?
            .build()?;

        // Sort should push through multiple projections and filter
        assert_optimized_plan_equal!(
            plan,
            @r"
        Sort: x ASC NULLS LAST, test.c DESC NULLS FIRST
          Projection: test.a AS x, test.c
            Filter: test.b > Int64(5)
              Projection: test.a, test.b, test.c
                TableScan: test preferred_ordering=[test.a ASC NULLS LAST, test.c DESC NULLS FIRST]
        "
        )
    }

    #[test]
    fn sort_preserves_original_node() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a").sort(true, false)])?
            .build()?;

        // Verify the original Sort node is preserved
        let optimized = {
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(PushDownSort::new())];
            let optimizer = crate::Optimizer::with_rules(rules);
            optimizer.optimize(plan, &optimizer_ctx, |_, _| {})?
        };

        // Check that the optimized plan still has a Sort node at the top
        assert!(matches!(optimized, LogicalPlan::Sort(_)));

        Ok(())
    }
}
