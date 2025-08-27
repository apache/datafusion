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

use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::logical_plan::{LogicalPlan, TableScan};
use datafusion_expr::{Expr, SortExpr};

/// Optimization rule that pushes sort expressions down to table scans
/// when the sort can potentially be optimized by the table provider.
///
/// This rule looks for `Sort -> TableScan` patterns and moves the sort
/// expressions into the `TableScan.preferred_ordering` field, allowing
/// table providers to potentially optimize the scan based on sort requirements.
#[derive(Default, Debug)]
pub struct PushDownSort {}

impl PushDownSort {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Checks if a sort expression can be pushed down to a table scan.
    ///
    /// Currently, we only support pushing down simple column references
    /// because table providers typically can't optimize complex expressions
    /// in sort pushdown.
    fn can_pushdown_sort_expr(expr: &SortExpr) -> bool {
        // Only push down simple column references
        matches!(expr.expr, Expr::Column(_))
    }

    /// Checks if all sort expressions in a list can be pushed down.
    fn can_pushdown_sort_exprs(sort_exprs: &[SortExpr]) -> bool {
        sort_exprs.iter().all(Self::can_pushdown_sort_expr)
    }
}

impl OptimizerRule for PushDownSort {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Look for Sort -> TableScan pattern
        let LogicalPlan::Sort(sort) = &plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::TableScan(table_scan) = sort.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };

        // Check if we can push down the sort expressions
        if !Self::can_pushdown_sort_exprs(&sort.expr) {
            return Ok(Transformed::no(plan));
        }

        // If the table scan already has preferred ordering, don't overwrite it
        // This preserves any existing sort preferences from other optimizations
        if table_scan.preferred_ordering.is_some() {
            return Ok(Transformed::no(plan));
        }

        // Create new TableScan with preferred ordering
        let new_table_scan = TableScan {
            table_name: table_scan.table_name.clone(),
            source: Arc::clone(&table_scan.source),
            projection: table_scan.projection.clone(),
            projected_schema: Arc::clone(&table_scan.projected_schema),
            filters: table_scan.filters.clone(),
            fetch: table_scan.fetch,
            preferred_ordering: Some(sort.expr.clone()),
        };

        // Preserve the Sort node as a fallback while passing the ordering
        // preference to the TableScan as an optimization hint
        let new_sort = datafusion_expr::logical_plan::Sort {
            expr: sort.expr.clone(),
            input: Arc::new(LogicalPlan::TableScan(new_table_scan)),
            fetch: sort.fetch,
        };
        let new_plan = LogicalPlan::Sort(new_sort);

        Ok(Transformed::yes(new_plan))
    }

    fn name(&self) -> &str {
        "push_down_sort"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::test_table_scan;
    use crate::{assert_optimized_plan_eq_snapshot, OptimizerContext};
    use datafusion_common::{Column, Result};
    use datafusion_expr::{col, lit, Expr, JoinType, LogicalPlanBuilder, SortExpr};
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(PushDownSort::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn test_can_pushdown_sort_expr() {
        // Simple column reference should be pushable
        let sort_expr = SortExpr::new(col("a"), true, false);
        assert!(PushDownSort::can_pushdown_sort_expr(&sort_expr));

        // Complex expression should not be pushable
        let sort_expr = SortExpr::new(col("a") + col("b"), true, false);
        assert!(!PushDownSort::can_pushdown_sort_expr(&sort_expr));

        // Function call should not be pushable
        let sort_expr = SortExpr::new(col("c").like(lit("test%")), true, false);
        assert!(!PushDownSort::can_pushdown_sort_expr(&sort_expr));

        // Literal should not be pushable
        let sort_expr = SortExpr::new(lit(42), true, false);
        assert!(!PushDownSort::can_pushdown_sort_expr(&sort_expr));
    }

    #[test]
    fn test_can_pushdown_sort_exprs() {
        // All simple columns should be pushable
        let sort_exprs = vec![
            SortExpr::new(col("a"), true, false),
            SortExpr::new(col("b"), false, true),
        ];
        assert!(PushDownSort::can_pushdown_sort_exprs(&sort_exprs));

        // Mix of simple and complex should not be pushable
        let sort_exprs = vec![
            SortExpr::new(col("a"), true, false),
            SortExpr::new(col("a") + col("b"), false, true),
        ];
        assert!(!PushDownSort::can_pushdown_sort_exprs(&sort_exprs));

        // Empty list should be pushable
        let sort_exprs = vec![];
        assert!(PushDownSort::can_pushdown_sort_exprs(&sort_exprs));
    }

    #[test]
    fn test_basic_sort_pushdown_to_table_scan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort node is preserved with preferred_ordering passed to TableScan
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_multiple_column_sort_pushdown() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false),
                SortExpr::new(col("b"), false, true),
            ])?
            .build()?;

        // Multi-column sort is preserved with preferred_ordering passed to TableScan
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_sort_node_preserved_with_preferred_ordering() -> Result<()> {
        let rule = PushDownSort::new();
        let table_scan = test_table_scan()?;
        let sort_plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        let config = &OptimizerContext::new();
        let result = rule.rewrite(sort_plan, config)?;

        // Verify Sort node is preserved
        match &result.data {
            LogicalPlan::Sort(sort) => {
                // Check that TableScan has preferred_ordering
                if let LogicalPlan::TableScan(ts) = sort.input.as_ref() {
                    assert!(ts.preferred_ordering.is_some());
                } else {
                    panic!("Expected TableScan input");
                }
            }
            _ => panic!("Expected Sort node to be preserved"),
        }

        Ok(())
    }

    #[test]
    fn test_no_pushdown_with_complex_expressions() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false),
                SortExpr::new(col("a") + col("b"), false, true), // Complex expression
            ])?
            .build()?;

        // Sort should remain unchanged
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.a + test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above projection
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").gt(lit(10)))?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above filter
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.a > Int32(10)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_aggregate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], Vec::<Expr>::new())?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above aggregate
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Aggregate: groupBy=[[test.a]], aggr=[[]]
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_join() -> Result<()> {
        let left_table = crate::test::test_table_scan_with_name("t1")?;
        let right_table = crate::test::test_table_scan_with_name("t2")?;

        let plan = LogicalPlanBuilder::from(left_table)
            .join(
                right_table,
                JoinType::Inner,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .sort(vec![SortExpr::new(
                Expr::Column(Column::new(Some("t1"), "a")),
                true,
                false,
            )])?
            .build()?;

        // Sort should remain above join
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: t1.a ASC NULLS LAST
          Inner Join: t1.a = t2.a
            TableScan: t1
            TableScan: t2
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(10))?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above limit
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Limit: skip=0, fetch=10
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_through_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .distinct()?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort should remain above distinct
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Distinct:
            TableScan: test
        "
        )
    }

    #[test]
    fn test_no_pushdown_on_non_sort_nodes() -> Result<()> {
        let table_scan = test_table_scan()?;

        // TableScan should remain unchanged
        assert_optimized_plan_equal!(
            table_scan,
            @ r"TableScan: test"
        )
    }

    // Tests for node types that currently block sort pushdown

    #[test]
    fn test_potential_pushdown_through_subquery_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .alias("aliased_table")?
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort remains above SubqueryAlias
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: aliased_table.a ASC NULLS LAST
          SubqueryAlias: aliased_table
            TableScan: test
        "
        )
    }

    #[test]
    fn test_potential_pushdown_through_order_preserving_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b"), col("c")])? // Identity projection - doesn't change column order
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Sort remains above Projection (conservative approach)
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Projection: test.a, test.b, test.c
            TableScan: test
        "
        )
    }

    #[test]
    fn test_potential_pushdown_through_order_preserving_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("b").gt(lit(0)))? // Filter on different column than sort
            .sort(vec![SortExpr::new(col("a"), true, false)])?
            .build()?;

        // Currently: Sort remains above Filter (conservative approach)
        // Future enhancement: Could push through filters that don't affect sort column relationships
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          Filter: test.b > Int32(0)
            TableScan: test
        "
        )
    }

    #[test]
    fn test_edge_case_empty_sort_expressions() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(Vec::<SortExpr>::new())? // Empty sort
            .build()?;

        // Empty sort is preserved
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: 
          TableScan: test
        "
        )
    }

    #[test]
    fn test_sort_with_nulls_first_last_variants() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false), // ASC NULLS LAST
                SortExpr::new(col("b"), true, true),  // ASC NULLS FIRST
                SortExpr::new(col("c"), false, false), // DESC NULLS LAST
            ])?
            .build()?;

        // All variants of nulls ordering should be pushable for simple columns
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b ASC NULLS FIRST, test.c DESC NULLS LAST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_mixed_simple_and_qualified_columns() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![
                SortExpr::new(col("a"), true, false), // Simple column
                SortExpr::new(Expr::Column(Column::new(Some("test"), "b")), false, true), // Qualified column
            ])?
            .build()?;

        // Both simple and qualified column references should be pushable
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST, test.b DESC NULLS FIRST
          TableScan: test
        "
        )
    }

    #[test]
    fn test_case_sensitive_column_references() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![SortExpr::new(col("A"), true, false)])? // Capital A
            .build()?;

        // Column reference case sensitivity should be handled by the schema
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Sort: test.a ASC NULLS LAST
          TableScan: test
        "
        )
    }
}
