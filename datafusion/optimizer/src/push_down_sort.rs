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

        // The sort can be completely eliminated since we've pushed it down
        // The table provider may or may not be able to satisfy the ordering,
        // but that's up to the table provider to decide
        let new_plan = LogicalPlan::TableScan(new_table_scan);

        Ok(Transformed::yes(new_plan))
    }

    fn name(&self) -> &str {
        "push_down_sort"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::{col, SortExpr};

    #[test]
    fn test_can_pushdown_sort_expr() {
        // Simple column reference should be pushable
        let sort_expr = SortExpr::new(col("a"), true, false);
        assert!(PushDownSort::can_pushdown_sort_expr(&sort_expr));

        // Complex expression should not be pushable
        let sort_expr = SortExpr::new(col("a") + col("b"), true, false);
        assert!(!PushDownSort::can_pushdown_sort_expr(&sort_expr));
    }

    #[test]
    fn test_name() {
        let rule = PushDownSort::new();
        assert_eq!(rule.name(), "push_down_sort");
    }
}
