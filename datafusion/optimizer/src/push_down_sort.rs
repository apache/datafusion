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

//! [`PushDownSort`] pushes `SORT` requirements into `TableScan`

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::{ExprSchema, Result};
use datafusion_expr::logical_plan::{LogicalPlan, Sort};
use datafusion_expr::SortExpr;

/// Optimization rule that tries to push down `SORT` ordering requirements into `TableScan`.
///
/// This optimization allows table providers to potentially provide pre-sorted data,
/// eliminating the need for expensive Sort operations.
#[derive(Default, Debug)]
pub struct PushDownSort {}

impl PushDownSort {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Attempts to push sort expressions down to a TableScan
    fn try_push_sort_to_table_scan(
        sort_exprs: &[SortExpr],
        mut plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::TableScan(ref mut scan) => {
                // If the table scan already has a preferred ordering, don't override it
                if scan.preferred_ordering.is_some() {
                    return Ok(Transformed::no(plan));
                }

                // Set the preferred ordering on the table scan
                scan.preferred_ordering = Some(sort_exprs.to_vec());
                Ok(Transformed::yes(plan))
            }
            LogicalPlan::Filter(mut filter) => {
                // Recursively try to push down through the filter
                let filter_input = Arc::clone(&filter.input);
                let input_result = Self::try_push_sort_to_table_scan(
                    sort_exprs,
                    Arc::unwrap_or_clone(filter_input),
                )?;
                if input_result.transformed {
                    filter.input = Arc::new(input_result.data);
                    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            }
            LogicalPlan::Projection(mut projection) => {
                // For projections, we need to check if the sort expressions are still valid
                // For simplicity, we'll only push down if all sort expressions reference
                // columns that exist in the projection's input
                let input_schema = projection.input.schema();
                let sort_is_valid = sort_exprs.iter().all(|sort_expr| {
                    // Check if the sort expression is a simple column reference
                    if let datafusion_expr::Expr::Column(col) = &sort_expr.expr {
                        input_schema.field_from_column(col).is_ok()
                    } else {
                        // For non-column expressions, we'd need more sophisticated analysis
                        false
                    }
                });

                if sort_is_valid {
                    let projection_input = Arc::clone(&projection.input);
                    let input_result = Self::try_push_sort_to_table_scan(
                        sort_exprs,
                        Arc::unwrap_or_clone(projection_input),
                    )?;
                    if input_result.transformed {
                        projection.input = Arc::new(input_result.data);
                        return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
                    }
                }
                Ok(Transformed::no(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::SubqueryAlias(mut alias) => {
                // Recursively try to push down through the subquery alias
                let alias_input = Arc::clone(&alias.input);
                let input_result = Self::try_push_sort_to_table_scan(
                    sort_exprs,
                    Arc::unwrap_or_clone(alias_input),
                )?;
                if input_result.transformed {
                    alias.input = Arc::new(input_result.data);
                    Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)))
                } else {
                    Ok(Transformed::no(LogicalPlan::SubqueryAlias(alias)))
                }
            }
            _ => {
                // Cannot push sort through other operations
                Ok(Transformed::no(plan))
            }
        }
    }
}

impl OptimizerRule for PushDownSort {
    fn name(&self) -> &str {
        "push_down_sort"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Sort(sort) = plan else {
            return Ok(Transformed::no(plan));
        };

        // Try to push the sort requirements down to a table scan
        let sort_exprs = sort.expr.clone();
        let sort_input = Arc::clone(&sort.input);
        let sort_fetch = sort.fetch;

        let input_result = Self::try_push_sort_to_table_scan(
            &sort_exprs,
            Arc::unwrap_or_clone(sort_input),
        )?;

        if input_result.transformed {
            // If we successfully pushed the sort down, we can potentially eliminate the sort
            // For now, we'll keep the sort for safety, but in a full implementation
            // we could eliminate it if we're confident the table provider will provide sorted data
            Ok(Transformed::yes(LogicalPlan::Sort(Sort {
                expr: sort_exprs,
                input: Arc::new(input_result.data),
                fetch: sort_fetch,
            })))
        } else {
            // Could not push down the sort
            Ok(Transformed::no(LogicalPlan::Sort(sort)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use crate::OptimizerContext;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};

    fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let rule = PushDownSort::new();
        let config = &OptimizerContext::new();
        let result = rule.rewrite(plan, config)?;
        Ok(result.data)
    }

    #[test]
    fn test_sort_table_scan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(vec![col("a").sort(true, true)])?
            .build()?;

        let optimized = optimize(plan)?;

        // The optimized plan should have the sort pushed down to the table scan
        match &optimized {
            LogicalPlan::Sort(sort) => match sort.input.as_ref() {
                LogicalPlan::TableScan(scan) => {
                    assert!(scan.preferred_ordering.is_some());
                    assert_eq!(scan.preferred_ordering.as_ref().unwrap().len(), 1);
                }
                _ => panic!("Expected TableScan after Sort"),
            },
            _ => panic!("Expected Sort at root"),
        }

        Ok(())
    }

    #[test]
    fn test_sort_through_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .filter(col("a").gt(col("b")))?
            .sort(vec![col("c").sort(true, true)])?
            .build()?;

        let optimized = optimize(plan)?;

        // Should push sort through filter to table scan
        match &optimized {
            LogicalPlan::Sort(sort) => match sort.input.as_ref() {
                LogicalPlan::Filter(filter) => match filter.input.as_ref() {
                    LogicalPlan::TableScan(scan) => {
                        assert!(scan.preferred_ordering.is_some());
                    }
                    _ => panic!("Expected TableScan after Filter"),
                },
                _ => panic!("Expected Filter after Sort"),
            },
            _ => panic!("Expected Sort at root"),
        }

        Ok(())
    }
}
