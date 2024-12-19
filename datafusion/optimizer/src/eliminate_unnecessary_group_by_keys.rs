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

//! [`EliminateDuplicatedExpr`] Removes redundant expressions

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Column, HashSet, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Aggregate, Expr, LogicalPlanBuilder, Sort, SortExpr};
use indexmap::IndexSet;
use std::hash::{Hash, Hasher};

/// Optimization rule that eliminate unnecessary group by keys
#[derive(Default, Debug)]
pub struct EliminateUnnecessaryGroupByKeys {}

impl EliminateUnnecessaryGroupByKeys {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateUnnecessaryGroupByKeys {
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
        match plan {
            LogicalPlan::Aggregate(agg) => {
                let len = agg.group_expr.len();

                // Collect column group keys
                let mut column_group_keys = HashSet::new();
                for group_key in agg.group_expr.iter() {
                    if let Expr::Column(col) = group_key {
                        column_group_keys.insert(col.clone());
                    }
                }

                // If no group keys, just return
                if column_group_keys.is_empty() {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(agg)));
                }

                // Try to eliminate the unnecessary group keys
                let mut keep_group_by_keys = Vec::new();
                for group_key in agg.group_expr.iter() {
                    if matches!(&group_key, Expr::BinaryExpr(_))
                        || matches!(&group_key, Expr::ScalarFunction(_))
                        || matches!(&group_key, Expr::Cast(_))
                        || matches!(&group_key, Expr::TryCast(_))
                    {
                        // If all of the cols in `column_group_keys`, we should eliminate this key.
                        // For example, `a + 1` in `group by a, a + 1` should be eliminated.
                        let cols_in_key = group_key.column_refs();

                        if cols_in_key.is_empty()
                            || cols_in_key
                                .iter()
                                .any(|col| !column_group_keys.contains(*col))
                        {
                            keep_group_by_keys.push(group_key.clone());
                        }
                    } else {
                        keep_group_by_keys.push(group_key.clone());
                    }
                }

                if len != keep_group_by_keys.len() {
                    let projection_expr =
                        agg.group_expr.into_iter().chain(agg.aggr_expr.clone());
                    let new_plan = LogicalPlanBuilder::from(agg.input)
                        .aggregate(keep_group_by_keys, agg.aggr_expr)?
                        .project(projection_expr)?
                        .build()?;

                    Ok(Transformed::yes(new_plan))
                } else {
                    Ok(Transformed::no(LogicalPlan::Aggregate(agg)))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &str {
        "eliminate_unnecessary_group_by_keys"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test::*, Optimizer, OptimizerContext};
    use datafusion_expr::{
        binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, Operator,
    };

    use datafusion_functions_aggregate::expr_fn::count;
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        crate::test::assert_optimized_plan_eq(
            Arc::new(EliminateUnnecessaryGroupByKeys::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn eliminate_binary_group_by_keys() {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a"), binary_expr(col("a"), Operator::Plus, lit(1))],
                vec![count(col("c"))],
            )
            .unwrap()
            .build()
            .unwrap();

        let opt_context = OptimizerContext::new().with_max_passes(1);
        let optimizer =
            Optimizer::with_rules(vec![Arc::new(EliminateUnnecessaryGroupByKeys::new())]);
        let optimized_plan = optimizer
            .optimize(
                plan,
                &opt_context,
                |_plan: &LogicalPlan, _rule: &dyn OptimizerRule| {},
            )
            .unwrap();
        println!("{optimized_plan}");
        // let expected = "Limit: skip=5, fetch=10\
        // \n  Sort: test.a ASC NULLS LAST, test.b ASC NULLS LAST, test.c ASC NULLS LAST\
        // \n    TableScan: test";
        // assert_optimized_plan_eq(plan, expected)
    }

    //     #[test]
    //     fn eliminate_sort_exprs_with_options() -> Result<()> {
    //         let table_scan = test_table_scan().unwrap();
    //         let sort_exprs = vec![
    //             col("a").sort(true, true),
    //             col("b").sort(true, false),
    //             col("a").sort(false, false),
    //             col("b").sort(false, true),
    //         ];
    //         let plan = LogicalPlanBuilder::from(table_scan)
    //             .sort(sort_exprs)?
    //             .limit(5, Some(10))?
    //             .build()?;
    //         let expected = "Limit: skip=5, fetch=10\
    //         \n  Sort: test.a ASC NULLS FIRST, test.b ASC NULLS LAST\
    //         \n    TableScan: test";
    //         assert_optimized_plan_eq(plan, expected)
    //     }
    // }
}
