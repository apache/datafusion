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
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{Aggregate, Expr, Sort, SortExpr};
use indexmap::IndexSet;
use std::hash::{Hash, Hasher};
/// Optimization rule that eliminate duplicated expr.
#[derive(Default, Debug)]
pub struct EliminateDuplicatedExpr;

impl EliminateDuplicatedExpr {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
// use this structure to avoid initial clone
#[derive(Eq, Clone, Debug)]
struct SortExprWrapper(SortExpr);
impl PartialEq for SortExprWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.expr == other.0.expr
    }
}
impl Hash for SortExprWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.expr.hash(state);
    }
}
impl OptimizerRule for EliminateDuplicatedExpr {
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
            LogicalPlan::Sort(sort) => {
                let len = sort.expr.len();
                let unique_exprs: Vec<_> = sort
                    .expr
                    .into_iter()
                    .map(SortExprWrapper)
                    .collect::<IndexSet<_>>()
                    .into_iter()
                    .map(|wrapper| wrapper.0)
                    .collect();

                let transformed = if len != unique_exprs.len() {
                    Transformed::yes
                } else {
                    Transformed::no
                };

                Ok(transformed(LogicalPlan::Sort(Sort {
                    expr: unique_exprs,
                    input: sort.input,
                    fetch: sort.fetch,
                })))
            }
            LogicalPlan::Aggregate(agg) => {
                let len = agg.group_expr.len();

                let unique_exprs: Vec<Expr> = agg
                    .group_expr
                    .into_iter()
                    .collect::<IndexSet<_>>()
                    .into_iter()
                    .collect();

                let transformed = if len != unique_exprs.len() {
                    Transformed::yes
                } else {
                    Transformed::no
                };

                Aggregate::try_new(agg.input, unique_exprs, agg.aggr_expr)
                    .map(|f| transformed(LogicalPlan::Aggregate(f)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
    fn name(&self) -> &str {
        "eliminate_duplicated_expr"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        crate::test::assert_optimized_plan_eq(
            Arc::new(EliminateDuplicatedExpr::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn eliminate_sort_expr() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort_by(vec![col("a"), col("a"), col("b"), col("c")])?
            .limit(5, Some(10))?
            .build()?;
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a ASC NULLS LAST, test.b ASC NULLS LAST, test.c ASC NULLS LAST\
        \n    TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }

    #[test]
    fn eliminate_sort_exprs_with_options() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let sort_exprs = vec![
            col("a").sort(true, true),
            col("b").sort(true, false),
            col("a").sort(false, false),
            col("b").sort(false, true),
        ];
        let plan = LogicalPlanBuilder::from(table_scan)
            .sort(sort_exprs)?
            .limit(5, Some(10))?
            .build()?;
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a ASC NULLS FIRST, test.b ASC NULLS LAST\
        \n    TableScan: test";
        assert_optimized_plan_eq(plan, expected)
    }
}
