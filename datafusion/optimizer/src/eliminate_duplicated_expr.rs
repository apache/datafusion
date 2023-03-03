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

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::Sort;
use hashbrown::HashSet;

/// Optimization rule that eliminate duplicated expr.
#[derive(Default)]
pub struct EliminateDuplicatedExpr;

impl EliminateDuplicatedExpr {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateDuplicatedExpr {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Sort(sort) => {
                let new_expr = sort.expr.iter().collect::<HashSet<_>>();
                if new_expr.len() == sort.expr.len() {
                    Ok(None)
                } else {
                    Ok(Some(LogicalPlan::Sort(Sort {
                        expr: new_expr.into_iter().cloned().collect::<Vec<_>>(),
                        input: sort.input.clone(),
                        fetch: sort.fetch,
                    })))
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_duplicated_expr"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{col, logical_plan::builder::LogicalPlanBuilder};
    use std::sync::Arc;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
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
            .sort(vec![col("a"), col("a")])?
            .limit(5, Some(10))?
            .build()?;
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a\
        \n    TableScan: test";
        assert_optimized_plan_eq(&plan, expected)
    }
}
