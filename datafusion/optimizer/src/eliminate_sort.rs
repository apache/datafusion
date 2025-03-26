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

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::Result;
use datafusion_expr::LogicalPlan;

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// An optimizer rule that eliminates unnecessary Sort operators in subqueries.
#[derive(Default, Debug)]
pub struct EliminateSort {}

impl EliminateSort {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateSort {
    fn name(&self) -> &str {
        "optimize_subquery_sort"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // When initializing subqueries, we examine sort options since they might be unnecessary.
        // They are only important if the subquery result is affected by the ORDER BY statement,
        // which can happen when we have:
        // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
        // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
        // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
        let mut has_limit = false;
        let new_plan = plan.transform_down(|c| {
            if let LogicalPlan::Limit(_) = c {
                has_limit = true;
                return Ok(Transformed::no(c));
            }
            match c {
                LogicalPlan::Sort(s) => {
                    if !has_limit {
                        has_limit = false;
                        return Ok(Transformed::yes(s.input.as_ref().clone()));
                    }
                    Ok(Transformed::no(LogicalPlan::Sort(s)))
                }
                _ => Ok(Transformed::no(c)),
            }
        });
        new_plan
    }
}
