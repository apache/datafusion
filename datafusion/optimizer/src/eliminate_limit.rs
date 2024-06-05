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

//! [`EliminateLimit`] eliminates `LIMIT` when possible
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{internal_err, Result};
use datafusion_expr::logical_plan::{tree_node::unwrap_arc, EmptyRelation, LogicalPlan};

/// Optimizer rule to replace `LIMIT 0` or `LIMIT` whose ancestor LIMIT's skip is
/// greater than or equal to current's fetch
///
/// It can cooperate with `propagate_empty_relation` and `limit_push_down`. on a
/// plan with an empty relation.
///
/// This rule also removes OFFSET 0 from the [LogicalPlan]
#[derive(Default)]
pub struct EliminateLimit;

impl EliminateLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateLimit {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called EliminateLimit::rewrite")
    }

    fn name(&self) -> &str {
        "eliminate_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<
        datafusion_common::tree_node::Transformed<LogicalPlan>,
        datafusion_common::DataFusionError,
    > {
        match plan {
            LogicalPlan::Limit(limit) => {
                if let Some(fetch) = limit.fetch {
                    if fetch == 0 {
                        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            EmptyRelation {
                                produce_one_row: false,
                                schema: limit.input.schema().clone(),
                            },
                        )));
                    }
                } else if limit.skip == 0 {
                    // input also can be Limit, so we should apply again.
                    return Ok(self.rewrite(unwrap_arc(limit.input), _config).unwrap());
                }
                Ok(Transformed::no(LogicalPlan::Limit(limit)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}
