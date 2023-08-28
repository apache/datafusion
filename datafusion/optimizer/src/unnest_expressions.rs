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

use datafusion_common::UnnestOptions;
use datafusion_expr::expr::Alias;
use datafusion_expr::expr::Unnest;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;

#[derive(Default)]
pub struct UnnestExpressions {}

impl OptimizerRule for UnnestExpressions {
    fn name(&self) -> &str {
        "unnest_expressions"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        Ok(Some(Self::optimize_internal(plan)?))
    }
}

impl UnnestExpressions {
    pub fn new() -> Self {
        Self::default()
    }

    fn optimize_internal(plan: &LogicalPlan) -> Result<LogicalPlan> {
        if let LogicalPlan::Projection(_) = plan {
            let mut array_exprs_to_unnest = vec![];

            for expr in plan.expressions() {
                if let Expr::Unnest(Unnest {
                    array_exprs,
                    options,
                }) = expr
                {
                    assert_eq!(array_exprs.len(), 1);

                    array_exprs_to_unnest.push(array_exprs[0].clone());
                } else if let Expr::Alias(Alias { expr, .. }) = expr {
                    if let Expr::Unnest(Unnest {
                        array_exprs,
                        options,
                    }) = expr.as_ref()
                    {
                        assert_eq!(array_exprs.len(), 1);

                        array_exprs_to_unnest.push(array_exprs[0].clone());
                    }
                }
            }

            if array_exprs_to_unnest.is_empty() {
                Ok(plan.clone())
            } else {
                let options: UnnestOptions = Default::default();
                let unnest_plan = LogicalPlanBuilder::from(plan.clone())
                    .unnest_arrays(array_exprs_to_unnest.clone(), options)?
                    .build()?;

                Ok(unnest_plan)
            }
        } else {
            Ok(plan.clone())
        }
    }
}
