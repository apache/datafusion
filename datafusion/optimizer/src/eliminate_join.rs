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

//! [`EliminateJoin`] rewrites `INNER JOIN` with `true`/`null`

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::tree_node::LogicalPlanPattern;
use datafusion_expr::JoinType::Inner;
use datafusion_expr::{
    logical_plan::{EmptyRelation, LogicalPlan},
    Expr,
};
use enumset::enum_set;

/// Eliminates joins when join condition is false.
/// Replaces joins when inner join condition is true with a cross join.
#[derive(Default, Debug)]
pub struct EliminateJoin;

impl EliminateJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateJoin {
    fn name(&self) -> &str {
        "eliminate_join"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down_with_subqueries(|plan| {
            if !plan.stats().contains_all_patterns(enum_set!(
                LogicalPlanPattern::LogicalPlanJoin | LogicalPlanPattern::ExprLiteral
            )) {
                return Ok(Transformed::jump(plan));
            }

            match plan {
                LogicalPlan::Join(join, _)
                    if join.join_type == Inner && join.on.is_empty() =>
                {
                    match join.filter {
                        Some(Expr::Literal(ScalarValue::Boolean(Some(false)), _)) => {
                            Ok(Transformed::yes(LogicalPlan::empty_relation(
                                EmptyRelation {
                                    produce_one_row: false,
                                    schema: join.schema,
                                },
                            )))
                        }
                        _ => Ok(Transformed::no(LogicalPlan::join(join))),
                    }
                }
                _ => Ok(Transformed::no(plan)),
            }
        })
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_join::EliminateJoin;
    use crate::test::*;
    use datafusion_common::Result;
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::{lit, logical_plan::builder::LogicalPlanBuilder, LogicalPlan};
    use std::sync::Arc;

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateJoin::new()), plan, expected)
    }

    #[test]
    fn join_on_false() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .join_on(
                LogicalPlanBuilder::empty(false).build()?,
                Inner,
                Some(lit(false)),
            )?
            .build()?;

        let expected = "EmptyRelation";
        assert_optimized_plan_equal(plan, expected)
    }
}
