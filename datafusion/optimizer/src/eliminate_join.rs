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
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::JoinType::Inner;
use datafusion_expr::{
    Expr,
    logical_plan::{EmptyRelation, LogicalPlan},
};

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

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join) if join.join_type == Inner && join.on.is_empty() => {
                match join.filter {
                    Some(Expr::Literal(ScalarValue::Boolean(Some(false)), _)) => Ok(
                        Transformed::yes(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema,
                        })),
                    ),
                    _ => Ok(Transformed::no(LogicalPlan::Join(join))),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::eliminate_join::EliminateJoin;
    use datafusion_common::Result;
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::{lit, logical_plan::builder::LogicalPlanBuilder};
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
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

        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }
}
