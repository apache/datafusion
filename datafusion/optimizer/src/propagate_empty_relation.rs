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

use datafusion_common::Result;
use datafusion_expr::logical_plan::LogicalPlan;

use crate::{utils, OptimizerConfig, OptimizerRule};

/// Optimization rule that bottom-up to eliminate plan by propagating empty_relation.
#[derive(Default)]
pub struct PropagateEmptyRelation;

impl PropagateEmptyRelation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PropagateEmptyRelation {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // optimize child plans first
        let optimized_children_plan =
            utils::optimize_children(self, plan, optimizer_config)?;
        let optimized_plan_opt = match optimized_children_plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_) => empty_child(&optimized_children_plan),
            _ => None,
        };

        match optimized_plan_opt {
            Some(optimized_plan) => Ok(optimized_plan),
            None => Ok(optimized_children_plan),
        }
    }

    fn name(&self) -> &str {
        "eliminate_limit"
    }
}

fn empty_child(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let inputs = plan.inputs();

    if inputs.len() == 1 {
        let input = inputs.get(0)?;
        match input {
            LogicalPlan::EmptyRelation(empty) => {
                if !empty.produce_one_row {
                    Some((*input).clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    } else if inputs.len() == 2 {
        let left = inputs.get(0)?;
        let right = inputs.get(1)?;
        let left_opt = match left {
            LogicalPlan::EmptyRelation(empty) => {
                if !empty.produce_one_row {
                    Some((*left).clone())
                } else {
                    None
                }
            }
            _ => None,
        };
        if left_opt.is_some() {
            return left_opt;
        }
        match right {
            LogicalPlan::EmptyRelation(empty) => {
                if !empty.produce_one_row {
                    Some((*right).clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_expr::{logical_plan::builder::LogicalPlanBuilder, Expr};

    use super::*;

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = PropagateEmptyRelation::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn propagate_empty() {
        let filter_true = Expr::Literal(ScalarValue::Boolean(Some(true)));

        let plan = LogicalPlanBuilder::empty(false)
            .filter(filter_true.clone())
            .unwrap()
            .filter(filter_true.clone())
            .unwrap()
            .filter(filter_true)
            .unwrap()
            .build()
            .unwrap();

        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);
    }
}
