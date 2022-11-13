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
use datafusion_expr::EmptyRelation;

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
        let optimized_plan_opt = match &optimized_children_plan {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
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

    let contains_empty = match inputs.len() {
        1 => {
            let input = inputs.get(0)?;
            match input {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            }
        }
        2 => {
            let left = inputs.get(0)?;
            let right = inputs.get(1)?;

            let left_empty = match left {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            let right_empty = match right {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            left_empty || right_empty
        }
        _ => false,
    };

    if contains_empty {
        Some(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: plan.schema().clone(),
        }))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_filter::EliminateFilter;
    use crate::test::{test_table_scan, test_table_scan_with_name};
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{
        binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, Expr, JoinType,
        Operator,
    };

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

    fn assert_together_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let optimize_one = EliminateFilter::new()
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let optimize_two = PropagateEmptyRelation::new()
            .optimize(&optimize_one, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimize_two);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimize_two.schema());
    }

    #[test]
    fn propagate_empty() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(true))))?
            .limit(10, None)?
            .project(vec![binary_expr(lit(1), Operator::Plus, lit(1))])?
            .build()?;

        let expected = "EmptyRelation";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn cooperate_with_eliminate_filter() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .project(vec![col("a")])?
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join_using(
                &right,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
