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

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use datafusion_expr::{EmptyRelation, JoinType, Projection};
use std::sync::Arc;

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
        match &optimized_children_plan {
            LogicalPlan::EmptyRelation(_) => Ok(optimized_children_plan),
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Limit(_) => match empty_child(&optimized_children_plan)? {
                Some(empty) => Ok(empty),
                None => Ok(optimized_children_plan),
            },
            LogicalPlan::CrossJoin(_) => {
                let (left_empty, right_empty) = binary_plan_children_is_empty(plan)?;
                if left_empty || right_empty {
                    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: plan.schema().clone(),
                    }))
                } else {
                    Ok(optimized_children_plan)
                }
            }
            LogicalPlan::Join(join) => {
                // TODO: For Join, more join type need to be careful:
                // For LeftOuter/LeftSemi/LeftAnti Join, only the left side is empty, the Join result is empty.
                // For LeftSemi Join, if the right side is empty, the Join result is empty.
                // For LeftAnti Join, if the right side is empty, the Join result is left side(should exclude null ??).
                // For RightOuter/RightSemi/RightAnti Join, only the right side is empty, the Join result is empty.
                // For RightSemi Join, if the left side is empty, the Join result is empty.
                // For RightAnti Join, if the left side is empty, the Join result is right side(should exclude null ??).
                // For Full Join, only both sides are empty, the Join result is empty.
                // For LeftOut/Full Join, if the right side is empty, the Join can be eliminated with a Projection with left side
                // columns + right side columns replaced with null values.
                // For RightOut/Full Join, if the left side is empty, the Join can be eliminated with a Projection with right side
                // columns + left side columns replaced with null values.
                if join.join_type == JoinType::Inner {
                    let (left_empty, right_empty) = binary_plan_children_is_empty(plan)?;
                    if left_empty || right_empty {
                        Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: plan.schema().clone(),
                        }))
                    } else {
                        Ok(optimized_children_plan)
                    }
                } else {
                    Ok(optimized_children_plan)
                }
            }
            LogicalPlan::Union(union) => {
                let (left_empty, right_empty) = binary_plan_children_is_empty(plan)?;
                if left_empty && right_empty {
                    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: plan.schema().clone(),
                    }))
                } else if !left_empty && right_empty {
                    Ok(LogicalPlan::Projection(Projection::new_from_schema(
                        Arc::new((**(union.inputs.get(0).unwrap())).clone()),
                        plan.schema().clone(),
                    )))
                } else if left_empty && !right_empty {
                    Ok(LogicalPlan::Projection(Projection::new_from_schema(
                        Arc::new((**(union.inputs.get(1).unwrap())).clone()),
                        plan.schema().clone(),
                    )))
                } else {
                    Ok(optimized_children_plan)
                }
            }
            LogicalPlan::Aggregate(agg) => {
                if !agg.group_expr.is_empty() {
                    match empty_child(&optimized_children_plan)? {
                        Some(empty) => Ok(empty),
                        None => Ok(optimized_children_plan),
                    }
                } else {
                    Ok(optimized_children_plan)
                }
            }
            _ => Ok(optimized_children_plan),
        }
    }

    fn name(&self) -> &str {
        "propagate_empty_relation"
    }
}

fn binary_plan_children_is_empty(plan: &LogicalPlan) -> Result<(bool, bool)> {
    let inputs = plan.inputs();

    // all binary-plan need to deal with separately.
    match inputs.len() {
        2 => {
            let left = inputs.get(0).unwrap();
            let right = inputs.get(1).unwrap();

            let left_empty = match left {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            let right_empty = match right {
                LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
                _ => false,
            };
            Ok((left_empty, right_empty))
        }
        _ => Err(DataFusionError::Plan(
            "plan just can have two child".to_string(),
        )),
    }
}

fn empty_child(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let inputs = plan.inputs();

    // all binary-plan need to deal with separately.
    match inputs.len() {
        1 => {
            let input = inputs.get(0).unwrap();
            match input {
                LogicalPlan::EmptyRelation(empty) => {
                    if !empty.produce_one_row {
                        Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: plan.schema().clone(),
                        })))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        }
        _ => Err(DataFusionError::Plan(
            "plan just can have one child".to_string(),
        )),
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
    fn propagate_union_right_empty() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        let expected = "Projection: a, b, c\
            \n  TableScan: test";
        assert_together_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn propagate_union_left_empty() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan).build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        let expected = "Projection: a, b, c\
            \n  TableScan: test2";
        assert_together_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn propagate_union_all_empty() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;
        let right_table_scan = test_table_scan_with_name("test2")?;
        let right = LogicalPlanBuilder::from(right_table_scan)
            .filter(Expr::Literal(ScalarValue::Boolean(Some(false))))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn cross_join_empty() -> Result<()> {
        let table_scan = test_table_scan()?;
        let left = LogicalPlanBuilder::from(table_scan).build()?;
        let right = LogicalPlanBuilder::empty(false).build()?;

        let plan = LogicalPlanBuilder::from(left)
            .cross_join(&right)?
            .filter(col("a").lt_eq(lit(1i64)))?
            .build()?;

        let expected = "EmptyRelation";
        assert_together_optimized_plan_eq(&plan, expected);

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
