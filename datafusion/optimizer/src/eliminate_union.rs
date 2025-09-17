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

//! [`EliminateUnion`] removes `UnionExec` with zero or one inputs.

use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::{EmptyRelation, LogicalPlan};
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

#[derive(Default, Debug)]
pub struct EliminateUnion;

impl EliminateUnion {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateUnion {
    fn name(&self) -> &str {
        "eliminate_union"
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
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Union(u) = &plan {
            let mut kept_inputs = Vec::with_capacity(u.inputs.len());
            for child in &u.inputs {
                match child.as_ref() {
                    LogicalPlan::EmptyRelation(e) if !e.produce_one_row => {}
                    _ => kept_inputs.push(Arc::clone(child)),
                }
            }

            match kept_inputs.len() {
                0 => {
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(&u.schema),
                        },
                    )));
                }
                1 => {
                    return Ok(Transformed::yes(kept_inputs[0].as_ref().clone()));
                }
                // Create new union plan with remaining inputs
                remaining_inputs if remaining_inputs < u.inputs.len() => {
                    let new_union = datafusion_expr::Union {
                        inputs: kept_inputs,
                        schema: Arc::clone(&u.schema),
                    };
                    return Ok(Transformed::yes(LogicalPlan::Union(new_union)));
                }
                _ => {
                    return Ok(Transformed::no(plan));
                }
            }
        }

        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::vec::Vec;

    use crate::assert_optimized_plan_eq_snapshot;
    use crate::{OptimizerContext, OptimizerRule};
    use datafusion_common::Result;
    use datafusion_expr::{
        col, logical_plan::builder::LogicalPlanBuilder, LogicalPlan, Union,
    };

    use crate::test::*;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
                vec![Arc::new(EliminateUnion::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    // Helper function for single input
    fn single_input_union(plan: &LogicalPlan) -> LogicalPlan {
        LogicalPlan::Union(Union {
            inputs: vec![Arc::new(plan.clone())],
            schema: Arc::clone(plan.schema()),
        })
    }

    // Create an empty input
    fn empty_input(like: &LogicalPlan) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(like.schema()),
        })
    }

    #[test]
    fn union_all_empty_children_eliminates_to_empty_relation() -> Result<()> {
        let scan = test_table_scan()?;
        let base = LogicalPlanBuilder::from(scan)
            .project(vec![col("a")])?
            .build()?;

        let e1 = empty_input(&base);
        let e2 = empty_input(&base);
        let e3 = empty_input(&base);

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(e1), Arc::new(e2), Arc::new(e3)],
            schema: Arc::clone(base.schema()),
        });

        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

    #[test]
    fn union_prunes_to_single_child_unwraps() -> Result<()> {
        let scan = test_table_scan()?;
        let survivor = LogicalPlanBuilder::from(scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        let e1 = empty_input(&survivor);
        let e2 = empty_input(&survivor);

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(e1), Arc::new(survivor.clone()), Arc::new(e2)],
            schema: Arc::clone(survivor.schema()),
        });

        // Expect union removed, just the survivor subtree remains
        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.b
          TableScan: test
        ")
    }

    #[test]
    fn union_prunes_some_children_but_keeps_union() -> Result<()> {
        let scan1 = test_table_scan()?;
        let left = LogicalPlanBuilder::from(scan1)
            .project(vec![col("a")])?
            .build()?;

        let scan2 = test_table_scan()?;
        let right = LogicalPlanBuilder::from(scan2)
            .project(vec![col("a")])?
            .build()?;

        let empty = empty_input(&left);

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(left.clone()), Arc::new(empty), Arc::new(right.clone())],
            schema: Arc::clone(left.schema()),
        });

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: test.a
            TableScan: test
          Projection: test.a
            TableScan: test
        ")
    }

    #[test]
    fn nested_union_inner_empty_collapses() -> Result<()> {
        let scan = test_table_scan()?;
        let leaf = LogicalPlanBuilder::from(scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        let inner = {
            let e1 = empty_input(&leaf);
            let e2 = empty_input(&leaf);
            LogicalPlan::Union(Union {
                inputs: vec![Arc::new(e1), Arc::new(e2)],
                schema: Arc::clone(leaf.schema()),
            })
        };

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(inner), Arc::new(leaf.clone())],
            schema: Arc::clone(leaf.schema()),
        });

        // After rewrite:
        // - inner union -> EmptyRelation(rows=0)
        // - outer union has two inputs (EmptyRelation + real plan) -> remains a Union
        assert_optimized_plan_equal!(plan, @r"
        Union
          EmptyRelation: rows=0
          Projection: test.a, test.b
            TableScan: test
        ")
    }

    // sanity check
    #[test]
    fn union_two_non_empty_children_is_unchanged() -> Result<()> {
        let scan1 = test_table_scan()?;
        let left = LogicalPlanBuilder::from(scan1)
            .project(vec![col("a")])?
            .build()?;

        let scan2 = test_table_scan()?;
        let right = LogicalPlanBuilder::from(scan2)
            .project(vec![col("a")])?
            .build()?;

        let plan = LogicalPlan::Union(Union {
            inputs: vec![Arc::new(left.clone()), Arc::new(right.clone())],
            schema: Arc::clone(left.schema()),
        });

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: test.a
            TableScan: test
          Projection: test.a
            TableScan: test
        ")
    }

    // Also validate the simple single-input-union unwrap (no empties involved)
    #[test]
    fn union_single_input_is_unwrapped() -> Result<()> {
        let scan = test_table_scan()?;
        let inner = LogicalPlanBuilder::from(scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        let plan = single_input_union(&inner);

        assert_optimized_plan_equal!(plan, @r"
        Projection: test.a, test.b
          TableScan: test
        ")
    }
}
