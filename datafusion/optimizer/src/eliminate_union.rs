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

//! [`EliminateEmptyUnion`] removes `UnionExec` with zero or one inputs.

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
            // Removes `LogicalPlan::Union` if it has zero or one inputs
            match u.inputs.len() {
                0 => {
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(&u.schema),
                        },
                    )));
                }
                1 => {
                    return Ok(Transformed::yes(u.inputs[0].as_ref().clone()));
                }
                _ => return Ok(Transformed::no(plan)),
            }
        }
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::vec::Vec;
    use std::sync::Arc;

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

    // Helper function for no input
    fn zero_input_union_like(plan_for_schema: &LogicalPlan) -> LogicalPlan {
        LogicalPlan::Union(Union {
            inputs: vec![],
            schema: Arc::clone(plan_for_schema.schema()),
        })
    }

    #[test]
    fn union_zero_inputs_eliminates_to_empty_relation() -> Result<()> {
        let scan = test_table_scan()?;
        let base = LogicalPlanBuilder::from(scan)
            .project(vec![col("a")])?
            .build()?;

        let plan = zero_input_union_like(&base);

        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

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

    // Sanity check
    #[test]
    fn union_two_inputs() -> Result<()> {
        let scan1 = test_table_scan()?;
        let left = LogicalPlanBuilder::from(scan1)
            .project(vec![col("a")])?
            .build()?;

        let scan2 = test_table_scan()?;
        let right = LogicalPlanBuilder::from(scan2)
            .project(vec![col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left.clone())
            .union(right.clone())?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: test.a
            TableScan: test
          Projection: test.a
            TableScan: test
        ")
    }

    #[test]
    fn nested_union_inner_single_input_is_unwrapped() -> Result<()> {
        let scan = test_table_scan()?;
        let leaf = LogicalPlanBuilder::from(scan)
            .project(vec![col("a"), col("b")])?
            .build()?;

        // build with single input which should be eliminated
        let inner_union = single_input_union(&leaf);

        // Outer union has 2 inputs
        let plan = LogicalPlanBuilder::from(inner_union)
            .union(leaf.clone())?
            .build()?;

        // After rewrite, the inner union disappears,
        // but the outer union still has 2 inputs so it remains.
        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: test.a, test.b
            TableScan: test
          Projection: test.a, test.b
            TableScan: test
        ")
    }

    #[test]
    fn union_single_and_zero_input() -> Result<()> {
        let scan = test_table_scan()?;
        let leaf = LogicalPlanBuilder::from(scan)
            .project(vec![col("a")])?
            .build()?;

        let empty_like = zero_input_union_like(&leaf);
        let single_like = single_input_union(&leaf);

        let plan = LogicalPlanBuilder::from(empty_like)
            .union(single_like)?
            .build()?;

        // left side becomes `EmptyRelation: rows=0` (kept as a child),
        // but since the outer union still has 2 children, it remains a Union
        assert_optimized_plan_equal!(plan.clone(), @r"
        Union
          EmptyRelation: rows=0
          Projection: test.a
            TableScan: test
        ")?;

        Ok(())
    }
}
