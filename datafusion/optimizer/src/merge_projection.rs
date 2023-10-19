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

use crate::optimizer::ApplyOrder;
use datafusion_common::Result;
use datafusion_expr::{Expr, LogicalPlan, Projection};
use std::collections::HashMap;

use crate::push_down_filter::replace_cols_by_name;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that merge [LogicalPlan::Projection].
#[derive(Default)]
pub struct MergeProjection;

impl MergeProjection {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for MergeProjection {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(parent_projection) => {
                match parent_projection.input.as_ref() {
                    LogicalPlan::Projection(child_projection) => {
                        let new_plan =
                            merge_projection(parent_projection, child_projection)?;
                        Ok(Some(
                            self.try_optimize(&new_plan, _config)?.unwrap_or(new_plan),
                        ))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "merge_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

pub(super) fn merge_projection(
    parent_projection: &Projection,
    child_projection: &Projection,
) -> Result<LogicalPlan> {
    let replace_map = collect_projection_expr(child_projection);
    let new_exprs = parent_projection
        .expr
        .iter()
        .map(|expr| replace_cols_by_name(expr.clone(), &replace_map))
        .enumerate()
        .map(|(i, e)| match e {
            Ok(e) => {
                let parent_expr = parent_projection.schema.fields()[i].qualified_name();
                e.alias_if_changed(parent_expr)
            }
            Err(e) => Err(e),
        })
        .collect::<Result<Vec<_>>>()?;
    let new_plan = LogicalPlan::Projection(Projection::try_new(
        new_exprs,
        child_projection.input.clone(),
    )?);
    Ok(new_plan)
}

pub fn collect_projection_expr(projection: &Projection) -> HashMap<String, Expr> {
    projection
        .schema
        .fields()
        .iter()
        .enumerate()
        .flat_map(|(i, field)| {
            // strip alias
            let expr = projection.expr[i].clone().unalias();
            // Convert both qualified and unqualified fields
            [
                (field.name().clone(), expr.clone()),
                (field.qualified_name(), expr),
            ]
        })
        .collect::<HashMap<_, _>>()
}

#[cfg(test)]
mod tests {
    use crate::merge_projection::MergeProjection;
    use datafusion_common::Result;
    use datafusion_expr::{
        binary_expr, col, lit, logical_plan::builder::LogicalPlanBuilder, LogicalPlan,
        Operator,
    };
    use std::sync::Arc;

    use crate::test::*;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(MergeProjection::new()), plan, expected)
    }

    #[test]
    fn merge_two_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn merge_three_projection() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a"), col("b")])?
            .project(vec![col("a")])?
            .project(vec![binary_expr(lit(1), Operator::Plus, col("a"))])?
            .build()?;

        let expected = "Projection: Int32(1) + test.a\
        \n  TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn merge_alias() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .project(vec![col("a").alias("alias")])?
            .build()?;

        let expected = "Projection: test.a AS alias\
        \n  TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }
}
