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

//! Optimizer rule to replace `where false` on a plan with an empty relation.
//! This saves time in planning and executing the query.
//! Note that this rule should be applied after simplify expressions optimizer rule.
use crate::optimizer::ApplyOrder;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    logical_plan::{EmptyRelation, LogicalPlan},
    Expr, Filter,
};

use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that eliminate the scalar value (true/false) filter with an [LogicalPlan::EmptyRelation]
#[derive(Default)]
pub struct EliminateFilter;

impl EliminateFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateFilter {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate: Expr::Literal(ScalarValue::Boolean(Some(v))),
                input,
                ..
            }) => {
                match *v {
                    // input also can be filter, apply again
                    true => Ok(Some(
                        self.try_optimize(input, _config)?
                            .unwrap_or_else(|| input.as_ref().clone()),
                    )),
                    false => Ok(Some(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: input.schema().clone(),
                    }))),
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "eliminate_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

#[cfg(test)]
mod tests {
    use crate::eliminate_filter::EliminateFilter;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{
        col, lit, logical_plan::builder::LogicalPlanBuilder, sum, Expr, LogicalPlan,
    };
    use std::sync::Arc;

    use crate::test::*;

    fn assert_optimized_plan_equal(plan: &LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateFilter::new()), plan, expected)
    }

    #[test]
    fn filter_false() -> Result<()> {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(false)));

        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .build()?;

        // No aggregate / scan / limit
        let expected = "EmptyRelation";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn filter_false_nested() -> Result<()> {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(false)));

        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .union(plan1)?
            .build()?;

        // Left side is removed
        let expected = "Union\
            \n  EmptyRelation\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n    TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn filter_true() -> Result<()> {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(true)));

        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .build()?;

        let expected = "Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
        \n  TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn filter_true_nested() -> Result<()> {
        let filter_expr = Expr::Literal(ScalarValue::Boolean(Some(true)));

        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .filter(filter_expr)?
            .union(plan1)?
            .build()?;

        // Filter is removed
        let expected = "Union\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n    TableScan: test\
            \n  Aggregate: groupBy=[[test.a]], aggr=[[SUM(test.b)]]\
            \n    TableScan: test";
        assert_optimized_plan_equal(&plan, expected)
    }

    #[test]
    fn filter_from_subquery() -> Result<()> {
        // SELECT a FROM (SELECT a FROM test WHERE FALSE) WHERE TRUE

        let false_filter = lit(false);
        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .filter(false_filter)?
            .build()?;

        let true_filter = lit(true);
        let plan = LogicalPlanBuilder::from(plan1)
            .project(vec![col("a")])?
            .filter(true_filter)?
            .build()?;

        // Filter is removed
        let expected = "Projection: test.a\
            \n  EmptyRelation";
        assert_optimized_plan_equal(&plan, expected)
    }
}
